/*
 * sched_drain.pml -- Promela model of the SINGLE-THREAD scheduler drain loop +
 * its deadlock detector in src/runloom_c/runloom_sched_drain.c.inc
 * (runloom_sched_drain).  The single-thread analogue of the M:N mn_run census.
 *
 * The drain is a `while (!stopping && CENSUS) { ... }` loop, where CENSUS is the
 * disjunction of every source of runnable-or-wakeable work the scheduler tracks:
 *
 *     while (!s->stopping && (!runloom_sched_ready_empty(s) ||  // a g on the ready ring
 *                             s->sleep_size  > 0  ||            // a sleeper due/pending
 *                             s->timer_size  > 0  ||            // an in-memory timed park
 *                             netpoll_parked > 0  ||            // an fd parker
 *                             iouring_inflight() > 0 ||
 *                             blockpool_inflight() > 0 ||
 *                             foreign_park_inflight() > 0 ||
 *                             s->quiescence_head != NULL ||     // run_ready waiters
 *                             wake_list_head != NULL))          // a cross-thread wake
 *
 * The DEADLOCK detector runs ONLY AFTER the loop exits, and ONLY when the exit
 * was genuine quiescence (not a clean sched_stop and not a signal break): every
 * one of those census sources was empty -- so any fiber still parked on a
 * channel/park_safe has no event source left to wake it (Go's "all goroutines
 * are asleep - deadlock!").
 *
 * SUBSYSTEM MODELLED (kept SMALL + proportionate -- this is a low-priority gap):
 * one drain loop, one ready item (a g that lands on the ready ring), and one
 * POSSIBLE foreign wake.  The foreign wake is the interesting case: a cross-
 * thread waker (a run_in_executor pool worker / an io_uring CQE) publishes its
 * intent to wake a parked g by making wake_list_head non-NULL BEFORE the g is
 * actually pushed onto the ready ring (runloom_sched_drain_wake_list does the
 * head-load then the per-g push).  The census must hold the loop alive on that
 * not-yet-on-ready window, or the g is stranded.
 *
 * PROVEN (all interleavings, invalid-end-state idiom from blockpool.pml):
 *   NO LOST WAKE / NO PREMATURE EXIT -- the drain never exits the loop while
 *      runnable-or-wakeable work remains: a g on the ready ring, OR a foreign
 *      wake that has published wake_list_head but not yet pushed the g.  The
 *      parked g blocks on its resume; a premature exit strands it forever, which
 *      Spin reports as an invalid end state (the parked-g proctype can't reach
 *      its end label).
 *   RESUMED ONCE -- the g is resumed exactly once (assert before the resume).
 *   QUIESCENT-ONLY DEADLOCK -- the drain only reaches its post-loop deadlock-
 *      detector branch (modelled as `declared_deadlock`) when genuinely
 *      quiescent: ready ring empty AND no wake outstanding.  Asserted at the
 *      detector site.
 *
 * NEGATIVE CONTROL (-DBUG_EXIT_WITH_WORK): the drain's exit test checks ONLY the
 * ready ring (runloom_sched_ready_empty) and OMITS the wakeable-work / wake-list
 * census term.  A foreign wake that has published wake_list_head (so a g is
 * about to be pushed) but has not yet pushed it leaves the ready ring momentarily
 * empty; the buggy loop sees "empty -> exit", strands the g, and -- worse --
 * declares deadlock with work still pending.  Spin finds the invalid end state
 * (the parked g never resumes).
 */

bit ready          = 0;   /* a g is on the ready ring (runloom_sched_ready)        */
bit wake_published = 0;   /* foreign waker set wake_list_head != NULL (intent)   */
bit wake_active    = 0;   /* a foreign wake is in flight (census keep-alive term) */
bit g_resumed      = 0;   /* the drain popped + resumed the g                    */
bit g_parked       = 0;   /* the parked g is yielded, waiting for its resume     */
bit declared_dead  = 0;   /* the post-loop deadlock detector branch was taken    */

/* The parked goroutine.  Before it can yield, it has REGISTERED the wake source
 * it is parking on -- the keep-alive census term (wake_active): the *_inflight
 * counter (blockpool/iouring/foreign-park) or netpoll_parked.  This mirrors the
 * source's discipline: a g never parks "on nothing" with a foreign rescue
 * outstanding -- the inflight/parked count is bumped BEFORE the yield, on the
 * drain thread (the blockpool `inflight++` happens-before-park ordering, the
 * cross_thread_wake `g->owner` registration).  So when the drain regains control
 * the keep-alive is ALREADY visible to its census.  (A g that parks with NO wake
 * source registered -- e.g. blocked on a dead channel -- is a GENUINE deadlock,
 * which is the detector's job, not a lost wake; that case is wake_active staying
 * 0, and the drain correctly exits + declares deadlock.  We model the
 * has-a-rescue case, where a premature exit IS a lost wake.)
 *
 * If the drain exits prematurely, g_resumed never goes 1 and this proctype is
 * stuck at the (g_resumed) statement -> an invalid end state Spin reports. */
active proctype parked_g()
{
    atomic {
        wake_active = 1;     /* register the wake source (inflight++/netpoll_parked) */
        g_parked    = 1;     /* ...THEN yield back to the drain, now parked          */
    }
    (g_resumed == 1);        /* park_safe: blocks until the drain resumes us.
                              * A premature drain exit never sets this. */
}

/* A FOREIGN-thread waker: a run_in_executor pool worker or an io_uring CQE that
 * resolves a future the parked g awaits.  The keep-alive (wake_active) is
 * already up (registered at park, above).  The waker PUBLISHES its head
 * (wake_list_head := non-NULL) so the drain's top-of-loop wake-list drain will
 * push the g onto the ready ring.  The window between "published" and "on the
 * ready ring" -- and the earlier window where only the keep-alive is up -- is
 * exactly what the census must cover. */
active proctype foreign_waker()
{
    (g_parked == 1);         /* the wake targets a parked g (wake_safe sees parked_safe) */
    atomic {
        wake_published = 1;  /* wake_list_head := non-NULL (intent published)        */
    }
    /* runloom_sched_drain_wake_list runs ON the drain thread (see below); it is
     * what actually moves the g from wake_list onto the ready ring, then clears
     * the keep-alive (re-queue-before-drop, the blockpool discipline).  So the
     * push + the clear happen inside the drain proctype, not here -- faithful to
     * runloom_sched_drain_wake_list being called from inside the loop body. */
}

/* The single-thread scheduler drain loop. */
active proctype drain()
{
    /* The drain regains control only after the g has parked (it resumed the g,
     * the g yielded back).  Until then there is nothing to drain. */
    (g_parked == 1);

    do
    /* Top of loop: drain any published cross-thread wake into the ready ring
     * (runloom_sched_drain_wake_list).  Publish -> push -> drop the keep-alive,
     * in THAT order, so the instant the keep-alive clears the g is already on
     * the ready ring (the blockpool re-queue-before-dec discipline). */
    :: atomic {
           (wake_published == 1) ->
           wake_published = 0;
           ready = 1;            /* push the woken g onto the ready ring FIRST */
           wake_active = 0;      /* ... then drop the wake_list/inflight keep-alive */
       }
    /* Pop a ready g and resume it (the runloom_ready_pop + runloom_coro_resume arm). */
    :: atomic {
           (ready == 1) ->
           ready = 0;
           assert(g_resumed == 0);   /* RESUMED ONCE */
           g_resumed = 1;
       }
    /* The loop's exit test == the drain CENSUS.  Exit ONLY when EVERY source of
     * runnable-or-wakeable work is empty. */
    :: atomic {
#ifdef BUG_EXIT_WITH_WORK
           /* NEGATIVE CONTROL: exit on an empty ready ring WITHOUT consulting the
            * wakeable-work / wake-list census term.  A foreign wake that has
            * published (wake_active==1) but not yet been drained onto the ready
            * ring leaves `ready==0` -> the buggy loop exits and strands the g. */
           (ready == 0) -> break;
#else
           /* FAITHFUL: the full census -- ready ring AND the wake keep-alive (the
            * wake_list_head / *_inflight / netpoll_parked disjunction).  Stay
            * alive while a foreign wake is in flight, even if not yet on ready. */
           (ready == 0 && wake_active == 0 && wake_published == 0) -> break;
#endif
       }
    od;

    /* Post-loop DEADLOCK detector (runloom_count_deadlockable_fibers branch).  It
     * runs only on a genuine-quiescence exit.  The drain MUST be quiescent here:
     * nothing on the ready ring and no wake outstanding.  Under the faithful
     * census this assertion holds; under BUG_EXIT_WITH_WORK the loop can fall
     * through here with a wake still active -> it would declare deadlock on live
     * work (and, more visibly, the parked g is already stranded -> invalid end
     * state). */
    declared_dead = 1;
    assert(ready == 0 && wake_active == 0 && wake_published == 0);
}
