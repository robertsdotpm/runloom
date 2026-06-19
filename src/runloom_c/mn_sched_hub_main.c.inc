/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- randomized work-stealing victim order (Go's randomOrder) ----
 * A fixed linear scan that always starts at hub 0 makes K idle hubs collide on
 * hub 0's single-element steal CAS simultaneously, then advance together to hub
 * 1, repeating the convoy -- thundering-herd CAS contention + load asymmetry at
 * high hub counts.  Instead each hub enumerates victims in a DISTINCT order:
 * start at its own id and step by a stride coprime with the hub count (so the
 * full residue set 0..count-1 is visited exactly once).  The start + stride are
 * derived deterministically from h->id, so the order is fixed per hub (no
 * wall-clock entropy) and the controlled-replay schedule is unchanged -- and in
 * replay mode steal is disabled anyway (runloom_mn_ctrl_no_steal). */
static int runloom_steal_gcd(int a, int b)
{
    while (b != 0) { int t = a % b; a = b; b = t; }
    return a;
}

/* A stride in [1, n-1] coprime with n, varied by the hub id.  Always returns
 * (s==1 is coprime with every n, and 1 is in the 1..n-1 cycle, so it
 * terminates).  Caller guarantees n >= 2. */
static int runloom_steal_stride(int n, int id)
{
    int s;
    if (n < 3) return 1;                          /* n==2: stride 1 is the only option */
    for (s = 1 + (id % (n - 1)); ; s = (s % (n - 1)) + 1)
        if (runloom_steal_gcd(s, n) == 1) return s;
}

/* gc-stop-the-world fairness (free-threaded 3.13+ only).
 *
 * A fiber that loops a stop-the-world operation -- a tight `gc.collect()`
 * loop is the canonical case -- while it is the SOLE runnable g on its hub gets
 * re-run back-to-back by the loop below, holding the world stopped ~100% of the
 * time.  That starves fibers stranded on OTHER hubs: in default (per-hub-
 * tstate) mode a started fiber is hub-pinned (its suspended eval frame caches
 * its origin tstate via the snap dance, so it is NOT stealable) and lives in its
 * hub's local ready ring; if that hub is held SUSPENDED for the world-stop it can
 * never re-attach to drain the ring -> the stranded work never finishes -> the
 * stop-the-world looper never sees its exit condition -> whole-process deadlock.
 * (Observed as the intermittent hang in test_freethread_stress::
 * test_gc_stw_under_fiber_churn.)
 *
 * A cooperative turn should relinquish to the whole system, not just respin the
 * same g.  When this hub is about to re-run its sole runnable g AND a sibling hub
 * has unfinished work but is NOT running it, briefly DETACH this thread and sleep:
 * a detached thread counts as already-stopped, so the window lets the stalled
 * sibling start-the-world / re-attach and make progress before this hub stops the
 * world again.
 *
 * "Not running its work" = the sibling tstate is SUSPENDED (stopped for an active
 * world-stop) OR DETACHED with pending fibers (it just got released by a
 * start-the-world and is trying to re-attach, or is otherwise blocked off-CPU with
 * work owed).  The DETACHED-with-work case is essential: right after THIS hub's own
 * gc.collect() returns (its start-the-world set the siblings DETACHED), the sibling
 * has not re-attached yet, so a SUSPENDED-only test would miss the window and let
 * the monopoly continue (observed as a residual ~1/120 hang).  pending>0 keeps a
 * genuinely-idle sibling (no work) from ever arming the pause.
 *
 * Precisely targeted -- a busy multi-g hub short-circuits on the ready-count test
 * (a couple of relaxed loads); a lone yielder whose siblings are idle (pending==0)
 * or running (ATTACHED) never pays the pause.  No-op before 3.13 (no attach-state /
 * stop-the-world protocol there). */
static void runloom_mn_world_yield_if_monopolizing(runloom_hub_t *h)
{
#if PY_VERSION_HEX >= 0x030D0000
    int i;
    if (__atomic_load_n(&h->stopping, __ATOMIC_ACQUIRE)) return;
    /* Sole runnable g: exactly one in the ready ring and the own deque empty.
     * Relaxed loads (as documented above): own-thread here, but the indices are
     * relaxed-atomic for the cross-thread census -- keep every reader consistent. */
    if ((__atomic_load_n(&h->sched.ready_tail, __ATOMIC_RELAXED)
         - __atomic_load_n(&h->sched.ready_head, __ATOMIC_RELAXED)) != 1) return;
    if (runloom_cldeque_size(&h->deque) != 0) return;
    for (i = 0; i < runloom_hub_count; i++) {
        int tss;
        long pend;
        if (i == h->id) continue;
        /* Acquire load: a sibling hub publishes its tstate on its own thread
         * (runloom_hub_main); NULL during that startup window -> attach-state
         * -1 -> neither branch below fires (skip). */
        tss  = runloom_tstate_attach_state((PyThreadState *)
                   __atomic_load_n((void **)&runloom_hubs[i].tstate, __ATOMIC_ACQUIRE));
        pend = __atomic_load_n(&runloom_hubs[i].pending, __ATOMIC_RELAXED);
        if (tss == RUNLOOM_TS_SUSPENDED ||
            (tss == RUNLOOM_TS_DETACHED && pend > 0)) {
            static long long yield_ns = -1;
            long long pause_ns = __atomic_load_n(&yield_ns, __ATOMIC_RELAXED);
            PyThreadState *yts;
            if (pause_ns < 0) {
                const char *e = getenv("RUNLOOM_WORLD_YIELD_NS");
                pause_ns = (e != NULL) ? atoll(e) : 100000LL;   /* 100 us default */
                if (pause_ns < 0) pause_ns = 0;
                __atomic_store_n(&yield_ns, pause_ns, __ATOMIC_RELAXED);
            }
            if (pause_ns == 0) return;
            runloom_delay_inject(RUNLOOM_DLY_WORLD_YIELD);
            RUNLOOM_EVT(RUNLOOM_EVT_WORLD_YIELD, h, 0, pause_ns);
            yts = PyEval_SaveThread();
            runloom_sleep_ns(pause_ns);
            PyEval_RestoreThread(yts);
            return;
        }
    }
#else
    (void)h;
#endif
}

/* Hub thread main loop.  Phase C v2: runs the same snap/load dance
 * as runloom_sched_drain.  Each iteration:
 *   1. Pop a g (local FIFO of yielded gs first, then own deque of
 *      fresh gs, then steal from a neighbour's deque).
 *   2. Save hub's tstate into hub_snap (local var on hub_main's stack).
 *   3. If g has a saved snap, load it; else NULL datastack so g's
 *      first run gets its own root chunk (Phase B initial-run dance).
 *   4. Resume g.  g either runs to completion or yields.
 *   5. Restore hub_snap.  If g is still alive AND did not self-queue
 *      (raw runloom_coro_yield, no scheduler call), push it back to the
 *      local FIFO so it keeps making progress.
 *
 * Idle policy: when all hubs report pending=0 we still poll (the
 * caller may runloom_mn_fiber more work at any time).  Stop signal comes
 * from runloom_mn_fini setting h->stopping. */
static RUNLOOM_THREAD_RET runloom_hub_main(void *arg)
{
    runloom_hub_t *h = (runloom_hub_t *)arg;
    runloom_pystate_snap_t hub_snap;
    PyThreadState *hts;

    /* Create this hub's PyThreadState ON THIS THREAD.  PyThreadState_New binds
     * the tstate (biased-refcount owner id + mimalloc heap) to the calling
     * thread, so doing it here -- not on the main thread in runloom_mn_init --
     * makes brc->tid == the ob_tid every object this hub creates carries.  That
     * is required for cross-thread DECREF to find the live owner instead of
     * merging its refcount as "exited" (the gc-churn use-after-free).  Publish
     * with release; sysmon / handoff / world-yield read it with acquire and
     * tolerate the brief NULL window before this store. */
    /* Serialize across hubs: CPython's new_threadstate() races on the
     * gc.immortalize check-then-act outside its HEAD_LOCK when several threads
     * create tstates at once (see runloom_hub_tstate_lock).  The lock's
     * release/acquire also publishes the immortalize-flag write to later hubs. */
    RUNLOOM_RLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
    hts = PyThreadState_New(runloom_mn_interp);
    RUNLOOM_RUNLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
    if (hts == NULL) {
        /* Astronomically unlikely (a small allocation).  This hub cannot run
         * Python; mark it stopping so the scheduler stops counting on it and
         * exit.  Other hubs keep draining the global runq; h->tstate stays NULL
         * and the fini cleanup skips it. */
        __atomic_store_n(&h->stopping, 1, __ATOMIC_RELEASE);
        RUNLOOM_THREAD_RETURN(NULL);
    }
    __atomic_store_n((void **)&h->tstate, (void *)hts, __ATOMIC_RELEASE);
    /* trace conformance: this thread just created + gilstate-bound its tstate */
    runloom_gilstate_trace("Create", h->id, h->id);
    PyEval_RestoreThread(hts);
    /* Per-OS-thread coro-backend setup.  On Windows Fibers this calls
     * ConvertThreadToFiber so SwitchToFiber works on this thread; on
     * POSIX it's a no-op.  Must run BEFORE the first runloom_coro_resume
     * (otherwise SwitchToFiber faults with "not a fiber"). */
    runloom_coro_thread_init();
    runloom_tls_hub = h;

#if defined(__linux__)
    /* Lever #2: pin each hub thread to a fixed CPU core (RUNLOOM_HUB_AFFINITY=1,
     * opt-in, default off).  Keeps the hub's working set -- and a pinned
     * connection's data -- hot in one core's cache instead of letting the OS
     * migrate the thread.  hub id -> core (id % ncpu).  On loopback only the
     * cache-locality half is measurable; the NIC-RX-steering half needs a real
     * NIC.  Best-effort: a failed pin just leaves the thread unpinned. */
    {
        /* Atomic load+store: hub threads race to first-init this static; the
         * plain read/write was a (benign, same-value) TSan data race. */
        static int aff_mode = -1;
        int am = __atomic_load_n(&aff_mode, __ATOMIC_RELAXED);
        if (am < 0) {
            const char *e = getenv("RUNLOOM_HUB_AFFINITY");
            am = (e != NULL && *e != '0') ? 1 : 0;
            __atomic_store_n(&aff_mode, am, __ATOMIC_RELAXED);
        }
        if (am) {
            long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
            if (ncpu > 0) {
                cpu_set_t set;
                CPU_ZERO(&set);
                CPU_SET((int)((long)h->id % ncpu), &set);
                (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
            }
        }
    }
#endif

    /* Create the hub's per-thread io_uring ring.  Try
     * SINGLE_ISSUER (Linux 5.18+) + DEFER_TASKRUN (6.1+); the create
     * call downgrades gracefully on older kernels.  With
     * DEFER_TASKRUN the kernel only posts CQEs + fires the eventfd
     * when this thread next calls io_uring_enter(GETEVENTS); the
     * idle path below issues that call before pumping so the
     * deferred work flushes in time. */
    /* Skip the ring entirely unless an io_uring feature is opted in: with all
     * of them off (the default) the ring carries no ops and only costs an
     * io_uring_setup + register per hub plus a periodic GETEVENTS flush. */
    h->iouring_ring = runloom_iouring_any_enabled()
                      ? runloom_iouring_ring_create(1 /*defer_taskrun*/) : NULL;
    if (h->iouring_ring != NULL) {
        if (runloom_iouring_loop_enabled()) {
            /* io_uring-as-loop: the hub will block DIRECTLY in this ring (see
             * the idle path below).  Arm the per-hub wake eventfd + a multishot
             * poll on the shared epoll fd ON the ring, instead of registering
             * the ring's eventfd into the epoll pump -- there is no bridge in
             * loop mode.  On arm failure, discard the ring and fall back to the
             * epoll pump (loop_wake_fd stays -1 so the idle path won't loop). */
            int wfd = runloom_iouring_loop_hub_arm(h->iouring_ring,
                                                   runloom_netpoll_hub_epoll_fd());
            if (wfd >= 0) {
                h->loop_wake_fd    = wfd;
                h->iouring_eventfd = -1;
            } else {
                runloom_iouring_ring_destroy(h->iouring_ring);
                h->iouring_ring    = NULL;
                h->iouring_eventfd = -1;
            }
        } else {
            h->iouring_eventfd = runloom_iouring_ring_eventfd(h->iouring_ring);
            if (runloom_netpoll_add_iouring_ring(h->iouring_eventfd,
                                              h->iouring_ring) != 0) {
                /* Registration failed (most likely no epoll backend, but
                 * this build path is Linux-only so it'd be unusual).
                 * Discard the ring; hub-bound iouring calls fall through. */
                runloom_iouring_ring_destroy(h->iouring_ring);
                h->iouring_ring     = NULL;
                h->iouring_eventfd  = -1;
            }
        }
    } else {
        h->iouring_eventfd = -1;
    }

    /* Warm this hub's parker-pool wake infrastructure (per-hub epoll + wake
     * eventfd) eagerly at thread start, independent of io_uring.  The hub WILL
     * need it the instant a fiber parks; creating it now (a) avoids an
     * epoll_create + eventfd + epoll_ctl hiccup on the first park under load,
     * and (b) keeps fd accounting honest -- the per-hub wake fds belong to hub
     * startup, not to whichever fiber happens to park first.  Idempotent (the
     * pool's once-init CAS), and a no-op return without per-hub epoll (there all
     * parkers share the default pool, already inited by netpoll_init).  Until
     * the io_uring ring was gated off in the default config, registering the
     * ring eventfd warmed this implicitly; gating the ring moved the per-hub
     * wake fds from hub startup into the first park -- this makes the warm
     * explicit and unconditional so it no longer rides on io_uring. */
    (void)runloom_netpoll_hub_epoll_fd();

    /* hub_snap is loop-invariant for the same reason sched_snap is in
     * runloom_sched_drain: hub_main runs no Python work between
     * iterations except via runloom_g_decref's tp_dealloc, where we
     * explicitly restore + re-snap.  Hoisting the per-iter snap+load
     * out of the loop is ~10 ns/yield on the M:N hot path. */
    runloom_pystate_snap(&hub_snap);

    /* sched_yield fairness (see the pick block below).  ready_streak counts
     * consecutive ready-ring services; after starve_bound of them we force one
     * deque turn so a busy sched_yield loop can't starve never-run fibers.
     * Hub-local (this thread only).  starve_bound resolved once from env;
     * default 64, 0 disables (legacy ready-before-deque).  First touch across
     * hubs is a relaxed-atomic lazy-static loaded into the loop-invariant local
     * `sbound` (all store the same value) -- same pattern as sweep_on below.
     * (Previously the read was plain while the store was atomic: a technical
     * C11 data race TSan flags, behaviourally benign but now removed.) */
    unsigned ready_streak = 0;
    static int starve_bound = -1;
    int sbound = __atomic_load_n(&starve_bound, __ATOMIC_RELAXED);
    if (sbound < 0) {
        const char *e = getenv("RUNLOOM_READY_STARVE_BOUND");
        sbound = (e != NULL) ? atoi(e) : 64;
        if (sbound < 0) sbound = 0;
        __atomic_store_n(&starve_bound, sbound, __ATOMIC_RELAXED);
    }

    /* Controlled-replay startup rendezvous: wait until mn_run has placed all
     * initial fibers before this hub starts scheduling, so the first
     * requester set is complete (no-op unless RUNLOOM_MN_SEED + barrier). */
    runloom_mn_ctrl_wait_armed();

    /* Per-hub kqueue load-balancing: cadence counter for the periodic
     * non-blocking self-pump below (see the loop top). */
    unsigned int self_pump_ctr = 0;
    (void)self_pump_ctr;
    while (!__atomic_load_n(&h->stopping, __ATOMIC_ACQUIRE)) {
        runloom_g_t *g;
        /* Drain the io_uring single-op cancel mailbox: a foreign task.cancel
         * deposited an op targeting THIS hub's SINGLE_ISSUER ring; submit the
         * ASYNC_CANCEL here, where we are the ring's sole issuer. */
        {
            /* MSVC atomics don't support void ** in _Generic; cast to intptr_t. */
            intptr_t cop = __atomic_exchange_n((intptr_t *)&h->iouring_cancel_op, 0,
                                              __ATOMIC_ACQ_REL);
            if (cop != 0) runloom_iouring_submit_cancel_for_op((void *)cop);
        }
        /* Set when g came from the global run-queue: it then carries a queue
         * ref this iteration must release, and its resume must claim it with the
         * QUEUED->RUNNING CAS.  Gs from the local FIFO / own deque / a neighbour
         * steal are single-owner by construction (already RUNNING) and skip
         * both. */
        int from_runq = 0;
#ifdef RUNLOOM_DBG_BADCORO
        int gsrc = 0, gvic = -1;   /* source-hunt: which queue delivered g */
#endif
#if defined(RUNLOOM_HAVE_KQUEUE)
        /* PERIODIC NON-BLOCKING SELF-PUMP (per-hub kqueue load balancing).
         * Each hub polls its OWN kqueue; the blocking pump below only runs when
         * this hub goes idle.  Under load a hub stays busy running ready gs and
         * never idles, so its OWN parkers' readiness (events sitting in its
         * kqueue) would starve -- idle hubs can't help, the kqueue is private.
         * Drain it with a non-blocking kevent(timeout=0) every N loop turns so a
         * busy hub keeps its parkers flowing; woken gs land in sub_head/ready and
         * are picked up immediately below.  This is the netpoll cadence Go's
         * scheduler runs in findrunnable.  Cheap (one kevent per N turns) and
         * compiled only on the per-hub-kqueue backend. */
        if ((++self_pump_ctr & 0x3fu) == 0) (void)runloom_netpoll_pump(0);
#endif
        /* Drain the submission list into the deque first.  Pushing to
         * the deque is owner-only, so we (the hub) move fresh gs from
         * external producers onto our deque before anyone else looks. */
        RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
        {
            runloom_g_t *sub = h->sub_head;
#ifdef RUNLOOM_DBG_BADCORO
            runloom_g_t *prev = NULL;   /* source-hunt: g whose ->next we follow */
            int last_wsite = -1; void *last_wval = NULL; unsigned long long last_wseq = 0;
#endif
            h->sub_tail = NULL;
            /* RELAXED-atomic clear: closes the lock-free census's ACQUIRE read race
             * on sub_head (Finding D #4 -- the TSan hub_main:273 report). */
            __atomic_store_n(&h->sub_head, (runloom_g_t *)NULL, __ATOMIC_RELAXED);
            RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
            while (sub != NULL) {
                runloom_g_t *next;
#ifdef RUNLOOM_DBG_BADCORO
                /* Validate `sub` BEFORE pushing.  If it is not a real g, the
                 * PREVIOUS g's ->next was corrupted between hub_submit and here:
                 * dump that g's full identity + state, then stop (rest is junk). */
                {
                    runloom_coro_t *sc = sub->coro;
                    if (sub->owner == NULL ||
                        (sub->callable == NULL && sub->c_entry == NULL) ||
                        sc == NULL || ((uintptr_t)sc >> 47) != 0 ||
                        ((uintptr_t)sc & 0xf) != 0) {
                        /* last_w* describe how prev->next (== this bad sub) was
                         * written: site (1=submit 2=drain 3=slabfree 5=staging),
                         * the value written, and its seq.  If last_wval == sub the
                         * bad value came straight from that site (a non-g was
                         * linked); if last_wval != sub, prev->next was overwritten
                         * AFTER the recorded write -> a wild write (the recorded
                         * site/seq bounds when the last legit write happened). */
                        char dbuf[460];
                        int dm = snprintf(dbuf, sizeof dbuf,
                            "[DRAINBAD] sub=%p submagic=%x bad <- prev=%p pmagic=%x id=%lld owner=%p rc=%d snapv=%d || prevNEXT wsite=%d wval=%p (wval==sub? %d)\n",
                            (void *)sub, sub->magic,
                            (void *)prev, prev ? prev->magic : 0,
                            prev ? (long long)prev->id : -1,
                            prev ? (void *)prev->owner : NULL,
                            prev ? (int)__atomic_load_n(&prev->refcount, __ATOMIC_ACQUIRE) : -1,
                            prev ? prev->snap.valid : -1,
                            last_wsite, last_wval,
                            (last_wval == (void *)sub) ? 1 : 0);
                        if (dm > 0) (void)write(2, dbuf, (size_t)dm);
                        break;
                    }
                }
#endif
                next = sub->next;
#ifdef RUNLOOM_DBG_BADCORO
                /* Capture how THIS sub's ->next was written, BEFORE we clear it,
                 * so the next iteration (if `next` is bad) can attribute it. */
                last_wsite = sub->next_wsite;
                last_wval  = sub->next_wval;
                last_wseq  = sub->next_wseq;
#endif
                RUNLOOM_NEXT_SET(sub, NULL, 2);   /* drain clear */
                /* Route by state: fresh (no snap) -> Chase-Lev deque
                 * (stealable by other hubs); woken (snap.valid) ->
                 * local FIFO (hub-pinned, the netpoll-wake path).
                 *
                 * Note: in_sub_queue stays 1 throughout drain ->
                 * ready/deque -> pop -> resume.  hub_main clears it
                 * just before the actual coro resume, gating duplicate
                 * wake_gs against ANY queued state, not just the
                 * sub list. */
                if (sub->snap.valid) {
                    runloom_sched_ready_push(&h->sched, sub);
                } else if (runloom_cldeque_push(&h->deque, sub) != 0) {
                    /* Deque full (>RUNLOOM_CLDEQUE_CAP=4096 fresh gs on this
                     * hub -- e.g. a big spawn burst at low hub count, the
                     * H=2/N>=8K case).  Fall back to the local ready FIFO,
                     * which grows on demand.  These gs run hub-pinned
                     * rather than stealable, but are NEVER dropped: the
                     * previous code ignored the push return and silently
                     * dropped the g, orphaning one whose pending-inc was
                     * already counted -> mn_run hung forever.  The resume
                     * prologue handles a snap-invalid g from either queue
                     * via the first-run install path. */
                    runloom_sched_ready_push(&h->sched, sub);
                }
#ifdef RUNLOOM_DBG_BADCORO
                prev = sub;
#endif
                sub = next;
            }
        }

        /* Wake any sleepers whose timers have expired and move them
         * onto the local FIFO so the pop below picks them up.  In controlled-
         * replay mode the clock is logical (advanced only at a quiescent census,
         * see runloom_mn_ctrl_try_grant), so timer firing is reproducible; otherwise
         * it is the wall clock, exactly as before. */
        if (h->sched.sleep_size > 0) {
            double now = runloom_mn_logical_now_or(runloom_sched_monotonic_seconds());
            while (h->sched.sleep_size > 0 &&
                   runloom_sched_sleep_peek(&h->sched)->wake_at <= now) {
                runloom_g_t *woke = runloom_sched_sleep_pop(&h->sched);
                if (runloom_use_global_runq()) {
                    /* The timer is a wake, and under a global-runq mode the
                     * sleep release left this g in wake_state PARKED.  Claim it
                     * back to RUNNING through the state machine (it has no other
                     * waker, so the CAS normally wins) and resume it hub-pinned
                     * on the local FIFO.  If the CAS loses, a concurrent waker
                     * (a g sleeping AND wakeable, e.g. a select-with-timeout)
                     * already drove PARKED->QUEUED and enqueued it on the global
                     * runq -- do NOT also push it here, or it would be scheduled
                     * twice. */
                    int pexp = RUNLOOM_WS_PARKED;
                    if (__atomic_compare_exchange_n(&woke->wake_state, &pexp,
                                                    RUNLOOM_WS_RUNNING, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        /* TIMER_CLAIM won: PARKED -> RUNNING (the sole edge into
                         * RUNNING that does not pass through QUEUED). */
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_PARKED, RUNLOOM_WS_RUNNING);
                        runloom_sched_ready_push(&h->sched, woke);
                    }
                } else {
                    runloom_sched_ready_push(&h->sched, woke);
                }
            }
        }
        /* Fire any due in-memory TIMED parks on this hub (runloom_c.park(timeout)).
         * REAL (monotonic) deadlines -- NOT the logical clock used for sleepers
         * above; each is claimed via the parked_safe CAS (exactly-once vs a real
         * wake_safe), routed back to its hub by runloom_sched_drain_timers. */
        if (h->sched.timer_size > 0) {
            runloom_sched_drain_timers(&h->sched, runloom_sched_monotonic_seconds());
        }

        /* Pick the next g.  Normal order: ready ring (yielded/woken -- "hot")
         * before the deque (fresh, never-run gs), which keeps woken I/O
         * fibers low-latency.  BUT a fiber busy-looping on sched_yield
         * re-enters the ready ring every time, so pure ready-before-deque lets
         * it starve every fresh g forever (they never get a first run -- the
         * sched_yield-fairness bug).  Bound it: after starve_bound consecutive
         * ready-ring services, force ONE deque turn.  ready_streak resets the
         * moment the ready ring drains naturally, so workloads that aren't
         * monopolizing never trip it; a sustained yielder pays only a 1-in-
         * starve_bound interleave.  All hub-local + single-consumer; the deque
         * is touched via its normal owner-pop, never re-ordered internally. */
        g = NULL;
        if (sbound && ready_streak >= (unsigned)sbound) {
            ready_streak = 0;
            g = (runloom_g_t *)runloom_cldeque_pop(&h->deque);  /* forced fresh turn */
#ifdef RUNLOOM_DBG_BADCORO
            if (g) gsrc = 1;   /* sbound forced own-deque pop */
#endif
        }
        if (g == NULL) {
            g = runloom_sched_ready_pop(&h->sched);     /* local yielded/woken */
            if (g != NULL) {
                ready_streak++;
#ifdef RUNLOOM_DBG_BADCORO
                gsrc = 2;      /* local ready FIFO */
#endif
            } else {
                g = (runloom_g_t *)runloom_cldeque_pop(&h->deque);  /* own fresh */
                ready_streak = 0;
#ifdef RUNLOOM_DBG_BADCORO
                if (g) gsrc = 3;   /* own-deque pop */
#endif
            }
        }
        if (g == NULL) {
            /* Global run-queue: woken migratable gs (per-g-tstate) that any
             * idle hub may run, so a hub stuck in a blocking C call can't
             * strand them.  Checked before neighbour-steal below so a
             * stalled hub's woken work is recovered promptly.  from_runq tells
             * the resume block to claim the g (QUEUED->RUNNING) and to drop the
             * entry's queue ref when the resume ends. */
            g = runloom_mn_global_runq_pull();
            if (g != NULL) {
                from_runq = 1;
#ifdef RUNLOOM_DBG_BADCORO
                gsrc = 4;      /* global run-queue */
#endif
            }
        }
        if (g == NULL) {
          if (!runloom_mn_ctrl_no_steal() && runloom_hub_count >= 2) {
            /* Randomized coprime-stride victim order (see runloom_steal_stride):
             * each hub starts at its own id and steps by a coprime stride, so K
             * idle hubs don't all hammer hub 0's steal CAS in lockstep. */
            int n = runloom_hub_count;
            int stride = runloom_steal_stride(n, h->id);
            int pos = h->id;
            int k;
            for (k = 0; k < n; k++, pos += stride) {
                int i = pos % n;
                if (i == h->id) continue;
                g = (runloom_g_t *)runloom_cldeque_steal(&runloom_hubs[i].deque);
                if (g != NULL) {
#ifdef RUNLOOM_DBG_BADCORO
                    gsrc = 5; gvic = i;   /* stole from hub i's deque */
#endif
                    /* ACQ_REL pair: mn_run's polling loop reads
                     * hubs[*].pending with ACQUIRE; under
                     * free-threaded Python the RELAXED pair we used
                     * to have let a transient "victim already
                     * decremented, stealer not yet incremented"
                     * state become observable -- total could appear
                     * 0 momentarily even with work in flight, which
                     * is harmless on its own but ALSO meant the
                     * stealer's later `h->pending` increment lacked
                     * a release pairing with reads in other hubs
                     * that might inspect this hub's queue. */
                    runloom_mn_pending_steal(&runloom_hubs[i], h);
                    break;
                }
            }
          }
            if (g == NULL) {
                long total = 0;
                int j;
                PyThreadState *saved;
                int parked;
                long long idle_ns;
                /* No fiber to run: check in as IDLE for the controlled-
                 * scheduler census round so the barrier-rendezvous grant isn't
                 * blocked waiting on a hub that has nothing to request.  No-op
                 * unless RUNLOOM_MN_SEED + barrier are on.  Returns 1 if a wake
                 * landed on our sub_list since this iteration's drain -> re-loop
                 * and drain it rather than falsely checking in idle (the guard
                 * that closes the sub-segment replay race). */
                if (runloom_mn_ctrl_census_idle(h)) continue;
                for (j = 0; j < runloom_hub_count; j++) {
                    total += __atomic_load_n(&runloom_hubs[j].pending,
                                             __ATOMIC_RELAXED);
                }
                /* Default idle wait (no work anywhere -> longer; some
                 * work elsewhere -> shorter so we re-poll for steal). */
                idle_ns = (total == 0) ? 500000LL : 100000LL;
                /* Cap by the next-due sleeper on THIS hub so we don't
                 * oversleep past a local timer.  (Other hubs' timers
                 * are handled by those hubs.)  Skipped in controlled-replay
                 * mode: there wake_at is LOGICAL (mismatched with wall-clock
                 * `now`, which would force a busy-spin), and the logical timer
                 * fires at loop top under the census-driven clock advance, so
                 * the hub need only re-poll at the default cadence. */
                if (h->sched.sleep_size > 0 && !runloom_mn_ctrl_no_steal()) {
                    double now = runloom_sched_monotonic_seconds();
                    double gap = runloom_sched_sleep_peek(&h->sched)->wake_at - now;
                    long long gap_ns = (long long)(gap * 1e9);
                    if (gap_ns < 0) gap_ns = 0;
                    if (gap_ns < idle_ns) idle_ns = gap_ns;
                }
                /* Likewise cap by the nearest in-memory TIMED-park deadline on
                 * this hub so we wake to fire it (REAL/monotonic, always). */
                if (h->sched.timer_size > 0) {
                    double now = runloom_sched_monotonic_seconds();
                    double gap = runloom_sched_timer_next_deadline(&h->sched) - now;
                    long long gap_ns = (long long)(gap * 1e9);
                    if (gap_ns < 0) gap_ns = 0;
                    if (gap_ns < idle_ns) idle_ns = gap_ns;
                }
                parked = runloom_netpoll_parked_count();
                /* Hub-idle dwell-based stack reclaim (opt-in,
                 * RUNLOOM_STACK_PARK_SWEEP=1; threshold ms via
                 * RUNLOOM_STACK_PARK_SWEEP_MS, default 5).  This hub is idle
                 * and is the sole resumer of its own parkers, so madvising
                 * their long-idle stacks here is race-free (see
                 * runloom_netpoll_sweep_idle).  Rate-limited to ~half the
                 * threshold so the O(parked) walk stays cheap. */
                {
                    static int sweep_on = -1;
                    /* 100 ms default: high enough that active round-trip
                     * parks (ms-scale, even with some queuing) are left
                     * alone, low enough to reclaim genuinely idle
                     * keepalive parks (seconds).  A saturated all-active
                     * bench inflates round-trip dwell, so it still pays
                     * some cost there; the N=1M target (5% active) does
                     * not.  Tunable via RUNLOOM_STACK_PARK_SWEEP_MS. */
                    static long long sweep_thresh_ns = 100000000LL;
                    int on = __atomic_load_n(&sweep_on, __ATOMIC_RELAXED);
                    if (on < 0) {
                        const char *e = getenv("RUNLOOM_STACK_PARK_SWEEP");
                        const char *ms = getenv("RUNLOOM_STACK_PARK_SWEEP_MS");
                        if (ms != NULL) {
                            long long v = atoll(ms);
                            if (v > 0) sweep_thresh_ns = v * 1000000LL;
                        }
                        /* Default ON (2026-05-29): the dwell sweep delivers
                         * -32% idle RSS at no robustly-measurable p99 cost
                         * (the churn cost proved within run-to-run noise),
                         * and the churn throttle (RUNLOOM_SWEEP_MAX_CHURN, see
                         * netpoll.c) degrades it to a no-op on active-churn
                         * workloads as insurance.  RUNLOOM_STACK_PARK_SWEEP=0
                         * disables; =1 (or any non-"0") forces on. */
                        on = (e != NULL && *e == '0') ? 0 : 1;
                        __atomic_store_n(&sweep_on, on, __ATOMIC_RELAXED);
                    }
                    /* Runs in BOTH modes now: under per-g-tstate the sweep's
                     * sole-resumer safety is supplied by the per-g claim
                     * handshake (PARKED->SWEEPING) inside runloom_netpoll_sweep_idle,
                     * so a stolen wake can't resume a g into a stack mid-madvise. */
                    if (on && parked > 0) {
                        double now_s = runloom_sched_monotonic_seconds();
                        double interval_s = (double)sweep_thresh_ns / 2e9;
                        if (now_s - h->last_sweep_s >= interval_s) {
                            runloom_netpoll_sweep_idle(
                                runloom_mn_current_hub_opaque(), sweep_thresh_ns);
                            h->last_sweep_s = now_s;
                        }
                    }
                }
                /* io_uring-as-loop idle path (RUNLOOM_IOURING_LOOP=1).  The hub
                 * blocks DIRECTLY in its ring -- which multiplexes ALL wake
                 * sources: io_uring op completions (Stage-2 proactor), the
                 * shared epoll fd (poll-added -> F_EPOLL -> pump(0) drains the
                 * ready fds into their parkers), the per-hub wake eventfd
                 * (cross-hub submit -> F_WAKE; the sub list is re-drained at the
                 * loop top regardless), and the idle-tick timeout (idle_ns,
                 * which already accounts for the next local sleeper/timer).
                 * ring_waiting is the lock-free hint hub_submit reads to write
                 * the wake eventfd; the announce + sub_head re-check below close
                 * the announce/submit race, and the TIMED wait makes a (rare)
                 * lost wake degrade to one idle_ns of latency, never a hang --
                 * the same guarantee the idle-condvar path gives.  loop_wait's
                 * io_uring_enter(GETEVENTS) also flushes this ring's
                 * DEFER_TASKRUN task work, so completions materialise here (no
                 * separate ring_get_events needed).  GIL released across the
                 * blocking enter, then re-held for pump(0). */
                if (runloom_iouring_loop_enabled() && h->iouring_ring != NULL) {
                    int lflags = 0;
                    int have_sub;
                    __atomic_store_n(&h->ring_waiting, 1, __ATOMIC_RELEASE);
                    /* SEQ_CST so the ring_waiting store is ordered before the
                     * sub_head load against hub_submit's push-then-read-
                     * ring_waiting (a Dekker handshake); without the fence the
                     * store/load could reorder and lose the wake for one tick
                     * (the timeout still bounds it, but the fence keeps the
                     * common path latency-free). */
                    __atomic_thread_fence(__ATOMIC_SEQ_CST);
                    RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
                    have_sub = (h->sub_head != NULL);
                    RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
                    if (!have_sub) {
                        if (idle_ns <= 0) idle_ns = 1;
                        saved = PyEval_SaveThread();
                        runloom_iouring_loop_wait(h->iouring_ring, idle_ns,
                                                  &lflags);
                        PyEval_RestoreThread(saved);
                    }
                    __atomic_store_n(&h->ring_waiting, 0, __ATOMIC_RELEASE);
                    if (!have_sub) {
                        /* Drain the per-hub epoll on EVERY idle iteration, not only
                         * when the ring's F_EPOLL poll fired (lflags & F_EPOLL).  The
                         * poll-on-the-epoll-fd bridge can MISS a socket-readable edge --
                         * the SAME unreliability the global-ring drain below works
                         * around for the EPOLLEXCLUSIVE eventfd.  A missed F_EPOLL then
                         * leaves a ready socket undrained and its parked reader hung
                         * forever while every hub just keeps timing out its idle_ns ring
                         * wait (the cov_workload --hubs 4 loop-backend hang: gdb showed
                         * all hubs cycling in loop_wait, the reader never woken, and the
                         * deadlock census blind because the I/O-parked fibers keep
                         * has_wakeable_work() true).  pump(0) is a non-blocking
                         * epoll_wait(0): on the common tick where nothing is ready it
                         * returns at once.  The idle_ns timeout thus bounds netpoll-wake
                         * latency to one tick instead of risking a lost wake -- the
                         * Layer-0 "re-check the source of truth on timeout" guarantee. */
                        runloom_netpoll_pump(0);
                    }
                    (void)lflags;
                    /* Reap GLOBAL-ring completions.  Cooperative file I/O
                     * (runloom_iouring_pread/pwrite via runloom_iouring_do) lives on
                     * the process-global ring, NOT this hub ring.  Its eventfd
                     * is in the shared epoll with EPOLLEXCLUSIVE, whose readiness
                     * does NOT reliably propagate to a poll-on-the-epoll-fd (the
                     * UD_EPOLL bridge) -- so F_EPOLL can miss it and a fiber
                     * parked in file_read would hang.  Drain the global ring
                     * directly here: a cheap no-op when it is uninitialised (the
                     * pure-network serve path never touches it) or its CQ is
                     * empty; otherwise it reaps the CQEs and wakes the reader via
                     * the op->wait handshake.  The idle_ns timeout bounds the
                     * latency on the (rare) ticks where UD_EPOLL didn't fire. */
                    runloom_iouring_drain();
                    (void)saved;
                    continue;
                }
                {
                    /* Also drive the pump when ANY iouring ring (this
                     * hub's, the global, or another hub's) has inflight
                     * ops: the shared netpoll's epoll set holds every
                     * registered iouring eventfd, so any hub's pump call
                     * drains everyone's CQEs.  Without this, a hub that
                     * parks all its gs on iouring would idle-sleep
                     * while completions sit unread.
                     *
                     * GIL handling: pump internally does
                     * Py_BEGIN_ALLOW_THREADS for the epoll_wait syscall,
                     * so it must be called with the GIL HELD (matching
                     * how single-thread runloom_sched_drain calls it).
                     * Wrapping pump in our own PyEval_SaveThread would
                     * drop the GIL twice and crash with
                     * "must be called with GIL held".  runloom_sleep_ns is
                     * a plain nanosleep that doesn't need the GIL, so
                     * release the GIL around THAT alone. */
                    int iouring_total = runloom_iouring_inflight();
                    if (parked > 0 || iouring_total > 0) {
                        long long pump_ns = 1000000LL;
                        if ((h->sched.sleep_size > 0 || h->sched.timer_size > 0) &&
                            idle_ns < pump_ns) {
                            pump_ns = idle_ns;      /* idle_ns already timer-bounded */
                        }
                        /* DEFER_TASKRUN heartbeat: if this hub's ring
                         * was created with that flag, completions on
                         * its own SQEs are queued in the kernel but
                         * NOT posted to the CQ (or the eventfd) until
                         * THIS thread next calls
                         * io_uring_enter(GETEVENTS).  Trigger that
                         * flush now so the upcoming epoll_wait sees a
                         * fresh eventfd hit if there's work pending.
                         * No-op for non-DEFER rings.  We only do this
                         * on OUR own ring -- under SINGLE_ISSUER each
                         * ring's GETEVENTS must come from its owner. */
                        runloom_iouring_ring_get_events(h->iouring_ring);
                        /* Ensure the shared pump-wake eventfd is registered in
                         * the epoll set BEFORE we block here, so a cross-hub
                         * runloom_mn_hub_submit/wake_g can break this epoll_wait
                         * immediately (else the woken g waits the full pump
                         * timeout -- the ~1ms cross-hub handoff latency).
                         * Idempotent + atomic-guarded: a no-op once armed. */
                        runloom_netpoll_wake_pump_arm();
                        /* GLOBAL-ring CQ-OVERFLOW safety.  TCPConn io_uring recv
                         * (multishot) and file I/O live on the process-global
                         * ring, not this hub ring.  When that ring's CQ fills
                         * under backpressure, excess completions go to the
                         * kernel's overflow backlog and do NOT re-signal the
                         * global eventfd -- so the pump below (even bounded to
                         * pump_ns) would never observe the edge that drains them,
                         * and the recv fiber whose completion is stranded in
                         * overflow hangs forever (the only thing that frees CQ
                         * space is that same fiber).  ring_get_events above only
                         * flushes THIS hub's ring; drain the global ring here so
                         * the overflow backlog is flushed + its CQEs processed +
                         * the waiter woken (cross-hub via mn_wake_g).  Cheap no-op
                         * when the global ring is uninitialised or its CQ empty. */
                        runloom_iouring_drain();
                        runloom_netpoll_pump(pump_ns);
                    } else {
                        if (idle_ns <= 0) idle_ns = 1;
                        /* BUG #10 throughput: this hub has no netpoll/iouring work
                         * but OWNS parked gs (pending>0) -- cooperative-lock /
                         * channel waiters a cross-hub unlock will hub_submit to
                         * us.  Wait on our per-hub idle condvar (TIMED at idle_ns)
                         * instead of a plain nanosleep, so that submit signals us
                         * awake in ~us rather than stranding the woken g for
                         * idle_ns (the ~100us cap that held the contended
                         * cooperative Lock at ~10K ops/s).  Per-hub condvar =>
                         * targeted wake, no shared-pump thundering herd; TIMED =>
                         * a missed signal degrades to the old idle_ns latency for
                         * one hand-off, never a hang. */
                        long hub_pending = __atomic_load_n(&h->pending,
                                                           __ATOMIC_RELAXED);
                        if (hub_pending > 0 && runloom_hub_idle_wake_enabled()) {
                            int waited = 0;
                            RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
                            /* Announce BEFORE the final sub_head re-check so a
                             * submit that races our announce either lands in
                             * sub_head (we see it -> skip the wait) or observes
                             * idle_waiting=1 (it signals us).  The push in
                             * hub_submit happens-before its sub_gen bump + signal;
                             * we hold idle_lock across the check+wait so a signal
                             * can't slip into the gap. */
                            __atomic_store_n(&h->idle_waiting, 1, __ATOMIC_RELEASE);
                            int have_sub;
                            RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
                            have_sub = (h->sub_head != NULL);
                            RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
                            if (!have_sub) {
                                /* Detach BEFORE the wait so a concurrent
                                 * stop-the-world sees this hub already stopped and
                                 * the cond wait holds no tstate. */
                                saved = PyEval_SaveThread();
                                runloom_cond_timedwait_ns(&h->idle_cond,
                                                          &h->idle_lock, idle_ns);
                                waited = 1;
                            }
                            __atomic_store_n(&h->idle_waiting, 0, __ATOMIC_RELEASE);
                            RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
                            /* Re-attach OUTSIDE idle_lock.  PyEval_RestoreThread
                             * blocks until any in-progress stop-the-world ends;
                             * holding idle_lock across that block would stall a
                             * hub_submit trying to signal us and could wedge STW
                             * progress (test_gc_stw_under_fiber_churn hung
                             * ~2/3 with the re-attach inside the lock). */
                            if (waited) PyEval_RestoreThread(saved);
                        } else {
                            saved = PyEval_SaveThread();
                            runloom_sleep_ns(idle_ns);
                            PyEval_RestoreThread(saved);
                        }
                    }
                }
                (void)saved;
                continue;
            }
        }

        /* Phase B snap dance: load g's tstate slice; resume; if g
         * completed, restore hub's tstate before any Python that
         * might allocate frames.  hub_snap is hoisted out of the loop
         * (see entry comment). */
        {
            int self_queued;

            if (runloom_get_per_g_tstate_mode()) {
                /* ---- per-g-tstate path (RUNLOOM_PER_G_TSTATE) ----
                 * Swap the hub's tstate out, the g's own tstate in, resume,
                 * swap back.  The g's suspended eval frames reference
                 * g->tstate (not this hub's), so resuming on ANY hub is
                 * consistent -- which is exactly what makes a stolen woken g
                 * safe here.  No snap dance (snap no-ops under this mode). */
                PyThreadState *hub_ts;
                int parked_offqueue;
                /* CLAIM the g for this hub.  A from_runq g arrived via an entry
                 * that holds a queue ref and put it in wake_state QUEUED; CAS
                 * QUEUED->RUNNING to take ownership.  This CAS CANNOT lose: the
                 * entry we pulled is the queue's sole reference to this g
                 * (at-most-one-entry) and only its holder runs this CAS -- so
                 * there is no contention and no re-push (the old design's
                 * livelock).  Gs from the deque / local FIFO / a neighbour steal
                 * are already RUNNING by the state-machine invariant (fresh gs
                 * spawn RUNNING; a yield leaves the g RUNNING), so they need no
                 * claim. */
                if (from_runq) {
                    int qexp = RUNLOOM_WS_QUEUED;
                    if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                     RUNLOOM_WS_RUNNING, 0,
                                                     __ATOMIC_ACQ_REL,
                                                     __ATOMIC_ACQUIRE)) {
                        /* Not QUEUED: a queued g is parked-waiting and never
                         * done, so this should not happen.  Stay leak-safe by
                         * dropping the queue ref and skipping. */
                        runloom_g_decref(g);   /* queue ref */
                        continue;
                    }
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_QUEUED, RUNLOOM_WS_RUNNING);
                }
                __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
                if (g->coro == NULL || g->tstate == NULL ||
                    __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                    /* Dead under our claim (shouldn't happen: a QUEUED g is
                     * parked, not done; a from_runq=0 g is freshly owned).  Stay
                     * leak-safe: a claimed runq g goes back to PARKED and drops
                     * its queue ref. */
                    if (from_runq) {
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_PARKED);
                        __atomic_store_n(&g->wake_state, RUNLOOM_WS_PARKED,
                                         __ATOMIC_RELEASE);
                        runloom_g_decref(g);   /* queue ref */
                    }
                    continue;
                }
                hub_ts = PyEval_SaveThread();        /* detach hub tstate */
                PyEval_RestoreThread(g->tstate);      /* attach g's own tstate */
                /* Deep-surface migration oracle (RUNLOOM_DBG_MIGRATE; zero-cost
                 * otherwise).  The per-g tstate's mimalloc heap / brc / QSBR are
                 * bound to the OS thread that first used them; running it on a
                 * DIFFERENT hub thread without the abandon/adopt re-bind handshake
                 * is the precise, early signature of the deferred _mi_page_retire
                 * corruption that RunloomTstateMigration.tla proves the handshake
                 * prevents.  Warn ONCE (don't abort -- this is a gated experimental
                 * mode) with both thread ids so the missing re-bind is pinpointed
                 * instead of surfacing as a random teardown SEGV.  A correct
                 * handshake would update tstate_owner_tid on adopt, keeping it quiet. */
                {
                    static int dbg = -1;
                    if (dbg < 0) {
                        const char *e = getenv("RUNLOOM_DBG_MIGRATE");
                        dbg = (e != NULL && e[0] != '0' && e[0] != '\0') ? 1 : 0;
                    }
                    if (dbg) {
                        unsigned long tid = (unsigned long)PyThread_get_thread_ident();
                        if (g->tstate_owner_tid == 0) {
                            g->tstate_owner_tid = tid;        /* first use: binds here */
                        } else if (g->tstate_owner_tid != tid) {
                            static int warned = 0;
                            if (!warned) {
                                warned = 1;
                                fprintf(stderr,
                                    "[RUNLOOM_DBG_MIGRATE] per-g PyThreadState used on "
                                    "thread %lu but bound to %lu WITHOUT a mimalloc "
                                    "abandon/adopt handshake -> _mi_page_retire "
                                    "corruption is imminent (g id=%lld). See "
                                    "docs/dev/STEAL_WOKEN_CLEANUP.md.\n",
                                    tid, g->tstate_owner_tid, (long long)g->id);
                                fflush(stderr);
                            }
                            g->tstate_owner_tid = tid;        /* track the move */
                        }
                    }
                }
                h->sched.current = g;
                runloom_tls_current_g = g;
                runloom_tls_self_queued = 0;
                runloom_tls_parked_offqueue = 0;
                runloom_g_state_set(g, RUNLOOM_GST_RUNNING);   /* observational: now on-CPU */
                runloom_hub_resume_begin(h, g);
                runloom_coro_resume(g->coro);
                runloom_hub_resume_end(h);
                self_queued = runloom_tls_self_queued;
                parked_offqueue = runloom_tls_parked_offqueue;
                runloom_tls_self_queued = 0;
                runloom_tls_parked_offqueue = 0;
                runloom_tls_current_g = NULL;
                h->sched.current = NULL;
                PyEval_SaveThread();                  /* detach g's tstate */
                PyEval_RestoreThread(hub_ts);          /* reattach hub tstate */
                if (runloom_coro_done(g->coro)) {
                    /* Done: the g did not park this resume (a resume ends in a
                     * park OR completion, never both), so no parker is live and
                     * wake_state is still RUNNING with no other entry.  Force-
                     * unlink any stale parker, then drop both refs -- the slab
                     * re-zeroes wake_state when it reallocs the g. */
                    runloom_advice_record_g(g);   /* fold stack HWM (profiling) */
                    runloom_g_state_set(g, RUNLOOM_GST_DONE);
                    runloom_netpoll_force_unlink_g_parker(g);
                    runloom_g_decref(g);                  /* scheduler ref */
                    if (from_runq) runloom_g_decref(g);   /* queue ref */
                    /* pending_complete LAST: the final decref may run a fiber_n
                     * batch teardown (arena cursor reset).  mn_run returns when
                     * global pending hits 0, so only mark complete AFTER teardown
                     * -- else the next fiber_n's arena alloc races a not-yet-reset
                     * cursor and falls back to the slow per-g path. */
                    runloom_mn_pending_complete(h);
                } else if (parked_offqueue) {
                    /* Parked off-queue (netpoll/chan/sleep): a wake_g will route
                     * the g back here.  End ownership now -- AFTER the tstate
                     * detach above, which is what closes the commit->detach
                     * window: while RUNNING, a wake only set RUNNING_WOKEN and
                     * never enqueued, so no other hub could attach this live
                     * tstate.  CAS RUNNING->PARKED so a later wake enqueues it.
                     * If a wake landed during the window (RUNNING_WOKEN), enqueue
                     * exactly once now instead of parking -- the wake is not
                     * lost and stays a single entry. */
                    int rexp = RUNLOOM_WS_RUNNING;
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    RUNLOOM_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_PARKED);
                        if (from_runq) runloom_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN.  Re-arm a single entry: incref
                         * the new one, drop the old (net zero for from_runq;
                         * first entry for from_runq=0). */
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING_WOKEN, RUNLOOM_WS_QUEUED);
                        __atomic_store_n(&g->wake_state, RUNLOOM_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        runloom_g_incref(g);                  /* new queue ref */
                        runloom_mn_global_runq_push(g);
                        if (from_runq) runloom_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield_current (already on the local FIFO) or a
                     * raw runloom_coro_yield: the g stays runnable and hub-pinned,
                     * still owned by this hub, so it ends at RUNNING.  A store is
                     * safe -- a yielding g holds no live parker, so no wake_g can
                     * fire to race it.  yield_current already pushed itself to
                     * the FIFO; a raw yield (self_queued==0) did not, so push it
                     * here. */
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_RUNNING);
                    __atomic_store_n(&g->wake_state, RUNLOOM_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) runloom_g_decref(g);       /* old queue ref */
                    if (!self_queued) runloom_sched_ready_push(&h->sched, g);
                }
                continue;
            }

            /* Steal-woken (or any non-per-g global-runq mode): a from_runq g
             * arrived via a queue entry holding a queue ref, in wake_state
             * QUEUED.  Claim it QUEUED->RUNNING (sole entry holder -> cannot
             * lose).  Fresh / deque / local-FIFO / stolen gs are already
             * RUNNING.  from_runq is only ever set under runloom_use_global_runq(),
             * so this block is inert in the byte-unchanged default. */
            int parked_offqueue = 0;
            if (from_runq) {
                int qexp = RUNLOOM_WS_QUEUED;
                if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                 RUNLOOM_WS_RUNNING, 0,
                                                 __ATOMIC_ACQ_REL,
                                                 __ATOMIC_ACQUIRE)) {
                    /* Not QUEUED: a queued g is parked-waiting, never done, so
                     * this shouldn't happen.  Stay leak-safe: drop the queue ref
                     * and skip. */
                    runloom_g_decref(g);   /* queue ref */
                    continue;
                }
                RUNLOOM_WS_NOTE(RUNLOOM_WS_QUEUED, RUNLOOM_WS_RUNNING);
            }

            if (g->snap.valid) {
                runloom_pystate_load(&g->snap);
            } else {
                runloom_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030D0000
                {
                    PyThreadState *ts = PyThreadState_GET();
                    ts->current_frame = NULL;
                }
#endif
            }

            h->sched.current = g;
            runloom_tls_current_g = g;
            runloom_tls_self_queued = 0;
            /* Clear queued flag right before resume so a wake_g that
             * fires during this g's execution (e.g., a netpoll pump
             * processing an event for the parker g is about to link
             * inside wait_fd) can enqueue g via submit.  The 1->0 exchange
             * also tells us whether THIS entry held the default-path queue
             * ref (taken in hub_submit) so we can drop it exactly once when
             * the entry is fully consumed below. */
            int had_qref = (__atomic_exchange_n(&g->in_sub_queue, 0,
                                                __ATOMIC_ACQ_REL) == 1);
            /* Defensive: pop a stale queue entry?  Two failure modes
             * for the queue under M:N + free-threaded:
             *
             *   coro == NULL: g was already decref'd to 0; runloom_g_decref
             *      already ran the pending_global decrement; just skip.
             *
             *   done == 1, coro != NULL: g_entry set done before the
             *      asm trampoline returned to its original caller hub,
             *      but THIS hub popped a duplicate queue entry for
             *      the same g (stale wake_g raced with completion).
             *      The original hub still owes the decrement -- it
             *      will run completion when its coro_resume returns.
             *      We must NOT decrement here (would double-count) and
             *      must NOT re-resume (would re-run the asm trampoline
             *      against a coro whose user frames already unwound).
             *      Skip without decrementing.
             *
             * In both cases skipping does not lose work: the original
             * processing path owns the decrement. */
            RUNLOOM_G_ASSERT_NOT(g, RUNLOOM_GST_BIT(RUNLOOM_GST_FREED));
            RUNLOOM_EVT(RUNLOOM_EVT_G_POP, g, h, 0);
            if (g->coro == NULL ||
                __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                h->sched.current = NULL;
                runloom_tls_current_g = NULL;
                if (from_runq) runloom_g_decref(g);   /* global-runq queue ref */
                if (had_qref) runloom_g_decref(g);    /* default queue ref */
                continue;
            }
            {
            /* Resume the value we validate (one load of g->coro, never a reload
             * at the call site).  A validly-spawned, validly-queued g ALWAYS has
             * owner set + an entry fn (callable or c_entry) + a 16-aligned
             * canonical coro.
             *
             * DEFENSE-IN-DEPTH (proven non-load-bearing): the arm64 crash this
             * once caught -- a queue entry pointing at a FREED-then-reused g
             * struct (owner=NULL / garbage coro -> SIGSEGV in coro_resume) -- is
             * now eliminated at the source.  The crasher was a stale wake_g (the
             * netpoll dup-wake) reaching hub_submit with a pointer to an already-
             * freed g whose struct had been returned to the OS and reused by a
             * non-runloom malloc (the [BADG] dumps showed garbage magic, not the
             * live/dead runloom canaries).  Two fixes close it: (1) hub_submit takes
             * the queue ref via runloom_g_try_incref BEFORE touching the g, so a
             * stale wake on a still-VALID freed g (refcount 0) bails [CBMC:
             * verify/cbmc/sched_qref_cbmc.c]; (2) runloom_g_slab_free no longer
             * frees g STRUCTS to the OS, so try_incref always reads a real
             * refcount instead of garbage on reused memory.  Validated: 0/16
             * connect-storm crashes on arm64 with this block compiled OUT, BADG
             * dumps drop from 3/10 dangerous cases to 0/10.  The block is kept as
             * a cheap (a few comparisons) crash-resistance assert for any future
             * weak-memory regression; on the happy path it never fires.  Skipping
             * a flagged entry is safe (drop the queue ref + continue, like the
             * coro==NULL / done case above): a real g ran via its valid entry. */
            runloom_coro_t *cp = g->coro;
            if (g->owner == NULL ||
                (g->callable == NULL && g->c_entry == NULL) ||
                cp == NULL || ((uintptr_t)cp >> 47) != 0 ||
                ((uintptr_t)cp & 0xf) != 0 || (uintptr_t)cp < 0x10000) {
#ifdef RUNLOOM_DBG_BADCORO
                /* Source-hunt dump (off by default).  Post-fix this should never
                 * print; magic distinguishes live (600dc0de) / slab-freed (dead)
                 * / OS-reused (other) memory, done splits completed (benign) from
                 * the once-dangerous non-completed case. */
                char dbuf[256];
                int dm = snprintf(dbuf, sizeof dbuf,
                    "[BADG] done=%d magic=%x owner=%p coro=%p call=%p centry=%p src=%d g=%p\n",
                    (int)__atomic_load_n(&g->done, __ATOMIC_ACQUIRE), g->magic,
                    (void *)g->owner, (void *)cp, (void *)g->callable,
                    (void *)g->c_entry, gsrc, (void *)g);
                if (dm > 0) (void)write(2, dbuf, (size_t)dm);
#endif
                h->sched.current = NULL;
                runloom_tls_current_g = NULL;
                if (from_runq) runloom_g_decref(g);   /* global-runq queue ref */
                if (had_qref) runloom_g_decref(g);    /* default queue ref */
                continue;
            }
            runloom_g_state_set(g, RUNLOOM_GST_RUNNING);   /* observational: now on-CPU */
            runloom_hub_resume_begin(h, g);
            runloom_coro_resume(cp);
            }
            runloom_hub_resume_end(h);
            self_queued = runloom_tls_self_queued;
            parked_offqueue = runloom_tls_parked_offqueue;
            runloom_tls_self_queued = 0;
            runloom_tls_parked_offqueue = 0;
            runloom_tls_current_g = NULL;
            h->sched.current = NULL;

            /* Drop THIS entry's default queue ref now that the entry is fully
             * consumed (the g was resumed).  One site covers every post-resume
             * outcome below (done / parked / yielded / re-queued): a g that
             * re-queued itself during the resume took a NEW queue ref via its
             * fresh hub_submit, so this drops only the old entry's ref.  Drops to
             * the spawn ref (not zero) for a still-live g, so the reads below are
             * safe; the done branch's scheduler decref then frees it.  from_runq
             * (global-runq) gs have had_qref==0 and keep their own accounting. */
            if (had_qref) runloom_g_decref(g);

            if (runloom_coro_done(g->coro)) {
                runloom_advice_record_g(g);   /* fold stack HWM (profiling) */
                runloom_g_state_set(g, RUNLOOM_GST_DONE);
                /* Drain g's chunks, restore hub's tstate so the decref
                 * (potentially calling Python tp_dealloc) allocates on
                 * the hub's chunk -- not on a NULL datastack that
                 * would otherwise leak when the next iter overwrites.
                 * Re-snap so the next completion path / hub exit can
                 * load again. */
                runloom_drain_g_datastack();
                runloom_pystate_load(&hub_snap);
                /* Force-unlink any netpoll parker still referencing g.
                 * If wait_fd's safety unlink missed (because a wake
                 * path bypassed the netpoll dispatcher AND the safety
                 * check's structural test couldn't see the leak), the
                 * parker would survive into stack-pool reuse and
                 * resurrect this just-freed g via a future pump
                 * dispatch.  This call covers that gap. */
                runloom_netpoll_force_unlink_g_parker(g);
                runloom_g_decref(g);                  /* scheduler ref */
                if (from_runq) runloom_g_decref(g);   /* queue ref */
                /* pending_complete LAST -- see the sibling completion site:
                 * the final decref may run a fiber_n batch teardown (arena reset),
                 * which must be visible before mn_run observes pending == 0. */
                runloom_mn_pending_complete(h);
                runloom_pystate_snap(&hub_snap);
            } else if (runloom_use_global_runq()) {
                /* Steal-woken release via the wake-state machine -- a near-exact
                 * mirror of the per-g-tstate release above; the difference is
                 * only that the migrated state rides in g->snap (saved by the
                 * parker BEFORE it yielded) instead of a per-g tstate.
                 *
                 * parked_offqueue => the g parked off-queue (netpoll/chan/sleep)
                 * and a wake_g will route it back through the global runq.  End
                 * ownership now: this happens AFTER the snap was saved, and while
                 * RUNNING a wake only set RUNNING_WOKEN (never enqueued), so no
                 * other hub could pull the g and load a half-saved snap.  CAS
                 * RUNNING->PARKED so a later wake enqueues it; if a wake already
                 * landed (RUNNING_WOKEN), enqueue exactly once now. */
                if (parked_offqueue) {
                    int rexp = RUNLOOM_WS_RUNNING;
                    /* Detach the g's ref-holding interpreter state from THIS
                     * hub's tstate now -- still RUNNING (sole owner), BEFORE the
                     * g becomes migratable.  The parker already saved g->snap,
                     * which owns its OWN refs to context / current_exception /
                     * exc_value; loading the hub's clean base here drops this
                     * hub's duplicate refs (rebalancing them).  Without it the
                     * origin hub and the stealing hub (which loads the snap) both
                     * own the g's exception and race its refcount -> the freed
                     * object surfaces as "error return without exception set" /
                     * SEGV deep in CPython's exception machinery.  This is the
                     * snap-mode analogue of per-g-tstate's PyEval_SaveThread
                     * (detach) before the RUNNING->PARKED transition.  Re-snap to
                     * keep hub_snap valid for the next g. */
                    runloom_pystate_load(&hub_snap);
                    runloom_pystate_snap(&hub_snap);
                    if (runloom_g_state_in(g, RUNLOOM_GST_MASK_PARKED)) {
                        /* madvise idle stack below sp while still RUNNING (sole
                         * owner -- a wake only flips RUNNING->RUNNING_WOKEN), so
                         * no hub can resume into pages mid-drop.  Cross-hub-safe
                         * for the same reason the sweep handshake is.  No-op
                         * unless RUNLOOM_STACK_PARK_DONTNEED=1. */
                        runloom_coro_park(g->coro);
                    }
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    RUNLOOM_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_PARKED);
                        if (from_runq) runloom_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN: a wake landed during the resume.
                         * Re-arm a single entry and enqueue now (snap is saved),
                         * exactly once -- no lost wake, no duplicate. */
                        RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING_WOKEN, RUNLOOM_WS_QUEUED);
                        __atomic_store_n(&g->wake_state, RUNLOOM_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        runloom_g_incref(g);                  /* new queue ref */
                        runloom_mn_global_runq_push(g);
                        if (from_runq) runloom_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield (sched_yield: already on local FIFO, or
                     * raw runloom_coro_yield: push it here).  Holds no live parker,
                     * so no wake_g can race -- stays RUNNING, hub-pinned. */
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_RUNNING);
                    __atomic_store_n(&g->wake_state, RUNLOOM_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) runloom_g_decref(g);       /* old queue ref */
                    if (!self_queued) runloom_sched_ready_push(&h->sched, g);
                }
            } else if (!self_queued) {
                /* Raw runloom_coro_yield() -- g didn't go through
                 * sched_yield to push itself.  Keep it alive on the
                 * local FIFO.  tstate still has g's state from after
                 * resume; that's fine -- next iter's g_next->snap
                 * load overwrites. */
                runloom_sched_ready_push(&h->sched, g);
            } else if (runloom_g_state_in(g, RUNLOOM_GST_MASK_PARKED)) {
                /* g parked on a waiter (netpoll/chan/sleep/park_safe)
                 * and won't run again until woken: drop its now-idle
                 * stack pages.  No-op unless RUNLOOM_STACK_PARK_DONTNEED=1.
                 * Gated on the PARKED states so a cooperative sched_yield
                 * (also self_queued, but re-queued + resuming shortly)
                 * doesn't pay an immediate re-fault.
                 *
                 * Safe under M:N: a woken g routes back to THIS (owning)
                 * hub's non-stealable local FIFO, so this hub is the sole
                 * resumer -- the madvise happens-before the next resume on
                 * one thread.  Default-OFF is a throughput choice, not a
                 * safety one (see runloom_coro_park doc in coro.h). */
                runloom_coro_park(g->coro);
            }
            /* remaining sched_yield path: g pushed itself, tstate has
             * g's state, no work needed here. */
        }
        /* Relinquish the world if re-running a sole g while a sibling is
         * stopped for the world (breaks the gc-stw-churn monopoly deadlock). */
        runloom_mn_world_yield_if_monopolizing(h);
    }
    /* Restore hub's tstate before the thread exits. */
    runloom_pystate_load(&hub_snap);
    runloom_tls_hub = NULL;
    /* Tear down the hub's iouring ring.  Unregister from netpoll FIRST
     * so the pump can't dispatch a CQE to a freed ring; THEN destroy.
     * Hubs only exit when the scheduler is shutting down + all gs have
     * completed, so inflight==0 here is the expected case; if a ring
     * still had inflight ops we'd leak the underlying CQEs (kernel
     * frees them on close). */
    if (h->iouring_ring != NULL) {
        if (h->loop_wake_fd >= 0) {
            /* Loop backend: no eventfd was registered in the epoll pump; just
             * close the wake eventfd (the multishot polls die with the ring). */
            runloom_iouring_loop_hub_disarm(h->iouring_ring);
            h->loop_wake_fd = -1;
        } else {
            runloom_netpoll_remove_iouring_ring(h->iouring_eventfd);
        }
        runloom_iouring_ring_destroy(h->iouring_ring);
        h->iouring_ring    = NULL;
        h->iouring_eventfd = -1;
    }
    /* Reverse runloom_coro_thread_init for clean exit on Windows
     * (ConvertFiberToThread); no-op elsewhere. */
    runloom_coro_thread_fini();
    /* Delete this hub's tstate ON THIS (owning) THREAD, not later in
     * runloom_mn_fini on the main thread.  PyThreadState_New (above) gilstate-
     * bound it to THIS thread's gilstate-TSS slot -- _PyThreadState_NewBound
     * binds the new tstate as the thread's gilstate tstate when that slot is
     * empty, which it always is for a fresh hub OS thread -- and the delete path
     * (tstate_delete_common -> unbind_gilstate_tstate) clears the CALLING
     * thread's slot.  Deleting from runloom_mn_fini on the main thread would
     * therefore clear the MAIN thread's gilstate binding instead of this hub's,
     * corrupting it: a latent fault for any later PyGILState_Ensure on the main
     * thread, and a hard abort under a --with-pydebug free-threaded CPython
     * (pystate.c:345 asserts tstate == this thread's gilstate tstate).  Clearing
     * + deleting here, where gilstate_tss == hts, satisfies the contract; we
     * publish h->tstate = NULL first so the mn_fini sweep skips this slot. */
    if (getenv("RUNLOOM_GILSTATE_DELETE_ON_MAIN") == NULL) {
        /* trace conformance: deleter is this hub's OWN thread (the fix) */
        runloom_gilstate_trace("Delete", h->id, h->id);
        PyThreadState_Clear(hts);
        __atomic_store_n((void **)&h->tstate, (void *)NULL, __ATOMIC_RELEASE);
        PyThreadState_DeleteCurrent();
    } else {
        /* NEGATIVE CONTROL (test-only): reproduce the pre-c28e5ca bug -- leave
         * the tstate alive for runloom_mn_fini to delete from the MAIN thread.
         * Trips pystate.c:345 under --with-pydebug, and the emitted trace shows a
         * Delete with deleter=main that the TLA trace check flags. */
        PyEval_SaveThread();
    }
    RUNLOOM_THREAD_RETURN(NULL);
}
