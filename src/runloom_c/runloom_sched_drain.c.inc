/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- Single-thread logical clock (deterministic asyncio-timer replay) ----
 *
 * When RUNLOOM_LOGICAL_CLOCK=1, sched_sleep deadlines and the loop clock
 * (runloom.aio loop.time()) are measured against this logical clock, not the wall
 * clock, and the drain advances it to the earliest pending timer ONLY at
 * quiescence -- ready ring empty and no I/O parkers, i.e. a pure-timer wait.
 * So timer firing order becomes a function of the schedule (e.g. PCT's
 * controlled ready-pop) rather than wall-clock timing: the single-thread
 * analogue of the M:N logical clock (mn lever 6).  Real I/O still rides the wall
 * clock (the netpoll pump), so a workload with live fds is not made
 * deterministic by this -- the open-system limit.  Off by default; zero cost. */
static int    runloom_sched_logical_on = -1;     /* -1 = lazy-init from env */
static double runloom_sched_logical_val = 0.0;

/* Deterministic simulated-I/O mode (RUNLOOM_SIM, Slice 2).  Cached read-once,
 * exported (netpoll's sim pump + deadline clock gate on it).  When set (to a
 * seed -- the value is the repro key, so ANY non-empty value including "0" is
 * ON), the netpoll deadline clock is routed through the single-thread logical
 * clock and the pump runs the sim model loop instead of a real poller.  Off by
 * default; production + every existing test are untouched.  See
 * docs/dev/soak/SIM_IO_DST.md. */
static int runloom_sim_on = -1;                  /* -1 = lazy-init from env */
int runloom_sim_enabled(void)
{
    if (__builtin_expect(runloom_sim_on < 0, 0)) {
        const char *e = getenv("RUNLOOM_SIM");
        runloom_sim_on = (e != NULL && e[0] != '\0') ? 1 : 0;
    }
    return runloom_sim_on;
}

static int runloom_sched_logical_enabled(void)
{
    if (__builtin_expect(runloom_sched_logical_on < 0, 0)) {
        const char *e = getenv("RUNLOOM_LOGICAL_CLOCK");
        /* RUNLOOM_SIM REQUIRES logical time: wait_fd deadlines ride the logical
         * clock under sim and sched_sleep must share it, so sim implies the
         * logical clock even when RUNLOOM_LOGICAL_CLOCK is unset -- otherwise a
         * mixed sched_sleep + wait_fd sim workload would run on two clocks. */
        runloom_sched_logical_on = ((e != NULL && e[0] == '1') ||
                                    runloom_sim_enabled()) ? 1 : 0;
    }
    return runloom_sched_logical_on;
}

/* The clock sched_sleep deadlines and loop.time() read: the logical clock when
 * enabled, else the wall-clock `fallback` the caller passes.  Callable across
 * translation units (m_sched_sleep / the loop-clock builtin are in module.c). */
double runloom_sched_logical_now_or(double fallback)
{
    return runloom_sched_logical_enabled() ? runloom_sched_logical_val : fallback;
}

/* The single-thread logical clock in NANOSECONDS.  netpoll's deadline clock
 * (netpoll_deadline_now_ns) reads this under RUNLOOM_SIM as the baseline for a
 * wait_fd's stored deadline, so timeouts ride logical time.  Exported. */
long long runloom_sched_logical_ns(void)
{
    return (long long)(runloom_sched_logical_val * 1e9);
}

/* Reset the logical clock to 0.  For runloom_sim_reset(), so an in-process run
 * starts from a fixed absolute magnitude and its logical-ns readings are bit-exact
 * across runs (a delta measured at a large accumulated magnitude jitters by the
 * double->ns quantization; a fresh subprocess per seed already starts at 0, so
 * this only matters for multi-scenario-per-process replay/tests).  Call only
 * between run()s, never mid-run. */
void runloom_sched_logical_reset(void)
{
    runloom_sched_logical_val = 0.0;
}

/* Sim clock advance (RUNLOOM_SIM only) -- the authoritative owner of logical-
 * clock advancement while netpoll parkers are outstanding.  Move the logical
 * clock FORWARD to the earliest pending deadline across BOTH the scheduler's
 * logical sleep heap AND the netpoll deadline heaps (netpoll_min_ns, -1 if
 * none).  The sim netpoll pump calls this because a wait_fd parker keeps
 * netpoll_parked > 0, which routes the drain into the pump every iteration
 * instead of its own quiescence-advance branch -- so without this a wait_fd
 * parker + a sched_sleep delivery fiber would either mis-order (jump past the
 * sleeper) or stall.  Real-time sleepers (sleep_real -- the aio keepalive
 * heartbeat) ride the WALL clock and are never advanced here; a logical sleeper
 * always sorts ahead of a real one in the heap (small logical wake_at vs a large
 * wall wake_at), so the earliest logical deadline is the heap front iff its
 * sleep_real is 0.  Monotonic (never moves backward).
 *
 * Returns the ns it advanced to (== the earliest of the two, EXACT ns), for the
 * caller to pass straight to runloom_pump_drain_expired -- so the expiry compare
 * is long-long-vs-long-long with no double round-trip.  Returns -1 when nothing
 * is pending (a settled system: the caller makes no progress, which for a
 * forever-park surfaces as a hang -- the instant netpoll deadlock oracle is a
 * documented follow-up tied to the readiness model). */
long long runloom_sched_sim_advance_clock(runloom_sched_t *s, long long netpoll_min_ns)
{
    long long sleeper_ns = -1, target_ns;
    double sleeper_sec = -1.0;
    if (s != NULL && s->sleep_size > 0) {
        runloom_g_t *pk = runloom_sched_sleep_peek(s);
        if (pk != NULL && !pk->sleep_real) {
            sleeper_sec = pk->wake_at;
            sleeper_ns  = (long long)(sleeper_sec * 1e9);
        }
    }
    target_ns = netpoll_min_ns;
    if (sleeper_ns >= 0 && (target_ns < 0 || sleeper_ns < target_ns))
        target_ns = sleeper_ns;
    if (target_ns < 0) return -1;
    {
        /* Advance to the earliest event.  Use the sleeper's EXACT wake_at double
         * when it IS the earliest, so the drain's (wake_at > now) compare is
         * exact -- matching the non-sim quiescence-advance path (line ~233); a
         * netpoll deadline derives seconds from its ns.  The RETURNED target_ns
         * (exact ns) is what fires the netpoll timeout, so logical_val's
         * ns round-trip never enters the expiry decision. */
        double tgt = (sleeper_ns == target_ns && sleeper_sec >= 0.0)
                     ? sleeper_sec : (double)target_ns * 1e-9;
        if (tgt > runloom_sched_logical_val) runloom_sched_logical_val = tgt;
    }
    return target_ns;
}

/* ---- Drain (main loop) ----
 *
 * sched_snap optimisation: the scheduler's tstate (the Python frame
 * chain anchored at runloom_c.run()'s caller, plus context / exc state
 * / recursion budgets at drain entry) is INVARIANT for the duration of
 * drain.  Drain itself does no Python work between iterations -- the
 * only places where it would are runloom_g_decref (may run tp_dealloc) and
 * the loop's final return to Python.
 *
 * So instead of snap+load per iteration (which was costing ~10 ns of
 * write traffic on the slow path), we snap once at entry and load only
 * where Python may run -- before runloom_g_decref and at drain exit.  Re-
 * snap after decref so the next completion-path load is still valid. */
Py_ssize_t runloom_sched_drain(runloom_sched_t *s)
{
    Py_ssize_t completed_before = s->completed;
    runloom_pystate_snap_t sched_snap;
    /* An async Python signal handler (Ctrl-C etc.) that RAISED while we were
     * idle: carry it across the exit-path sched_snap load (which restores
     * current_exception from the snap and would otherwise clobber it), then
     * PyErr_Restore it so it propagates out of m_run -> run_forever. */
    int sig_break = 0;
    PyObject *sig_t = NULL, *sig_v = NULL, *sig_b = NULL;

    s->stopping = 0;
    runloom_pystate_snap(&sched_snap);

    while (!s->stopping && (!runloom_sched_ready_empty(s) ||
                            s->sleep_size > 0 ||
                            s->timer_size > 0 ||
                            __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                            runloom_iouring_inflight() > 0 ||
                            runloom_blockpool_inflight() > 0 ||
                            runloom_foreign_park_inflight() > 0 ||
                            s->quiescence_head != NULL ||
                            __atomic_load_n(&s->wake_list_head,
                                            __ATOMIC_ACQUIRE) != NULL)) {
        double now = runloom_sched_logical_now_or(runloom_sched_monotonic_seconds());
        /* Drain cross-thread wakes into the ready ring before any
         * other work this iteration.  Empty in the common (same-
         * thread wake_safe) case -- one atomic load on the head.
         * Otherwise: a single lock acquire on wake_list_lock, then
         * lock-free per-g pushes to ready. */
        if (__atomic_load_n(&s->wake_list_head, __ATOMIC_ACQUIRE) != NULL) {
            runloom_sched_drain_wake_list(s);
            /* WAKE-TRACE: loop-top drained the durable wake_list to ready (model
             * DrainConsume) -- the unconditional re-peek that makes a lost poke
             * self-healing.  Whole-list at once, so one event per loop-top drain. */
            runloom_wake_trace_event("DRAIN_CONSUME", 0, 0);
        }
        /* Wake up any sleepers whose time has come.  A LOGICAL sleeper is due
         * when the logical clock (`now`) reaches it; a REAL one (keepalive) when
         * the wall clock does.  Off the logical clock both reduce to `now`
         * (== wall), the original behaviour. */
        while (s->sleep_size > 0) {
            runloom_g_t *pk = runloom_sleep_peek(s);
            double clk = pk->sleep_real ? runloom_sched_monotonic_seconds() : now;
            if (pk->wake_at > clk) break;
            runloom_ready_push(s, runloom_sleep_pop(s));
        }
        /* Fire any due in-memory TIMED parks (runloom_c.park(timeout)).  Their
         * deadlines are REAL (monotonic), like sleep_real -- never the logical
         * clock.  Claims each via the parked_safe CAS (exactly-once vs a real
         * wake). */
        if (s->timer_size > 0) {
            runloom_sched_drain_timers(s, runloom_sched_monotonic_seconds());
        }
        /* Quiescence checkpoint: if nothing else is immediately runnable
         * (ready ring empty after draining wakes + due sleepers), resume the
         * run_ready() waiters NOW -- before we would block on netpoll/timers.
         * This is the "no fiber is immediately runnable" boundary the
         * primitive promises; do it before the netpoll-pump block below so a
         * waiter never waits on I/O.  Re-loop to pop them as ready. */
        if (runloom_sched_ready_empty(s) && s->quiescence_head != NULL) {
            runloom_sched_flush_quiescence(s);
            continue;
        }
        /* Pump netpoll: if any fibers are parked, wait for I/O up
         * to the next sleep deadline (or forever if none).  Drives
         * runloom_sched_wake which moves ready I/O fibers back to
         * the ready queue. */
        if (runloom_sched_ready_empty(s) &&
            (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 || s->sleep_size > 0 ||
             s->timer_size > 0 ||
             runloom_iouring_inflight() > 0 || runloom_blockpool_inflight() > 0 ||
             runloom_foreign_park_inflight() > 0)) {
            long long timeout_ns = -1;
            if (s->sleep_size > 0) {
                double gap = runloom_sleep_peek(s)->wake_at - now;
                if (gap < 0) gap = 0;
                if (gap > 60.0) gap = 60.0;
                timeout_ns = (long long)(gap * 1e9);
            }
            /* In-memory TIMED parks have REAL (monotonic) deadlines: bound the
             * upcoming block by the nearest so the timer drain at the loop top
             * fires it on time.  min() with any sleep gap above. */
            {
                double tdl = runloom_sched_timer_next_deadline(s);
                if (tdl >= 0.0) {
                    double tgap = tdl - runloom_sched_monotonic_seconds();
                    long long t_ns = tgap <= 0.0 ? 0 : (long long)(tgap * 1e9);
                    if (timeout_ns < 0 || t_ns < timeout_ns) timeout_ns = t_ns;
                }
            }
            /* io_uring CQ-OVERFLOW safety -- before any blocking pump.
             * When the kernel CQ ring fills (multishot recv under backpressure,
             * or many concurrent single-shot completions), excess completions go
             * to the kernel's overflow backlog and do NOT re-signal the
             * registered eventfd.  A blocking epoll_wait below would then sleep
             * forever waiting for an eventfd edge that never comes, stranding the
             * fiber whose completion is in overflow -- and the only thing that
             * would free CQ space is that same stranded fiber (a real
             * backpressure deadlock; observed: CQ empty + IORING_SQ_CQ_OVERFLOW
             * set + asleep in epoll_wait(-1)).  Drain here first:
             * runloom_iouring_drain flushes the overflow backlog into the CQ ring,
             * processes the CQEs, and wakes the waiters.  If that readied a fiber,
             * re-loop to run it instead of blocking.  Cheap when nothing is
             * pending (one EAGAIN eventfd read + a flag check). */
            if (runloom_iouring_inflight() > 0) {
                runloom_iouring_drain();
                if (!runloom_sched_ready_empty(s)) continue;
            }
            /* iouring fibers wake when the pump observes a CQE on
             * the registered eventfd; blocking-pool fibers wake when
             * a worker pokes the pump-interrupt eventfd -- both ride the
             * netpoll pump, so the pump call covers netpoll parkers,
             * iouring waiters AND blocking-pool waiters in one syscall. */
            if (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                runloom_iouring_inflight() > 0 ||
                runloom_blockpool_inflight() > 0 ||
                runloom_foreign_park_inflight() > 0 ||
                s->timer_size > 0) {
                /* Logical-clock mode: timeout_ns is a LOGICAL gap, meaningless
                 * as a real I/O wait, so cap it to a short real re-poll.  The
                 * logical clock advances only at full quiescence (the else-if
                 * below), reached once no fd is parked -- so a live-I/O workload
                 * stays wall-clock (the open-system limit). */
                if (runloom_sched_logical_enabled() && timeout_ns > 50000000LL)
                    timeout_ns = 50000000LL;
                /* FOREIGN-THREAD WAKE BACKSTOP.  Blockpool workers / foreign
                 * parkers (run_in_executor, etc.) wake us by poking the pump
                 * eventfd (runloom_netpoll_wake_pump), but that poke RACES our
                 * decision to block here.  Under load the poke can land in the
                 * window after we last checked the wake_list and before we
                 * enter epoll_wait, and is then lost -- leaving the sched asleep
                 * in epoll_wait(timeout=-1) FOREVER while the worker has already
                 * run the job and queued the woken g on the wake_list (gdb-
                 * confirmed: bp_inflight=0, all workers idle on bp_cond, main
                 * thread parked in epoll_wait(-1) on the default pool's epoll).
                 * M:N hubs never hit this -- they busy-poll ~1 ms, so the bounded
                 * pump timeout is their backstop (see runloom_netpoll_wake_pump's
                 * own comment); the single-thread drain is the ONLY path that
                 * blocks UNBOUNDED on a foreign poke.  Give it the same backstop:
                 * cap the block so a lost poke self-heals on the next re-poll,
                 * which re-drains the wake_list at the loop top.  The poke stays
                 * the fast path (microseconds); this only bounds the rare
                 * lost-poke tail.  Reproduced ~1% in the gate / reliably under
                 * `-s` parallel load (tests/test_swarm_blockpool_diag_crash.py
                 * TestBlockingResults). */
                if ((runloom_blockpool_inflight() > 0 ||
                     runloom_foreign_park_inflight() > 0) &&
                    (timeout_ns < 0 || timeout_ns > 2000000LL))
                    timeout_ns = 2000000LL;   /* 2 ms */
                /* WAKE-TRACE: about to block in the pump (model DrainPeekEmpty ->
                 * DrainDecide -> DrainBlock).  cap=1 iff the 2 ms foreign-wake
                 * backstop is armed (timeout capped) -- the observable witness that
                 * the f214341 cap is live; a regression that fails to arm it would
                 * emit cap=0 here while inflight>0 and the driver flags it. */
                runloom_wake_trace_event("DRAIN_BLOCK", 0,
                                         timeout_ns == 2000000LL ? 1 : 0);
                /* IOUWAKE-TRACE: the same pump block, but for the io_uring CQE
                 * model (DrainPeekEmpty -> DrainDecide -> DrainBlock).  cap=1 iff
                 * an iouring op is inflight -- the witness that the drain-first
                 * overflow flush (runloom_iouring_drain at the loop top above,
                 * gated on the same inflight>0) was armed BEFORE this block, the
                 * model's Heal/\IouringInflight -> flush_first decision.  A
                 * regression that dropped the loop-top flush would leave inflight>0
                 * but skip the flush, which the driver's two-tier mapping catches. */
                runloom_iouwake_trace_event("DRAIN_BLOCK", 0,
                                            runloom_iouring_inflight() > 0 ? 1 : 0);
                runloom_netpoll_pump(timeout_ns);
                /* WAKE-TRACE: the pump returned -- a delivered poke (model
                 * DrainPumpWake) or the backstop timeout (DrainBackstopTimeout);
                 * either re-loops to the top to re-drain wake_list. */
                runloom_wake_trace_event("DRAIN_UNBLOCK", 0, 0);
                runloom_iouwake_trace_event("DRAIN_UNBLOCK", 0, 0);
            } else if (timeout_ns > 0) {
                if (runloom_sched_logical_enabled() &&
                    !runloom_sleep_peek(s)->sleep_real) {
                    /* Earliest pending timer is a LOGICAL one (app sched_sleep /
                     * call_at): jump the logical clock straight to its deadline;
                     * the sweep (below + next loop top) fires it.  No real sleep
                     * -> deterministic order.  A real-time sleeper (the loop
                     * keepalive heartbeat) is never at the heap front while any
                     * logical timer is pending (its wall-clock wake_at dwarfs the
                     * small logical ones), so this only fires app timers. */
                    runloom_sched_logical_val = runloom_sleep_peek(s)->wake_at;
                } else {
                    /* Real-time sleeper (keepalive), or logical clock off: sleep
                     * real wall-clock until it.  Under the logical clock the
                     * timeout_ns above was computed against logical `now`, which
                     * is meaningless for a wall-clock sleeper, so recompute the
                     * real gap.  Cap at 50 ms so a signal/debugger can unstick. */
                    if (runloom_sched_logical_enabled()) {
                        double rgap = runloom_sleep_peek(s)->wake_at -
                                      runloom_sched_monotonic_seconds();
                        if (rgap < 0) rgap = 0;
                        timeout_ns = (long long)(rgap * 1e9);
                    }
                    if (timeout_ns > 50000000LL) timeout_ns = 50000000LL;
                    if (timeout_ns > 0) runloom_sleep_ns(timeout_ns);
                }
            }
            /* We just blocked on I/O/timers with NO Python on this thread's
             * stack.  An async signal (Ctrl-C -> SIGINT) that fired during the
             * block left a pending Python handler but only EINTR'd the syscall
             * -- CPython's eval breaker runs handlers only inside a fiber's
             * eval loop, so without this the loop would re-block forever.  Run
             * pending handlers NOW, in the scheduler's tstate, BEFORE looping
             * back to resume any woken fiber: the scheduler must WIN the
             * race to catch the signal (whichever fiber's eval loop catches
             * it first would just swallow it -> hang).  A raised exception
             * (KeyboardInterrupt / a custom handler's) then propagates out of
             * run_forever().  PyErr_CheckSignals is a cheap no-op when nothing
             * is tripped, and a no-op off the main thread (correct: signals run
             * only there).  This idle path is not the hot path. */
            runloom_pystate_load(&sched_snap);
            /* load() consumed sched_snap (valid=0).  Re-snap immediately to
             * restore the loop invariant the completion path (the load+snap at
             * the bottom) and the drain-exit load both rely on: sched_snap must
             * be a VALID scheduler-tstate baseline at the top of every
             * iteration.  Without this, a g that completes after an idle block
             * hits a no-op load at completion, so tp_dealloc/cal_record runs on
             * the just-freed g datastack chunk -> use-after-free (silent pooled-
             * memory corruption on glibc; a hard access violation on Windows).
             * Mirrors the load->snap pairing the completion path already uses. */
            runloom_pystate_snap(&sched_snap);
            /* Re-queue any sleeper whose deadline elapsed while we blocked: the
             * top-of-loop sweep won't run until the NEXT iteration, but the grab
             * below runs NOW, so without this an expired sleeper leaves the ready
             * ring "empty" and the grab steals its signal.  A busy-poll
             * cooperative wrapper (CoPoll: select.poll has no backing fd) parks
             * on a short sched_sleep; once it expires it resumes and its OWN eval
             * loop runs the pending signal handler -- so let it. */
            now = runloom_sched_logical_now_or(runloom_sched_monotonic_seconds());
            while (s->sleep_size > 0) {
                runloom_g_t *pk = runloom_sleep_peek(s);
                double clk = pk->sleep_real ? runloom_sched_monotonic_seconds() : now;
                if (pk->wake_at > clk) break;
                runloom_ready_push(s, runloom_sleep_pop(s));
            }
            /* A due sleeper just re-queued (above) runs Python next and its own
             * eval loop delivers any pending signal in-context -- so only probe
             * for a signal when the ready ring is still empty.  We're the only
             * thread that can run Python signal handlers (the loop thread); the
             * idle blocking syscall above EINTR'd on a signal but ran no handler
             * (no Python on the C scheduler's stack).  Run pending handlers NOW.
             * If one RAISES, hand the exception to a fiber parked in wait_fd
             * (runloom_netpoll_signal_wake) so it propagates out of that fiber's
             * cooperative blocking call -- where a user try/except sees it,
             * exactly as a signal interrupting a real recv()/select() does (the
             * verbatim CPython test_select_interrupt_exc contract).  Only when no
             * such parker exists (genuinely idle / sleep-only) does the scheduler
             * carry the raised exception out of run_forever()/run_until_complete,
             * as stock asyncio does for Ctrl-C. */
            if (runloom_sched_ready_empty(s) && PyErr_CheckSignals() < 0) {
                PyErr_Fetch(&sig_t, &sig_v, &sig_b);
                PyErr_NormalizeException(&sig_t, &sig_v, &sig_b);
                if (sig_v != NULL && sig_b != NULL)
                    PyException_SetTraceback(sig_v, sig_b);
                /* Prefer in-fiber delivery: stash the raised exception and
                 * wake one wait_fd parker to pick it up (see signal_wake /
                 * wait_fd).  signal_wake is called only here, after a handler
                 * actually raised -- never on a bare EINTR -- so a fiber is
                 * never woken spuriously. */
                if (s->signal_exc == NULL && sig_v != NULL) {
                    s->signal_exc = sig_v;
                    if (runloom_netpoll_signal_wake()) {
                        Py_XDECREF(sig_t); Py_XDECREF(sig_b);
                        sig_t = sig_v = sig_b = NULL;
                        continue;   /* the woken parker raises it in its own stack */
                    }
                    s->signal_exc = NULL;   /* no parker; carry it out of run() */
                }
                sig_break = 1;
                break;
            }
            continue;
        }
        /* Pop a ready g and resume it.
         *
         * Snap dance (Phase B):
         *   1. Save the SCHEDULER's tstate into a local snap on drain's
         *      own stack.  This captures the scheduler's frame chain,
         *      contextvars, recursion budget, etc.
         *   2. If g has a valid saved snap, load it into tstate -- this
         *      restores g's frame chain, contextvars, etc.  Otherwise
         *      g is on its first run; the initial root cframe is
         *      installed inside runloom_g_entry, on g's stack.
         *   3. Resume into g.  G runs Python code.  When it yields it
         *      calls runloom_sched_yield/park/sleep, all of which call
         *      runloom_pystate_snap to capture g's tstate into g->snap.
         *   4. After the swap returns, load the scheduler's snap back
         *      so the next loop iteration starts from a clean baseline.
         */
        {
            runloom_g_t *g = runloom_ready_pop(s);
            runloom_g_t *prev = s->current;

            /* Defensive (flagged by gcc -fanalyzer): the ready ring can be
             * empty here even though the loop condition was true -- the loop is
             * also entered for a non-NULL wake_list_head, and draining it (or a
             * concurrent cross-thread wake) need not leave a g in the ring.
             * runloom_ready_pop then returns NULL; re-loop rather than deref it. */
            if (g == NULL)
                continue;

            /* sched_snap is loop-invariant (taken at drain entry).  We
             * deliberately do NOT snap or load it per-iter: drain runs
             * no Python between iterations, so tstate can stay in g's
             * state across the brief window between coro_resume return
             * and the next iter's g->snap load. */

            s->current = g;

            /* noyield short-circuit -- the caller has asserted g
             * runs to completion without yielding.  We can share the
             * scheduler's datastack chunk and current_frame across
             * the resume: g's Python frames push onto the same chunk,
             * run, pop, and tstate ends up exactly where it was.  No
             * snap, no install, no drain, no load+resnap on
             * completion. */
            if (g->noyield) {
                runloom_coro_resume(g->coro);
                s->current = prev;
                if (runloom_coro_done(g->coro)) {
                    s->completed++;
                    runloom_cal_record(g);
                    runloom_g_decref(g);
                }
                /* If a noyield g ACTUALLY yielded (caller broke its
                 * promise) we'd land here with !done.  Carry on; the
                 * snap dance never happened, so tstate is now in g's
                 * uncommitted state and the next iter will see garbage.
                 * Undefined behaviour per the noyield contract. */
                continue;
            }

            if (g->snap.valid) {
                runloom_pystate_load(&g->snap);
            } else {
                /* First run for this g.  We must give it its own slice
                 * of the per-thread interpreter state, otherwise:
                 *   - g would allocate Python frame storage into the
                 *     scheduler's datastack_chunk, then g2 would do the
                 *     same starting from where the scheduler left off
                 *     (the snap restored that position), overwriting
                 *     g1's live frames -> segfault on g1 resume.
                 *   - g would inherit current_frame from the scheduler,
                 *     linking g's frame chain back into shared frames
                 *     across all fibers (the original cliff).
                 *
                 * Install a chunk from the per-thread pool (or NULL the
                 * datastack pointers so PyEval allocates fresh).  For
                 * 3.13 also NULL current_frame so g's first frame
                 * chains to nothing.  For 3.12, g_entry will install a
                 * root cframe on g's own stack before any Python code
                 * runs. */
                runloom_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030B0000
                /* Also reset exception state for first-run g.  Without
                 * this, the new g inherits whatever ts->exc_info /
                 * exc_state / current_exception was left by the
                 * scheduler's last load, which under heavy concurrent
                 * cascades may be a pointer into a freed chunk or a
                 * stale exception object.  Each fresh g starts with
                 * default-state exception. */
                {
                    PyThreadState *ts = PyThreadState_GET();
                    Py_CLEAR(ts->exc_state.exc_value);
                    ts->exc_state.previous_item = NULL;
                    ts->exc_info = &ts->exc_state;
#  if PY_VERSION_HEX >= 0x030C0000
                    Py_CLEAR(ts->current_exception);
#  endif
                }
#endif
#if PY_VERSION_HEX >= 0x030D0000
                {
                    PyThreadState *ts = PyThreadState_GET();
                    ts->current_frame = NULL;
                }
#endif
            }

            /* WAKE-TRACE: running a ready fiber (model DrainResume).  Emitted for
             * EVERY resume; the driver keeps only those that close a foreign-wake
             * episode (a preceding FOREIGN_WAKE for this g), dropping first-run /
             * cooperative-yield resumes -- so a RESUME with no durable append is a
             * resume-without-wake and deadlocks the replay (the lost-wakeup class). */
            runloom_wake_trace_event("RESUME", (unsigned long)(uintptr_t)g, 0);
            runloom_coro_resume(g->coro);

            s->current = prev;

            if (runloom_coro_done(g->coro)) {
                /* g done: runloom_g_decref below may run tp_dealloc, which
                 * needs drain's tstate (frame chain + datastack chunk)
                 * to be installed before allocating frames -- otherwise
                 * tp_dealloc allocs a chunk on the wrong root and we
                 * leak it on the next iter's g->snap load.  So free g's
                 * chunks, restore drain's tstate, decref, then re-snap
                 * so a subsequent completion (or drain exit) has a
                 * valid sched_snap to load. */
                runloom_drain_g_datastack();
                runloom_pystate_load(&sched_snap);
                s->completed++;
                runloom_cal_record(g);
                /* Force-unlink any wait_fd parker this g left linked (it can
                 * complete WHILE parked -- e.g. an accept/recv loop torn down
                 * by close()/cancel, or run() exiting on a parked g).  The
                 * parker is pool-allocated and its by_fd/heap links point at
                 * p->g; without this the parker stays linked after the g is
                 * decref'd/freed, and the next pump or netpoll_cancel_fd
                 * dereferences freed memory -> use-after-free.  Silent on glibc
                 * (freed heap still mapped) but a hard access violation on
                 * Windows -- the single-thread analogue of the M:N hub_main
                 * completion-path cleanup (mn_sched_hub_main.c.inc). */
                runloom_netpoll_force_unlink_g_parker(g);
                runloom_g_decref(g);
                runloom_pystate_snap(&sched_snap);
            } else if (runloom_g_state_in(g, RUNLOOM_GST_MASK_PARKED)) {
                /* Parked on a waiter (netpoll/chan/sleep/park_safe):
                 * drop the g's now-idle stack pages until it's woken.
                 * No-op unless RUNLOOM_STACK_PARK_DONTNEED=1.  Cooperative
                 * sched_yield gs are RUNNABLE (already re-queued), not
                 * PARKED, so they skip this and avoid a re-fault. */
                runloom_coro_park(g->coro);
            }
            /* Yielded gs: tstate stays in g's state.  Next iter's
             * g_next->snap load (or first-run install) overwrites. */
        }
    }
    /* Restore drain's tstate before returning to Python. */
    runloom_pystate_load(&sched_snap);
    /* Deadlock detection.  The loop above exited; if it exited because the
     * scheduler went QUIESCENT (not because of a clean sched_stop) yet
     * fibers remain parked on a channel / via park_safe, those fibers
     * are blocked on each other with no event source left to wake them (every
     * netpoll/timer/io/blockpool source was empty -- that's the loop's own exit
     * condition).  That is Go's "all fibers are asleep - deadlock!".  Report
     * it instead of returning silently with the work unfinished.  aio shuts its
     * loop down via sched_stop (s->stopping), so a clean aio teardown with
     * pending background tasks is excluded.  Single-thread only (M:N hubs have
     * their own liveness, and the registry count is process-global). */
    if (!s->stopping && !sig_break && runloom_mn_hub_count() == 0 &&
        runloom_deadlock_mode() != 0) {
        long ndead = runloom_count_deadlockable_fibers((const void *)s);
        if (ndead > 0) {
            fprintf(stderr,
                "\nrunloom: DEADLOCK -- the scheduler ran out of work with %ld "
                "fiber(s) still blocked on a channel/park and no way to "
                "wake them:\n", ndead);
            fflush(stderr);
            runloom_dump_fibers_fd(2);
            if (runloom_deadlock_mode() == 2) {
                PyErr_Format(PyExc_RuntimeError,
                    "runloom: all fibers are blocked -- deadlock "
                    "(%ld parked, none runnable)", ndead);
            }
        }
    }
    /* A signal handler raised (Ctrl-C): re-arm the exception on the now-restored
     * scheduler tstate so m_run returns NULL and it propagates out of the
     * run_forever()/run_until_complete() drive, as on stock asyncio. */
    if (sig_break) {
        PyErr_Restore(sig_t, sig_v, sig_b);
    }
    /* Defensive: a signal exception stashed for in-fiber delivery should
     * have been consumed by the woken parker's wait_fd resume before we return.
     * If the drain somehow exits first (e.g. a concurrent sched_stop), don't
     * leak it -- and don't carry it out silently either; drop it (the parker it
     * targeted is no longer being driven). */
    if (s->signal_exc != NULL) {
        Py_CLEAR(s->signal_exc);
    }
    return s->completed - completed_before;
}

/* ---- Time-sliced preemption (3.13t only) ----
 *
 * A separate pthread sleeps for quantum_us microseconds, then schedules
 * a pending call via Py_AddPendingCall.  CPython's eval loop checks
 * its pending queue at bytecode back-edges and call instructions; when
 * the call fires, runloom_preempt_yield_cb runs on whatever tstate the
 * eval loop is in, sees a current fiber (via the M:N TLS or the
 * single-thread sched), and yields cooperatively through the existing
 * snap + asm-yield path.
 *
 * Net effect: a fiber that never calls sched_yield() still gets
 * preempted every quantum_us, so it can't starve other gs.  This is
 * Go's runtime preemption model (since 1.14) ported to CPython.  Zero
 * hot-path overhead -- the eval_breaker bit is already checked by
 * CPython on every back-edge.
 *
 * For M:N hubs we post `runloom_hub_count` pending calls per quantum so
 * each hub eventually picks one up; whichever hub's eval loop dequeues
 * a call runs the yield on its currently-running g. */

static runloom_thread_t runloom_preempt_thread;
static volatile int runloom_preempt_running = 0;
static volatile long runloom_preempt_quantum_us = 10000;
