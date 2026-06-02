/* Part of pygo_sched.c -- #included into that translation unit, not compiled
 * standalone.  See pygo_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- Drain (main loop) ----
 *
 * sched_snap optimisation: the scheduler's tstate (the Python frame
 * chain anchored at pygo_core.run()'s caller, plus context / exc state
 * / recursion budgets at drain entry) is INVARIANT for the duration of
 * drain.  Drain itself does no Python work between iterations -- the
 * only places where it would are pygo_g_decref (may run tp_dealloc) and
 * the loop's final return to Python.
 *
 * So instead of snap+load per iteration (which was costing ~10 ns of
 * write traffic on the slow path), we snap once at entry and load only
 * where Python may run -- before pygo_g_decref and at drain exit.  Re-
 * snap after decref so the next completion-path load is still valid. */
Py_ssize_t pygo_sched_drain(pygo_sched_t *s)
{
    Py_ssize_t completed_before = s->completed;
    pygo_pystate_snap_t sched_snap;
    /* An async Python signal handler (Ctrl-C etc.) that RAISED while we were
     * idle: carry it across the exit-path sched_snap load (which restores
     * current_exception from the snap and would otherwise clobber it), then
     * PyErr_Restore it so it propagates out of m_run -> run_forever. */
    int sig_break = 0;
    PyObject *sig_t = NULL, *sig_v = NULL, *sig_b = NULL;

    s->stopping = 0;
    pygo_pystate_snap(&sched_snap);

    while (!s->stopping && (!pygo_sched_ready_empty(s) ||
                            s->sleep_size > 0 ||
                            __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                            pygo_iouring_inflight() > 0 ||
                            pygo_blockpool_inflight() > 0 ||
                            s->quiescence_head != NULL ||
                            __atomic_load_n(&s->wake_list_head,
                                            __ATOMIC_ACQUIRE) != NULL)) {
        double now = pygo_sched_monotonic_seconds();
        /* Drain cross-thread wakes into the ready ring before any
         * other work this iteration.  Empty in the common (same-
         * thread wake_safe) case -- one atomic load on the head.
         * Otherwise: a single lock acquire on wake_list_lock, then
         * lock-free per-g pushes to ready. */
        if (__atomic_load_n(&s->wake_list_head, __ATOMIC_ACQUIRE) != NULL) {
            pygo_sched_drain_wake_list(s);
        }
        /* Wake up any sleepers whose time has come. */
        while (s->sleep_size > 0 && pygo_sleep_peek(s)->wake_at <= now) {
            pygo_g_t *woke = pygo_sleep_pop(s);
            pygo_ready_push(s, woke);
        }
        /* Quiescence checkpoint: if nothing else is immediately runnable
         * (ready ring empty after draining wakes + due sleepers), resume the
         * run_ready() waiters NOW -- before we would block on netpoll/timers.
         * This is the "no goroutine is immediately runnable" boundary the
         * primitive promises; do it before the netpoll-pump block below so a
         * waiter never waits on I/O.  Re-loop to pop them as ready. */
        if (pygo_sched_ready_empty(s) && s->quiescence_head != NULL) {
            pygo_sched_flush_quiescence(s);
            continue;
        }
        /* Pump netpoll: if any goroutines are parked, wait for I/O up
         * to the next sleep deadline (or forever if none).  Drives
         * pygo_sched_wake which moves ready I/O goroutines back to
         * the ready queue. */
        if (pygo_sched_ready_empty(s) &&
            (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 || s->sleep_size > 0 ||
             pygo_iouring_inflight() > 0 || pygo_blockpool_inflight() > 0)) {
            long long timeout_ns = -1;
            if (s->sleep_size > 0) {
                double gap = pygo_sleep_peek(s)->wake_at - now;
                if (gap < 0) gap = 0;
                if (gap > 60.0) gap = 60.0;
                timeout_ns = (long long)(gap * 1e9);
            }
            /* iouring goroutines wake when the pump observes a CQE on
             * the registered eventfd; blocking-pool goroutines wake when
             * a worker pokes the pump-interrupt eventfd -- both ride the
             * netpoll pump, so the pump call covers netpoll parkers,
             * iouring waiters AND blocking-pool waiters in one syscall. */
            if (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                pygo_iouring_inflight() > 0 ||
                pygo_blockpool_inflight() > 0) {
                pygo_netpoll_pump(timeout_ns);
            } else if (timeout_ns > 0) {
                /* No fds parked, just a sleep heap timer.  Cap at 50 ms
                 * so an external caller (signal handler, debugger) can
                 * unstick us quickly. */
                if (timeout_ns > 50000000LL) timeout_ns = 50000000LL;
                pygo_sleep_ns(timeout_ns);
            }
            /* We just blocked on I/O/timers with NO Python on this thread's
             * stack.  An async signal (Ctrl-C -> SIGINT) that fired during the
             * block left a pending Python handler but only EINTR'd the syscall
             * -- CPython's eval breaker runs handlers only inside a goroutine's
             * eval loop, so without this the loop would re-block forever.  Run
             * pending handlers NOW, in the scheduler's tstate, BEFORE looping
             * back to resume any woken goroutine: the scheduler must WIN the
             * race to catch the signal (whichever goroutine's eval loop catches
             * it first would just swallow it -> hang).  A raised exception
             * (KeyboardInterrupt / a custom handler's) then propagates out of
             * run_forever().  PyErr_CheckSignals is a cheap no-op when nothing
             * is tripped, and a no-op off the main thread (correct: signals run
             * only there).  This idle path is not the hot path. */
            pygo_pystate_load(&sched_snap);
            /* Re-queue any sleeper whose deadline elapsed while we blocked: the
             * top-of-loop sweep won't run until the NEXT iteration, but the grab
             * below runs NOW, so without this an expired sleeper leaves the ready
             * ring "empty" and the grab steals its signal.  A busy-poll
             * cooperative wrapper (CoPoll: select.poll has no backing fd) parks
             * on a short sched_sleep; once it expires it resumes and its OWN eval
             * loop runs the pending signal handler -- so let it. */
            now = pygo_sched_monotonic_seconds();
            while (s->sleep_size > 0 && pygo_sleep_peek(s)->wake_at <= now) {
                pygo_ready_push(s, pygo_sleep_pop(s));
            }
            /* A due sleeper just re-queued (above) runs Python next and its own
             * eval loop delivers any pending signal in-context -- so only probe
             * for a signal when the ready ring is still empty.  We're the only
             * thread that can run Python signal handlers (the loop thread); the
             * idle blocking syscall above EINTR'd on a signal but ran no handler
             * (no Python on the C scheduler's stack).  Run pending handlers NOW.
             * If one RAISES, hand the exception to a goroutine parked in wait_fd
             * (pygo_netpoll_signal_wake) so it propagates out of that goroutine's
             * cooperative blocking call -- where a user try/except sees it,
             * exactly as a signal interrupting a real recv()/select() does (the
             * verbatim CPython test_select_interrupt_exc contract).  Only when no
             * such parker exists (genuinely idle / sleep-only) does the scheduler
             * carry the raised exception out of run_forever()/run_until_complete,
             * as stock asyncio does for Ctrl-C. */
            if (pygo_sched_ready_empty(s) && PyErr_CheckSignals() < 0) {
                PyErr_Fetch(&sig_t, &sig_v, &sig_b);
                PyErr_NormalizeException(&sig_t, &sig_v, &sig_b);
                if (sig_v != NULL && sig_b != NULL)
                    PyException_SetTraceback(sig_v, sig_b);
                /* Prefer in-goroutine delivery: stash the raised exception and
                 * wake one wait_fd parker to pick it up (see signal_wake /
                 * wait_fd).  signal_wake is called only here, after a handler
                 * actually raised -- never on a bare EINTR -- so a goroutine is
                 * never woken spuriously. */
                if (s->signal_exc == NULL && sig_v != NULL) {
                    s->signal_exc = sig_v;
                    if (pygo_netpoll_signal_wake()) {
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
         *      installed inside pygo_g_entry, on g's stack.
         *   3. Resume into g.  G runs Python code.  When it yields it
         *      calls pygo_sched_yield/park/sleep, all of which call
         *      pygo_pystate_snap to capture g's tstate into g->snap.
         *   4. After the swap returns, load the scheduler's snap back
         *      so the next loop iteration starts from a clean baseline.
         */
        {
            pygo_g_t *g = pygo_ready_pop(s);
            pygo_g_t *prev = s->current;

            /* Defensive (flagged by gcc -fanalyzer): the ready ring can be
             * empty here even though the loop condition was true -- the loop is
             * also entered for a non-NULL wake_list_head, and draining it (or a
             * concurrent cross-thread wake) need not leave a g in the ring.
             * pygo_ready_pop then returns NULL; re-loop rather than deref it. */
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
                pygo_coro_resume(g->coro);
                s->current = prev;
                if (pygo_coro_done(g->coro)) {
                    s->completed++;
                    pygo_cal_record(g);
                    pygo_g_decref(g);
                }
                /* If a noyield g ACTUALLY yielded (caller broke its
                 * promise) we'd land here with !done.  Carry on; the
                 * snap dance never happened, so tstate is now in g's
                 * uncommitted state and the next iter will see garbage.
                 * Undefined behaviour per the noyield contract. */
                continue;
            }

            if (g->snap.valid) {
                pygo_pystate_load(&g->snap);
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
                 *     across all goroutines (the original cliff).
                 *
                 * Install a chunk from the per-thread pool (or NULL the
                 * datastack pointers so PyEval allocates fresh).  For
                 * 3.13 also NULL current_frame so g's first frame
                 * chains to nothing.  For 3.12, g_entry will install a
                 * root cframe on g's own stack before any Python code
                 * runs. */
                pygo_first_run_install_datastack();
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

            pygo_coro_resume(g->coro);

            s->current = prev;

            if (pygo_coro_done(g->coro)) {
                /* g done: pygo_g_decref below may run tp_dealloc, which
                 * needs drain's tstate (frame chain + datastack chunk)
                 * to be installed before allocating frames -- otherwise
                 * tp_dealloc allocs a chunk on the wrong root and we
                 * leak it on the next iter's g->snap load.  So free g's
                 * chunks, restore drain's tstate, decref, then re-snap
                 * so a subsequent completion (or drain exit) has a
                 * valid sched_snap to load. */
                pygo_drain_g_datastack();
                pygo_pystate_load(&sched_snap);
                s->completed++;
                pygo_cal_record(g);
                pygo_g_decref(g);
                pygo_pystate_snap(&sched_snap);
            } else if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                /* Parked on a waiter (netpoll/chan/sleep/park_safe):
                 * drop the g's now-idle stack pages until it's woken.
                 * No-op unless PYGO_STACK_PARK_DONTNEED=1.  Cooperative
                 * sched_yield gs are RUNNABLE (already re-queued), not
                 * PARKED, so they skip this and avoid a re-fault. */
                pygo_coro_park(g->coro);
            }
            /* Yielded gs: tstate stays in g's state.  Next iter's
             * g_next->snap load (or first-run install) overwrites. */
        }
    }
    /* Restore drain's tstate before returning to Python. */
    pygo_pystate_load(&sched_snap);
    /* A signal handler raised (Ctrl-C): re-arm the exception on the now-restored
     * scheduler tstate so m_run returns NULL and it propagates out of the
     * run_forever()/run_until_complete() drive, as on stock asyncio. */
    if (sig_break) {
        PyErr_Restore(sig_t, sig_v, sig_b);
    }
    /* Defensive: a signal exception stashed for in-goroutine delivery should
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
 * the call fires, pygo_preempt_yield_cb runs on whatever tstate the
 * eval loop is in, sees a current goroutine (via the M:N TLS or the
 * single-thread sched), and yields cooperatively through the existing
 * snap + asm-yield path.
 *
 * Net effect: a goroutine that never calls sched_yield() still gets
 * preempted every quantum_us, so it can't starve other gs.  This is
 * Go's runtime preemption model (since 1.14) ported to CPython.  Zero
 * hot-path overhead -- the eval_breaker bit is already checked by
 * CPython on every back-edge.
 *
 * For M:N hubs we post `pygo_hub_count` pending calls per quantum so
 * each hub eventually picks one up; whichever hub's eval loop dequeues
 * a call runs the yield on its currently-running g. */

static pygo_thread_t pygo_preempt_thread;
static volatile int pygo_preempt_running = 0;
static volatile long pygo_preempt_quantum_us = 10000;
