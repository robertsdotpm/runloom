/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


extern int runloom_mn_hub_count(void);   /* defined in mn_sched.c */

static int runloom_preempt_yield_cb(void *user)
{
    (void)user;
    /* Defer the preempt yield while this tstate is mid object-destruction.  This
     * callback is posted by the time-slicer (runloom_preempt_main, Py_AddPendingCall)
     * and fires from _Py_HandlePending on whatever frame the eval loop is in --
     * which can be NESTED inside an in-flight tp_dealloc / finalizer / weakref
     * callback (the free-threaded biased-refcount merge drain and the trashcan
     * unwind both re-enter _Py_HandlePending).  Yielding there freezes a
     * half-finished destructor on the goroutine's coro stack; a concurrent
     * stop-the-world GC / QSBR reclaim then runs against partially-destroyed
     * objects -> use-after-free (the test_weakref class the scheduler invariant in
     * CLAUDE.md forbids).  The two M:N preempt yield sites already gate on this
     * (mn_sched_hub_resume_preempt.c.inc); the time-slicer reached the yield via a
     * SEPARATE path with NO gate (audit hang #1).  Leave the trigger armed -- the
     * next quantum re-posts once destruction unwinds, so it is deferred a few
     * frames, never lost.  Mirrors the liveness-backstop pending-call gate. */
    if (runloom_tstate_in_destruction(PyThreadState_GET())) {
        return 0;
    }
    /* If we're in a hub, yield via M:N path.  Otherwise check the
     * single-thread global scheduler.  Either way, this is a no-op
     * when no fiber is currently running on this tstate. */
    if (runloom_mn_yield_current()) {
        return 0;
    }
    {
        runloom_sched_t *s = runloom_sched_get();
        if (s->current != NULL) {
            runloom_sched_yield(s);
        }
    }
    return 0;
}

static RUNLOOM_THREAD_RET runloom_preempt_main(void *arg)
{
    (void)arg;
    while (runloom_preempt_running) {
        long us = runloom_preempt_quantum_us;
        int posts, i;

        if (us < 100) us = 100;        /* clamp lower bound */
        runloom_sleep_ns((long long)us * 1000LL);
        if (!runloom_preempt_running) break;

        /* Post one pending call per hub (or just one for single-thread)
         * so each hub's eval loop has something to pick up.  The
         * pending queue is shared per-interp; whichever hub drains
         * fastest gets the next one. */
        posts = runloom_mn_hub_count();
        if (posts < 1) posts = 1;
        for (i = 0; i < posts; i++) {
            /* Py_AddPendingCall is documented to be callable from any
             * thread without holding the GIL. */
            Py_AddPendingCall(runloom_preempt_yield_cb, NULL);
        }
    }
    RUNLOOM_THREAD_RETURN(NULL);
}

int runloom_preempt_init(long quantum_us)
{
    if (quantum_us <= 0) {
        PyErr_SetString(PyExc_ValueError, "quantum_us must be > 0");
        return -1;
    }
    /* Native mn-sim (MN_SIM_DST_PLAN.md I2, gate [8]): the wall-clock
     * time-slicer posts pending-call yields on an OS-timer schedule -- a
     * nondeterministic preemption source that would fuzz the seeded baton
     * schedule.  Refuse (no-op, log once) when the mn-sim path is opted in;
     * the deterministic frame-count preempt (RUNLOOM_MN_PREEMPT_FRAMES) is
     * the only preemption under the barrier.  Scoped to sim && RUNLOOM_SIM_MN
     * so the frozen H=1 sim plane and all non-sim users are untouched.
     * ORDER MATTERS (review): check the SIM_MN flag (plain getenv, no state)
     * BEFORE runloom_sim_enabled() -- the latter LATCHES its env read, so
     * calling it here on every non-sim preempt_init would freeze sim off for
     * a process that sets the env later in-process.  The
     * slicer-already-running ordering hole is closed on the other side:
     * mn_init's fence refuses a native mn-sim run while the slicer is live. */
    {
        const char *simmn = getenv("RUNLOOM_SIM_MN");
        if (simmn != NULL && simmn[0] != '\0' && simmn[0] != '0' &&
            runloom_sim_enabled()) {
            static int warned = 0;
            if (!warned) {
                warned = 1;
                fprintf(stderr, "runloom mn-sim: preempt_init is a no-op under "
                        "RUNLOOM_SIM_MN (wall-clock preemption is "
                        "nondeterministic; the frame-count preempt rules)\n");
            }
            return 0;
        }
    }
    runloom_preempt_quantum_us = quantum_us;
    if (runloom_preempt_running) {
        /* Already running -- the timer loop reloads quantum on its
         * next iteration. */
        return 0;
    }
    runloom_preempt_running = 1;
    if (runloom_thread_create(&runloom_preempt_thread,
                           runloom_preempt_main, NULL) != 0) {
        runloom_preempt_running = 0;
        PyErr_SetString(PyExc_OSError, "runloom preempt thread create failed");
        return -1;
    }
    return 0;
}

/* Is the time-slicer thread running?  mn_init's native mn-sim fence reads
 * this: a slicer started BEFORE the sim env was set would keep posting
 * nondeterministic yields into a seeded run (I2 review). */
int runloom_preempt_active(void)
{
    return runloom_preempt_running;
}

void runloom_preempt_fini(void)
{
    if (!runloom_preempt_running) return;
    runloom_preempt_running = 0;
    /* Release the GIL so the timer thread's final pending-call post
     * (if any) doesn't deadlock with our join.  The timer's sleep
     * doesn't need the GIL but Py_AddPendingCall briefly touches
     * shared state. */
    {
        PyThreadState *saved = PyEval_SaveThread();
        runloom_thread_join(runloom_preempt_thread);
        PyEval_RestoreThread(saved);
    }
}
