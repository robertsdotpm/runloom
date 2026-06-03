/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


extern int runloom_mn_hub_count(void);   /* defined in mn_sched.c */

static int runloom_preempt_yield_cb(void *user)
{
    (void)user;
    /* If we're in a hub, yield via M:N path.  Otherwise check the
     * single-thread global scheduler.  Either way, this is a no-op
     * when no goroutine is currently running on this tstate. */
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
