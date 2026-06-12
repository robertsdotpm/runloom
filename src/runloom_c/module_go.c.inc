/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static PyObject *m_blocking(PyObject *self, PyObject *args, PyObject *kw)
{
    py_blocking_job_t job;
    PyObject *fn, *call_args;
    (void)self;

    if (PyTuple_GET_SIZE(args) < 1) {
        PyErr_SetString(PyExc_TypeError,
                        "blocking() requires a callable as the first argument");
        return NULL;
    }
    fn = PyTuple_GET_ITEM(args, 0);
    if (!PyCallable_Check(fn)) {
        PyErr_SetString(PyExc_TypeError, "blocking(): first argument must be callable");
        return NULL;
    }
    call_args = PyTuple_GetSlice(args, 1, PyTuple_GET_SIZE(args));
    if (call_args == NULL) return NULL;

    job.fn       = fn;
    job.args     = call_args;
    job.kwargs   = kw;            /* NULL if no kwargs */
    job.result   = NULL;
    job.exc_type = job.exc_value = job.exc_tb = NULL;

    /* Parks the fiber and runs py_blocking_worker on a pool thread;
     * returns once the worker has produced a result or exception.  Inline
     * fallback (not on a fiber / pool unavailable) runs it here. */
    runloom_blocking_call(py_blocking_worker, &job);

    Py_DECREF(call_args);

    if (job.result == NULL) {
        if (job.exc_type != NULL) {
            PyErr_Restore(job.exc_type, job.exc_value, job.exc_tb);
        } else {
            PyErr_SetString(PyExc_RuntimeError,
                            "blocking(): call produced neither result nor exception");
        }
        return NULL;
    }
    return job.result;
}

static PyObject *m_go(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"fn", "stack_size", "fifo", NULL};
    runloom_sched_t *s;
    RunloomG *handle;
    runloom_g_t *g;
    PyObject *cap, *callable;
    Py_ssize_t stack_size = -1;
    int fifo = 0;
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|np", kwlist,
                                     &callable, &stack_size, &fifo)) {
        return NULL;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go(): callable required");
        return NULL;
    }
    s = runloom_sched_get();
    if (stack_size > 0) {
        cap = runloom_sched_spawn_sized(s, callable, (size_t)stack_size);
    } else {
        cap = runloom_sched_spawn(s, callable);
    }
    if (cap == NULL) return NULL;
    g = (runloom_g_t *)PyCapsule_GetPointer(cap, "runloom_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;
    /* fifo=True: PCT must keep this g in spawn order vs other FIFO gs (asyncio
     * call_soon-FIFO; see pct_fifo).  No effect unless RUNLOOM_PCT_SEED is set. */
    if (fifo) g->pct_fifo = 1;

    handle = PyObject_New(RunloomG, &RunloomGType);
    if (handle == NULL) {
        return NULL;
    }
    runloom_g_incref(g);   /* second ref for the Python wrapper */
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_go_noyield(PyObject *self, PyObject *callable)
{
    runloom_sched_t *s;
    RunloomG *handle;
    runloom_g_t *g;
    PyObject *cap;
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go_noyield(): callable required");
        return NULL;
    }
    s = runloom_sched_get();
    cap = runloom_sched_spawn_noyield(s, callable);
    if (cap == NULL) return NULL;
    g = (runloom_g_t *)PyCapsule_GetPointer(cap, "runloom_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;

    handle = PyObject_New(RunloomG, &RunloomGType);
    if (handle == NULL) return NULL;
    runloom_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_sched_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_sched_yield(runloom_sched_get());
    Py_RETURN_NONE;
}

/* Signal the C scheduler to exit its drain loop at the next safe
 * point.  Used by runloom.aio.RunloomEventLoop.run_until_complete to bail
 * out when the user-visible future is done, even if there are still
 * background fibers parked on I/O (accept loops, etc). */
static PyObject *m_sched_stop(PyObject *self, PyObject *unused)
{
    runloom_sched_t *s = runloom_sched_get();
    (void)self; (void)unused;
    s->stopping = 1;
    Py_RETURN_NONE;
}

/* Forcibly drop everything from the scheduler: clears the ready queue,
 * sleep heap, and (best-effort) any netpoll-parked fibers.  Used
 * by paio.run's cleanup after the main future completes, so leftover
 * call_later runners / accept loops / ticker fibers don't block
 * the next runloom_c.run() with a sleep heap that won't drain for
 * minutes.  Goroutines being dropped have their coros marked done +
 * freed; any user-visible Python references (G handles) will report
 * done=True.
 *
 * NOTE: per-frame localsplus refs on the fibers' datastack chunks
 * are not unwound.  A correct unwind would either walk the frame
 * chain and call _PyFrame_Clear on each (CPython internal API; layout
 * changes per minor version), or inject SystemExit and resume the
 * coro so the eval loop's exception cascade unwinds the frames
 * naturally.  The latter was tried and runs user code in a
 * destruction context where __exit__ / finally / done-callbacks have
 * unpredictable side effects on subsequent paio.run cycles -- it left
 * SystemExit visible in tests that ran later in the same process.
 * Reviewer item 6 in HANDOFF.md; resolving it cleanly needs a
 * frame-walker hardened across 3.11/3.12/3.13t. */
static PyObject *m_sched_reset(PyObject *self, PyObject *unused)
{
    runloom_sched_t *s = runloom_sched_get();
    int n_ready = 0, n_sleep = 0, n_parked;
    (void)self; (void)unused;

    /* Wake netpoll-parked fibers with ready_mask=-1 first.  This
     * pushes them back to ready; we then drain ready below. */
    n_parked = runloom_netpoll_drain_parked();

    /* Drain ready queue. */
    while (!runloom_sched_ready_empty(s)) {
        runloom_g_t *g = runloom_sched_ready_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            runloom_g_decref(g);
            n_ready++;
        }
    }
    /* Drain sleep heap. */
    while (s->sleep_size > 0) {
        runloom_g_t *g = runloom_sched_sleep_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            runloom_g_decref(g);
            n_sleep++;
        }
    }
    return Py_BuildValue("(iii)", n_ready, n_sleep, n_parked);
}

/* Park the current fiber until G.wake() is called on it.
 * Race-safe: a wake that arrives before the park (sync callback firing
 * from add_done_callback) makes this a no-op.  Used by runloom.aio's
 * RunloomTask to replace the per-task Chan(1) wake mechanism. */
static PyObject *m_park_self(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_sched_park_safe();
    Py_RETURN_NONE;
}

/* park(foreign_wakeable=False): park the current fiber in memory (NO fd),
 * M:N-correctly.  Unlike park_self (single-thread sched only -- it busy-loops on
 * a hub), this routes by hub presence.  Woken by the g's handle .wake() (which
 * routes by how it parked).  foreign_wakeable=True keeps a single-thread run()
 * alive for a foreign-OS-thread waker.  No-op outside a fiber. */
static PyObject *m_park(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"foreign_wakeable", "timeout", NULL};
    int fw = 0;
    PyObject *timeout_obj = Py_None;
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|pO", kwlist, &fw, &timeout_obj))
        return NULL;
    if (timeout_obj == Py_None) {
        /* Untimed in-memory park (0 fds).  Returns None (backward compatible). */
        runloom_park_generic(fw);
        Py_RETURN_NONE;
    }
    {
        /* Timed in-memory park (0 fds): wake at a real (monotonic) deadline OR on
         * g.wake().  Returns True if it timed out, False if woken.  A spurious
         * early False is possible (real wake / a stale timer entry); the caller
         * re-checks its own deadline, exactly as the wait_fd(timeout) path did. */
        double timeout = PyFloat_AsDouble(timeout_obj);
        double deadline;
        int r;
        if (timeout == -1.0 && PyErr_Occurred()) return NULL;
        if (timeout < 0.0) timeout = 0.0;
        deadline = runloom_sched_monotonic_seconds() + timeout;
        r = runloom_park_generic_timed(fw, deadline);
        if (r < 0) {
            if (PyErr_Occurred()) return NULL;          /* OOM (PyErr set) */
            PyErr_SetString(PyExc_RuntimeError,
                            "park(timeout=...): not running on a fiber");
            return NULL;
        }
        if (r) Py_RETURN_TRUE;                          /* timed out */
        Py_RETURN_FALSE;                                /* woken */
    }
}

/* Return a G handle to the currently-running fiber, or None if
 * called outside any fiber.  Used by runloom.aio's RunloomTask driver
 * to capture its own handle for wake-from-callback. */
static PyObject *m_current_g(PyObject *self, PyObject *unused)
{
    runloom_g_t *g;
    RunloomG *handle;
    (void)self; (void)unused;

    /* On an M:N hub the running fiber lives in the hub's TLS, NOT in
     * the single-thread scheduler's ->current -- check it first so
     * current_g() works under mn_run too.  Without this it returned None
     * on every hub, which broke CoRLock owner identity (self-deadlock) and
     * anything else keying off runloom.current() under M:N. */
    if (runloom_mn_current_hub_opaque() != NULL) {
        g = runloom_mn_tls_current_g();
    } else {
        /* Foreign-thread-safe PEEK, never runloom_sched_get(): current_g() is
         * reached from arbitrary OS threads via the monkey shims'
         * _in_fiber() (e.g. a multiprocessing.Queue feeder thread taking a
         * patched threading.Lock).  runloom_sched_get() would lazily malloc a
         * single-thread scheduler AND arm the netpoll wake-pump on that foreign
         * thread -- unsafe under concurrent M:N (raced -> SIGSEGV) and a leak.
         * A thread with no scheduler has no running fiber, so NULL is the
         * correct, allocation-free answer. */
        g = runloom_sched_peek_current();
    }
    if (g == NULL) {
        Py_RETURN_NONE;
    }
    handle = PyObject_New(RunloomG, &RunloomGType);
    if (handle == NULL) return NULL;
    runloom_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
}

/* unpark_many(handles) -> list[int] : directly wake a batch of fibers parked
 * in wait_fd (the wake side of a fan-in primitive -- Event.set / notify_all /
 * Semaphore.release).  `handles` is a sequence of G objects (each a waiter's
 * current_g()).  Claims+re-queues each parked g in one C call, replacing the
 * per-waiter os.write(pipe) -> epoll -> drain round-trip.  Returns the list of
 * INDICES into `handles` that could NOT be direct-woken (the waiter has not yet
 * committed its wait_fd park -- the edge-before-park window); the caller falls
 * back to a pipe-write for exactly those.  A G whose ->g already completed is
 * skipped (not reported missed). */
static PyObject *m_unpark_many(PyObject *self, PyObject *seq)
{
    Py_ssize_t n, i;
    runloom_g_t **gs;
    int *missed;
    int nmissed;
    PyObject *fast, *result;
    (void)self;

    fast = PySequence_Fast(seq, "unpark_many() requires a sequence of G handles");
    if (fast == NULL) return NULL;
    n = PySequence_Fast_GET_SIZE(fast);
    if (n == 0) {
        Py_DECREF(fast);
        return PyList_New(0);
    }
    gs = (runloom_g_t **)PyMem_Malloc((size_t)n * sizeof(runloom_g_t *));
    missed = (int *)PyMem_Malloc((size_t)n * sizeof(int));
    if (gs == NULL || missed == NULL) {
        PyMem_Free(gs); PyMem_Free(missed); Py_DECREF(fast);
        return PyErr_NoMemory();
    }
    for (i = 0; i < n; i++) {
        PyObject *item = PySequence_Fast_GET_ITEM(fast, i);   /* borrowed */
        if (!PyObject_TypeCheck(item, &RunloomGType)) {
            PyMem_Free(gs); PyMem_Free(missed); Py_DECREF(fast);
            PyErr_SetString(PyExc_TypeError,
                            "unpark_many() items must be G handles");
            return NULL;
        }
        gs[i] = ((RunloomG *)item)->g;   /* NULL if g completed -> skipped */
    }
    /* No GIL juggling: the wake path's mutual exclusion is the per-pool
     * runloom_mutex (taken inside unpark_many), the same locks the pump uses
     * independent of the GIL; under free-threaded 3.13t there is no GIL anyway. */
    nmissed = runloom_netpoll_unpark_many(gs, (int)n, missed);

    result = PyList_New(nmissed);
    if (result != NULL) {
        for (i = 0; i < nmissed; i++) {
            PyObject *idx = PyLong_FromLong((long)missed[i]);
            if (idx == NULL) { Py_CLEAR(result); break; }
            PyList_SET_ITEM(result, i, idx);   /* steals idx */
        }
    }
    PyMem_Free(gs); PyMem_Free(missed); Py_DECREF(fast);
    return result;
}

/* Stack high-water-mark (bytes) of the currently-running fiber, page-
 * granular, measured paint-free via mincore (runloom_coro_scan_hwm).  Returns
 * 0 outside a fiber, or where the backend has no introspectable stack
 * (Windows Fibers) -- callers treat 0 as "unknown, keep the current size".
 *
 * This is the read half of the function-bound grow-down: a fiber calls it
 * just before returning to learn how deep it actually went, then writes the
 * derived size back onto its own callable so the next spawn reserves less. */
static PyObject *m_current_g_hwm(PyObject *self, PyObject *unused)
{
    runloom_g_t *g;
    (void)self; (void)unused;

    if (runloom_mn_current_hub_opaque() != NULL) {
        g = runloom_mn_tls_current_g();
    } else {
        g = runloom_sched_peek_current();
    }
    if (g == NULL) {
        return PyLong_FromLong(0);
    }
    return PyLong_FromSize_t(runloom_coro_scan_hwm(g->coro));
}

/* Vectorcall fast-dispatch version of sched_yield.
 *
 * METH_NOARGS for a module-level function in CPython 3.12+ specializes
 * the bytecode CALL to CALL_BUILTIN_O which is fast, but goes through
 * PyCFunction_NewEx / etc. layers.  A type with tp_vectorcall_offset
 * exposes its instances' vectorcallfunc directly, so the interpreter
 * can branch straight to the C function pointer with zero argument-
 * unpacking overhead.
 *
 * We expose a singleton instance as `runloom_c.sched_yield_fast` (and
 * also assign it to `runloom_c.sched_yield` so existing call sites
 * pick it up transparently).  The METH_NOARGS PyCFunction stays
 * available under `sched_yield_classic` for benchmarking comparisons. */
typedef struct {
    PyObject_HEAD
    vectorcallfunc vectorcall;
} RunloomYielder;

static PyObject *yielder_vectorcall(PyObject *self,
                                    PyObject *const *args,
                                    size_t nargsf,
                                    PyObject *kwnames)
{
    (void)self;
    (void)args;
    if (PyVectorcall_NARGS(nargsf) != 0 || kwnames != NULL) {
        PyErr_SetString(PyExc_TypeError, "yielder takes no arguments");
        return NULL;
    }
    runloom_sched_yield(runloom_sched_get());
    Py_RETURN_NONE;
}

static PyTypeObject RunloomYielderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "runloom_c._Yielder",
    .tp_basicsize = sizeof(RunloomYielder),
    .tp_vectorcall_offset = offsetof(RunloomYielder, vectorcall),
    .tp_call = PyVectorcall_Call,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL,
    .tp_doc = "Singleton wrapper exposing sched_yield via vectorcall.",
};


