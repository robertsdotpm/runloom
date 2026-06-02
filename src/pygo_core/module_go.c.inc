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

    /* Parks the goroutine and runs py_blocking_worker on a pool thread;
     * returns once the worker has produced a result or exception.  Inline
     * fallback (not on a goroutine / pool unavailable) runs it here. */
    pygo_blocking_call(py_blocking_worker, &job);

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
    static char *kwlist[] = {"fn", "stack_size", NULL};
    pygo_sched_t *s;
    PygoG *handle;
    pygo_g_t *g;
    PyObject *cap, *callable;
    Py_ssize_t stack_size = -1;
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|n", kwlist,
                                     &callable, &stack_size)) {
        return NULL;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go(): callable required");
        return NULL;
    }
    s = pygo_sched_get();
    if (stack_size > 0) {
        cap = pygo_sched_spawn_sized(s, callable, (size_t)stack_size);
    } else {
        cap = pygo_sched_spawn(s, callable);
    }
    if (cap == NULL) return NULL;
    g = (pygo_g_t *)PyCapsule_GetPointer(cap, "pygo_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;

    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) {
        return NULL;
    }
    pygo_g_incref(g);   /* second ref for the Python wrapper */
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_go_noyield(PyObject *self, PyObject *callable)
{
    pygo_sched_t *s;
    PygoG *handle;
    pygo_g_t *g;
    PyObject *cap;
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go_noyield(): callable required");
        return NULL;
    }
    s = pygo_sched_get();
    cap = pygo_sched_spawn_noyield(s, callable);
    if (cap == NULL) return NULL;
    g = (pygo_g_t *)PyCapsule_GetPointer(cap, "pygo_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;

    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) return NULL;
    pygo_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_sched_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_yield(pygo_sched_get());
    Py_RETURN_NONE;
}

/* Signal the C scheduler to exit its drain loop at the next safe
 * point.  Used by pygo.aio.PygoEventLoop.run_until_complete to bail
 * out when the user-visible future is done, even if there are still
 * background goroutines parked on I/O (accept loops, etc). */
static PyObject *m_sched_stop(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    (void)self; (void)unused;
    s->stopping = 1;
    Py_RETURN_NONE;
}

/* Forcibly drop everything from the scheduler: clears the ready queue,
 * sleep heap, and (best-effort) any netpoll-parked goroutines.  Used
 * by paio.run's cleanup after the main future completes, so leftover
 * call_later runners / accept loops / ticker goroutines don't block
 * the next pygo_core.run() with a sleep heap that won't drain for
 * minutes.  Goroutines being dropped have their coros marked done +
 * freed; any user-visible Python references (G handles) will report
 * done=True.
 *
 * NOTE: per-frame localsplus refs on the goroutines' datastack chunks
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
    pygo_sched_t *s = pygo_sched_get();
    int n_ready = 0, n_sleep = 0, n_parked;
    (void)self; (void)unused;

    /* Wake netpoll-parked goroutines with ready_mask=-1 first.  This
     * pushes them back to ready; we then drain ready below. */
    n_parked = pygo_netpoll_drain_parked();

    /* Drain ready queue. */
    while (!pygo_sched_ready_empty(s)) {
        pygo_g_t *g = pygo_sched_ready_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            pygo_g_decref(g);
            n_ready++;
        }
    }
    /* Drain sleep heap. */
    while (s->sleep_size > 0) {
        pygo_g_t *g = pygo_sched_sleep_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            pygo_g_decref(g);
            n_sleep++;
        }
    }
    return Py_BuildValue("(iii)", n_ready, n_sleep, n_parked);
}

/* Park the current goroutine until G.wake() is called on it.
 * Race-safe: a wake that arrives before the park (sync callback firing
 * from add_done_callback) makes this a no-op.  Used by pygo.aio's
 * PygoTask to replace the per-task Chan(1) wake mechanism. */
static PyObject *m_park_self(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_park_safe();
    Py_RETURN_NONE;
}

/* Return a G handle to the currently-running goroutine, or None if
 * called outside any goroutine.  Used by pygo.aio's PygoTask driver
 * to capture its own handle for wake-from-callback. */
static PyObject *m_current_g(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    pygo_g_t *g;
    PygoG *handle;
    (void)self; (void)unused;

    g = s->current;
    if (g == NULL) {
        Py_RETURN_NONE;
    }
    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) return NULL;
    pygo_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
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
 * We expose a singleton instance as `pygo_core.sched_yield_fast` (and
 * also assign it to `pygo_core.sched_yield` so existing call sites
 * pick it up transparently).  The METH_NOARGS PyCFunction stays
 * available under `sched_yield_classic` for benchmarking comparisons. */
typedef struct {
    PyObject_HEAD
    vectorcallfunc vectorcall;
} PygoYielder;

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
    pygo_sched_yield(pygo_sched_get());
    Py_RETURN_NONE;
}

static PyTypeObject PygoYielderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core._Yielder",
    .tp_basicsize = sizeof(PygoYielder),
    .tp_vectorcall_offset = offsetof(PygoYielder, vectorcall),
    .tp_call = PyVectorcall_Call,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL,
    .tp_doc = "Singleton wrapper exposing sched_yield via vectorcall.",
};


