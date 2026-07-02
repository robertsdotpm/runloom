/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* Entry function: runs inside the coroutine stack.  Note: we are
 * already executing Python C code on the OS thread that owns the GIL
 * (on GIL-builds), so we can call Py_* freely.  Under free-threaded
 * builds, the calling thread state is still valid here. */
static void runloom_coro_python_entry(void *user)
{
    RunloomCoro *self = (RunloomCoro *)user;
    PyObject *res;
    res = PyObject_CallNoArgs(self->callable);
    if (res == NULL) {
        /* The exception is set; capture and clear so the resumer sees it. */
        PyObject *type, *value, *tb;
        PyErr_Fetch(&type, &value, &tb);
        PyErr_NormalizeException(&type, &value, &tb);
        if (value == NULL) {
            value = Py_None; Py_INCREF(value);
        }
        if (tb != NULL) {
            PyException_SetTraceback(value, tb);
            Py_DECREF(tb);
        }
        Py_XDECREF(type);
        self->error = value;
    } else {
        self->result = res;
    }
}

static int RunloomCoro_init(RunloomCoro *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"callable", "stack_size", NULL};
    PyObject *callable;
    Py_ssize_t stack_size = 131072; /* 128 KB default */
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|n", kwlist,
                                     &callable, &stack_size)) {
        return -1;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "callable must be callable");
        return -1;
    }
    if (stack_size < 0) {
        /* A negative size casts to a huge size_t and overflows the guard-page
         * arithmetic in runloom_coro_new -> an undersized mapping + OOB write
         * (SIGSEGV).  go() / set_stack_size() reject non-positive sizes; Coro
         * must validate too rather than crash on a bad argument. */
        PyErr_SetString(PyExc_ValueError, "stack_size must be non-negative");
        return -1;
    }
    /* tp_init is re-invocable from Python (c.__init__(g)).  Refuse to do it
     * while the fiber is live -- freeing its stack out from under a running
     * swapcontext (another thread, or a re-entrant call from the coro body)
     * would crash. */
    if (__atomic_load_n(&self->executing, __ATOMIC_ACQUIRE) != 0) {
        PyErr_SetString(PyExc_RuntimeError,
                        "cannot re-initialise a Coro while it is executing");
        return -1;
    }
    /* Release whatever a previous init left on this instance before we
     * overwrite the fields, or we leak the old callable/result/error refs
     * and -- the costly part -- the previous coro's guard-paged stack mmap.
     * On a fresh instance every field is NULL/zero (tp_alloc), so these are
     * no-ops the first time through. */
    if (self->coro != NULL) {
        runloom_coro_destroy(self->coro);
        self->coro = NULL;
    }
    Py_CLEAR(self->callable);
    Py_CLEAR(self->result);
    Py_CLEAR(self->error);
    self->tstate_snap.initialised = 0;

    Py_INCREF(callable);
    self->callable = callable;
    self->has_run = 0;
    self->executing = 0;
    self->coro = runloom_coro_new(
                               runloom_fiber_stack_floor((size_t)stack_size),
                               runloom_coro_python_entry,
                               self);
    if (self->coro == NULL) {
        PyErr_SetString(PyExc_MemoryError, "runloom_coro_new failed");
        return -1;
    }
    return 0;
}

static void RunloomCoro_dealloc(RunloomCoro *self)
{
    if (self->coro != NULL) {
        runloom_coro_destroy(self->coro);
        self->coro = NULL;
    }
    Py_XDECREF(self->callable);
    Py_XDECREF(self->result);
    Py_XDECREF(self->error);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomCoro_resume(RunloomCoro *self, PyObject *unused)
{
    RunloomTstateSnapshot caller_snap;
    (void)unused;
    /* Atomically claim the "executing" flag before touching the fiber.  A
     * plain check-then-set races on free-threaded 3.13t: two threads could
     * both observe executing==0 and both swapcontext into the same live C
     * stack -> corruption / guard-page SIGSEGV.  __atomic_exchange_n lets
     * exactly one caller win; every other -- including a re-entrant resume
     * from the coro's own body, which would swapcontext into a context
     * already live on this stack -- sees the prior 1 and is rejected
     * cleanly.  The claim is held across the swap and the tstate_snap
     * save/restore below, and released once we are safely back. */
    if (__atomic_exchange_n(&self->executing, 1, __ATOMIC_ACQ_REL) != 0) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Coro is already executing (re-entrant resume)");
        return NULL;
    }
    if (self->coro == NULL || runloom_coro_done(self->coro)) {
        __atomic_store_n(&self->executing, 0, __ATOMIC_RELEASE);
        if (self->error != NULL) {
            PyObject *err = self->error;
            self->error = NULL;
            PyErr_SetObject((PyObject *)Py_TYPE(err), err);
            Py_DECREF(err);
            return NULL;
        }
        Py_INCREF(Py_None);
        return Py_None;
    }
    self->has_run = 1;

    /* Save the caller's (scheduler's) thread-state recursion counters,
     * then restore the coro's snapshot if it has one.  After
     * swapcontext returns (the coro yielded or finished), save its
     * counters into our snapshot and restore the caller's.  Without
     * this, each yield permanently decrements py_recursion_remaining
     * on the OS thread, so a sufficiently-long runloom.run() hits
     * RecursionError. */
    runloom_tstate_save(&caller_snap);
    if (self->tstate_snap.initialised) {
        runloom_tstate_restore(&self->tstate_snap);
    }

    /* executing was claimed atomically above; hold it across the swap and
     * the tstate save/restore so a racing thread can neither re-enter the
     * fiber nor observe a half-written tstate_snap. */
    runloom_coro_resume(self->coro);

    /* Snapshot the coro's tstate at the yield/return point, then
     * restore the caller's counters. */
    runloom_tstate_save(&self->tstate_snap);
    runloom_tstate_restore(&caller_snap);
    __atomic_store_n(&self->executing, 0, __ATOMIC_RELEASE);

    if (runloom_coro_done(self->coro) && self->error != NULL) {
        PyObject *err = self->error;
        self->error = NULL;
        PyErr_SetObject((PyObject *)Py_TYPE(err), err);
        Py_DECREF(err);
        return NULL;
    }
    if (runloom_coro_done(self->coro) && self->result != NULL) {
        Py_INCREF(self->result);
        return self->result;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *RunloomCoro_done_get(RunloomCoro *self, void *closure)
{
    (void)closure;
    if (self->coro == NULL || runloom_coro_done(self->coro)) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *RunloomCoro_result_get(RunloomCoro *self, void *closure)
{
    (void)closure;
    if (self->result == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->result);
    return self->result;
}

static PyMethodDef RunloomCoro_methods[] = {
    {"resume", (PyCFunction)RunloomCoro_resume, METH_NOARGS,
     "Resume the coroutine.  Returns when it yields or returns."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef RunloomCoro_getset[] = {
    {"done",   (getter)RunloomCoro_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)RunloomCoro_result_get, NULL, "Return value, if any",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject RunloomCoroType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "runloom_c.Coro",          /* tp_name */
    sizeof(RunloomCoro),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)RunloomCoro_dealloc, /* tp_dealloc */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Stackful coroutine.",     /* tp_doc */
    0, 0, 0, 0, 0, 0,
    RunloomCoro_methods,          /* tp_methods */
    0,                         /* tp_members */
    RunloomCoro_getset,           /* tp_getset */
    0, 0, 0, 0, 0,
    (initproc)RunloomCoro_init,   /* tp_init */
    0,
    PyType_GenericNew,         /* tp_new */
};
