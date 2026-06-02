/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* Entry function: runs inside the coroutine stack.  Note: we are
 * already executing Python C code on the OS thread that owns the GIL
 * (on GIL-builds), so we can call Py_* freely.  Under free-threaded
 * builds, the calling thread state is still valid here. */
static void pygo_coro_python_entry(void *user)
{
    PygoCoro *self = (PygoCoro *)user;
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

static int PygoCoro_init(PygoCoro *self, PyObject *args, PyObject *kw)
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
    Py_INCREF(callable);
    self->callable = callable;
    self->result = NULL;
    self->error = NULL;
    self->has_run = 0;
    self->coro = pygo_coro_new((size_t)stack_size,
                               pygo_coro_python_entry,
                               self);
    if (self->coro == NULL) {
        PyErr_SetString(PyExc_MemoryError, "pygo_coro_new failed");
        return -1;
    }
    return 0;
}

static void PygoCoro_dealloc(PygoCoro *self)
{
    if (self->coro != NULL) {
        pygo_coro_destroy(self->coro);
        self->coro = NULL;
    }
    Py_XDECREF(self->callable);
    Py_XDECREF(self->result);
    Py_XDECREF(self->error);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoCoro_resume(PygoCoro *self, PyObject *unused)
{
    PygoTstateSnapshot caller_snap;
    (void)unused;
    if (self->coro == NULL || pygo_coro_done(self->coro)) {
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
     * on the OS thread, so a sufficiently-long pygo.run() hits
     * RecursionError. */
    pygo_tstate_save(&caller_snap);
    if (self->tstate_snap.initialised) {
        pygo_tstate_restore(&self->tstate_snap);
    }

    pygo_coro_resume(self->coro);

    /* Snapshot the coro's tstate at the yield/return point, then
     * restore the caller's counters. */
    pygo_tstate_save(&self->tstate_snap);
    pygo_tstate_restore(&caller_snap);

    if (pygo_coro_done(self->coro) && self->error != NULL) {
        PyObject *err = self->error;
        self->error = NULL;
        PyErr_SetObject((PyObject *)Py_TYPE(err), err);
        Py_DECREF(err);
        return NULL;
    }
    if (pygo_coro_done(self->coro) && self->result != NULL) {
        Py_INCREF(self->result);
        return self->result;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *PygoCoro_done_get(PygoCoro *self, void *closure)
{
    (void)closure;
    if (self->coro == NULL || pygo_coro_done(self->coro)) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *PygoCoro_result_get(PygoCoro *self, void *closure)
{
    (void)closure;
    if (self->result == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->result);
    return self->result;
}

static PyMethodDef PygoCoro_methods[] = {
    {"resume", (PyCFunction)PygoCoro_resume, METH_NOARGS,
     "Resume the coroutine.  Returns when it yields or returns."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoCoro_getset[] = {
    {"done",   (getter)PygoCoro_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)PygoCoro_result_get, NULL, "Return value, if any",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoCoroType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pygo_core.Coro",          /* tp_name */
    sizeof(PygoCoro),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)PygoCoro_dealloc, /* tp_dealloc */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Stackful coroutine.",     /* tp_doc */
    0, 0, 0, 0, 0, 0,
    PygoCoro_methods,          /* tp_methods */
    0,                         /* tp_members */
    PygoCoro_getset,           /* tp_getset */
    0, 0, 0, 0, 0,
    (initproc)PygoCoro_init,   /* tp_init */
    0,
    PyType_GenericNew,         /* tp_new */
};
