/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* ============================================================ */
/* PygoChan -- Go-style channel                                 */
/* ============================================================ */
typedef struct {
    PyObject_HEAD
    pygo_chan_t *ch;
} PygoChan;

static int PygoChan_init(PygoChan *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"capacity", NULL};
    Py_ssize_t cap = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|n", kwlist, &cap)) {
        return -1;
    }
    self->ch = pygo_chan_new(cap);
    if (self->ch == NULL) return -1;
    return 0;
}

static void PygoChan_dealloc(PygoChan *self)
{
    if (self->ch != NULL) {
        pygo_chan_decref(self->ch);
        self->ch = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoChan_send(PygoChan *self, PyObject *value)
{
    if (pygo_chan_send(self->ch, value) < 0) return NULL;
    Py_RETURN_NONE;
}

static PyObject *PygoChan_try_send(PygoChan *self, PyObject *value)
{
    int r = pygo_chan_try_send(self->ch, value);
    if (r < 0) return NULL;
    if (r == 0) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoChan_recv(PygoChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    v = pygo_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;
    /* Return (value, ok) matching Go's `v, ok := <-ch` idiom. */
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);                     /* steals ref */
        PyTuple_SET_ITEM(tup, 1, PyBool_FromLong(ok));
        return tup;
    }
}

static PyObject *PygoChan_try_recv(PygoChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    if (pygo_chan_try_recv(self->ch, &v, &ok) < 0) return NULL;
    if (v == NULL) {
        /* Would-block. */
        Py_RETURN_NONE;
    }
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);
        PyTuple_SET_ITEM(tup, 1, PyBool_FromLong(ok));
        return tup;
    }
}

static PyObject *PygoChan_close(PygoChan *self, PyObject *unused)
{
    (void)unused;
    if (pygo_chan_close(self->ch) < 0) return NULL;
    Py_RETURN_NONE;
}

/* Iterator protocol: `for v in ch:` calls recv() repeatedly and stops
 * on close.  Matches Go's `for v := range ch { ... }`. */
static PyObject *PygoChan_iter(PygoChan *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *PygoChan_iternext(PygoChan *self)
{
    int ok;
    PyObject *v = pygo_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;            /* error */
    if (!ok) {
        /* Channel closed and empty -> end iteration. */
        Py_DECREF(v);                       /* v was Py_None */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
    return v;                               /* new ref to caller */
}

static PyObject *PygoChan_closed_get(PygoChan *self, void *closure)
{
    (void)closure;
    if (pygo_chan_is_closed(self->ch)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoChan_cap_get(PygoChan *self, void *closure)
{
    (void)closure;
    return PyLong_FromSsize_t(pygo_chan_cap(self->ch));
}

static Py_ssize_t PygoChan_len(PygoChan *self)
{
    return pygo_chan_len(self->ch);
}

static PyMethodDef PygoChan_methods[] = {
    {"send", (PyCFunction)PygoChan_send, METH_O,
     "Send a value, blocking until delivered."},
    {"try_send", (PyCFunction)PygoChan_try_send, METH_O,
     "Try to send.  Returns True if delivered, False if would-block."},
    {"recv", (PyCFunction)PygoChan_recv, METH_NOARGS,
     "Receive (value, ok); blocks until a value or close.  ok=False "
     "means the channel was closed and empty (Go's `v, ok := <-ch`)."},
    {"try_recv", (PyCFunction)PygoChan_try_recv, METH_NOARGS,
     "Try to receive.  Returns (value, ok) on success, None if "
     "would-block."},
    {"close", (PyCFunction)PygoChan_close, METH_NOARGS,
     "Mark the channel closed.  Wakes all parked senders + receivers."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoChan_getset[] = {
    {"closed",   (getter)PygoChan_closed_get,   NULL, "True after close()", NULL},
    {"capacity", (getter)PygoChan_cap_get,      NULL, "buffered capacity", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PySequenceMethods PygoChan_seq = {
    (lenfunc)PygoChan_len,            /* sq_length */
    0, 0, 0, 0, 0, 0, 0, 0, 0,
};

static PyTypeObject PygoChanType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core.Chan",
    .tp_basicsize = sizeof(PygoChan),
    .tp_dealloc = (destructor)PygoChan_dealloc,
    .tp_as_sequence = &PygoChan_seq,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Go-style channel.  Chan(capacity=0).",
    .tp_iter = (getiterfunc)PygoChan_iter,
    .tp_iternext = (iternextfunc)PygoChan_iternext,
    .tp_methods = PygoChan_methods,
    .tp_getset = PygoChan_getset,
    .tp_init = (initproc)PygoChan_init,
    .tp_new = PyType_GenericNew,
};

/* ---- blocking(): run a Python callable on the blocking-offload pool ----
 *
 * Generalises the C-level getaddrinfo offload to any user-named blocking
 * call: blocking(fn, *args, **kwargs) parks the calling goroutine and runs
 * fn(*args, **kwargs) on a pool thread, so a blocking stdlib/C-extension
 * call doesn't wedge the goroutine's hub.  The pool worker has no thread
 * state of its own, so it PyGILState_Ensure()s one to run fn; fn's own
 * blocking section releases the GIL (as socket / time.sleep / blocking C
 * extensions do), which is what lets other goroutines keep running.
 *
 * fn runs OFF any goroutine, so it must not call pygo scheduler ops
 * (sched_yield, channels, wait_fd, ...) -- it is for plain blocking work. */
typedef struct {
    PyObject *fn;
    PyObject *args;        /* call args tuple (owned by m_blocking's frame) */
    PyObject *kwargs;      /* call kwargs dict or NULL */
    PyObject *result;      /* out: new ref, or NULL on exception */
    PyObject *exc_type;    /* out: captured + normalised exception (or NULL) */
    PyObject *exc_value;
    PyObject *exc_tb;
} py_blocking_job_t;

static void *py_blocking_worker(void *p)
{
    py_blocking_job_t *j = (py_blocking_job_t *)p;
    /* Ensure a thread state for this pool thread (reentrant-safe if the
     * call ran inline on a thread that already holds the GIL). */
    PyGILState_STATE st = PyGILState_Ensure();
    j->result = PyObject_Call(j->fn, j->args, j->kwargs);
    if (j->result == NULL) {
        PyErr_Fetch(&j->exc_type, &j->exc_value, &j->exc_tb);
        PyErr_NormalizeException(&j->exc_type, &j->exc_value, &j->exc_tb);
    }
    PyGILState_Release(st);
    return NULL;
}

