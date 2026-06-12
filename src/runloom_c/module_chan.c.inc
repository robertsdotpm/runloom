/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* ============================================================ */
/* RunloomChan -- Go-style channel                                 */
/* ============================================================ */
typedef struct {
    PyObject_HEAD
    runloom_chan_t *ch;
} RunloomChan;

static int RunloomChan_init(RunloomChan *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"capacity", NULL};
    Py_ssize_t cap = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|n", kwlist, &cap)) {
        return -1;
    }
    self->ch = runloom_chan_new(cap);
    if (self->ch == NULL) return -1;
    return 0;
}

static void RunloomChan_dealloc(RunloomChan *self)
{
    if (self->ch != NULL) {
        runloom_chan_decref(self->ch);
        self->ch = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomChan_send(RunloomChan *self, PyObject *value)
{
    if (runloom_chan_send(self->ch, value) < 0) return NULL;
    Py_RETURN_NONE;
}

static PyObject *RunloomChan_try_send(RunloomChan *self, PyObject *value)
{
    int r = runloom_chan_try_send(self->ch, value);
    if (r < 0) return NULL;
    if (r == 0) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomChan_recv(RunloomChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    v = runloom_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;
    /* Return (value, ok) matching Go's `v, ok := <-ch` idiom. */
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);                     /* steals ref */
        /* inline the bool instead of an out-of-line PyBool_FromLong call;
         * Py_True/Py_False are immortal singletons (perf finding F6b). */
        PyTuple_SET_ITEM(tup, 1, Py_NewRef(ok ? Py_True : Py_False));
        return tup;
    }
}

static PyObject *RunloomChan_try_recv(RunloomChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    if (runloom_chan_try_recv(self->ch, &v, &ok) < 0) return NULL;
    if (v == NULL) {
        /* Would-block. */
        Py_RETURN_NONE;
    }
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);
        PyTuple_SET_ITEM(tup, 1, Py_NewRef(ok ? Py_True : Py_False));
        return tup;
    }
}

static PyObject *RunloomChan_close(RunloomChan *self, PyObject *unused)
{
    (void)unused;
    if (runloom_chan_close(self->ch) < 0) return NULL;
    Py_RETURN_NONE;
}

/* Iterator protocol: `for v in ch:` calls recv() repeatedly and stops
 * on close.  Matches Go's `for v := range ch { ... }`. */
static PyObject *RunloomChan_iter(RunloomChan *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *RunloomChan_iternext(RunloomChan *self)
{
    int ok;
    PyObject *v = runloom_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;            /* error */
    if (!ok) {
        /* Channel closed and empty -> end iteration. */
        Py_DECREF(v);                       /* v was Py_None */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
    return v;                               /* new ref to caller */
}

static PyObject *RunloomChan_closed_get(RunloomChan *self, void *closure)
{
    (void)closure;
    if (runloom_chan_is_closed(self->ch)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomChan_cap_get(RunloomChan *self, void *closure)
{
    (void)closure;
    return PyLong_FromSsize_t(runloom_chan_cap(self->ch));
}

static Py_ssize_t RunloomChan_len(RunloomChan *self)
{
    return runloom_chan_len(self->ch);
}

static PyMethodDef RunloomChan_methods[] = {
    {"send", (PyCFunction)RunloomChan_send, METH_O,
     "Send a value, blocking until delivered."},
    {"try_send", (PyCFunction)RunloomChan_try_send, METH_O,
     "Try to send.  Returns True if delivered, False if would-block."},
    {"recv", (PyCFunction)RunloomChan_recv, METH_NOARGS,
     "Receive (value, ok); blocks until a value or close.  ok=False "
     "means the channel was closed and empty (Go's `v, ok := <-ch`)."},
    {"try_recv", (PyCFunction)RunloomChan_try_recv, METH_NOARGS,
     "Try to receive.  Returns (value, ok) on success, None if "
     "would-block."},
    {"close", (PyCFunction)RunloomChan_close, METH_NOARGS,
     "Mark the channel closed.  Wakes all parked senders + receivers."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef RunloomChan_getset[] = {
    {"closed",   (getter)RunloomChan_closed_get,   NULL, "True after close()", NULL},
    {"capacity", (getter)RunloomChan_cap_get,      NULL, "buffered capacity", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PySequenceMethods RunloomChan_seq = {
    (lenfunc)RunloomChan_len,            /* sq_length */
    0, 0, 0, 0, 0, 0, 0, 0, 0,
};

static PyTypeObject RunloomChanType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "runloom_c.Chan",
    .tp_basicsize = sizeof(RunloomChan),
    .tp_dealloc = (destructor)RunloomChan_dealloc,
    .tp_as_sequence = &RunloomChan_seq,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Go-style channel.  Chan(capacity=0).",
    .tp_iter = (getiterfunc)RunloomChan_iter,
    .tp_iternext = (iternextfunc)RunloomChan_iternext,
    .tp_methods = RunloomChan_methods,
    .tp_getset = RunloomChan_getset,
    .tp_init = (initproc)RunloomChan_init,
    .tp_new = PyType_GenericNew,
};

/* ============================================================ */
/* RunloomMutex -- an M:N-safe fiber mutex.                    */
/*                                                                */
/* A buffered channel of capacity 1 holding one token IS a mutex: */
/*   unlocked = the token sits in the buffer                      */
/*   lock()   = recv the token (blocks while another holder has   */
/*              it; a parked locker is woken when the holder sends */
/*              the token back -- direct hand-off, FIFO)           */
/*   unlock() = send the token back (full buffer => double-release)*/
/*                                                                 */
/* Backing the mutex with the channel reuses its formally-verified */
/* (Spin/CBMC/GenMC) cross-hub park/wake + publish ordering rather */
/* than hand-rolling a second lock-free primitive.  acquire() only */
/* ever BLOCKS (chan_recv) when the Python wrapper knows it runs   */
/* inside a fiber; foreign OS threads must use try_lock (which */
/* never parks).  A bare-CAS uncontended fast path is a possible   */
/* follow-up -- it would need its own GenMC harness to land.       */
/* ============================================================ */
typedef struct {
    PyObject_HEAD
    runloom_chan_t *ch;     /* cap 1; one buffered token == unlocked */
} RunloomMutex;

static int RunloomMutex_init(RunloomMutex *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "", kwlist)) return -1;
    self->ch = runloom_chan_new(1);
    if (self->ch == NULL) return -1;
    /* Seed the token: the mutex starts unlocked.  try_send returns 0 on
     * success (room in the empty cap-1 buffer). */
    if (runloom_chan_try_send(self->ch, Py_None) != 0) {
        runloom_chan_decref(self->ch);
        self->ch = NULL;
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_RuntimeError, "mutex init failed");
        return -1;
    }
    return 0;
}

static void RunloomMutex_dealloc(RunloomMutex *self)
{
    if (self->ch != NULL) {
        runloom_chan_decref(self->ch);
        self->ch = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* lock(): take the token, blocking until available.  MUST be called from a
 * fiber (it can park).  The Python wrapper guarantees that; a foreign
 * thread uses try_lock instead. */
static PyObject *RunloomMutex_lock(RunloomMutex *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    v = runloom_chan_recv(self->ch, &ok);   /* blocks while held elsewhere */
    if (v == NULL) return NULL;
    Py_DECREF(v);                            /* the token (None); discard */
    Py_RETURN_NONE;
}

/* try_lock(): non-blocking.  Never parks -> safe from a foreign OS thread. */
static PyObject *RunloomMutex_try_lock(RunloomMutex *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    if (runloom_chan_try_recv(self->ch, &v, &ok) < 0) return NULL;
    if (v == NULL) Py_RETURN_FALSE;          /* would-block: held elsewhere */
    Py_DECREF(v);
    Py_RETURN_TRUE;
}

/* unlock(): return the token.  A full buffer means the token was never taken
 * -> releasing an unlocked mutex, which raises (matches threading.Lock). */
static PyObject *RunloomMutex_unlock(RunloomMutex *self, PyObject *unused)
{
    int r;
    (void)unused;
    r = runloom_chan_try_send(self->ch, Py_None);
    if (r < 0) return NULL;
    if (r == 1) {
        PyErr_SetString(PyExc_RuntimeError, "release unlocked lock");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *RunloomMutex_locked(RunloomMutex *self, PyObject *unused)
{
    (void)unused;
    /* token taken (buffer empty) => held */
    if (runloom_chan_len(self->ch) == 0) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomMutex_enter(RunloomMutex *self, PyObject *unused)
{
    PyObject *r = RunloomMutex_lock(self, unused);
    if (r == NULL) return NULL;
    Py_DECREF(r);
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *RunloomMutex_exit(RunloomMutex *self, PyObject *args)
{
    (void)args;
    return RunloomMutex_unlock(self, NULL);
}

static PyMethodDef RunloomMutex_methods[] = {
    {"lock",     (PyCFunction)RunloomMutex_lock,     METH_NOARGS,
     "Acquire, blocking until available.  Call only from a fiber."},
    {"try_lock", (PyCFunction)RunloomMutex_try_lock, METH_NOARGS,
     "Try to acquire without blocking.  Returns True/False.  Foreign-thread safe."},
    {"unlock",   (PyCFunction)RunloomMutex_unlock,   METH_NOARGS,
     "Release.  Raises RuntimeError if not held."},
    {"locked",   (PyCFunction)RunloomMutex_locked,   METH_NOARGS,
     "True if currently held."},
    {"__enter__", (PyCFunction)RunloomMutex_enter,   METH_NOARGS, NULL},
    {"__exit__",  (PyCFunction)RunloomMutex_exit,    METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject RunloomMutexType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "runloom_c.Mutex",
    .tp_basicsize = sizeof(RunloomMutex),
    .tp_dealloc = (destructor)RunloomMutex_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "M:N-safe fiber mutex (channel-backed).  lock/unlock/"
              "try_lock/locked.",
    .tp_methods = RunloomMutex_methods,
    .tp_init = (initproc)RunloomMutex_init,
    .tp_new = PyType_GenericNew,
};

/* ---- blocking(): run a Python callable on the blocking-offload pool ----
 *
 * Generalises the C-level getaddrinfo offload to any user-named blocking
 * call: blocking(fn, *args, **kwargs) parks the calling fiber and runs
 * fn(*args, **kwargs) on a pool thread, so a blocking stdlib/C-extension
 * call doesn't wedge the fiber's hub.  The pool worker has no thread
 * state of its own, so it PyGILState_Ensure()s one to run fn; fn's own
 * blocking section releases the GIL (as socket / time.sleep / blocking C
 * extensions do), which is what lets other fibers keep running.
 *
 * fn runs OFF any fiber, so it must not call runloom scheduler ops
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

