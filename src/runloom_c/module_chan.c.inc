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

/* GC hooks for the channel's ring buffer.  The buffer holds strong references
 * to arbitrary Python objects (Py_INCREF on send), so a reference cycle passing
 * through a buffered value -- e.g. a request that carries its own reply channel
 * -- must be visible to the cyclic collector or it leaks forever.  struct
 * runloom_chan is opaque in this translation unit, so the actual walk/drop over
 * the buffer live next to it in chan.c (chan_ops.c.inc) and are called from
 * here.  gc_traverse tolerates a partially-initialised (ch == NULL) instance. */
int  runloom_chan_gc_traverse(runloom_chan_t *ch, visitproc visit, void *arg);
void runloom_chan_gc_clear(runloom_chan_t *ch);

/* Reject a method call on an object built via Chan.__new__ / Mutex.__new__ (or
 * whose __init__ never ran): self->ch is still NULL, and handing it to a channel
 * op would dereference a NULL runloom_chan_t and hard-crash the interpreter.
 * The sibling Coro/G types NULL-check their handle the same way. */
static int chan_require(runloom_chan_t *ch)
{
    if (ch == NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "channel is not initialised "
                        "(object created without calling __init__)");
        return -1;
    }
    return 0;
}

static int RunloomChan_init(RunloomChan *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"capacity", NULL};
    Py_ssize_t cap = 0;
    runloom_chan_t *ch;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|n", kwlist, &cap)) {
        return -1;
    }
    ch = runloom_chan_new(cap);
    if (ch == NULL) return -1;
    /* Support re-invocation of __init__ on a live object: drop the previous
     * channel (and everything buffered in it) before installing the new one,
     * or the old runloom_chan_t leaks.  The instance is already GC-tracked
     * (PyType_GenericAlloc tracks HAVE_GC objects at allocation), so no
     * PyObject_GC_Track here -- and RunloomChan_traverse tolerates ch == NULL. */
    if (self->ch != NULL) runloom_chan_decref(self->ch);
    self->ch = ch;
    return 0;
}

static void RunloomChan_dealloc(RunloomChan *self)
{
    PyObject_GC_UnTrack(self);
    if (self->ch != NULL) {
        runloom_chan_decref(self->ch);
        self->ch = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomChan_send(RunloomChan *self, PyObject *value)
{
    if (chan_require(self->ch) < 0) return NULL;
    if (runloom_chan_send(self->ch, value) < 0) return NULL;
    Py_RETURN_NONE;
}

static PyObject *RunloomChan_try_send(RunloomChan *self, PyObject *value)
{
    int r;
    if (chan_require(self->ch) < 0) return NULL;
    r = runloom_chan_try_send(self->ch, value);
    if (r < 0) return NULL;
    if (r == 0) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomChan_recv(RunloomChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    if (chan_require(self->ch) < 0) return NULL;
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
    if (chan_require(self->ch) < 0) return NULL;
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
    if (chan_require(self->ch) < 0) return NULL;
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
    PyObject *v;
    if (chan_require(self->ch) < 0) return NULL;
    v = runloom_chan_recv(self->ch, &ok);
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
    if (chan_require(self->ch) < 0) return NULL;
    if (runloom_chan_is_closed(self->ch)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomChan_cap_get(RunloomChan *self, void *closure)
{
    (void)closure;
    if (chan_require(self->ch) < 0) return NULL;
    return PyLong_FromSsize_t(runloom_chan_cap(self->ch));
}

static Py_ssize_t RunloomChan_len(RunloomChan *self)
{
    if (chan_require(self->ch) < 0) return -1;
    return runloom_chan_len(self->ch);
}

/* ---- GC protocol ---- */
static int RunloomChan_traverse(RunloomChan *self, visitproc visit, void *arg)
{
    /* Expose the strong refs the C ring buffer holds so the cyclic collector
     * can see cycles that reach back through a buffered value.  self->ch may be
     * NULL on a partially-initialised instance (PyType_GenericAlloc tracks
     * HAVE_GC objects before __init__ runs). */
    if (self->ch != NULL)
        return runloom_chan_gc_traverse(self->ch, visit, arg);
    return 0;
}

static int RunloomChan_clear(RunloomChan *self)
{
    /* Break cycles by dropping every buffered value's ref.  The channel object
     * survives (now empty), matching how the collector clears any container. */
    if (self->ch != NULL)
        runloom_chan_gc_clear(self->ch);
    return 0;
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
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_doc = "Go-style channel.  Chan(capacity=0).",
    .tp_traverse = (traverseproc)RunloomChan_traverse,
    .tp_clear = (inquiry)RunloomChan_clear,
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
    runloom_chan_t *ch;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "", kwlist)) return -1;
    ch = runloom_chan_new(1);
    if (ch == NULL) return -1;
    /* Seed the token: the mutex starts unlocked.  try_send returns 0 on
     * success (room in the empty cap-1 buffer). */
    if (runloom_chan_try_send(ch, Py_None) != 0) {
        runloom_chan_decref(ch);
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_RuntimeError, "mutex init failed");
        return -1;
    }
    /* Re-invocation of __init__ replaces the channel; drop the old one so it
     * (and its buffered token) doesn't leak. */
    if (self->ch != NULL) runloom_chan_decref(self->ch);
    self->ch = ch;
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
    if (chan_require(self->ch) < 0) return NULL;
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
    if (chan_require(self->ch) < 0) return NULL;
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
    if (chan_require(self->ch) < 0) return NULL;
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
    if (chan_require(self->ch) < 0) return NULL;
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

/* Each blockpool worker thread caches ONE PyThreadState here (lazily, on its
 * first Python offload) and reuses it via cheap attach/detach per job, instead
 * of PyGILState_Ensure/Release creating + destroying a tstate every call.  The
 * per-job create/destroy each took the runtime's global HEAD_LOCK, which
 * serialized all workers and capped offload throughput at ~one round-trip at a
 * time (measured: flat ~20k offloads/s regardless of workers/hubs/shards).  With
 * a persistent tstate the per-job cost is just PyEval_RestoreThread/SaveThread
 * (lock-free attach/detach), so offload scales.  `tried` records that creation
 * was attempted (NULL result => M:N not up => fall back to PyGILState). */
static RUNLOOM_TLS PyThreadState *bp_worker_pyts = NULL;
static RUNLOOM_TLS int            bp_worker_pyts_tried = 0;

/* Worker-exit hook (installed on runloom_blockpool_worker_thread_fini): release
 * this thread's cached tstate on this thread, where its gilstate-TSS is bound. */
static void py_blocking_worker_thread_fini(void)
{
    if (bp_worker_pyts != NULL) {
        runloom_mn_worker_tstate_delete(bp_worker_pyts);
        bp_worker_pyts = NULL;
    }
    bp_worker_pyts_tried = 0;
}

static void py_blocking_worker_call(py_blocking_job_t *j)
{
    j->result = PyObject_Call(j->fn, j->args, j->kwargs);
    if (j->result == NULL) {
        PyErr_Fetch(&j->exc_type, &j->exc_value, &j->exc_tb);
        PyErr_NormalizeException(&j->exc_type, &j->exc_value, &j->exc_tb);
    }
}

static void *py_blocking_worker(void *p)
{
    py_blocking_job_t *j = (py_blocking_job_t *)p;
    /* If this thread already has an attached tstate, the call is running INLINE
     * (the off-fiber fallback in runloom_blocking_call, e.g. a top-level
     * blocking()).  Call directly: the tstate is held, so PyObject_Call is
     * safe, and we MUST NOT PyGILState_Ensure() -- after an M:N run() has torn
     * down on this thread, the gilstate TSS is desynced from the live tstate,
     * so Ensure() tries to attach a fresh tstate over the still-current one and
     * aborts ("_PyThreadState_Attach: non-NULL old thread state").  Only a bare
     * pool thread (no tstate) needs one. */
    if (PyThreadState_GetUnchecked() != NULL) {
        py_blocking_worker_call(j);
        return NULL;
    }

    /* Bare pool worker.  Use a PERSISTENT per-thread tstate, created once and
     * attached ONLY around the Python call (so a pure-C offload on this same
     * worker, or the idle wait between jobs, never holds an attached tstate that
     * would stall a stop-the-world GC). */
    if (!bp_worker_pyts_tried) {
        bp_worker_pyts_tried = 1;
        bp_worker_pyts = runloom_mn_worker_tstate_new();   /* NULL if M:N not up */
    }
    if (bp_worker_pyts != NULL) {
        PyEval_RestoreThread(bp_worker_pyts);              /* cheap attach */
        py_blocking_worker_call(j);
        PyEval_SaveThread();                               /* cheap detach */
        return NULL;
    }

    /* Fallback (single-thread sched / M:N not running): per-call gilstate. */
    {
        PyGILState_STATE st = PyGILState_Ensure();
        py_blocking_worker_call(j);
        PyGILState_Release(st);
    }
    return NULL;
}

