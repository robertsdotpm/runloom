/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static PyTypeObject RunloomGType;   /* defined below; referenced by richcompare */

static void RunloomG_dealloc(RunloomG *self)
{
    if (self->g != NULL) {
        runloom_g_decref(self->g);
        self->g = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomG_done_get(RunloomG *self, void *closure)
{
    (void)closure;
    if (self->g == NULL) Py_RETURN_TRUE;
    /* ACQUIRE pairs with the RELEASE store in runloom_g_entry; ensures
     * that if done is true, we also see the matching result/error
     * stores even on free-threaded 3.13t reading from another thread. */
    if (__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *RunloomG_result_get(RunloomG *self, void *closure)
{
    PyObject *r;
    (void)closure;
    if (self->g == NULL) Py_RETURN_NONE;
    /* Same acquire pattern as RunloomG_done_get -- gate the result read
     * behind the done flag's release ordering. */
    if (!__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_NONE;
    r = self->g->result;
    if (r == NULL) Py_RETURN_NONE;
    Py_INCREF(r);
    return r;
}

/* Wake a fiber parked via runloom_c.park_self() or runloom_c.park().  Safe to
 * call before the park (race-handled inside the wake primitives) and from a
 * foreign OS thread.  ROUTES BY HOW THE g PARKED: a g parked on an M:N hub
 * (park_hub != NULL, set by runloom_park_generic) must be re-queued via
 * runloom_mn_wake_g -- runloom_sched_wake_safe only handles the single-thread
 * park_safe handshake (it CASes parked_safe, which the M:N park never sets, so it
 * would silently drop the wake and strand the fiber).  The hub pointer is the
 * one RECORDED on the g (stable for the process lifetime); never pass a hub from
 * the foreign caller (a stale hub is the documented SIGSEGV).  The pump kick
 * breaks an idle single-thread run() epoll_wait(-1) so a cross-thread wake is
 * delivered promptly; harmless (level-triggered drain) on the same-thread path. */
static PyObject *RunloomG_wake(RunloomG *self, PyObject *unused)
{
    (void)unused;
    if (self->g != NULL) {
        /* ALWAYS go through wake_safe: it drives the parked_safe / wake_pending
         * Dekker (so a wake that races the park is never lost) and routes the
         * re-queue by g->park_hub -- runloom_mn_wake_g for an M:N hub parker,
         * the single-thread ready_push/wake_list otherwise.  (An earlier version
         * called mn_wake_g directly for hub parkers and BYPASSED the Dekker, which
         * lost a wake-before-park.)  The pump kick breaks an idle single-thread
         * run() epoll_wait(-1); harmless on the same-thread / hub path. */
        runloom_sched_wake_safe(self->g);
        runloom_netpoll_wake_pump();
    }
    Py_RETURN_NONE;
}

/* Cancel this fiber if it is parked in runloom_c.wait_fd: its wait_fd
 * returns the WAIT_FD_CANCELLED sentinel and the g is re-queued.  Returns True
 * if it was netpoll-parked (and woken), False otherwise (running, or parked via
 * park_self -- use wake() for that).  This is the cancel path for a fiber
 * blocked in a socket recv/accept/connect, which has no coro await-point for
 * the driver to throw CancelledError into. */
static PyObject *RunloomG_cancel_wait_fd(RunloomG *self, PyObject *unused)
{
    int woke = 0;
    (void)unused;
    if (self->g != NULL) {
        woke = runloom_netpoll_cancel_g(self->g);
    }
    if (woke) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/* Return a small introspection dict for a parked fiber:
 *   {"state": "done"|"running"|"parked"|"fresh",
 *    "has_snap": bool}
 *
 * Originally this returned a full Python frame walk via the saved
 * snap.  The internal _PyInterpreterFrame layout changes across
 * patch releases (3.11/3.12/3.13 all differ), so walking it from a
 * stable C extension would require pinning to the internal-API build
 * flag -- something we deliberately avoid for portability.  This
 * minimal version still lets a watchdog fiber answer "what state
 * is task X in?" without paying the internal-header dependency. */
static PyObject *RunloomG_stack(RunloomG *self, PyObject *unused)
{
    PyObject *d;
    const char *state;
    int has_snap;
    (void)unused;

    if (self->g == NULL) {
        state = "freed";
        has_snap = 0;
    } else if (self->g->done) {
        state = "done";
        has_snap = 0;
    } else if (self->g->snap.valid) {
        state = (runloom_sched_get()->current == self->g) ? "running" : "parked";
        has_snap = 1;
    } else {
        state = "fresh";
        has_snap = 0;
    }

    d = PyDict_New();
    if (d == NULL) return NULL;
    if (PyDict_SetItemString(d, "state", PyUnicode_FromString(state)) < 0 ||
        PyDict_SetItemString(d, "has_snap", PyBool_FromLong(has_snap)) < 0) {
        Py_DECREF(d);
        return NULL;
    }
    return d;
}

/* Two G handles compare equal iff they wrap the same underlying fiber.
 * runloom_c.current_g() mints a fresh RunloomG on every call (a new PyObject
 * wrapping the same runloom_g_t*), so identity-based '==' would make a fiber
 * fail to recognise its own handle across calls -- breaking, e.g., a
 * reentrant cooperative lock that records the owner as current_g().  Compare
 * by the wrapped pointer, and hash to match so G works as a dict/set key. */
static PyObject *RunloomG_richcompare(PyObject *a, PyObject *b, int op)
{
    int eq;
    if ((op != Py_EQ && op != Py_NE) ||
        !PyObject_TypeCheck(a, &RunloomGType) ||
        !PyObject_TypeCheck(b, &RunloomGType)) {
        Py_RETURN_NOTIMPLEMENTED;
    }
    eq = (((RunloomG *)a)->g == ((RunloomG *)b)->g);
    if (op == Py_NE) eq = !eq;
    if (eq) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static Py_hash_t RunloomG_hash(PyObject *self)
{
    Py_hash_t h = (Py_hash_t)(Py_uintptr_t)((RunloomG *)self)->g;
    return h == -1 ? -2 : h;
}

static PyMethodDef RunloomG_methods[] = {
    {"wake", (PyCFunction)RunloomG_wake, METH_NOARGS,
     "Wake this fiber if parked via park_self().  Safe to call "
     "before park_self (race-handled)."},
    {"cancel_wait_fd", (PyCFunction)RunloomG_cancel_wait_fd, METH_NOARGS,
     "Cancel this fiber if parked in wait_fd: its wait_fd returns the "
     "WAIT_FD_CANCELLED sentinel and it is re-queued.  Returns True if it was "
     "netpoll-parked, else False (use wake() for park_self parkers)."},
    {"stack", (PyCFunction)RunloomG_stack, METH_NOARGS,
     "Return a small introspection dict: "
     "{'state': 'done'|'running'|'parked'|'fresh', 'has_snap': bool}.  "
     "Watchdog-safe; cheap to poll for 'where is task X stuck?'."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef RunloomG_getset[] = {
    {"done",   (getter)RunloomG_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)RunloomG_result_get, NULL, "Return value or None",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject RunloomGType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "runloom_c.G",
    sizeof(RunloomG),
    0,
    (destructor)RunloomG_dealloc,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,
    "Goroutine handle.",
    0, 0, 0, 0, 0, 0,
    RunloomG_methods,
    0,
    RunloomG_getset,
    0, 0, 0, 0, 0,
    0, 0,
    PyType_GenericNew,
};

