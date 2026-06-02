/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static PyTypeObject PygoGType;   /* defined below; referenced by richcompare */

static void PygoG_dealloc(PygoG *self)
{
    if (self->g != NULL) {
        pygo_g_decref(self->g);
        self->g = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoG_done_get(PygoG *self, void *closure)
{
    (void)closure;
    if (self->g == NULL) Py_RETURN_TRUE;
    /* ACQUIRE pairs with the RELEASE store in pygo_g_entry; ensures
     * that if done is true, we also see the matching result/error
     * stores even on free-threaded 3.13t reading from another thread. */
    if (__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoG_result_get(PygoG *self, void *closure)
{
    PyObject *r;
    (void)closure;
    if (self->g == NULL) Py_RETURN_NONE;
    /* Same acquire pattern as PygoG_done_get -- gate the result read
     * behind the done flag's release ordering. */
    if (!__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_NONE;
    r = self->g->result;
    if (r == NULL) Py_RETURN_NONE;
    Py_INCREF(r);
    return r;
}

/* Wake a goroutine that's parked via pygo_core.park_self().  Safe to
 * call before the park (race-handled inside pygo_sched_wake_safe). */
static PyObject *PygoG_wake(PygoG *self, PyObject *unused)
{
    (void)unused;
    if (self->g != NULL) {
        pygo_sched_wake_safe(self->g);
    }
    Py_RETURN_NONE;
}

/* Cancel this goroutine if it is parked in pygo_core.wait_fd: its wait_fd
 * returns the WAIT_FD_CANCELLED sentinel and the g is re-queued.  Returns True
 * if it was netpoll-parked (and woken), False otherwise (running, or parked via
 * park_self -- use wake() for that).  This is the cancel path for a goroutine
 * blocked in a socket recv/accept/connect, which has no coro await-point for
 * the driver to throw CancelledError into. */
static PyObject *PygoG_cancel_wait_fd(PygoG *self, PyObject *unused)
{
    int woke = 0;
    (void)unused;
    if (self->g != NULL) {
        woke = pygo_netpoll_cancel_g(self->g);
    }
    if (woke) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/* Return a small introspection dict for a parked goroutine:
 *   {"state": "done"|"running"|"parked"|"fresh",
 *    "has_snap": bool}
 *
 * Originally this returned a full Python frame walk via the saved
 * snap.  The internal _PyInterpreterFrame layout changes across
 * patch releases (3.11/3.12/3.13 all differ), so walking it from a
 * stable C extension would require pinning to the internal-API build
 * flag -- something we deliberately avoid for portability.  This
 * minimal version still lets a watchdog goroutine answer "what state
 * is task X in?" without paying the internal-header dependency. */
static PyObject *PygoG_stack(PygoG *self, PyObject *unused)
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
        state = (pygo_sched_get()->current == self->g) ? "running" : "parked";
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

/* Two G handles compare equal iff they wrap the same underlying goroutine.
 * pygo_core.current_g() mints a fresh PygoG on every call (a new PyObject
 * wrapping the same pygo_g_t*), so identity-based '==' would make a goroutine
 * fail to recognise its own handle across calls -- breaking, e.g., a
 * reentrant cooperative lock that records the owner as current_g().  Compare
 * by the wrapped pointer, and hash to match so G works as a dict/set key. */
static PyObject *PygoG_richcompare(PyObject *a, PyObject *b, int op)
{
    int eq;
    if ((op != Py_EQ && op != Py_NE) ||
        !PyObject_TypeCheck(a, &PygoGType) ||
        !PyObject_TypeCheck(b, &PygoGType)) {
        Py_RETURN_NOTIMPLEMENTED;
    }
    eq = (((PygoG *)a)->g == ((PygoG *)b)->g);
    if (op == Py_NE) eq = !eq;
    if (eq) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static Py_hash_t PygoG_hash(PyObject *self)
{
    Py_hash_t h = (Py_hash_t)(Py_uintptr_t)((PygoG *)self)->g;
    return h == -1 ? -2 : h;
}

static PyMethodDef PygoG_methods[] = {
    {"wake", (PyCFunction)PygoG_wake, METH_NOARGS,
     "Wake this goroutine if parked via park_self().  Safe to call "
     "before park_self (race-handled)."},
    {"cancel_wait_fd", (PyCFunction)PygoG_cancel_wait_fd, METH_NOARGS,
     "Cancel this goroutine if parked in wait_fd: its wait_fd returns the "
     "WAIT_FD_CANCELLED sentinel and it is re-queued.  Returns True if it was "
     "netpoll-parked, else False (use wake() for park_self parkers)."},
    {"stack", (PyCFunction)PygoG_stack, METH_NOARGS,
     "Return a small introspection dict: "
     "{'state': 'done'|'running'|'parked'|'fresh', 'has_snap': bool}.  "
     "Watchdog-safe; cheap to poll for 'where is task X stuck?'."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef PygoG_getset[] = {
    {"done",   (getter)PygoG_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)PygoG_result_get, NULL, "Return value or None",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoGType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pygo_core.G",
    sizeof(PygoG),
    0,
    (destructor)PygoG_dealloc,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,
    "Goroutine handle.",
    0, 0, 0, 0, 0, 0,
    PygoG_methods,
    0,
    PygoG_getset,
    0, 0, 0, 0, 0,
    0, 0,
    PyType_GenericNew,
};

