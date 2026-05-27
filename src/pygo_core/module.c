/* module.c -- Python bindings for pygo_core.
 *
 * Exposes:
 *   pygo_core.Coro(callable, stack_size=131072) -> coro object
 *      .resume()         switch into the coroutine
 *      .done             True if entry returned
 *   pygo_core.yield_()   yield from inside a coroutine
 *   pygo_core.backend()  "fibers" | "ucontext"
 *
 * Free-threaded friendly: each OS thread runs its own coroutines.
 * We do NOT release the GIL during resume() because the Python callable
 * we run inside the coro will reacquire/release as it pleases.  Under
 * free-threaded Python (3.13t) there is no global lock to release.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "coro.h"
#include "pygo_sched.h"
#include "netpoll.h"
#include "mn_sched.h"

/* ---- Per-coro Python object ---- */

/* CPython thread-state snapshot.  These fields are not preserved by a
 * raw C-stack swap, but Python frame chain + recursion counters live
 * on the thread state and need to follow the coroutine.  We snapshot
 * what we can portably:
 *  - py_recursion_remaining / c_recursion_remaining (3.12+)
 *  - recursion_depth (older 3.x)
 * Other fields like the topmost frame chain are still UB territory and
 * are why we don't run unittest harness frames over yields. */
typedef struct {
#if PY_VERSION_HEX >= 0x030C0000
    int py_recursion_remaining;
    int c_recursion_remaining;
#else
    int recursion_depth;
#endif
    int initialised;
} PygoTstateSnapshot;

typedef struct {
    PyObject_HEAD
    pygo_coro_t *coro;
    PyObject *callable;   /* invoked once when the coro first resumes */
    PyObject *result;     /* return value of callable, or NULL */
    PyObject *error;      /* unhandled exception caught, or NULL */
    int has_run;
    PygoTstateSnapshot tstate_snap;  /* captured at yield, restored at resume */
} PygoCoro;

PYGO_INLINE void pygo_tstate_save(PygoTstateSnapshot *s)
{
    PyThreadState *ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    s->py_recursion_remaining = ts->py_recursion_remaining;
    s->c_recursion_remaining = ts->c_recursion_remaining;
#else
    s->recursion_depth = ts->recursion_depth;
#endif
    s->initialised = 1;
}

PYGO_INLINE void pygo_tstate_restore(const PygoTstateSnapshot *s)
{
    PyThreadState *ts;
    if (!s->initialised) {
        return;
    }
    ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    ts->py_recursion_remaining = s->py_recursion_remaining;
    ts->c_recursion_remaining = s->c_recursion_remaining;
#else
    ts->recursion_depth = s->recursion_depth;
#endif
}

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

/* ---- Module-level functions ---- */

static PyObject *m_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    /* yield is just "switch back to caller".  We hold the GIL across the
     * switch on GIL-builds because the caller will resume us on the same
     * OS thread; under free-threaded Python the thread state stays
     * attached.  This is safe because we never migrate a coroutine
     * between OS threads (each thread has its own scheduler). */
    pygo_coro_yield();
    Py_RETURN_NONE;
}

static PyObject *m_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(pygo_coro_backend());
}

static PyObject *m_thread_init(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    if (pygo_coro_thread_init() != 0) {
        PyErr_SetString(PyExc_OSError, "pygo_coro_thread_init failed");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *m_thread_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_coro_thread_fini();
    Py_RETURN_NONE;
}

/* ---- Fast-path: C scheduler ----
 *
 * pygo_core.go(fn)       -> PygoG handle.  Schedules fn to run.
 * pygo_core.sched_yield() -> None.  Yields current g via the C scheduler.
 * pygo_core.sched_sleep(s) -> None.  Parks current g on the sleep heap.
 * pygo_core.run()        -> int.  Drives the scheduler until idle.
 *
 * These call directly into sched.c -- no Python-level scheduler.
 * Compared to the original Python pygo.runtime.Scheduler this is a
 * single C call per yield/spawn rather than 3-5 Python frames.
 */

typedef struct {
    PyObject_HEAD
    pygo_g_t *g;
} PygoG;

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
    if (self->g == NULL || self->g->done) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoG_result_get(PygoG *self, void *closure)
{
    (void)closure;
    if (self->g == NULL || self->g->result == NULL) Py_RETURN_NONE;
    Py_INCREF(self->g->result);
    return self->g->result;
}

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
    0, 0,
    PygoG_getset,
    0, 0, 0, 0, 0,
    0, 0,
    PyType_GenericNew,
};

static PyObject *m_go(PyObject *self, PyObject *callable)
{
    pygo_sched_t *s;
    PygoG *handle;
    pygo_g_t *g;
    PyObject *cap;
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go(): callable required");
        return NULL;
    }
    s = pygo_sched_get();
    cap = pygo_sched_spawn(s, callable);
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

static PyObject *m_sched_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_yield(pygo_sched_get());
    Py_RETURN_NONE;
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


static PyObject *m_sched_sleep(PyObject *self, PyObject *arg)
{
    double secs;
    pygo_sched_t *s;
    (void)self;
    secs = PyFloat_AsDouble(arg);
    if (secs == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
    s = pygo_sched_get();
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        double now = (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
        pygo_sched_sleep_until(s, now + secs);
    }
    Py_RETURN_NONE;
}

static PyObject *m_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = pygo_sched_drain(pygo_sched_get());
    return PyLong_FromSsize_t(completed);
}

static PyObject *m_wait_fd(PyObject *self, PyObject *args)
{
    int fd, events;
    long timeout_ms = -1;
    int result;
    (void)self;
    if (!PyArg_ParseTuple(args, "ii|l", &fd, &events, &timeout_ms)) {
        return NULL;
    }
    {
        long long timeout_ns = timeout_ms < 0 ? -1LL :
                               (long long)timeout_ms * 1000000LL;
        result = pygo_netpoll_wait_fd(fd, events, timeout_ns);
    }
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    return PyLong_FromLong((long)result);
}

static PyObject *m_netpoll_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(pygo_netpoll_backend());
}

/* ---- M:N scheduler bindings (Phase C) ---- */
static PyObject *m_mn_init(PyObject *self, PyObject *args)
{
    int n = 0;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &n)) return NULL;
    {
        int got = pygo_mn_init(n);
        if (got < 0) return NULL;
        return PyLong_FromLong(got);
    }
}

static PyObject *m_mn_go(PyObject *self, PyObject *callable)
{
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "mn_go(): callable required");
        return NULL;
    }
    return pygo_mn_go(callable);
}

static PyObject *m_mn_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = pygo_mn_run();
    return PyLong_FromSsize_t(completed);
}

static PyObject *m_mn_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_mn_fini();
    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"yield_",      m_yield,       METH_NOARGS,
     "Yield from inside a raw Coro (a no-op outside one)."},
    {"backend",     m_backend,     METH_NOARGS,
     "Return the active stack-switch backend name."},
    {"thread_init", m_thread_init, METH_NOARGS,
     "Per-OS-thread setup; idempotent."},
    {"thread_fini", m_thread_fini, METH_NOARGS,
     "Per-OS-thread teardown."},
    /* C-scheduler fast path. */
    {"go",          m_go,          METH_O,
     "Spawn a goroutine via the C scheduler.  Returns a G handle."},
    {"sched_yield_classic", m_sched_yield, METH_NOARGS,
     "Yield the current goroutine (METH_NOARGS PyCFunction form, kept "
     "for benchmarking against the vectorcall singleton)."},
    {"sched_sleep", m_sched_sleep, METH_O,
     "Sleep the current goroutine for N seconds (C scheduler aware)."},
    {"run",         m_run,         METH_NOARGS,
     "Drive the C scheduler until all goroutines finish.  Returns count."},
    {"wait_fd",     m_wait_fd,     METH_VARARGS,
     "wait_fd(fd, events, timeout_ms=-1): park the current goroutine "
     "until fd is ready.  events is a bitmask: 1=read, 2=write."},
    {"netpoll_backend", m_netpoll_backend, METH_NOARGS,
     "Return active netpoll backend name (\"epoll\", \"kqueue\", \"select\")."},
    {"mn_init",     m_mn_init,     METH_VARARGS,
     "mn_init(n=cpus): start N hub threads.  Returns count."},
    {"mn_go",       m_mn_go,       METH_O,
     "mn_go(callable): spawn a goroutine on a round-robin hub.  "
     "v1 only supports run-to-completion gs (no yield)."},
    {"mn_run",      m_mn_run,      METH_NOARGS,
     "mn_run(): wait for all gs to complete.  Returns total completed."},
    {"mn_fini",     m_mn_fini,     METH_NOARGS,
     "mn_fini(): tear down the hub pool."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef pygo_core_module = {
    PyModuleDef_HEAD_INIT,
    "pygo_core",
    "Portable stackful coroutines + scheduler primitives.",
    -1,
    module_methods,
    NULL, NULL, NULL, NULL
};

/* Declare the extension as safe under free-threaded Python (3.13t).
 * Our single-OS-thread scheduler doesn't have multi-thread races
 * today; the M:N work-stealing in Phase C will need actual atomic
 * work-queue ops to keep this declaration honest. */
#ifdef Py_GIL_DISABLED
#  define PYGO_FT_OK 1
#endif

PyMODINIT_FUNC PyInit_pygo_core(void)
{
    PyObject *m;
    if (PyType_Ready(&PygoCoroType) < 0) return NULL;
    if (PyType_Ready(&PygoGType) < 0) return NULL;
    m = PyModule_Create(&pygo_core_module);
    if (m == NULL) {
        return NULL;
    }
    Py_INCREF(&PygoCoroType);
    Py_INCREF(&PygoGType);
    if (PyModule_AddObject(m, "G", (PyObject *)&PygoGType) < 0) {
        Py_DECREF(&PygoGType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyModule_AddObject(m, "Coro", (PyObject *)&PygoCoroType) < 0) {
        Py_DECREF(&PygoCoroType);
        Py_DECREF(m);
        return NULL;
    }
    /* Set up the vectorcall singleton and expose it as `sched_yield`.
     * The interpreter's CALL bytecode will specialize this to the
     * vectorcall fast path, shaving Python-call-dispatch overhead off
     * each yield site. */
    if (PyType_Ready(&PygoYielderType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    {
        PygoYielder *y = PyObject_New(PygoYielder, &PygoYielderType);
        if (y == NULL) {
            Py_DECREF(m);
            return NULL;
        }
        y->vectorcall = yielder_vectorcall;
        if (PyModule_AddObject(m, "sched_yield", (PyObject *)y) < 0) {
            Py_DECREF(y);
            Py_DECREF(m);
            return NULL;
        }
    }
#ifdef Py_GIL_DISABLED
    /* Declare free-thread safety.  v0 scheduler is still single-OS-
     * thread, so this is trivially safe.  Phase C M:N needs to maintain
     * this when adding the work-stealing ring queue. */
    PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED);
#endif
    return m;
}
