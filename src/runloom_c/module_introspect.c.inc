/* Part of module.c -- #included into that translation unit.  Python
 * bindings for the fiber registry / dump (runloom_introspect.c). */

/* Build one {...} dict for a snapshot entry.  Returns a new ref or NULL
 * (exception set). */
static PyObject *runloom_g_info_to_dict(const runloom_g_info_t *o, double now)
{
    PyObject *d = PyDict_New();
    PyObject *v = NULL;
    if (d == NULL) return NULL;

#define SET(key, expr) do {                                               \
        v = (expr);                                                       \
        if (v == NULL || PyDict_SetItemString(d, (key), v) < 0) {         \
            Py_XDECREF(v); Py_DECREF(d); return NULL;                     \
        }                                                                 \
        Py_DECREF(v);                                                     \
    } while (0)

    SET("id",       PyLong_FromUnsignedLongLong((unsigned long long)o->id));
    SET("state",    PyUnicode_FromString(runloom_g_state_name(o->state)));
    SET("blocked_on", PyUnicode_FromString(runloom_g_state_blockclass(o->state)));
    SET("refcount", PyLong_FromLong((long)o->refcount));
    SET("noyield",  PyBool_FromLong(o->noyield));
    SET("owner",    PyLong_FromVoidPtr((void *)o->owner));

    if (o->state == RUNLOOM_GST_PARKED_NETPOLL && o->park_fd >= 0) {
        char ev[3]; int n = 0;
        if (o->park_events & 1) ev[n++] = 'R';
        if (o->park_events & 2) ev[n++] = 'W';
        ev[n] = '\0';
        SET("fd",     PyLong_FromLong((long)o->park_fd));
        SET("events", PyUnicode_FromString(ev));
    } else {
        SET("fd",     Py_NewRef(Py_None));
        SET("events", Py_NewRef(Py_None));
    }

    if (o->state == RUNLOOM_GST_PARKED_SLEEP) {
        SET("wake_in", PyFloat_FromDouble(o->wake_at - now));
    } else {
        SET("wake_in", Py_NewRef(Py_None));
    }

    if (o->age_ns >= 0) {
        SET("age", PyFloat_FromDouble((double)o->age_ns / 1e9));
    } else {
        SET("age", Py_NewRef(Py_None));
    }

#undef SET
    return d;
}

static PyObject *m_fibers(PyObject *self, PyObject *unused)
{
    runloom_g_info_t *arr;
    long n = 0, i;
    PyObject *list;
    double now;
    (void)self; (void)unused;

    arr = runloom_fiber_snapshot(&n);
    if (arr == NULL && n == 0) {
        /* Empty (or not yet initialised) -- return [], not an error. */
        return PyList_New(0);
    }
    now = runloom_sched_monotonic_seconds();
    list = PyList_New(0);
    if (list == NULL) { runloom_fiber_snapshot_free(arr, n); return NULL; }
    for (i = 0; i < n; i++) {
        PyObject *d = runloom_g_info_to_dict(&arr[i], now);
        if (d == NULL || PyList_Append(list, d) < 0) {
            Py_XDECREF(d); Py_DECREF(list);
            runloom_fiber_snapshot_free(arr, n);
            return NULL;
        }
        Py_DECREF(d);
    }
    runloom_fiber_snapshot_free(arr, n);
    return list;
}

static PyObject *m_fiber_count(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong(runloom_fiber_count());
}

/* Build one {...} dict for a hub snapshot entry (runloom.inspect.hubs()).
 * Returns a new ref or NULL (exception set). */
static PyObject *runloom_hub_info_to_dict(const runloom_hub_info_t *o)
{
    PyObject *d = PyDict_New();
    PyObject *v = NULL;
    const char *state;
    if (d == NULL) return NULL;

#define SET(key, expr) do {                                               \
        v = (expr);                                                       \
        if (v == NULL || PyDict_SetItemString(d, (key), v) < 0) {         \
            Py_XDECREF(v); Py_DECREF(d); return NULL;                     \
        }                                                                 \
        Py_DECREF(v);                                                     \
    } while (0)

    /* attach_state mirrors RUNLOOM_TS_* (0 DETACHED / 1 ATTACHED / 2 SUSPENDED;
     * -1 unknown, e.g. pre-3.13).  DETACHED with a long dwell == a blocking-IO
     * wedge; ATTACHED == CPU-bound; the dwell tells idle from stuck. */
    switch (o->attach_state) {
        case 0:  state = "detached";  break;
        case 1:  state = "attached";  break;
        case 2:  state = "suspended"; break;
        default: state = "unknown";   break;
    }

    SET("id",      PyLong_FromLong((long)o->id));
    SET("state",   PyUnicode_FromString(state));
    SET("pending", PyLong_FromLong(o->pending));
    SET("preempt_requested", PyBool_FromLong(o->preempt_requested));
    SET("instrumented",      PyBool_FromLong(o->instrumented));

    if (o->has_running_g) {
        SET("running_g", PyLong_FromUnsignedLongLong((unsigned long long)o->running_g));
        SET("dwell_ms",  PyFloat_FromDouble(o->dwell_ms));
    } else {
        SET("running_g", Py_NewRef(Py_None));
        SET("dwell_ms",  Py_NewRef(Py_None));
    }

    if (o->blocked_at[0] != '\0') {
        SET("blocked_at", PyUnicode_FromString(o->blocked_at));
    } else {
        SET("blocked_at", Py_NewRef(Py_None));
    }

#undef SET
    return d;
}

static PyObject *m_mn_hub_states(PyObject *self, PyObject *unused)
{
    runloom_hub_info_t *arr;
    long n = 0, i;
    PyObject *list;
    (void)self; (void)unused;

    arr = runloom_mn_hub_snapshot(&n);
    if (arr == NULL || n == 0) {
        free(arr);                 /* free(NULL) is a no-op */
        return PyList_New(0);      /* not running / no hubs -> [] */
    }
    list = PyList_New(0);
    if (list == NULL) { free(arr); return NULL; }
    for (i = 0; i < n; i++) {
        PyObject *d = runloom_hub_info_to_dict(&arr[i]);
        if (d == NULL || PyList_Append(list, d) < 0) {
            Py_XDECREF(d); Py_DECREF(list); free(arr);
            return NULL;
        }
        Py_DECREF(d);
    }
    free(arr);
    return list;
}

static PyObject *m_dump_fibers(PyObject *self, PyObject *args)
{
    int fd = 2;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &fd)) return NULL;
    Py_BEGIN_ALLOW_THREADS
    runloom_dump_fibers_fd(fd);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject *m_fiber_stack(PyObject *self, PyObject *arg)
{
    long long id;
    (void)self;
    id = PyLong_AsLongLong(arg);
    if (id == -1 && PyErr_Occurred()) return NULL;
    return runloom_fiber_frames_by_id(id);
}

static PyObject *m_introspect_timestamps(PyObject *self, PyObject *arg)
{
    int on;
    (void)self;
    on = PyObject_IsTrue(arg);
    if (on < 0) return NULL;
    runloom_introspect_set_timestamps(on);
    Py_RETURN_NONE;
}

static PyObject *m_get_introspect_timestamps(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyBool_FromLong(runloom_introspect_get_timestamps());
}

static PyObject *m_set_deadlock_mode(PyObject *self, PyObject *arg)
{
    long mode;
    (void)self;
    mode = PyLong_AsLong(arg);
    if (mode == -1 && PyErr_Occurred()) return NULL;
    runloom_set_deadlock_mode((int)mode);
    Py_RETURN_NONE;
}

static PyObject *m_get_deadlock_mode(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong((long)runloom_deadlock_mode());
}

static PyObject *m_count_deadlocked(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong(runloom_count_deadlockable_fibers(NULL));
}

static PyObject *m_set_wait_reason(PyObject *self, PyObject *arg)
{
    long r;
    (void)self;
    r = PyLong_AsLong(arg);
    if (r == -1 && PyErr_Occurred()) return NULL;
    runloom_set_current_wait_reason((int)r);
    Py_RETURN_NONE;
}

static PyObject *m_set_max_fibers(PyObject *self, PyObject *arg)
{
    long n;
    (void)self;
    n = PyLong_AsLong(arg);
    if (n == -1 && PyErr_Occurred()) return NULL;
    runloom_set_max_fibers(n);
    Py_RETURN_NONE;
}

static PyObject *m_get_max_fibers(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong(runloom_get_max_fibers());
}

static PyObject *m_live_fibers(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong(runloom_live_fibers());
}

/* ---- SIGQUIT-style fiber dump (Go's GOTRACEBACK) ----
 *
 * A RAW C signal handler so the dump works even when the interpreter is
 * wedged (a Python signal.signal handler only runs at a bytecode boundary,
 * which a fully-stalled process never reaches).  The handler does nothing
 * but the async-signal-safe-ish structural dump. */
#if !defined(_WIN32)
#include <signal.h>
static void runloom_traceback_signal_handler(int sig)
{
    (void)sig;
    runloom_dump_fibers_fd(2);
}
#else
/* Windows analog of the SIGQUIT dump: a console control handler that fires on
 * Ctrl+Break (CTRL_BREAK_EVENT), the closest thing to `kill -QUIT`.  Windows
 * runs this on a dedicated thread, so the try-locking, write()-only structural
 * dump is safe here too.  Returning TRUE marks the event handled so the
 * process keeps running (Ctrl+Break would otherwise terminate it). */
static BOOL WINAPI runloom_console_ctrl_handler(DWORD ctrl_type)
{
    if (ctrl_type == CTRL_BREAK_EVENT) {
        runloom_dump_fibers_fd(2);
        return TRUE;
    }
    return FALSE;   /* leave Ctrl-C / close / logoff / shutdown to the default */
}
#endif

static PyObject *m_reset_after_fork(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_after_fork_child();
    Py_RETURN_NONE;
}

/* Test-only: hold / acquire the g-slab balance lock for the fork-deadlock
 * regression test (tests/test_fork_balance_lock.py).  Not for production use. */
static PyObject *m_test_g_balance_hold_ns(PyObject *self, PyObject *arg)
{
    long long ns;
    (void)self;
    ns = PyLong_AsLongLong(arg);
    if (ns == -1 && PyErr_Occurred()) return NULL;
    Py_BEGIN_ALLOW_THREADS
    runloom_g_global_test_hold_ns(ns);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject *m_test_g_balance_acquire(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    Py_BEGIN_ALLOW_THREADS
    runloom_g_global_test_acquire();
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject *m_install_traceback_signal(PyObject *self, PyObject *args)
{
    int signum = -1;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &signum)) return NULL;
#if defined(_WIN32)
    /* On Windows there is no SIGQUIT; the dump fires on Ctrl+Break instead
     * (CTRL_BREAK_EVENT), installed via a console control handler. */
    (void)signum;
    if (!SetConsoleCtrlHandler(runloom_console_ctrl_handler, TRUE)) {
        PyErr_SetFromWindowsErr(0);
        return NULL;
    }
    return PyLong_FromLong(0);   /* no signal number on Windows */
#else
    if (signum < 0) signum = SIGQUIT;
    {
        struct sigaction sa;
        memset(&sa, 0, sizeof sa);
        sa.sa_handler = runloom_traceback_signal_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART;   /* don't EINTR the hubs' syscalls */
        if (sigaction(signum, &sa, NULL) != 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
    }
    return PyLong_FromLong((long)signum);
#endif
}
