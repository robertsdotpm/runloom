/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

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
        /* Deadline base clock: the single-thread logical clock (aio timer
         * replay, PYGO_LOGICAL_CLOCK) if on, else the M:N controlled logical
         * clock if active, else wall-clock -- exactly as before when both off. */
        double now = pygo_sched_logical_now_or(
                         pygo_mn_logical_now_or(pygo_monotonic_seconds_compat()));
        pygo_sched_sleep_until(s, now + secs);
    }
    Py_RETURN_NONE;
}

/* pygo_core.sched_sleep_real(s) -> None.  Like sched_sleep but a REAL wall-clock
 * sleep even under PYGO_LOGICAL_CLOCK -- for the aio keepalive heartbeat, which
 * must poll in real time and must not advance the logical clock. */
static PyObject *m_sched_sleep_real(PyObject *self, PyObject *arg)
{
    double secs;
    (void)self;
    secs = PyFloat_AsDouble(arg);
    if (secs == -1.0 && PyErr_Occurred()) return NULL;
    pygo_sched_sleep_until_real(pygo_sched_get(),
                                pygo_monotonic_seconds_compat() + secs);
    Py_RETURN_NONE;
}

/* pygo_core.loop_clock() -> float.  The clock pygo.aio's loop.time() reads: the
 * single-thread logical clock when PYGO_LOGICAL_CLOCK is on (so call_at /
 * call_later deadlines line up with sched_sleep's logical deadlines and the
 * timer schedule replays deterministically), else wall-clock monotonic. */
static PyObject *m_loop_clock(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyFloat_FromDouble(
        pygo_sched_logical_now_or(pygo_monotonic_seconds_compat()));
}

static PyObject *m_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = pygo_sched_drain(pygo_sched_get());
    /* drain may have run a pending Python signal handler that RAISED
     * (Ctrl-C -> KeyboardInterrupt); surface it instead of a result so it
     * aborts run_forever()/run_until_complete like stock asyncio. */
    if (PyErr_Occurred()) {
        return NULL;
    }
    return PyLong_FromSsize_t(completed);
}

/* pygo_core.run_ready() -> None.  Quiescence-barrier yield: parks the calling
 * goroutine until no other goroutine is immediately runnable (every currently-
 * ready g, including freshly-woken ones, has run to its next park or to
 * completion), then resumes -- before the scheduler would block on I/O/timers.
 * asyncio's "run the ready callbacks for this loop iteration" boundary,
 * iterated to quiescence.  Must be called from inside a goroutine (no-op
 * otherwise). */
static PyObject *m_run_ready(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_run_ready(pygo_sched_get());
    Py_RETURN_NONE;
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
        /* wait_fd may have run a Python signal handler that raised (an alarm
         * handler, Ctrl-C); propagate that instead of overwriting it with
         * OSError.  This is what lets a signal interrupt a cooperative
         * select()/poll() and raise out of the call, like real CPython. */
        return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
    }
    return PyLong_FromLong((long)result);
}

static PyObject *m_netpoll_unregister(PyObject *self, PyObject *arg)
{
    long fd;
    (void)self;
    fd = PyLong_AsLong(arg);
    if (fd == -1 && PyErr_Occurred()) return NULL;
    pygo_netpoll_unregister((int)fd);
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(pygo_netpoll_backend());
}

/* Test-only: read how many times a Windows netpoll fault-injection site fired
 * (see netpoll.c).  Returns -1 for an unknown site / non-Windows build. */
static PyObject *m_fault_count(PyObject *self, PyObject *arg)
{
    const char *name;
    (void)self;
    name = PyUnicode_AsUTF8(arg);
    if (name == NULL) return NULL;
    return PyLong_FromLong(pygo_fault_count(name));
}

static PyObject *m_fault_reset(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_fault_reset();
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_poll(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    /* Non-blocking netpoll drain: deliver any ready fd readiness by waking
     * the parked goroutines (it only enqueues them; it never runs them), then
     * return.  pygo.aio calls this on a bare `yield` (asyncio.sleep(0)) so a
     * sleep(0) advances pending socket I/O the way stock asyncio's per-loop-
     * iteration selector poll does.  Without it, sleep(0) loops never deliver
     * I/O on parked goroutines because sched_yield bypasses the scheduler
     * drain loop's idle pump (and the aio keepalive keeps it from going idle).
     * pygo_netpoll_pump handles the GIL around the syscall itself. */
    pygo_netpoll_pump(0);
    Py_RETURN_NONE;
}

/* Production introspection: returns a dict of scheduler counters so a
 * stuck deployment can be debugged without attaching a C debugger.
 * Counts cover the single-thread scheduler; M:N hub stats arrive when
 * Phase C grows its own introspection hooks. */
static PyObject *m_stats(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    Py_ssize_t ready;
    PyObject *d;
    (void)self; (void)unused;

    ready = (Py_ssize_t)((s->ready_tail - s->ready_head) & s->ready_mask);
    d = PyDict_New();
    if (d == NULL) return NULL;

#define PYGO_STATS_SET(k, v) do {                                      \
        PyObject *pv = PyLong_FromSsize_t((Py_ssize_t)(v));            \
        if (pv == NULL || PyDict_SetItemString(d, (k), pv) < 0) {      \
            Py_XDECREF(pv); Py_DECREF(d); return NULL;                 \
        }                                                              \
        Py_DECREF(pv);                                                 \
    } while (0)

    PYGO_STATS_SET("ready",     ready);
    PYGO_STATS_SET("sleeping",  s->sleep_size);
    /* netpoll_parked is the GLOBAL count (all scheds/threads).  netpoll_parked_self
     * is THIS thread's sched only -- the right metric for "did the work I ran on
     * this thread leak a parker", immune to parkers stranded on a dead/other
     * thread's sched (e.g. a thread that exited with a goroutine still parked;
     * the per-thread sched is intentionally leaked at thread exit). */
    PYGO_STATS_SET("netpoll_parked", pygo_netpoll_parked_count());
    PYGO_STATS_SET("netpoll_parked_self",
                   __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE));
    PYGO_STATS_SET("completed", s->completed);
    PYGO_STATS_SET("running",   (s->current != NULL) ? 1 : 0);
    PYGO_STATS_SET("stack_size_default", s->stack_size);
    PYGO_STATS_SET("ready_capacity", (Py_ssize_t)s->ready_cap);

    {
        pygo_stack_stats_t st;
        pygo_sched_stack_stats(&st);
        PYGO_STATS_SET("stack_hwm",            (Py_ssize_t)st.max_hwm);
        PYGO_STATS_SET("stack_completed",      (Py_ssize_t)st.completed);
        PYGO_STATS_SET("stack_calibrated",     st.calibrated);
        PYGO_STATS_SET("stack_painting",       st.painting);
    }

#undef PYGO_STATS_SET
    /* Strings: backends are useful in the same payload. */
    {
        PyObject *coro    = PyUnicode_FromString(pygo_coro_backend());
        PyObject *netpoll = PyUnicode_FromString(pygo_netpoll_backend());
        if (!coro || !netpoll ||
            PyDict_SetItemString(d, "backend", coro) < 0 ||
            PyDict_SetItemString(d, "netpoll", netpoll) < 0) {
            Py_XDECREF(coro); Py_XDECREF(netpoll); Py_DECREF(d);
            return NULL;
        }
        Py_DECREF(coro); Py_DECREF(netpoll);
    }
    return d;
}

static PyObject *m_set_stack_size(PyObject *self, PyObject *arg)
{
    Py_ssize_t bytes;
    (void)self;
    bytes = PyNumber_AsSsize_t(arg, PyExc_OverflowError);
    if (bytes == -1 && PyErr_Occurred()) return NULL;
    if (bytes <= 0) {
        PyErr_SetString(PyExc_ValueError,
                        "set_stack_size: bytes must be > 0");
        return NULL;
    }
    pygo_sched_set_default_stack_size((size_t)bytes);
    Py_RETURN_NONE;
}

static PyObject *m_get_stack_size(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromSize_t(pygo_sched_get_default_stack_size());
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

/* Preemption: 3.13t only.  Refuse on other versions with a clear error
 * rather than silently failing -- the timer + Py_AddPendingCall path
 * has only been validated on free-threaded 3.13, and the M:N hub model
 * that needs preemption most only makes sense there. */
static PyObject *m_preempt_init(PyObject *self, PyObject *args)
{
    long quantum_us = 10000;
    (void)self;
    if (!PyArg_ParseTuple(args, "|l", &quantum_us)) return NULL;
#if !(defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030D0000)
    PyErr_SetString(PyExc_RuntimeError,
                    "preempt_init: free-threaded Python 3.13t only "
                    "(GIL builds and pre-3.13 not supported yet)");
    return NULL;
#else
    if (pygo_preempt_init(quantum_us) < 0) return NULL;
    Py_RETURN_NONE;
#endif
}

static PyObject *m_preempt_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_preempt_fini();
    Py_RETURN_NONE;
}

