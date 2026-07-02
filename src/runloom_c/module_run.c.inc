/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static PyObject *m_sched_sleep(PyObject *self, PyObject *arg)
{
    double secs;
    runloom_sched_t *s;
    (void)self;
    secs = PyFloat_AsDouble(arg);
    if (secs == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
    s = runloom_sched_get();
    {
        /* Deadline base clock: the single-thread logical clock (aio timer
         * replay, RUNLOOM_LOGICAL_CLOCK) if on, else the M:N controlled logical
         * clock if active, else wall-clock -- exactly as before when both off. */
        double now = runloom_sched_logical_now_or(
                         runloom_mn_logical_now_or(runloom_monotonic_seconds_compat()));
        runloom_sched_sleep_until(s, now + secs);
    }
    Py_RETURN_NONE;
}

/* runloom_c.sched_sleep_real(s) -> None.  Like sched_sleep but a REAL wall-clock
 * sleep even under RUNLOOM_LOGICAL_CLOCK -- for the aio keepalive heartbeat, which
 * must poll in real time and must not advance the logical clock. */
static PyObject *m_sched_sleep_real(PyObject *self, PyObject *arg)
{
    double secs;
    (void)self;
    secs = PyFloat_AsDouble(arg);
    if (secs == -1.0 && PyErr_Occurred()) return NULL;
    runloom_sched_sleep_until_real(runloom_sched_get(),
                                runloom_monotonic_seconds_compat() + secs);
    Py_RETURN_NONE;
}

/* runloom_c.loop_clock() -> float.  The clock runloom.aio's loop.time() reads: the
 * single-thread logical clock when RUNLOOM_LOGICAL_CLOCK is on (so call_at /
 * call_later deadlines line up with sched_sleep's logical deadlines and the
 * timer schedule replays deterministically), else wall-clock monotonic. */
static PyObject *m_loop_clock(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyFloat_FromDouble(
        runloom_sched_logical_now_or(runloom_monotonic_seconds_compat()));
}

static PyObject *m_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = runloom_sched_drain(runloom_sched_get());
    /* Release g-refs held by any leftover timed-park entries (a g woken early by
     * a real wake leaves a future-deadline entry the run never fired); the
     * timer_push incref would otherwise pin those gs past run end. */
    runloom_sched_release_timers(runloom_sched_get());
    /* drain may have run a pending Python signal handler that RAISED
     * (Ctrl-C -> KeyboardInterrupt); surface it instead of a result so it
     * aborts run_forever()/run_until_complete like stock asyncio. */
    if (PyErr_Occurred()) {
        return NULL;
    }
    return PyLong_FromSsize_t(completed);
}

/* runloom_c.run_ready() -> None.  Quiescence-barrier yield: parks the calling
 * fiber until no other fiber is immediately runnable (every currently-
 * ready g, including freshly-woken ones, has run to its next park or to
 * completion), then resumes -- before the scheduler would block on I/O/timers.
 * asyncio's "run the ready callbacks for this loop iteration" boundary,
 * iterated to quiescence.  Must be called from inside a fiber (no-op
 * otherwise). */
static PyObject *m_run_ready(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_sched_run_ready(runloom_sched_get());
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
        result = runloom_netpoll_wait_fd(fd, events, timeout_ns);
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
    runloom_netpoll_unregister((int)fd);
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_release_if_idle(PyObject *self, PyObject *arg)
{
    long fd;
    (void)self;
    fd = PyLong_AsLong(arg);
    if (fd == -1 && PyErr_Occurred()) return NULL;
    runloom_netpoll_release_if_idle((int)fd);
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_cancel_fd(PyObject *self, PyObject *arg)
{
    long fd;
    (void)self;
    fd = PyLong_AsLong(arg);
    if (fd == -1 && PyErr_Occurred()) return NULL;
    runloom_netpoll_cancel_fd((int)fd);
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_cancel_all_parked(PyObject *self, PyObject *unused)
{
    int n;
    (void)self; (void)unused;
    n = runloom_netpoll_cancel_all_parked();
    return PyLong_FromLong((long)n);
}

static PyObject *m_netpoll_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(runloom_netpoll_backend());
}

/* Test-only: read how many times a Windows netpoll fault-injection site fired
 * (see netpoll.c).  Returns -1 for an unknown site / non-Windows build. */
static PyObject *m_fault_count(PyObject *self, PyObject *arg)
{
    const char *name;
    (void)self;
    name = PyUnicode_AsUTF8(arg);
    if (name == NULL) return NULL;
    return PyLong_FromLong(runloom_fault_count(name));
}

static PyObject *m_fault_reset(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_fault_reset();
    Py_RETURN_NONE;
}

static PyObject *m_netpoll_poll(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    /* Non-blocking netpoll drain: deliver any ready fd readiness by waking
     * the parked fibers (it only enqueues them; it never runs them), then
     * return.  runloom.aio calls this on a bare `yield` (asyncio.sleep(0)) so a
     * sleep(0) advances pending socket I/O the way stock asyncio's per-loop-
     * iteration selector poll does.  Without it, sleep(0) loops never deliver
     * I/O on parked fibers because sched_yield bypasses the scheduler
     * drain loop's idle pump (and the aio keepalive keeps it from going idle).
     * runloom_netpoll_pump handles the GIL around the syscall itself. */
    runloom_netpoll_pump(0);
    Py_RETURN_NONE;
}

/* Production introspection: returns a dict of scheduler counters so a
 * stuck deployment can be debugged without attaching a C debugger.
 * Counts cover the single-thread scheduler; M:N hub stats arrive when
 * Phase C grows its own introspection hooks. */
static PyObject *m_stats(PyObject *self, PyObject *unused)
{
    runloom_sched_t *s = runloom_sched_get();
    Py_ssize_t ready;
    PyObject *d;
    (void)self; (void)unused;

    ready = (Py_ssize_t)((s->ready_tail - s->ready_head) & s->ready_mask);
    d = PyDict_New();
    if (d == NULL) return NULL;

#define RUNLOOM_STATS_SET(k, v) do {                                      \
        PyObject *pv = PyLong_FromSsize_t((Py_ssize_t)(v));            \
        if (pv == NULL || PyDict_SetItemString(d, (k), pv) < 0) {      \
            Py_XDECREF(pv); Py_DECREF(d); return NULL;                 \
        }                                                              \
        Py_DECREF(pv);                                                 \
    } while (0)

    RUNLOOM_STATS_SET("ready",     ready);
    RUNLOOM_STATS_SET("sleeping",  s->sleep_size);
    /* netpoll_parked is the GLOBAL count (all scheds/threads).  netpoll_parked_self
     * is THIS thread's sched only -- the right metric for "did the work I ran on
     * this thread leak a parker", immune to parkers stranded on a dead/other
     * thread's sched (e.g. a thread that exited with a fiber still parked;
     * the per-thread sched is intentionally leaked at thread exit). */
    RUNLOOM_STATS_SET("netpoll_parked", runloom_netpoll_parked_count());
    RUNLOOM_STATS_SET("netpoll_parked_self",
                   __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE));
    RUNLOOM_STATS_SET("completed", s->completed);
    RUNLOOM_STATS_SET("running",   (s->current != NULL) ? 1 : 0);
    RUNLOOM_STATS_SET("stack_size_default", s->stack_size);
    RUNLOOM_STATS_SET("ready_capacity", (Py_ssize_t)s->ready_cap);

    {
        runloom_stack_stats_t st;
        runloom_sched_stack_stats(&st);
        RUNLOOM_STATS_SET("stack_hwm",            (Py_ssize_t)st.max_hwm);
        RUNLOOM_STATS_SET("stack_completed",      (Py_ssize_t)st.completed);
        RUNLOOM_STATS_SET("stack_calibrated",     st.calibrated);
        RUNLOOM_STATS_SET("stack_painting",       st.painting);
    }

#undef RUNLOOM_STATS_SET
    /* Strings: backends are useful in the same payload. */
    {
        PyObject *coro    = PyUnicode_FromString(runloom_coro_backend());
        PyObject *netpoll = PyUnicode_FromString(runloom_netpoll_backend());
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
    runloom_sched_set_default_stack_size((size_t)bytes);
    Py_RETURN_NONE;
}

static PyObject *m_get_stack_size(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromSize_t(runloom_sched_get_default_stack_size());
}

static PyObject *m_set_stack_scrub(PyObject *self, PyObject *arg)
{
    int on;
    (void)self;
    on = PyObject_IsTrue(arg);
    if (on < 0) return NULL;
    runloom_coro_scrub_set(on);
    Py_RETURN_NONE;
}

static PyObject *m_get_stack_scrub(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyBool_FromLong(runloom_coro_scrub_enabled());
}

/* ---- M:N scheduler bindings (Phase C) ---- */
static PyObject *m_mn_init(PyObject *self, PyObject *args)
{
    int n = 0;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &n)) return NULL;
    {
        int got = runloom_mn_init(n);
        if (got < 0) return NULL;
        return PyLong_FromLong(got);
    }
}

static PyObject *m_mn_fiber(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"fn", "stack_size", NULL};
    PyObject *callable;
    Py_ssize_t stack_size = 0;
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|n", kwlist,
                                     &callable, &stack_size)) {
        return NULL;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "mn_fiber(): callable required");
        return NULL;
    }
    return runloom_mn_fiber(callable, stack_size > 0 ? (size_t)stack_size : 0);
}

/* fiber_n(fn, n, stack_size=0, indexed=False): bulk-spawn n fibers all running
 * fn(), looping the spawn core in C to skip n Python->C dispatches.  fn still
 * runs as Python in each fiber.  indexed=True calls fn(i) for i in 0..n-1
 * (the fiber's index) instead of fn() -- the per-fiber arg that lets a
 * shared worker distinguish itself (wid). */
static PyObject *m_fiber_n(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"fn", "n", "stack_size", "indexed", NULL};
    PyObject *callable;
    long n;
    Py_ssize_t stack_size = 0;
    int indexed = 0;
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Ol|np", kwlist,
                                     &callable, &n, &stack_size, &indexed)) {
        return NULL;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "fiber_n(): fn must be callable");
        return NULL;
    }
    if (n < 0) n = 0;
    if (runloom_mn_fiber_n(callable, n,
                        stack_size > 0 ? (size_t)stack_size : 0, indexed) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/* EXPERIMENT (docs/dev/HUB_SCALING.md, A1b): immortalize a shared long-lived
 * object so cross-hub incref/decref on it become no-ops.  Implemented in the
 * Py_BUILD_CORE iframe TU (declared here locally -- module.c does not pull
 * runloom_iframe.h). */
extern void runloom_immortalize(PyObject *op);

static PyObject *m_immortalize(PyObject *self, PyObject *op)
{
    (void)self;
    runloom_immortalize(op);
    return Py_NewRef(op);
}

static PyObject *m_mn_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = runloom_mn_run();
    /* mn_run returns -1 with an exception set when RUNLOOM_DEADLOCK=raise and
     * the deadlock census fired -- propagate it instead of masking it as a
     * completed-count of -1. */
    if (PyErr_Occurred()) return NULL;
    return PyLong_FromSsize_t(completed);
}

static PyObject *m_mn_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_mn_fini();
    Py_RETURN_NONE;
}

static PyObject *m_mn_hub_count(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyLong_FromLong(runloom_mn_hub_count());
}

/* ---- fiber() fast entry --------------------------------------------------
 * runloom.fiber is bound to this C function (METH_FASTCALL) instead of the
 * Python wrapper.  The common Go-style spawn -- fiber(callable), no extra args,
 * no stack_size=, hubs running -- goes straight to the M:N spawn core with NO
 * Python wrapper frame and NO args-tuple build (~330k->440k/s naked).  Every
 * richer call (extra args, stack_size=, single-thread, auto per-core mode) is
 * delegated to the Python wrapper registered via _fiber_register, which owns
 * those features.  This collapses runloom.fiber / mn_fiber / fiber_noyield into
 * ONE public entry that is fast AND M:N-correct. */
static PyObject *runloom_py_fiber_full = NULL;   /* the Python full-feature wrapper */
static int       runloom_fiber_force_full = 0;   /* 1 = always delegate (auto mode) */
static int       runloom_fiber_speed = 0;        /* 1 = runloom.fiber == fiber_fast
                                                  * (optimize("throughput")); 0 = the
                                                  * grow-down default (optimize("memory")) */

static PyObject *m_fiber_fast(PyObject *self, PyObject *const *args,
                              Py_ssize_t nargs, PyObject *kwnames)
{
    (void)self;
    if (!runloom_fiber_force_full && nargs == 1 && kwnames == NULL
            && runloom_mn_hub_count() > 0 && PyCallable_Check(args[0])) {
        return runloom_mn_fiber(args[0], 0);   /* fire-and-forget M:N spawn */
    }
    if (runloom_py_fiber_full == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "runloom: fiber fallback not registered");
        return NULL;
    }
    return PyObject_Vectorcall(runloom_py_fiber_full, args, (size_t)nargs, kwnames);
}

static PyObject *m_fiber_register(PyObject *self, PyObject *fn)
{
    (void)self;
    Py_INCREF(fn);
    Py_XSETREF(runloom_py_fiber_full, fn);
    Py_RETURN_NONE;
}

static PyObject *m_fiber_force_full(PyObject *self, PyObject *arg)
{
    int v = PyObject_IsTrue(arg);
    (void)self;
    if (v < 0) return NULL;
    runloom_fiber_force_full = v;
    Py_RETURN_NONE;
}

/* ---- default runloom.fiber: C-side frozen grow-down -----------------------
 * runloom.fiber binds to fiber_grow.  It IS the grow-down auto-sizer, but the
 * FROZEN case -- a callable measured >= GROW_DOWN_SAMPLES times, i.e. the steady
 * state -- is handled here in C: the learned size lives in
 * fn.__dict__[GROW_DOWN_KEY] = [size, count]; read it and spawn at it with no
 * Python frame.  The sampling phase (first GROW_DOWN_SAMPLES spawns of each
 * callable -- which must wrap + measure the C-stack HWM), grow-down disabled,
 * single-thread, rich (args/kwargs), and non-plain-function callables all
 * delegate to the registered Python _fiber_full wrapper.  The constants + the
 * on/off flag are pushed from runtime.py so Python stays the source of truth. */
static PyObject *runloom_grow_down_key = NULL;       /* interned GROW_DOWN_KEY */
static long      runloom_grow_down_samples = (1L << 30); /* never frozen until registered */
static int       runloom_grow_down_active = 1;       /* mirrors runtime.grow_down_active */

static PyObject *m_fiber_register_growdown(PyObject *self, PyObject *args)
{
    PyObject *key;
    long samples;
    (void)self;
    if (!PyArg_ParseTuple(args, "Ul", &key, &samples)) return NULL;
    Py_INCREF(key);
    PyUnicode_InternInPlace(&key);                    /* ptr-compare dict lookups */
    Py_XSETREF(runloom_grow_down_key, key);
    runloom_grow_down_samples = samples;
    Py_RETURN_NONE;
}

static PyObject *m_fiber_set_grow_down(PyObject *self, PyObject *arg)
{
    int v = PyObject_IsTrue(arg);
    (void)self;
    if (v < 0) return NULL;
    runloom_grow_down_active = v;
    Py_RETURN_NONE;
}

/* optimize("throughput") -> 1: runloom.fiber spawns like fiber_fast (max naked-
 * spawn rate, fixed default stack).  optimize("memory") / default -> 0: the
 * grow-down auto-sizer (small right-sized stacks).  Keeps runloom.fiber a STABLE
 * object (no rebinding) so `from runloom import fiber` honors the choice too. */
static PyObject *m_fiber_set_speed(PyObject *self, PyObject *arg)
{
    int v = PyObject_IsTrue(arg);
    (void)self;
    if (v < 0) return NULL;
    runloom_fiber_speed = v;
    Py_RETURN_NONE;
}

static PyObject *m_fiber_grow(PyObject *self, PyObject *const *args,
                              Py_ssize_t nargs, PyObject *kwnames)
{
    (void)self;
    if (runloom_fiber_speed) {
        /* optimize("throughput"): raw fiber_fast spawn.  Wins over force_full
         * (the _AUTO hot latch) and grow-down -- the user explicitly chose max
         * spawn rate, which is incompatible with routing every spawn through the
         * Python _fiber_full frame that auto-hot promotion / grow-down need. */
        if (nargs == 1 && kwnames == NULL && runloom_mn_hub_count() > 0
                && PyCallable_Check(args[0]))
            return runloom_mn_fiber(args[0], 0);
        if (runloom_py_fiber_full != NULL)   /* rich/single-thread: delegate */
            return PyObject_Vectorcall(runloom_py_fiber_full, args,
                                       (size_t)nargs, kwnames);
    }
    if (!runloom_fiber_force_full && runloom_grow_down_active && nargs == 1
            && kwnames == NULL && runloom_mn_hub_count() > 0
            && runloom_grow_down_key != NULL && PyFunction_Check(args[0])) {
        PyObject *fn = args[0];
        PyObject *d = ((PyFunctionObject *)fn)->func_dict;   /* borrowed, may be NULL */
        if (d != NULL && PyDict_CheckExact(d)) {
            /* FT-safe: take a STRONG ref to the store list itself, not a borrow.
             * A concurrent replacement of fn.__dict__[GROW_DOWN_KEY] (e.g. the
             * grow_down_prepare first-write race, or user code) can drop the old
             * list's last reference mid-use; a borrowed pointer would then be a
             * freed object. */
            PyObject *store = NULL;   /* strong ref (or NULL) */
            if (PyDict_GetItemRef(d, runloom_grow_down_key, &store) < 0) {
                PyErr_Clear();
            } else if (store != NULL && PyList_CheckExact(store) && PyList_GET_SIZE(store) == 2) {
                /* FT-safe: a late sampling completion may still be writing store[0];
                 * take strong refs under the list lock rather than borrow + race. */
                PyObject *cnt_o = PyList_GetItemRef(store, 1);
                PyObject *sz_o  = PyList_GetItemRef(store, 0);
                int frozen = 0;
                size_t size = 0;
                if (cnt_o != NULL && sz_o != NULL
                        && PyLong_CheckExact(cnt_o) && PyLong_CheckExact(sz_o)) {
                    long count = PyLong_AsLong(cnt_o);
                    long sz    = PyLong_AsLong(sz_o);
                    if (count >= runloom_grow_down_samples && sz > 0) {
                        frozen = 1;
                        size = (size_t)sz;
                    }
                }
                Py_XDECREF(cnt_o);
                Py_XDECREF(sz_o);
                if (frozen) {
                    /* learned size, no Python frame, DEFERRED stack alloc (lazy
                     * path) so a tight front-load loop doesn't cold-mmap per spawn */
                    Py_DECREF(store);
                    return runloom_mn_fiber_grown(fn, size);
                }
            }
            Py_XDECREF(store);
        }
        /* no store / not frozen / size 0 -> Python sampling path below */
    }
    if (runloom_py_fiber_full == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "runloom: fiber fallback not registered");
        return NULL;
    }
    return PyObject_Vectorcall(runloom_py_fiber_full, args, (size_t)nargs, kwnames);
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
    if (runloom_preempt_init(quantum_us) < 0) return NULL;
    Py_RETURN_NONE;
#endif
}

static PyObject *m_preempt_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_preempt_fini();
    Py_RETURN_NONE;
}

