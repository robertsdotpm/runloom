/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static PyMethodDef module_methods[] = {
    {"select", (PyCFunction)m_select, METH_VARARGS | METH_KEYWORDS,
     "select(cases, default=False): wait on multiple channels.  Each "
     "case is ('recv', ch) or ('send', ch, value).  Returns "
     "(index, (value, ok)) for recv or (index, None) for send.  With "
     "default=True returns -1 if no case is immediately ready."},
    {"yield_",      m_yield,       METH_NOARGS,
     "Yield from inside a raw Coro (a no-op outside one)."},
    {"backend",     m_backend,     METH_NOARGS,
     "Return the active stack-switch backend name."},
    {"thread_init", m_thread_init, METH_NOARGS,
     "Per-OS-thread setup; idempotent."},
    {"thread_fini", m_thread_fini, METH_NOARGS,
     "Per-OS-thread teardown."},
    {"warmup", m_warmup, METH_VARARGS,
     "warmup(n, stack_size=131072) -> int: pre-mmap n stacks of "
     "stack_size bytes into the per-thread stack pool, eliminating "
     "first-spawn mmap latency for servers that know they're about "
     "to spawn N goroutines.  Returns the number actually allocated."},
    {"tcp_recv", m_tcp_recv, METH_VARARGS,
     "tcp_recv(fd, writable_buffer, n, flags=0) -> bytes_received.  "
     "C-level recv into a pre-allocated buffer with cooperative "
     "blocking; bypasses socket.recv_into's Python frame dispatch "
     "AND the BlockingIOError-on-EAGAIN raise/catch cost."},
    {"tcp_recv_alloc", m_tcp_recv_alloc, METH_VARARGS,
     "tcp_recv_alloc(fd, n, flags=0) -> bytes.  Like socket.recv but "
     "loops in C on EAGAIN via netpoll; no BlockingIOError raise/catch."},
    {"tcp_send", m_tcp_send, METH_VARARGS,
     "tcp_send(fd, bytes_like, flags=0) -> bytes_sent.  C-level "
     "sendall with cooperative blocking; loops until all bytes sent."},
    {"tcp_send_once", m_tcp_send_once, METH_VARARGS,
     "tcp_send_once(fd, bytes_like, flags=0) -> int.  Single send "
     "syscall (may return short); parks on EAGAIN until writable."},
    {"fd_read", m_fd_read, METH_VARARGS,
     "fd_read(fd, writable_buffer, n) -> bytes_read.  POSIX read(2) "
     "with cooperative blocking via netpoll.  Works on pipes, ttys, "
     "any pollable fd.  On Windows file fds aren't pollable: blocks "
     "the OS thread (use the monkey.py thread-pool path instead)."},
    {"fd_write", m_fd_write, METH_VARARGS,
     "fd_write(fd, bytes_like) -> bytes_written.  POSIX write(2) "
     "loop with cooperative blocking.  Same Windows caveat as fd_read."},
    {"file_read", m_file_read, METH_VARARGS,
     "file_read(fd, writable_buffer, n, offset=-1) -> bytes_read.  "
     "Uses io_uring on Linux >=5.1, falls back to pread/read.  "
     "Cooperative-ish: avoids the thread-pool roundtrip the monkey-"
     "patch path takes for regular files."},
    {"file_write", m_file_write, METH_VARARGS,
     "file_write(fd, bytes_like, offset=-1) -> bytes_written.  "
     "io_uring on Linux, plain write/pwrite elsewhere."},
    {"iouring_available", m_iouring_available, METH_NOARGS,
     "True if the io_uring kernel interface is usable (Linux 5.1+)."},
    /* C-scheduler fast path. */
    {"go",          (PyCFunction)m_go, METH_VARARGS | METH_KEYWORDS,
     "go(fn, stack_size=None): spawn a goroutine via the C scheduler.\n"
     "Returns a G handle.  stack_size overrides the scheduler default\n"
     "(post-calibration) for this one goroutine -- use when the entry\n"
     "function is known to recurse deeply or call into a C extension\n"
     "that consumes large amounts of C stack."},
    {"blocking",    (PyCFunction)m_blocking, METH_VARARGS | METH_KEYWORDS,
     "blocking(fn, *args, **kwargs): run fn(*args, **kwargs) on the\n"
     "blocking-offload thread pool, parking the calling goroutine until it\n"
     "returns.  Use for non-preemptible blocking calls (DNS, blocking\n"
     "sockets/files, GIL-releasing C extensions) that would otherwise wedge\n"
     "the goroutine's hub and strand everything queued behind it.  fn runs\n"
     "off any goroutine and must not call pygo scheduler ops."},
    {"go_noyield",  m_go_noyield,  METH_O,
     "Spawn a goroutine that the caller promises will run to "
     "completion without yielding.  Skips the per-g datastack/snap/"
     "load-sched dance -- ~150-400 ns/g faster than go() for pure-"
     "compute callables.  If the callable does yield (sched_yield, "
     "sched_sleep, wait_fd, monkey-patched I/O), behaviour is "
     "undefined.  Use only when you know the work is CPU-bound."},
    {"sched_yield_classic", m_sched_yield, METH_NOARGS,
     "Yield the current goroutine (METH_NOARGS PyCFunction form, kept "
     "for benchmarking against the vectorcall singleton)."},
    {"sched_sleep", m_sched_sleep, METH_O,
     "Sleep the current goroutine for N seconds (C scheduler aware)."},
    {"sched_stop",  m_sched_stop,  METH_NOARGS,
     "Signal the C scheduler to exit its drain loop at the next safe "
     "point.  Background goroutines parked on netpoll/sleep/wake will "
     "be left in their parked state; cleanup happens when the wrapping "
     "Python objects are gc'd."},
    {"sched_reset", m_sched_reset, METH_NOARGS,
     "Drop everything from the scheduler (ready + sleep heap).  Used "
     "by paio.run's cleanup so leftover background goroutines don't "
     "block the next pygo_core.run() with a sleep heap that won't "
     "drain for minutes.  Returns (n_ready_dropped, n_sleep_dropped)."},
    {"park_self",   m_park_self,   METH_NOARGS,
     "Park the current goroutine until G.wake() is called on its "
     "handle.  Race-safe: a wake that arrives before the park is "
     "consumed and the park returns immediately."},
    {"current_g",   m_current_g,   METH_NOARGS,
     "Return a G handle to the currently-running goroutine, or None "
     "if called outside one.  Used together with park_self/G.wake to "
     "implement lightweight per-task wake primitives without the "
     "overhead of a Chan."},
    {"run",         m_run,         METH_NOARGS,
     "Drive the C scheduler until all goroutines finish.  Returns count."},
    {"run_ready",   m_run_ready,   METH_NOARGS,
     "run_ready(): quiescence-barrier yield.  Park the calling goroutine "
     "until no other goroutine is immediately runnable (every ready g, "
     "including freshly-woken ones, has run to its next park/completion), "
     "then resume -- before the scheduler blocks on I/O/timers.  asyncio's "
     "one-loop-iteration boundary, iterated to quiescence."},
    {"wait_fd",     m_wait_fd,     METH_VARARGS,
     "wait_fd(fd, events, timeout_ms=-1): park the current goroutine "
     "until fd is ready.  events is a bitmask: 1=read, 2=write."},
    {"netpoll_backend", m_netpoll_backend, METH_NOARGS,
     "Return active netpoll backend name (\"epoll\", \"kqueue\", \"select\")."},
    {"_fault_count", m_fault_count, METH_O,
     "_fault_count(site): test-only.  Times the Windows netpoll fault-injection "
     "site (WSAPOLL/SELECT/IOCP_WAIT/IOCP_SUBMIT) fired; -1 if unknown/non-Win."},
    {"_fault_reset", m_fault_reset, METH_NOARGS,
     "_fault_reset(): test-only.  Clear all Windows fault-injection counters."},
    {"netpoll_poll", m_netpoll_poll, METH_NOARGS,
     "netpoll_poll(): non-blocking netpoll drain -- deliver any ready fd "
     "readiness by waking parked goroutines, then return.  Used by pygo.aio "
     "so asyncio.sleep(0) advances pending I/O like a stock loop iteration."},
    {"netpoll_unregister", m_netpoll_unregister, METH_O,
     "netpoll_unregister(fd): clear the netpoll registration cache "
     "bit for fd.  Call from socket close so fd reuse re-registers "
     "cleanly under the edge-triggered register-once scheme."},
    {"stats",       m_stats,       METH_NOARGS,
     "Return a dict of scheduler counters: ready, sleeping, "
     "netpoll_parked, completed, running, plus backend names.  "
     "Cheap; safe to poll from a watchdog goroutine in production."},
    {"set_stack_size", m_set_stack_size, METH_O,
     "set_stack_size(bytes): override the per-goroutine default stack\n"
     "size and freeze calibration.  Use to lock in a known-good size\n"
     "before spawning, or to bump after seeing a near-overflow."},
    {"get_stack_size", m_get_stack_size, METH_NOARGS,
     "Return the current per-goroutine default stack size in bytes."},
    {"mn_init",     m_mn_init,     METH_VARARGS,
     "mn_init(n=cpus): start N hub threads.  Returns count."},
    {"mn_go",       m_mn_go,       METH_O,
     "mn_go(callable): spawn a goroutine on a round-robin hub.  "
     "v1 only supports run-to-completion gs (no yield)."},
    {"mn_run",      m_mn_run,      METH_NOARGS,
     "mn_run(): wait for all gs to complete.  Returns total completed."},
    {"mn_fini",     m_mn_fini,     METH_NOARGS,
     "mn_fini(): tear down the hub pool."},
    {"preempt_init", m_preempt_init, METH_VARARGS,
     "preempt_init(quantum_us=10000): start the time-sliced preemption "
     "timer.  3.13t only.  Goroutines without explicit sched_yield "
     "calls will be preempted every quantum_us microseconds via a "
     "Py_AddPendingCall hook into CPython's eval_breaker."},
    {"preempt_fini", m_preempt_fini, METH_NOARGS,
     "preempt_fini(): stop the preemption timer (if running)."},
    {"_self_check", m_self_check, METH_VARARGS,
     "_self_check(verbose=0) -> int.  Walk every live scheduler/netpoll "
     "data structure and assert invariants (cycle-free lists, counters "
     "match, no self-loops).  Returns the count of violations.  Cheap "
     "(O(parked)); safe to call between bench iterations."},
    {"_diag_dump", m_diag_dump, METH_VARARGS,
     "_diag_dump(fd=2) -> None.  Dump every OS thread's lifecycle "
     "event ring to fd (default stderr).  Newest-first."},
    {"_dump_parkers", m_dump_parkers, METH_NOARGS,
     "_dump_parkers() -> None.  Dump every parked netpoll parker "
     "(fd/g/hub/commit) to stderr.  Diagnostic."},
    {"_diag_flags", m_diag_flags, METH_NOARGS,
     "_diag_flags() -> int.  Current PYGO_DEBUG flag mask."},
    {"_datastack_sweep_stats", m_datastack_sweep_stats, METH_NOARGS,
     "_datastack_sweep_stats() -> (tail_bytes, resident_bytes, chunks).  "
     "Datastack-tail sweep decompose counters; nonzero only under "
     "PYGO_DATASTACK_DEBUG."},
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

/* --------------------------------------------------------------------------
 * Goroutine-safe module attribute lookup.
 *
 * CPython 3.13's module getattr, on a *missing* attribute, calls
 * _PyModule_IsPossiblyShadowing to append a "did you shadow a stdlib module?"
 * hint to the AttributeError.  That helper reserves ~32 KB of C stack (two
 * wchar_t[MAXPATHLEN] path buffers) -- more than a whole default goroutine
 * stack -- so an ordinary attribute miss (hasattr(mod, x) == False, a
 * namespace __getattr__ proxy, ...) inside a goroutine overflows the stack and
 * crashes (the only thing the hint adds is a nicer error string).
 *
 * Replace PyModule_Type's getattr slot with a thin wrapper: do the normal
 * lookup (every hit, plus a proper AttributeError on miss), honour a
 * module-level __getattr__, and then -- only when running on a goroutine's
 * small swapped stack -- raise that AttributeError directly, skipping the
 * 32 KB hint.  Off-goroutine we defer to the stock implementation, so the hint
 * and every other behaviour are byte-for-byte preserved on the main thread.
 * -------------------------------------------------------------------------- */
static getattrofunc pygo_orig_module_getattro = NULL;
static PyObject    *pygo_str_dunder_getattr   = NULL;   /* interned "__getattr__" */

static int pygo_running_in_goroutine(void)
{
    /* Both are non-allocating thread-local peeks (never pygo_sched_get). */
    if (pygo_mn_tls_current_g() != NULL) return 1;       /* M:N hub path        */
    return pygo_sched_peek_current() != NULL;            /* single-thread path  */
}

static PyObject *pygo_module_getattro(PyObject *m, PyObject *name)
{
    PyObject *res = PyObject_GenericGetAttr(m, name);
    if (res != NULL) return res;                         /* hit -- common case  */
    if (!PyErr_ExceptionMatches(PyExc_AttributeError)) return NULL;

    /* Miss: keep GenericGetAttr's AttributeError (correct .name/.obj) and look
     * for a module-level __getattr__ (PEP 562), honoured in both domains. */
    PyObject *exc = PyErr_GetRaisedException();          /* clears current exc  */
    PyObject *mdict = PyModule_GetDict(m);               /* borrowed; non-NULL for a module */
    PyObject *getattr = NULL;
    if (mdict != NULL && pygo_str_dunder_getattr != NULL) {
        if (PyDict_GetItemRef(mdict, pygo_str_dunder_getattr, &getattr) < 0) {
            Py_DECREF(exc);
            return NULL;                                 /* lookup error        */
        }
    }
    if (getattr != NULL) {
        Py_DECREF(exc);
        PyObject *r = PyObject_CallOneArg(getattr, name);
        Py_DECREF(getattr);
        return r;
    }
    if (pygo_running_in_goroutine()) {
        PyErr_SetRaisedException(exc);                   /* plain AttributeError */
        return NULL;                                     /* ...no 32 KB hint    */
    }
    Py_DECREF(exc);
    return pygo_orig_module_getattro(m, name);           /* stock hint off-g    */
}

static void pygo_install_module_getattro(void)
{
    if (pygo_orig_module_getattro != NULL) return;       /* idempotent          */
    pygo_str_dunder_getattr = PyUnicode_InternFromString("__getattr__");
    if (pygo_str_dunder_getattr == NULL) { PyErr_Clear(); return; }
    pygo_orig_module_getattro = PyModule_Type.tp_getattro;
    PyModule_Type.tp_getattro = pygo_module_getattro;
    /* This covers every direct module attribute lookup -- getattr(mod, x),
     * hasattr(mod, x), mod.x -- which is how the overflow is reached in
     * practice (feature-detection misses on user/cwd modules).  It does NOT
     * cover a ModuleType *subclass* that defines its own __getattr__: that
     * gets tp_getattro = slot_tp_getattr_hook, which reaches the stock module
     * getattr through the __getattribute__ wrapper descriptor, bypassing this
     * slot.  Repointing that descriptor was tried and crashes PyInit, so the
     * subclass-__getattr__ pattern is left alone -- use the equivalent
     * module-level __getattr__ *function* (PEP 562) instead, which is both the
     * common idiom and inherently safe (the stock getattr finds and calls it
     * before ever reaching the 32 KB hint). */
}

PyMODINIT_FUNC PyInit_pygo_core(void)
{
    PyObject *m;
    pygo_diag_init();          /* parses PYGO_DEBUG once; cheap; idempotent */
    pygo_timer_res_init();     /* Windows: 1ms timer resolution (no-op POSIX) */
    pygo_install_module_getattro();   /* goroutine-safe module attr lookup */
    if (PyType_Ready(&PygoCoroType) < 0) return NULL;
    /* Set by hand rather than in the positional initializer, whose long run
     * of zero slots is easy to miscount: identity-free equality/hash so a
     * goroutine recognises its own current_g() handle across calls. */
    PygoGType.tp_richcompare = PygoG_richcompare;
    PygoGType.tp_hash = PygoG_hash;
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
    if (PyType_Ready(&PygoChanType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(&PygoChanType);
    if (PyModule_AddObject(m, "Chan", (PyObject *)&PygoChanType) < 0) {
        Py_DECREF(&PygoChanType);
        Py_DECREF(m);
        return NULL;
    }
    if (pygo_tcpconn_register(m) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    /* Sentinel that wait_fd returns when cancelled via G.cancel_wait_fd();
     * pygo.aio turns it into CancelledError. */
    if (PyModule_AddIntConstant(m, "WAIT_FD_CANCELLED",
                                PYGO_NETPOLL_CANCELLED) < 0) {
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
