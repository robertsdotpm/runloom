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
     "off any goroutine and must not call runloom scheduler ops."},
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
    {"sched_sleep_real", m_sched_sleep_real, METH_O,
     "Like sched_sleep but a REAL wall-clock sleep even under "
     "RUNLOOM_LOGICAL_CLOCK (for the aio keepalive heartbeat)."},
    {"loop_clock",  m_loop_clock,  METH_NOARGS,
     "loop_clock() -> float: the aio event-loop clock -- the logical clock when "
     "RUNLOOM_LOGICAL_CLOCK is set (deterministic call_at/sleep replay), else "
     "monotonic.  Used by runloom.aio loop.time()."},
    {"sched_stop",  m_sched_stop,  METH_NOARGS,
     "Signal the C scheduler to exit its drain loop at the next safe "
     "point.  Background goroutines parked on netpoll/sleep/wake will "
     "be left in their parked state; cleanup happens when the wrapping "
     "Python objects are gc'd."},
    {"sched_reset", m_sched_reset, METH_NOARGS,
     "Drop everything from the scheduler (ready + sleep heap).  Used "
     "by paio.run's cleanup so leftover background goroutines don't "
     "block the next runloom_c.run() with a sleep heap that won't "
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
     "readiness by waking parked goroutines, then return.  Used by runloom.aio "
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
    {"set_stack_scrub", m_set_stack_scrub, METH_O,
     "set_stack_scrub(bool): when True, zero each goroutine's stack as it is\n"
     "recycled so the next goroutine to reuse it can't read the previous\n"
     "one's leftovers (TLS keys, request bodies).  Off by default (costs a\n"
     "stack-sized memset per goroutine completion).  Also via RUNLOOM_STACK_SCRUB=1."},
    {"get_stack_scrub", m_get_stack_scrub, METH_NOARGS,
     "Return whether recycled-stack scrubbing is enabled."},
    {"mn_init",     m_mn_init,     METH_VARARGS,
     "mn_init(n=cpus): start N hub threads.  Returns count."},
    {"mn_go",       m_mn_go,       METH_O,
     "mn_go(callable): spawn a goroutine on a round-robin hub.  "
     "v1 only supports run-to-completion gs (no yield)."},
    {"mn_run",      m_mn_run,      METH_NOARGS,
     "mn_run(): wait for all gs to complete.  Returns total completed."},
    {"mn_fini",     m_mn_fini,     METH_NOARGS,
     "mn_fini(): tear down the hub pool."},
    {"mn_hub_count", m_mn_hub_count, METH_NOARGS,
     "mn_hub_count(): number of M:N hubs currently running (0 if mn_init "
     "was never called or after mn_fini).  Lets the runloom.go() wrapper "
     "dispatch to mn_go inside an M:N run."},
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
    {"goroutines",  m_goroutines,  METH_NOARGS,
     "goroutines() -> list[dict].  A snapshot of every live goroutine: "
     "id, state ('io-wait'/'sleep'/'chan-wait'/'running'/...), blocked_on "
     "class, fd + events (when io-wait), wake_in (when sleeping), age (when "
     "RUNLOOM_INTROSPECT_TIME tracking is on), refcount, noyield, owner.  "
     "Cheap; safe to poll from a watchdog goroutine."},
    {"goroutine_count", m_goroutine_count, METH_NOARGS,
     "goroutine_count() -> int.  Number of live goroutines."},
    {"dump_goroutines", m_dump_goroutines, METH_VARARGS,
     "dump_goroutines(fd=2) -> None.  Write a Go-style structural dump of "
     "every live goroutine (state histogram + per-goroutine line) to fd "
     "using only write(2).  Async-signal-safe-ish: try-locks the registry, "
     "touches no Python objects, so it is usable from a SIGQUIT handler and "
     "when the interpreter is wedged."},
    {"goroutine_stack", m_goroutine_stack, METH_O,
     "goroutine_stack(id) -> (callable_repr, [(file, line, func), ...]).  "
     "Best-effort reconstructed Python stack of one goroutine (deepest "
     "first).  Full stack under per-g-tstate; top frame under the single-"
     "thread (runloom.aio) loop; empty under default M:N (no safe claim).  "
     "Safe: the goroutine is pinned + claimed so it can't resume/free "
     "mid-walk."},
    {"set_introspect_timestamps", m_introspect_timestamps, METH_O,
     "set_introspect_timestamps(bool): when True, stamp each goroutine's "
     "park time so goroutines()/dump report 'age' (how long it has been "
     "wedged).  Off by default (one vDSO clock read per park); also via "
     "RUNLOOM_INTROSPECT_TIME=1."},
    {"reset_after_fork", m_reset_after_fork, METH_NOARGS,
     "reset_after_fork() -> None.  Reset every runloom subsystem to a clean "
     "single-process state after os.fork() -- abandons the (now-dead) M:N "
     "hub + offload threads, re-inits inherited locks, and gives the child "
     "its own netpoll fd.  Registered automatically as an "
     "os.register_at_fork(after_in_child=...) handler; call directly only if "
     "you fork without importing the runloom package.  CHILD process only."},
    {"get_introspect_timestamps", m_get_introspect_timestamps, METH_NOARGS,
     "get_introspect_timestamps() -> bool.  Whether park-age tracking is on."},
    {"set_deadlock_mode", m_set_deadlock_mode, METH_O,
     "set_deadlock_mode(0|1|2): 0=off, 1=warn (print the goroutine dump), "
     "2=raise RuntimeError when the scheduler quiesces with goroutines still "
     "blocked on channels/parks (Go's 'all goroutines are asleep - deadlock!'). "
     "Default 1; also via RUNLOOM_DEADLOCK=off|warn|raise.  Single-thread "
     "scheduler only; aio's clean stop is excluded."},
    {"get_deadlock_mode", m_get_deadlock_mode, METH_NOARGS,
     "get_deadlock_mode() -> int.  Current deadlock-detection mode."},
    {"count_deadlocked", m_count_deadlocked, METH_NOARGS,
     "count_deadlocked() -> int.  Goroutines currently parked on a "
     "channel/park (the set that deadlocks if the scheduler quiesces)."},
    {"set_max_goroutines", m_set_max_goroutines, METH_O,
     "set_max_goroutines(n): cap the number of live goroutines (0 = unlimited, "
     "the default).  Over the cap, go()/spawn raises RuntimeError -- an "
     "admission gate so an unbounded spawn loop can't OOM the process.  Zero "
     "hot-path cost when unset.  Also via RUNLOOM_MAX_GOROUTINES."},
    {"get_max_goroutines", m_get_max_goroutines, METH_NOARGS,
     "get_max_goroutines() -> int.  Current cap (0 = unlimited)."},
    {"live_goroutines", m_live_goroutines, METH_NOARGS,
     "live_goroutines() -> int.  Goroutines admitted under the cap and not yet "
     "finished (0 when no cap is set)."},
    {"install_traceback_signal", m_install_traceback_signal, METH_VARARGS,
     "install_traceback_signal(signum=SIGQUIT) -> int.  Install a RAW C "
     "handler that writes the goroutine dump to stderr -- Go's GOTRACEBACK.  "
     "On POSIX it's `kill -QUIT` (a signal handler that works even when the "
     "interpreter is wedged, unlike a Python signal handler).  On Windows it's "
     "Ctrl+Break / CTRL_BREAK_EVENT via a console control handler (signum is "
     "ignored).  Also via RUNLOOM_TRACEBACK=1."},
    {"_diag_flags", m_diag_flags, METH_NOARGS,
     "_diag_flags() -> int.  Current RUNLOOM_DEBUG flag mask."},
    {"_datastack_sweep_stats", m_datastack_sweep_stats, METH_NOARGS,
     "_datastack_sweep_stats() -> (tail_bytes, resident_bytes, chunks).  "
     "Datastack-tail sweep decompose counters; nonzero only under "
     "RUNLOOM_DATASTACK_DEBUG."},
    {"install_crash_handler", (PyCFunction)m_install_crash_handler,
     METH_VARARGS | METH_KEYWORDS,
     "install_crash_handler(level=None, file=None) -> int.  Install a "
     "fatal-signal (SIGSEGV/SIGBUS/SIGILL/SIGFPE/SIGABRT) handler that, on a "
     "crash, classifies the fault against the per-goroutine guard pages "
     "(goroutine stack overflow vs wild pointer), dumps the live-goroutine "
     "registry, and chains to the default handler so a core / Python traceback "
     "still follows.  level: 'on'|'all'|'backtrace'|'pystack'|'wait'|'gdb' "
     "(comma-separated; default from RUNLOOM_CRASH env, else goroutine dump).  "
     "file: also append the report there.  Returns the flag bitmask installed."},
    {"uninstall_crash_handler", m_uninstall_crash_handler, METH_NOARGS,
     "uninstall_crash_handler() -> None.  Restore the previous signal "
     "dispositions."},
    {"crash_handler_installed", m_crash_handler_installed, METH_NOARGS,
     "crash_handler_installed() -> bool."},
    {"crash_thread_arm", m_crash_thread_arm, METH_NOARGS,
     "crash_thread_arm() -> None.  Arm the calling OS thread's sigaltstack so "
     "the crash handler survives a stack overflow on this thread (done "
     "automatically for runloom's own threads)."},
    {"_crash_selftest_overflow", m_crash_selftest_overflow, METH_NOARGS,
     "_crash_selftest_overflow() -> None.  TEST ONLY: overflow the current C "
     "stack via real-C recursion (does not return).  Run inside a goroutine to "
     "fault into its guard page."},
    {"enable_stack_advice", m_enable_stack_advice, METH_VARARGS,
     "enable_stack_advice(on=True) -> None.  Turn the per-goroutine-kind stack "
     "profiler on/off.  While on, each kind's actual C-stack high-water mark is "
     "measured so stack_advice() can recommend a stack_size.  Advisory only -- "
     "it never changes or persists stack sizes itself.  Off by default."},
    {"stack_advice_enabled", m_stack_advice_enabled, METH_NOARGS,
     "stack_advice_enabled() -> bool."},
    {"stack_advice", m_stack_advice, METH_NOARGS,
     "stack_advice() -> list[dict].  Per goroutine kind: {kind, samples, "
     "max_hwm, reserved, suggested} -- the deepest stack used vs reserved, and "
     "a suggested stack_size (next_pow2(max_hwm*4))."},
    {"reset_stack_advice", m_reset_stack_advice, METH_NOARGS,
     "reset_stack_advice() -> None.  Clear accumulated samples."},
    {"enable_stack_autosize", m_enable_stack_autosize, METH_VARARGS,
     "enable_stack_autosize(on=True) -> None.  Turn the adaptive stack auto-"
     "sizer on/off.  Each goroutine kind starts large when first seen and, once "
     "its real stack use is measured, later goroutines of that kind start at the "
     "learned size (start large, learn down).  In-memory only -- never persisted. "
     "Implies measurement + park-time reclaim.  An explicit stack_size= always "
     "wins.  Off by default."},
    {"stack_autosize_enabled", m_stack_autosize_enabled, METH_NOARGS,
     "stack_autosize_enabled() -> bool."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef runloom_c_module = {
    PyModuleDef_HEAD_INIT,
    "runloom_c",
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
#  define RUNLOOM_FT_OK 1
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
static getattrofunc runloom_orig_module_getattro = NULL;
static PyObject    *runloom_str_dunder_getattr   = NULL;   /* interned "__getattr__" */

static int runloom_running_in_goroutine(void)
{
    /* Both are non-allocating thread-local peeks (never runloom_sched_get). */
    if (runloom_mn_tls_current_g() != NULL) return 1;       /* M:N hub path        */
    return runloom_sched_peek_current() != NULL;            /* single-thread path  */
}

static PyObject *runloom_module_getattro(PyObject *m, PyObject *name)
{
    PyObject *res = PyObject_GenericGetAttr(m, name);
    if (res != NULL) return res;                         /* hit -- common case  */
    if (!PyErr_ExceptionMatches(PyExc_AttributeError)) return NULL;

    /* Miss: keep GenericGetAttr's AttributeError (correct .name/.obj) and look
     * for a module-level __getattr__ (PEP 562), honoured in both domains. */
    PyObject *exc = PyErr_GetRaisedException();          /* clears current exc  */
    PyObject *mdict = PyModule_GetDict(m);               /* borrowed; non-NULL for a module */
    PyObject *getattr = NULL;
    if (mdict != NULL && runloom_str_dunder_getattr != NULL) {
        if (PyDict_GetItemRef(mdict, runloom_str_dunder_getattr, &getattr) < 0) {
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
    if (runloom_running_in_goroutine()) {
        PyErr_SetRaisedException(exc);                   /* plain AttributeError */
        return NULL;                                     /* ...no 32 KB hint    */
    }
    Py_DECREF(exc);
    return runloom_orig_module_getattro(m, name);           /* stock hint off-g    */
}

static void runloom_install_module_getattro(void)
{
    if (runloom_orig_module_getattro != NULL) return;       /* idempotent          */
    runloom_str_dunder_getattr = PyUnicode_InternFromString("__getattr__");
    if (runloom_str_dunder_getattr == NULL) { PyErr_Clear(); return; }
    runloom_orig_module_getattro = PyModule_Type.tp_getattro;
    PyModule_Type.tp_getattro = runloom_module_getattro;
    /* The slot above covers every DIRECT module lookup (getattr/hasattr/mod.x).
     * A ModuleType *subclass* that defines its own __getattr__ instead gets
     * tp_getattro = slot_tp_getattr_hook, which reaches stock module getattr
     * via the __getattribute__ wrapper descriptor, bypassing the slot -- as
     * does an explicit mod.__getattribute__(x).  Repoint that descriptor at
     * our wrapper too, so EVERY path is covered.  (Static-type tp_dict is NULL
     * in 3.12+, so go through PyType_GetDict, not &PyModule_Type->tp_dict.) */
    {
        PyObject *mtdict = PyType_GetDict(&PyModule_Type);   /* new ref */
        if (mtdict != NULL) {
            PyObject *ga = PyDict_GetItemString(mtdict, "__getattribute__");
            if (ga != NULL && Py_IS_TYPE(ga, &PyWrapperDescr_Type)) {
                ((PyWrapperDescrObject *)ga)->d_wrapped =
                    (void *)runloom_module_getattro;
            }
            Py_DECREF(mtdict);
        }
        PyErr_Clear();   /* a failed lookup is non-fatal: slot still covers the common path */
    }
}

PyMODINIT_FUNC PyInit_runloom_c(void)
{
    PyObject *m;
    const char *scrub_env;
    runloom_diag_init();          /* parses RUNLOOM_DEBUG once; cheap; idempotent */
    runloom_introspect_init();    /* goroutine registry lock; reads RUNLOOM_INTROSPECT_TIME */
    runloom_timer_res_init();     /* Windows: 1ms timer resolution (no-op POSIX) */
    scrub_env = getenv("RUNLOOM_STACK_SCRUB");   /* opt-in recycled-stack wipe */
    if (scrub_env && scrub_env[0] && scrub_env[0] != '0') {
        runloom_coro_scrub_set(1);
    }
    {
        /* GOTRACEBACK-style goroutine dump: SIGQUIT on POSIX, Ctrl+Break
         * (CTRL_BREAK_EVENT) on Windows. */
        const char *tb_env = getenv("RUNLOOM_TRACEBACK");
        if (tb_env && tb_env[0] && tb_env[0] != '0') {
#if !defined(_WIN32)
            struct sigaction sa;
            memset(&sa, 0, sizeof sa);
            sa.sa_handler = runloom_traceback_signal_handler;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = SA_RESTART;
            (void)sigaction(SIGQUIT, &sa, NULL);
#else
            (void)SetConsoleCtrlHandler(runloom_console_ctrl_handler, TRUE);
#endif
        }
    }
    runloom_install_module_getattro();   /* goroutine-safe module attr lookup */
    if (PyType_Ready(&RunloomCoroType) < 0) return NULL;
    /* Set by hand rather than in the positional initializer, whose long run
     * of zero slots is easy to miscount: identity-free equality/hash so a
     * goroutine recognises its own current_g() handle across calls. */
    RunloomGType.tp_richcompare = RunloomG_richcompare;
    RunloomGType.tp_hash = RunloomG_hash;
    if (PyType_Ready(&RunloomGType) < 0) return NULL;
    m = PyModule_Create(&runloom_c_module);
    if (m == NULL) {
        return NULL;
    }
    Py_INCREF(&RunloomCoroType);
    Py_INCREF(&RunloomGType);
    if (PyModule_AddObject(m, "G", (PyObject *)&RunloomGType) < 0) {
        Py_DECREF(&RunloomGType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyModule_AddObject(m, "Coro", (PyObject *)&RunloomCoroType) < 0) {
        Py_DECREF(&RunloomCoroType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyType_Ready(&RunloomChanType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(&RunloomChanType);
    if (PyModule_AddObject(m, "Chan", (PyObject *)&RunloomChanType) < 0) {
        Py_DECREF(&RunloomChanType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyType_Ready(&RunloomMutexType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(&RunloomMutexType);
    if (PyModule_AddObject(m, "Mutex", (PyObject *)&RunloomMutexType) < 0) {
        Py_DECREF(&RunloomMutexType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyType_Ready(&RunloomMachineCodeType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(&RunloomMachineCodeType);
    if (PyModule_AddObject(m, "MachineCode",
                           (PyObject *)&RunloomMachineCodeType) < 0) {
        Py_DECREF(&RunloomMachineCodeType);
        Py_DECREF(m);
        return NULL;
    }
    if (runloom_tcpconn_register(m) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    /* Sentinel that wait_fd returns when cancelled via G.cancel_wait_fd();
     * runloom.aio turns it into CancelledError. */
    if (PyModule_AddIntConstant(m, "WAIT_FD_CANCELLED",
                                RUNLOOM_NETPOLL_CANCELLED) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    /* Set up the vectorcall singleton and expose it as `sched_yield`.
     * The interpreter's CALL bytecode will specialize this to the
     * vectorcall fast path, shaving Python-call-dispatch overhead off
     * each yield site. */
    if (PyType_Ready(&RunloomYielderType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    {
        RunloomYielder *y = PyObject_New(RunloomYielder, &RunloomYielderType);
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
