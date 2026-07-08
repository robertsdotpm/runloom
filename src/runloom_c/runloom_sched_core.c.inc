/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static int       runloom_cal_frozen = 0;
/* The stack-calibration globals (runloom_cal_frozen/max_hwm/completed/default)
 * are written by runloom_cal_record, which runs on EVERY fiber completion
 * during the brief startup calibration window -- concurrently across M:N hubs.
 * Guard them with a lock (self-initialising so it needs no PyInit hook on
 * Windows, where CRITICAL_SECTION can't be statically initialised).  Cost is
 * zero in steady state: once frozen, cal_record early-returns on the atomic
 * frozen flag and never touches the lock. */
static runloom_mutex_t runloom_cal_lock = RUNLOOM_MUTEX_STATIC_INIT;
static int runloom_cal_lock_inited = 0;

static void runloom_cal_ensure_lock(void)
{
#if defined(RUNLOOM_OS_WINDOWS)
    int st = __atomic_load_n(&runloom_cal_lock_inited, __ATOMIC_ACQUIRE);
    if (st == 2) return;
    if (st == 0 &&
        __atomic_compare_exchange_n(&runloom_cal_lock_inited, &st, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&runloom_cal_lock);
        __atomic_store_n(&runloom_cal_lock_inited, 2, __ATOMIC_RELEASE);
        return;
    }
    while (__atomic_load_n(&runloom_cal_lock_inited, __ATOMIC_ACQUIRE) != 2) { }
#else
    (void)runloom_cal_lock_inited;   /* POSIX: PTHREAD_MUTEX_INITIALIZER suffices */
#endif
}

/* After fork(): the child inherits runloom_cal_lock in whatever state it had at
 * fork -- possibly LOCKED by a hub/drain thread that did not survive -- so the
 * next calibration acquire (runloom_cal_record / get/set_default_stack_size /
 * stack_stats) would deadlock against the dead owner.  Re-init it, exactly as
 * runloom_mn_reset_after_fork does for runloom_global_runq_lock ("may be inherited
 * held").  The child is single-threaded at this point, so this is unraced; the
 * inited=2 store keeps the Windows lazy-init state coherent (fork is POSIX-only,
 * so that branch is moot there, but the store is harmless). */
void runloom_cal_reset_after_fork(void)
{
    runloom_mutex_init(&runloom_cal_lock);
    __atomic_store_n(&runloom_cal_lock_inited, 2, __ATOMIC_RELAXED);
}

static size_t runloom_next_pow2(size_t v)
{
    size_t p = 1;
    while (p < v && p < RUNLOOM_MAX_STACK_SIZE) p <<= 1;
    return p;
}

/* Called from drain after a g completes.  Scans the g's stack for
 * HWM, updates the running max, and freezes the calibration window
 * if we've collected enough samples. */
static void runloom_cal_record(runloom_g_t *g)
{
    size_t hwm;
    int freeze_now = 0;
    size_t chosen = 0;
    if (g == NULL || g->coro == NULL) return;
    /* Stack-advice profiler: fold this g's HWM into its kind, independent of
     * whether the global calibration window has frozen. */
    runloom_advice_record_g(g);
    /* Fast path: once frozen, skip the lock entirely (steady state). */
    if (__atomic_load_n(&runloom_cal_frozen, __ATOMIC_ACQUIRE) ||
        !runloom_coro_paint_enabled()) return;
    hwm = runloom_coro_scan_hwm(g->coro);
    runloom_cal_ensure_lock();
    RUNLOOM_RLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    if (runloom_cal_frozen) {                 /* lost the freeze race -- done */
        RUNLOOM_RUNLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
        return;
    }
    if (hwm > runloom_cal_max_hwm) runloom_cal_max_hwm = hwm;
    runloom_cal_completed++;
    if (runloom_cal_completed >= RUNLOOM_CAL_TARGET) {
        size_t bound;
        runloom_delay_inject(RUNLOOM_DLY_CAL_FREEZE);   /* widen freeze transition */
        RUNLOOM_EVT(RUNLOOM_EVT_CAL_FREEZE, 0, 0, (long long)runloom_cal_max_hwm);
        bound = runloom_cal_max_hwm * RUNLOOM_CAL_SAFETY;
        chosen = runloom_next_pow2(bound);
        /* Floor auto-calibration at the safe default: it adapts UP for
         * stack-hungry programs but never shrinks below 512 KB (which would
         * re-expose Decimal/crypto/deep frames).  Explicit
         * set_default_stack_size() can still go down to RUNLOOM_MIN_STACK_SIZE. */
        if (chosen < RUNLOOM_DEFAULT_STACK_SIZE) chosen = RUNLOOM_DEFAULT_STACK_SIZE;
        if (chosen > RUNLOOM_MAX_STACK_SIZE) chosen = RUNLOOM_MAX_STACK_SIZE;
        runloom_cal_default = chosen;
        __atomic_store_n(&runloom_cal_frozen, 1, __ATOMIC_RELEASE);
        freeze_now = 1;
    }
    RUNLOOM_RUNLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    if (freeze_now) {
        runloom_coro_paint_set(0);
        /* cal_record runs inside drain on this thread's own sched; bump it so
         * the freeze takes effect immediately here.  Other threads' scheds pick
         * up runloom_cal_default when they spawn their next g.  Done OUTSIDE the
         * cal lock to avoid holding it across coro_paint_set / sched_get. */
        runloom_sched_get()->stack_size = (Py_ssize_t)chosen;
    }
}

void runloom_sched_set_default_stack_size(size_t bytes)
{
    if (bytes < RUNLOOM_MIN_STACK_SIZE) bytes = RUNLOOM_MIN_STACK_SIZE;
    /* On free-threaded 3.14 the SP-vs-soft_limit guard needs a roomier physical
     * stack than the 16KB 3.13 floor (else a false RecursionError -- p226). */
    bytes = runloom_fiber_stack_floor(bytes);
    if (bytes > RUNLOOM_MAX_STACK_SIZE) bytes = RUNLOOM_MAX_STACK_SIZE;
    runloom_cal_ensure_lock();
    RUNLOOM_RLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    runloom_cal_default = bytes;
    __atomic_store_n(&runloom_cal_frozen, 1, __ATOMIC_RELEASE);
    RUNLOOM_RUNLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    runloom_coro_paint_set(0);
    /* Update the calling thread's sched immediately; other threads pick up
     * runloom_cal_default on their next spawn. */
    runloom_sched_get()->stack_size = (Py_ssize_t)bytes;
}

size_t runloom_sched_get_default_stack_size(void)
{
    size_t v;
    runloom_cal_ensure_lock();
    RUNLOOM_RLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    v = runloom_cal_default;
    RUNLOOM_RUNLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    return v;
}

void runloom_sched_stack_stats(runloom_stack_stats_t *out)
{
    if (out == NULL) return;
    runloom_cal_ensure_lock();
    RUNLOOM_RLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    out->default_size = runloom_cal_default;
    out->max_hwm      = runloom_cal_max_hwm;
    out->completed    = runloom_cal_completed;
    out->calibrated   = runloom_cal_frozen;
    RUNLOOM_RUNLOCK(&runloom_cal_lock, RUNLOOM_RANK_CAL);
    out->painting     = runloom_coro_paint_enabled();
}

void runloom_sched_init(runloom_sched_t *s)
{
    /* Initial ring of 64 entries; grows on demand.  Empty when
     * ready_head == ready_tail == 0. */
    s->ready_ring = (runloom_g_t **)PyMem_Calloc(64, sizeof(runloom_g_t *));
    s->ready_cap = 64;
    s->ready_mask = 63;
    s->ready_head = 0;
    s->ready_tail = 0;
    s->current = NULL;
    s->sleep_heap = NULL;
    s->sleep_size = 0;
    s->sleep_cap = 0;
    s->timer_heap = NULL;
    s->timer_size = 0;
    s->timer_cap = 0;
    s->stack_size = (Py_ssize_t)runloom_cal_default;
    s->completed = 0;
    s->stopping = 0;
    s->signal_exc = NULL;
    s->netpoll_parked = 0;
    runloom_mutex_init(&s->wake_list_lock);
    s->wake_list_head = NULL;
    s->wake_list_tail = NULL;
    s->quiescence_head = NULL;
    s->quiescence_tail = NULL;
}

/* Pop the cross-thread wake list and push each entry onto the
 * scheduler's lock-free ready ring.  Called by the sched owner thread
 * once per drain iteration.  Holding the lock only for the swap (not
 * the per-g ready_push) keeps wake_safe latency bounded even when the
 * owner is doing a large fan-in. */
static void runloom_sched_drain_wake_list(runloom_sched_t *s)
{
    runloom_g_t *head;
    RUNLOOM_RLOCK(&s->wake_list_lock, RUNLOOM_RANK_WAKE_LIST);
    head = s->wake_list_head;
    /* RELEASE atomic: this clear is read by the owner's lock-free ACQUIRE peek
     * (the under-lock read above doesn't race -- producers write under the same
     * lock). */
    __atomic_store_n(&s->wake_list_head, NULL, __ATOMIC_RELEASE);
    s->wake_list_tail = NULL;
    RUNLOOM_RUNLOCK(&s->wake_list_lock, RUNLOOM_RANK_WAKE_LIST);
    while (head != NULL) {
        runloom_g_t *next = head->wake_next;
        head->wake_next = NULL;
        runloom_ready_push(s, head);
        head = next;
    }
}

/* Flush the whole quiescence-barrier wait list back onto the ready ring, in
 * FIFO order (head first -- the order they called run_ready()).  Called by the
 * drain loop at a quiescence point.  Linked through g->next. */
static void runloom_sched_flush_quiescence(runloom_sched_t *s)
{
    runloom_g_t *head = s->quiescence_head;
    s->quiescence_head = NULL;
    s->quiescence_tail = NULL;
    while (head != NULL) {
        runloom_g_t *next = head->next;
        head->next = NULL;
        runloom_ready_push(s, head);
        head = next;
    }
}

/* Phase C: ONE scheduler per OS thread.  runloom.aio runs each event loop on the
 * thread that drives it (runloom_c.run -> runloom_sched_drain on this thread's
 * sched), so two loops on two threads are fully independent -- one thread
 * blocking synchronously inside a coroutine (concurrent.futures.Future.result,
 * thread.join, queue.get -- anyio BlockingPortal, run_coroutine_threadsafe,
 * threaded server controllers) only freezes ITS OWN sched, never the other's.
 *
 * The sched is thread-local and lazily created on first use.  No cross-thread
 * init race (each thread builds its own), so the old 0->1->2 election is gone.
 * M:N hubs keep their own per-hub scheds via runloom_mn_current_sched and never
 * funnel through here for their run loop.
 *
 * Lifetime: the per-thread sched is intentionally leaked at thread exit (no
 * portable pre-C11 TLS destructor across GCC/MSVC; a sched is small and the
 * thread is gone).  A thread churning many short loops (e.g. aiosmtpd's
 * per-test Controller thread) leaks one sched per thread, not per loop. */
static RUNLOOM_TLS runloom_sched_t *runloom_tls_sched = NULL;
/* Count of per-thread scheds ever created.  When a SECOND one appears,
 * cross-thread wakes become possible (a g owned by one thread's sched woken by
 * another -- a foreign-thread future resolution, or the shared netpoll pump on
 * a different loop's thread delivering an fd event for this thread's parker).
 * That is when we arm the pump-interrupt eventfd (Phase 2); a single-loop app
 * stays exactly as before, with no eventfd in the shared epoll. */
static volatile int runloom_sched_count = 0;

runloom_sched_t *runloom_sched_get(void)
{
    runloom_sched_t *s = runloom_tls_sched;
    if (__builtin_expect(s != NULL, 1)) return s;
    s = (runloom_sched_t *)PyMem_RawMalloc(sizeof(*s));
    if (s == NULL) {
        Py_FatalError("runloom: per-thread scheduler allocation failed");
    }
    runloom_sched_init(s);                 /* sets stack_size = runloom_cal_default */
    runloom_tls_sched = s;
    /* First call on THIS thread -- never under pool->lock (it runs before this
     * thread touches netpoll), so arming (which takes pool->lock via
     * runloom_netpoll_init) cannot self-deadlock against the wake path.  Arm only
     * once a second thread's sched exists; idempotent thereafter. */
    if (__atomic_add_fetch(&runloom_sched_count, 1, __ATOMIC_ACQ_REL) >= 2) {
        runloom_netpoll_wake_pump_arm();
    }
    return s;
}

/* Non-allocating peek at the fiber currently running on THIS thread's
 * single-thread scheduler (NULL if none / no sched here yet).  Unlike
 * runloom_sched_get() it never lazily mallocs a sched, so it is safe to call from
 * any thread / hot path.  The M:N path tracks its current g separately
 * (runloom_mn_tls_current_g). */
runloom_g_t *runloom_sched_peek_current(void)
{
    runloom_sched_t *s = runloom_tls_sched;
    return s != NULL ? s->current : NULL;
}

/* ---- Coro entry shim ----
 *
 * Runs ON THE GOROUTINE'S STACK.  Local variables here live for the
 * lifetime of g.  We exploit that by allocating the initial _PyCFrame
 * here (3.12 only) so its address remains valid for as long as we
 * might switch back to g. */

/* RUNLOOM_GOROUTINE_PANIC: how an unhandled exception that escapes a goroutine
 * body is surfaced.  Without it the exception is captured into g->error but
 * never raised out of run()/mn_run(), never on G.result, and never printed -- a
 * silent failure (and most goroutines, every mn_fiber included, expose no handle
 * to inspect at all).  Modes:
 *   silent : the legacy behaviour -- capture into g->error, no report
 *   print  : (default) report via PyErr_WriteUnraisable -> sys.unraisablehook /
 *            stderr, naming the goroutine callable.  The exception is STILL kept
 *            on g->error for runloom_c.G.exception; this is memory-NEUTRAL --
 *            g->error is already retained until the g's final decref either way.
 *   abort  : Go-style -- report, then abort() the process.
 * Read once (cached).  Only the error path pays anything; the success path is
 * untouched. */
static int runloom_goroutine_panic_mode(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_GOROUTINE_PANIC");
        cur = (e != NULL && e[0] == 's') ? 0 :     /* silent */
              (e != NULL && e[0] == 'a') ? 2 :     /* abort  */
              1;                                    /* print (default) */
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

void runloom_g_entry(void *user)
{
    runloom_g_t *g = (runloom_g_t *)user;
    PyObject *res;

    /* C-only entry: skip all Python-frame setup and just call the
     * registered C function.  Used by the pure-C bench harness; no
     * Python state to manage. */
    if (g->c_entry != NULL) {
        g->c_entry(g->c_arg);
        __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
        runloom_g_state_set(g, RUNLOOM_GST_DONE);
        RUNLOOM_EVT(RUNLOOM_EVT_G_COMPLETE, g, NULL, 0);
        return;
    }

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    _PyCFrame root_cframe_storage;
    runloom_install_initial_root_frame(&root_cframe_storage);
#elif PY_VERSION_HEX >= 0x030D0000
    runloom_install_initial_root_frame();
#endif

    /* Reset recursion counters at g entry -- each g has its own
     * physical C stack, so the per-tstate counters don't reflect
     * actual stack usage when shared across multiple gs. */
    {
        PyThreadState *ts = PyThreadState_GET();
        int py_limit = Py_GetRecursionLimit();
#if PY_VERSION_HEX >= 0x030E0000
        /* 3.14: unified recursion counter + SP-based C-stack overflow check.  ts is
         * g->tstate here (current), so tell CPython this g runs on its OWN mmap'd C
         * stack -- _Py_MakeRecCheck then bounds against the fiber stack, not the hub
         * thread's (which lazy _Py_InitializeRecursionLimits would wrongly pick up). */
        ts->py_recursion_remaining = py_limit;
        if (g->coro != NULL) {
            runloom_arm_fiber_stackprot(ts, g->coro);
        }
#elif PY_VERSION_HEX >= 0x030C0000
        ts->py_recursion_remaining = py_limit;
        /* Scale the C-recursion budget to THIS fiber's actual C stack so a
         * big-stack fiber can recurse as deep as it has room for (nested
         * JSON, copy.deepcopy, recursive-descent parsers).  The old hard-coded
         * 200 (sized for a 128KB stack) capped EVERY fiber at ~88 Python
         * frames regardless of stack size (BUG #6).  Give the fiber the same
         * per-byte budget CPython gives the 8MB main thread (Py_C_RECURSION_LIMIT
         * already carries CPython's safety margin), floor 200, ceiling the
         * platform limit.
         *
         * Raising this used to SIGSEGV ~30% under deep-recursion stress -- but
         * the real cause was a snapshot bug, NOT the depth: the per-fiber
         * snap Py_XINCREF'd tstate->delete_later (the trashcan's deferred-dealloc
         * chain), and on free-threaded builds Py_INCREF reads op->ob_tid to pick
         * the refcount path -- but delete_later repurposes ob_tid as its
         * next-pointer, so the INCREF scrambled the dying objects -> UAF when the
         * trashcan destroyed them.  That bit only deep recursion (the one case
         * that leaves delete_later non-NULL at a yield).  With delete_later now
         * carried as a raw pointer (see runloom_pystate_snap), the scaled budget
         * is crash-free (p70 20/20 + freethread_stress). */
        if (g->coro != NULL) {
            size_t stk = runloom_coro_stack_size(g->coro);
            long b = (long)Py_C_RECURSION_LIMIT * (long)stk / (8L*1024*1024);
            if (b < 200) b = 200;
            if (b > Py_C_RECURSION_LIMIT) b = Py_C_RECURSION_LIMIT;
            ts->c_recursion_remaining = (int)b;
        } else {
            ts->c_recursion_remaining = 200;
        }
#else
        ts->recursion_remaining = py_limit;
#endif
    }

#if PY_VERSION_HEX >= 0x030B0000
    /* Per-fiber contextvars context.  In the default shared-hub-tstate mode
     * every fiber starts on the SAME ts->context object, so one fiber's
     * ContextVar.set() mutates it for every sibling -- the per-g swap saves and
     * restores the context POINTER, not a private copy of the contents.  Give
     * each fiber its own copy of the spawn-time context, exactly as asyncio
     * runs each Task in copy_context(), so a set() stays fiber-local.
     * Skipped under per-g-tstate mode, where each fiber already owns a
     * distinct tstate (and thus a distinct context) -- copying there is a pure
     * waste.  On OOM we leave the shared context in place (leaky across
     * siblings, but correct for this fiber and never a crash).
     *
     * RUNLOOM_NO_CTX_COPY=1 skips this entirely: the copy is one PyContext
     * alloc + gc-track PER fiber (the freelist drains after a handful, so a
     * 1M fan-out pays ~1M real allocs), and a workload that never calls
     * ContextVar.set() in a fiber sees zero behavioural difference.  Opt-in
     * because skipping it DOES break contextvar isolation for code that relies
     * on it (asyncio Task semantics) -- so it's a per-run promise, not a default.
     * Read once, cached. */
    {
    /* Atomic load too (store was already atomic): the plain `ctx_copy < 0` /
     * `ctx_copy` reads raced the store across hub threads first-init'ing it
     * (benign same-value, but a TSan data race at runloom_g_entry). */
    static int ctx_copy = -1;
    int cc = __atomic_load_n(&ctx_copy, __ATOMIC_RELAXED);
    if (cc < 0) {
        const char *e = getenv("RUNLOOM_NO_CTX_COPY");
        cc = (e && *e == '1') ? 0 : 1;
        __atomic_store_n(&ctx_copy, cc, __ATOMIC_RELAXED);
    }
    if (cc && !runloom_get_per_g_tstate_mode()) {
        PyThreadState *cts = PyThreadState_GET();
        /* Give this fiber a PRIVATE, EMPTY context as its baseline (p460).
         *
         * The old code did PyContext_CopyCurrent(), which copies WHATEVER
         * ts->context is when the fiber first runs -- i.e. the LEFTOVER context
         * of the previously-run sibling on this hub.  If that sibling was inside
         * a decimal localcontext() (or anything that forked ts->context via
         * ContextVar.set/setcontext and then parked mid-block), the copy carries
         * the sibling's `decimal_context_var -> <sibling's private Context obj>`
         * binding.  Because decimal's getcontext() returns the binding's VALUE
         * (a live, mutable Context object) and lazily REUSES an existing binding
         * rather than forking a private one, this new fiber's getcontext() then
         * returns -- and mutates -- the sibling's private localcontext object,
         * breaking localcontext()'s documented privacy (the p460 leak: genuine,
         * hard-ref-confirmed sharing of one live Context object across fibers).
         *
         * Clearing ts->context to NULL first makes PyContext_CopyCurrent() copy
         * the EMPTY default context (context_get() lazily materialises an empty
         * one), so every fiber starts from a clean, binding-free context -- which
         * is exactly what a fresh OS thread gets in stock CPython (each thread
         * owns its own empty context; that is why the plain-threads control is
         * green).  Per-contextvar isolation (BUG#7) is preserved: each fiber
         * still gets its OWN PyContext, so a ContextVar.set() inside the fiber
         * stays fiber-local.  Cross-fiber INHERITANCE of a parent's pre-spawn
         * contextvars is NOT provided here by design -- the asyncio Task layer
         * captures+runs its own copy_context() in Python (aio/tasks.py), so task
         * semantics are unaffected; only the raw fiber() baseline changes (from
         * "inherit the hub's leftover context" to "fresh empty context"). */
        PyObject *old = cts->context;
        cts->context = NULL;
        cts->context_ver++;
        Py_XDECREF(old);
        PyObject *ctx = PyContext_CopyCurrent();
        if (ctx != NULL) {
            old = cts->context;
            cts->context = ctx;          /* steals the new ref from CopyCurrent */
            cts->context_ver++;
            Py_XDECREF(old);
        } else {
            PyErr_Clear();
        }
    }
    }
#endif

    runloom_delay_inject(RUNLOOM_DLY_G_ENTRY);
    if (g->pass_index) {
        /* fiber_n(indexed=True): call fn(index).  The index rode in c_arg as a raw
         * integer; mint the PyLong here on the hub (parallel, overlapped).  Use
         * CallFunctionObjArgs for portability across Python versions. */
        PyObject *idx = PyLong_FromSsize_t((Py_ssize_t)(intptr_t)g->c_arg);
        if (idx != NULL) {
            res = PyObject_CallFunctionObjArgs(g->callable, idx, NULL);
            Py_DECREF(idx);
        } else {
            res = NULL;                       /* OOM minting the index */
        }
    } else {
        res = PyObject_CallNoArgs(g->callable);
    }
    if (res == NULL) {
        PyObject *type, *value, *tb;
        PyErr_Fetch(&type, &value, &tb);
        PyErr_NormalizeException(&type, &value, &tb);
        if (value == NULL) {
            value = Py_None;
            Py_INCREF(value);
        }
        if (tb != NULL) {
            PyException_SetTraceback(value, tb);
            Py_DECREF(tb);
        }
        Py_XDECREF(type);
        g->error = value;
        /* Surface the otherwise-swallowed goroutine exception.  Report through
         * the unraisable hook WITHOUT consuming our g->error ref: hand
         * WriteUnraisable its OWN incref'd error indicator, which it then clears,
         * leaving g->error intact for runloom_c.G.exception. */
        {
            int panic = runloom_goroutine_panic_mode();
            if (panic != 0 && value != Py_None) {
                PyObject *tb2 = PyException_GetTraceback(value);  /* new ref/NULL */
                PyObject *ty = (PyObject *)Py_TYPE(value);
                Py_INCREF(ty);
                Py_INCREF(value);
                PyErr_Restore(ty, value, tb2);        /* steals all three */
                PyErr_WriteUnraisable(g->callable);   /* prints + clears indicator */
                if (panic == 2) {
                    abort();                          /* Go-style crash-on-panic */
                }
            }
        }
    } else {
        g->result = res;
    }
    /* The fiber has run to completion; g->callable is never invoked
     * again.  Release it NOW rather than waiting for the g's last decref --
     * otherwise a runloom.aio task, whose callable is the task's own bound
     * _driver method, is kept alive forever by an unbreakable cycle:
     *
     *     task -> task._g (RunloomG) -> runloom_g_t -> g->callable (_driver) -> task
     *
     * runloom_g_t is a C struct, invisible to cyclic GC, and the RunloomG wrapper
     * has no tp_traverse, so the collector cannot see the g->callable edge and
     * never reclaims the completed task (its 'exception never retrieved'
     * warning never fires either).  Clearing callable here cuts that edge at
     * the source, so the remaining task graph collects by plain refcounting.
     *
     * Running the resulting finalizer here is safe: we are on g's own stack in
     * g's tstate (any finalizer frames land on g's datastack chunk, drained
     * immediately after this returns), and the scheduler still holds its
     * ref(s) to g, so g itself cannot be freed under us.  runloom_g_decref's later
     * Py_XDECREF(g->callable) becomes a NULL no-op.  Done last, so g->done
     * publishes a fully torn-down g. */
    Py_CLEAR(g->callable);
    /* RELEASE store on g->done publishes the prior g->result/g->error
     * writes; RunloomG_done_get / RunloomG_result_get load g->done with
     * ACQUIRE and only read result/error if done. */
    __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
    runloom_g_state_set(g, RUNLOOM_GST_DONE);
    RUNLOOM_EVT(RUNLOOM_EVT_G_COMPLETE, g, NULL, 0);
    /* Falls back through asm trampoline -> infinite swap to caller. */
}

/* ---- runloom_g_t slab allocator ----
 *
 * spawn/decref-last-ref on the hot path is dominated by
 * PyMem_Calloc(144) / PyMem_Free.  At 100k spawns/sec that's 200k
 * calls/sec through CPython's small-block allocator, which is
 * thread-contended on free-threaded 3.13t.
 *
 * Per-thread LIFO free list of recycled runloom_g_t.  Push on last
 * decref; pop on next spawn.  Reuses the `next` field for the
 * free-list link (it's NULL'd by ready_pop after each scheduler
 * cycle, so it's free real estate while g is unallocated).  Cap of
 * 256 entries / thread bounds memory footprint after a burst. */
/* Cap chosen so a steady-state spawn-heavy workload (100k+ gs in
 * flight) doesn't continuously overflow back to PyMem_Free, while
 * still bounding memory to ~150 KB / thread when not in use. */
#define RUNLOOM_G_SLAB_CAP 4096
static RUNLOOM_TLS runloom_g_t *runloom_g_slab = NULL;
static RUNLOOM_TLS int runloom_g_slab_size = 0;
#ifdef RUNLOOM_DBG_BADCORO
unsigned long long runloom_next_wseq_ctr = 0;   /* source-hunt: ->next write seq */
#endif

/* ---- cross-hub g-slab balancing -------------------------------------------
 * The slab above is per-thread (RUNLOOM_TLS).  Under a single-producer /
 * many-consumer spawn pattern (one root fiber spawns, N hubs run) the producer
 * hub allocs every g while they free into the CONSUMER hubs' slabs -- so the
 * producer's slab drains and callocs every spawn (-> glibc grow_heap/mprotect;
 * MEASURED ~30% of spawn time at 8 hubs, and it VANISHES at 1 hub).  A global
 * balance pool fixes it, mirroring the stack depot: a thread flushes a batch
 * once its slab passes the cap, and refills a batch when its slab is empty.
 * The structs stay VALID DEAD g's (FREED state, still greg-linked, NEVER freed
 * to the OS), so the try_incref UAF invariant is fully preserved -- they just
 * sit on the global freelist between uses.  The lock is amortized (once per
 * RUNLOOM_G_GLOBAL_BATCH ops), so the hot alloc/free stay lock-free. */
#define RUNLOOM_G_GLOBAL_BATCH 256
static runloom_g_t   *runloom_g_global_slab = NULL;
static int            runloom_g_global_size = 0;
static runloom_mutex_t runloom_g_global_lock = RUNLOOM_MUTEX_STATIC_INIT;
static int             runloom_g_global_lock_inited = 0;

/* This g-slab balance lock lives in a TU compiled on EVERY platform (unlike the
 * coro balance lock, which is FCONTEXT/POSIX-only and can rely on a real static
 * init).  On Windows RUNLOOM_MUTEX_STATIC_INIT is {0} -- a CRITICAL_SECTION
 * cannot be statically initialised -- so lazily init on first use, exactly like
 * runloom_cal_lock above.  POSIX gets a real static init and this is a no-op. */
static void runloom_g_global_ensure_lock(void)
{
#if defined(RUNLOOM_OS_WINDOWS)
    int st = __atomic_load_n(&runloom_g_global_lock_inited, __ATOMIC_ACQUIRE);
    if (st == 2) return;
    if (st == 0 &&
        __atomic_compare_exchange_n(&runloom_g_global_lock_inited, &st, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&runloom_g_global_lock);
        __atomic_store_n(&runloom_g_global_lock_inited, 2, __ATOMIC_RELEASE);
        return;
    }
    while (__atomic_load_n(&runloom_g_global_lock_inited, __ATOMIC_ACQUIRE) != 2) { }
#else
    (void)runloom_g_global_lock_inited;   /* POSIX: static init suffices */
#endif
}

/* Move up to BATCH structs from the global pool into this thread's empty slab.
 * Returns the new slab head (== runloom_g_slab), or NULL if the global is dry. */
static runloom_g_t *runloom_g_global_refill(void)
{
    runloom_g_t *head, *tail;
    int n;
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    head = runloom_g_global_slab;
    if (head == NULL) { runloom_mutex_unlock(&runloom_g_global_lock); return NULL; }
    tail = head; n = 1;
    while (n < RUNLOOM_G_GLOBAL_BATCH && tail->next != NULL) { tail = tail->next; n++; }
    runloom_g_global_slab = tail->next;
    runloom_g_global_size -= n;
    runloom_mutex_unlock(&runloom_g_global_lock);
    /* caller guarantees the TLS slab is empty -> detach the batch tail and
     * make the batch the new TLS slab. */
    RUNLOOM_NEXT_SET(tail, runloom_g_slab, 3);   /* runloom_g_slab == NULL here */
    runloom_g_slab = head;
    runloom_g_slab_size += n;
    return head;
}

/* Move a batch from this thread's slab into the global pool (over-cap spill). */
static void runloom_g_global_flush(void)
{
    runloom_g_t *head, *tail;
    int n;
    head = runloom_g_slab;
    if (head == NULL) return;
    tail = head; n = 1;
    while (n < RUNLOOM_G_GLOBAL_BATCH && tail->next != NULL) { tail = tail->next; n++; }
    runloom_g_slab = tail->next;
    runloom_g_slab_size -= n;
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    RUNLOOM_NEXT_SET(tail, runloom_g_global_slab, 3);
    runloom_g_global_slab = head;
    runloom_g_global_size += n;
    runloom_mutex_unlock(&runloom_g_global_lock);
}

/* ---- test-only hooks for the fork-deadlock regression test ----------------
 * Not used in production; exposed only as runloom_c._test_* for the regression
 * test that proves the after-fork balance-lock reset.  Kept here so they share
 * the (file-static) lock + ensure shim. */
void runloom_g_global_test_hold_ns(long long ns)
{
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    if (ns > 0) runloom_sleep_ns(ns);
    runloom_mutex_unlock(&runloom_g_global_lock);
}

void runloom_g_global_test_acquire(void)
{
    /* Blocks forever if the lock was inherited HELD by a dead owner across
     * fork() -- exactly the deadlock the after-fork reset prevents. */
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    runloom_mutex_unlock(&runloom_g_global_lock);
}

/* After fork(): a hub may have died mid-splice holding runloom_g_global_lock,
 * leaving it locked with the global freelist possibly inconsistent.  Re-init the
 * lock and abandon the bounded inherited batch.  The structs are leaked, never
 * freed to the OS, so the "freed g never returns to the OS" UAF invariant still
 * holds; the child has abandoned all hubs/fibers (runloom_mn_reset_after_fork) so
 * no stale wake can reference them.  Non-hot-path: runs once, child-side only.
 * Mirrors runloom_cal_reset_after_fork. */
void runloom_g_global_reset_after_fork(void)
{
    runloom_mutex_init(&runloom_g_global_lock);
    __atomic_store_n(&runloom_g_global_lock_inited, 2, __ATOMIC_RELAXED);
    runloom_g_global_slab = NULL;
    runloom_g_global_size = 0;
}

runloom_g_t *runloom_g_slab_alloc(void)
{
    runloom_g_t *g = runloom_g_slab;
    /* OOM fault injection (test-only; inert unless RUNLOOM_FAULT_SPAWN_G is set):
     * simulate "can't allocate a fiber struct" regardless of the slab
     * cache, so the spawn error paths get exercised. */
    if (RUNLOOM_SPAWN_FINJ(RUNLOOM_FAULT_SPAWN_G)) return NULL;
    /* TLS slab empty -> pull a batch from the cross-hub global pool before
     * falling to a cold calloc (the producer-hub-drains fix). */
    if (g == NULL) g = runloom_g_global_refill();
    if (g != NULL) {
        runloom_g_slab = g->next;
        runloom_g_slab_size--;
        /* Reuse path (HOT).  The scrub is in TWO parts so the load-bearing
         * `state` byte can be re-initialised atomically in between -- a
         * concurrent registry dump reads `state` with an atomic load and must
         * never race a non-atomic memset of it.
         *   part 1: clear everything BEFORE `state`.
         *   part 2: clear the fields AFTER `state` up to the introspection
         *           block (arena/batch/pass_index).  These are NOT preserved
         *           on purpose and are NOT all reset at spawn -- pass_index is
         *           set only on the indexed fiber_n path, so without this clear a
         *           recycled g carries a stale pass_index and mis-calls
         *           fn(stale index) instead of fn() (a real wrong-result bug),
         *           and any FUTURE field added in this gap would leak the same
         *           way.  This is what makes the "everything before the
         *           introspection block is cleared" contract on runloom_g_t
         *           actually hold.
         * The introspection block (id, state_since_ns, park_fd/events,
         * reg_prev/next) is left untouched: reg_prev/reg_next MUST survive
         * recycling (the g stays linked in the global registry, written only
         * under the registry lock); id / park_fd are reset at spawn.  spawn
         * always overwrites callable, coro, refcount. */
        memset(g, 0, offsetof(runloom_g_t, state));
        __atomic_store_n(&g->state, (unsigned char)RUNLOOM_GST_INIT,
                         __ATOMIC_RELEASE);
        memset((char *)g + offsetof(runloom_g_t, arena), 0,
               offsetof(runloom_g_t, id) - offsetof(runloom_g_t, arena));
#ifdef RUNLOOM_DBG_BADCORO
        g->magic = 0x600DC0DEu;   /* source-hunt: live-g canary (reuse path) */
#endif
        return g;
    }
    /* Cold path: a brand-new struct from the OS.  Use libc calloc, NOT
     * PyMem_Calloc: under a cold burst of millions of spawns the slab freelist
     * (cap RUNLOOM_G_SLAB_CAP) is empty, so this runs ~1M times, and pymalloc
     * services that by mmap'ing fresh arenas (_PyMem_ArenaAlloc) -- every mmap
     * takes the process's single mmap_lock, which the 8 hub threads + the
     * spawner then serialize on (a kernel-lock storm that scales WORSE with hub
     * count; perf-confirmed).  libc calloc serves a small fixed struct from the
     * per-thread heap (no per-alloc mmap, no shared lock), removing the storm.
     * The g-struct is a plain C struct (not a PyObject), so it never needed
     * pymalloc.  calloc zeroes everything (registry links + id); link it into
     * the global registry exactly once for the struct's whole lifetime. */
    g = (runloom_g_t *)calloc(1, sizeof(*g));
    if (g != NULL) {
        runloom_greg_link(g);
#ifdef RUNLOOM_DBG_BADCORO
        g->magic = 0x600DC0DEu;   /* source-hunt: live-g canary (cold path) */
#endif
    }
    return g;
}

void runloom_g_slab_free(runloom_g_t *g)
{
#ifdef RUNLOOM_DBG_BADCORO
    g->magic = 0xDEADu;   /* source-hunt: freed-g marker (distinguishes UAF from reuse) */
#endif
    runloom_g_state_set(g, RUNLOOM_GST_FREED);
    /* Do NOT return the g STRUCT to the OS.  A stale wake_g (the netpoll
     * dup-wake) can reach hub_submit with a pointer to an already-freed g;
     * hub_submit's try_incref reads g->refcount to decide whether to re-queue,
     * which is only sound while g is still a VALID g struct (refcount 0).  If the
     * struct were freed to the OS and its memory reused, try_incref reads garbage
     * and spuriously resurrects a non-g -> the arm64 crash residual (a reused g:
     * magic != 0x600DC0DE, owner=NULL, garbage coro, reaching coro_resume from
     * the ready FIFO).  The g's coro/stack are still released at completion; only
     * the small g STRUCT is retained, cached in the per-thread slab forever so
     * the try_incref read stays sound.  Memory is bounded by peak concurrency
     * (the max-fibers admission cap); RUNLOOM_G_SLAB_CAP is now a soft hint (we
     * keep caching past it rather than free).  A bounded QSBR/quarantine reclaim
     * that frees only after no wake can reference the struct is a later
     * refinement. */
    RUNLOOM_NEXT_SET(g, runloom_g_slab, 3);   /* slab freelist link */
    runloom_g_slab = g;
    runloom_g_slab_size++;
    /* over-cap -> spill a batch to the cross-hub global pool so a producer hub
     * on another thread can refill from it instead of callocing. */
    if (runloom_g_slab_size > RUNLOOM_G_SLAB_CAP) runloom_g_global_flush();
}

/* ---- teardown reclaim (mn_fini) -------------------------------------------
 * runloom_g_slab_free never returns a g STRUCT to the OS *while a runtime is
 * live* (a stale netpoll dup-wake can still try_incref it -- see the invariant
 * in runloom_g_slab_free).  That retention is meant to be BOUNDED by peak
 * concurrency and REUSED, but a per-thread slab dies WITH its owning hub thread
 * at mn_fini: the hub's TLS cache of completed-fiber structs is stranded (no
 * portable pre-C11 TLS destructor to hand them back), so across repeated
 * mn_init/mn_fini cycles every fiber ever completed leaks its struct forever --
 * repeated runloom.run(n>1) grows RSS without bound.
 *
 * The reclaim below is sound for the SAME reason mn_fini already frees the
 * bulk-arena g structs (runloom_garena_quar_drain) to the OS at teardown: by the
 * time it runs every hub thread is joined and no wake_g can be in flight, so no
 * try_incref can reach these structs -- the grace period that makes returning
 * them to the OS safe.  Two halves:
 *   runloom_g_slab_thread_flush() -- an EXITING hub thread splices its OWN TLS
 *       slab into the shared global pool BEFORE it dies (call it from hub_main),
 *       so its cached structs survive the thread and become reachable from the
 *       main thread.  Also folds in the mn_fini thread's own slab (reclaim below).
 *   runloom_g_slab_reclaim() -- the main thread, after joining every hub, folds
 *       in its own slab then frees the ENTIRE global pool back to the OS,
 *       unlinking each struct from the fiber registry (call it from mn_fini).
 * Only the global pool is freed -- never a still-live thread's TLS slab -- so no
 * other running thread (e.g. a concurrent aio loop) can be left with a dangling
 * cache entry. */
void runloom_g_slab_thread_flush(void)
{
    runloom_g_t *head = runloom_g_slab;
    runloom_g_t *tail;
    int n;
    if (head == NULL) return;
    tail = head; n = 1;
    while (tail->next != NULL) { tail = tail->next; n++; }
    runloom_g_slab = NULL;
    runloom_g_slab_size = 0;
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    RUNLOOM_NEXT_SET(tail, runloom_g_global_slab, 3);
    runloom_g_global_slab = head;
    runloom_g_global_size += n;
    runloom_mutex_unlock(&runloom_g_global_lock);
}

void runloom_g_slab_reclaim(void)
{
    runloom_g_t *head;
    /* Fold the calling (main) thread's own TLS slab into the global pool first:
     * mn_fini's leftover-drain (runloom_g_decref) freed those structs into THIS
     * thread's slab. */
    runloom_g_slab_thread_flush();
    /* Detach the whole global pool under the lock, then free each struct to the
     * OS OUTSIDE the lock -- runloom_greg_unlink takes the registry lock, so
     * don't nest the two. */
    runloom_g_global_ensure_lock();
    runloom_mutex_lock(&runloom_g_global_lock);
    head = runloom_g_global_slab;
    runloom_g_global_slab = NULL;
    runloom_g_global_size = 0;
    runloom_mutex_unlock(&runloom_g_global_lock);
    while (head != NULL) {
        runloom_g_t *next = head->next;
        runloom_greg_unlink(head);   /* drop from the global fiber registry */
        free(head);                  /* mirror the cold-path libc calloc() */
        head = next;
    }
}

/* ---- Refcount ---- */
void runloom_g_incref(runloom_g_t *g)
{
    if (g) __atomic_add_fetch(&g->refcount, 1, __ATOMIC_RELAXED);
}

int runloom_g_try_incref(runloom_g_t *g)
{
    int old;
    if (g == NULL) return 0;
    old = __atomic_load_n(&g->refcount, __ATOMIC_RELAXED);
    while (old > 0) {
        if (__atomic_compare_exchange_n(&g->refcount, &old, old + 1, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
            return 1;
        /* old reloaded with the current value; loop while still live. */
    }
    return 0;
}

void runloom_g_decref(runloom_g_t *g)
{
    int new_count;
    if (g == NULL) return;
    RUNLOOM_EVT(RUNLOOM_EVT_G_DECREF, g, NULL, (long long)g->refcount);
    /* ACQ_REL: pairs with other threads' decrefs so all prior writes
     * (including g->result / g->error / g->done done-flag updates)
     * are observable on the last reference's owner before free. */
    new_count = __atomic_sub_fetch(&g->refcount, 1, __ATOMIC_ACQ_REL);
    if (new_count <= 0) {
        runloom_pystate_snap_clear(&g->snap);
        if (g->tstate != NULL) {
            /* per-g tstate (RUNLOOM_PER_G_TSTATE): g is done + detached, so its
             * tstate is current on no thread.  Clear+Delete frees its
             * datastack/frames/exc.  Mirrors mn_fini's hub-tstate teardown,
             * which also clears non-current tstates with the caller's tstate
             * held -- runloom_g_decref's contexts (hub completion, drain, RunloomG
             * dealloc) all run with some other tstate current.
             *
             * ============================ KNOWN BUG ============================
             * If you arrived here from a SEGV backtrace that goes
             *   PyThreadState_Clear -> _PyMem_AbandonDelayed -> ... -> _mi_page_retire
             * STOP -- this is a CHARACTERIZED, NOT-YET-FIXED bug.  Do not re-derive
             * it; read docs/dev/STEAL_WOKEN_CLEANUP.md ("Tracked bug" + "Fix design").
             *
             * What it is: this Clear is NOT the cause.  Under RUNLOOM_PER_G_TSTATE
             * (and RUNLOOM_STEAL_WOKEN, which redirects to it) a fiber's per-g
             * PyThreadState -- carrying its OWN mimalloc heap + QSBR delayed-free
             * state -- migrates across OS hub threads as the fiber moves hub->hub.
             * Free-threaded CPython binds a tstate's allocator to ONE OS thread
             * (PyThreadState_Bind; mimalloc keys page ownership on _Py_ThreadId).
             * Migrating it without the mimalloc segment abandon/adopt handshake
             * leaves the heap's per-thread bookkeeping inconsistent; the corruption
             * just SURFACES here, when Clear abandons the delayed-free queue and
             * retires an already-inconsistent page.
             *
             * Fingerprint: intermittent SEGV, ONLY at H>=2 (needs real migration --
             * H=1 is always clean), ONLY under allocation churn (e.g.
             * test_chan_queue MPMC).  The DEFAULT scheduler (both flags off) never
             * takes this path and is unaffected.
             *
             * Do NOT try to "fix" it here (teardown) or by reordering Clear/Delete
             * or making g->tstate current first -- the pages are already corrupt by
             * the time we get here.  The only real fix is the mimalloc + brc + qsbr
             * thread abandon/adopt handshake at each park/resume boundary (see the
             * hub_main per-g-tstate branch + the "Fix design" doc section); it is a
             * dedicated, validated effort, not a patch.  Until then the migratable
             * modes stay opt-in / experimental.
             * ==================================================================*/
            PyThreadState_Clear(g->tstate);
            PyThreadState_Delete(g->tstate);
            g->tstate = NULL;
        }
        if (g->arena) {
            /* Bulk-arena g: the g, its coro and its stack are slices of a batch
             * (one calloc / one mmap), NOT individually allocated.  Do NOT
             * runloom_coro_destroy (it would pool/free an arena coro) nor
             * runloom_g_slab_free (it would free an arena g) -- either corrupts
             * the heap.  Drop the Python refs (g_entry already Py_CLEAR'd
             * callable; result/error are normally NULL for fiber_n workers), then
             * decrement the batch; the last finisher reclaims the whole batch.
             * Bulk gs are never admitted under the max-fibers limit, so
             * limit_counted is 0 -- nothing to release. */
            Py_XDECREF(g->callable);
            Py_XDECREF(g->result);
            Py_XDECREF(g->error);
            runloom_g_state_set(g, RUNLOOM_GST_FREED);
            runloom_fibern_batch_finish_one(g->batch);
            return;
        }
        if (g->coro != NULL) {
            runloom_coro_destroy(g->coro);
            g->coro = NULL;
        }
        Py_XDECREF(g->callable);
        Py_XDECREF(g->result);
        Py_XDECREF(g->error);
        if (g->limit_counted) {           /* release the admission slot */
            runloom_fiber_release();
            g->limit_counted = 0;
        }
        runloom_g_slab_free(g);
    }
}

/* ---- Spawn ---- */
static PyObject *spawn_common(runloom_sched_t *s, PyObject *callable,
                              int noyield, size_t stack_size, size_t advice_key)
{
    runloom_g_t *g;
    if (__builtin_expect(!runloom_pct.inited, 0))
        runloom_pct_init();   /* read RUNLOOM_PCT_SEED once, before any ready-pop */
    /* Backpressure: admit under the max-fibers limit before allocating.
     * adm: 0 rejected, 1 admitted (no limit), 2 admitted+counted. */
    {
        int adm = runloom_fiber_admit();
        if (adm == 0) {
            PyErr_Format(PyExc_RuntimeError,
                         "runloom: fiber limit (%ld) exceeded",
                         runloom_get_max_fibers());
            return NULL;
        }
        g = runloom_g_slab_alloc();
        if (g == NULL) {
            if (adm == 2) runloom_fiber_release();
            PyErr_NoMemory();
            return NULL;
        }
        g->limit_counted = (unsigned char)(adm == 2);
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->advice_key = advice_key;   /* kind key (0 unless profiling on); resolved by caller */
    /* Publish these three via RELAXED atomic stores (not plain), matched by the
     * introspect reader's RELAXED loads (runloom_introspect.c:602-604).  The
     * state>=RUNNABLE gate (9b21ec18) closes the FRESH-spawn read-before-publish
     * case, but NOT slab REUSE: a retained g (never unlinked from the registry)
     * keeps a STALE RUNNABLE from its PRIOR incarnation in state's modification
     * order, so a registry-walking sampler can acquire-load that stale RUNNABLE
     * -- an earlier value it is not forced past, resetting state to FREED(:743)/
     * INIT(:708) does NOT prevent it (GenMC greg_reuse.c: race persists WITH both
     * resets) -- pass the gate, and read these fields while we rewrite them.  A
     * data race on plain accesses; making BOTH sides RELAXED-atomic removes it
     * (GenMC-proven).  Relaxed is free (same instruction) and, being sequenced
     * before the :989 RELEASE publish, is still visible to any hub that later
     * acquires g through the ready/submit handshake.  Fields are display-only
     * (runloom.fibers() dict); no scheduling consumer reads the snapshot. */
    __atomic_store_n(&g->refcount, 1, __ATOMIC_RELAXED);  /* one ref for the sched queue */
    __atomic_store_n(&g->owner, s, __ATOMIC_RELAXED);     /* owning per-thread sched (Phase C) */
    __atomic_store_n(&g->noyield, noyield, __ATOMIC_RELAXED);
    __atomic_store_n(&g->id, runloom_next_goid(), __ATOMIC_RELAXED);
    g->park_fd = -1;   /* not parked on any fd yet (reset after slab reuse) */
    g->pct_prio = 0;   /* fresh fiber: unassigned PCT priority (slab reuse) */
    g->pct_fifo = 0;   /* default reorderable; m_go(fifo=True) sets it after spawn */
    /* OOM fault injection (test-only): simulate a stack-allocation failure
     * (mmap ENOMEM) by forcing coro_new to "fail", exercising the existing
     * coro==NULL cleanup path.  Inert unless RUNLOOM_FAULT_SPAWN_STACK is set. */
    g->coro = RUNLOOM_SPAWN_FINJ(RUNLOOM_FAULT_SPAWN_STACK)
              ? NULL : runloom_coro_new(stack_size, runloom_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(g->callable);
        g->callable = NULL;
        if (g->limit_counted) {             /* release the admission slot we took */
            runloom_fiber_release();
            g->limit_counted = 0;
        }
        runloom_g_slab_free(g);
        PyErr_SetString(PyExc_MemoryError, "runloom_coro_new failed");
        return NULL;
    }
    runloom_g_state_set(g, RUNLOOM_GST_RUNNABLE);
    runloom_ready_push(s, g);
    return PyCapsule_New(g, "runloom_g", NULL);
}

PyObject *runloom_sched_spawn(runloom_sched_t *s, PyObject *callable)
{
    /* Default-size path: profile the kind, and (autosize) start it at the
     * kind's learned size instead of the global default. */
    size_t key = runloom_advice_note_spawn(callable);
    size_t sz  = runloom_fiber_stack_floor(
                     runloom_advice_size_for(key, callable, (size_t)s->stack_size));
    return spawn_common(s, callable, /*noyield*/0, sz, key);
}

PyObject *runloom_sched_spawn_noyield(runloom_sched_t *s, PyObject *callable)
{
    size_t key = runloom_advice_note_spawn(callable);
    size_t sz  = runloom_fiber_stack_floor(
                     runloom_advice_size_for(key, callable, (size_t)s->stack_size));
    return spawn_common(s, callable, /*noyield*/1, sz, key);
}
