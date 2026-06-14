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
#if PY_VERSION_HEX >= 0x030C0000
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
        PyObject *ctx = PyContext_CopyCurrent();
        if (ctx != NULL) {
            PyObject *old = cts->context;
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
        /* go_n(indexed=True): call fn(index).  The index rode in c_arg as a raw
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

runloom_g_t *runloom_g_slab_alloc(void)
{
    runloom_g_t *g = runloom_g_slab;
    /* OOM fault injection (test-only; inert unless RUNLOOM_FAULT_SPAWN_G is set):
     * simulate "can't allocate a fiber struct" regardless of the slab
     * cache, so the spawn error paths get exercised. */
    if (RUNLOOM_SPAWN_FINJ(RUNLOOM_FAULT_SPAWN_G)) return NULL;
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
         *           set only on the indexed go_n path, so without this clear a
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
    if (g != NULL) runloom_greg_link(g);
    return g;
}

void runloom_g_slab_free(runloom_g_t *g)
{
    runloom_g_state_set(g, RUNLOOM_GST_FREED);
    if (runloom_g_slab_size >= RUNLOOM_G_SLAB_CAP) {
        /* Struct is leaving for the OS -- drop it from the registry
         * first so the dump never walks freed memory.  free (not PyMem_Free)
         * to match the libc calloc in slab_alloc's cold path. */
        runloom_greg_unlink(g);
        free(g);
        return;
    }
    g->next = runloom_g_slab;
    runloom_g_slab = g;
    runloom_g_slab_size++;
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
             * callable; result/error are normally NULL for go_n workers), then
             * decrement the batch; the last finisher reclaims the whole batch.
             * Bulk gs are never admitted under the max-fibers limit, so
             * limit_counted is 0 -- nothing to release. */
            Py_XDECREF(g->callable);
            Py_XDECREF(g->result);
            Py_XDECREF(g->error);
            runloom_g_state_set(g, RUNLOOM_GST_FREED);
            runloom_gon_batch_finish_one(g->batch);
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
    g->refcount = 1;   /* one ref for the scheduler queue */
    g->owner = s;      /* per-thread sched that owns this g (Phase C) */
    g->noyield = noyield;
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
    size_t sz  = runloom_advice_size_for(key, callable, (size_t)s->stack_size);
    return spawn_common(s, callable, /*noyield*/0, sz, key);
}

PyObject *runloom_sched_spawn_noyield(runloom_sched_t *s, PyObject *callable)
{
    size_t key = runloom_advice_note_spawn(callable);
    size_t sz  = runloom_advice_size_for(key, callable, (size_t)s->stack_size);
    return spawn_common(s, callable, /*noyield*/1, sz, key);
}
