/* Part of pygo_sched.c -- #included into that translation unit, not compiled
 * standalone.  See pygo_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static int       pygo_cal_frozen = 0;

static size_t pygo_next_pow2(size_t v)
{
    size_t p = 1;
    while (p < v && p < PYGO_MAX_STACK_SIZE) p <<= 1;
    return p;
}

/* Called from drain after a g completes.  Scans the g's stack for
 * HWM, updates the running max, and freezes the calibration window
 * if we've collected enough samples. */
static void pygo_cal_record(pygo_g_t *g)
{
    size_t hwm;
    if (pygo_cal_frozen || !pygo_coro_paint_enabled()) return;
    if (g == NULL || g->coro == NULL) return;
    hwm = pygo_coro_scan_hwm(g->coro);
    if (hwm > pygo_cal_max_hwm) pygo_cal_max_hwm = hwm;
    pygo_cal_completed++;
    if (pygo_cal_completed >= PYGO_CAL_TARGET) {
        size_t bound = pygo_cal_max_hwm * PYGO_CAL_SAFETY;
        size_t chosen = pygo_next_pow2(bound);
        if (chosen < PYGO_MIN_STACK_SIZE) chosen = PYGO_MIN_STACK_SIZE;
        if (chosen > PYGO_MAX_STACK_SIZE) chosen = PYGO_MAX_STACK_SIZE;
        pygo_cal_default = chosen;
        pygo_cal_frozen = 1;
        pygo_coro_paint_set(0);
        /* cal_record runs inside drain on this thread's own sched; bump it so
         * the freeze takes effect immediately here.  Other threads' scheds pick
         * up pygo_cal_default when they spawn their next g. */
        pygo_sched_get()->stack_size = (Py_ssize_t)chosen;
    }
}

void pygo_sched_set_default_stack_size(size_t bytes)
{
    if (bytes < PYGO_MIN_STACK_SIZE) bytes = PYGO_MIN_STACK_SIZE;
    if (bytes > PYGO_MAX_STACK_SIZE) bytes = PYGO_MAX_STACK_SIZE;
    pygo_cal_default = bytes;
    pygo_cal_frozen = 1;
    pygo_coro_paint_set(0);
    /* Update the calling thread's sched immediately; other threads pick up
     * pygo_cal_default on their next spawn. */
    pygo_sched_get()->stack_size = (Py_ssize_t)bytes;
}

size_t pygo_sched_get_default_stack_size(void)
{
    return pygo_cal_default;
}

void pygo_sched_stack_stats(pygo_stack_stats_t *out)
{
    if (out == NULL) return;
    out->default_size = pygo_cal_default;
    out->max_hwm      = pygo_cal_max_hwm;
    out->completed    = pygo_cal_completed;
    out->calibrated   = pygo_cal_frozen;
    out->painting     = pygo_coro_paint_enabled();
}

void pygo_sched_init(pygo_sched_t *s)
{
    /* Initial ring of 64 entries; grows on demand.  Empty when
     * ready_head == ready_tail == 0. */
    s->ready_ring = (pygo_g_t **)PyMem_Calloc(64, sizeof(pygo_g_t *));
    s->ready_cap = 64;
    s->ready_mask = 63;
    s->ready_head = 0;
    s->ready_tail = 0;
    s->current = NULL;
    s->sleep_heap = NULL;
    s->sleep_size = 0;
    s->sleep_cap = 0;
    s->stack_size = (Py_ssize_t)pygo_cal_default;
    s->completed = 0;
    s->stopping = 0;
    s->signal_exc = NULL;
    s->netpoll_parked = 0;
    pygo_mutex_init(&s->wake_list_lock);
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
static void pygo_sched_drain_wake_list(pygo_sched_t *s)
{
    pygo_g_t *head;
    pygo_mutex_lock(&s->wake_list_lock);
    head = s->wake_list_head;
    /* RELEASE atomic: this clear is read by the owner's lock-free ACQUIRE peek
     * (the under-lock read above doesn't race -- producers write under the same
     * lock). */
    __atomic_store_n(&s->wake_list_head, NULL, __ATOMIC_RELEASE);
    s->wake_list_tail = NULL;
    pygo_mutex_unlock(&s->wake_list_lock);
    while (head != NULL) {
        pygo_g_t *next = head->wake_next;
        head->wake_next = NULL;
        pygo_ready_push(s, head);
        head = next;
    }
}

/* Flush the whole quiescence-barrier wait list back onto the ready ring, in
 * FIFO order (head first -- the order they called run_ready()).  Called by the
 * drain loop at a quiescence point.  Linked through g->next. */
static void pygo_sched_flush_quiescence(pygo_sched_t *s)
{
    pygo_g_t *head = s->quiescence_head;
    s->quiescence_head = NULL;
    s->quiescence_tail = NULL;
    while (head != NULL) {
        pygo_g_t *next = head->next;
        head->next = NULL;
        pygo_ready_push(s, head);
        head = next;
    }
}

/* Phase C: ONE scheduler per OS thread.  pygo.aio runs each event loop on the
 * thread that drives it (pygo_core.run -> pygo_sched_drain on this thread's
 * sched), so two loops on two threads are fully independent -- one thread
 * blocking synchronously inside a coroutine (concurrent.futures.Future.result,
 * thread.join, queue.get -- anyio BlockingPortal, run_coroutine_threadsafe,
 * threaded server controllers) only freezes ITS OWN sched, never the other's.
 *
 * The sched is thread-local and lazily created on first use.  No cross-thread
 * init race (each thread builds its own), so the old 0->1->2 election is gone.
 * M:N hubs keep their own per-hub scheds via pygo_mn_current_sched and never
 * funnel through here for their run loop.
 *
 * Lifetime: the per-thread sched is intentionally leaked at thread exit (no
 * portable pre-C11 TLS destructor across GCC/MSVC; a sched is small and the
 * thread is gone).  A thread churning many short loops (e.g. aiosmtpd's
 * per-test Controller thread) leaks one sched per thread, not per loop. */
static PYGO_TLS pygo_sched_t *pygo_tls_sched = NULL;
/* Count of per-thread scheds ever created.  When a SECOND one appears,
 * cross-thread wakes become possible (a g owned by one thread's sched woken by
 * another -- a foreign-thread future resolution, or the shared netpoll pump on
 * a different loop's thread delivering an fd event for this thread's parker).
 * That is when we arm the pump-interrupt eventfd (Phase 2); a single-loop app
 * stays exactly as before, with no eventfd in the shared epoll. */
static volatile int pygo_sched_count = 0;

pygo_sched_t *pygo_sched_get(void)
{
    pygo_sched_t *s = pygo_tls_sched;
    if (__builtin_expect(s != NULL, 1)) return s;
    s = (pygo_sched_t *)PyMem_RawMalloc(sizeof(*s));
    if (s == NULL) {
        Py_FatalError("pygo: per-thread scheduler allocation failed");
    }
    pygo_sched_init(s);                 /* sets stack_size = pygo_cal_default */
    pygo_tls_sched = s;
    /* First call on THIS thread -- never under pool->lock (it runs before this
     * thread touches netpoll), so arming (which takes pool->lock via
     * pygo_netpoll_init) cannot self-deadlock against the wake path.  Arm only
     * once a second thread's sched exists; idempotent thereafter. */
    if (__atomic_add_fetch(&pygo_sched_count, 1, __ATOMIC_ACQ_REL) >= 2) {
        pygo_netpoll_wake_pump_arm();
    }
    return s;
}

/* Non-allocating peek at the goroutine currently running on THIS thread's
 * single-thread scheduler (NULL if none / no sched here yet).  Unlike
 * pygo_sched_get() it never lazily mallocs a sched, so it is safe to call from
 * any thread / hot path.  The M:N path tracks its current g separately
 * (pygo_mn_tls_current_g). */
pygo_g_t *pygo_sched_peek_current(void)
{
    pygo_sched_t *s = pygo_tls_sched;
    return s != NULL ? s->current : NULL;
}

/* ---- Coro entry shim ----
 *
 * Runs ON THE GOROUTINE'S STACK.  Local variables here live for the
 * lifetime of g.  We exploit that by allocating the initial _PyCFrame
 * here (3.12 only) so its address remains valid for as long as we
 * might switch back to g. */
void pygo_g_entry(void *user)
{
    pygo_g_t *g = (pygo_g_t *)user;
    PyObject *res;

    /* C-only entry: skip all Python-frame setup and just call the
     * registered C function.  Used by the pure-C bench harness; no
     * Python state to manage. */
    if (g->c_entry != NULL) {
        g->c_entry(g->c_arg);
        __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
        pygo_g_state_set(g, PYGO_GST_DONE);
        PYGO_EVT(PYGO_EVT_G_COMPLETE, g, NULL, 0);
        return;
    }

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    _PyCFrame root_cframe_storage;
    pygo_install_initial_root_frame(&root_cframe_storage);
#elif PY_VERSION_HEX >= 0x030D0000
    pygo_install_initial_root_frame();
#endif

    /* Reset recursion counters at g entry -- each g has its own
     * physical C stack, so the per-tstate counters don't reflect
     * actual stack usage when shared across multiple gs. */
    {
        PyThreadState *ts = PyThreadState_GET();
        int py_limit = Py_GetRecursionLimit();
#if PY_VERSION_HEX >= 0x030C0000
        ts->py_recursion_remaining = py_limit;
        /* 200 frames matches what a 128KB stack can safely hold. */
        ts->c_recursion_remaining  = 200;
#else
        ts->recursion_remaining = py_limit;
#endif
    }

    res = PyObject_CallNoArgs(g->callable);
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
    /* The goroutine has run to completion; g->callable is never invoked
     * again.  Release it NOW rather than waiting for the g's last decref --
     * otherwise a pygo.aio task, whose callable is the task's own bound
     * _driver method, is kept alive forever by an unbreakable cycle:
     *
     *     task -> task._g (PygoG) -> pygo_g_t -> g->callable (_driver) -> task
     *
     * pygo_g_t is a C struct, invisible to cyclic GC, and the PygoG wrapper
     * has no tp_traverse, so the collector cannot see the g->callable edge and
     * never reclaims the completed task (its 'exception never retrieved'
     * warning never fires either).  Clearing callable here cuts that edge at
     * the source, so the remaining task graph collects by plain refcounting.
     *
     * Running the resulting finalizer here is safe: we are on g's own stack in
     * g's tstate (any finalizer frames land on g's datastack chunk, drained
     * immediately after this returns), and the scheduler still holds its
     * ref(s) to g, so g itself cannot be freed under us.  pygo_g_decref's later
     * Py_XDECREF(g->callable) becomes a NULL no-op.  Done last, so g->done
     * publishes a fully torn-down g. */
    Py_CLEAR(g->callable);
    /* RELEASE store on g->done publishes the prior g->result/g->error
     * writes; PygoG_done_get / PygoG_result_get load g->done with
     * ACQUIRE and only read result/error if done. */
    __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
    pygo_g_state_set(g, PYGO_GST_DONE);
    PYGO_EVT(PYGO_EVT_G_COMPLETE, g, NULL, 0);
    /* Falls back through asm trampoline -> infinite swap to caller. */
}

/* ---- pygo_g_t slab allocator ----
 *
 * spawn/decref-last-ref on the hot path is dominated by
 * PyMem_Calloc(144) / PyMem_Free.  At 100k spawns/sec that's 200k
 * calls/sec through CPython's small-block allocator, which is
 * thread-contended on free-threaded 3.13t.
 *
 * Per-thread LIFO free list of recycled pygo_g_t.  Push on last
 * decref; pop on next spawn.  Reuses the `next` field for the
 * free-list link (it's NULL'd by ready_pop after each scheduler
 * cycle, so it's free real estate while g is unallocated).  Cap of
 * 256 entries / thread bounds memory footprint after a burst. */
/* Cap chosen so a steady-state spawn-heavy workload (100k+ gs in
 * flight) doesn't continuously overflow back to PyMem_Free, while
 * still bounding memory to ~150 KB / thread when not in use. */
#define PYGO_G_SLAB_CAP 4096
static PYGO_TLS pygo_g_t *pygo_g_slab = NULL;
static PYGO_TLS int pygo_g_slab_size = 0;

pygo_g_t *pygo_g_slab_alloc(void)
{
    pygo_g_t *g = pygo_g_slab;
    /* OOM fault injection (test-only; inert unless PYGO_FAULT_SPAWN_G is set):
     * simulate "can't allocate a goroutine struct" regardless of the slab
     * cache, so the spawn error paths get exercised. */
    if (PYGO_SPAWN_FINJ(PYGO_FAULT_SPAWN_G)) return NULL;
    if (g != NULL) {
        pygo_g_slab = g->next;
        pygo_g_slab_size--;
        /* Reuse path (HOT).  Clear only the fields up to the
         * introspection block -- everything from `state` onward (the
         * state byte, id, state_since_ns, park_fd/events, reg_prev/next)
         * is left untouched: reg_prev/reg_next MUST survive recycling
         * (the g stays linked in the global registry, see
         * pygo_introspect.c) and they are only ever written under the
         * registry lock, never here.  `state` is re-initialised
         * atomically below so a concurrent dump's atomic load never
         * races a non-atomic memset.  id / park_fd are reset at spawn.
         * spawn always overwrites callable, coro, refcount; we only
         * need to clear the rest. */
        memset(g, 0, offsetof(pygo_g_t, state));
        __atomic_store_n(&g->state, (unsigned char)PYGO_GST_INIT,
                         __ATOMIC_RELEASE);
        return g;
    }
    /* Cold path: a brand-new struct from the OS.  PyMem_Calloc zeroes
     * everything (incl. the registry links + id); link it into the
     * global registry exactly once for the struct's whole lifetime. */
    g = (pygo_g_t *)PyMem_Calloc(1, sizeof(*g));
    if (g != NULL) pygo_greg_link(g);
    return g;
}

void pygo_g_slab_free(pygo_g_t *g)
{
    pygo_g_state_set(g, PYGO_GST_FREED);
    if (pygo_g_slab_size >= PYGO_G_SLAB_CAP) {
        /* Struct is leaving for the OS -- drop it from the registry
         * first so the dump never walks freed memory. */
        pygo_greg_unlink(g);
        PyMem_Free(g);
        return;
    }
    g->next = pygo_g_slab;
    pygo_g_slab = g;
    pygo_g_slab_size++;
}

/* ---- Refcount ---- */
void pygo_g_incref(pygo_g_t *g)
{
    if (g) __atomic_add_fetch(&g->refcount, 1, __ATOMIC_RELAXED);
}

int pygo_g_try_incref(pygo_g_t *g)
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

void pygo_g_decref(pygo_g_t *g)
{
    int new_count;
    if (g == NULL) return;
    PYGO_EVT(PYGO_EVT_G_DECREF, g, NULL, (long long)g->refcount);
    /* ACQ_REL: pairs with other threads' decrefs so all prior writes
     * (including g->result / g->error / g->done done-flag updates)
     * are observable on the last reference's owner before free. */
    new_count = __atomic_sub_fetch(&g->refcount, 1, __ATOMIC_ACQ_REL);
    if (new_count <= 0) {
        pygo_pystate_snap_clear(&g->snap);
        if (g->tstate != NULL) {
            /* per-g tstate (PYGO_PER_G_TSTATE): g is done + detached, so its
             * tstate is current on no thread.  Clear+Delete frees its
             * datastack/frames/exc.  Mirrors mn_fini's hub-tstate teardown,
             * which also clears non-current tstates with the caller's tstate
             * held -- pygo_g_decref's contexts (hub completion, drain, PygoG
             * dealloc) all run with some other tstate current. */
            PyThreadState_Clear(g->tstate);
            PyThreadState_Delete(g->tstate);
            g->tstate = NULL;
        }
        if (g->coro != NULL) {
            pygo_coro_destroy(g->coro);
            g->coro = NULL;
        }
        Py_XDECREF(g->callable);
        Py_XDECREF(g->result);
        Py_XDECREF(g->error);
        if (g->limit_counted) {           /* release the admission slot */
            pygo_goroutine_release();
            g->limit_counted = 0;
        }
        pygo_g_slab_free(g);
    }
}

/* ---- Spawn ---- */
static PyObject *spawn_common(pygo_sched_t *s, PyObject *callable,
                              int noyield, size_t stack_size)
{
    pygo_g_t *g;
    if (__builtin_expect(!pygo_pct.inited, 0))
        pygo_pct_init();   /* read PYGO_PCT_SEED once, before any ready-pop */
    /* Backpressure: admit under the max-goroutines limit before allocating.
     * adm: 0 rejected, 1 admitted (no limit), 2 admitted+counted. */
    {
        int adm = pygo_goroutine_admit();
        if (adm == 0) {
            PyErr_Format(PyExc_RuntimeError,
                         "pygo: goroutine limit (%ld) exceeded",
                         pygo_get_max_goroutines());
            return NULL;
        }
        g = pygo_g_slab_alloc();
        if (g == NULL) {
            if (adm == 2) pygo_goroutine_release();
            PyErr_NoMemory();
            return NULL;
        }
        g->limit_counted = (unsigned char)(adm == 2);
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->refcount = 1;   /* one ref for the scheduler queue */
    g->owner = s;      /* per-thread sched that owns this g (Phase C) */
    g->noyield = noyield;
    __atomic_store_n(&g->id, pygo_next_goid(), __ATOMIC_RELAXED);
    g->park_fd = -1;   /* not parked on any fd yet (reset after slab reuse) */
    g->pct_prio = 0;   /* fresh goroutine: unassigned PCT priority (slab reuse) */
    g->pct_fifo = 0;   /* default reorderable; m_go(fifo=True) sets it after spawn */
    /* OOM fault injection (test-only): simulate a stack-allocation failure
     * (mmap ENOMEM) by forcing coro_new to "fail", exercising the existing
     * coro==NULL cleanup path.  Inert unless PYGO_FAULT_SPAWN_STACK is set. */
    g->coro = PYGO_SPAWN_FINJ(PYGO_FAULT_SPAWN_STACK)
              ? NULL : pygo_coro_new(stack_size, pygo_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(g->callable);
        pygo_g_slab_free(g);
        PyErr_SetString(PyExc_MemoryError, "pygo_coro_new failed");
        return NULL;
    }
    pygo_g_state_set(g, PYGO_GST_RUNNABLE);
    pygo_ready_push(s, g);
    return PyCapsule_New(g, "pygo_g", NULL);
}

PyObject *pygo_sched_spawn(pygo_sched_t *s, PyObject *callable)
{
    return spawn_common(s, callable, /*noyield*/0, (size_t)s->stack_size);
}

PyObject *pygo_sched_spawn_noyield(pygo_sched_t *s, PyObject *callable)
{
    return spawn_common(s, callable, /*noyield*/1, (size_t)s->stack_size);
}
