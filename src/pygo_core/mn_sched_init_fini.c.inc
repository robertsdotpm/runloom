/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


int pygo_mn_init(int n_threads)
{
    int i;
    PyInterpreterState *interp;
    PyThreadState *main_ts;
    PyThreadState *saved;

    if (pygo_hubs != NULL) return 0;  /* already inited */
    pygo_introspect_init();   /* registry lock; idempotent (also done in PyInit) */
#if defined(PYGO_OS_WINDOWS)
    /* CRITICAL_SECTION can't be statically initialised; do it exactly once
     * for the process (never destroyed -- a process-lifetime lock).  mn_init
     * runs single-threaded before any hub spawns, so the flag needs no
     * atomics beyond surviving repeated mn_init/mn_fini cycles. */
    {
        static int runq_lock_inited = 0;
        if (!runq_lock_inited) {
            pygo_mutex_init(&pygo_global_runq_lock);
            runq_lock_inited = 1;
        }
    }
#endif
    if (n_threads <= 0) {
        /* Auto: min(cores, 16).  Agent 6 measured pygo on 3.13t scaling
         * linearly to H=16 (~1.17 M ops/sec) then REGRESSING past H=32
         * for Python workloads -- atomic-refcount cache-line contention,
         * the same ceiling threads/asyncio hit.  16 is the right default
         * for Python; an explicit pygo_mn_init(H>16) is still honored
         * (worthwhile only for pure-C goroutine workloads). */
        n_threads = pygo_cpu_count();
        if (n_threads <= 0) n_threads = 4;
        if (n_threads > 16) n_threads = 16;
    }
    pygo_hubs = (pygo_hub_t *)PyMem_Calloc((size_t)n_threads, sizeof(pygo_hub_t));
    if (pygo_hubs == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    pygo_hub_count = n_threads;
    main_ts = PyThreadState_Get();
    interp = main_ts->interp;
    /* Per-g-tstate mode: capture the interp for PyThreadState_New and stand
     * the snap down BEFORE any hub thread starts running goroutines. */
    pygo_mn_interp = interp;
    pygo_set_per_g_tstate_mode(pygo_per_g_tstate_flag());
    /* Configure the sysmon watchdog (enable + threshold) BEFORE the hubs
     * spawn so the per-resume progress instrumentation is live immediately. */
    pygo_sysmon_config();
    /* Install the PYGO_PREEMPT eval-frame wrapper (no-op unless enabled) while
     * we still hold the main tstate and before any hub runs Python. */
    pygo_preempt_install(interp);
    /* controlled M:N (PYGO_MN_SEED): set up the execution baton before any hub
     * runs; forces handoff off, keeps preempt on (see pygo_mn_ctrl_init).
     * No-op when PYGO_MN_SEED is unset. */
    pygo_mn_ctrl_init(n_threads);

    for (i = 0; i < n_threads; i++) {
        pygo_hub_t *h = &pygo_hubs[i];
        h->id = i;
        pygo_sched_init(&h->sched);
        pygo_cldeque_init(&h->deque);
        pygo_mutex_init(&h->sub_lock);
        h->sub_head = h->sub_tail = NULL;
        h->tstate = PyThreadState_New(interp);
        if (h->tstate == NULL) {
            /* Clean up everything we've partially initialised so far
             * (mutexes + earlier tstates) and reset module state.
             * Without this, a later mn_fini would join an
             * uninitialised thread handle = undefined behaviour. */
            int j;
            pygo_mutex_destroy(&h->sub_lock);   /* this hub's mutex */
            for (j = 0; j < i; j++) {
                PyThreadState_Clear(pygo_hubs[j].tstate);
                PyThreadState_Delete(pygo_hubs[j].tstate);
                pygo_mutex_destroy(&pygo_hubs[j].sub_lock);
            }
            PyMem_Free(pygo_hubs);
            pygo_hubs = NULL;
            pygo_hub_count = 0;
            PyErr_NoMemory();
            return -1;
        }
    }
    saved = PyEval_SaveThread();
    for (i = 0; i < n_threads; i++) {
        if (pygo_thread_create(&pygo_hubs[i].thread,
                               pygo_hub_main, &pygo_hubs[i]) != 0) {
            /* Mark the unspawned hubs as already-stopping + join the
             * ones we did spawn before returning -1. */
            int j;
            for (j = i; j < n_threads; j++) {
                __atomic_store_n(&pygo_hubs[j].stopping, 1, __ATOMIC_RELEASE);
            }
            for (j = 0; j < i; j++) {
                __atomic_store_n(&pygo_hubs[j].stopping, 1, __ATOMIC_RELEASE);
                pygo_thread_join(pygo_hubs[j].thread);
            }
            PyEval_RestoreThread(saved);
            for (j = 0; j < n_threads; j++) {
                PyThreadState_Clear(pygo_hubs[j].tstate);
                PyThreadState_Delete(pygo_hubs[j].tstate);
                pygo_mutex_destroy(&pygo_hubs[j].sub_lock);
            }
            PyMem_Free(pygo_hubs);
            pygo_hubs = NULL;
            pygo_hub_count = 0;
            PyErr_SetString(PyExc_OSError, "thread spawn failed");
            return -1;
        }
    }
    PyEval_RestoreThread(saved);
    /* Hubs are up; start the watchdog (no-op unless PYGO_SYSMON / PYGO_HANDOFF
     * is set) and the rescue thread (no-op unless PYGO_HANDOFF is set). */
    pygo_sysmon_spawn();
    pygo_handoff_spawn();
    return n_threads;
}

/* Release any gs still parked in this hub's queues after the hub
 * thread has exited.  Hub thread is already joined by the caller, so
 * sub_head / deque / sched.ready / sched.sleep_heap are quiescent and
 * safe to drain from a single thread regardless of which primitive
 * they normally use for concurrent access.
 *
 * Each leaked g still holds: its Python callable ref, an fcontext
 * stack, and a slab entry.  pygo_g_decref unwinds all three when its
 * last reference drops.  The hub holds exactly one ref per g it
 * accepted (taken at spawn in pygo_mn_go_core, dropped at completion
 * in hub_main), so a single decref here mirrors the completion path. */
static void pygo_mn_hub_drain_leftovers(pygo_hub_t *h)
{
    pygo_g_t *sub;
    pygo_g_t *g;
    sub = h->sub_head;
    h->sub_head = h->sub_tail = NULL;
    while (sub != NULL) {
        pygo_g_t *next = sub->next;
        sub->next = NULL;
        pygo_g_decref(sub);
        sub = next;
    }
    while ((g = pygo_sched_ready_pop(&h->sched)) != NULL) {
        pygo_g_decref(g);
    }
    while ((g = (pygo_g_t *)pygo_cldeque_pop(&h->deque)) != NULL) {
        pygo_g_decref(g);
    }
    while (h->sched.sleep_size > 0) {
        g = pygo_sched_sleep_pop(&h->sched);
        if (g != NULL) pygo_g_decref(g);
    }
}

void pygo_mn_fini(void)
{
    int i;
    if (pygo_hubs == NULL) return;
    /* Stop the watchdog before any hub teardown so its scan never touches
     * freed hub state, then stop the rescue thread (after the watchdog so no
     * fresh dispatch lands) so it can never hold a tstate about to be deleted.
     * At fini all gs have completed -> no wedge -> the rescue is idle, but the
     * stop flag also unwinds an in-progress rescue (load base + detach). */
    pygo_sysmon_stop_join();
    pygo_handoff_stop_join();
    for (i = 0; i < pygo_hub_count; i++) {
        __atomic_store_n(&pygo_hubs[i].stopping, 1, __ATOMIC_RELEASE);
    }
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (i = 0; i < pygo_hub_count; i++) {
            pygo_thread_join(pygo_hubs[i].thread);
        }
        PyEval_RestoreThread(saved);
    }
    /* controlled-mode baton: all hubs joined, nobody can acquire/release now */
    pygo_mn_ctrl_fini();
    /* Uninstall the preempt wrapper now that every hub thread is joined (no one
     * can call it) and the main tstate is held -- restores the original
     * eval-frame func so a later mn_init re-captures it cleanly. */
    pygo_preempt_uninstall(pygo_mn_interp);
    /* Drain leftover gs BEFORE tearing down per-hub tstates: g->snap
     * may reference per-tstate state (exc_info, datastack chunks) that
     * pystate_snap_clear needs to release while the tstate is still
     * walkable.  pygo_g_decref also runs Py_XDECREF on callable /
     * result / error, which needs the main thread's GIL -- already
     * held here (we restored above). */
    for (i = 0; i < pygo_hub_count; i++) {
        pygo_mn_hub_drain_leftovers(&pygo_hubs[i]);
    }
    /* Drain the global run-queue too.  Exactly-once-wake means an entry here
     * is a single, live, yet-to-run woken g holding its one scheduler ref (no
     * per-entry ref; no stale duplicates).  A single decref mirrors the
     * completion path and frees it -- exactly like pygo_mn_hub_drain_leftovers
     * does for the sub_list / deque / FIFO.  Hubs are joined, so this is
     * single-threaded -- no lock needed. */
    {
        pygo_g_t *g = pygo_global_runq_head;
        pygo_global_runq_head = pygo_global_runq_tail = NULL;
        __atomic_store_n(&pygo_global_runq_len, 0, __ATOMIC_RELEASE);
        while (g != NULL) {
            pygo_g_t *next = g->next;
            g->next = NULL;
            pygo_g_decref(g);
            g = next;
        }
    }
    for (i = 0; i < pygo_hub_count; i++) {
        PyThreadState_Clear(pygo_hubs[i].tstate);
        PyThreadState_Delete(pygo_hubs[i].tstate);
        pygo_mutex_destroy(&pygo_hubs[i].sub_lock);
    }
    PyMem_Free(pygo_hubs);
    pygo_hubs = NULL;
    pygo_hub_count = 0;
    /* Reset the global pending counter so the next mn_init starts at
     * 0.  Any leaked g (incomplete at fini) leaves a non-zero residue
     * otherwise. */
    __atomic_store_n(&pygo_mn_pending_global, 0, __ATOMIC_RELEASE);
    /* Stand the snap back up for any later single-thread scheduler use. */
    pygo_set_per_g_tstate_mode(0);
    pygo_mn_interp = NULL;
}

/* Internal core: pick hub, alloc g, set up coro, submit, bump counters.
 * Either callable (Python path) or c_fn+c_arg (C-only path) must be set,
 * not both.  Returns 0 on success, -1 on failure (errno set).  Python
 * error is also set on failure when callable != NULL. */
static int pygo_mn_go_core(PyObject *callable, pygo_c_entry_fn c_fn,
                           void *c_arg)
{
    long n;
    int hub_idx;
    pygo_g_t *g;
    pygo_hub_t *h;
    if (pygo_hubs == NULL) {
        if (callable != NULL) {
            PyErr_SetString(PyExc_RuntimeError,
                            "pygo_mn_init() must be called first");
        }
        errno = EINVAL;
        return -1;
    }
    n = __atomic_fetch_add(&pygo_mn_spawn_counter, 1, __ATOMIC_RELAXED);
    hub_idx = (int)(n % pygo_hub_count);
    h = &pygo_hubs[hub_idx];
    g = pygo_g_slab_alloc();
    if (g == NULL) {
        if (callable != NULL) PyErr_NoMemory();
        errno = ENOMEM;
        return -1;
    }
    if (callable != NULL) {
        Py_INCREF(callable);
        g->callable = callable;
    } else {
        g->c_entry = c_fn;
        g->c_arg   = c_arg;
    }
    g->refcount = 1;
    g->owner = &h->sched;   /* owning hub's sched -- groups goroutines in the dump */
    __atomic_store_n(&g->id, pygo_next_goid(), __ATOMIC_RELAXED);
    g->park_fd = -1;
    g->coro = pygo_coro_new((size_t)h->sched.stack_size,
                            pygo_g_entry, g);
    if (g->coro == NULL) {
        if (callable != NULL) {
            Py_DECREF(callable);
            PyErr_NoMemory();
        }
        pygo_greg_unlink(g);
        PyMem_Free(g);
        errno = ENOMEM;
        return -1;
    }
    /* Wake-state machine init.  Any global-runq mode (per-g-tstate OR
     * steal-woken) reads wake_state: a fresh g has never parked, so it is
     * conceptually owned/running from the moment a hub first resumes it
     * (slab-zeroed wake_state would be PARKED, which the QUEUED->RUNNING claim
     * and the park CAS both mis-handle).  Lift it to RUNNING up front. */
    if (pygo_use_global_runq()) {
        __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING, __ATOMIC_RELEASE);
    }
    /* PYGO_PER_G_TSTATE only: give the g its own migratable PyThreadState.
     * g->tstate was zeroed by the slab alloc, so every other path leaves it
     * NULL (and pygo_g_decref's teardown is a no-op).  Steal-woken migrates via
     * the snap instead, so it allocates no tstate here. */
    if (pygo_get_per_g_tstate_mode() && pygo_mn_interp != NULL) {
        g->tstate = PyThreadState_New(pygo_mn_interp);
        if (g->tstate == NULL) {
            pygo_coro_destroy(g->coro);
            if (callable != NULL) {
                Py_DECREF(callable);
                PyErr_NoMemory();
            }
            pygo_greg_unlink(g);
            PyMem_Free(g);
            errno = ENOMEM;
            return -1;
        }
    }
    pygo_mn_hub_submit(h, g);
    pygo_mn_pending_inc(h);
    return 0;
}

int pygo_mn_go_c(pygo_c_entry_fn fn, void *arg)
{
    if (fn == NULL) { errno = EINVAL; return -1; }
    return pygo_mn_go_core(NULL, fn, arg);
}

PyObject *pygo_mn_go(PyObject *callable)
{
    if (pygo_mn_go_core(callable, NULL, NULL) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

Py_ssize_t pygo_mn_run(void)
{
    int i;
    /* Py_ssize_t to match the per-hub completed counter (which is
     * Py_ssize_t -- long long on 64-bit Windows MSVC, long on
     * 64-bit POSIX).  Using plain long would truncate on Windows. */
    Py_ssize_t total_completed = 0;
    PyThreadState *saved;
    /* Controlled-replay: all pre-run mn_go placement is done; release the hubs'
     * startup gate so their first census is over the complete requester set. */
    pygo_mn_ctrl_arm();
    saved = PyEval_SaveThread();
    for (;;) {
        /* Read the GLOBAL pending counter, not the per-hub fields.
         * The per-hub fields race during work-stealing (source dec
         * before dest inc); the global counter is only touched on
         * spawn + completion, so it's monotonic across the lifecycle
         * of each g and reliably reaches 0 iff all gs are done. */
        long total = __atomic_load_n(&pygo_mn_pending_global,
                                     __ATOMIC_ACQUIRE);
        if (total == 0) break;
        pygo_sleep_ns(1000000LL);   /* 1 ms poll */
    }
    /* Silence the unused-variable warning that the per-hub loop used
     * to consume. */
    (void)i;
    PyEval_RestoreThread(saved);
    for (i = 0; i < pygo_hub_count; i++) {
        total_completed += __atomic_load_n(&pygo_hubs[i].sched.completed,
                                           __ATOMIC_ACQUIRE);
    }
    return total_completed;
}
