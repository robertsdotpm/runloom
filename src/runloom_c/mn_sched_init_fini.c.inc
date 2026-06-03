/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


int runloom_mn_init(int n_threads)
{
    int i;
    PyInterpreterState *interp;
    PyThreadState *main_ts;
    PyThreadState *saved;

    if (runloom_hubs != NULL) return 0;  /* already inited */
    runloom_introspect_init();   /* registry lock; idempotent (also done in PyInit) */
#if defined(RUNLOOM_OS_WINDOWS)
    /* CRITICAL_SECTION can't be statically initialised; do it exactly once
     * for the process (never destroyed -- a process-lifetime lock).  mn_init
     * runs single-threaded before any hub spawns, so the flag needs no
     * atomics beyond surviving repeated mn_init/mn_fini cycles. */
    {
        static int runq_lock_inited = 0;
        if (!runq_lock_inited) {
            runloom_mutex_init(&runloom_global_runq_lock);
            runq_lock_inited = 1;
        }
    }
#endif
    if (n_threads <= 0) {
        /* Auto: min(cores, 16).  Agent 6 measured runloom on 3.13t scaling
         * linearly to H=16 (~1.17 M ops/sec) then REGRESSING past H=32
         * for Python workloads -- atomic-refcount cache-line contention,
         * the same ceiling threads/asyncio hit.  16 is the right default
         * for Python; an explicit runloom_mn_init(H>16) is still honored
         * (worthwhile only for pure-C goroutine workloads). */
        n_threads = runloom_cpu_count();
        if (n_threads <= 0) n_threads = 4;
        if (n_threads > 16) n_threads = 16;
    }
    runloom_hubs = (runloom_hub_t *)PyMem_Calloc((size_t)n_threads, sizeof(runloom_hub_t));
    if (runloom_hubs == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    runloom_hub_count = n_threads;
    main_ts = PyThreadState_Get();
    interp = main_ts->interp;
    /* Per-g-tstate mode: capture the interp for PyThreadState_New and stand
     * the snap down BEFORE any hub thread starts running goroutines. */
    runloom_mn_interp = interp;
    runloom_set_per_g_tstate_mode(runloom_per_g_tstate_flag());
    /* Configure the sysmon watchdog (enable + threshold) BEFORE the hubs
     * spawn so the per-resume progress instrumentation is live immediately. */
    runloom_sysmon_config();
    /* Install the RUNLOOM_PREEMPT eval-frame wrapper (no-op unless enabled) while
     * we still hold the main tstate and before any hub runs Python. */
    runloom_preempt_install(interp);
    /* controlled M:N (RUNLOOM_MN_SEED): set up the execution baton before any hub
     * runs; forces handoff off, keeps preempt on (see runloom_mn_ctrl_init).
     * No-op when RUNLOOM_MN_SEED is unset. */
    runloom_mn_ctrl_init(n_threads);

    /* Init-once the lock that serializes the hubs' PyThreadState_New (see its
     * declaration in mn_sched.c).  mn_init is single-threaded here, before any
     * hub spawns, so the flag needs no atomics. */
    if (!runloom_hub_tstate_lock_inited) {
        runloom_mutex_init(&runloom_hub_tstate_lock);
        runloom_hub_tstate_lock_inited = 1;
    }

    for (i = 0; i < n_threads; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        h->id = i;
        runloom_sched_init(&h->sched);
        runloom_cldeque_init(&h->deque);
        runloom_mutex_init(&h->sub_lock);
        h->sub_head = h->sub_tail = NULL;
        /* h->tstate is created BY THE HUB THREAD (in runloom_hub_main), NOT
         * here.  PyThreadState_New eagerly binds the tstate to the CALLING
         * thread: bind_tstate() -> _Py_brc_init_thread() + the mimalloc bind,
         * which CPython documents "needs to be initialized from the active
         * thread."  Binding on the main thread stamps the hub tstate's
         * biased-refcount owner id (brc->tid) with MAIN's thread id, while
         * every object the hub later creates gets ob_tid = the HUB thread's id.
         * A cross-thread DECREF of such an object then can't find its owner in
         * the brc hash table (find_thread_state() looks up by ob_tid and
         * misses), so CPython concludes the owner "must have exited" and merges
         * the refcount out from under the still-running hub -- a data race on
         * ob_ref_local -> torn refcount -> premature free -> use-after-free (the
         * gc-churn SIGSEGV the hang-hunter found).  Creating the tstate on the
         * hub's own thread makes brc->tid == ob_tid for every object the hub
         * owns, so the owner is always found.  PyMem_Calloc already zeroed
         * h->tstate; sysmon / handoff / world-yield read it with an acquire load
         * and treat NULL as attach-state -1 (fail closed) during the brief
         * window before the hub publishes it. */
    }
    saved = PyEval_SaveThread();
    for (i = 0; i < n_threads; i++) {
        if (runloom_thread_create(&runloom_hubs[i].thread,
                               runloom_hub_main, &runloom_hubs[i]) != 0) {
            /* Mark the unspawned hubs as already-stopping + join the
             * ones we did spawn before returning -1. */
            int j;
            for (j = i; j < n_threads; j++) {
                __atomic_store_n(&runloom_hubs[j].stopping, 1, __ATOMIC_RELEASE);
            }
            for (j = 0; j < i; j++) {
                __atomic_store_n(&runloom_hubs[j].stopping, 1, __ATOMIC_RELEASE);
                runloom_thread_join(runloom_hubs[j].thread);
            }
            PyEval_RestoreThread(saved);
            for (j = 0; j < n_threads; j++) {
                /* tstate is created by the hub thread, so an unspawned hub (or
                 * one that failed its own PyThreadState_New) leaves it NULL.
                 * Spawned hubs were joined just above, so any tstate is now
                 * detached and safe to clear/delete from this (main) thread. */
                PyThreadState *hts = runloom_hubs[j].tstate;
                if (hts != NULL) {
                    PyThreadState_Clear(hts);
                    PyThreadState_Delete(hts);
                }
                runloom_mutex_destroy(&runloom_hubs[j].sub_lock);
            }
            PyMem_Free(runloom_hubs);
            runloom_hubs = NULL;
            runloom_hub_count = 0;
            PyErr_SetString(PyExc_OSError, "thread spawn failed");
            return -1;
        }
    }
    PyEval_RestoreThread(saved);
    /* Hubs are up; start the watchdog (no-op unless RUNLOOM_SYSMON / RUNLOOM_HANDOFF
     * is set) and the rescue thread (no-op unless RUNLOOM_HANDOFF is set). */
    runloom_sysmon_spawn();
    runloom_handoff_spawn();
    return n_threads;
}

/* Release any gs still parked in this hub's queues after the hub
 * thread has exited.  Hub thread is already joined by the caller, so
 * sub_head / deque / sched.ready / sched.sleep_heap are quiescent and
 * safe to drain from a single thread regardless of which primitive
 * they normally use for concurrent access.
 *
 * Each leaked g still holds: its Python callable ref, an fcontext
 * stack, and a slab entry.  runloom_g_decref unwinds all three when its
 * last reference drops.  The hub holds exactly one ref per g it
 * accepted (taken at spawn in runloom_mn_go_core, dropped at completion
 * in hub_main), so a single decref here mirrors the completion path. */
static void runloom_mn_hub_drain_leftovers(runloom_hub_t *h)
{
    runloom_g_t *sub;
    runloom_g_t *g;
    sub = h->sub_head;
    h->sub_head = h->sub_tail = NULL;
    while (sub != NULL) {
        runloom_g_t *next = sub->next;
        sub->next = NULL;
        runloom_g_decref(sub);
        sub = next;
    }
    while ((g = runloom_sched_ready_pop(&h->sched)) != NULL) {
        runloom_g_decref(g);
    }
    while ((g = (runloom_g_t *)runloom_cldeque_pop(&h->deque)) != NULL) {
        runloom_g_decref(g);
    }
    while (h->sched.sleep_size > 0) {
        g = runloom_sched_sleep_pop(&h->sched);
        if (g != NULL) runloom_g_decref(g);
    }
}

void runloom_mn_fini(void)
{
    int i;
    if (runloom_hubs == NULL) return;
    /* Stop the watchdog before any hub teardown so its scan never touches
     * freed hub state, then stop the rescue thread (after the watchdog so no
     * fresh dispatch lands) so it can never hold a tstate about to be deleted.
     * At fini all gs have completed -> no wedge -> the rescue is idle, but the
     * stop flag also unwinds an in-progress rescue (load base + detach). */
    runloom_sysmon_stop_join();
    runloom_handoff_stop_join();
    for (i = 0; i < runloom_hub_count; i++) {
        __atomic_store_n(&runloom_hubs[i].stopping, 1, __ATOMIC_RELEASE);
    }
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (i = 0; i < runloom_hub_count; i++) {
            runloom_thread_join(runloom_hubs[i].thread);
        }
        PyEval_RestoreThread(saved);
    }
    /* controlled-mode baton: all hubs joined, nobody can acquire/release now */
    runloom_mn_ctrl_fini();
    /* Uninstall the preempt wrapper now that every hub thread is joined (no one
     * can call it) and the main tstate is held -- restores the original
     * eval-frame func so a later mn_init re-captures it cleanly. */
    runloom_preempt_uninstall(runloom_mn_interp);
    /* Drain leftover gs BEFORE tearing down per-hub tstates: g->snap
     * may reference per-tstate state (exc_info, datastack chunks) that
     * pystate_snap_clear needs to release while the tstate is still
     * walkable.  runloom_g_decref also runs Py_XDECREF on callable /
     * result / error, which needs the main thread's GIL -- already
     * held here (we restored above). */
    for (i = 0; i < runloom_hub_count; i++) {
        runloom_mn_hub_drain_leftovers(&runloom_hubs[i]);
    }
    /* Drain the global run-queue too.  Exactly-once-wake means an entry here
     * is a single, live, yet-to-run woken g holding its one scheduler ref (no
     * per-entry ref; no stale duplicates).  A single decref mirrors the
     * completion path and frees it -- exactly like runloom_mn_hub_drain_leftovers
     * does for the sub_list / deque / FIFO.  Hubs are joined, so this is
     * single-threaded -- no lock needed. */
    {
        runloom_g_t *g = runloom_global_runq_head;
        runloom_global_runq_head = runloom_global_runq_tail = NULL;
        __atomic_store_n(&runloom_global_runq_len, 0, __ATOMIC_RELEASE);
        while (g != NULL) {
            runloom_g_t *next = g->next;
            g->next = NULL;
            runloom_g_decref(g);
            g = next;
        }
    }
    for (i = 0; i < runloom_hub_count; i++) {
        /* Created on the hub thread (see runloom_mn_init); NULL if that hub
         * never started or failed its own PyThreadState_New.  Hubs are joined
         * by now, so a non-NULL tstate is detached and safe to delete here. */
        PyThreadState *hts = runloom_hubs[i].tstate;
        if (hts != NULL) {
            PyThreadState_Clear(hts);
            PyThreadState_Delete(hts);
        }
        runloom_mutex_destroy(&runloom_hubs[i].sub_lock);
    }
    PyMem_Free(runloom_hubs);
    runloom_hubs = NULL;
    runloom_hub_count = 0;
    /* Reset the global pending counter so the next mn_init starts at
     * 0.  Any leaked g (incomplete at fini) leaves a non-zero residue
     * otherwise. */
    __atomic_store_n(&runloom_mn_pending_global, 0, __ATOMIC_RELEASE);
    /* Stand the snap back up for any later single-thread scheduler use. */
    runloom_set_per_g_tstate_mode(0);
    runloom_mn_interp = NULL;
}

/* Reset the M:N scheduler in a forked CHILD.  After fork() only the
 * forking thread survives -- the hub OS threads are gone -- so we must
 * NOT join them (runloom_mn_run would hang forever waiting on dead hubs:
 * the reproduced fork deadlock) and must NOT free/clear their structs
 * (they hold PyThreadStates + locks tied to the dead threads).  Instead
 * abandon the inherited hub array (a one-time leak in a short-lived
 * child) and reset every global so the child sees an UNINITIALISED M:N
 * scheduler: a fresh runloom_mn_init() works, and until then runloom runs on
 * the single-thread scheduler.  The child is single-threaded at this
 * point, so no lock is taken -- we only re-init the global run-queue lock
 * in case a dead hub held it at fork time. */
void runloom_mn_reset_after_fork(void)
{
    runloom_hubs = NULL;            /* abandon (leak) the inherited hub array */
    runloom_hub_count = 0;
    runloom_mn_spawn_counter = 0;
    __atomic_store_n(&runloom_mn_pending_global, 0, __ATOMIC_RELAXED);
    runloom_mn_interp = NULL;
    runloom_set_per_g_tstate_mode(0);
    runloom_mutex_init(&runloom_global_runq_lock);   /* may be inherited held */
    runloom_global_runq_head = runloom_global_runq_tail = NULL;
    __atomic_store_n(&runloom_global_runq_len, 0, __ATOMIC_RELAXED);
    runloom_tls_hub = NULL;        /* the surviving thread is no longer a hub */
    runloom_tls_current_g = NULL;
}

/* Internal core: pick hub, alloc g, set up coro, submit, bump counters.
 * Either callable (Python path) or c_fn+c_arg (C-only path) must be set,
 * not both.  Returns 0 on success, -1 on failure (errno set).  Python
 * error is also set on failure when callable != NULL. */
static int runloom_mn_go_core(PyObject *callable, runloom_c_entry_fn c_fn,
                           void *c_arg)
{
    long n;
    int hub_idx;
    runloom_g_t *g;
    runloom_hub_t *h;
    if (runloom_hubs == NULL) {
        if (callable != NULL) {
            PyErr_SetString(PyExc_RuntimeError,
                            "runloom_mn_init() must be called first");
        }
        errno = EINVAL;
        return -1;
    }
    n = __atomic_fetch_add(&runloom_mn_spawn_counter, 1, __ATOMIC_RELAXED);
    hub_idx = (int)(n % runloom_hub_count);
    h = &runloom_hubs[hub_idx];
    {   /* Backpressure: admit under the max-goroutines limit. */
        int adm = runloom_goroutine_admit();
        if (adm == 0) {
            if (callable != NULL)
                PyErr_Format(PyExc_RuntimeError,
                             "runloom: goroutine limit (%ld) exceeded",
                             runloom_get_max_goroutines());
            errno = EAGAIN;
            return -1;
        }
        g = runloom_g_slab_alloc();
        if (g == NULL) {
            if (adm == 2) runloom_goroutine_release();
            if (callable != NULL) PyErr_NoMemory();
            errno = ENOMEM;
            return -1;
        }
        g->limit_counted = (unsigned char)(adm == 2);
    }
    if (callable != NULL) {
        Py_INCREF(callable);
        g->callable = callable;
        /* Stack-advice: note the kind at spawn (0 unless profiling on). */
        g->advice_key = runloom_advice_note_spawn(callable);
    } else {
        g->c_entry = c_fn;
        g->c_arg   = c_arg;
        g->advice_key = 0;   /* C-entry goroutines aren't profiled */
    }
    g->refcount = 1;
    g->owner = &h->sched;   /* owning hub's sched -- groups goroutines in the dump */
    __atomic_store_n(&g->id, runloom_next_goid(), __ATOMIC_RELAXED);
    g->park_fd = -1;
    g->coro = runloom_coro_new((size_t)h->sched.stack_size,
                            runloom_g_entry, g);
    if (g->coro == NULL) {
        if (callable != NULL) {
            Py_DECREF(callable);
            PyErr_NoMemory();
        }
        runloom_greg_unlink(g);
        PyMem_Free(g);
        errno = ENOMEM;
        return -1;
    }
    /* Wake-state machine init.  Any global-runq mode (per-g-tstate OR
     * steal-woken) reads wake_state: a fresh g has never parked, so it is
     * conceptually owned/running from the moment a hub first resumes it
     * (slab-zeroed wake_state would be PARKED, which the QUEUED->RUNNING claim
     * and the park CAS both mis-handle).  Lift it to RUNNING up front. */
    if (runloom_use_global_runq()) {
        __atomic_store_n(&g->wake_state, RUNLOOM_WS_RUNNING, __ATOMIC_RELEASE);
    }
    /* RUNLOOM_PER_G_TSTATE only: give the g its own migratable PyThreadState.
     * g->tstate was zeroed by the slab alloc, so every other path leaves it
     * NULL (and runloom_g_decref's teardown is a no-op).  Steal-woken migrates via
     * the snap instead, so it allocates no tstate here. */
    if (runloom_get_per_g_tstate_mode() && runloom_mn_interp != NULL) {
        g->tstate = PyThreadState_New(runloom_mn_interp);
        if (g->tstate == NULL) {
            runloom_coro_destroy(g->coro);
            if (callable != NULL) {
                Py_DECREF(callable);
                PyErr_NoMemory();
            }
            runloom_greg_unlink(g);
            PyMem_Free(g);
            errno = ENOMEM;
            return -1;
        }
    }
    runloom_mn_hub_submit(h, g);
    runloom_mn_pending_inc(h);
    return 0;
}

int runloom_mn_go_c(runloom_c_entry_fn fn, void *arg)
{
    if (fn == NULL) { errno = EINVAL; return -1; }
    return runloom_mn_go_core(NULL, fn, arg);
}

PyObject *runloom_mn_go(PyObject *callable)
{
    if (runloom_mn_go_core(callable, NULL, NULL) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

Py_ssize_t runloom_mn_run(void)
{
    int i;
    /* Py_ssize_t to match the per-hub completed counter (which is
     * Py_ssize_t -- long long on 64-bit Windows MSVC, long on
     * 64-bit POSIX).  Using plain long would truncate on Windows. */
    Py_ssize_t total_completed = 0;
    PyThreadState *saved;
    /* Controlled-replay: all pre-run mn_go placement is done; release the hubs'
     * startup gate so their first census is over the complete requester set. */
    runloom_mn_ctrl_arm();
    saved = PyEval_SaveThread();
    for (;;) {
        /* Read the GLOBAL pending counter, not the per-hub fields.
         * The per-hub fields race during work-stealing (source dec
         * before dest inc); the global counter is only touched on
         * spawn + completion, so it's monotonic across the lifecycle
         * of each g and reliably reaches 0 iff all gs are done. */
        long total = __atomic_load_n(&runloom_mn_pending_global,
                                     __ATOMIC_ACQUIRE);
        if (total == 0) break;
        runloom_sleep_ns(1000000LL);   /* 1 ms poll */
    }
    /* Silence the unused-variable warning that the per-hub loop used
     * to consume. */
    (void)i;
    PyEval_RestoreThread(saved);
    for (i = 0; i < runloom_hub_count; i++) {
        total_completed += __atomic_load_n(&runloom_hubs[i].sched.completed,
                                           __ATOMIC_ACQUIRE);
    }
    return total_completed;
}
