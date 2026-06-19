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
         * (worthwhile only for pure-C fiber workloads). */
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
     * the snap down BEFORE any hub thread starts running fibers.
     *
     * RUNLOOM_STEAL_WOKEN REDIRECTS here: its original snap-migration mechanism
     * is structurally unsound (a suspended fiber's eval frames bake in the origin
     * hub's tstate pointer, which the snap cannot re-root, so a migrated fiber
     * drives the origin hub's tstate from a foreign OS thread -- TSan-proven data
     * race -> SEGV; see docs/dev/STEAL_WOKEN_CLEANUP.md).  Per-g-tstate delivers
     * the SAME goal (any idle hub may resume a wedged hub's woken fiber, closing
     * the wedged-hub strand) CORRECTLY -- each fiber owns a migratable tstate that
     * is valid on any hub.  So RUNLOOM_STEAL_WOKEN now enables per-g-tstate and the
     * broken snap-migration branch in hub_main is never taken (dead for both
     * flags; removable in a follow-up). */
    runloom_mn_interp = interp;
    /* Through the safety interlock (runloom_resolve_migratable_mode): a migratable
     * flag activates per-g-tstate ONLY with RUNLOOM_ALLOW_UNSAFE_MIGRATION=1; else
     * it warns + stays off (default scheduler).  Gating here also keeps a flagged-
     * but-gated RUNLOOM_STEAL_WOKEN out of the unsound snap branch -- see
     * runloom_use_global_runq. */
    runloom_set_per_g_tstate_mode(runloom_resolve_migratable_mode());
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
        h->sub_tail = NULL;
        __atomic_store_n(&h->sub_head, (runloom_g_t *)NULL, __ATOMIC_RELAXED);
        /* BUG #10 per-hub idle wake (see mn_sched.c sub_gen). */
        h->sub_gen = 0;
        runloom_mutex_init(&h->idle_lock);
        runloom_cond_init(&h->idle_cond);
        h->idle_waiting = 0;
        /* io_uring-as-loop: the wake eventfd is created by the hub thread in
         * runloom_hub_main only when the loop backend is on; -1 = absent. */
        h->loop_wake_fd = -1;
        h->ring_waiting = 0;
        /* Controlled-replay deferred-publish staging (barrier mode only): one
         * per-target list so a segment's cross-hub submits flush atomically at
         * release.  PyMem_Calloc zeroes; OOM -> NULL -> hub_submit falls back to
         * the immediate (non-deterministic) push, still correct. */
        h->stage_head = NULL;
        h->stage_tail = NULL;
        h->stage_pending = 0;
        if (runloom_mn_ctrl.enabled && runloom_mn_ctrl.barrier) {
            h->stage_head = (runloom_g_t **)PyMem_Calloc((size_t)n_threads,
                                                         sizeof(runloom_g_t *));
            h->stage_tail = (runloom_g_t **)PyMem_Calloc((size_t)n_threads,
                                                         sizeof(runloom_g_t *));
            if (h->stage_head == NULL || h->stage_tail == NULL) {
                PyMem_Free(h->stage_head); h->stage_head = NULL;
                PyMem_Free(h->stage_tail); h->stage_tail = NULL;
            }
        }
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
                runloom_mutex_destroy(&runloom_hubs[j].idle_lock);
                runloom_cond_destroy(&runloom_hubs[j].idle_cond);
            }
            PyMem_Free(runloom_hubs);
            runloom_hubs = NULL;
            runloom_hub_count = 0;
            PyErr_SetString(PyExc_OSError, "thread spawn failed");
            return -1;
        }
    }
    PyEval_RestoreThread(saved);
    /* Resolve the depot auto-cap's SAFE_MAX (reads /proc once) before the watchdog,
     * whose tick will recompute the cap from the live-stack watermark. */
    runloom_stack_autocap_init();
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
 * accepted (taken at spawn in runloom_mn_fiber_core, dropped at completion
 * in hub_main), so a single decref here mirrors the completion path. */
static void runloom_mn_hub_drain_leftovers(runloom_hub_t *h)
{
    runloom_g_t *sub;
    runloom_g_t *g;
    /* A still-queued g (in_sub_queue==1) in the default path also holds the
     * queue ref hub_submit took; drop it in addition to the spawn ref the
     * existing single decref releases.  Self-gated by the 1->0 exchange; skipped
     * in per-g-tstate mode (no queue ref there).  RUNLOOM_DROP_QREF is a no-op
     * for sleep-heap / raw-yield gs (in_sub_queue==0). */
    int per_g_qref = !runloom_get_per_g_tstate_mode();
#define RUNLOOM_DROP_QREF(gp) do {                                            \
        if (per_g_qref &&                                                     \
            __atomic_exchange_n(&(gp)->in_sub_queue, 0, __ATOMIC_ACQ_REL) == 1) \
            runloom_g_decref(gp);                                             \
    } while (0)
    sub = h->sub_head;
    h->sub_tail = NULL;
    __atomic_store_n(&h->sub_head, (runloom_g_t *)NULL, __ATOMIC_RELAXED);  /* census x-thread read */
    while (sub != NULL) {
        runloom_g_t *next = sub->next;
        sub->next = NULL;
        RUNLOOM_DROP_QREF(sub);
        runloom_g_decref(sub);
        sub = next;
    }
    /* Residual deferred-publish staging (barrier mode): a segment always flushes
     * at release, so these are normally empty -- drain defensively for an
     * abort/early-exit path so a staged g's hub ref is not leaked. */
    if (h->stage_head != NULL) {
        int t;
        for (t = 0; t < runloom_hub_count; t++) {
            runloom_g_t *s = h->stage_head[t];
            h->stage_head[t] = h->stage_tail[t] = NULL;
            while (s != NULL) {
                runloom_g_t *next = s->next;
                s->next = NULL;
                RUNLOOM_DROP_QREF(s);
                runloom_g_decref(s);
                s = next;
            }
        }
        h->stage_pending = 0;
    }
    while ((g = runloom_sched_ready_pop(&h->sched)) != NULL) {
        RUNLOOM_DROP_QREF(g);
        runloom_g_decref(g);
    }
    while ((g = (runloom_g_t *)runloom_cldeque_pop(&h->deque)) != NULL) {
        RUNLOOM_DROP_QREF(g);
        runloom_g_decref(g);
    }
    while (h->sched.sleep_size > 0) {
        g = runloom_sched_sleep_pop(&h->sched);
        if (g != NULL) { RUNLOOM_DROP_QREF(g); runloom_g_decref(g); }
    }
#undef RUNLOOM_DROP_QREF
}

void runloom_mn_fini(void)
{
    int i;
    if (runloom_hubs == NULL) return;
    /* Bump the session generation FIRST (RELEASE), before any hub state is torn
     * down, so the ENTIRE teardown window -- not just the post-free tail -- is
     * covered: a RunloomG handle that outlives this run (a foreign thread still
     * holding one and calling .wake()) sees the changed generation and treats
     * its g's park_hub as a no-op, never dereferencing a hub that is mid-
     * teardown or already freed.  All hubs' gs have completed by now (mn_run
     * joins on the pending count), so there is no legitimate in-session wake to
     * lose by gating early.  See runloom_mn_gen in mn_sched.c and the dekker
     * finding (docs/dev/repro/DEKKER_SIGSEGV_FINDING.md). */
    __atomic_add_fetch(&runloom_mn_gen, 1, __ATOMIC_RELEASE);
    /* Stop the watchdog before any hub teardown so its scan never touches
     * freed hub state, then stop the rescue thread (after the watchdog so no
     * fresh dispatch lands) so it can never hold a tstate about to be deleted.
     * At fini all gs have completed -> no wedge -> the rescue is idle, but the
     * stop flag also unwinds an in-progress rescue (load base + detach). */
    runloom_sysmon_stop_join();
    runloom_handoff_stop_join();
    for (i = 0; i < runloom_hub_count; i++) {
        __atomic_store_n(&runloom_hubs[i].stopping, 1, __ATOMIC_RELEASE);
        /* BUG #10 idle wake: a hub parked in its idle condvar must observe the
         * stop promptly instead of waiting out idle_ns.  Signal under the lock
         * so it can't slip between the hub's stopping re-check and its wait. */
        runloom_mutex_lock(&runloom_hubs[i].idle_lock);
        runloom_cond_signal(&runloom_hubs[i].idle_cond);
        runloom_mutex_unlock(&runloom_hubs[i].idle_lock);
    }
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (i = 0; i < runloom_hub_count; i++) {
            runloom_thread_join(runloom_hubs[i].thread);
        }
        PyEval_RestoreThread(saved);
    }
    /* Now both the rescue threads (joined at runloom_handoff_stop_join above) AND
     * the hubs are joined, so no thread can hold runloom_hub_tstate_lock across a
     * startup stop-the-world: safe to delete the rescue tstates here, attached.
     * (Deferred from runloom_handoff_stop_join to dodge the STW deadlock -- see
     * the note there.) */
    runloom_handoff_delete_tstates();
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
        /* A hub that ran deletes its OWN tstate on its own thread at hub_main
         * exit (it was gilstate-bound to that thread; a cross-thread delete from
         * here corrupts the main thread's gilstate binding -- see the comment in
         * mn_sched_hub_main.c.inc), publishing tstate = NULL, so this is normally
         * NULL.  Non-NULL only for a hub that allocated a tstate but never
         * reached its self-delete (a crash/abort path); clear it here as a
         * best-effort fallback.  Hubs are joined by now, so this is unraced. */
        PyThreadState *hts = runloom_hubs[i].tstate;
        if (hts != NULL) {
            /* trace conformance: deleter is the MAIN thread (-1).  Normally
             * unreached (hubs self-delete); fires under the negative-control
             * RUNLOOM_GILSTATE_DELETE_ON_MAIN, the pre-c28e5ca bug path. */
            runloom_gilstate_trace("Delete", i, -1);
            PyThreadState_Clear(hts);
            PyThreadState_Delete(hts);
        }
        runloom_mutex_destroy(&runloom_hubs[i].sub_lock);
        runloom_mutex_destroy(&runloom_hubs[i].idle_lock);
        runloom_cond_destroy(&runloom_hubs[i].idle_cond);
        /* Release the g-refs held by this hub's still-pending timer entries
         * (future-deadline timed parks that never fired) so they don't leak,
         * then free the timer-heap buffer (otherwise leaked -- it was never
         * freed). */
        runloom_sched_release_timers(&runloom_hubs[i].sched);
        PyMem_Free(runloom_hubs[i].sched.timer_heap);
        runloom_hubs[i].sched.timer_heap = NULL;
        runloom_hubs[i].sched.timer_cap = 0;
        PyMem_Free(runloom_hubs[i].stage_head);
        runloom_hubs[i].stage_head = NULL;
        PyMem_Free(runloom_hubs[i].stage_tail);
        runloom_hubs[i].stage_tail = NULL;
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
    /* Sysmon is joined -> no more auto-cap ticks; forget the watermark so the next
     * session (or a sysmon-less context) falls back to the static default. */
    runloom_stack_autocap_reset();
    /* (Session generation was bumped at the TOP of fini -- see the note there.) */
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
    runloom_coro_prewarm_reset_after_fork();   /* daemon thread didn't survive */
    runloom_stack_autocap_reset();             /* sysmon gone -> forget the watermark */
    /* The inherited hub array is abandoned (leaked); bump the generation so a
     * handle carried across the fork can't route a wake into it.  See the bump
     * in runloom_mn_fini for the rationale. */
    __atomic_add_fetch(&runloom_mn_gen, 1, __ATOMIC_RELEASE);
}

/* EXPERIMENT (docs/dev/HUB_SCALING.md, A1a): pair-affinity placement.  When
 * RUNLOOM_PAIR_AFFINITY is set, fiber index i is placed on hub (i>>1)%H instead
 * of i%H, so an adjacent (2k, 2k+1) goroutine pair co-locates on ONE hub.  This
 * is the probe for "is the profile's _Py_DecRefShared cost cross-hub object
 * sharing?": co-locating a producer/consumer pair keeps the handoff object
 * brc-local (no shared-refcount path).  Forced OFF under RUNLOOM_MN_SEED, whose
 * controlled-replay barrier relies on the deterministic spawn_counter%H map. */
static int runloom_pair_affinity_mode(void)
{
    static int v = -1;
    if (v < 0) {
        const char *e = getenv("RUNLOOM_PAIR_AFFINITY");
        v = (e && *e && *e != '0') ? 1 : 0;
        if (v && getenv("RUNLOOM_MN_SEED")) v = 0;
    }
    return v;
}

/* Internal core: pick hub, alloc g, set up coro, submit, bump counters.
 * Either callable (Python path) or c_fn+c_arg (C-only path) must be set,
 * not both.  Returns 0 on success, -1 on failure (errno set).  Python
 * error is also set on failure when callable != NULL. */
static int runloom_mn_fiber_core(PyObject *callable, runloom_c_entry_fn c_fn,
                           void *c_arg, size_t stack_size, long py_index)
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
    if (runloom_pair_affinity_mode()) {
        /* stride pairs together: prefer the explicit fiber index when present
         * (go_n indexed / the bulk-path fallback), else the spawn counter. */
        long key = (py_index >= 0) ? py_index : n;
        hub_idx = (int)((key >> 1) % runloom_hub_count);
    } else {
        hub_idx = (int)(n % runloom_hub_count);
    }
    h = &runloom_hubs[hub_idx];
    {   /* Backpressure: admit under the max-fibers limit. */
        int adm = runloom_fiber_admit();
        if (adm == 0) {
            if (callable != NULL)
                PyErr_Format(PyExc_RuntimeError,
                             "runloom: fiber limit (%ld) exceeded",
                             runloom_get_max_fibers());
            errno = EAGAIN;
            return -1;
        }
        g = runloom_g_slab_alloc();
        if (g == NULL) {
            if (adm == 2) runloom_fiber_release();
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
        if (py_index >= 0) {                 /* go_n(indexed=True): call fn(index) */
            g->pass_index = 1;
            g->c_arg = (void *)(intptr_t)py_index;
        }
    } else {
        g->c_entry = c_fn;
        g->c_arg   = c_arg;
        g->advice_key = 0;   /* C-entry fibers aren't profiled */
    }
    g->refcount = 1;
    g->owner = &h->sched;   /* owning hub's sched -- groups fibers in the dump */
    __atomic_store_n(&g->id, runloom_next_goid(), __ATOMIC_RELAXED);
    g->park_fd = -1;
    /* Clamp an explicit per-spawn size to [MIN, MAX], exactly like the
     * single-thread go() path (runloom_sched_spawn_sized): "the explicit size
     * wins" means it overrides the autosizer, not that it can exceed the 8 MiB
     * ceiling -- an unclamped wild size fails runloom_coro_new's mmap with
     * MemoryError instead of clamping the way single-thread go() does. */
    if (stack_size > 0) {
        if (stack_size < RUNLOOM_MIN_STACK_SIZE) stack_size = RUNLOOM_MIN_STACK_SIZE;
        if (stack_size > RUNLOOM_MAX_STACK_SIZE) stack_size = RUNLOOM_MAX_STACK_SIZE;
    }
    /* Stack size, in precedence order:
     *  1. an explicit per-spawn override (mn_go(fn, stack_size) > 0) -- a caller
     *     that knows it runs a deep, NON-yielding C burst (cold stdlib import,
     *     readline/terminfo init, a doctest, OpenSSL) asks for a roomy stack up
     *     front; the resume-boundary copy-grow only rescues stacks that deepen
     *     ACROSS yields, not a single-frame burst;
     *  2. otherwise the adaptive autosizer -- this fiber-kind's learned
     *     size (start-large default for an unseen kind), which itself falls
     *     back to the hub default when autosize is off. */
    g->coro = runloom_coro_new(
                stack_size > 0
                    ? stack_size
                    : runloom_advice_size_for(g->advice_key, callable,
                                              (size_t)h->sched.stack_size),
                runloom_g_entry, g);
    if (g->coro == NULL) {
        if (callable != NULL) {
            Py_DECREF(callable);
            g->callable = NULL;             /* don't leave a dangling ref on the slab */
            PyErr_NoMemory();
        }
        if (g->limit_counted) {             /* release the admission slot we took */
            runloom_fiber_release();
            g->limit_counted = 0;
        }
        runloom_g_slab_free(g);             /* return to slab (handles greg + free) */
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
            g->coro = NULL;
            if (callable != NULL) {
                Py_DECREF(callable);
                g->callable = NULL;
                PyErr_NoMemory();
            }
            if (g->limit_counted) {         /* release the admission slot */
                runloom_fiber_release();
                g->limit_counted = 0;
            }
            /* slab_free, NOT PyMem_Free: the g came from runloom_g_slab_alloc
             * whose cold path is libc calloc -- PyMem_Free here was an allocator
             * mismatch (heap corruption risk), and it also leaked the limit. */
            runloom_g_slab_free(g);
            errno = ENOMEM;
            return -1;
        }
    }
    runloom_mn_hub_submit(h, g);
    runloom_mn_pending_inc(h);
    return 0;
}

int runloom_mn_fiber_c(runloom_c_entry_fn fn, void *arg)
{
    if (fn == NULL) { errno = EINVAL; return -1; }
    return runloom_mn_fiber_core(NULL, fn, arg, 0, -1);
}

PyObject *runloom_mn_fiber(PyObject *callable, size_t stack_size)
{
    if (runloom_mn_fiber_core(callable, NULL, NULL, stack_size, -1) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Bulk-spawn: create `n` fibers, ALL running the same Python `callable`,
 * by looping the spawn core in C.  This removes the n Python->C call dispatches
 * + per-call PyArg parsing that a Python `for i: mn_go(fn)` loop pays -- the
 * fibers still run the Python callable (each gets its own g + tstate slot
 * and invokes `callable()` when scheduled).  Returns 0 on success, or -1 with a
 * Python error set if a spawn fails partway (the already-created fibers are
 * live and will run; the caller decides whether to treat a partial as fatal).
 * Must be called from inside the M:N runtime (root or a fiber). */
/* Read RUNLOOM_GON_BULK once: 1 = go_n uses the bulk-arena fast path. */
static int runloom_fibern_bulk_enabled(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_GON_BULK");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* ---- go_n bulk-arena batch lifecycle ----
 * One descriptor per go_n() call.  Owns the two contiguous arenas (g + coro)
 * and the reserved stack block; `live` starts at N and is decremented by each
 * fiber's final decref.  The decrement that reaches 0 is the last finisher,
 * which reclaims everything wholesale -- the safe answer to "you can't free a
 * slice of a calloc/mmap individually".  No per-slot freelist yet: a batch is
 * reclaimed all-or-nothing, which is exact for go_n's "spawn N, they all run"
 * shape; partial slot reuse is a later refinement. */
struct runloom_fibern_batch {
    void  *garena;       /* calloc'd g-struct arena   (one free) */
    void  *carena;       /* calloc'd coro-struct arena (one free) */
    size_t start_slot;   /* first stack-arena slot of this batch's block */
    long   nslots;       /* slots in the block (== N) -- for MADV + slot return */
    long   live;         /* fibers not yet finished; atomic */
};

void runloom_fibern_batch_finish_one(struct runloom_fibern_batch *b)
{
    if (b == NULL) return;
    if (__atomic_sub_fetch(&b->live, 1, __ATOMIC_ACQ_REL) == 0) {
        /* Last finisher: tear the whole batch down.  No other thread can touch
         * these arenas now -- every g that lived in them has completed.  The
         * stack block is MADV'd back AND its slots returned for reuse. */
        free(b->garena);
        free(b->carena);
        runloom_coro_arena_release(b->start_slot, b->nslots);
        free(b);
    }
}

/* Bulk-arena spawn (the "ultra-fast alloc path"): allocate the N g-structs and
 * N coro-structs as TWO contiguous calloc'd arenas (one allocation each, no
 * per-g malloc), carve each stack from the stack arena (lock-free), build each
 * fiber with a straight line of stores (the calloc-zeroed arena IS the
 * template; only the few non-zero fields are written) + a placement coro init,
 * then SPLICE each hub's whole batch onto its sub-list under ONE lock and bump
 * pending once.  Zero function-call depth in the hot loop, no per-g lock, no
 * per-g mmap.  PROTOTYPE: arena g/coro/stacks are never individually freed
 * (the free path is not yet taught about arena memory), so this is only valid
 * for workloads whose fibers do NOT complete during the measured window
 * (e.g. long-parked workers) -- guarded by RUNLOOM_GON_BULK so the default
 * go_n (loop) stays the safe path. */
static int runloom_mn_fiber_n_bulk(PyObject *callable, long n, size_t stack_size,
                                int indexed)
{
    int H = runloom_hub_count;
    size_t gsz = sizeof(runloom_g_t);
    size_t csz = runloom_coro_struct_size();
    size_t hub_stack;
    int use_grunq = runloom_use_global_runq();
    char *garena, *carena;
    runloom_g_t *heads[64], *tails[64];
    long counts[64];
    int hub;
    long i;
    long long goid_base;
    struct runloom_fibern_batch *batch = NULL;       /* arena lifecycle, real path only */
    size_t start_slot = 0;                         /* this batch's stack-arena block start */

    if (H > 64) return -1;                       /* fixed per-hub list cap */
    garena = (char *)calloc((size_t)n, gsz);
    carena = (char *)calloc((size_t)n, csz);
    if (garena == NULL || carena == NULL) { free(garena); free(carena);
                                            PyErr_NoMemory(); return -1; }
    hub_stack = (stack_size > 0) ? stack_size
                                 : (size_t)runloom_hubs[0].sched.stack_size;

    goid_base = runloom_next_goid_block(n);       /* reserve N ids in one atomic */
    for (hub = 0; hub < H; hub++) { heads[hub] = tails[hub] = NULL; counts[hub] = 0; }

    /* TEMPLATE: bake the common g fields ONCE; the loop memcpy's this whole
     * struct per slot, then sets the few per-g fields (owner, id, link).  This
     * is the "one buffer, memcpy in a loop" form.  RUNLOOM_GON_NOFIELDS=1 skips
     * the g build entirely (isolates the coro/asm/fault cost); NOSUBMIT=1 builds
     * but never publishes (isolates build from submit + run). */
    {
        static int nofields = -1, nosub = -1;
        const char *e;
        runloom_g_t tmpl;
        if (nofields < 0) { e = getenv("RUNLOOM_GON_NOFIELDS"); nofields = (e && *e!='0' && *e)?1:0; }
        if (nosub < 0)    { e = getenv("RUNLOOM_GON_NOSUBMIT"); nosub    = (e && *e!='0' && *e)?1:0; }

        /* Real (submitting) path: allocate the batch descriptor so each g can
         * carry an arena-ownership marker + back-pointer.  Skipped for the
         * nofields/nosub benchmark gates (they never run, so never tear down). */
        if (!nofields && !nosub) {
            batch = (struct runloom_fibern_batch *)calloc(1, sizeof *batch);
            if (batch == NULL) { free(garena); free(carena); PyErr_NoMemory(); return -1; }
        }

        memset(&tmpl, 0, sizeof tmpl);
        tmpl.callable     = callable;             /* refs batched below */
        /* refcount = spawn ref (+ the default queue ref implied by
         * in_sub_queue=1, which hub_main drops when it consumes the entry --
         * mirrors the runloom_g_incref in hub_submit).  Per-g-tstate mode uses
         * the global-runq from_runq ref instead, so it needs only the spawn ref. */
        tmpl.refcount     = runloom_get_per_g_tstate_mode() ? 1 : 2;
        tmpl.park_fd      = -1;
        tmpl.in_sub_queue = 1;
        tmpl.state        = (unsigned char)RUNLOOM_GST_SUBMITTED;
        tmpl.arena        = (batch != NULL);      /* arena-owned: skip individual free */
        tmpl.batch        = batch;                /* final decref -> batch_finish_one */
        tmpl.pass_index   = (unsigned char)(indexed != 0);  /* go_n(indexed=True): fn(i) */
        if (use_grunq) tmpl.wake_state = RUNLOOM_WS_RUNNING;

        if (!nofields) {
            int pair_aff = runloom_pair_affinity_mode();
            for (i = 0; i < n; i++) {
                runloom_g_t *g = (runloom_g_t *)(garena + (size_t)i * gsz);
                hub = pair_aff ? (int)((i >> 1) % H) : (int)(i % H);
                memcpy(g, &tmpl, gsz);            /* <-- template memcpy */
                g->owner = &runloom_hubs[hub].sched;
                g->id    = goid_base + (long long)i;
                g->c_arg = (void *)(intptr_t)i;   /* go_n index (read iff pass_index) */
                g->next  = NULL;
                if (tails[hub] != NULL) tails[hub]->next = g; else heads[hub] = g;
                tails[hub] = g;
                counts[hub]++;
            }
        }

        /* Fill the coro arena + set each g->coro.  MUST run after the memcpy
         * (which would otherwise clobber g->coro).  On arena failure, fall back
         * to the per-g path. */
        if (runloom_coro_bulk_init(carena, csz, garena, gsz,
                                   offsetof(runloom_g_t, coro),
                                   hub_stack, n, runloom_g_entry,
                                   &start_slot) != 0) {
            free(garena); free(carena); free(batch);
            for (i = 0; i < n; i++)
                if (runloom_mn_fiber_core(callable, NULL, NULL, stack_size,
                                       indexed ? i : -1) < 0) return -1;
            return 0;
        }

        if (nofields || nosub) { free(garena); free(carena); free(batch); return 0; }
    }

    /* Hand the arenas + reserved stack block to the batch; arm its live count.
     * From here, each fiber's final decref calls runloom_fibern_batch_finish_one,
     * and the Nth reclaims all three allocations. */
    batch->garena     = garena;
    batch->carena     = carena;
    batch->start_slot = start_slot;
    batch->nslots     = n;
    __atomic_store_n(&batch->live, n, __ATOMIC_RELEASE);

    /* ===== bookkeeping, LAST ===== */
    /* All N callable references taken here, in one place (the new gs hold them). */
    {
        static int norefcnt = -1;
        if (norefcnt < 0) { const char *e = getenv("RUNLOOM_GON_NOREFCNT");
                            norefcnt = (e && *e != '0' && *e) ? 1 : 0; }
        if (!norefcnt) for (i = 0; i < n; i++) Py_INCREF(callable);
    }

    /* Splice each hub's whole batch onto its sub-list under ONE lock. */
    for (hub = 0; hub < H; hub++) {
        runloom_hub_t *h;
        if (heads[hub] == NULL) continue;
        h = &runloom_hubs[hub];
        RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
        if (h->sub_tail != NULL) h->sub_tail->next = heads[hub];
        else /* RELEASE: publishes to the census ACQUIRE read (Finding D #4) */
            __atomic_store_n(&h->sub_head, heads[hub], __ATOMIC_RELEASE);
        h->sub_tail = tails[hub];
        RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
        __atomic_add_fetch(&h->pending, counts[hub], __ATOMIC_ACQ_REL);
        /* GLOBAL pending too: mn_run polls runloom_mn_pending_global, not the
         * per-hub fields.  The per-g spawn path does this via runloom_mn_pending_inc;
         * the bulk path must add its whole batch here or mn_run sees 0 and exits
         * before the fibers run.  Each g's completion (runloom_mn_pending_complete)
         * decrements it, so the count balances. */
        __atomic_add_fetch(&runloom_mn_pending_global, counts[hub], __ATOMIC_ACQ_REL);
        __atomic_add_fetch(&h->sub_gen, 1, __ATOMIC_RELEASE);
        /* Wake the target hub in WHATEVER wait mode it is in -- this MUST mirror
         * runloom_mn_hub_submit's full routing, not just the idle_cond signal.  A
         * bulk batch landing on a hub blocked in its io_uring ring (loop backend)
         * or in netpoll_pump/epoll_wait (any fiber parked on I/O anywhere makes an
         * idle hub pump instead of waiting on idle_cond) would otherwise sit un-run
         * until that wait's idle_ns/pump timeout expires.  (audit lost-wake #5.) */
        if (__atomic_load_n(&h->ring_waiting, __ATOMIC_RELAXED)) {
            __atomic_thread_fence(__ATOMIC_SEQ_CST);   /* Dekker vs the hub's re-check */
            if (__atomic_load_n(&h->ring_waiting, __ATOMIC_ACQUIRE))
                runloom_iouring_loop_wake(h->loop_wake_fd);
        }
        if (__atomic_load_n(&h->idle_waiting, __ATOMIC_ACQUIRE)) {
            RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
            runloom_cond_signal(&h->idle_cond);
            RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        }
        runloom_netpoll_wake_pump((void *)h);   /* pump/epoll-blocked hub */
    }
    return 0;
}

int runloom_mn_fiber_n(PyObject *callable, long n, size_t stack_size, int indexed)
{
    long i;
    if (runloom_hubs == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "runloom_mn_init() must be called first");
        return -1;
    }
    if (n > 0 && runloom_fibern_bulk_enabled()) {
        return runloom_mn_fiber_n_bulk(callable, n, stack_size, indexed);
    }
    for (i = 0; i < n; i++) {
        if (runloom_mn_fiber_core(callable, NULL, NULL, stack_size,
                               indexed ? i : -1) < 0) {
            return -1;
        }
    }
    return 0;
}

/* ---- M:N deadlock census (Go's checkdead analogue) -------------------------
 *
 * runloom_mn_run() polls runloom_mn_pending_global and returns when it reaches
 * 0 (every fiber finished).  If the count instead stays > 0 while the WHOLE
 * system is quiescent -- every hub idle, no runnable / stealable work anywhere,
 * no sleeper or timer due, and nothing in flight on netpoll / blockpool /
 * io_uring / a foreign park -- then the remaining fibers are blocked on a
 * channel / lock / await with no event that can ever wake them.  That is Go's
 * "all goroutines are asleep - deadlock!".  Without this census mn_run would
 * spin the 1 ms poll forever: a silent hang with no diagnostic.  This is the
 * M:N analogue of the single-thread detector at drain exit (which self-disables
 * once hubs exist -- runloom_sched_drain.c.inc). */

/* True if any source could still wake a parked fiber.  Reads only process-
 * global + per-hub atomics (no GIL / tstate), exactly like the sysmon watchdog.
 * A racy read is fine: the verdict requires this to stay false for many
 * consecutive polls, and a real wake (microseconds) resets the streak. */
static int runloom_mn_has_wakeable_work(void)
{
    int i;
    /* Cross-thread / external wake sources: an fd that may become readable, an
     * executor or io_uring completion, or a foreign (non-fiber) thread parked
     * on a runloom primitive that another thread will release.  Any of these
     * can wake a parked fiber -> not a deadlock (Go's checkdead likewise
     * excludes netpollWaiters and syscall/cgo threads). */
    if (runloom_netpoll_parked_count()  != 0) return 1;
    if (runloom_blockpool_inflight()    != 0) return 1;
    if (runloom_iouring_inflight()      != 0) return 1;
    if (runloom_foreign_park_inflight() != 0) return 1;
    if (runloom_use_global_runq() &&
        __atomic_load_n(&runloom_global_runq_len, __ATOMIC_ACQUIRE) != 0) return 1;
    for (i = 0; i < runloom_hub_count; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        /* Mid-resume: the hub is running a fiber right now. */
        if (__atomic_load_n(&h->resume_start_ns, __ATOMIC_RELAXED) != 0) return 1;
        /* Runnable work queued on this hub: ready ring / steal deque / the
         * mn_go + wake_g submission list. */
        if (!runloom_sched_ready_empty(&h->sched))                  return 1;
        if (runloom_cldeque_size(&h->deque) != 0)                   return 1;
        /* Lock-free ACQUIRE read of the submission list head, paired with RELEASE
         * stores at the 6 sub_head WRITE sites (mn_api / hub_main / hub_resume /
         * init): a publish (store g) is RELEASE so this read sees a non-NULL head
         * with g's fields visible; a clear (store NULL) is RELAXED.  Closes the
         * tools/lifefuzz TSan race (hub_main:273); tools/README Finding D #4. */
        if (__atomic_load_n(&h->sub_head, __ATOMIC_ACQUIRE) != NULL) return 1;
        /* A sleeper / timer that will fire and wake a fiber.  RELAXED-atomic: the
         * owning hub writes these non-atomically (sleep_push/pop, timer_push/pop),
         * so this cross-thread read is the lone plain access otherwise -- match the
         * atomic siblings above (the ready_head/tail fix in runloom_sched_ready_empty). */
        if (__atomic_load_n(&h->sched.sleep_size, __ATOMIC_RELAXED) != 0) return 1;
        if (__atomic_load_n(&h->sched.timer_size, __ATOMIC_RELAXED) != 0) return 1;
    }
    return 0;
}

/* For the mn_run progress-stall watchdog: TRUE iff some hub has a QUEUED runnable
 * fiber (ready ring / steal deque / submission list / global runq) AND no hub is
 * currently resuming one.  That is the stranded-runnable signature -- work that
 * should be executing but nothing is running it and nothing is completing.  It
 * deliberately EXCLUDES fibers parked on I/O / timers / foreign waits (a
 * legitimately idle server must NOT be kicked) and the case where a hub is
 * mid-resume (a long fiber is genuinely executing, not stranded). */
static int runloom_mn_stalled_with_runnable(void)
{
    int i, runnable = 0;
    for (i = 0; i < runloom_hub_count; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        if (__atomic_load_n(&h->resume_start_ns, __ATOMIC_RELAXED) != 0)
            return 0;                                   /* a fiber is executing */
        if (!runnable &&
            (!runloom_sched_ready_empty(&h->sched) ||
             runloom_cldeque_size(&h->deque) != 0 ||
             __atomic_load_n(&h->sub_head, __ATOMIC_ACQUIRE) != NULL))
            runnable = 1;
    }
    if (!runnable && runloom_use_global_runq())
        runnable = (__atomic_load_n(&runloom_global_runq_len, __ATOMIC_ACQUIRE) != 0);
    return runnable;
}

/* Wake every hub out of WHATEVER wait it is in so it re-polls its submission list
 * + neighbours.  A recovery nudge for a fiber made runnable whose hub missed the
 * wakeup; harmless when there is nothing to find (the hub re-checks and re-parks).
 * It MUST mirror the full wake routing in runloom_mn_hub_submit, because a hub can
 * be blocked three different ways and the idle condvar reaches only one: the
 * iouring-loop backend blocks in its RING (woken by the per-hub loop_wake_fd
 * eventfd), a netpoll hub blocks in epoll_wait / the pump (woken by the pump
 * eventfd), and only a bare idle hub waits on idle_cond.  The old version signalled
 * idle_cond ONLY -- so a hub stranded in its ring (RUNLOOM_IOURING_LOOP) could not
 * be recovered at all, and a runnable fiber pinned to it hung forever (the
 * cov_workload --hubs 4 loop-backend hang; the deadlock census never even fired
 * because the I/O-parked fibers keep has_wakeable_work() true). */
static void runloom_mn_kick_all_hubs(void)
{
    int i;
    for (i = 0; i < runloom_hub_count; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        if (h->loop_wake_fd >= 0)
            runloom_iouring_loop_wake(h->loop_wake_fd);   /* ring-blocked hub */
        RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        runloom_cond_signal(&h->idle_cond);               /* idle-condvar hub */
        RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        runloom_netpoll_wake_pump((void *)h);             /* pump / epoll hub */
    }
}

/* RUNLOOM_DEADLOCK_MS: consecutive-quiescent-millisecond budget mn_run requires
 * before declaring a deadlock (0 disables the census).  Generous default so a
 * brief all-hubs-idle window -- e.g. while a foreign thread computes a value it
 * will send into a channel -- does not trip it; a genuine deadlock stays
 * quiescent forever.  Read once. */
static long runloom_mn_deadlock_ms(void)
{
    static long v = -1;
    long cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_DEADLOCK_MS");
        cur = (e != NULL && *e != '\0') ? atol(e) : 200;
        if (cur < 0) cur = 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* RUNLOOM_STALL_KICK_MS: consecutive no-progress 1ms polls mn_run tolerates before
 * kicking every hub awake as a liveness backstop (0 disables).  Default 4: a busy
 * run's pending changes each poll so it never triggers; a genuine stranded-runnable
 * stall recovers in a few ms instead of hanging.  Read once. */
static long runloom_mn_stall_kick_polls(void)
{
    static long v = -1;
    long cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_STALL_KICK_MS");
        cur = (e != NULL && *e != '\0') ? atol(e) : 4;
        if (cur < 0) cur = 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
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
    {
        long thresh   = runloom_mn_deadlock_ms();   /* quiescent-ms budget; 0 = off */
        long quiet    = 0;                          /* consecutive quiescent polls */
        int  kicked   = 0;                          /* lost-wakeup re-kick fired? */
        int  reported = 0;                          /* diagnostic already dumped? */
        int  raise    = 0;                          /* RUNLOOM_DEADLOCK=raise verdict */
        int  sig_raised = 0;                        /* a Python signal handler raised */
        unsigned sigpoll = 0;                       /* throttle the signal service */
        long last_total  = -1;                      /* stall watchdog: last pending seen */
        long stall       = 0;                       /* consecutive no-progress polls */
        long stall_kick  = runloom_mn_stall_kick_polls();  /* flat polls before kick; 0=off */
        for (;;) {
            /* Read the GLOBAL pending counter, not the per-hub fields.
             * The per-hub fields race during work-stealing (source dec
             * before dest inc); the global counter is only touched on
             * spawn + completion, so it's monotonic across the lifecycle
             * of each g and reliably reaches 0 iff all gs are done. */
            long total = __atomic_load_n(&runloom_mn_pending_global,
                                         __ATOMIC_ACQUIRE);
            if (total == 0) break;

            /* Progress-stall liveness backstop.  If pending has not moved for
             * stall_kick consecutive 1ms polls WHILE a queued runnable fiber sits
             * un-run and no hub is resuming, that fiber is stranded on a hub that
             * missed its wake -- kick EVERY hub in EVERY wait mode to force a
             * re-drain.  Independent of the deadlock census below (which only fires
             * when has_wakeable_work() is FALSE): a stranded-runnable hang keeps
             * has_wakeable_work() TRUE via the I/O-parked fibers, so the census
             * never reaches its own kick.  runloom_mn_stalled_with_runnable()
             * excludes legitimate idle (all fibers parked on I/O) and a running
             * long fiber, so a correctly-idle server is never kicked. */
            if (total == last_total && stall_kick > 0 &&
                runloom_mn_stalled_with_runnable()) {
                if (++stall >= stall_kick) {
                    runloom_mn_kick_all_hubs();
                    stall = 0;
                }
            } else {
                last_total = total;
                stall = 0;
            }

            if (thresh > 0 && runloom_deadlock_mode() != 0 &&
                !runloom_mn_has_wakeable_work()) {
                quiet++;
                /* Partway through the stall, re-kick the idle hubs once: if a
                 * fiber was made runnable but its hub missed the wakeup, this
                 * recovers it and the next poll resets the streak. */
                if (!kicked && quiet == thresh / 4 + 1) {
                    runloom_mn_kick_all_hubs();
                    kicked = 1;
                }
                if (quiet >= thresh) {
                    /* Stable quiescence with live fibers and no wake source,
                     * even after a re-kick: deadlock (or, if none are parked on
                     * a channel / park_safe, a lost wakeup / accounting leak). */
                    long ndead = runloom_count_deadlockable_fibers(NULL);
                    if (!reported) {
                        if (ndead > 0)
                            fprintf(stderr,
                                "\nrunloom: DEADLOCK -- %ld fiber(s) blocked on a "
                                "channel/lock/await with every hub idle and no I/O, "
                                "timer, or runnable work left to wake them:\n", ndead);
                        else
                            fprintf(stderr,
                                "\nrunloom: STALL -- %ld live fiber(s) but the M:N "
                                "scheduler is quiescent with no wake source "
                                "(possible lost wakeup):\n", total);
                        fflush(stderr);
                        runloom_dump_fibers_fd(2);
                        reported = 1;
                    }
                    if (runloom_deadlock_mode() == 2) { raise = 1; break; }
                    /* warn (default): do NOT abandon the fibers -- keep polling
                     * so a correct program whose quiescence was a false alarm
                     * (e.g. a slow foreign feeder) still resumes.  The
                     * diagnostic has been surfaced; a real deadlock now hangs
                     * VISIBLY instead of silently. */
                }
            } else {
                quiet = 0; kicked = 0; reported = 0;
            }
            runloom_sleep_ns(1000000LL);   /* 1 ms poll */
            /* Service Python signal handlers on the MAIN thread (~every 16 ms).
             * On free-threaded 3.13t ONLY the main thread runs Python signal
             * handlers, and mn_run otherwise polls with the main tstate detached
             * (PyEval_SaveThread above) -- so a SIGINT/SIGALRM during an M:N run
             * was deferred until every fiber finished, i.e. Ctrl-C could not
             * interrupt a server run (audit finding B5).  Briefly re-attach and
             * run pending handlers; if one raised, carry it out of mn_run.  The
             * hub fibers' wait_fd parkers are owned by the HUB scheds, not this
             * main sched, so runloom_netpoll_signal_wake (keyed on the caller's
             * sched) finds none eligible from here -- the documented
             * no-eligible-parker fallback is exactly "carry it out of run()".
             * The still-running hubs are stopped by the caller's mn_fini, the
             * same as the RUNLOOM_DEADLOCK=raise early-return path below. */
            if ((++sigpoll & 0xfu) == 0) {
                PyEval_RestoreThread(saved);
                if (PyErr_CheckSignals() < 0) { sig_raised = 1; break; }
                saved = PyEval_SaveThread();
            }
        }
        /* Silence the unused-variable warning that the per-hub loop used
         * to consume. */
        (void)i;
        /* sig_raised broke the loop with the main tstate ALREADY attached and the
         * handler's exception set -- carry it straight out (don't double-attach). */
        if (sig_raised) return -1;
        PyEval_RestoreThread(saved);
        if (raise) {
            long ndead = runloom_count_deadlockable_fibers(NULL);
            PyErr_Format(PyExc_RuntimeError,
                "runloom: deadlock -- %ld fiber(s) blocked on a channel/lock/await "
                "with no way to wake (RUNLOOM_DEADLOCK=raise)",
                ndead > 0 ? ndead : 0);
            return -1;
        }
    }
    for (i = 0; i < runloom_hub_count; i++) {
        total_completed += __atomic_load_n(&runloom_hubs[i].sched.completed,
                                           __ATOMIC_ACQUIRE);
    }
    return total_completed;
}
