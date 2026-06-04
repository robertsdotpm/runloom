/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Drive a single adopt -> drain-to-empty -> handback cycle for the wedged hub,
 * repeating until the wedge clears.  Runs on the rescue OS thread. */
static void runloom_handoff_rescue(runloom_hub_t *h)
{
    runloom_pystate_snap_t rescue_base;
    int attached = 0;
    long drained = 0;

    for (;;) {
        runloom_g_t *g;

        if (__atomic_load_n(&runloom_handoff_stop, __ATOMIC_ACQUIRE)) {
            if (attached) {
                runloom_pystate_load(&rescue_base);
                runloom_tls_hub = NULL;
                PyEval_SaveThread();
            }
            return;
        }

        if (!attached) {
            /* Adopt only a genuinely DETACHED, still-active wedge.  The racy
             * resume_start_ns + state checks keep us off (a) an idle-sleeping
             * owner (resume_start_ns==0 -- it briefly detaches at idle), and
             * (b) an owner actively in the hub loop (state==ATTACHED).  A lost
             * race (owner re-grabs, or a STW sets SUSPENDED, between the check
             * and the attach) makes PyEval_RestoreThread block until it can
             * attach -- bounded and safe; we re-verify the wedge after. */
            long long start = __atomic_load_n(&h->resume_start_ns,
                                              __ATOMIC_RELAXED);
            long long now = runloom_monotonic_ns();
            /* Acquire load: the hub publishes its tstate on its own thread.
             * NULL during that startup window -> fail closed (return). */
            PyThreadState *hts = __atomic_load_n(&h->tstate, __ATOMIC_ACQUIRE);
            if (hts == NULL || start == 0 ||
                (now - start) <= runloom_sysmon_wedge_ns ||
                runloom_tstate_attach_state(hts) != RUNLOOM_TS_DETACHED) {
                return;   /* wedge gone / not a DETACHED block -> nothing to do */
            }
            runloom_delay_inject(RUNLOOM_DLY_HANDOFF_ADOPT);
            RUNLOOM_EVT(RUNLOOM_EVT_HANDOFF_ADOPT, h, hts, 0);
            PyEval_RestoreThread(hts);     /* attach hub_ts to THIS thread */
            runloom_tls_hub = h;
            /* Capture g_block's live slice; every handback restores it so the
             * owner's END_ALLOW_THREADS re-attach finds it intact. */
            runloom_pystate_snap(&rescue_base);
            attached = 1;
            if (runloom_handoff_debug)
                fprintf(stderr, "[RUNLOOM_HANDOFF] adopt hub %d\n", h->id);
        }

        /* ---- one drain pass (mirror hub_main, default-only, NO steal) ---- */
        /* Drain submission list into deque (fresh) / local FIFO (woken). */
        runloom_mutex_lock(&h->sub_lock);
        {
            runloom_g_t *sub = h->sub_head;
            h->sub_head = h->sub_tail = NULL;
            runloom_mutex_unlock(&h->sub_lock);
            while (sub != NULL) {
                runloom_g_t *next = sub->next;
                sub->next = NULL;
                if (sub->snap.valid) {
                    runloom_sched_ready_push(&h->sched, sub);
                } else if (runloom_cldeque_push(&h->deque, sub) != 0) {
                    runloom_sched_ready_push(&h->sched, sub);
                }
                sub = next;
            }
        }
        /* Expired sleepers -> local FIFO (default branch). */
        if (h->sched.sleep_size > 0) {
            double now = runloom_sched_monotonic_seconds();
            while (h->sched.sleep_size > 0 &&
                   runloom_sched_sleep_peek(&h->sched)->wake_at <= now) {
                runloom_sched_ready_push(&h->sched,
                                      runloom_sched_sleep_pop(&h->sched));
            }
        }

        g = runloom_sched_ready_pop(&h->sched);          /* local yielded/woken */
        if (g == NULL) {
            g = (runloom_g_t *)runloom_cldeque_pop(&h->deque);  /* own fresh */
        }

        if (g != NULL) {
            runloom_handoff_resume_g(h, g, &rescue_base);
            drained++;
            continue;                                 /* try for more work */
        }

        if (runloom_handoff_debug)
            fprintf(stderr, "[RUNLOOM_HANDOFF] hub %d drain pass empty "
                            "(drained=%ld so far)\n", h->id, drained);
        /* Runq empty: restore g_block's slice, detach so the owner can reclaim
         * the instant its block ends, then re-verify.  Still DETACHED-wedged ->
         * loop re-adopts (after a short sleep that bounds the added latency for
         * gs woken into the sub_list during the block); otherwise released. */
        runloom_pystate_load(&rescue_base);
        runloom_tls_hub = NULL;
        PyEval_SaveThread();
        attached = 0;
        {
            long long start = __atomic_load_n(&h->resume_start_ns,
                                              __ATOMIC_RELAXED);
            long long now = runloom_monotonic_ns();
            if (start == 0 || (now - start) <= runloom_sysmon_wedge_ns ||
                runloom_tstate_attach_state(
                    __atomic_load_n(&h->tstate, __ATOMIC_ACQUIRE))
                    != RUNLOOM_TS_DETACHED) {
                return;   /* block ended or owner/STW reclaimed -> released */
            }
        }
        runloom_sleep_ns(200000LL);   /* 0.2 ms re-poll for wakes during the block */
    }
}

/* Rescue M main loop.  One of a pool of standby threads; scans the per-hub
 * claim slots for a PENDING wedge, CASes it PENDING->OWNED to take exclusive
 * ownership of that hub's rescue (so two threads never rescue the same hub),
 * rescues it, then stores OWNED->FREE.  At most one rescue per scan pass, then
 * re-scan from the top for fairness across hubs.  Holds no tstate while idle. */
static RUNLOOM_THREAD_RET runloom_handoff_main(void *arg)
{
    (void)arg;
    runloom_coro_thread_init();   /* per-OS-thread coro backend (no-op on POSIX) */
    while (!__atomic_load_n(&runloom_handoff_stop, __ATOMIC_ACQUIRE)) {
        int i, claimed = 0;
        for (i = 0; i < runloom_hub_count; i++) {
            int pend = RUNLOOM_HANDOFF_PENDING;
            if (__atomic_compare_exchange_n(&runloom_handoff_claim[i], &pend,
                                            RUNLOOM_HANDOFF_OWNED, 0,
                                            __ATOMIC_ACQ_REL,
                                            __ATOMIC_RELAXED)) {
                runloom_handoff_rescue(&runloom_hubs[i]);
                __atomic_store_n(&runloom_handoff_claim[i], RUNLOOM_HANDOFF_FREE,
                                 __ATOMIC_RELEASE);
                claimed = 1;
                break;   /* one rescue per pass; re-scan from the top */
            }
        }
        if (!claimed) runloom_sleep_ns(500000LL);   /* 0.5 ms idle poll */
    }
    runloom_coro_thread_fini();
    RUNLOOM_THREAD_RETURN(NULL);
}

/* Spawn the rescue-thread pool (after hubs exist).  Enabled-gated; a partial or
 * total spawn failure is non-fatal -- the scheduler runs fine, just with fewer
 * (or no) parallel rescuers.  Allocates the per-hub claim array first. */
static void runloom_handoff_spawn(void)
{
    int k;
    if (!runloom_handoff_enabled) return;
    runloom_handoff_stop = 0;
    runloom_handoff_claim = (int *)calloc((size_t)(runloom_hub_count > 0 ?
                                                runloom_hub_count : 1), sizeof(int));
    runloom_handoff_threads = (runloom_thread_t *)calloc(
        (size_t)(runloom_handoff_pool > 0 ? runloom_handoff_pool : 1),
        sizeof(runloom_thread_t));
    if (runloom_handoff_claim == NULL || runloom_handoff_threads == NULL) {
        free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
        free(runloom_handoff_threads); runloom_handoff_threads = NULL;
        runloom_handoff_enabled = 0;
        fprintf(stderr, "[RUNLOOM_HANDOFF] alloc failed; stalled-hub recovery "
                        "disabled\n");
        return;
    }
    runloom_handoff_running = 0;
    for (k = 0; k < runloom_handoff_pool; k++) {
        /* Pack successfully-created handles at [0, running) so stop_join joins
         * exactly the threads that started. */
        if (runloom_thread_create(&runloom_handoff_threads[runloom_handoff_running],
                               runloom_handoff_main, NULL) == 0) {
            runloom_handoff_running++;
        }
    }
    if (runloom_handoff_running == 0) {
        free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
        free(runloom_handoff_threads); runloom_handoff_threads = NULL;
        runloom_handoff_enabled = 0;
        fprintf(stderr, "[RUNLOOM_HANDOFF] no rescue threads spawned; "
                        "stalled-hub recovery disabled\n");
    }
}

/* Stop + join the rescue-thread pool.  Called in mn_fini AFTER the watchdog
 * stops (so no new dispatch) but BEFORE the hubs are torn down (so no rescue
 * thread can still hold a tstate that is about to be deleted). */
static void runloom_handoff_stop_join(void)
{
    int k;
    if (runloom_handoff_running <= 0) return;
    __atomic_store_n(&runloom_handoff_stop, 1, __ATOMIC_RELEASE);
    for (k = 0; k < runloom_handoff_running; k++) {
        runloom_thread_join(runloom_handoff_threads[k]);
    }
    runloom_handoff_running = 0;
    free(runloom_handoff_threads); runloom_handoff_threads = NULL;
    free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
}
