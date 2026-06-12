/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Drive fresh-goroutine rescue for a wedged hub.  The rescue thread uses its
 * OWN PyThreadState (never the hub's) so the hub owner can re-attach its own
 * tstate at any time without racing on _PyThreadState_Attach.
 *
 * BUG #2 FIX.  The old approach called PyEval_RestoreThread(hub_tstate) to
 * adopt the hub's tstate.  Between the "is-DETACHED?" check and that call, the
 * hub's goroutine could unblock (Py_END_ALLOW_THREADS) → both the hub owner
 * and the rescue thread would simultaneously call _PyThreadState_Attach on the
 * SAME tstate.  CPython only ever mutates _status from one owner; two threads
 * racing there corrupts the biased-refcount ob_tid, tstate TLS binding, and
 * the _status field → use-after-free (SIGSEGV/SIGBUS under heavy I/O load).
 *
 * New design:
 *   • rescue_ts is this thread's OWN PyThreadState (created at spawn, TID-bound
 *     to this OS thread's biased-refcount heap — see runloom_handoff_main).
 *   • The hub's hts is NEVER touched; the hub owner can freely re-attach it.
 *   • We steal ONLY from the hub's Chase-Lev deque (steal() is the safe
 *     multi-thread interface; pop() is owner-only and would race hub_main if
 *     the hub's goroutine unblocks while we are in mid-steal).
 *   • We do NOT drain the sub_list or the sleep heap: both require
 *     deque-push / ready-push (hub-private single-consumer operations) that
 *     would race hub_main.  Sub_list / sleepers drain naturally when the hub
 *     wakes.  The rescue benefit is running goroutines already in the deque
 *     (queued before the wedge) while the hub is blocked. */
static void runloom_handoff_rescue(runloom_hub_t *h, PyThreadState *rescue_ts)
{
    runloom_pystate_snap_t rescue_base;
    int base_valid = 0;    /* rescue_base snapped once per session, while attached */
    long drained = 0;

    for (;;) {
        runloom_g_t *g;

        if (__atomic_load_n(&runloom_handoff_stop, __ATOMIC_ACQUIRE))
            return;   /* rescue_ts is DETACHED here; nothing to clean up */

        /* Quality-of-dispatch check.  All reads from hub fields are safe while
         * DETACHED (pure C atomics/reads; no Python C-API).  This is NOT a
         * safety gate -- the BUG #2 race is gone because rescue_ts is the
         * rescue's OWN tstate and the hub's tstate is never touched. */
        {
            long long start = __atomic_load_n(&h->resume_start_ns,
                                              __ATOMIC_RELAXED);
            long long now = runloom_monotonic_ns();
            PyThreadState *hts = (PyThreadState *)__atomic_load_n(
                (void **)&h->tstate, __ATOMIC_ACQUIRE);
            if (hts == NULL || start == 0 ||
                (now - start) <= runloom_sysmon_wedge_ns ||
                runloom_tstate_attach_state(hts) != RUNLOOM_TS_DETACHED) {
                return;
            }
        }

        /* Steal ONE fresh goroutine from the hub's deque.  steal() is safe from
         * any thread; it does NOT require being the deque's owner.  Done WHILE
         * DETACHED so the rescue thread imposes no stop-the-world load between
         * goroutine runs (see the attach/detach cycle below). */
        g = (runloom_g_t *)runloom_cldeque_steal(&h->deque);

        if (g == NULL) {
            if (runloom_handoff_debug && base_valid)
                fprintf(stderr, "[RUNLOOM_HANDOFF] hub %d deque empty "
                                "(drained=%ld)\n", h->id, drained);
            /* Deque empty.  Still wedged? If so, wait briefly and re-check. */
            {
                long long start = __atomic_load_n(&h->resume_start_ns,
                                                  __ATOMIC_RELAXED);
                long long now = runloom_monotonic_ns();
                if (start == 0 || (now - start) <= runloom_sysmon_wedge_ns ||
                    runloom_tstate_attach_state((PyThreadState *)
                        __atomic_load_n((void **)&h->tstate, __ATOMIC_ACQUIRE))
                        != RUNLOOM_TS_DETACHED) {
                    return;
                }
            }
            runloom_sleep_ns(200000LL);
            continue;
        }

        /* We have a fresh goroutine.  Attach rescue_ts ONLY for this resume.
         *
         * Key: PyEval_RestoreThread blocks if a GC stop-the-world is pending
         * before attaching.  This means:
         *  a) we give the GC a safe window BETWEEN goroutine runs (we are
         *     DETACHED while checking/stealing above, so STW can complete then);
         *  b) the actual goroutine run proceeds only when the world is running.
         * WITHOUT this detach-reattach cycle, the rescue thread stays ATTACHED
         * through all the C code between goroutine runs, and a GC stop-the-world
         * that fires during that window can never complete (the rescue thread
         * never checks the eval-breaker in C code) ->
         * test_mn_iouring_fileread_under_gc deadlock. */
        runloom_delay_inject(RUNLOOM_DLY_HANDOFF_ADOPT);
        RUNLOOM_EVT(RUNLOOM_EVT_HANDOFF_ADOPT, h, h->tstate, 0);
        PyEval_RestoreThread(rescue_ts);   /* attach; blocks during STW */
        runloom_tls_hub = h;
        if (!base_valid) {
            /* Snap the clean base state of rescue_ts once per session.
             * Restored between goroutine runs (just below) and at final detach.
             * Goroutine Python state (context, exc, etc.) rides g->snap. */
            runloom_pystate_snap(&rescue_base);
            base_valid = 1;
            if (runloom_handoff_debug)
                fprintf(stderr, "[RUNLOOM_HANDOFF] adopt hub %d (own tstate)\n",
                        h->id);
        }

        runloom_handoff_resume_g(h, g, &rescue_base);
        drained++;

        /* Restore clean rescue state, then DETACH immediately.  This is the
         * critical STW-safety point: by detaching after every goroutine, we
         * guarantee rescue_ts is NOT ATTACHED during the C code between runs,
         * so a concurrent GC stop-the-world can complete.  The next iteration's
         * PyEval_RestoreThread will then block until the STW ends. */
        runloom_pystate_load(&rescue_base);
        runloom_tls_hub = NULL;
        PyEval_SaveThread();
    }
}

/* Rescue M main loop.  One of a pool of standby threads; scans per-hub claim
 * slots for a PENDING wedge, CASes PENDING->OWNED (at most one rescuer per hub),
 * rescues, then OWNED->FREE.  One rescue per pass, then re-scan from the top
 * for fairness.  Creates its own PyThreadState at startup (used during rescues;
 * detached while idle) -- the tstate is TID-bound to this OS thread's
 * biased-refcount heap, so Python objects allocated during rescues are correctly
 * accounted. */
/* index counter: monotonic, rescue threads claim slots 0, 1, 2, ... */
static volatile int runloom_rescue_thread_next = 0;

static RUNLOOM_THREAD_RET runloom_handoff_main(void *arg)
{
    PyThreadState *rescue_ts;
    int my_idx;
    (void)arg;
    runloom_coro_thread_init();

    /* Claim a slot in the rescue_tstates array.  Threads spawn serially so the
     * counter never races in practice; ACQ_REL makes it sound under any order. */
    my_idx = __atomic_fetch_add(&runloom_rescue_thread_next, 1, __ATOMIC_ACQ_REL);

    /* Create OWN tstate ON THIS THREAD so biased-refcount ob_tid binds to this
     * OS thread's TID -- same serialised construction hub threads use. */
    runloom_mutex_lock(&runloom_hub_tstate_lock);
    rescue_ts = PyThreadState_New(runloom_mn_interp);
    runloom_mutex_unlock(&runloom_hub_tstate_lock);
    if (rescue_ts == NULL) {
        runloom_coro_thread_fini();
        RUNLOOM_THREAD_RETURN(NULL);
    }
    /* Publish tstate for stop_join to delete -- see runloom_rescue_tstates comment.
     * RELEASE: pairs with stop_join's post-join read (which gets a happens-before
     * via the pthread_join). */
    __atomic_store_n(&runloom_rescue_tstates[my_idx], rescue_ts, __ATOMIC_RELEASE);

    /* Keep tstate DETACHED while idle; attach only during an active rescue
     * (PyEval_RestoreThread is called per goroutine in runloom_handoff_rescue). */
    while (!__atomic_load_n(&runloom_handoff_stop, __ATOMIC_ACQUIRE)) {
        int i, claimed = 0;
        for (i = 0; i < runloom_hub_count; i++) {
            int pend = RUNLOOM_HANDOFF_PENDING;
            if (__atomic_compare_exchange_n(&runloom_handoff_claim[i], &pend,
                                            RUNLOOM_HANDOFF_OWNED, 0,
                                            __ATOMIC_ACQ_REL,
                                            __ATOMIC_RELAXED)) {
                runloom_handoff_rescue(&runloom_hubs[i], rescue_ts);
                __atomic_store_n(&runloom_handoff_claim[i], RUNLOOM_HANDOFF_FREE,
                                 __ATOMIC_RELEASE);
                claimed = 1;
                break;
            }
        }
        if (!claimed) runloom_sleep_ns(500000LL);
    }

    /* Exit with rescue_ts DETACHED (PyEval_SaveThread at the end of each rescue
     * cycle; never re-attached while idle).  Do NOT call PyThreadState_Delete here:
     * during mn_fini the GC gcer thread may still be running a stop-the-world;
     * PyThreadState_Delete interacts with that bookkeeping and deadlocks when
     * called from the rescue thread while the main thread blocks in pthread_join.
     * Deletion is done by stop_join from the main thread after all rescuers join. */
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
    runloom_rescue_thread_next = 0;
    runloom_handoff_claim = (int *)calloc((size_t)(runloom_hub_count > 0 ?
                                                runloom_hub_count : 1), sizeof(int));
    runloom_handoff_threads = (runloom_thread_t *)calloc(
        (size_t)(runloom_handoff_pool > 0 ? runloom_handoff_pool : 1),
        sizeof(runloom_thread_t));
    /* Per-rescue-thread tstate pointers: set by each rescue thread, deleted by
     * stop_join after all threads are joined.  Zeroed so stop_join can skip
     * threads that failed to create a tstate. */
    runloom_rescue_tstates = (PyThreadState **)calloc(
        (size_t)(runloom_handoff_pool > 0 ? runloom_handoff_pool : 1),
        sizeof(PyThreadState *));
    if (runloom_handoff_claim == NULL || runloom_handoff_threads == NULL
        || runloom_rescue_tstates == NULL) {
        free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
        free(runloom_handoff_threads); runloom_handoff_threads = NULL;
        free(runloom_rescue_tstates);  runloom_rescue_tstates = NULL;
        runloom_handoff_enabled = 0;
        fprintf(stderr, "[RUNLOOM_HANDOFF] alloc failed; stalled-hub recovery "
                        "disabled\n");
        return;
    }
    runloom_handoff_running = 0;
    for (k = 0; k < runloom_handoff_pool; k++) {
        if (runloom_thread_create(&runloom_handoff_threads[runloom_handoff_running],
                               runloom_handoff_main, NULL) == 0) {
            runloom_handoff_running++;
        }
    }
    if (runloom_handoff_running == 0) {
        free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
        free(runloom_handoff_threads); runloom_handoff_threads = NULL;
        free(runloom_rescue_tstates);  runloom_rescue_tstates = NULL;
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
    /* Join the rescue threads with the MAIN THREAD DETACHED -- mirrors the hub
     * join in runloom_mn_fini.  A rescue thread may still be in its STARTUP
     * PyThreadState_New (handoff_main): on free-threaded 3.13t the qsbr-slot
     * reservation does a stop-the-world, which waits for EVERY other attached
     * thread to reach a safe point.  pthread_join is NOT a safe point and leaves
     * the main tstate attached, so joining while attached deadlocks -- the STW
     * waits for the main thread while the main thread waits for the rescue thread
     * to exit (observed: trivial-workload mn_run() returns before a rescue thread
     * finishes starting, then mn_fini joins it mid-STW -> ~80% hang).  Detaching
     * parks the main thread at a safe point so the STW completes and the rescue
     * thread reaches its stop check and exits.  Re-attach before the tstate
     * deletion below (PyThreadState_Clear/Delete run Python decrefs). */
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (k = 0; k < runloom_handoff_running; k++) {
            runloom_thread_join(runloom_handoff_threads[k]);
        }
        PyEval_RestoreThread(saved);
    }
    /* The rescue THREADS are joined, but do NOT delete their tstates here.
     * Deletion acquires runloom_hub_tstate_lock and runs Python decrefs (must be
     * ATTACHED) -- yet a HUB may still be in its OWN startup PyThreadState_New
     * (free-threaded qsbr-slot reservation = a stop-the-world) while HOLDING
     * runloom_hub_tstate_lock (the serialised hub-tstate construction).  An
     * attached main thread blocking on that lock is not at a safe point, so the
     * hub's STW never completes -> deadlock (the ~2% residual after the join was
     * moved off-thread).  Defer deletion to runloom_handoff_delete_tstates(),
     * called from mn_fini AFTER the hubs are joined: by then no hub can hold the
     * lock or be mid-STW.  The pthread_join above gives the happens-before that
     * makes the rescue threads' tstate-pointer stores visible to that deletion. */
}

/* Delete the rescue tstates + free the pool arrays.  MUST be called on the MAIN
 * thread AFTER both the rescue threads AND the hubs are joined (see the deadlock
 * note in runloom_handoff_stop_join): only then can no other thread hold
 * runloom_hub_tstate_lock across a startup stop-the-world.  Safe to call even when
 * handoff was disabled / no threads spawned (arrays are NULL). */
static void runloom_handoff_delete_tstates(void)
{
    int k;
    if (runloom_rescue_tstates != NULL) {
        runloom_mutex_lock(&runloom_hub_tstate_lock);
        for (k = 0; k < runloom_handoff_running; k++) {
            PyThreadState *rts = __atomic_load_n(&runloom_rescue_tstates[k],
                                                  __ATOMIC_ACQUIRE);
            if (rts != NULL) {
                PyThreadState_Clear(rts);
                PyThreadState_Delete(rts);
            }
        }
        runloom_mutex_unlock(&runloom_hub_tstate_lock);
        free(runloom_rescue_tstates);
        runloom_rescue_tstates = NULL;
    }
    runloom_handoff_running = 0;
    free(runloom_handoff_threads); runloom_handoff_threads = NULL;
    free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
}
