/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Drive fresh-fiber rescue for a wedged hub.  The rescue thread uses its
 * OWN PyThreadState (never the hub's) so the hub owner can re-attach its own
 * tstate at any time without racing on _PyThreadState_Attach.
 *
 * BUG #2 FIX.  The old approach called PyEval_RestoreThread(hub_tstate) to
 * adopt the hub's tstate.  Between the "is-DETACHED?" check and that call, the
 * hub's fiber could unblock (Py_END_ALLOW_THREADS) → both the hub owner
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
 *     the hub's fiber unblocks while we are in mid-steal).
 *   • We do NOT drain the sub_list or the sleep heap: both require
 *     deque-push / ready-push (hub-private single-consumer operations) that
 *     would race hub_main.  Sub_list / sleepers drain naturally when the hub
 *     wakes.  The rescue benefit is running fibers already in the deque
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

        /* Steal ONE fresh fiber from the hub's deque.  steal() is safe from
         * any thread; it does NOT require being the deque's owner.  Done WHILE
         * DETACHED so the rescue thread imposes no stop-the-world load between
         * fiber runs (see the attach/detach cycle below). */
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

        /* We have a fresh fiber.  Attach rescue_ts ONLY for this resume.
         *
         * Key: PyEval_RestoreThread blocks if a GC stop-the-world is pending
         * before attaching.  This means:
         *  a) we give the GC a safe window BETWEEN fiber runs (we are
         *     DETACHED while checking/stealing above, so STW can complete then);
         *  b) the actual fiber run proceeds only when the world is running.
         * WITHOUT this detach-reattach cycle, the rescue thread stays ATTACHED
         * through all the C code between fiber runs, and a GC stop-the-world
         * that fires during that window can never complete (the rescue thread
         * never checks the eval-breaker in C code) ->
         * test_mn_iouring_fileread_under_gc deadlock. */
        runloom_delay_inject(RUNLOOM_DLY_HANDOFF_ADOPT);
        RUNLOOM_EVT(RUNLOOM_EVT_HANDOFF_ADOPT, h, h->tstate, 0);
        PyEval_RestoreThread(rescue_ts);   /* attach; blocks during STW */
        runloom_tls_hub = h;
        if (!base_valid) {
            /* Snap the clean base state of rescue_ts once per session.
             * Restored between fiber runs (just below) and at final detach.
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
         * critical STW-safety point: by detaching after every fiber, we
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

/* gilstate trace conformance: a rescue tstate lives on its OWN rescue OS thread
 * (distinct from any hub), but rescue-slot indices overlap hub ids 0,1,2,...
 * Trace rescue tstates in a DISJOINT id band so RunloomGilstate.tla treats each
 * as its own thread (its own Create + self-Delete), not a re-created hub.  Mirror
 * this base in tools/tla_trace_conform.py (RESCUE_BASE).  (The trace is only
 * emitted under RUNLOOM_GILSTATE_TRACE -- a debug/verification env var -- so the
 * 1000 band never collides with a real hub count.) */
#define RUNLOOM_GILSTATE_RESCUE_TRACE_ID(idx) (1000 + (idx))

static RUNLOOM_THREAD_RET runloom_handoff_main(void *arg)
{
    PyThreadState *rescue_ts;
    int my_idx;
    (void)arg;
    runloom_coro_thread_init();

    /* Claim a slot in the rescue_tstates array.  Threads spawn serially so the
     * counter never races in practice; ACQ_REL makes it sound under any order. */
    my_idx = __atomic_fetch_add(&runloom_rescue_thread_next, 1, __ATOMIC_ACQ_REL);

    /* TEST-ONLY fault injection (RUNLOOM_HANDOFF_STARTUP_DELAY_MS): delay this
     * rescue thread's startup PyThreadState_New so its qsbr stop-the-world
     * reliably lands inside mn_fini's teardown window -- the deterministic repro
     * for the teardown STW deadlock.  Zero cost when the env var is unset. */
    {
        const char *dly = getenv("RUNLOOM_HANDOFF_STARTUP_DELAY_MS");
        if (dly != NULL && *dly != '\0') {
            long long dms = atoll(dly);
            if (dms > 0) runloom_sleep_ns(dms * 1000000LL);
        }
    }

    /* Create OWN tstate ON THIS THREAD so biased-refcount ob_tid binds to this
     * OS thread's TID -- same serialised construction hub threads use. */
    RUNLOOM_RLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
    rescue_ts = PyThreadState_New(runloom_mn_interp);
    RUNLOOM_RUNLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
    if (rescue_ts == NULL) {
        runloom_coro_thread_fini();
        RUNLOOM_THREAD_RETURN(NULL);
    }
    /* trace conformance: this rescue tstate was just created on (and gilstate-
     * bound to) THIS rescue thread -- emit its Create so the trace balances the
     * later self-Delete.  Mirrors the hub Create at mn_sched_hub_main.c.inc:159. */
    runloom_gilstate_trace("Create", RUNLOOM_GILSTATE_RESCUE_TRACE_ID(my_idx),
                                     RUNLOOM_GILSTATE_RESCUE_TRACE_ID(my_idx));
    /* Publish tstate for stop_join to delete -- see runloom_rescue_tstates comment.
     * RELEASE: pairs with stop_join's post-join read (which gets a happens-before
     * via the pthread_join). */
    __atomic_store_n((void **)&runloom_rescue_tstates[my_idx], (void *)rescue_ts,
                     __ATOMIC_RELEASE);

    /* Keep tstate DETACHED while idle; attach only during an active rescue
     * (PyEval_RestoreThread is called per fiber in runloom_handoff_rescue). */
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

    /* rescue_ts is DETACHED here (PyEval_SaveThread ends every rescue cycle).
     * Delete it ON THIS (owning) thread -- mirror the hub self-delete
     * (mn_sched_hub_main.c.inc): PyThreadState_New gilstate-bound rescue_ts to
     * THIS thread's gilstate-TSS slot, so it MUST be cleared/deleted HERE (where
     * gilstate_tss == rescue_ts).  Deleting it cross-thread from the MAIN thread
     * (the old runloom_handoff_delete_tstates approach) clears the main thread's
     * gilstate slot instead -> a hard abort under a --with-pydebug FT CPython
     * (pystate.c:345 asserts tstate == this thread's gilstate tstate) and silent
     * main-thread gilstate corruption in release.  The old STW-deadlock worry is
     * gone: runloom_handoff_stop_join detaches the main thread (PyEval_SaveThread)
     * BEFORE pthread_join, so a GC stop-the-world completes while we attach to
     * delete.  NULL the published slot first so the delete_tstates sweep skips it. */
    runloom_coro_thread_fini();
    if (getenv("RUNLOOM_GILSTATE_DELETE_ON_MAIN") == NULL) {
        runloom_gilstate_trace("Delete", RUNLOOM_GILSTATE_RESCUE_TRACE_ID(my_idx),
                                         RUNLOOM_GILSTATE_RESCUE_TRACE_ID(my_idx));   /* deleter = own thread (the fix) */
        __atomic_store_n((void **)&runloom_rescue_tstates[my_idx], (void *)NULL,
                         __ATOMIC_RELEASE);
        PyEval_RestoreThread(rescue_ts);   /* attach; waits out any pending STW */
        PyThreadState_Clear(rescue_ts);
        PyThreadState_DeleteCurrent();     /* clears THIS thread's gilstate slot */
    }
    /* else NEGATIVE CONTROL (RUNLOOM_GILSTATE_DELETE_ON_MAIN): leave rescue_ts
     * published + alive for runloom_handoff_delete_tstates to delete from the
     * MAIN thread, reproducing the pre-fix cross-thread gilstate delete that
     * aborts at pystate.c:345 under --with-pydebug. */
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

/* Free the pool arrays + reap any rescue tstate NOT already self-deleted.  In
 * the DEFAULT path each rescue thread self-deletes its OWN tstate on its own
 * thread (the gilstate-correct deleter; see runloom_handoff_main) and NULLs its
 * slot, so this sweep finds all-NULL slots and only frees the arrays.  A
 * non-NULL slot here means the RUNLOOM_GILSTATE_DELETE_ON_MAIN negative control
 * deliberately deferred the delete to THIS main thread -- reproducing the pre-fix
 * cross-thread gilstate delete (aborts at pystate.c:345 under --with-pydebug).
 * MUST be called on the MAIN thread AFTER both the rescue threads AND the hubs
 * are joined (the deadlock note in runloom_handoff_stop_join): only then can no
 * other thread hold runloom_hub_tstate_lock across a startup stop-the-world.
 * Safe when handoff was disabled / no threads spawned (arrays are NULL). */
static void runloom_handoff_delete_tstates(void)
{
    int k;
    if (runloom_rescue_tstates != NULL) {
        RUNLOOM_RLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
        for (k = 0; k < runloom_handoff_running; k++) {
            PyThreadState *rts = (PyThreadState *)__atomic_load_n(
                                      (void **)&runloom_rescue_tstates[k],
                                      __ATOMIC_ACQUIRE);
            if (rts != NULL) {
                /* Negative-control / straggler path only (see above): a
                 * cross-thread delete from MAIN.  Trace deleter=-1 (main) so the
                 * gilstate TLA trace check flags it, matching the hub control. */
                runloom_gilstate_trace("Delete", RUNLOOM_GILSTATE_RESCUE_TRACE_ID(k), -1);
                PyThreadState_Clear(rts);
                PyThreadState_Delete(rts);
            }
        }
        RUNLOOM_RUNLOCK(&runloom_hub_tstate_lock, RUNLOOM_RANK_HUB_TSTATE);
        free(runloom_rescue_tstates);
        runloom_rescue_tstates = NULL;
    }
    runloom_handoff_running = 0;
    free(runloom_handoff_threads); runloom_handoff_threads = NULL;
    free(runloom_handoff_claim);   runloom_handoff_claim = NULL;
}
