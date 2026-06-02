/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Hub thread main loop.  Phase C v2: runs the same snap/load dance
 * as pygo_sched_drain.  Each iteration:
 *   1. Pop a g (local FIFO of yielded gs first, then own deque of
 *      fresh gs, then steal from a neighbour's deque).
 *   2. Save hub's tstate into hub_snap (local var on hub_main's stack).
 *   3. If g has a saved snap, load it; else NULL datastack so g's
 *      first run gets its own root chunk (Phase B initial-run dance).
 *   4. Resume g.  g either runs to completion or yields.
 *   5. Restore hub_snap.  If g is still alive AND did not self-queue
 *      (raw pygo_coro_yield, no scheduler call), push it back to the
 *      local FIFO so it keeps making progress.
 *
 * Idle policy: when all hubs report pending=0 we still poll (the
 * caller may pygo_mn_go more work at any time).  Stop signal comes
 * from pygo_mn_fini setting h->stopping. */
static PYGO_THREAD_RET pygo_hub_main(void *arg)
{
    pygo_hub_t *h = (pygo_hub_t *)arg;
    pygo_pystate_snap_t hub_snap;

    PyEval_RestoreThread(h->tstate);
    /* Per-OS-thread coro-backend setup.  On Windows Fibers this calls
     * ConvertThreadToFiber so SwitchToFiber works on this thread; on
     * POSIX it's a no-op.  Must run BEFORE the first pygo_coro_resume
     * (otherwise SwitchToFiber faults with "not a fiber"). */
    pygo_coro_thread_init();
    pygo_tls_hub = h;

    /* Create the hub's per-thread io_uring ring.  Try
     * SINGLE_ISSUER (Linux 5.18+) + DEFER_TASKRUN (6.1+); the create
     * call downgrades gracefully on older kernels.  With
     * DEFER_TASKRUN the kernel only posts CQEs + fires the eventfd
     * when this thread next calls io_uring_enter(GETEVENTS); the
     * idle path below issues that call before pumping so the
     * deferred work flushes in time. */
    h->iouring_ring = pygo_iouring_ring_create(1 /*defer_taskrun*/);
    if (h->iouring_ring != NULL) {
        h->iouring_eventfd = pygo_iouring_ring_eventfd(h->iouring_ring);
        if (pygo_netpoll_add_iouring_ring(h->iouring_eventfd,
                                          h->iouring_ring) != 0) {
            /* Registration failed (most likely no epoll backend, but
             * this build path is Linux-only so it'd be unusual).
             * Discard the ring; hub-bound iouring calls fall through. */
            pygo_iouring_ring_destroy(h->iouring_ring);
            h->iouring_ring     = NULL;
            h->iouring_eventfd  = -1;
        }
    } else {
        h->iouring_eventfd = -1;
    }

    /* hub_snap is loop-invariant for the same reason sched_snap is in
     * pygo_sched_drain: hub_main runs no Python work between
     * iterations except via pygo_g_decref's tp_dealloc, where we
     * explicitly restore + re-snap.  Hoisting the per-iter snap+load
     * out of the loop is ~10 ns/yield on the M:N hot path. */
    pygo_pystate_snap(&hub_snap);

    /* sched_yield fairness (see the pick block below).  ready_streak counts
     * consecutive ready-ring services; after starve_bound of them we force one
     * deque turn so a busy sched_yield loop can't starve never-run goroutines.
     * Hub-local (this thread only).  starve_bound resolved once from env;
     * default 64, 0 disables (legacy ready-before-deque).  First touch across
     * hubs is a relaxed-atomic lazy-static loaded into the loop-invariant local
     * `sbound` (all store the same value) -- same pattern as sweep_on below.
     * (Previously the read was plain while the store was atomic: a technical
     * C11 data race TSan flags, behaviourally benign but now removed.) */
    unsigned ready_streak = 0;
    static int starve_bound = -1;
    int sbound = __atomic_load_n(&starve_bound, __ATOMIC_RELAXED);
    if (sbound < 0) {
        const char *e = getenv("PYGO_READY_STARVE_BOUND");
        sbound = (e != NULL) ? atoi(e) : 64;
        if (sbound < 0) sbound = 0;
        __atomic_store_n(&starve_bound, sbound, __ATOMIC_RELAXED);
    }

    /* Controlled-replay startup rendezvous: wait until mn_run has placed all
     * initial goroutines before this hub starts scheduling, so the first
     * requester set is complete (no-op unless PYGO_MN_SEED + barrier). */
    pygo_mn_ctrl_wait_armed();

    while (!__atomic_load_n(&h->stopping, __ATOMIC_ACQUIRE)) {
        pygo_g_t *g;
        /* Set when g came from the global run-queue: it then carries a queue
         * ref this iteration must release, and its resume must claim it with the
         * QUEUED->RUNNING CAS.  Gs from the local FIFO / own deque / a neighbour
         * steal are single-owner by construction (already RUNNING) and skip
         * both. */
        int from_runq = 0;
        /* Drain the submission list into the deque first.  Pushing to
         * the deque is owner-only, so we (the hub) move fresh gs from
         * external producers onto our deque before anyone else looks. */
        pygo_mutex_lock(&h->sub_lock);
        {
            pygo_g_t *sub = h->sub_head;
            h->sub_head = h->sub_tail = NULL;
            pygo_mutex_unlock(&h->sub_lock);
            while (sub != NULL) {
                pygo_g_t *next = sub->next;
                sub->next = NULL;
                /* Route by state: fresh (no snap) -> Chase-Lev deque
                 * (stealable by other hubs); woken (snap.valid) ->
                 * local FIFO (hub-pinned, the netpoll-wake path).
                 *
                 * Note: in_sub_queue stays 1 throughout drain ->
                 * ready/deque -> pop -> resume.  hub_main clears it
                 * just before the actual coro resume, gating duplicate
                 * wake_gs against ANY queued state, not just the
                 * sub list. */
                if (sub->snap.valid) {
                    pygo_sched_ready_push(&h->sched, sub);
                } else if (pygo_cldeque_push(&h->deque, sub) != 0) {
                    /* Deque full (>PYGO_CLDEQUE_CAP=4096 fresh gs on this
                     * hub -- e.g. a big spawn burst at low hub count, the
                     * H=2/N>=8K case).  Fall back to the local ready FIFO,
                     * which grows on demand.  These gs run hub-pinned
                     * rather than stealable, but are NEVER dropped: the
                     * previous code ignored the push return and silently
                     * dropped the g, orphaning one whose pending-inc was
                     * already counted -> mn_run hung forever.  The resume
                     * prologue handles a snap-invalid g from either queue
                     * via the first-run install path. */
                    pygo_sched_ready_push(&h->sched, sub);
                }
                sub = next;
            }
        }

        /* Wake any sleepers whose timers have expired and move them
         * onto the local FIFO so the pop below picks them up. */
        if (h->sched.sleep_size > 0) {
            double now = pygo_sched_monotonic_seconds();
            while (h->sched.sleep_size > 0 &&
                   pygo_sched_sleep_peek(&h->sched)->wake_at <= now) {
                pygo_g_t *woke = pygo_sched_sleep_pop(&h->sched);
                if (pygo_use_global_runq()) {
                    /* The timer is a wake, and under a global-runq mode the
                     * sleep release left this g in wake_state PARKED.  Claim it
                     * back to RUNNING through the state machine (it has no other
                     * waker, so the CAS normally wins) and resume it hub-pinned
                     * on the local FIFO.  If the CAS loses, a concurrent waker
                     * (a g sleeping AND wakeable, e.g. a select-with-timeout)
                     * already drove PARKED->QUEUED and enqueued it on the global
                     * runq -- do NOT also push it here, or it would be scheduled
                     * twice. */
                    int pexp = PYGO_WS_PARKED;
                    if (__atomic_compare_exchange_n(&woke->wake_state, &pexp,
                                                    PYGO_WS_RUNNING, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        pygo_sched_ready_push(&h->sched, woke);
                    }
                } else {
                    pygo_sched_ready_push(&h->sched, woke);
                }
            }
        }

        /* Pick the next g.  Normal order: ready ring (yielded/woken -- "hot")
         * before the deque (fresh, never-run gs), which keeps woken I/O
         * goroutines low-latency.  BUT a goroutine busy-looping on sched_yield
         * re-enters the ready ring every time, so pure ready-before-deque lets
         * it starve every fresh g forever (they never get a first run -- the
         * sched_yield-fairness bug).  Bound it: after starve_bound consecutive
         * ready-ring services, force ONE deque turn.  ready_streak resets the
         * moment the ready ring drains naturally, so workloads that aren't
         * monopolizing never trip it; a sustained yielder pays only a 1-in-
         * starve_bound interleave.  All hub-local + single-consumer; the deque
         * is touched via its normal owner-pop, never re-ordered internally. */
        g = NULL;
        if (sbound && ready_streak >= (unsigned)sbound) {
            ready_streak = 0;
            g = (pygo_g_t *)pygo_cldeque_pop(&h->deque);  /* forced fresh turn */
        }
        if (g == NULL) {
            g = pygo_sched_ready_pop(&h->sched);     /* local yielded/woken */
            if (g != NULL) {
                ready_streak++;
            } else {
                g = (pygo_g_t *)pygo_cldeque_pop(&h->deque);  /* own fresh */
                ready_streak = 0;
            }
        }
        if (g == NULL) {
            /* Global run-queue: woken migratable gs (per-g-tstate) that any
             * idle hub may run, so a hub stuck in a blocking C call can't
             * strand them.  Checked before neighbour-steal below so a
             * stalled hub's woken work is recovered promptly.  from_runq tells
             * the resume block to claim the g (QUEUED->RUNNING) and to drop the
             * entry's queue ref when the resume ends. */
            g = pygo_mn_global_runq_pull();
            if (g != NULL) from_runq = 1;
        }
        if (g == NULL) {
            int i;
            for (i = 0; !pygo_mn_ctrl_no_steal() && i < pygo_hub_count; i++) {
                if (i == h->id) continue;
                g = (pygo_g_t *)pygo_cldeque_steal(&pygo_hubs[i].deque);
                if (g != NULL) {
                    /* ACQ_REL pair: mn_run's polling loop reads
                     * hubs[*].pending with ACQUIRE; under
                     * free-threaded Python the RELAXED pair we used
                     * to have let a transient "victim already
                     * decremented, stealer not yet incremented"
                     * state become observable -- total could appear
                     * 0 momentarily even with work in flight, which
                     * is harmless on its own but ALSO meant the
                     * stealer's later `h->pending` increment lacked
                     * a release pairing with reads in other hubs
                     * that might inspect this hub's queue. */
                    pygo_mn_pending_steal(&pygo_hubs[i], h);
                    break;
                }
            }
            if (g == NULL) {
                long total = 0;
                int j;
                PyThreadState *saved;
                int parked;
                long long idle_ns;
                /* No goroutine to run: check in as IDLE for the controlled-
                 * scheduler census round so the barrier-rendezvous grant isn't
                 * blocked waiting on a hub that has nothing to request.  No-op
                 * unless PYGO_MN_SEED + barrier are on.  Returns 1 if a wake
                 * landed on our sub_list since this iteration's drain -> re-loop
                 * and drain it rather than falsely checking in idle (the guard
                 * that closes the sub-segment replay race). */
                if (pygo_mn_ctrl_census_idle(h)) continue;
                for (j = 0; j < pygo_hub_count; j++) {
                    total += __atomic_load_n(&pygo_hubs[j].pending,
                                             __ATOMIC_RELAXED);
                }
                /* Default idle wait (no work anywhere -> longer; some
                 * work elsewhere -> shorter so we re-poll for steal). */
                idle_ns = (total == 0) ? 500000LL : 100000LL;
                /* Cap by the next-due sleeper on THIS hub so we don't
                 * oversleep past a local timer.  (Other hubs' timers
                 * are handled by those hubs.) */
                if (h->sched.sleep_size > 0) {
                    double now = pygo_sched_monotonic_seconds();
                    double gap = pygo_sched_sleep_peek(&h->sched)->wake_at - now;
                    long long gap_ns = (long long)(gap * 1e9);
                    if (gap_ns < 0) gap_ns = 0;
                    if (gap_ns < idle_ns) idle_ns = gap_ns;
                }
                parked = pygo_netpoll_parked_count();
                /* Hub-idle dwell-based stack reclaim (opt-in,
                 * PYGO_STACK_PARK_SWEEP=1; threshold ms via
                 * PYGO_STACK_PARK_SWEEP_MS, default 5).  This hub is idle
                 * and is the sole resumer of its own parkers, so madvising
                 * their long-idle stacks here is race-free (see
                 * pygo_netpoll_sweep_idle).  Rate-limited to ~half the
                 * threshold so the O(parked) walk stays cheap. */
                {
                    static int sweep_on = -1;
                    /* 100 ms default: high enough that active round-trip
                     * parks (ms-scale, even with some queuing) are left
                     * alone, low enough to reclaim genuinely idle
                     * keepalive parks (seconds).  A saturated all-active
                     * bench inflates round-trip dwell, so it still pays
                     * some cost there; the N=1M target (5% active) does
                     * not.  Tunable via PYGO_STACK_PARK_SWEEP_MS. */
                    static long long sweep_thresh_ns = 100000000LL;
                    int on = __atomic_load_n(&sweep_on, __ATOMIC_RELAXED);
                    if (on < 0) {
                        const char *e = getenv("PYGO_STACK_PARK_SWEEP");
                        const char *ms = getenv("PYGO_STACK_PARK_SWEEP_MS");
                        if (ms != NULL) {
                            long long v = atoll(ms);
                            if (v > 0) sweep_thresh_ns = v * 1000000LL;
                        }
                        /* Default ON (2026-05-29): the dwell sweep delivers
                         * -32% idle RSS at no robustly-measurable p99 cost
                         * (the churn cost proved within run-to-run noise),
                         * and the churn throttle (PYGO_SWEEP_MAX_CHURN, see
                         * netpoll.c) degrades it to a no-op on active-churn
                         * workloads as insurance.  PYGO_STACK_PARK_SWEEP=0
                         * disables; =1 (or any non-"0") forces on. */
                        on = (e != NULL && *e == '0') ? 0 : 1;
                        __atomic_store_n(&sweep_on, on, __ATOMIC_RELAXED);
                    }
                    /* Runs in BOTH modes now: under per-g-tstate the sweep's
                     * sole-resumer safety is supplied by the per-g claim
                     * handshake (PARKED->SWEEPING) inside pygo_netpoll_sweep_idle,
                     * so a stolen wake can't resume a g into a stack mid-madvise. */
                    if (on && parked > 0) {
                        double now_s = pygo_sched_monotonic_seconds();
                        double interval_s = (double)sweep_thresh_ns / 2e9;
                        if (now_s - h->last_sweep_s >= interval_s) {
                            pygo_netpoll_sweep_idle(
                                pygo_mn_current_hub_opaque(), sweep_thresh_ns);
                            h->last_sweep_s = now_s;
                        }
                    }
                }
                {
                    /* Also drive the pump when ANY iouring ring (this
                     * hub's, the global, or another hub's) has inflight
                     * ops: the shared netpoll's epoll set holds every
                     * registered iouring eventfd, so any hub's pump call
                     * drains everyone's CQEs.  Without this, a hub that
                     * parks all its gs on iouring would idle-sleep
                     * while completions sit unread.
                     *
                     * GIL handling: pump internally does
                     * Py_BEGIN_ALLOW_THREADS for the epoll_wait syscall,
                     * so it must be called with the GIL HELD (matching
                     * how single-thread pygo_sched_drain calls it).
                     * Wrapping pump in our own PyEval_SaveThread would
                     * drop the GIL twice and crash with
                     * "must be called with GIL held".  pygo_sleep_ns is
                     * a plain nanosleep that doesn't need the GIL, so
                     * release the GIL around THAT alone. */
                    int iouring_total = pygo_iouring_inflight();
                    if (parked > 0 || iouring_total > 0) {
                        long long pump_ns = 1000000LL;
                        if (h->sched.sleep_size > 0 && idle_ns < pump_ns) {
                            pump_ns = idle_ns;
                        }
                        /* DEFER_TASKRUN heartbeat: if this hub's ring
                         * was created with that flag, completions on
                         * its own SQEs are queued in the kernel but
                         * NOT posted to the CQ (or the eventfd) until
                         * THIS thread next calls
                         * io_uring_enter(GETEVENTS).  Trigger that
                         * flush now so the upcoming epoll_wait sees a
                         * fresh eventfd hit if there's work pending.
                         * No-op for non-DEFER rings.  We only do this
                         * on OUR own ring -- under SINGLE_ISSUER each
                         * ring's GETEVENTS must come from its owner. */
                        pygo_iouring_ring_get_events(h->iouring_ring);
                        pygo_netpoll_pump(pump_ns);
                    } else {
                        if (idle_ns <= 0) idle_ns = 1;
                        saved = PyEval_SaveThread();
                        pygo_sleep_ns(idle_ns);
                        PyEval_RestoreThread(saved);
                    }
                }
                (void)saved;
                continue;
            }
        }

        /* Phase B snap dance: load g's tstate slice; resume; if g
         * completed, restore hub's tstate before any Python that
         * might allocate frames.  hub_snap is hoisted out of the loop
         * (see entry comment). */
        {
            int self_queued;

            if (pygo_get_per_g_tstate_mode()) {
                /* ---- per-g-tstate path (PYGO_PER_G_TSTATE) ----
                 * Swap the hub's tstate out, the g's own tstate in, resume,
                 * swap back.  The g's suspended eval frames reference
                 * g->tstate (not this hub's), so resuming on ANY hub is
                 * consistent -- which is exactly what makes a stolen woken g
                 * safe here.  No snap dance (snap no-ops under this mode). */
                PyThreadState *hub_ts;
                int parked_offqueue;
                /* CLAIM the g for this hub.  A from_runq g arrived via an entry
                 * that holds a queue ref and put it in wake_state QUEUED; CAS
                 * QUEUED->RUNNING to take ownership.  This CAS CANNOT lose: the
                 * entry we pulled is the queue's sole reference to this g
                 * (at-most-one-entry) and only its holder runs this CAS -- so
                 * there is no contention and no re-push (the old design's
                 * livelock).  Gs from the deque / local FIFO / a neighbour steal
                 * are already RUNNING by the state-machine invariant (fresh gs
                 * spawn RUNNING; a yield leaves the g RUNNING), so they need no
                 * claim. */
                if (from_runq) {
                    int qexp = PYGO_WS_QUEUED;
                    if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                     PYGO_WS_RUNNING, 0,
                                                     __ATOMIC_ACQ_REL,
                                                     __ATOMIC_ACQUIRE)) {
                        /* Not QUEUED: a queued g is parked-waiting and never
                         * done, so this should not happen.  Stay leak-safe by
                         * dropping the queue ref and skipping. */
                        pygo_g_decref(g);   /* queue ref */
                        continue;
                    }
                }
                __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
                if (g->coro == NULL || g->tstate == NULL ||
                    __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                    /* Dead under our claim (shouldn't happen: a QUEUED g is
                     * parked, not done; a from_runq=0 g is freshly owned).  Stay
                     * leak-safe: a claimed runq g goes back to PARKED and drops
                     * its queue ref. */
                    if (from_runq) {
                        __atomic_store_n(&g->wake_state, PYGO_WS_PARKED,
                                         __ATOMIC_RELEASE);
                        pygo_g_decref(g);   /* queue ref */
                    }
                    continue;
                }
                hub_ts = PyEval_SaveThread();        /* detach hub tstate */
                PyEval_RestoreThread(g->tstate);      /* attach g's own tstate */
                h->sched.current = g;
                pygo_tls_current_g = g;
                pygo_tls_self_queued = 0;
                pygo_tls_parked_offqueue = 0;
                pygo_hub_resume_begin(h, g);
                pygo_coro_resume(g->coro);
                pygo_hub_resume_end(h);
                self_queued = pygo_tls_self_queued;
                parked_offqueue = pygo_tls_parked_offqueue;
                pygo_tls_self_queued = 0;
                pygo_tls_parked_offqueue = 0;
                pygo_tls_current_g = NULL;
                h->sched.current = NULL;
                PyEval_SaveThread();                  /* detach g's tstate */
                PyEval_RestoreThread(hub_ts);          /* reattach hub tstate */
                if (pygo_coro_done(g->coro)) {
                    /* Done: the g did not park this resume (a resume ends in a
                     * park OR completion, never both), so no parker is live and
                     * wake_state is still RUNNING with no other entry.  Force-
                     * unlink any stale parker, then drop both refs -- the slab
                     * re-zeroes wake_state when it reallocs the g. */
                    pygo_netpoll_force_unlink_g_parker(g);
                    pygo_mn_pending_complete(h);
                    pygo_g_decref(g);                  /* scheduler ref */
                    if (from_runq) pygo_g_decref(g);   /* queue ref */
                } else if (parked_offqueue) {
                    /* Parked off-queue (netpoll/chan/sleep): a wake_g will route
                     * the g back here.  End ownership now -- AFTER the tstate
                     * detach above, which is what closes the commit->detach
                     * window: while RUNNING, a wake only set RUNNING_WOKEN and
                     * never enqueued, so no other hub could attach this live
                     * tstate.  CAS RUNNING->PARKED so a later wake enqueues it.
                     * If a wake landed during the window (RUNNING_WOKEN), enqueue
                     * exactly once now instead of parking -- the wake is not
                     * lost and stays a single entry. */
                    int rexp = PYGO_WS_RUNNING;
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    PYGO_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN.  Re-arm a single entry: incref
                         * the new one, drop the old (net zero for from_runq;
                         * first entry for from_runq=0). */
                        __atomic_store_n(&g->wake_state, PYGO_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        pygo_g_incref(g);                  /* new queue ref */
                        pygo_mn_global_runq_push(g);
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield_current (already on the local FIFO) or a
                     * raw pygo_coro_yield: the g stays runnable and hub-pinned,
                     * still owned by this hub, so it ends at RUNNING.  A store is
                     * safe -- a yielding g holds no live parker, so no wake_g can
                     * fire to race it.  yield_current already pushed itself to
                     * the FIFO; a raw yield (self_queued==0) did not, so push it
                     * here. */
                    __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) pygo_g_decref(g);       /* old queue ref */
                    if (!self_queued) pygo_sched_ready_push(&h->sched, g);
                }
                continue;
            }

            /* Steal-woken (or any non-per-g global-runq mode): a from_runq g
             * arrived via a queue entry holding a queue ref, in wake_state
             * QUEUED.  Claim it QUEUED->RUNNING (sole entry holder -> cannot
             * lose).  Fresh / deque / local-FIFO / stolen gs are already
             * RUNNING.  from_runq is only ever set under pygo_use_global_runq(),
             * so this block is inert in the byte-unchanged default. */
            int parked_offqueue = 0;
            if (from_runq) {
                int qexp = PYGO_WS_QUEUED;
                if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                 PYGO_WS_RUNNING, 0,
                                                 __ATOMIC_ACQ_REL,
                                                 __ATOMIC_ACQUIRE)) {
                    /* Not QUEUED: a queued g is parked-waiting, never done, so
                     * this shouldn't happen.  Stay leak-safe: drop the queue ref
                     * and skip. */
                    pygo_g_decref(g);   /* queue ref */
                    continue;
                }
            }

            if (g->snap.valid) {
                pygo_pystate_load(&g->snap);
            } else {
                pygo_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030D0000
                {
                    PyThreadState *ts = PyThreadState_GET();
                    ts->current_frame = NULL;
                }
#endif
            }

            h->sched.current = g;
            pygo_tls_current_g = g;
            pygo_tls_self_queued = 0;
            /* Clear queued flag right before resume so a wake_g that
             * fires during this g's execution (e.g., a netpoll pump
             * processing an event for the parker g is about to link
             * inside wait_fd) can enqueue g via submit. */
            __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
            /* Defensive: pop a stale queue entry?  Two failure modes
             * for the queue under M:N + free-threaded:
             *
             *   coro == NULL: g was already decref'd to 0; pygo_g_decref
             *      already ran the pending_global decrement; just skip.
             *
             *   done == 1, coro != NULL: g_entry set done before the
             *      asm trampoline returned to its original caller hub,
             *      but THIS hub popped a duplicate queue entry for
             *      the same g (stale wake_g raced with completion).
             *      The original hub still owes the decrement -- it
             *      will run completion when its coro_resume returns.
             *      We must NOT decrement here (would double-count) and
             *      must NOT re-resume (would re-run the asm trampoline
             *      against a coro whose user frames already unwound).
             *      Skip without decrementing.
             *
             * In both cases skipping does not lose work: the original
             * processing path owns the decrement. */
            PYGO_G_ASSERT_NOT(g, PYGO_GST_BIT(PYGO_GST_FREED));
            PYGO_EVT(PYGO_EVT_G_POP, g, h, 0);
            if (g->coro == NULL ||
                __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                h->sched.current = NULL;
                pygo_tls_current_g = NULL;
                if (from_runq) pygo_g_decref(g);   /* queue ref */
                continue;
            }
            pygo_hub_resume_begin(h, g);
            pygo_coro_resume(g->coro);
            pygo_hub_resume_end(h);
            self_queued = pygo_tls_self_queued;
            parked_offqueue = pygo_tls_parked_offqueue;
            pygo_tls_self_queued = 0;
            pygo_tls_parked_offqueue = 0;
            pygo_tls_current_g = NULL;
            h->sched.current = NULL;

            if (pygo_coro_done(g->coro)) {
                /* Drain g's chunks, restore hub's tstate so the decref
                 * (potentially calling Python tp_dealloc) allocates on
                 * the hub's chunk -- not on a NULL datastack that
                 * would otherwise leak when the next iter overwrites.
                 * Re-snap so the next completion path / hub exit can
                 * load again. */
                pygo_drain_g_datastack();
                pygo_pystate_load(&hub_snap);
                /* Force-unlink any netpoll parker still referencing g.
                 * If wait_fd's safety unlink missed (because a wake
                 * path bypassed the netpoll dispatcher AND the safety
                 * check's structural test couldn't see the leak), the
                 * parker would survive into stack-pool reuse and
                 * resurrect this just-freed g via a future pump
                 * dispatch.  This call covers that gap. */
                pygo_netpoll_force_unlink_g_parker(g);
                pygo_mn_pending_complete(h);
                pygo_g_decref(g);                  /* scheduler ref */
                if (from_runq) pygo_g_decref(g);   /* queue ref */
                pygo_pystate_snap(&hub_snap);
            } else if (pygo_use_global_runq()) {
                /* Steal-woken release via the wake-state machine -- a near-exact
                 * mirror of the per-g-tstate release above; the difference is
                 * only that the migrated state rides in g->snap (saved by the
                 * parker BEFORE it yielded) instead of a per-g tstate.
                 *
                 * parked_offqueue => the g parked off-queue (netpoll/chan/sleep)
                 * and a wake_g will route it back through the global runq.  End
                 * ownership now: this happens AFTER the snap was saved, and while
                 * RUNNING a wake only set RUNNING_WOKEN (never enqueued), so no
                 * other hub could pull the g and load a half-saved snap.  CAS
                 * RUNNING->PARKED so a later wake enqueues it; if a wake already
                 * landed (RUNNING_WOKEN), enqueue exactly once now. */
                if (parked_offqueue) {
                    int rexp = PYGO_WS_RUNNING;
                    /* Detach the g's ref-holding interpreter state from THIS
                     * hub's tstate now -- still RUNNING (sole owner), BEFORE the
                     * g becomes migratable.  The parker already saved g->snap,
                     * which owns its OWN refs to context / current_exception /
                     * exc_value; loading the hub's clean base here drops this
                     * hub's duplicate refs (rebalancing them).  Without it the
                     * origin hub and the stealing hub (which loads the snap) both
                     * own the g's exception and race its refcount -> the freed
                     * object surfaces as "error return without exception set" /
                     * SEGV deep in CPython's exception machinery.  This is the
                     * snap-mode analogue of per-g-tstate's PyEval_SaveThread
                     * (detach) before the RUNNING->PARKED transition.  Re-snap to
                     * keep hub_snap valid for the next g. */
                    pygo_pystate_load(&hub_snap);
                    pygo_pystate_snap(&hub_snap);
                    if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                        /* madvise idle stack below sp while still RUNNING (sole
                         * owner -- a wake only flips RUNNING->RUNNING_WOKEN), so
                         * no hub can resume into pages mid-drop.  Cross-hub-safe
                         * for the same reason the sweep handshake is.  No-op
                         * unless PYGO_STACK_PARK_DONTNEED=1. */
                        pygo_coro_park(g->coro);
                    }
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    PYGO_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN: a wake landed during the resume.
                         * Re-arm a single entry and enqueue now (snap is saved),
                         * exactly once -- no lost wake, no duplicate. */
                        __atomic_store_n(&g->wake_state, PYGO_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        pygo_g_incref(g);                  /* new queue ref */
                        pygo_mn_global_runq_push(g);
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield (sched_yield: already on local FIFO, or
                     * raw pygo_coro_yield: push it here).  Holds no live parker,
                     * so no wake_g can race -- stays RUNNING, hub-pinned. */
                    __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) pygo_g_decref(g);       /* old queue ref */
                    if (!self_queued) pygo_sched_ready_push(&h->sched, g);
                }
            } else if (!self_queued) {
                /* Raw pygo_coro_yield() -- g didn't go through
                 * sched_yield to push itself.  Keep it alive on the
                 * local FIFO.  tstate still has g's state from after
                 * resume; that's fine -- next iter's g_next->snap
                 * load overwrites. */
                pygo_sched_ready_push(&h->sched, g);
            } else if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                /* g parked on a waiter (netpoll/chan/sleep/park_safe)
                 * and won't run again until woken: drop its now-idle
                 * stack pages.  No-op unless PYGO_STACK_PARK_DONTNEED=1.
                 * Gated on the PARKED states so a cooperative sched_yield
                 * (also self_queued, but re-queued + resuming shortly)
                 * doesn't pay an immediate re-fault.
                 *
                 * Safe under M:N: a woken g routes back to THIS (owning)
                 * hub's non-stealable local FIFO, so this hub is the sole
                 * resumer -- the madvise happens-before the next resume on
                 * one thread.  Default-OFF is a throughput choice, not a
                 * safety one (see pygo_coro_park doc in coro.h). */
                pygo_coro_park(g->coro);
            }
            /* remaining sched_yield path: g pushed itself, tstate has
             * g's state, no work needed here. */
        }
    }
    /* Restore hub's tstate before the thread exits. */
    pygo_pystate_load(&hub_snap);
    pygo_tls_hub = NULL;
    /* Tear down the hub's iouring ring.  Unregister from netpoll FIRST
     * so the pump can't dispatch a CQE to a freed ring; THEN destroy.
     * Hubs only exit when the scheduler is shutting down + all gs have
     * completed, so inflight==0 here is the expected case; if a ring
     * still had inflight ops we'd leak the underlying CQEs (kernel
     * frees them on close). */
    if (h->iouring_ring != NULL) {
        pygo_netpoll_remove_iouring_ring(h->iouring_eventfd);
        pygo_iouring_ring_destroy(h->iouring_ring);
        h->iouring_ring    = NULL;
        h->iouring_eventfd = -1;
    }
    /* Reverse pygo_coro_thread_init for clean exit on Windows
     * (ConvertFiberToThread); no-op elsewhere. */
    pygo_coro_thread_fini();
    PyEval_SaveThread();
    PYGO_THREAD_RETURN(NULL);
}
