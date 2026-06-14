/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


int runloom_mn_hub_count(void)
{
    return runloom_hub_count;
}

void *runloom_mn_current_hub_opaque(void)
{
    return (void *)runloom_tls_hub;
}

int runloom_mn_hub_id_of(void *hub_opaque)
{
    if (hub_opaque == NULL) return -1;
    return ((runloom_hub_t *)hub_opaque)->id;
}

runloom_g_t *runloom_mn_tls_current_g(void)
{
    return runloom_tls_current_g;
}

void runloom_mn_tls_mark_parked(void)
{
    runloom_tls_self_queued = 1;
    runloom_tls_parked_offqueue = 1;
}

runloom_sched_t *runloom_mn_current_sched(void)
{
    runloom_hub_t *h = runloom_tls_hub;
    return h ? &h->sched : NULL;
}

struct runloom_iouring_ring *runloom_mn_current_iouring_ring(void)
{
    runloom_hub_t *h = runloom_tls_hub;
    return h ? h->iouring_ring : NULL;
}

/* Push g onto a hub's submission list.  Called by netpoll pump to
 * route an I/O-woken g back to whichever hub it was running on.
 * (Also used internally by mn_go.)  Hub_main drains submissions every
 * iteration and routes each entry to either the deque (if g is fresh)
 * or the local FIFO (if g has saved state -- the netpoll-wake case).
 *
 * Idempotency: a CAS on g->in_sub_queue makes duplicate submissions
 * no-ops.  Under M:N + free-threaded 3.13t a parker can legitimately
 * be wake_g'd more than once (e.g., wake_g from a netpoll pump that
 * unlinked it normally, followed by a stale wake from the safety
 * unlink at the next wait_fd's yield-return).  Without the CAS the
 * sub queue ends up with g twice, hub_main pops it twice, and the
 * second resume hits a g whose coro was already destroyed by the
 * decref that ran after the first run-to-completion -- segfault in
 * runloom_asm_swap on *(NULL coro). */
static void runloom_mn_hub_submit(runloom_hub_t *h, runloom_g_t *g)
{
    int expected = 0;
    /* Defence-in-depth: a submitted g must not already be DONE/FREED.
     * If this fires, the in_sub_queue CAS below would still no-op but
     * the diag ring + abort give a precise place to look. */
    RUNLOOM_G_ASSERT_NOT(g, RUNLOOM_GST_MASK_DEAD);
    if (!__atomic_compare_exchange_n(&g->in_sub_queue, &expected, 1,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        /* Already queued; no need to enqueue again.  The pending resume
         * will pick up whatever state the g has when hub_main gets to it. */
        return;
    }
    /* Controlled-replay deferred publish: a cross-hub submit made DURING a
     * running segment (the spawning hub holds the baton and is running a g) is
     * staged on the SPAWNING hub per target, then flushed onto the target's
     * sub_list atomically at this segment's release (runloom_mn_ctrl_stage_flush in
     * resume_end).  That makes the target's drain observe the complete set the
     * segment produced rather than a partial mid-segment snapshot -- the
     * deterministic-replay fix.  Gated tight so it is inert otherwise: barrier
     * active, the caller is a hub (runloom_tls_hub) running a fiber
     * (runloom_tls_current_g) targeting a DIFFERENT hub, and staging is allocated.
     * Same-hub submits, non-segment producers (idle pump / pre-run placement),
     * and the entire default scheduler fall straight through to the immediate
     * push below (zero added cost there). */
    {
        runloom_hub_t *cur = runloom_tls_hub;
        if (runloom_mn_ctrl.enabled && runloom_mn_ctrl.barrier &&
            cur != NULL && cur != h && cur->stage_head != NULL &&
            runloom_tls_current_g != NULL &&
            h->id >= 0 && h->id < runloom_hub_count) {
            g->next = NULL;
            if (cur->stage_tail[h->id] != NULL)
                cur->stage_tail[h->id]->next = g;
            else
                cur->stage_head[h->id] = g;
            cur->stage_tail[h->id] = g;
            cur->stage_pending++;
            RUNLOOM_EVT(RUNLOOM_EVT_G_SUBMIT, g, h, 0);
            runloom_g_state_set(g, RUNLOOM_GST_SUBMITTED);
            return;
        }
    }
    RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
    g->next = NULL;
    if (h->sub_tail != NULL) {
        h->sub_tail->next = g;
    } else {
        /* RELEASE: publishes g to the lock-free deadlock census's ACQUIRE read of
         * sub_head (mn_has_wakeable_work) -- tools/README Finding D #4. */
        __atomic_store_n(&h->sub_head, g, __ATOMIC_RELEASE);
    }
    h->sub_tail = g;
    RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
    RUNLOOM_EVT(RUNLOOM_EVT_G_SUBMIT, g, h, 0);
    runloom_g_state_set(g, RUNLOOM_GST_SUBMITTED);
    /* BUG #10 throughput: bump the submit generation, then signal the target hub
     * if it is parked in its idle condvar (it owns parked gs and nothing else to
     * run), so the woken g is drained in ~us instead of stranded for idle_ns
     * (~100us).  RELEASE bump pairs with the waiter's predicate re-read.  The
     * idle_waiting hint is a lock-free fast path: when the hub is NOT waiting
     * (the common busy case) we skip the lock entirely.  A signal that races the
     * hub entering/leaving the wait is harmless -- the wait is TIMED, so a
     * (rare) missed signal just falls back to the idle_ns poll, never a hang. */
    __atomic_add_fetch(&h->sub_gen, 1, __ATOMIC_RELEASE);
    if (__atomic_load_n(&h->idle_waiting, __ATOMIC_ACQUIRE)) {
        RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        runloom_cond_signal(&h->idle_cond);
        RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
    }
    /* Cross-hub wake latency fix: the idle_cond signal above ONLY reaches a hub
     * parked in the parked==0 idle-condvar branch.  But `parked` is the GLOBAL
     * netpoll-parked count, so whenever ANY fiber anywhere is parked on I/O
     * (a server, file I/O, a timer, even the harness progress loop), every idle
     * hub instead blocks in netpoll_pump -> epoll_wait(idle_ns) and the cond
     * signal hits nobody.  The submitted g then waits up to the full pump
     * timeout (~1ms) before the hub loops back to drain sub_head -- which pins
     * cross-hub lock/channel handoff throughput to ~700/s (big_100 p87/p43/p47).
     * Kick the shared pump-wake eventfd (registered in the epoll set) so a
     * pumping hub returns from epoll_wait NOW; it drains the eventfd and re-loops
     * to pick up sub_head/global-runq work.  Level-triggered + non-exclusive, so
     * every blocked hub wakes (the target among them drains sub_head; the rest
     * find nothing and re-pump).  No-op if the eventfd isn't armed. */
    runloom_netpoll_wake_pump();
}

void runloom_mn_wake_g(void *hub_opaque, runloom_g_t *g)
{
    if (hub_opaque == NULL) {
        /* g belongs to a per-thread (single-thread) scheduler, or netpoll was
         * used outside any hub context.  Route via runloom_sched_wake, which (Phase
         * 2) sends the g to its OWNER sched's wake_list + kicks that thread's
         * pump when the waker is a foreign thread -- e.g. the shared netpoll
         * pump on one loop's thread delivering an fd event for a g parked on
         * another loop's thread.  A plain ready_push here would wake it on the
         * wrong (waker's) thread. */
        runloom_sched_wake(g);
        return;
    }
    if (runloom_use_global_runq()) {
        /* per-g-tstate OR steal-woken: g is migratable, so route it to the
         * global run-queue any idle hub can drain instead of the origin hub's
         * owner-drained sub list -- recovers it even if the origin hub is wedged
         * in a blocking C call.  Drive the per-g wake state machine (see
         * runloom_sched.h); it is tstate-agnostic -- the snap (steal-woken) or the
         * per-g tstate (per-g-tstate) carries the migrated state:
         *
         *   PARKED  -> QUEUED          : we won the wake; enqueue exactly once
         *                                (+ a queue ref so the entry keeps g
         *                                alive until a hub consumes it).
         *   RUNNING -> RUNNING_WOKEN    : g is owned by a hub mid-resume; just
         *                                remember the wake -- the owner enqueues
         *                                it at release.  Enqueuing now would let
         *                                a second hub attach g's live tstate
         *                                during the owner's commit->detach gap.
         *   QUEUED / RUNNING_WOKEN      : a wake is already pending -> drop, so a
         *                                g is never enqueued twice (the old
         *                                two-flag design's livelock).
         *
         * CAS-on-failure reloads `st`, so the loop only spins while it keeps
         * losing the PARKED->QUEUED or RUNNING->RUNNING_WOKEN race (a couple of
         * instructions); every other state returns immediately. */
        int st = __atomic_load_n(&g->wake_state, __ATOMIC_ACQUIRE);
        (void)hub_opaque;   /* origin no longer needed: no per-hub rebalance */
        RUNLOOM_G_ASSERT_NOT(g, RUNLOOM_GST_MASK_DEAD);
        for (;;) {
            if (st == RUNLOOM_WS_PARKED) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                RUNLOOM_WS_QUEUED, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    runloom_g_incref(g);   /* queue ref, dropped when consumed */
                    runloom_mn_global_runq_push(g);
                    /* Same cross-hub wake-latency fix as hub_submit: any hub can
                     * drain the global runq, but an idle one is blocked in
                     * epoll_wait -- kick the pump so it wakes now (no-op if not
                     * armed; the pump drains the eventfd and re-loops). */
                    runloom_netpoll_wake_pump();
                    return;
                }
                /* lost: st reloaded to the current state; re-evaluate */
            } else if (st == RUNLOOM_WS_RUNNING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                RUNLOOM_WS_RUNNING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    return;   /* owner will enqueue at release */
                }
            } else if (st == RUNLOOM_WS_SWEEPING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                RUNLOOM_WS_SWEEPING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    return;   /* sweeper will enqueue at release */
                }
            } else {
                return;   /* QUEUED / RUNNING_WOKEN / SWEEPING_WOKEN: pending */
            }
        }
    }
    runloom_mn_hub_submit((runloom_hub_t *)hub_opaque, g);
}

int runloom_mn_hub_request_iouring_cancel(void *hub_opaque, void *op)
{
    runloom_hub_t *h = (runloom_hub_t *)hub_opaque;
    void *expected = NULL;
    if (h == NULL || op == NULL) return 0;
    if (!__atomic_compare_exchange_n(&h->iouring_cancel_op, &expected, op, 0,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        return 0;   /* a cancel is already pending for this hub (best-effort) */
    /* Wake the hub if idle-parked so it drains the mailbox promptly. */
    if (__atomic_load_n(&h->idle_waiting, __ATOMIC_ACQUIRE)) {
        RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        runloom_cond_signal(&h->idle_cond);
        RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
    }
    return 1;
}

/* Idle-stack-sweep handshake (RUNLOOM_PER_G_TSTATE).  An idle hub about to
 * MADV_DONTNEED a long-parked g's below-SP stack pages must hold the g
 * un-resumable for the madvise's duration, or another hub could pull a wake and
 * resume the g into pages the kernel is concurrently zeroing.  try_claim CASes
 * PARKED -> SWEEPING (the same exclusivity QUEUED->RUNNING gives a resumer); it
 * loses to any non-PARKED state (woken/owned) and the sweeper skips that g.
 * The held g cannot run, complete, or be freed, so its pointer stays valid
 * across the unlocked madvise -- restoring the exact liveness the default
 * (sole-resumer) mode relies on. */
int runloom_mn_sweep_try_claim(runloom_g_t *g)
{
    int st = RUNLOOM_WS_PARKED;
    return __atomic_compare_exchange_n(&g->wake_state, &st, RUNLOOM_WS_SWEEPING,
                                       0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

/* End a sweep claim after the madvise.  SWEEPING -> PARKED if no wake landed.
 * If a wake landed during the madvise the state is SWEEPING_WOKEN (wake_g
 * remembered it without enqueueing); convert it to QUEUED and push exactly once
 * -- the sweeper is the sole "winner" of that deferred wake, mirroring the
 * RUNNING_WOKEN -> QUEUED release, so the wake is never lost and never
 * duplicated. */
void runloom_mn_sweep_claim_release(runloom_g_t *g)
{
    int st = RUNLOOM_WS_SWEEPING;
    if (__atomic_compare_exchange_n(&g->wake_state, &st, RUNLOOM_WS_PARKED, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return;   /* clean release; no wake during the madvise */
    }
    /* The only state try_claim could leave that isn't SWEEPING is
     * SWEEPING_WOKEN (no other actor touches a SWEEPING g except wake_g, which
     * only flips it to SWEEPING_WOKEN).  Re-enqueue the deferred wake. */
    __atomic_store_n(&g->wake_state, RUNLOOM_WS_QUEUED, __ATOMIC_RELEASE);
    runloom_g_incref(g);   /* queue ref, dropped when consumed */
    runloom_mn_global_runq_push(g);
}

int runloom_mn_yield_current(void)
{
    runloom_hub_t *h = runloom_tls_hub;
    runloom_g_t *g = runloom_tls_current_g;
    if (h == NULL || g == NULL) {
        return 0;
    }
    /* Trivial-switch fast path: if there's no other work for this hub
     * -- no yielded g in local FIFO, no fresh g in our deque, no
     * sleeper due, no parked I/O -- yielding would just snap, swap to
     * hub_main, find an empty queue, swap back.  Skip it.
     *
     * We do NOT peek neighbours' deques here: a steal happens only when
     * a hub goes idle (hub_main's main path), and idle implies the
     * neighbour itself has nothing local to run.  Letting a g monopolise
     * a hub while neighbours have work is fine -- the work-stealing
     * scheduler is allowed to leave stealable items on a busy hub.
     * This matches single-thread's runloom_sched_yield fast path. */
    if (__builtin_expect(runloom_sched_ready_empty(&h->sched)
                         && runloom_cldeque_size(&h->deque) == 0
                         && h->sched.sleep_size == 0
                         && runloom_netpoll_parked_count() == 0
                         && runloom_blockpool_inflight() == 0, 1)) {
        /* The fast path deliberately does NOT look at h->sub_head -- work
         * delivered by mn_go (main thread) or wake_g (another hub) lands there,
         * and checking it lock-free on this hot path is fragile.  But that
         * means a g looping on sched_yield with momentarily-empty local queues
         * would spin forever and NEVER return to hub_main to drain sub_head, so
         * a fiber later mn_go'd / woken onto this hub starves (the multi-hub
         * sched_yield-fairness bug).  Bound it: after RUNLOOM_YIELD_FASTPATH_BOUND
         * consecutive trivial fast-paths, fall through to a real yield so
         * hub_main drains sub_head (under sub_lock, as always) + re-polls. */
        if (++runloom_tls_fastpath_streak < RUNLOOM_YIELD_FASTPATH_BOUND) {
            return 1;
        }
    }
    runloom_tls_fastpath_streak = 0;
    runloom_sched_ready_push(&h->sched, g);
    runloom_pystate_snap(&g->snap);
    runloom_tls_self_queued = 1;
    runloom_coro_yield();
    /* On return: hub_main has loaded g->snap, so we're back in our
     * own tstate slice and can keep running user code. */
    return 1;
}
