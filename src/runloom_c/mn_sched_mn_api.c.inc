/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


int runloom_mn_hub_count(void)
{
    return runloom_hub_count;
}

/* R0 gauges: per-hub census over runloom_hubs[].  Lock-free, following the
 * sysmon walk pattern (mn_sched_sysmon.c.inc): the array is PyMem_Calloc'd
 * once at mn_init and never realloc'd within a run, so snapshotting
 * runloom_hub_count and reading each hub's atomic field is race-free.  Safe
 * from m_stats -- takes no lock any pump/hub holds.
 *
 * mn_pending_total: submitted-minus-completed gs across all hub run queues
 * (the true live in-scheduler g population; work-stealing moves `pending`
 * between hubs but the SUM is conserved).  Falls to ~0 at quiescence; a
 * climbing floor across soak iterations = fibers stuck in hub queues.
 * mn_completed_total: cumulative retired gs across hubs -- an odometer
 * (never falls), a liveness signal to confirm the soak is retiring work.
 * mn_deque_depth_total: fresh (stealable) gs across all hub deques. */
long runloom_mn_pending_total(void)
{
    long total = 0;
    int i, n = runloom_hub_count;
    if (runloom_hubs == NULL) return 0;
    for (i = 0; i < n; i++)
        total += __atomic_load_n(&runloom_hubs[i].pending, __ATOMIC_ACQUIRE);
    return total;
}

long long runloom_mn_completed_total(void)
{
    long long total = 0;
    int i, n = runloom_hub_count;
    if (runloom_hubs == NULL) return 0;
    for (i = 0; i < n; i++)
        total += (long long)__atomic_load_n(&runloom_hubs[i].sched.completed,
                                            __ATOMIC_ACQUIRE);
    return total;
}

long runloom_mn_deque_depth_total(void)
{
    long total = 0;
    int i, n = runloom_hub_count;
    if (runloom_hubs == NULL) return 0;
    for (i = 0; i < n; i++) {
        long d = runloom_cldeque_size(&runloom_hubs[i].deque);
        if (d > 0) total += d;
    }
    return total;
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

void runloom_mn_tls_mark_parked_foreign(void)
{
    /* The segment about to yield is parking on a foreign-thread completion
     * (blocking-IO / offload).  Recorded per hub-thread; consumed at the resume
     * boundary by runloom_mn_ctrl_release.  No effect unless the controlled
     * barrier census is active. */
    runloom_tls_parked_foreign = 1;
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
 * (Also used internally by mn_fiber.)  Hub_main drains submissions every
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
    int per_g = runloom_get_per_g_tstate_mode();
    /* QUEUE REF, taken via TRY_INCREF *before* touching g->in_sub_queue.
     *
     * A queued g used to be held only by its spawn ref, so a STALE wake_g racing
     * the g's completion (the duplicate-wake the comment above describes) could
     * re-reference a g that completion had already freed -> use-after-free (the
     * arm64 per-hub-kqueue crash: a freed g, magic 0xDEAD, re-submitted into a
     * hub's sub_head and resumed).
     *
     * The ref MUST be a refcount>0-gated try_incref taken up front, not a plain
     * incref after the CAS: CBMC (tools/verify/cbmc/sched_qref_cbmc.c) shows the latter
     * still has a window where completion frees the g between the CAS and the
     * incref (it resurrects a freed g).  try_incref FAILS if the g is already
     * freeing (refcount 0), so a stale wake on a completed g bails here, and on
     * success the g is pinned for the lifetime of THIS queue entry -- hub_main
     * drops it exactly once when it consumes the entry (the in_sub_queue 1->0
     * exchange).  Per-g-tstate mode uses the global-runq from_runq ref instead,
     * so it keeps the original path. */
    if (!per_g && !runloom_g_try_incref(g))
        return;                          /* g is freeing -> drop the stale wake */
    /* Perturbation seam (RUNLOOM_DELAY): widen the window between the
     * try_incref that guards resurrection and the in_sub_queue CAS below -- the
     * exact ABA window sched_qref_cbmc.c proves.  The formal proof shows it is
     * safe FOR ALL interleavings; this widener lets a soak/TSan run actually
     * HIT that window on the real .so.  No-op unless RUNLOOM_DELAY names it. */
    runloom_delay_inject(RUNLOOM_DLY_G_RESURRECT);
    /* Defence-in-depth (debug): a live g must not already be DONE/FREED. */
    RUNLOOM_G_ASSERT_NOT(g, RUNLOOM_GST_MASK_DEAD);
    if (!__atomic_compare_exchange_n(&g->in_sub_queue, &expected, 1,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        /* Already queued -> drop the ref we just took and bail.  The pending
         * resume picks up whatever state the g has when hub_main reaches it. */
        if (!per_g) runloom_g_decref(g);
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
            RUNLOOM_NEXT_SET(g, NULL, 5);   /* staging (controlled mode) */
            if (cur->stage_tail[h->id] != NULL)
                RUNLOOM_NEXT_SET(cur->stage_tail[h->id], g, 5);
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
    RUNLOOM_NEXT_SET(g, NULL, 1);   /* hub_submit: new tail's next */
    if (h->sub_tail != NULL) {
        RUNLOOM_NEXT_SET(h->sub_tail, g, 1);   /* link old tail -> g */
    } else {
        /* RELEASE: publishes g to the lock-free deadlock census's ACQUIRE read of
         * sub_head (mn_has_wakeable_work) -- tools/README Finding D #4. */
        __atomic_store_n(&h->sub_head, g, __ATOMIC_RELEASE);
    }
    h->sub_tail = g;
    RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
    RUNLOOM_EVT(RUNLOOM_EVT_G_SUBMIT, g, h, 0);
    runloom_g_state_set(g, RUNLOOM_GST_SUBMITTED);
    /* MNWAKE-TRACE: the DURABLE append to the owner hub's sub_head (model
     * WakerAppend) -- opens a model episode for g.  Gated on g->snap.valid so only
     * a WOKEN g (snapped its state at park) emits, not a FRESH spawn (snap-invalid,
     * routed to the deque not the ready FIFO -> no HUB_DRAIN, not a wake episode);
     * this matches the HUB_DRAIN emit (also snap.valid only).  snap.valid is set at
     * park on the owner thread before this foreign waker's parked_safe CAS, so the
     * read is race-free.  Immediate-push path only (the staged path returns above). */
    /* trace_active() short-circuits so g->snap.valid is NOT read in production
     * (trace off) -- removing the benign diagnostics-only race against the owner
     * hub's snap.valid write at park/resume, plus a hot-path load. */
    if (runloom_mnwake_trace_active() && g->snap.valid)
        runloom_mnwake_trace_event("FOREIGN_WAKE", (unsigned long)(uintptr_t)g, 0);
    /* BUG #10 throughput: bump the submit generation, then signal the target hub
     * if it is parked in its idle condvar (it owns parked gs and nothing else to
     * run), so the woken g is drained in ~us instead of stranded for idle_ns
     * (~100us).  RELEASE bump pairs with the waiter's predicate re-read.  The
     * idle_waiting hint is a lock-free fast path: when the hub is NOT waiting
     * (the common busy case) we skip the lock entirely.  A signal that races the
     * hub entering/leaving the wait is harmless -- the wait is TIMED, so a
     * (rare) missed signal just falls back to the idle_ns poll, never a hang. */
    __atomic_add_fetch(&h->sub_gen, 1, __ATOMIC_RELEASE);
    /* io_uring-as-loop: if the target hub is blocked in its ring (not the idle
     * condvar), interrupt that wait by writing its per-hub wake eventfd.  The
     * SEQ_CST fence pairs with the hub's ring_waiting store + sub_head re-check
     * (a Dekker handshake): we pushed to sub_head above, so either the hub sees
     * it on its re-check and skips the wait, or it is blocked and we wake it
     * here.  ring_waiting is 0 unless the loop backend is on, so the default
     * path skips straight to the idle_cond signal below. */
    if (__atomic_load_n(&h->ring_waiting, __ATOMIC_RELAXED)) {
        __atomic_thread_fence(__ATOMIC_SEQ_CST);
        if (__atomic_load_n(&h->ring_waiting, __ATOMIC_ACQUIRE))
            runloom_iouring_loop_wake(h->loop_wake_fd);
    }
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
     * Per-hub kqueue: kick ONLY the target hub `h`'s own pump (its self-pipe),
     * not a broadcast -- this is the hot cross-hub handoff path.  Shared-handle
     * backends (epoll/IOCP/select) ignore the arg. */
    runloom_netpoll_wake_pump((void *)h);
    /* MNWAKE-TRACE: both kicks fired (model WakerSignal -- idle_cond + wake-pump
     * eventfd, each FREE delivery).  One event per WOKEN-g submit (snap.valid, as
     * for FOREIGN_WAKE); delivery left free. */
    if (runloom_mnwake_trace_active() && g->snap.valid)
        runloom_mnwake_trace_event("SIGNAL", (unsigned long)(uintptr_t)g, 0);
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
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_PARKED, RUNLOOM_WS_QUEUED);
                    runloom_g_incref(g);   /* queue ref, dropped when consumed */
                    runloom_mn_global_runq_push(g);
                    /* Same cross-hub wake-latency fix as hub_submit: any hub can
                     * drain the global runq, but an idle one is blocked in
                     * epoll_wait -- kick the pump so it wakes now (no-op if not
                     * armed; the pump drains the eventfd and re-loops).  NULL =
                     * default pool; global runq is drained by ANY hub (steal mode,
                     * default-off) which re-polls on its bounded timeout. */
                    runloom_netpoll_wake_pump(NULL);
                    return;
                }
                /* lost: st reloaded to the current state; re-evaluate */
            } else if (st == RUNLOOM_WS_RUNNING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                RUNLOOM_WS_RUNNING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_RUNNING, RUNLOOM_WS_RUNNING_WOKEN);
                    return;   /* owner will enqueue at release */
                }
            } else if (st == RUNLOOM_WS_SWEEPING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                RUNLOOM_WS_SWEEPING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    RUNLOOM_WS_NOTE(RUNLOOM_WS_SWEEPING, RUNLOOM_WS_SWEEPING_WOKEN);
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
    /* MSVC atomics don't support void ** in _Generic; cast to intptr_t. */
    intptr_t expected = 0;
    intptr_t cop = (intptr_t)op;
    if (h == NULL || op == NULL) return 0;
    if (!__atomic_compare_exchange_n((intptr_t *)&h->iouring_cancel_op, &expected, cop, 0,
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

/* Kick a hub out of EVERY wait mode (pump epoll_wait / its own io_uring ring
 * wait under IOURING_LOOP / the idle condvar) so it drains a freshly-deposited
 * mailbox promptly.  Mirrors runloom_mn_kick_all_hubs' per-hub trio -- a
 * cond_signal alone strands a ring-blocked or pump-blocked hub. */
static void runloom_mn_kick_hub_all_modes(runloom_hub_t *h)
{
    if (h->loop_wake_fd >= 0) runloom_iouring_loop_wake(h->loop_wake_fd);
    if (__atomic_load_n(&h->idle_waiting, __ATOMIC_ACQUIRE)) {
        RUNLOOM_RLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
        runloom_cond_signal(&h->idle_cond);
        RUNLOOM_RUNLOCK(&h->idle_lock, RUNLOOM_RANK_HUB_IDLE);
    }
    runloom_netpoll_wake_pump((void *)h);
}

#if defined(IORING_ASYNC_CANCEL_FD)
/* Broadcast a cancel-by-fd to every live hub's ring (R7 item 1 /
 * DESIGN_mn_iouring_cancel_fd.md).  Called by TCPConn.close() BEFORE closesock
 * (so `fd` is still open): dup() it once per hub -- a dup shares the same
 * struct file, so an ASYNC_CANCEL_FD on the dup matches the recv op (keyed by
 * that file) even after the original fd closes -- and deposit each dup in the
 * owning hub's cancel-by-fd mailbox.  The hub (its ring's SINGLE issuer)
 * submits the cancel at its loop top and closes the dup when the cancel CQE
 * drains.  A hub with no matching op cancels nothing (-ENOENT) but still closes
 * its dup.  No-op when M:N isn't running / a hub has no ring.
 *
 * Teardown safety: the hub-thread gate below makes walking runloom_hubs race-free
 * against mn_fini's free (see the gate comment).  Any node a stopped hub never
 * claims is drained + its dup closed by mn_fini's per-hub cleanup (and the
 * mn_init failure path); a submitted-but-undrained cancel op's dup is reaped by
 * the owning hub's final ring drain at exit (mn_sched_hub_main.c.inc). */
void runloom_iouring_cancel_fd_all_hubs(int fd)
{
    int n, i;
    if (fd < 0) return;
    /* Only walk runloom_hubs when we are running ON a hub thread -- i.e. this is a
     * close() from a goroutine, the case the cancel actually exists for.  Then the
     * M:N runtime is provably live and mn_fini CANNOT free runloom_hubs from under
     * us: mn_fini joins EVERY hub thread (init_fini) before it destroys the per-hub
     * cancel_fd_lock / PyMem_Free's the array, and THIS thread is one of those hubs
     * still inside its loop running our goroutine -- so that join is blocked on us,
     * and every other hub is likewise alive.  A foreign (non-hub) OS thread has no
     * such guarantee and would race that free -- a TOCTOU use-after-free on the hub
     * array + a lock-after-destroy on cancel_fd_lock (the RunloomG.wake dekker
     * hazard class, module_g.c.inc) -- and it has nothing to cancel here anyway (a
     * hub-ring recv only exists within a live session).  So it bails; close()'s
     * GLOBAL-ring cancel (runloom_iouring_cancel_fd) still runs for it. */
    if (runloom_mn_current_hub_opaque() == NULL) return;
    n = runloom_hub_count;
    if (n <= 0 || runloom_hubs == NULL) return;
    for (i = 0; i < n; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        runloom_cancel_fd_node_t *node;
        int d;
        if (h->iouring_ring == NULL)
            continue;   /* no ring -> no single-shot recv parked on this hub */
        d = dup(fd);
        if (d < 0) continue;
        node = (runloom_cancel_fd_node_t *)malloc(sizeof(*node));
        if (node == NULL) { close(d); continue; }
        node->dup_fd = d;
        runloom_mutex_lock(&h->cancel_fd_lock);
        node->next = h->cancel_fd_head;
        /* RELAXED-atomic publish: the owning hub PEEKs cancel_fd_head lock-free at
         * its loop top (mn_sched_hub_main.c.inc), so a plain store here would be a
         * C11 data race vs that load.  Matches the sub_head lock-free-reader fix
         * and the sibling iouring_cancel_op mailbox (atomic on both sides). The
         * authoritative read is still under the lock; the post-store kick barrier
         * makes a stale-NULL peek self-correct on the next bounded loop turn. */
        __atomic_store_n(&h->cancel_fd_head, node, __ATOMIC_RELAXED);
        runloom_mutex_unlock(&h->cancel_fd_lock);
        runloom_mn_kick_hub_all_modes(h);
    }
}
#else
void runloom_iouring_cancel_fd_all_hubs(int fd) { (void)fd; }
#endif

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
    if (__atomic_compare_exchange_n(&g->wake_state, &st, RUNLOOM_WS_SWEEPING,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        RUNLOOM_WS_NOTE(RUNLOOM_WS_PARKED, RUNLOOM_WS_SWEEPING);
        return 1;
    }
    return 0;
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
        RUNLOOM_WS_NOTE(RUNLOOM_WS_SWEEPING, RUNLOOM_WS_PARKED);
        return;   /* clean release; no wake during the madvise */
    }
    /* The only state try_claim could leave that isn't SWEEPING is
     * SWEEPING_WOKEN (no other actor touches a SWEEPING g except wake_g, which
     * only flips it to SWEEPING_WOKEN).  Re-enqueue the deferred wake. */
    RUNLOOM_WS_NOTE(RUNLOOM_WS_SWEEPING_WOKEN, RUNLOOM_WS_QUEUED);
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
         * delivered by mn_fiber (main thread) or wake_g (another hub) lands there,
         * and checking it lock-free on this hot path is fragile.  But that
         * means a g looping on sched_yield with momentarily-empty local queues
         * would spin forever and NEVER return to hub_main to drain sub_head, so
         * a fiber later mn_fiber'd / woken onto this hub starves (the multi-hub
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
