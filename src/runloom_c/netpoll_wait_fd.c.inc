/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Forcibly wake the CALLING thread's parked fibers with ready_mask=-1
 * (cancelled).  Each waiter's runloom_netpoll_wait_fd call returns -1; callers
 * (server accept loops, etc.) see that and exit their loops.  Returns count
 * woken.
 *
 * Phase 2: SCOPED to the calling thread's scheduler (g->owner == this sched).
 * runloom runs one scheduler per OS thread but a single shared netpoll, and every
 * non-hub loop's parkers share the default pool.  A global drain (the old
 * behavior) cancelled OTHER still-running loops' in-flight I/O too -- e.g. one
 * paio.run()'s teardown (sched_reset) stranded a concurrent loop's recv with a
 * spurious -1, surfacing as a BlockingIOError out of StreamReader._fill.  Only
 * this thread's own parkers are drained now; others (and M:N hub gs, whose
 * owner is NULL) stay linked. */
int runloom_netpoll_drain_parked(void)
{
    runloom_sched_t *owner = runloom_sched_get();
    int n = 0;
    int pi;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        runloom_parked_t *p;
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        p = pool->head;
        while (p != NULL) {
            runloom_parked_t *next = p->next;   /* capture before unlink splices p out */
            if (p->g != NULL && p->g->owner == owner) {
                int cur;
                /* Claim the parker (same protocol as the pump) so a g that is
                 * mid-commit doesn't both park and get cancel-woken. */
                for (;;) {
                    cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
                    if (cur == RUNLOOM_PARK_WOKEN) break;
                    if (__atomic_compare_exchange_n(&p->commit, &cur,
                                                    RUNLOOM_PARK_WOKEN, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        RUNLOOM_PARK_NOTE(cur, RUNLOOM_PARK_WOKEN);
                        break;
                    }
                }
                /* Removes p from the global list, per-fd bucket and deadline
                 * heap, decrements pool->total, and clears p->g->netpoll_parker. */
                (void)runloom_parker_unlink(pool, p);
                if (cur != RUNLOOM_PARK_WOKEN) {   /* not already claimed by the pump */
                    if (p->ready_out != NULL) {
                        *p->ready_out = -1;   /* signal cancellation */
                    }
                    /* Only re-queue a g that had committed to parking; an ARMED
                     * g will see WOKEN at its commit CAS and abort itself. */
                    if (cur == RUNLOOM_PARK_PARKED) {
                        if (p->hub != NULL) {
                            runloom_mn_wake_g(p->hub, p->g);
                        } else {
                            runloom_sched_wake(p->g);
                        }
                    }
                    n++;
                }
            }
            p = next;
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return n;
}

/* Hand a raised Python signal-handler exception (already stashed on the calling
 * thread's scheduler ->signal_exc) to ONE wait_fd parker owned by that
 * scheduler: claim it, store the RUNLOOM_NETPOLL_SIGNALED sentinel in its
 * ready_out, and re-queue it.  On resume runloom_netpoll_wait_fd sees the sentinel,
 * restores ->signal_exc into the fiber's tstate, and returns -1 -- so the
 * exception propagates out of the cooperative blocking call (recv/accept/
 * select/...) through THAT fiber's stack, where its own try/except sees it,
 * exactly as a signal interrupting a real recv()/select() does.  This is what
 * lets the verbatim CPython test_select_interrupt_exc pass instead of the
 * scheduler swallowing the signal or carrying it out of run().  Returns 1 if a
 * parker took it, 0 if none were eligible (the scheduler then carries the
 * exception out of run_forever()/run_until_complete -- the sleep-only/idle
 * fallback).  Called only from the scheduler grab, which has already verified
 * (via PyErr_CheckSignals) that a handler actually raised -- so a fiber is
 * never woken spuriously (an EINTR with no pending Python signal wakes no one). */
int runloom_netpoll_signal_wake(void)
{
    runloom_sched_t *owner = runloom_sched_get();
    int pi;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        runloom_parked_t *p;
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        for (p = pool->head; p != NULL; p = p->next) {
            int cur;
            if (p->g == NULL || p->g->owner != owner) continue;
            /* Claim via the same commit CAS the pump/timeout/cancel use, so
             * exactly one waker wins this parker. */
            for (;;) {
                cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
                if (cur == RUNLOOM_PARK_WOKEN) break;
                if (__atomic_compare_exchange_n(&p->commit, &cur, RUNLOOM_PARK_WOKEN,
                                                0, __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    RUNLOOM_PARK_NOTE(cur, RUNLOOM_PARK_WOKEN);
                    break;
                }
            }
            /* Only a committed (PARKED) g is re-queued here: it has yielded and
             * will resume in wait_fd to pick up the stashed exception.  An ARMED
             * g hasn't yielded (still running on its own thread) and a WOKEN one
             * was just claimed by another waker -- skip both, try the next. */
            if (cur != RUNLOOM_PARK_PARKED) continue;
            if (p->ready_out != NULL) *p->ready_out = RUNLOOM_NETPOLL_SIGNALED;
            runloom_parker_unlink(pool, p);
            if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
            else                runloom_sched_wake(p->g);
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            return 1;
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return 0;
}

/* ---- park / wake ---- */
int runloom_netpoll_wait_fd(int fd, int events, long long timeout_ns)
{
    runloom_parked_t *park;
    runloom_sched_t *s;
    int ready_mask = 0;
    int new_earliest_deadline = 0;
    void *hub_opaque;
    runloom_g_t *current_g;
    runloom_parker_pool_t *pool;

    if (runloom_netpoll_init() != 0) return -1;

    /* fd validity, checked up front (before linking a parker).  A negative fd is
     * rejected uniformly across backends as OSError(EBADF): epoll_ctl / recv
     * would reject it too, but the POSIX select pump's FD_SET(fd) is UNDEFINED
     * for fd < 0 -- glibc's _FORTIFY_SOURCE __fdelt_chk aborts the process.  The
     * POSIX select backend additionally cannot represent an fd >= FD_SETSIZE in
     * an fd_set, so reject those too (epoll/kqueue handle high fds fine). */
    if (fd < 0) { errno = EBADF; return -1; }
#if !defined(RUNLOOM_HAVE_EPOLL) && !defined(RUNLOOM_HAVE_KQUEUE) && \
    !defined(RUNLOOM_OS_WINDOWS)
    if (fd >= FD_SETSIZE) { errno = EINVAL; return -1; }
#endif
#if !defined(RUNLOOM_OS_WINDOWS)
    /* Reject a fd at/above the process's hard max-open-files: it cannot be a
     * live fd, and indexing the per-fd parker/arm arrays by it (in
     * runloom_parker_fd_index_ensure / the epoll arm cache) would force a growth
     * to fd-size -- a multi-GB realloc whose zero-fill hangs the thread under
     * overcommit.  A bogus huge fd thus errors cleanly instead of wedging. */
    {
        static long runloom_max_fd_cap = -1;
        long cap = __atomic_load_n(&runloom_max_fd_cap, __ATOMIC_RELAXED);
        if (cap < 0) {
            struct rlimit rl;
            cap = (getrlimit(RLIMIT_NOFILE, &rl) == 0 &&
                   rl.rlim_max != RLIM_INFINITY && rl.rlim_max > 0)
                  ? (long)rl.rlim_max : (1L << 22);
            __atomic_store_n(&runloom_max_fd_cap, cap, __ATOMIC_RELAXED);
        }
        if ((long)fd >= cap) { errno = EBADF; return -1; }
    }
#endif

    park = runloom_parker_pool_acquire();
    if (park == NULL) { errno = ENOMEM; return -1; }

    /* Determine where this g lives so pump can route the wake.
     * If we're inside an M:N hub, the hub TLS gives us the target;
     * otherwise it's the global single-thread scheduler. */
    hub_opaque = runloom_mn_current_hub_opaque();
    s = runloom_sched_get();
    if (hub_opaque != NULL) {
        /* Hub context: current g is in TLS (set by hub_main).  We
         * don't read s->current because that's the single-thread
         * sched's slot, not the hub's. */
        current_g = runloom_mn_tls_current_g();
    } else {
        current_g = s->current;
    }

    /* Pick the parker pool by current hub.  Per-hub pools mean a
     * fiber parked on hub H links into pool[H].head + pool[H]
     * .by_fd[fd] + pool[H].dh_arr; another hub's wait_fd contends
     * only on its own pool's lock.  Single-thread sched + non-hub
     * callers fall back to pool[DEFAULT]. */
    pool = runloom_parker_pool_for_hub(hub_opaque);
    runloom_parker_pool_lock_ensure_inited(pool);

    park->fd = fd;
    park->events = events;
    park->deadline_ns = timeout_ns < 0 ? -1 : monotonic_ns() + timeout_ns;
    park->ready_out = &ready_mask;
    park->g = current_g;
    park->hub = hub_opaque;
    /* next/slot/next_by_fd/prev_by_fd are NULL from pool acquire. */

    /* Per-g parker tracking: each g has at most one parker active.
     * Setting this lets hub_main's completion path detect+forcibly
     * unlink any leaked parker before the g's stack is returned to
     * the pool (which would otherwise let pump dereference freed
     * memory via the stale parker pointer). */
    if (current_g != NULL) {
        current_g->netpoll_parker = park;
    }

    /* ORDER MATTERS (M:N + free-threaded race fix):
     *
     *   1. Link the parker.  Now any pump that wakes on an event for
     *      this fd can find it in pool->by_fd[fd].
     *   2. Consume any wakeups that fired BEFORE we got here.  Under
     *      M:N another hub's pump may have observed an event between
     *      this g's previous unlink (on its last wake) and the link
     *      above; the pending-wake bitmap captured it.
     *   3. Only then call epoll_ctl ADD / IOCP submit.  If ADD
     *      synthesizes an edge because the fd is already ready, the
     *      parker is already visible -- the pump will route it.
     *
     * The PREVIOUS ordering (submit before link) lost wakes in two
     * ways: (a) ADD-synthesized edges processed by another hub before
     * the parker was visible; (b) edges fired during the gap between
     * a previous parker being unlinked-on-wake and a new one being
     * linked. */
    RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    {
        /* Cross-hub poller-break (Go's wakeNetPoller, when < pollUntil).  If
         * this parker arms the pool's new EARLIEST deadline, a sibling hub
         * already blocked in the pump's epoll_wait/kevent computed its timeout
         * from a LATER deadline and would oversleep this one.  Note whether we
         * are the new minimum, then break the poller AFTER dropping the lock so
         * it re-reads the global min-deadline and re-clamps.  Guarded on "new
         * pool min" so an ordinary later-deadline park does not thundering-herd
         * the pump; a no-op on the single-thread scheduler (wake fd unarmed). */
        long long prev_min = runloom_dh_peek_deadline(pool);
        runloom_parker_link(pool, park);
        new_earliest_deadline = (park->deadline_ns >= 0 &&
                                 (prev_min < 0 || park->deadline_ns < prev_min));
    }
    RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    /* Per-hub kqueue: the parker + its deadline live in `pool`, owned by
     * hub_opaque, so break THAT pump (NULL on the single-thread sched -> default
     * pool, matching the old single-pump behaviour). */
    if (new_earliest_deadline) runloom_netpoll_wake_pump(hub_opaque);
    if (current_g != NULL) {
        /* Record what we're blocked on as plain data ON the g, so the
         * fiber dump can report "fd=N R/W" without ever dereferencing
         * the (pool-recycled) parker.  Set before the state flip so a dump
         * that sees PARKED_NETPOLL also sees a matching fd. */
        __atomic_store_n(&current_g->park_fd, fd, __ATOMIC_RELAXED);
        __atomic_store_n(&current_g->park_events, events, __ATOMIC_RELAXED);
        runloom_g_state_set(current_g, RUNLOOM_GST_PARKED_NETPOLL);
    }

    /* Drain any pre-existing pending-wake bits.  If something is
     * already there (from a pump that saw an event between our last
     * unlink and this link), wake ourselves immediately instead of
     * parking. */
    {
        int pending = runloom_fd_pending_wake_consume(fd, events);
        if (pending != 0) {
            RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_unlink(pool, park);
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            if (current_g != NULL) runloom_g_state_set(current_g, RUNLOOM_GST_RUNNING);
            runloom_parker_pool_release(park);
            return pending;
        }
    }

#if defined(RUNLOOM_OS_WINDOWS)
    /* IOCP-AFD: submit the poll request now; pump just drains
     * completions.  Falls through to the no-op register for the
     * WSAPoll / select paths. */
    if (runloom_win_use_iocp) {
        /* Fault injection short-circuits the real submit so no AFD IRP is left
         * in flight on a simulated failure; the parker is cleanly unlinked. */
        int finj = runloom_fault_inject(RUNLOOM_FAULT_IOCP_SUBMIT);
        void *ctx = NULL;
        /* ONE TIMEOUT OWNER: submit with an INFINITE AFD Timeout (-1) regardless
         * of this park's deadline, so the IRP NEVER self-completes on timeout and
         * is never left in flight after the deadline heap releases the parker.
         * The deadline heap (runloom_pump_drain_expired) is the sole timeout: when
         * it expires it unlinks the parker, and the unlink choke point
         * (runloom_parker_unlink) runloom_iocp_cancel's this exact IRP.  Without
         * this, a per-op AFD Timeout could fire AFTER the heap released + the fd
         * was reused, dispatching a stale completion against the new owner (UAF). */
        if (finj || runloom_iocp_submit(fd, events, -1, park->gen, &ctx) != 0) {
            RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_unlink(pool, park);
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_pool_release(park);
            return -1;
        }
        /* Track the in-flight IRP's ctx on the parker (a tracking ref backed by
         * the ctx's two-party refcount, refs==2 from submit) under pool->lock so
         * the unlink choke point can runloom_iocp_cancel it on EVERY early-release
         * path.  The lock makes the field consistent: every runloom_parker_unlink
         * runs holding it, and it EXTRACTS iocp_ctx exactly once (clear-after-read),
         * so whichever unlink sees the non-NULL ctx drops the parker party's ref.
         *
         * Race with an immediate pump claim: the IRP can complete and be drained
         * the instant it is queued (before this store), so a pump on another hub
         * may claim+unlink this parker while iocp_ctx is still NULL -- it then
         * does NOT cancel, leaving the parker ref undropped.  That is fine: the
         * pump's claim CASes commit to WOKEN, so THIS fiber's own commit-CAS below
         * fails and takes the abort path, whose runloom_parker_unlink runs AFTER
         * this store, extracts the now-stored ctx, and drops the parker ref then.
         * So the parker ref is always dropped exactly once (here-stored ctx is
         * extracted by whichever unlink runs after the store), and the IRP ref is
         * dropped exactly once by the wait-drain -- refcount frees on the second.
         * No store can be "lost": the abort path is guaranteed to run an unlink
         * after a lost commit-CAS.  See the self-review for the full interleaving. */
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        park->iocp_ctx = ctx;
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    } else {
        if (runloom_netpoll_register(fd, events) != 0) {
            RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_unlink(pool, park);
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_pool_release(park);
            return -1;
        }
    }
#else
    if (runloom_netpoll_register(fd, events) != 0) {
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        runloom_parker_unlink(pool, park);
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        runloom_parker_pool_release(park);
        return -1;
    }
#endif

    /* Re-check pending bits after register: the ADD may have
     * synthesized an edge that another hub's pump processed in the
     * window between link and register; that pump's "no parker"
     * fallback now sets the pending bit (because we re-arrange the
     * pump path below to do that).  Drain again before yielding. */
    {
        int pending = runloom_fd_pending_wake_consume(fd, events);
        if (pending != 0) {
            RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_unlink(pool, park);
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_pool_release(park);
            return pending;
        }
    }

    /* Commit to parking (Go netpollblockcommit).  CAS commit ARMED->
     * PARKED.  If it fails, a pump has already claimed this parker
     * (commit == WOKEN): it recorded readiness into ready_mask and
     * unlinked us but did NOT re-queue (we hadn't committed), so abort
     * the park and return that readiness directly.  This closes the
     * window between the last pending re-check above and the yield
     * below, where a pump on another hub could otherwise wake a parker
     * we were about to park on -- the residual lost-wake. */
    {
        int expc = RUNLOOM_PARK_ARMED;
        if (!__atomic_compare_exchange_n(&park->commit, &expc,
                                         RUNLOOM_PARK_PARKED, 0,
                                         __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            /* expc == WOKEN here (the only state a claimer leaves); no NOTE --
             * this is a FAILED commit, not a transition we performed.  The
             * successful ARMED->PARKED commit is NOTE'd just below. */
            /* expc == WOKEN: claimed by a pump.  ready_mask is set.  The g
             * never committed to parking, so it keeps running; its wake_state
             * stays RUNNING (the pump claimed an ARMED parker and so did NOT
             * call wake_g -- nothing to undo). */
            RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            runloom_parker_unlink(pool, park);   /* no-op if pump unlinked */
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            if (current_g != NULL)
                runloom_g_state_set(current_g, RUNLOOM_GST_RUNNING);
            if (current_g != NULL && current_g->netpoll_parker == park)
                current_g->netpoll_parker = NULL;
            runloom_parker_pool_release(park);
            return ready_mask;
        }
        RUNLOOM_PARK_NOTE(RUNLOOM_PARK_ARMED, RUNLOOM_PARK_PARKED);
    }

    /* Committed to parking: stamp the dwell clock for the hub-idle
     * stack-reclaim sweep (RUNLOOM_STACK_PARK_SWEEP).  Plain store -- only
     * the sweep on this g's owning hub reads it, after this point. */
    park->park_ts = monotonic_ns();

    /* Snapshot tstate (same as runloom_sched_yield does) so the next
     * resume restores it.  Then yield WITHOUT re-queueing -- pump
     * pushes us back when the fd becomes ready. */
    if (current_g != NULL) {
        runloom_sched_park_current();
    }
    runloom_coro_yield();
    /* On wake: pump SHOULD have set ready_mask and removed us from the
     * parked lists.  Defensive unlink covers the case where pump
     * routed the wake via a path that bypassed runloom_parker_unlink. */
    if (park->slot != NULL || park->prev_by_fd != NULL ||
        park->next_by_fd != NULL ||
        (park->fd >= 0 && (size_t)park->fd < pool->by_fd_cap &&
         pool->by_fd[park->fd] == park)) {
#ifdef RUNLOOM_PARKER_DEBUG
        fprintf(stderr,
                "[runloom] wait_fd resumed with parker still linked: "
                "parker=%p gen=%u fd=%d g=%p slot=%p next=%p nbf=%p pbf=%p "
                "bucket=%p\n",
                (void *)park, park->gen, park->fd, (void *)park->g,
                (void *)park->slot, (void *)park->next,
                (void *)park->next_by_fd, (void *)park->prev_by_fd,
                (park->fd >= 0 && (size_t)park->fd < pool->by_fd_cap)
                    ? (void *)pool->by_fd[park->fd] : NULL);
#endif
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        runloom_parker_unlink(pool, park);
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    /* Clear g->netpoll_parker before release so completion's force-
     * unlink cannot dereference a freelist entry. */
    if (current_g != NULL && current_g->netpoll_parker == park) {
        current_g->netpoll_parker = NULL;
    }
    runloom_parker_pool_release(park);
    /* The scheduler handed us a raised Python signal-handler exception (Ctrl-C
     * -> KeyboardInterrupt, an alarm handler, ...): it ran the handler in its
     * own tstate, stashed the raised exception on our owner scheduler, and woke
     * us with this sentinel (runloom_netpoll_signal_wake).  Restore it into THIS
     * fiber's tstate and return -1 so it propagates out of the cooperative
     * blocking call (recv/accept/select/...) through this fiber's own stack
     * -- where its try/except sees it -- instead of out of run().  Callers treat
     * a -1 with a Python error already set as "propagate", not OSError. */
    if (ready_mask == RUNLOOM_NETPOLL_SIGNALED) {
        runloom_sched_t *owner = (current_g != NULL) ? current_g->owner : s;
        PyObject *exc = (owner != NULL) ? owner->signal_exc : NULL;
        if (owner != NULL) owner->signal_exc = NULL;
        if (exc != NULL) {
            PyObject *tb = PyException_GetTraceback(exc);   /* new ref or NULL */
            /* PyErr_Restore steals all three refs. */
            PyErr_Restore((PyObject *)Py_NewRef(Py_TYPE(exc)), exc, tb);
        }
        return -1;
    }
    return ready_mask;
}

/* wait_fd variant for the cooperative SOCKET fast paths (monkey tcp_recv/send,
 * the TCPConn methods, module_tcp.c.inc): translate a cancellation into
 * errno=ECANCELED + return -1 so a caller that tests "(wait_fd < 0) -> raise
 * OSError" unwinds the parked op.  The raw runloom_netpoll_wait_fd returns the
 * POSITIVE RUNLOOM_NETPOLL_CANCELLED sentinel, which slips a "< 0" test -- the
 * op then retries on the still-OPEN socket and RE-PARKS (a hang on
 * cancel_all_parked / a cross-fiber close of an open fd; surfaced by
 * test_kqueue_cancel::test_monkey_recv_cancel_all_open_socket).  The aio bridge
 * keeps using the raw wait_fd so it maps the sentinel to its own CancelledError.
 * Audit finding B3. */
int runloom_netpoll_wait_fd_coop(int fd, int events, long long timeout_ns)
{
    int r = runloom_netpoll_wait_fd(fd, events, timeout_ns);
    if (r == RUNLOOM_NETPOLL_CANCELLED) { errno = ECANCELED; return -1; }
    return r;
}
