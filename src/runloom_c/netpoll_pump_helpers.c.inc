/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Claim a parker for the pump: CAS its commit to WOKEN.  Returns the
 * state we claimed FROM -- RUNLOOM_PARK_PARKED (the g had committed to
 * parking; caller must re-queue it via runloom_mn_wake_g) or
 * RUNLOOM_PARK_ARMED (g not yet parked; caller records readiness + unlinks
 * but must NOT re-queue -- the g's own commit CAS will fail and it aborts
 * the park, returning ready_mask itself) -- or RUNLOOM_PARK_WOKEN if another
 * waker already claimed it (caller must skip: don't touch ready_out,
 * don't unlink, don't wake).  The g's ARMED->PARKED commit is the only
 * competing writer, so the loop runs at most twice.  This is the single
 * source of truth for the Go-netpollblockcommit protocol; every pump
 * backend (epoll/kqueue dispatch, WSAPoll, select) routes through it so
 * the exactly-one-of-{waker,g}-wins guarantee is identical everywhere. */
static inline int runloom_pump_claim(runloom_parked_t *p)
{
    int cur;
    for (;;) {
        cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
        if (cur == RUNLOOM_PARK_WOKEN) break;            /* already claimed */
        if (__atomic_compare_exchange_n(&p->commit, &cur, RUNLOOM_PARK_WOKEN, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            break;
    }
    return cur;
}

/* Walk every parker pool looking for a parker matching (fd, mask).
 * The first match's ready_out gets the mask, the parker is unlinked
 * from its pool, and runloom_mn_wake_g routes the wake to its hub.
 * Returns 1 if a parker was found+woken, 0 otherwise.
 *
 * Lock ordering: pool->lock then hub->sub_lock (inside runloom_mn_wake_g).
 * We never hold pool[H1].lock while taking pool[H2].lock; the for
 * loop drops each pool's lock before moving on.  Same ordering as
 * runloom_pump_drain_expired below so the two are deadlock-free against
 * each other. */
static int runloom_pump_dispatch_event(int fd, int mask, int wake_all)
{
    int pi;
    int woke = 0;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        if ((size_t)fd < pool->by_fd_cap) {
            runloom_parked_t *p = pool->by_fd[fd];
            while (p != NULL) {
                runloom_parked_t *next_p = p->next_by_fd;
                if (p->events & mask) {
                    /* Claim the parker (see runloom_pump_claim). */
                    int cur = runloom_pump_claim(p);
                    if (cur == RUNLOOM_PARK_WOKEN) { p = next_p; continue; }
                    *(p->ready_out) = mask & p->events;
                    runloom_parker_unlink(pool, p);
#ifdef RUNLOOM_PARKER_DEBUG
                    if (p->g != NULL &&
                        __atomic_load_n(&p->g->done, __ATOMIC_ACQUIRE)) {
                        fprintf(stderr,
                            "[runloom] pump waking DEAD g: g=%p done=1 "
                            "refcount=%d parker=%p fd=%d\n",
                            (void *)p->g,
                            (int)__atomic_load_n(&p->g->refcount,
                                                 __ATOMIC_ACQUIRE),
                            (void *)p, fd);
                    }
#endif
                    /* Only re-queue if the g had already committed to
                     * parking.  If we claimed an ARMED parker, the g is
                     * still running and will see WOKEN at its commit CAS,
                     * abort the park, and return ready_mask itself --
                     * re-queueing here would double-resume it. */
                    if (cur == RUNLOOM_PARK_PARKED) {
                        runloom_mn_wake_g(p->hub, p->g);
                    }
                    woke++;
                    /* wake_all (kqueue one-shot path): keep walking this fd's
                     * bucket so EVERY same-direction parker is woken.  The
                     * single (ident,filter) one-shot knote stood in for all of
                     * them and will not re-report, so a first-match-only wake
                     * strands the siblings forever (no re-arm path) -- audit
                     * finding B2.  The level-triggered epoll/IOCP pumps pass
                     * wake_all=0 and keep first-match: they re-report a
                     * still-ready fd until each waiter is served. */
                    if (!wake_all) {
                        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
                        return 1;
                    }
                }
                p = next_p;
            }
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return woke;
}

/* Drain expired-deadline parkers from every pool.  Returns count
 * woken.  Each parker is unlinked, gets ready_out=0 (timeout), and
 * its g is routed back via runloom_mn_wake_g / runloom_sched_wake.
 * Wakes happen inside the pool lock; lock ordering matches
 * runloom_pump_dispatch_event so the two are deadlock-free. */
static int runloom_pump_drain_expired(long long now)
{
    int woke = 0;
    int pi;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        while (pool->dh_size > 0 && pool->dh_arr[0]->deadline_ns <= now) {
            runloom_parked_t *p = pool->dh_arr[0];
            int cur;
            /* Claim like the pump so a g mid-commit doesn't both park and
             * get timeout-woken. */
            for (;;) {
                cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
                if (cur == RUNLOOM_PARK_WOKEN) break;
                if (__atomic_compare_exchange_n(&p->commit, &cur,
                                                RUNLOOM_PARK_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE))
                    break;
            }
            runloom_parker_unlink(pool, p);   /* always drop from the heap */
            if (cur != RUNLOOM_PARK_WOKEN) {
                if (p->ready_out != NULL) *p->ready_out = 0;   /* timeout */
                /* Re-queue only a committed g; an ARMED g aborts itself. */
                if (cur == RUNLOOM_PARK_PARKED) {
                    if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
                    else                runloom_sched_wake(p->g);
                }
                woke++;
                RUNLOOM_EVT(RUNLOOM_EVT_PARKER_TIMEOUT, p, p->g, (long long)p->fd);
            }
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return woke;
}

/* Min deadline across all pools.  Returns -1 if no pool has any
 * timed parker.  Takes each pool's lock briefly. */
static long long runloom_dh_peek_deadline_global(void)
{
    long long earliest = -1;
    int pi;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        long long d;
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        d = runloom_dh_peek_deadline(pool);
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        if (d >= 0 && (earliest < 0 || d < earliest)) earliest = d;
    }
    return earliest;
}

/* Recover from a failed blocking wait (epoll_wait / kevent).
 *
 * EINTR is benign: a signal interrupted the wait, the caller re-pumps, and the
 * next epoll_wait/kevent blocks normally -- no spin, nothing to do.
 *
 * Any OTHER errno is "impossible": the epoll/kqueue fd is owned by netpoll,
 * created once in runloom_netpoll_init and only closed in runloom_netpoll_fini.  The
 * ways it can still happen are a teardown race (fd closed under a pump on
 * another thread -> EBADF) or corruption (EINVAL/EFAULT).  The danger is that
 * the error returns WITHOUT blocking, so the scheduler's idle loop -- which
 * re-pumps the instant pump() returns 0 with parkers still outstanding -- turns
 * into a 100% CPU busy-spin that can never make progress (the same trap the
 * sub-ms-timeout rounding above guards against, but driven by a hard error
 * instead of a 0 timeout).  libuv abort()s here; we can't kill the embedding
 * interpreter, so instead we log once and back off a millisecond, degrading a
 * silent CPU peg into an observable, throttled slow-loop with a logged cause.
 * Returns 1 if it handled an error (caller should not treat n as a count). */
#if !defined(RUNLOOM_OS_WINDOWS)   /* epoll, kqueue, AND the POSIX select fallback */
static int runloom_netpoll_wait_failed(int n)
{
    static int warned;            /* relaxed: a few duplicate logs are harmless */
    static unsigned eintr_run;    /* consecutive EINTRs (signal-storm throttle) */
    int err = errno;
    if (n >= 0) return 0;                        /* not an error (defensive) */
    if (err == EINTR) {
        /* EINTR is transient -> retry immediately (POSIX / libuv convention;
         * the common case is a preemption signal interrupting an idle
         * epoll_wait/kevent).  But a PERSISTENT EINTR storm (a signal flood, or
         * the synthetic always:EINTR fault) would busy-spin the idle pump at
         * 100% CPU until a deadline fires -- surfaced by test_kqueue_faults_ext.
         * After a long run of CONSECUTIVE EINTRs, fall through to the same 1 ms
         * backoff the hard-error path uses, bounding the spin into a throttled
         * slow-loop without penalizing ordinary transient EINTR (the threshold
         * is far above any normal burst; relaxed counter -- approximate is fine
         * for a throttle, and a multi-hub storm just trips it sooner). */
        if (__atomic_add_fetch(&eintr_run, 1, __ATOMIC_RELAXED) < 64u)
            return 1;
        __atomic_store_n(&eintr_run, 0, __ATOMIC_RELAXED);
    } else {
        __atomic_store_n(&eintr_run, 0, __ATOMIC_RELAXED);
        if (!__atomic_exchange_n(&warned, 1, __ATOMIC_RELAXED)) {
            fprintf(stderr,
                    "[runloom] netpoll pump: blocking wait failed (errno=%d); "
                    "backing off to avoid a busy-spin -- the poll fd is likely "
                    "closed (teardown race) or corrupt\n", err);
        }
    }
    {
        struct timespec backoff = { 0, 1000000L };   /* 1 ms */
        Py_BEGIN_ALLOW_THREADS
        nanosleep(&backoff, NULL);
        Py_END_ALLOW_THREADS
    }
    return 1;
}
#endif /* !RUNLOOM_OS_WINDOWS */
