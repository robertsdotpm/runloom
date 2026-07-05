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
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            RUNLOOM_PARK_NOTE(cur, RUNLOOM_PARK_WOKEN);   /* cur in {ARMED,PARKED} */
            break;
        }
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
    /* Visit ONLY the hub pools whose dispatch-mask bit is set for this fd.  The
     * bit is set in runloom_parker_link BEFORE the backend is armed, so any hub
     * pumping an event for fd sees it -- no lost wake.  fm == ~0 means "fall
     * back to all hub pools" (fd beyond the mask array, mask not yet allocated,
     * or genuinely in every pool).  The DEFAULT pool (index 64) is never masked:
     * the `pi < 64` guard lets it through to its lock_inited check below. */
    unsigned long long fm = ~0ULL;
    {
        unsigned long long *m = __atomic_load_n(&runloom_fd_poolmask, __ATOMIC_ACQUIRE);
        if (m != NULL && fd >= 0 && (size_t)fd < runloom_fd_poolmask_cap)
            fm = __atomic_load_n(&m[fd], __ATOMIC_ACQUIRE);
    }
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        if (pi < 64 && fm != ~0ULL && !(fm & (1ULL << pi))) continue;
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
/* Max stale-arm probes serviced per pool per sweep pass.  Bounds the work
 * done between the pool-lock release and the epoll_ctl probes; a burst of
 * simultaneous probe ticks beyond the cap simply waits for the next pump
 * pass (ms away -- the re-keyed survivors still sit at the heap front). */
#define RUNLOOM_STALE_PROBE_BATCH 64

static int runloom_pump_drain_expired(long long now)
{
    int woke = 0;
    int pi;
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
#if defined(RUNLOOM_HAVE_EPOLL)
        int probe_fds[RUNLOOM_STALE_PROBE_BATCH];
        int nprobe = 0;
#endif
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        while (pool->dh_size > 0 && pool->dh_arr[0]->deadline_ns <= now) {
            runloom_parked_t *p = pool->dh_arr[0];
            int cur;
#if defined(RUNLOOM_HAVE_EPOLL)
            if (p->probe_pending) {
                /* Stale-arm probe tick, NOT a timeout (see the parker-struct
                 * comment in netpoll.c).  Re-key the parker back to its real
                 * deadline -- out of the heap entirely for an untimed wait --
                 * WITHOUT claiming, unlinking, or waking it (the commit
                 * protocol is untouched; heap membership under this lock is
                 * only ever ARMED/PARKED, and neither state reads deadline
                 * fields concurrently).  The fd is validated after this
                 * pool's lock drops: runloom_netpoll_validate_arm takes
                 * runloom_pool.lock, same rank as pool->lock, so the two may
                 * never nest.  probe_pending clears here, so each park
                 * episode probes at most once and this loop always makes
                 * forward progress (a re-keyed short wait whose REAL deadline
                 * already passed comes back to the front with the flag clear
                 * and is drained as a genuine timeout in this same pass). */
                int dup = 0, pj;
                for (pj = 0; pj < nprobe; pj++) {   /* per-fd dedupe: N same-fd
                     * probes (N sleepers on one shared fd) need ONE epoll_ctl */
                    if (probe_fds[pj] == p->fd) { dup = 1; break; }
                }
                if (!dup && nprobe >= RUNLOOM_STALE_PROBE_BATCH) break;
                p->probe_pending = 0;
                runloom_dh_remove(pool, p);
                p->deadline_ns = p->user_deadline_ns;
                runloom_dh_insert(pool, p);      /* no-op when untimed (-1) */
                if (!dup) probe_fds[nprobe++] = p->fd;
                continue;
            }
#endif
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
#if defined(RUNLOOM_HAVE_EPOLL)
        /* Service the collected probes with NO pool lock held (validate_arm
         * takes runloom_pool.lock -- same rank).  A HEALED arm (stale cache,
         * fd re-ADDed) needs nothing more: the LEVEL epoll re-reports any
         * present readiness and the normal pump dispatch wakes the parker.
         * A DEAD fd (-1: EBADF/EPERM -- closed without unregister, no
         * pollable reuse) can never produce a kernel event again, so error-
         * wake every parker still on it: dispatch with the full direction
         * mask and wake_all=1 (the parker's op retries, hits EBADF, and the
         * app sees a loud OSError instead of a park to the ceiling --
         * forever, when untimed).  The parker may already be gone (claimed
         * between the re-key and here); dispatch on an empty bucket is a
         * no-op and the probe was still useful (cache hygiene). */
        {
            int i;
            for (i = 0; i < nprobe; i++) {
                if (runloom_netpoll_validate_arm(probe_fds[i]) < 0) {
                    (void)runloom_pump_dispatch_event(probe_fds[i],
                            RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE, 1);
                }
            }
        }
#endif
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
