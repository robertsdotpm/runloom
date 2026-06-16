/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


void runloom_netpoll_force_unlink_g_parker(runloom_g_t *g)
{
    runloom_parked_t *p;
    runloom_parker_pool_t *pool;
    if (g == NULL) return;
    /* Cheap path: no parker tracked, nothing to do. */
    if (g->netpoll_parker == NULL) return;
    p = (runloom_parked_t *)g->netpoll_parker;
    /* Route via the parker's recorded hub -- pool ownership is set
     * once at link time (wait_fd's runloom_parker_pool_for_hub) and the
     * parker stays in that pool for its whole lifetime, so reading
     * p->hub is sufficient to find the right lock here even though
     * we're not in the parker's owning hub. */
    pool = runloom_parker_pool_for_hub(p->hub);
    RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    /* Re-read p inside the lock in case g->netpoll_parker was
     * cleared by a concurrent unlink between the check above and
     * the lock acquire. */
    p = (runloom_parked_t *)g->netpoll_parker;
    if (p != NULL) {
        /* runloom_parker_unlink is no-op if p is already clean (e.g.,
         * pump unlinked it between the cheap-path check and the lock
         * acquire) -- safe to call unconditionally. */
        (void)runloom_parker_unlink(pool, p);
        g->netpoll_parker = NULL;
        RUNLOOM_EVT(RUNLOOM_EVT_PARKER_FORCE, p, g, (long long)p->gen);
    }
    RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    /* The g is completing and its wait_fd will never resume to call
     * pool_release, so we release here.  Safe: parker is unlinked
     * above so no other thread holds a reference. */
    if (p != NULL) runloom_parker_pool_release(p);
}

/* Lock the parker pool that currently owns g's wait_fd parker, defending against
 * RE-PARK MIGRATION.  g->netpoll_parker can change to a parker on a DIFFERENT hub
 * (hence a different pool) between the lock-free hub read and the lock acquire --
 * if the g is woken by another claimer and re-parks elsewhere in that window.
 * cancel_g and unpark_many both wake a g that WILL resume and may re-park, so
 * they must re-validate the pool under the lock; the pump/deadline sweep reach a
 * parker by walking the pool's OWN structures (so they always hold the right
 * lock), and force_unlink acts on a COMPLETING g (which never re-parks) -- only
 * these two need this guard.
 *
 * On success returns 1 holding *pool_out->lock with *p_out == g->netpoll_parker
 * living in *pool_out, so the caller has exclusive UNLINK rights on it (a
 * concurrent pump/timeout claimer can still CAS its commit lock-free, which the
 * caller's own claim CAS then loses cleanly -> exactly-once preserved).  Returns
 * 0 with NO lock held if g is not parked (parker NULL: not yet committed the
 * park, or already woken+unlinked). */
static int runloom_netpoll_lock_g_pool(runloom_g_t *g,
                                       runloom_parked_t **p_out,
                                       runloom_parker_pool_t **pool_out)
{
    for (;;) {
        runloom_parked_t *p = (runloom_parked_t *)g->netpoll_parker;
        runloom_parker_pool_t *pool;
        if (p == NULL) return 0;
        pool = runloom_parker_pool_for_hub(p->hub);
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        /* Re-read under the lock: a pump/timeout/force-unlink may have cleared
         * it, OR the g may have re-parked on a different pool. */
        p = (runloom_parked_t *)g->netpoll_parker;
        if (p == NULL) { RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL); return 0; }
        if (runloom_parker_pool_for_hub(p->hub) != pool) {
            /* g re-parked on a different pool in the lock-acquire window; the
             * lock we hold is the WRONG one -- drop it and retry with the fresh
             * pool.  Bounded: a re-park needs a prior wake, and once our claim
             * below wins, no further re-wake/re-park can occur. */
            RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
            continue;
        }
        *p_out = p;
        *pool_out = pool;
        return 1;
    }
}

/* Cancel a fiber parked in runloom_netpoll_wait_fd.  Unlike force_unlink
 * (g-completion: unlink + release, no wake), the g here WILL resume in wait_fd,
 * so we WAKE it instead of releasing -- wait_fd's own resume path releases the
 * parker.  We claim via the same commit CAS the pump uses, so the
 * {pump fd-ready, timeout sweep, cancel} race resolves to exactly one winner;
 * the loser observes WOKEN and leaves ready_out untouched. */
int runloom_netpoll_cancel_g(runloom_g_t *g)
{
    runloom_parked_t *p;
    runloom_parker_pool_t *pool;
    int cur, woke = 0;
    if (g == NULL) return 0;
    if (g->netpoll_parker == NULL) return 0;   /* cheap path: not parked */
    if (!runloom_netpoll_lock_g_pool(g, &p, &pool)) return 0;
    /* holds pool->lock; p == g->netpoll_parker, living in pool */
    for (;;) {
        cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
        if (cur == RUNLOOM_PARK_WOKEN) break;       /* pump/timeout won */
        if (__atomic_compare_exchange_n(&p->commit, &cur, RUNLOOM_PARK_WOKEN, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            break;
    }
    if (cur != RUNLOOM_PARK_WOKEN) {
        if (p->ready_out != NULL) *p->ready_out = RUNLOOM_NETPOLL_CANCELLED;
        runloom_parker_unlink(pool, p);   /* clears g->netpoll_parker */
        /* Re-queue only a committed (PARKED) g; an ARMED g hasn't yielded
         * yet -- it sees WOKEN at its own commit CAS, aborts the park, and
         * returns RUNLOOM_NETPOLL_CANCELLED itself (re-queueing would double-
         * resume it). */
        if (cur == RUNLOOM_PARK_PARKED) {
            if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
            else                runloom_sched_wake(p->g);
        }
        woke = 1;
        RUNLOOM_EVT(RUNLOOM_EVT_PARKER_FORCE, p, p->g, (long long)p->fd);
    }
    RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    return woke;
}

/* Directly wake a BATCH of fibers parked in wait_fd -- the wake side of a
 * fan-in primitive (Event.set / Condition.notify_all / Semaphore.release).  Each
 * g is claimed + ready_out=UNPARKED + unlinked + re-queued, identical to cancel_g
 * (UNPARKED instead of CANCELLED), one g at a time under the right pool lock via
 * the re-validating lock helper.  A g whose parker is NULL -- it appended itself
 * to the primitive's wait list but has not yet committed the wait_fd park (the
 * edge-before-park window), or it was already woken+unlinked -- cannot be
 * direct-woken; its index goes into missed_out so the caller falls back to the
 * pipe-write backstop (a stray byte for the already-woken sub-case is harmless:
 * drained on parker release / discarded by the wait_fd poll() re-check).  This
 * replaces N os.write syscalls + N epoll edges + N pump drains with N in-process
 * claims.  Returns the count of missed indices written to missed_out[0..ret). */
int runloom_netpoll_unpark_many(runloom_g_t **gs, int n, int *missed_out)
{
    int i, missed = 0;
    for (i = 0; i < n; i++) {
        runloom_g_t *g = gs[i];
        runloom_parked_t *p;
        runloom_parker_pool_t *pool;
        int cur;
        if (g == NULL) continue;               /* skipped, not missed */
        if (g->netpoll_parker == NULL) {        /* cheap path: not parked yet */
            missed_out[missed++] = i;
            continue;
        }
        if (!runloom_netpoll_lock_g_pool(g, &p, &pool)) {
            /* parker raced to NULL -> edge-before-park backstop via pipe-write. */
            missed_out[missed++] = i;
            continue;
        }
        /* holds pool->lock; p == g->netpoll_parker, living in pool */
        for (;;) {
            cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
            if (cur == RUNLOOM_PARK_WOKEN) break;       /* pump/timeout/cancel won */
            if (__atomic_compare_exchange_n(&p->commit, &cur, RUNLOOM_PARK_WOKEN, 0,
                                            __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
                break;
        }
        if (cur != RUNLOOM_PARK_WOKEN) {
            if (p->ready_out != NULL) *p->ready_out = RUNLOOM_NETPOLL_UNPARKED;
            runloom_parker_unlink(pool, p);   /* clears g->netpoll_parker */
            /* Re-queue only a committed (PARKED) g; an ARMED g hasn't yielded --
             * waking it would double-resume.  It sees WOKEN at its own commit CAS,
             * aborts the park, and returns RUNLOOM_NETPOLL_UNPARKED itself. */
            if (cur == RUNLOOM_PARK_PARKED) {
                if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
                else                runloom_sched_wake(p->g);
            }
            RUNLOOM_EVT(RUNLOOM_EVT_PARKER_FORCE, p, p->g, (long long)p->fd);
        }
        /* cur==WOKEN: lost the claim to another waker -> the g is already being
         * resumed by that winner; NOT missed (a pipe-write would be a stray byte
         * we do not want). */
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return missed;
}

/* Cancel EVERY fiber parked in runloom_netpoll_wait_fd on `fd` -- used by
 * the socket close hook so a fiber parked in accept()/recv()/connect() on a
 * socket that ANOTHER fiber closes is woken instead of stranded forever
 * (BUG #5: epoll silently drops a closed fd, so the parked waiter never gets an
 * event).  Each woken waiter returns RUNLOOM_NETPOLL_CANCELLED and the monkey
 * socket op retries on the now-closed (fileno == -1) socket, getting EBADF.
 * Same per-parker claim/unlink/wake as runloom_netpoll_cancel_g, walked over the
 * pool's per-fd bucket; we save next_by_fd before unlink since the cancel
 * removes the parker from that chain. */
void runloom_netpoll_cancel_fd(int fd)
{
    int i;
    if (fd < 0) return;
#if defined(RUNLOOM_OS_WINDOWS)
    /* IOCP-AFD: every wait_fd submits an AFD_POLL whose mask ALWAYS includes
     * AFD_POLL_LOCAL_CLOSE (see runloom_to_afd_events), so closing the socket
     * itself fires a completion that the pump dispatches to wake the parked
     * fiber.  Calling cancel_fd from the close hook on IOCP therefore adds a
     * SECOND, concurrent waker for the same parker -- it races the LOCAL_CLOSE
     * completion drained on another hub (both claim+unlink+free the same
     * stack-allocated parker) -> use-after-free / access violation under M:N
     * free-threading (crashed every big_100 socket program at scale; POSIX has
     * no such auto-wake on close so cancel_fd is the sole, race-free waker
     * there).  Skip it on IOCP; LOCAL_CLOSE is the race-free close-waker. */
    if (runloom_win_use_iocp) return;
#endif
    for (i = 0; i < RUNLOOM_PARKER_POOL_MAX; i++) {
        runloom_parker_pool_t *pool = &runloom_pools[i];
        runloom_parked_t *p, *next;
        /* Skip pools whose lock was never initialized.  An unused / not-yet-up
         * pool slot holds a ZERO-initialized mutex; locking that is a benign
         * no-op on POSIX (a zeroed pthread_mutex_t is valid) but a hard access
         * violation inside EnterCriticalSection on Windows.  This fires when a
         * socket is closed EARLY -- e.g. in a project's setup(), before every
         * parker pool is brought up -- through the monkey close hook while
         * runloom_win_use_iocp is still 0 (backend selection not finished), so
         * the IOCP early-return above is skipped and we reach this loop (the p08
         * setup AV).  Matches the guard runloom_netpoll_cancel_all_parked and
         * the netpoll pump helpers already use. */
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2)
            continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        if ((size_t)fd < pool->by_fd_cap) {
            p = pool->by_fd[fd];
            while (p != NULL) {
                int cur;
                next = p->next_by_fd;           /* save: cancel unlinks p */
                for (;;) {
                    cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
                    if (cur == RUNLOOM_PARK_WOKEN) break;
                    if (__atomic_compare_exchange_n(&p->commit, &cur,
                            RUNLOOM_PARK_WOKEN, 0,
                            __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
                        break;
                }
                if (cur != RUNLOOM_PARK_WOKEN) {
                    if (p->ready_out != NULL)
                        *p->ready_out = RUNLOOM_NETPOLL_CANCELLED;
                    runloom_parker_unlink(pool, p);
                    if (cur == RUNLOOM_PARK_PARKED) {
                        if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
                        else                runloom_sched_wake(p->g);
                    }
                    RUNLOOM_EVT(RUNLOOM_EVT_PARKER_FORCE, p, p->g,
                                (long long)p->fd);
                }
                p = next;
            }
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
}

/* Cancel EVERY fiber parked on ANY fd, across all pools -- a teardown backstop
 * (audit finding B3).  After the normal shutdown (close registered listeners +
 * drain grace), a fiber still parked in recv()/connect() on an IDLE-but-OPEN
 * socket will never make progress (the run is ending), yet mn_run joins on the
 * pending goroutine count -- so even one such fiber wedges the whole run (the
 * macOS teardown-tail hangs; the parker dump shows readyParked=0 because the fd
 * genuinely has no data and was never closed).  Force-wake each with
 * RUNLOOM_NETPOLL_CANCELLED; the cooperative op observes the cancel and unwinds
 * (the C fast paths via runloom_netpoll_wait_fd_coop -> OSError(ECANCELED); the
 * aio bridge -> CancelledError).  Same per-parker claim/unlink/wake as
 * runloom_netpoll_cancel_fd, walked over every by_fd bucket of every pool.
 * Returns the number of parkers cancelled.  Idempotent and cheap when nothing
 * is parked (the common clean-drain case). */
int runloom_netpoll_cancel_all_parked(void)
{
    int i;
    int n = 0;
    for (i = 0; i < RUNLOOM_PARKER_POOL_MAX; i++) {
        runloom_parker_pool_t *pool = &runloom_pools[i];
        size_t f;
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        for (f = 0; f < pool->by_fd_cap; f++) {
            runloom_parked_t *p = pool->by_fd[f];
            while (p != NULL) {
                int cur;
                runloom_parked_t *next = p->next_by_fd;   /* save: cancel unlinks p */
                for (;;) {
                    cur = __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE);
                    if (cur == RUNLOOM_PARK_WOKEN) break;
                    if (__atomic_compare_exchange_n(&p->commit, &cur,
                            RUNLOOM_PARK_WOKEN, 0,
                            __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
                        break;
                }
                if (cur != RUNLOOM_PARK_WOKEN) {
                    if (p->ready_out != NULL)
                        *p->ready_out = RUNLOOM_NETPOLL_CANCELLED;
                    runloom_parker_unlink(pool, p);
                    if (cur == RUNLOOM_PARK_PARKED) {
                        if (p->hub != NULL) runloom_mn_wake_g(p->hub, p->g);
                        else                runloom_sched_wake(p->g);
                    }
                    RUNLOOM_EVT(RUNLOOM_EVT_PARKER_FORCE, p, p->g, (long long)p->fd);
                    n++;
                }
                p = next;
            }
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
    return n;
}

int runloom_netpoll_add_iouring_eventfd(int fd)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    struct epoll_event ev;
    if (fd < 0) return -1;
    if (runloom_netpoll_init() != 0) return -1;
    /* Idempotent: if the same fd is already registered, skip the
     * epoll_ctl call.  io_uring.c only ever creates one eventfd per
     * process, so this matters only if init runs twice. */
    if (runloom_iouring_eventfd_in_epoll == fd) return 0;
    /* EPOLLEXCLUSIVE: see comment in runloom_netpoll_register. */
    ev.events = EPOLLIN | EPOLLET | EPOLLEXCLUSIVE;
    ev.data.fd = fd;
    if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        if (errno == EINVAL) {
            ev.events = EPOLLIN | EPOLLET;
            if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0
                && errno != EEXIST) return -1;
        } else if (errno != EEXIST) {
            return -1;
        }
    }
    runloom_iouring_eventfd_in_epoll = fd;
    return 0;
#else
    /* No epoll to deliver the io_uring CQE-ready eventfd, so io_uring
     * cannot wake the pump on this build.  Report failure: io_uring.c
     * treats a non-zero return as init failure and disables itself
     * (iouring_available() -> false), so callers use the netpoll path.
     * Normally non-epoll => non-Linux => io_uring.c is #if'd out and
     * this is unreachable; it IS reached under RUNLOOM_FORCE_SELECT on
     * Linux, where reporting unavailable is exactly right. */
    (void)fd;
    return -1;
#endif
}

/* Arm the generic pump-interrupt eventfd (see runloom_pump_wake_fd).  Lazily
 * created and registered LEVEL-triggered in the shared epoll; idempotent.
 * Returns 0 if armed (so callers may rely on runloom_netpoll_wake_pump to
 * wake an idle pump), -1 otherwise (the backend has no such primitive --
 * callers fall back to not offloading on the single-thread scheduler). */
int runloom_netpoll_wake_pump_arm(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    struct epoll_event ev;
    int fd;
    if (runloom_netpoll_init() != 0) return -1;
    if (runloom_perhub_epoll()) {
        /* Per-hub: this hub's wake eventfd is created with its epoll in
         * runloom_pool_backend_create.  Ensure THIS hub's pool is up and report
         * armed iff its eventfd came up -- that is the targeted wake primitive.
         * (The shared runloom_pump_wake_fd below is still created as the hub-less
         * fallback waker, but the per-hub eventfd is the one wake_pump targets.) */
        runloom_parker_pool_t *pool =
            runloom_parker_pool_for_hub(runloom_mn_current_hub_opaque());
        runloom_parker_pool_lock_ensure_inited(pool);
        if (pool->wake_efd < 0) return -1;
        /* fall through: also ensure the shared fallback eventfd exists */
    }
    /* Fast path: already armed.  Acquire-load pairs with the release-store
     * below (and matches the waker's load in runloom_netpoll_wake_pump). */
    if (__atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_ACQUIRE) >= 0) return 0;
    /* Serialise the lazy create under runloom_pool.lock -- the file's convention
     * for once-init globals.  Without it two hub threads arming concurrently
     * each create+register an eventfd and one leaks (registered, never fired),
     * besides the read/write data race TSan flagged. */
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    if (__atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_RELAXED) >= 0) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return 0;
    }
    fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) { RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL); return -1; }
    /* Level-triggered, NOT exclusive: the single-thread scheduler is the
     * only pumper that blocks indefinitely, so there is no thundering
     * herd; the pump drains it on fire to clear the level. */
    ev.events  = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0 && errno != EEXIST) {
        close(fd);
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return -1;
    }
    __atomic_store_n(&runloom_pump_wake_fd, fd, __ATOMIC_RELEASE);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    return 0;
#elif defined(RUNLOOM_OS_WINDOWS)
    /* IOCP: PostQueuedCompletionStatus wakes an idle
     * GetQueuedCompletionStatus, so no per-arm primitive is needed beyond
     * the IOCP existing (created in runloom_netpoll_init).  The WSAPoll /
     * select pumps don't block on a wakeable object -- they re-poll the
     * parked-fd set on a timeout -- so they expose no pump-wake and
     * single-thread offload callers fall back to inline there.
     *
     * Init FIRST: runloom_win_use_iocp is 0 until backend selection runs, and
     * a blocking-offload caller may arm before any socket I/O has triggered
     * netpoll init (e.g. a fiber that only does runloom.blocking()). */
    if (runloom_netpoll_init() != 0) return -1;
    if (runloom_win_use_iocp && runloom_iocp_wake_armed()) return 0;
    return -1;
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Per-hub kqueue: each pool's self-pipe is created with its kqueue in
     * runloom_pool_backend_create (lock_ensure_inited).  Ensure THIS hub's pool
     * and report armed iff its pipe came up. */
    if (runloom_netpoll_init() != 0) return -1;
    {
        runloom_parker_pool_t *pool =
            runloom_parker_pool_for_hub(runloom_mn_current_hub_opaque());
        runloom_parker_pool_lock_ensure_inited(pool);
        return (pool->selfpipe_r >= 0) ? 0 : -1;
    }
#else
    /* select() fallback: the self-pipe created in runloom_netpoll_init is
     * the wake primitive.  Report armed iff the pipe came up, so the
     * blocking-offload pool offloads (instead of running inline) on the
     * single-thread scheduler too. */
    if (runloom_netpoll_init() != 0) return -1;
    return (__atomic_load_n(&runloom_selfpipe_r, __ATOMIC_ACQUIRE) >= 0) ? 0 : -1;
#endif
}

/* Break an idle pump (epoll_wait / GetQueuedCompletionStatus) from any
 * thread so the scheduler wakes to drain its wake_list.  A no-op if not
 * armed. */
void runloom_netpoll_wake_pump(void *hub_opaque)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (runloom_perhub_epoll()) {
        /* Per-hub: poke ONLY the target hub's wake eventfd so its (and only its)
         * idle epoll_wait breaks -- no cross-hub thundering herd.  NULL routes to
         * the default pool.  Guard on lock_inited==2 (ACQUIRE, pairs with
         * backend_create's publish) so we never read a never-touched pool's
         * still-zero wake_efd.  If the target's eventfd isn't up, fall through to
         * the shared eventfd (nested in every pool epoll) -- correct, just a herd. */
        runloom_parker_pool_t *pool = runloom_parker_pool_for_hub(hub_opaque);
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) == 2 &&
            pool->wake_efd >= 0) {
            uint64_t one = 1;
            ssize_t w = write(pool->wake_efd, &one, sizeof one);
            (void)w;
            return;
        }
    }
    {
        int fd = __atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_ACQUIRE);
        if (fd >= 0) {
            uint64_t one = 1;
            ssize_t w = write(fd, &one, sizeof one);
            (void)w;
        }
    }
#elif defined(RUNLOOM_OS_WINDOWS)
    (void)hub_opaque;                       /* one shared IOCP for all pumps */
    if (runloom_win_use_iocp) {
        runloom_iocp_wake();
    }
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Per-hub kqueue: poke the TARGET hub's pool self-pipe so its (and only its)
     * idle kevent() breaks -- no cross-hub thundering herd.  NULL routes to the
     * default/single-thread pool.  Guard on lock_inited==2 (ACQUIRE, pairs with
     * backend_create's publish) so we never read a never-touched pool's still-
     * zero selfpipe_w and write to fd 0.  No-op when the target pool's pipe isn't
     * up -- that hub re-polls on its bounded pump timeout / periodic self-pump. */
    runloom_parker_pool_t *pool = runloom_parker_pool_for_hub(hub_opaque);
    if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) == 2) {
        int fd = pool->selfpipe_w;
        if (fd >= 0) {
            char b = 1;
            ssize_t w = write(fd, &b, 1);
            (void)w;
        }
    }
#else
    /* select() fallback: poke the self-pipe to break an idle select().
     * write() to a pipe is thread-safe and needs no lock; EAGAIN (pipe
     * already has unread bytes) is fine -- select will still fire. */
    int fd = __atomic_load_n(&runloom_selfpipe_w, __ATOMIC_ACQUIRE);
    (void)hub_opaque;
    if (fd >= 0) {
        char b = 1;
        ssize_t w = write(fd, &b, 1);
        (void)w;
    }
#endif
}

int runloom_netpoll_add_iouring_ring(int eventfd_fd,
                                  struct runloom_iouring_ring *ring)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    struct epoll_event ev;
    int i;
    if (eventfd_fd < 0 || ring == NULL) return -1;
    if (runloom_netpoll_init() != 0) return -1;
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    /* Idempotent: re-registering the same eventfd just updates the
     * ring pointer (cheap; the eventfd is already in epoll). */
    for (i = 0; i < runloom_iouring_ring_count; i++) {
        if (runloom_iouring_ring_efds[i] == eventfd_fd) {
            runloom_iouring_ring_ptrs[i] = ring;
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return 0;
        }
    }
    if (runloom_iouring_ring_count >= RUNLOOM_IOURING_RINGS_MAX) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        errno = ENOSPC;
        return -1;
    }
    runloom_iouring_ring_efds[runloom_iouring_ring_count] = eventfd_fd;
    runloom_iouring_ring_ptrs[runloom_iouring_ring_count] = ring;
    runloom_iouring_ring_count++;
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);

    /* EPOLLEXCLUSIVE: see comment in runloom_netpoll_register.  Kernel
     * wakes exactly one waiter per ring-eventfd hit instead of all
     * hubs racing to drain the same CQ. */
    {
        int rc;
        ev.events = EPOLLIN | EPOLLET | EPOLLEXCLUSIVE;
        ev.data.fd = eventfd_fd;
        rc = epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, eventfd_fd, &ev);
        if (rc < 0 && errno == EINVAL) {
            ev.events = EPOLLIN | EPOLLET;
            rc = epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, eventfd_fd, &ev);
        }
        if (rc < 0 && errno != EEXIST) {
            /* Undo the table insert. */
            RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            for (i = 0; i < runloom_iouring_ring_count; i++) {
                if (runloom_iouring_ring_efds[i] == eventfd_fd) {
                    runloom_iouring_ring_efds[i] =
                        runloom_iouring_ring_efds[runloom_iouring_ring_count - 1];
                    runloom_iouring_ring_ptrs[i] =
                        runloom_iouring_ring_ptrs[runloom_iouring_ring_count - 1];
                    runloom_iouring_ring_count--;
                    break;
                }
            }
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return -1;
        }
    }
    return 0;
#else
    (void)eventfd_fd; (void)ring;
    return 0;
#endif
}

void runloom_netpoll_remove_iouring_ring(int eventfd_fd)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    int i;
    if (eventfd_fd < 0) return;
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    for (i = 0; i < runloom_iouring_ring_count; i++) {
        if (runloom_iouring_ring_efds[i] == eventfd_fd) {
            runloom_iouring_ring_efds[i] =
                runloom_iouring_ring_efds[runloom_iouring_ring_count - 1];
            runloom_iouring_ring_ptrs[i] =
                runloom_iouring_ring_ptrs[runloom_iouring_ring_count - 1];
            runloom_iouring_ring_count--;
            break;
        }
    }
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    /* Caller is about to close the eventfd; epoll auto-removes when
     * the last fd reference is closed, so no EPOLL_CTL_DEL syscall. */
#else
    (void)eventfd_fd;
#endif
}
