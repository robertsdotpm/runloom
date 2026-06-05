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
    runloom_mutex_lock(&pool->lock);
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
    runloom_mutex_unlock(&pool->lock);
    /* The g is completing and its wait_fd will never resume to call
     * pool_release, so we release here.  Safe: parker is unlinked
     * above so no other thread holds a reference. */
    if (p != NULL) runloom_parker_pool_release(p);
}

/* Cancel a goroutine parked in runloom_netpoll_wait_fd.  Unlike force_unlink
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
    if (g->netpoll_parker == NULL) return 0;   /* not parked in wait_fd */
    p = (runloom_parked_t *)g->netpoll_parker;
    pool = runloom_parker_pool_for_hub(p->hub);
    runloom_mutex_lock(&pool->lock);
    /* Re-read under the lock (a pump/timeout/force-unlink may have cleared it
     * between the cheap check and the lock acquire). */
    p = (runloom_parked_t *)g->netpoll_parker;
    if (p != NULL) {
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
    }
    runloom_mutex_unlock(&pool->lock);
    return woke;
}

/* Cancel EVERY goroutine parked in runloom_netpoll_wait_fd on `fd` -- used by
 * the socket close hook so a goroutine parked in accept()/recv()/connect() on a
 * socket that ANOTHER goroutine closes is woken instead of stranded forever
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
    for (i = 0; i < RUNLOOM_PARKER_POOL_MAX; i++) {
        runloom_parker_pool_t *pool = &runloom_pools[i];
        runloom_parked_t *p, *next;
        runloom_mutex_lock(&pool->lock);
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
        runloom_mutex_unlock(&pool->lock);
    }
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
    /* Fast path: already armed.  Acquire-load pairs with the release-store
     * below (and matches the waker's load in runloom_netpoll_wake_pump). */
    if (__atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_ACQUIRE) >= 0) return 0;
    /* Serialise the lazy create under runloom_pool.lock -- the file's convention
     * for once-init globals.  Without it two hub threads arming concurrently
     * each create+register an eventfd and one leaks (registered, never fired),
     * besides the read/write data race TSan flagged. */
    runloom_mutex_lock(&runloom_pool.lock);
    if (__atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_RELAXED) >= 0) {
        runloom_mutex_unlock(&runloom_pool.lock);
        return 0;
    }
    fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) { runloom_mutex_unlock(&runloom_pool.lock); return -1; }
    /* Level-triggered, NOT exclusive: the single-thread scheduler is the
     * only pumper that blocks indefinitely, so there is no thundering
     * herd; the pump drains it on fire to clear the level. */
    ev.events  = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0 && errno != EEXIST) {
        close(fd);
        runloom_mutex_unlock(&runloom_pool.lock);
        return -1;
    }
    __atomic_store_n(&runloom_pump_wake_fd, fd, __ATOMIC_RELEASE);
    runloom_mutex_unlock(&runloom_pool.lock);
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
     * netpoll init (e.g. a goroutine that only does runloom.blocking()). */
    if (runloom_netpoll_init() != 0) return -1;
    if (runloom_win_use_iocp && runloom_iocp_wake_armed()) return 0;
    return -1;
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
void runloom_netpoll_wake_pump(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    int fd = __atomic_load_n(&runloom_pump_wake_fd, __ATOMIC_ACQUIRE);
    if (fd >= 0) {
        uint64_t one = 1;
        ssize_t w = write(fd, &one, sizeof one);
        (void)w;
    }
#elif defined(RUNLOOM_OS_WINDOWS)
    if (runloom_win_use_iocp) {
        runloom_iocp_wake();
    }
#else
    /* select() fallback: poke the self-pipe to break an idle select().
     * write() to a pipe is thread-safe and needs no lock; EAGAIN (pipe
     * already has unread bytes) is fine -- select will still fire. */
    int fd = __atomic_load_n(&runloom_selfpipe_w, __ATOMIC_ACQUIRE);
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
    runloom_mutex_lock(&runloom_pool.lock);
    /* Idempotent: re-registering the same eventfd just updates the
     * ring pointer (cheap; the eventfd is already in epoll). */
    for (i = 0; i < runloom_iouring_ring_count; i++) {
        if (runloom_iouring_ring_efds[i] == eventfd_fd) {
            runloom_iouring_ring_ptrs[i] = ring;
            runloom_mutex_unlock(&runloom_pool.lock);
            return 0;
        }
    }
    if (runloom_iouring_ring_count >= RUNLOOM_IOURING_RINGS_MAX) {
        runloom_mutex_unlock(&runloom_pool.lock);
        errno = ENOSPC;
        return -1;
    }
    runloom_iouring_ring_efds[runloom_iouring_ring_count] = eventfd_fd;
    runloom_iouring_ring_ptrs[runloom_iouring_ring_count] = ring;
    runloom_iouring_ring_count++;
    runloom_mutex_unlock(&runloom_pool.lock);

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
            runloom_mutex_lock(&runloom_pool.lock);
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
            runloom_mutex_unlock(&runloom_pool.lock);
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
    runloom_mutex_lock(&runloom_pool.lock);
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
    runloom_mutex_unlock(&runloom_pool.lock);
    /* Caller is about to close the eventfd; epoll auto-removes when
     * the last fd reference is closed, so no EPOLL_CTL_DEL syscall. */
#else
    (void)eventfd_fd;
#endif
}

int runloom_netpoll_any_iouring_inflight(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    int i, total;
    /* Global ring inflight. */
    total = runloom_iouring_inflight();
    /* Per-hub rings.  Read snapshot under the lock (matching add/remove)
     * but the inflight load itself is atomic so we don't hold the lock
     * over the loop. */
    runloom_mutex_lock(&runloom_pool.lock);
    {
        struct runloom_iouring_ring *snapshot[RUNLOOM_IOURING_RINGS_MAX];
        int n = runloom_iouring_ring_count;
        for (i = 0; i < n; i++) snapshot[i] = runloom_iouring_ring_ptrs[i];
        runloom_mutex_unlock(&runloom_pool.lock);
        for (i = 0; i < n; i++) {
            total += runloom_iouring_ring_inflight(snapshot[i]);
        }
    }
    return total;
#else
    return 0;
#endif
}
