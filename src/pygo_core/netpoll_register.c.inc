/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- registration ----
 * Edge-triggered, register-once.  On epoll/kqueue the fd is ADDed
 * exactly once with both READ and WRITE arms in ET mode.  All
 * subsequent wait_fd calls just consult the bitmap and skip the
 * epoll_ctl/kevent syscall -- the kernel keeps reporting edges
 * until the fd closes.
 *
 * Safety: the caller MUST try the operation first and only call
 * wait_fd after EAGAIN.  That serialises the "kernel observed not-
 * ready" state with our parking, so the next not-ready->ready
 * transition is guaranteed to deliver an edge.  This is the same
 * pattern Go's netpoll uses.
 *
 * Stale-fd recovery: socket close auto-clears the kernel
 * registration when the last fd reference goes away.  monkey.py's
 * close hook calls pygo_netpoll_unregister so the bitmap stays in
 * sync with the kernel for fd reuse. */
static int pygo_netpoll_register(int fd, int events)
{
#if defined(PYGO_HAVE_EPOLL)
    int need_register;
    struct epoll_event ev;

    /* T1.5 lost-wake fix: arm ONLY the requested direction,
     * LEVEL-triggered + EPOLLONESHOT, re-arming on every park.
     *
     * The old scheme (register once, both arms, EPOLLET|EPOLLEXCLUSIVE,
     * never re-armed) lost readiness wakeups under concurrency -- a g
     * could sit parked on READ forever with its socket Recv-Q > 0
     * (data waiting), all hubs idle.  Two mechanisms:
     *   - EPOLLEXCLUSIVE + EPOLLET drops an edge whose single exclusive
     *     wakeup lands on a hub not currently inside epoll_wait;
     *   - EPOLLET never refires once an edge is missed, and register-once
     *     never re-armed, so the parker was never woken.
     * The cure, which is also the standard multi-threaded epoll pattern:
     *   - LEVEL-triggered: EPOLL_CTL_MOD re-evaluates readiness and
     *     reports an fd ready *now*, so data that arrived before the
     *     parker linked is delivered (EPOLLET would NOT re-report it --
     *     verified: EPOLLET+ONESHOT+re-arm hung 96/96);
     *   - EPOLLONESHOT: exactly one delivery per arm, so no thundering
     *     herd (EPOLLEXCLUSIVE unneeded) and the always-ready OUT side
     *     can't busy-loop the pump;
     *   - per-DIRECTION arming: with both IN+OUT one-shot, the always-
     *     writable OUT would consume the single delivery before a READ
     *     waiter's data arrived -- so arm exactly what this wait needs.
     * Cost: one epoll_ctl per park (the bitmap now only distinguishes
     * ADD from MOD).  Correctness over the saved syscall. */
    ev.events = EPOLLONESHOT;
    if (events & PYGO_NETPOLL_READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (events & PYGO_NETPOLL_WRITE) ev.events |= EPOLLOUT;
    ev.data.fd = fd;

    pygo_mutex_lock(&pygo_pool.lock);
    need_register = !pygo_fd_bit_get(fd);
    if (need_register && pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    pygo_mutex_unlock(&pygo_pool.lock);

    if (need_register) {
        if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
        /* Stale registration (dup'd fd / missed close-hook): re-arm. */
        if (errno == EEXIST &&
            epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
    } else {
        /* Re-arm the one-shot for this park; MOD re-checks readiness. */
        if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
        /* ENOENT = fd dropped from epoll (close/reuse race); ADD. */
        if (errno == ENOENT &&
            epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
    }
    /* Failed: drop the bit so a future caller can retry. */
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    /* Arm ONLY the requested direction(s), ONE-SHOT (NOT EV_CLEAR), re-arming
     * on EVERY park -- the kqueue counterpart of the epoll EPOLLONESHOT re-arm
     * fix above.  The old scheme (register-once + EV_CLEAR, and SKIP the kevent
     * when the per-fd bit was already set) lost wakeups two ways, both caught by
     * test_netpoll_conformance's fd-reuse case:
     *   - EV_CLEAR is edge-triggered: an edge missed in the window between
     *     EV_ADD and the next kevent() never refired (same trap as epoll's old
     *     EPOLLET register-once);
     *   - a reused fd NUMBER whose close-hook had not run (raw socket.close(),
     *     no pygo wrapper) kept its bitmap bit set, so the second register
     *     RETURNED EARLY without issuing the kevent -> the new socket was never
     *     armed and its reader parked forever (timed out via the deadline, or
     *     hung outright with no timeout).
     * EV_ADD re-checks readiness NOW (kqueue reports a level-ready fd at add
     * time, so data that arrived before the parker linked is still delivered),
     * and EV_ONESHOT delivers once then auto-disables (no thundering herd; the
     * always-writable OUT can't busy-loop the pump).  The bitmap is still kept
     * in sync for unregister, but no longer gates the kevent. */
    pygo_mutex_lock(&pygo_pool.lock);
    if (pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    pygo_mutex_unlock(&pygo_pool.lock);
    {
        struct kevent kev[2];
        int n = 0;
        if (events & PYGO_NETPOLL_READ)
            EV_SET(&kev[n++], fd, EVFILT_READ,  EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (events & PYGO_NETPOLL_WRITE)
            EV_SET(&kev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (n == 0) return 0;
        {   /* test-only: force the registration kevent() to fail; the error
             * surfaces as OSError(errno) to the goroutine parking on this fd
             * (the fd-bit is rolled back below). */
            int finj = pygo_fault_inject(PYGO_FAULT_KQUEUE_CTL);
            if (finj) errno = finj;
            else if (kevent(pygo_kqueue_fd, kev, n, NULL, 0, NULL) == 0) return 0;
        }
    }
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    return -1;
#else
    (void)fd;
    return 0;  /* select doesn't need pre-registration */
#endif
}

void pygo_netpoll_unregister(int fd)
{
#if defined(PYGO_HAVE_EPOLL) || defined(PYGO_HAVE_KQUEUE)
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    /* No kernel syscall: epoll/kqueue auto-remove the fd when the
     * last reference closes.  Calling EPOLL_CTL_DEL after close
     * would race with fd reuse anyway. */
#else
    (void)fd;
#endif
}

int pygo_netpoll_parked_count(void)
{
    int total = 0;
    int i;
    for (i = 0; i < PYGO_PARKER_POOL_MAX; i++) {
        total += __atomic_load_n(&pygo_pools[i].total, __ATOMIC_ACQUIRE);
    }
    return total;
}

/* Hub-idle dwell-based stack reclaim (PYGO_STACK_PARK_SWEEP).  Walk the
 * CALLING hub's parker pool and madvise the below-SP idle stack pages of
 * goroutines whose current park has already exceeded threshold_ns.
 *
 * SAFE under M:N: every parker in pool[hub] is owned by `hub` (wakes
 * route to p->hub and land in that hub's non-stealable local FIFO, so
 * `hub` is the sole resumer).  The caller IS that hub and is idle (not
 * resuming) here, so nothing runs on the stacks we madvise; a concurrent
 * pump on another hub may unlink+re-queue a parker but never touches its
 * stack, and a re-queued g only waits in this hub's FIFO until this
 * sweep returns -- it cannot run, complete, or be freed meanwhile.  We
 * snapshot candidate g's under the pool lock (marking them reclaimed),
 * then madvise OUTSIDE the lock so the syscall doesn't stall pumps that
 * search this pool.  Bounded to PYGO_SWEEP_BATCH per call; the next idle
 * sweep takes the rest.  Returns the number of stacks reclaimed. */
#define PYGO_SWEEP_BATCH 128       /* max stacks madvised per sweep call */
#define PYGO_SWEEP_MAX_VISIT 1024  /* max parkers examined per sweep (lock-hold bound) */
/* Churn throttle default: skip the sweep when this pool sees more than
 * this many parker arrivals/sec (per hub).  Calibrated so a genuinely
 * idle keepalive workload (the N=1M target: parkers dwelling seconds,
 * low arrival rate) always sweeps and reclaims, while an active-churn
 * workload (sub-second per-connection cycling, high arrival rate) skips
 * -- degrading the sweep to a no-op rather than paying lock contention.
 * Override with PYGO_SWEEP_MAX_CHURN (0 disables the throttle).
 *
 * 600 calibrated on the N=65K keepalive bench (H=16): the idle target
 * (think=10s, ~250 arrivals/s/hub) sweeps FULLY at 600 -- RSS 3419 MB
 * (full -32% win), p99 50 ms (the OFF floor, zero cost) -- while the
 * active-churn case (think=2s, ~1200/s/hub) throttles (RSS 4929 ~= OFF,
 * sweep degrades to a no-op).  600 sits cleanly between the two rates. */
#define PYGO_SWEEP_DEFAULT_MAX_CHURN 600LL
int pygo_netpoll_sweep_idle(void *hub_opaque, long long threshold_ns)
{
    /* No backend #if here: the actual madvise lives in
     * pygo_coro_madvise_idle (coro.c), which no-ops on backends without
     * an inspectable SP / MADV_DONTNEED.  netpoll.c does NOT define
     * MADV_DONTNEED (no <sys/mman.h>), so gating this body on it -- as an
     * earlier draft did -- silently compiled the whole sweep away. */
    pygo_parker_pool_t *pool = pygo_parker_pool_for_hub(hub_opaque);
    pygo_g_t *batch[PYGO_SWEEP_BATCH];
    int n = 0, i, visited = 0;
    int per_g = pygo_get_per_g_tstate_mode();
    long long now;
    if (pool == NULL ||
        __atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) {
        return 0;
    }
    now = monotonic_ns();
    pygo_mutex_lock(&pool->lock);
    /* Churn throttle.  Compute this pool's arrival rate since the last
     * sweep eval; if it exceeds PYGO_SWEEP_MAX_CHURN links/sec, skip the
     * walk entirely.  Rationale (perf-confirmed): the active-churn tail
     * cost is the sweep's lock-hold competing with the wake path on
     * pool->lock, not refault; and a fast-churning pool's parkers aren't
     * long-idle, so there's little to reclaim.  Skipping is always safe
     * -- a not-yet-reclaimed stack just stays resident a bit longer.  We
     * still update the churn baseline so the rate window stays recent. */
    {
        static long long max_churn = -1;     /* links/sec; -1 = read env once */
        long long mc = __atomic_load_n(&max_churn, __ATOMIC_RELAXED);
        if (mc < 0) {
            const char *e = getenv("PYGO_SWEEP_MAX_CHURN");
            mc = (e != NULL) ? atoll(e) : PYGO_SWEEP_DEFAULT_MAX_CHURN;
            if (mc < 0) mc = 0;
            __atomic_store_n(&max_churn, mc, __ATOMIC_RELAXED);
        }
        if (mc > 0) {
            unsigned long long links = pool->link_count;
            long long dt = now - pool->churn_last_ns;
            int throttle = 0;
            if (pool->churn_last_ns != 0 && dt > 0) {
                unsigned long long arrivals = links - pool->churn_last_links;
                /* arrivals/dt[s] >= mc  <=>  arrivals*1e9 >= mc*dt  (ns) */
                if (arrivals * 1000000000ULL >=
                    (unsigned long long)mc * (unsigned long long)dt) {
                    throttle = 1;
                }
            }
            pool->churn_last_links = links;
            pool->churn_last_ns = now;
            if (throttle) {
                pygo_mutex_unlock(&pool->lock);
                return 0;
            }
        }
    }
    {
        /* Resume from the saved cursor; reset to head if it was unlinked
         * and its slot reused (gen changed) or we ran off the end last
         * time.  The whole walk runs under the lock, so the list is
         * stable here; only between sweeps can the cursor go stale, which
         * the gen check catches.  Walking off the live chain into pooled-
         * but-unlinked parkers is memory-safe (parkers are pool-recycled,
         * never freed) and action-safe (the commit==PARKED + dwell gate
         * skips woken/young ones); MAX_VISIT bounds it either way. */
        pygo_parked_t *p = pool->sweep_cursor;
        if (p == NULL || p->gen != pool->sweep_cursor_gen) {
            p = pool->head;
        }
        while (p != NULL && n < PYGO_SWEEP_BATCH &&
               visited < PYGO_SWEEP_MAX_VISIT) {
            if (!p->reclaimed && p->park_ts != 0 &&
                p->g != NULL && p->g->coro != NULL &&
                __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE)
                    == PYGO_PARK_PARKED &&
                (now - p->park_ts) >= threshold_ns) {
                /* Default mode: this hub is the g's sole resumer, so marking it
                 * reclaimed and madvising below is race-free.  Per-g-tstate: a
                 * woken g can be stolen by ANY hub, so claim it exclusively
                 * (PARKED->SWEEPING) first; if the claim loses (the g is being
                 * woken/owned) skip it -- never madvise a resumable stack. */
                if (!per_g || pygo_mn_sweep_try_claim(p->g)) {
                    p->reclaimed = 1;
                    batch[n++] = p->g;
                }
            }
            p = p->next;
            visited++;
        }
        pool->sweep_cursor = p;                 /* NULL at end -> next resets to head */
        pool->sweep_cursor_gen = (p != NULL) ? p->gen : 0;
    }
    pygo_mutex_unlock(&pool->lock);
    for (i = 0; i < n; i++) {
        pygo_coro_madvise_idle(batch[i]->coro);
        /* Companion reclaim: drop the parked Python g's idle datastack-
         * chunk tail too (the C-stack madvise above never touches it).
         * Same owning-hub safety contract; gated by PYGO_DATASTACK_SWEEP. */
        pygo_sched_madvise_datastack_idle(batch[i]);
        /* Per-g-tstate: release the exclusive sweep claim now that this g's
         * madvise has completed (SWEEPING->PARKED, or re-enqueue a wake that
         * landed mid-madvise).  Per-g release bounds a woken g's extra latency
         * to its own single madvise, not the whole batch. */
        if (per_g) {
            pygo_mn_sweep_claim_release(batch[i]);
        }
    }
    return n;
}
