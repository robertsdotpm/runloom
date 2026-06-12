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
 * close hook calls runloom_netpoll_unregister so the bitmap stays in
 * sync with the kernel for fd reuse. */
static int runloom_netpoll_register(int fd, int events)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    int cur, target, need;
    struct epoll_event ev;

    /* LEVEL register-PER-DIRECTION-once.  Arm each fd LEVEL-triggered (NO
     * EPOLLET, NO EPOLLONESHOT) for whichever directions have an active waiter,
     * and skip epoll_ctl on any re-park whose direction is already armed.
     * Because LEVEL re-reports a still-ready fd on EVERY epoll_wait, a parker
     * that links after an edge -- or that partially consumes and re-parks -- is
     * still delivered (no edge-drop, no pending-bitmap dependency).  The hot
     * path is a READ socket re-parking recv-after-recv: IN stays armed, so it
     * costs ZERO syscalls -- the win over the old LEVEL+EPOLLONESHOT scheme's
     * one epoll_ctl/park.
     *
     * EPOLLONESHOT is gone, so the always-writable OUT could level-busy-loop
     * the pump; OUT is armed only when a WRITE waiter exists (and a long-lived
     * WRITE-heavy fd's lazy OUT-disarm is a documented follow-up -- the READ
     * hot path never arms OUT; short WRITE waits drop their registration on
     * close).  IN never busy-loops (a not-readable socket produces no IN
     * event), so IN is safe register-once.  EPOLLEXCLUSIVE stays OFF.
     *
     * The arm mask (runloom_fd_armed) doubles as registration state: armed==0
     * means "not in the epoll set" (-> ADD), armed!=0 means "already ADDed"
     * (-> MOD to widen the direction set). */
    need = events & (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE);

    runloom_mutex_lock(&runloom_pool.lock);
    cur = runloom_fd_armed_get(fd);
    target = cur | need;
    if (cur != 0 && target == cur) {
        runloom_mutex_unlock(&runloom_pool.lock);
        return 0;                               /* already armed -> 0 syscalls */
    }
    if (runloom_fd_armed_set(fd, target) != 0) {
        runloom_mutex_unlock(&runloom_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    runloom_mutex_unlock(&runloom_pool.lock);

    ev.events = 0;
    if (target & RUNLOOM_NETPOLL_READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (target & RUNLOOM_NETPOLL_WRITE) ev.events |= EPOLLOUT;
    ev.data.fd = fd;

    if (cur == 0) {
        if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
        if (errno == EEXIST &&
            epoll_ctl(runloom_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
    } else {
        if (epoll_ctl(runloom_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
        if (errno == ENOENT &&
            epoll_ctl(runloom_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
    }
    /* Failed: roll the arm mask back so a future caller can retry. */
    runloom_mutex_lock(&runloom_pool.lock);
    runloom_fd_armed_set(fd, cur);
    runloom_mutex_unlock(&runloom_pool.lock);
    return -1;
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Arm ONLY the requested direction(s), ONE-SHOT (NOT EV_CLEAR), re-arming
     * on EVERY park -- the kqueue counterpart of the epoll EPOLLONESHOT re-arm
     * fix above.  The old scheme (register-once + EV_CLEAR, and SKIP the kevent
     * when the per-fd bit was already set) lost wakeups two ways, both caught by
     * test_netpoll_conformance's fd-reuse case:
     *   - EV_CLEAR is edge-triggered: an edge missed in the window between
     *     EV_ADD and the next kevent() never refired (same trap as epoll's old
     *     EPOLLET register-once);
     *   - a reused fd NUMBER whose close-hook had not run (raw socket.close(),
     *     no runloom wrapper) kept its bitmap bit set, so the second register
     *     RETURNED EARLY without issuing the kevent -> the new socket was never
     *     armed and its reader parked forever (timed out via the deadline, or
     *     hung outright with no timeout).
     * EV_ADD re-checks readiness NOW (kqueue reports a level-ready fd at add
     * time, so data that arrived before the parker linked is still delivered),
     * and EV_ONESHOT delivers once then auto-disables (no thundering herd; the
     * always-writable OUT can't busy-loop the pump).  The bitmap is still kept
     * in sync for unregister, but no longer gates the kevent. */
    runloom_mutex_lock(&runloom_pool.lock);
    if (runloom_fd_bit_set(fd) != 0) {
        runloom_mutex_unlock(&runloom_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    runloom_mutex_unlock(&runloom_pool.lock);
    {
        struct kevent kev[2];
        int n = 0;
        if (events & RUNLOOM_NETPOLL_READ)
            EV_SET(&kev[n++], fd, EVFILT_READ,  EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (events & RUNLOOM_NETPOLL_WRITE)
            EV_SET(&kev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (n == 0) return 0;
        {   /* test-only: force the registration kevent() to fail; the error
             * surfaces as OSError(errno) to the fiber parking on this fd
             * (the fd-bit is rolled back below). */
            int finj = runloom_fault_inject(RUNLOOM_FAULT_KQUEUE_CTL);
            if (finj) errno = finj;
            else if (kevent(runloom_kqueue_fd, kev, n, NULL, 0, NULL) == 0) return 0;
        }
    }
    runloom_mutex_lock(&runloom_pool.lock);
    runloom_fd_bit_clear(fd);
    runloom_mutex_unlock(&runloom_pool.lock);
    return -1;
#else
    (void)fd;
    return 0;  /* select doesn't need pre-registration */
#endif
}

void runloom_netpoll_unregister(int fd)
{
#if defined(RUNLOOM_HAVE_EPOLL) || defined(RUNLOOM_HAVE_KQUEUE)
    runloom_mutex_lock(&runloom_pool.lock);
    runloom_fd_bit_clear(fd);
    /* Reset the epoll LEVEL arm mask: a reused fd number with a stale "armed"
     * mask would skip its EPOLL_CTL_ADD and never be re-armed (a hang).  All
     * real close paths call this hook, so the mask tracks fd identity. */
    runloom_fd_armed_set(fd, 0);
    runloom_mutex_unlock(&runloom_pool.lock);
    /* No kernel syscall: epoll/kqueue auto-remove the fd when the
     * last reference closes.  Calling EPOLL_CTL_DEL after close
     * would race with fd reuse anyway. */
#else
    (void)fd;
#endif
}

/* Release an fd's epoll registration IFF no fiber is currently parked on it.
 *
 * The aio bridge's low-level loop.sock_* operate on USER-OWNED sockets that the
 * caller closes with a plain socket.close() -- which does NOT run the monkey /
 * _close_sock unregister hook (only the bridge's OWN sockets go through that).
 * Without clearing the per-fd LEVEL arm cache, a reused fd NUMBER inherits the
 * stale "armed" mask and runloom_netpoll_register's already-armed skip path never
 * re-arms it in the kernel -> the new socket's wait_fd parks forever (the
 * long-standing test_recvfrom / fast-socket-churn hang).  The bridge calls this
 * after each low-level sock_* op so the registration is dropped once the op is
 * done, and a later raw close + fd reuse re-registers cleanly.
 *
 * Guarded on "no parker remains on this fd in ANY pool": a concurrent waiter on
 * the same fd (e.g. a WRITE parker while a READ op finishes) keeps its arm -- we
 * never EPOLL_CTL_DEL out from under a live direction, and never drop a wakeup.
 * Unlike unregister this DOES issue EPOLL_CTL_DEL (the fd is still OPEN here, so
 * the kernel will not auto-remove it; leaving it LEVEL-armed with no parker would
 * busy-report on every epoll_wait).  Monkey and the bridge transports are
 * untouched -- they keep the register-once fast path and clear the cache via
 * their own close hooks, so the throughput hot path pays zero extra syscalls. */
void runloom_netpoll_release_if_idle(int fd)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (fd < 0) return;
    /* Scoped to runloom_pool, the single-thread / DEFAULT parker pool the aio
     * bridge runs on (run_until_complete drives a single-thread scheduler, whose
     * wait_fd links into runloom_pool via runloom_parker_pool_for_hub(NULL)).  The
     * "no parker on fd" check, the arm-cache clear, AND the EPOLL_CTL_DEL all run
     * under runloom_pool.lock -- the SAME lock wait_fd takes to link a parker and
     * register() takes to set the arm.  So a wait_fd that links+arms cannot slip
     * between the idle check and the DEL (which would DEL an fd it just armed -> a
     * lost wake): either it links first (we see the parker, no-op) or we DEL first
     * (an idle fd with no waiter) and its later register() re-ADDs fresh
     * (armed==0).  (M:N hub pools are not in play -- only the bridge calls this,
     * and it never shares an fd with a concurrently-running hub.) */
    runloom_mutex_lock(&runloom_pool.lock);
    if (((size_t)fd >= runloom_pool.by_fd_cap || runloom_pool.by_fd[fd] == NULL)
        && runloom_fd_armed_get(fd) != 0) {
        runloom_fd_armed_set(fd, 0);
        runloom_fd_bit_clear(fd);
        epoll_ctl(runloom_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    }
    runloom_mutex_unlock(&runloom_pool.lock);
#else
    /* kqueue re-arms ONE-SHOT on every park (no persistent arm to go stale) and
     * select needs no pre-registration, so neither has the epoll fd-reuse bug. */
    (void)fd;
#endif
}

int runloom_netpoll_parked_count(void)
{
    int total = 0;
    int i;
    for (i = 0; i < RUNLOOM_PARKER_POOL_MAX; i++) {
        total += __atomic_load_n(&runloom_pools[i].total, __ATOMIC_ACQUIRE);
    }
    return total;
}

/* Hub-idle dwell-based stack reclaim (RUNLOOM_STACK_PARK_SWEEP).  Walk the
 * CALLING hub's parker pool and madvise the below-SP idle stack pages of
 * fibers whose current park has already exceeded threshold_ns.
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
 * search this pool.  Bounded to RUNLOOM_SWEEP_BATCH per call; the next idle
 * sweep takes the rest.  Returns the number of stacks reclaimed. */
#define RUNLOOM_SWEEP_BATCH 128       /* max stacks madvised per sweep call */
#define RUNLOOM_SWEEP_MAX_VISIT 1024  /* max parkers examined per sweep (lock-hold bound) */
/* Churn throttle default: skip the sweep when this pool sees more than
 * this many parker arrivals/sec (per hub).  Calibrated so a genuinely
 * idle keepalive workload (the N=1M target: parkers dwelling seconds,
 * low arrival rate) always sweeps and reclaims, while an active-churn
 * workload (sub-second per-connection cycling, high arrival rate) skips
 * -- degrading the sweep to a no-op rather than paying lock contention.
 * Override with RUNLOOM_SWEEP_MAX_CHURN (0 disables the throttle).
 *
 * 600 calibrated on the N=65K keepalive bench (H=16): the idle target
 * (think=10s, ~250 arrivals/s/hub) sweeps FULLY at 600 -- RSS 3419 MB
 * (full -32% win), p99 50 ms (the OFF floor, zero cost) -- while the
 * active-churn case (think=2s, ~1200/s/hub) throttles (RSS 4929 ~= OFF,
 * sweep degrades to a no-op).  600 sits cleanly between the two rates. */
#define RUNLOOM_SWEEP_DEFAULT_MAX_CHURN 600LL
int runloom_netpoll_sweep_idle(void *hub_opaque, long long threshold_ns)
{
    /* No backend #if here: the actual madvise lives in
     * runloom_coro_madvise_idle (coro.c), which no-ops on backends without
     * an inspectable SP / MADV_DONTNEED.  netpoll.c does NOT define
     * MADV_DONTNEED (no <sys/mman.h>), so gating this body on it -- as an
     * earlier draft did -- silently compiled the whole sweep away. */
    runloom_parker_pool_t *pool = runloom_parker_pool_for_hub(hub_opaque);
    runloom_g_t *batch[RUNLOOM_SWEEP_BATCH];
    int n = 0, i, visited = 0;
    int per_g = runloom_get_per_g_tstate_mode();
    long long now;
    if (pool == NULL ||
        __atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) {
        return 0;
    }
    now = monotonic_ns();
    runloom_mutex_lock(&pool->lock);
    /* Churn throttle.  Compute this pool's arrival rate since the last
     * sweep eval; if it exceeds RUNLOOM_SWEEP_MAX_CHURN links/sec, skip the
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
            const char *e = getenv("RUNLOOM_SWEEP_MAX_CHURN");
            mc = (e != NULL) ? atoll(e) : RUNLOOM_SWEEP_DEFAULT_MAX_CHURN;
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
                runloom_mutex_unlock(&pool->lock);
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
        runloom_parked_t *p = pool->sweep_cursor;
        if (p == NULL || p->gen != pool->sweep_cursor_gen) {
            p = pool->head;
        }
        while (p != NULL && n < RUNLOOM_SWEEP_BATCH &&
               visited < RUNLOOM_SWEEP_MAX_VISIT) {
            if (!p->reclaimed && p->park_ts != 0 &&
                p->g != NULL && p->g->coro != NULL &&
                __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE)
                    == RUNLOOM_PARK_PARKED &&
                (now - p->park_ts) >= threshold_ns) {
                /* Default mode: this hub is the g's sole resumer, so marking it
                 * reclaimed and madvising below is race-free.  Per-g-tstate: a
                 * woken g can be stolen by ANY hub, so claim it exclusively
                 * (PARKED->SWEEPING) first; if the claim loses (the g is being
                 * woken/owned) skip it -- never madvise a resumable stack. */
                if (!per_g || runloom_mn_sweep_try_claim(p->g)) {
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
    runloom_mutex_unlock(&pool->lock);
    for (i = 0; i < n; i++) {
        runloom_coro_madvise_idle(batch[i]->coro);
        /* Companion reclaim: drop the parked Python g's idle datastack-
         * chunk tail too (the C-stack madvise above never touches it).
         * Same owning-hub safety contract; gated by RUNLOOM_DATASTACK_SWEEP. */
        runloom_sched_madvise_datastack_idle(batch[i]);
        /* Per-g-tstate: release the exclusive sweep claim now that this g's
         * madvise has completed (SWEEPING->PARKED, or re-enqueue a wake that
         * landed mid-madvise).  Per-g release bounds a woken g's extra latency
         * to its own single madvise, not the whole batch. */
        if (per_g) {
            runloom_mn_sweep_claim_release(batch[i]);
        }
    }
    return n;
}
