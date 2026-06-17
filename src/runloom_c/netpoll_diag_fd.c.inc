/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

#if !defined(RUNLOOM_OS_WINDOWS)
#include <poll.h>   /* runloom_netpoll_dump_parkers ready-but-parked probe */
#endif


int runloom_netpoll_inspect_for_self_check(struct runloom_self_check_stats *out)
{
    runloom_parker_pool_t *pool = &runloom_pool;
    int global_count = 0;
    int global_cycle = 0;
    int bucket_total = 0;
    int bucket_self  = 0;
    int bucket_unreach = 0;
    int parked_atomic;
    runloom_parked_t *slow, *fast;
    size_t i;

    if (!__atomic_load_n(&runloom_netpoll_inited, __ATOMIC_ACQUIRE)) {
        runloom_self_check_stats_set(out, 0, 0, 0, 0, 0, 0);
        return 0;
    }
    runloom_parker_pool_lock_ensure_inited(pool);
    RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);

    /* Floyd cycle detection on the global list, with a safety cap. */
    slow = pool->head;
    fast = pool->head;
    {
        int iters = 0;
        const int CAP = 200000;
        while (fast != NULL && iters < CAP) {
            if (iters > 0) slow = slow ? slow->next : NULL;
            fast = fast->next;
            if (fast != NULL) fast = fast->next;
            if (iters > 0 && slow != NULL && slow == fast) {
                global_cycle = 1;
                break;
            }
            iters++;
        }
    }
    /* Linear walk for a count (after cycle check; if cycle present we
     * cap at CAP to avoid spinning here). */
    if (!global_cycle) {
        runloom_parked_t *p = pool->head;
        while (p != NULL && global_count < 200000) {
            global_count++;
            p = p->next;
        }
    } else {
        /* On cycle, just report the count we walked to before detecting. */
        runloom_parked_t *p = pool->head;
        int iters = 0;
        while (p != NULL && iters < 200000) {
            global_count++;
            p = p->next;
            iters++;
            if (iters > 100000) break;
        }
    }
    parked_atomic = __atomic_load_n(&pool->total, __ATOMIC_ACQUIRE);

    /* Walk every per-fd bucket. */
    if (pool->by_fd != NULL) {
        for (i = 0; i < pool->by_fd_cap; i++) {
            runloom_parked_t *p = pool->by_fd[i];
            int chain_iters = 0;
            while (p != NULL && chain_iters < 10000) {
                bucket_total++;
                if (p->next_by_fd == p || p->prev_by_fd == p) {
                    bucket_self++;
                    break;          /* avoid infinite-loop */
                }
                /* Reachable-from-global check: walk global list,
                 * O(N*M) but only if we actually want it.  Skip the
                 * full check here; cheaper to assert via the link
                 * invariants. */
                (void)bucket_unreach;
                p = p->next_by_fd;
                chain_iters++;
            }
        }
    }

    RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    runloom_self_check_stats_set(out, global_count, global_cycle,
                              parked_atomic, bucket_total, bucket_self,
                              bucket_unreach);
    return 0;
}

/* DIAG: walk the global parker list and print each parker's fd/g/hub/commit to
 * stderr.  Used to identify a leaked netpoll parker (cross-file flake). */
void runloom_netpoll_dump_parkers(void)
{
    char buf[256];
    int m;
    int pi;
    if (!__atomic_load_n(&runloom_netpoll_inited, __ATOMIC_ACQUIRE)) {
        m = snprintf(buf, sizeof buf, "[parker-dump] netpoll not inited\n");
        (void)write(2, buf, (size_t)m);
        return;
    }
    /* Walk EVERY pool (per-hub kqueue: parkers live in per-hub pools, not the
     * default).  Summarize per pool by event mask, then list a sample. */
    for (pi = 0; pi < RUNLOOM_PARKER_POOL_MAX; pi++) {
        runloom_parker_pool_t *pool = &runloom_pools[pi];
        runloom_parked_t *p;
        int total = __atomic_load_n(&pool->total, __ATOMIC_ACQUIRE);
        int n = 0, rd = 0, wr = 0, rdwr = 0, other = 0;
        int rdyp = 0;   /* ready-but-parked: fd IS OS-ready yet g still parked */
        if (total == 0) continue;
        runloom_parker_pool_lock_ensure_inited(pool);
        RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
        for (p = pool->head; p != NULL && n < 200000; p = p->next, n++) {
            int e = p->events & (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE);
            if (e == RUNLOOM_NETPOLL_READ) rd++;
            else if (e == RUNLOOM_NETPOLL_WRITE) wr++;
            else if (e == (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE)) rdwr++;
            else other++;
#if !defined(RUNLOOM_OS_WINDOWS)
            /* Diagnostic readiness probe (lost-wakeup detector): is this fd
             * ACTUALLY ready right now while its g is still parked?  If many are
             * ready-but-parked the wedge is a lost netpoll wakeup; if few/none
             * the gs are genuinely waiting on I/O that never arrives. */
            {
                struct pollfd pfd;
                short want = 0;
                if (p->events & RUNLOOM_NETPOLL_READ)  want |= POLLIN;
                if (p->events & RUNLOOM_NETPOLL_WRITE) want |= POLLOUT;
                pfd.fd = p->fd; pfd.events = want; pfd.revents = 0;
                if (poll(&pfd, 1, 0) > 0 &&
                    (pfd.revents & (want | POLLHUP | POLLERR))) rdyp++;
            }
#endif
        }
        m = snprintf(buf, sizeof buf,
            "[parker-dump] pool#%d total=%d walked=%d READ=%d WRITE=%d RW=%d other=%d readyParked=%d kq=%d\n",
            pi, total, n, rd, wr, rdwr, other, rdyp,
#if defined(RUNLOOM_HAVE_KQUEUE)
            pool->kqueue_fd
#else
            -1
#endif
            );
        (void)write(2, buf, (size_t)m);
        n = 0;
        for (p = pool->head; p != NULL && n < 8; p = p->next, n++) {
            m = snprintf(buf, sizeof buf,
                "[parker-dump]   pool#%d sample fd=%d events=%d g=%p hub=%p commit=%d gen=%u\n",
                pi, p->fd, p->events, (void *)p->g, (void *)p->hub, p->commit, p->gen);
            (void)write(2, buf, (size_t)m);
        }
        RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    }
}

/* ---- per-fd registration cache ----
 * One bit per fd; set when we've already issued EPOLL_CTL_ADD (or
 * the kqueue equivalent) for this fd as edge-triggered for both
 * READ and WRITE.  Subsequent wait_fd calls then skip the
 * epoll_ctl syscall entirely -- the kernel keeps reporting edges
 * until the fd is closed (which auto-clears the registration).
 *
 * Every access (get/set/clear) is under runloom_pool.lock, keeping the
 * check + ADD atomic against concurrent registers for the same fd.
 * The backing array is preallocated once (runloom_fd_arrays_init) and
 * never realloc'd, so the pointer is stable for the process lifetime. */
static unsigned char *runloom_fd_registered_bm = NULL;
static size_t         runloom_fd_registered_cap_bytes = 0;

/* ---- per-fd pending-wakeup bitmap ----
 * Closes the M:N + free-threaded race where an epoll edge fires for
 * an fd whose parker hasn't been linked yet (or whose previous
 * parker was just unlinked by a wake but the fiber hasn't
 * called wait_fd again).  Without this, the pump finds an empty
 * runloom_pool.by_fd[fd] bucket and silently drops the event; with
 * EPOLLET the kernel won't refire and the fiber waits forever.
 *
 * Each fd gets one byte holding a mask of RUNLOOM_NETPOLL_READ /
 * RUNLOOM_NETPOLL_WRITE bits.  Pump sets bits when it can't find a
 * matching parker (runloom_fd_pending_wake_set, called with NO lock and
 * from any hub's pump concurrently); wait_fd consumes bits before
 * parking (runloom_fd_pending_wake_consume, lock-free) and returns
 * immediately if a pending bit covers the requested event.
 *
 * The array is preallocated once to the fd hard limit and never
 * realloc'd (see runloom_fd_arrays_init).  That is what makes concurrent
 * lock-free set/consume safe: with no realloc the base pointer never
 * moves, so there is no writer/writer or writer/reader UAF -- only
 * per-byte atomic fetch_or / fetch_and on a stable buffer.
 *
 * Memory ordering: the pump's fetch_or pairs with wait_fd's fetch_and
 * via ACQ_REL on both, providing a clean happens-before for "kernel
 * event happened" -> "next wait observes it".  The init publish stores
 * the base (RELEASE) before the cap (RELEASE); readers load the cap
 * (ACQUIRE) before the base, so a non-zero cap implies a valid base.
 *
 * FSM SCOPE (RUNLOOM_FSM_VALIDATE): tier-4 LEAVE (FSM_ADOPTION.md).  Each byte is
 * an independent edge-readiness bitmask, not an enumerable (state,event) machine:
 * the per-byte atomic fetch_or (set) / fetch_and (consume) on a never-realloc'd
 * buffer IS the optimal proof on this hot, lock-free path (the ACQ_REL pairing
 * above is the correctness argument).  A transition table / NOTE-edge would be
 * ceremony with no missing-handler bug to catch.  Backend-agnostic: epoll, kqueue,
 * select, and WSAPoll all feed it via the same pump -> parker recheck. */
static unsigned char *runloom_fd_pending_wake = NULL;
static size_t         runloom_fd_pending_wake_cap = 0;

/* Per-fd epoll LEVEL arm mask (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE
 * bits): which direction(s) the fd is currently armed for.  The epoll backend
 * arms each fd LEVEL-triggered, register-PER-DIRECTION-once -- a re-park whose
 * direction is already armed skips epoll_ctl entirely (the read-park hot path
 * costs zero syscalls).  Shares the pending-wake fd index space + cap.
 * Accessed only under runloom_pool.lock, like the registration bitmap. */
static unsigned char *runloom_fd_armed = NULL;

/* Per-fd owning parker-pool INDEX for the per-hub epoll backend
 * (RUNLOOM_PERHUB_EPOLL): which pool's epoll_fd this fd is registered in, so
 * release_if_idle DELs from the right epoll and a cross-hub re-park MODs the
 * OWNING epoll (where the fd physically lives), not the current hub's.  0xFF =
 * none.  Shares the pending-wake fd index space + cap; accessed under
 * runloom_pool.lock like runloom_fd_armed.  NULL/unused on the shared path. */
static unsigned char *runloom_fd_epoll_pool = NULL;

/* Upper/lower bounds on the preallocated fd capacity.  4M fds = 4MB
 * pending-wake array + 512KB registration bitmap, worst case. */
#define RUNLOOM_FD_CAP_MIN   1024u
#define RUNLOOM_FD_CAP_MAX   (8u * 1024u * 1024u)

/* Capacity to preallocate the per-fd arrays to.  An open fd is always
 * < the RLIMIT_NOFILE soft limit, which can never exceed the hard
 * limit without privilege; sizing to the hard limit guarantees no
 * legal fd ever indexes past the array, so it is never realloc'd.
 * RUNLOOM_NETPOLL_MAXFD overrides for hosts whose hard limit is
 * unlimited (then both rlimits read as RLIM_INFINITY). */
static size_t runloom_fd_cap_target(void)
{
    size_t target = 65536;
    const char *env;
#if !defined(RUNLOOM_OS_WINDOWS)
    struct rlimit r;
    if (getrlimit(RLIMIT_NOFILE, &r) == 0) {
        if (r.rlim_max != RLIM_INFINITY && r.rlim_max > 0)
            target = (size_t)r.rlim_max;
        else if (r.rlim_cur != RLIM_INFINITY && r.rlim_cur > 0)
            target = (size_t)r.rlim_cur;
    }
#endif
    env = getenv("RUNLOOM_NETPOLL_MAXFD");
    if (env != NULL && *env != '\0') {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end != env && v > 0) target = (size_t)v;
    }
    if (target < RUNLOOM_FD_CAP_MIN) target = RUNLOOM_FD_CAP_MIN;
    if (target > RUNLOOM_FD_CAP_MAX) target = RUNLOOM_FD_CAP_MAX;
    return target;
}

/* One-time preallocation of both per-fd arrays.  Called from
 * runloom_netpoll_init while holding runloom_pool.lock, which serialises it
 * against bit get/set/clear (same lock) and against a racing init.
 * Idempotent: a second call that finds the arrays already present
 * (e.g. after fini/init) is a no-op. */
static void runloom_fd_arrays_init(void)
{
    size_t cap_fds, bm_bytes;
    unsigned char *pw, *bm, *am;
    if (runloom_fd_pending_wake != NULL) return;
    cap_fds  = runloom_fd_cap_target();
    bm_bytes = (cap_fds + 7u) >> 3;
    pw = (unsigned char *)calloc(cap_fds, 1);
    bm = (unsigned char *)calloc(bm_bytes, 1);
    am = (unsigned char *)calloc(cap_fds, 1);
    {
    /* Per-hub epoll ownership map: 0xFF (= "none") default, so calloc's zero
     * (which is a VALID pool index, hub 0) is not mistaken for an owner. */
    unsigned char *ep = (unsigned char *)malloc(cap_fds);
    if (pw == NULL || bm == NULL || am == NULL || ep == NULL) {
        free(pw);
        free(bm);
        free(am);
        free(ep);
        return;   /* caps stay 0; set/consume/bit_* bounds-check to no-ops */
    }
    memset(ep, 0xFF, cap_fds);
    runloom_fd_epoll_pool           = ep;
    runloom_fd_registered_bm        = bm;
    runloom_fd_registered_cap_bytes = bm_bytes;
    runloom_fd_armed                = am;
    }
    /* Publish base before cap so the lock-free consumer that loads cap
     * (ACQUIRE) and sees it non-zero is guaranteed to see this base. */
    __atomic_store_n(&runloom_fd_pending_wake, pw, __ATOMIC_RELEASE);
    __atomic_store_n(&runloom_fd_pending_wake_cap, cap_fds, __ATOMIC_RELEASE);
}

/* Warn once when an fd exceeds the preallocated ceiling.  This cannot
 * happen for an fd this process can legally hold (it would be >= the
 * hard limit); the guard exists only so a privileged runtime
 * limit-raise degrades to a dropped event + diagnostic, never a
 * reintroduced realloc race. */
static void runloom_fd_cap_warn_once(int fd)
{
    static int warned = 0;
    int expected = 0;
    if (__atomic_compare_exchange_n(&warned, &expected, 1, 0,
                                    __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
        fprintf(stderr,
                "[runloom] fd %d exceeds preallocated netpoll capacity %zu; "
                "raise RUNLOOM_NETPOLL_MAXFD (event dropped)\n",
                fd, runloom_fd_pending_wake_cap);
    }
}

/* Pump-side: mark an event as observed-but-unrouted.  Lock-free and
 * callable concurrently from any hub's pump -- safe because the array
 * is preallocated and never moves. */
static void runloom_fd_pending_wake_set(int fd, int mask)
{
    unsigned char *base;
    size_t cap;
    if (fd < 0 || mask == 0) return;
    cap = __atomic_load_n(&runloom_fd_pending_wake_cap, __ATOMIC_ACQUIRE);
    if ((size_t)fd >= cap) { runloom_fd_cap_warn_once(fd); return; }
    base = __atomic_load_n(&runloom_fd_pending_wake, __ATOMIC_ACQUIRE);
    if (base == NULL) return;
    __atomic_fetch_or(&base[fd], (unsigned char)mask, __ATOMIC_ACQ_REL);
}

/* wait_fd-side: claim any pending bits matching `events`.  Returns
 * the bits that were pending AND in events (0 = nothing pending).
 * Lock-free. */
static int runloom_fd_pending_wake_consume(int fd, int events)
{
    unsigned char *base;
    size_t cap = __atomic_load_n(&runloom_fd_pending_wake_cap, __ATOMIC_ACQUIRE);
    if (fd < 0 || (size_t)fd >= cap) return 0;
    base = __atomic_load_n(&runloom_fd_pending_wake, __ATOMIC_ACQUIRE);
    if (base == NULL) return 0;
    {
        unsigned char take = (unsigned char)events;
        unsigned char prev =
            __atomic_fetch_and(&base[fd],
                               (unsigned char)~take, __ATOMIC_ACQ_REL);
        int pending = prev & events;
#if defined(RUNLOOM_HAVE_EPOLL)
        /* A claimed pending bit may be a STALE level re-fire, not a real
         * wake-up owed to us.  epoll is LEVEL-triggered (netpoll_register
         * arms EPOLLIN|EPOLLRDHUP, no EPOLLET/EPOLLONESHOT), so an undrained
         * byte re-fires the pump while no parker is linked; that 2nd fire
         * stashes here (netpoll_pump dispatch-miss).  The parker the 1st fire
         * woke then DRAINS the byte AFTER wait_fd returns, and on fd REUSE the
         * next wait_fd would consume this now-stale stash and wake spuriously
         * (the reuse_iso double-count).  Ground-truth the stash against the
         * kernel's ACTUAL readiness with a non-blocking poll -- NOT a read():
         * `fd` is the app's real socket, so reading it would discard payload.
         *
         * Soundness: this only filters the redundant bitmap fast-path.  When we
         * return 0 the parker is NOT unlinked in wait_fd (the unlink runs only
         * on a non-zero pending) -- it falls through to register (the LEVEL arm)
         * and parks still LINKED + ARMED, so any genuinely-still-present
         * readiness re-fires to it on the next epoll_wait.  Discarding a stale
         * stash therefore can only force a correct re-park, never drop a real
         * edge; the level arm is the authoritative carrier and stays untouched.
         *
         * Key ONLY on the direction bits plus POLLHUP/POLLERR (which the kernel
         * auto-co-reports on a dead / half-closed socket -- a real readiness we
         * must consume), and EXCLUDE POLLNVAL: a closed-but-reused fd number
         * polls rc=1 with revents=POLLNVAL, so trusting poll's RETURN COUNT as
         * "ready" would re-admit the very double-count we are killing.  Gated to
         * epoll: kqueue (EV_ONESHOT) and IOCP/WSAPoll (per-parker IRPs) never
         * stash, so `pending` is always 0 there and this block is dead anyway. */
        if (pending != 0) {
            struct pollfd pfd;
            int pr;
            pfd.fd = fd;
            pfd.events = 0;
            if (events & RUNLOOM_NETPOLL_READ)  pfd.events |= POLLIN;
            if (events & RUNLOOM_NETPOLL_WRITE) pfd.events |= POLLOUT;
            do {
                pfd.revents = 0;
                pr = poll(&pfd, 1, 0);
            } while (pr < 0 && errno == EINTR);
            if (pr <= 0) {
                /* Not ready (the stash was stale) or a poll error -> park.  A
                 * genuine readiness is still LEVEL-armed + linked and re-fires. */
                return 0;
            }
            {
                int ready = 0;     /* POLLNVAL deliberately omitted */
                if (pfd.revents & (POLLIN  | POLLHUP | POLLERR))
                    ready |= RUNLOOM_NETPOLL_READ;
                if (pfd.revents & (POLLOUT | POLLERR | POLLHUP))
                    ready |= RUNLOOM_NETPOLL_WRITE;
                return ready & events;
            }
        }
#endif
        return pending;
    }
}

static int runloom_fd_bit_set(int fd)
{
    if (fd < 0) return -1;
    size_t byte = (size_t)fd >> 3;
    if (byte >= runloom_fd_registered_cap_bytes) {
        /* Beyond the preallocated hard-limit ceiling (runloom_fd_arrays_init);
         * cannot happen for a legal fd.  Fail so the caller treats it as
         * ENOMEM rather than silently registering nothing. */
        return -1;
    }
    runloom_fd_registered_bm[byte] |= (unsigned char)(1u << (fd & 7));
    return 0;
}

static void runloom_fd_bit_clear(int fd)
{
    if (fd < 0) return;
    size_t byte = (size_t)fd >> 3;
    if (byte >= runloom_fd_registered_cap_bytes) return;
    runloom_fd_registered_bm[byte] &= (unsigned char)~(1u << (fd & 7));
}

/* Per-fd epoll LEVEL arm mask accessors.  Caller MUST hold runloom_pool.lock. */
static int runloom_fd_armed_get(int fd)
{
    if (fd < 0 || runloom_fd_armed == NULL) return 0;
    if ((size_t)fd >= runloom_fd_pending_wake_cap) return 0;
    return runloom_fd_armed[fd];
}

static int runloom_fd_armed_set(int fd, int mask)
{
    if (fd < 0 || runloom_fd_armed == NULL) return -1;
    if ((size_t)fd >= runloom_fd_pending_wake_cap) return -1;
    runloom_fd_armed[fd] = (unsigned char)(mask & (RUNLOOM_NETPOLL_READ |
                                                   RUNLOOM_NETPOLL_WRITE));
    return 0;
}

/* Per-fd owning-pool accessors (RUNLOOM_PERHUB_EPOLL).  Caller MUST hold
 * runloom_pool.lock, like the arm-mask accessors.  get returns -1 for "none". */
static int runloom_fd_epoll_pool_get(int fd)
{
    if (fd < 0 || runloom_fd_epoll_pool == NULL) return -1;
    if ((size_t)fd >= runloom_fd_pending_wake_cap) return -1;
    {
        unsigned char v = runloom_fd_epoll_pool[fd];
        return (v == 0xFFu) ? -1 : (int)v;
    }
}

static void runloom_fd_epoll_pool_set(int fd, int idx)
{
    if (fd < 0 || runloom_fd_epoll_pool == NULL) return;
    if ((size_t)fd >= runloom_fd_pending_wake_cap) return;
    runloom_fd_epoll_pool[fd] = (idx < 0 || idx > 0xFE) ? 0xFFu : (unsigned char)idx;
}

#if defined(RUNLOOM_HAVE_EPOLL)
static int runloom_epoll_fd = -1;
/* Eventfd registered by io_uring.c for the GLOBAL ring; events on this
 * fd are dispatched to runloom_iouring_drain().  -1 = none registered. */
static int runloom_iouring_eventfd_in_epoll = -1;
/* Generic cross-thread pump interrupt.  An eventfd in the shared epoll
 * whose only job is to break an idle epoll_wait from another thread, so a
 * waker that re-queues a fiber via the scheduler's lists (not via a
 * netpoll fd event) can still wake a scheduler blocked in the pump.  Used
 * by the blocking-offload pool to wake the single-thread scheduler, which
 * (unlike the busy-polling hubs) blocks in epoll_wait with no timeout.
 * On fire the pump just drains it -- it carries no parker. */
static int runloom_pump_wake_fd = -1;

/* Per-hub iouring rings registered via runloom_netpoll_add_iouring_ring.
 * The dispatch path matches epoll evs[i].data.fd against the eventfd
 * column and calls runloom_iouring_ring_drain on the corresponding ring.
 * Sized for typical CPU counts (one hub == one ring); 64 is comfortable
 * for any host we run on.  Protected by runloom_pool.lock.
 *
 * Parallel arrays instead of array-of-struct: small (one cache line each
 * at 64 entries) + lets us scan the fd column tightly.
 */
#define RUNLOOM_IOURING_RINGS_MAX 64
static int                       runloom_iouring_ring_efds[RUNLOOM_IOURING_RINGS_MAX];
static struct runloom_iouring_ring *runloom_iouring_ring_ptrs[RUNLOOM_IOURING_RINGS_MAX];
static int                       runloom_iouring_ring_count = 0;
#elif defined(RUNLOOM_HAVE_KQUEUE)
/* Per-hub kqueue: there is NO process-wide kqueue handle or self-pipe.  Each
 * parker pool owns its own kqueue + pump-wake self-pipe
 * (runloom_parker_pool.kqueue_fd / selfpipe_r / selfpipe_w), created in
 * runloom_pool_backend_create() at pool lazy-init.  A single shared kqueue
 * serialised every hub's register/collect/wake on the kernel's per-kqueue lock
 * and collapsed connect throughput super-linearly with hub count. */
#elif defined(RUNLOOM_OS_WINDOWS)
#  include "netpoll_iocp.h"
/* Runtime-selected Windows backend.  Tier order:
 *   1. IOCP+AFD via \Device\Afd -- fastest at scale, O(1) per
 *      ready socket.  Requires NtDeviceIoControlFile (NT 4.0+).
 *   2. WSAPoll -- Vista+, no FD_SETSIZE cap, linear over fds.
 *   3. select() -- XP / Server 2003 fallback.
 *
 * Selection happens once in runloom_netpoll_init; backend_name is
 * exported via runloom_c.netpoll_backend() for introspection. */
typedef int (WSAAPI *runloom_wsapoll_fn)(LPWSAPOLLFD, ULONG, INT);
static runloom_wsapoll_fn runloom_win_wsapoll = NULL;
static int             runloom_win_use_iocp = 0;
static const char     *runloom_win_backend_name = "select";   /* updated by init */
#else
/* POSIX select() fallback: self-pipe pump interrupt -- the select()
 * analogue of the epoll eventfd.  The read end joins every select()
 * read set; any thread pokes the write end (runloom_netpoll_wake_pump) to
 * break an idle select so a cross-thread re-queue (e.g. a blocking-
 * offload worker) wakes a scheduler blocked in the pump.  -1 = unarmed.
 * Created once in runloom_netpoll_init under runloom_pool.lock. */
static int runloom_selfpipe_r = -1;
static int runloom_selfpipe_w = -1;
#endif
