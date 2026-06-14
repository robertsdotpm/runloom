/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* Wrapping macros for the raw syscalls -- glibc doesn't expose these
 * even on systems with io_uring kernel support. */
static int sys_io_uring_setup(unsigned entries, struct io_uring_params *p)
{
    return (int)syscall(__NR_io_uring_setup, entries, p);
}

static int sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
                              unsigned flags, void *arg, size_t argsz)
{
    return (int)syscall(__NR_io_uring_enter, fd, to_submit, min_complete,
                        flags, arg, argsz);
}

static int sys_io_uring_register(int fd, unsigned opcode, void *arg,
                                 unsigned nr_args)
{
    return (int)syscall(__NR_io_uring_register, fd, opcode, arg, nr_args);
}

/* Submit pending SQEs (min_complete=0, non-blocking), retrying on EINTR.
 *
 * A submit-only io_uring_enter does not wait, but the kernel still checks for a
 * pending signal first and can return EINTR before consuming the SQEs.  Letting
 * that EINTR reach the caller corrupts an otherwise-fine file_read/recv into a
 * spurious OSError(EINTR) -- confirmed by fault injection (strace
 * -e inject=io_uring_enter:error=EINTR:when=1).  Retry instead: a re-issued
 * enter submits whatever the previous call left unconsumed in the SQ.  Our
 * submit path holds sub_lock across the single-SQE write + enter, so that is
 * exactly our one SQE (if the EINTR'd call hadn't taken it) or nothing (if it
 * had -- the op is already in flight and enter returns 0); the SQE is submitted
 * exactly once either way.  EINTR is transient (a delivered signal), so this is
 * bounded; any other error is returned unchanged.  Mirrors the EINTR-retry the
 * GETEVENTS wait loops already do. */
static int sys_io_uring_submit_eintr(int ring_fd, unsigned to_submit)
{
    int n;
    do {
        n = sys_io_uring_enter(ring_fd, to_submit, 0, 0, NULL, 0);
    } while (n < 0 && errno == EINTR);
    return n;
}

/* CQ-ring overflow recovery (mapped from the SQ ring's flags word).
 *
 * If more completions arrive than the CQ ring can hold, the kernel sets
 * IORING_SQ_CQ_OVERFLOW and (on IORING_FEAT_NODROP kernels, >= 5.5) keeps the
 * overflowed CQEs in a kernel-side list rather than dropping them.  A drainer
 * that only walks the visible CQ ring never sees those completions -- so the
 * fibers parked on them are NEVER woken (a permanent hang) and the inflight
 * counter never balances.  A GETEVENTS enter flushes the kernel list back into
 * the ring; the drain loop's fresh cq_tail read then picks them up.
 *
 * Go never adopted io_uring, so there is no upstream reference for this -- it is
 * a runloom-native-backend invariant: a completion must never be silently
 * dropped.  This is structurally avoided for SINGLE-shot ops (submit_sqe
 * spin-drains the CQ when the SQ is full) but multishot recv can post many CQEs
 * the submitter is not synchronously draining, which is where overflow bites. */
#ifndef IORING_SQ_CQ_OVERFLOW
#define IORING_SQ_CQ_OVERFLOW (1U << 1)
#endif
static void runloom_iouring_flush_cq_overflow(unsigned *sq_flags, int ring_fd)
{
    if (sq_flags == NULL || ring_fd < 0) return;
    if (__atomic_load_n(sq_flags, __ATOMIC_ACQUIRE) & IORING_SQ_CQ_OVERFLOW) {
        /* Best-effort: a re-issued GETEVENTS just re-flushes if it raced. */
        (void)sys_io_uring_enter(ring_fd, 0, 0, IORING_ENTER_GETEVENTS, NULL, 0);
    }
}

/* Per-op record.  user_data on the SQE points to one of these so
 * drain can route the completion back to the right waiter.  For
 * "single" ops the record lives on the submitter's C stack across
 * the park; for "multishot" ops it lives as the first field of a
 * runloom_iouring_ms_t handle (heap-allocated, longer-lived). */
typedef enum {
    RUNLOOM_IOURING_OP_SINGLE    = 0,
    RUNLOOM_IOURING_OP_MULTISHOT = 1,
    RUNLOOM_IOURING_OP_CANCEL    = 2,    /* fire-and-forget */
} runloom_iouring_op_type_t;

/* SINGLE-op park/wake handshake for hub (M:N) callers.  A hub fiber
 * that submits a SINGLE op PARKS (coro_yield) instead of blocking the OS
 * thread; a drainer (the idle-hub netpoll pump) wakes it when the CQE
 * arrives.  Without a handshake the INLINE drain -- which runs ON the
 * submitter, synchronously, BEFORE it parks -- could wake a not-yet-parked
 * g via runloom_mn_wake_g, stranding it on the hub sub-list and double-
 * resuming it (this was "Bug 1", and the same latent hole exists in the
 * recv/send ring path).  `wait` is the single atomic commit point:
 *   INFLIGHT -> PARKED  (submitter, CAS): committed to yield; any drainer
 *                        that completes the op after this MUST wake us.
 *   * -> DONE           (drainer, exchange): record completion; wake the
 *                        submitter IFF the prior state was PARKED (i.e. it
 *                        had already committed to yielding).  A drainer that
 *                        sees INFLIGHT does NOT wake -- the submitter will
 *                        observe DONE on its own CAS and skip the park. */
#define RUNLOOM_IOURING_WAIT_INFLIGHT 0
#define RUNLOOM_IOURING_WAIT_PARKED   1
#define RUNLOOM_IOURING_WAIT_DONE     2

typedef struct runloom_iouring_op {
    int       type;          /* runloom_iouring_op_type_t */
    runloom_g_t *g;             /* SINGLE: g to wake when this op completes */
    void     *hub;           /* opaque hub_t* or NULL for global sched */
    int32_t   result;        /* SINGLE: CQE res field; valid after wake */
    int       wait;          /* SINGLE hub op: park/wake commit (see above) */
} runloom_iouring_op_t;

/* Forward-declare for the drain handler. */
typedef struct runloom_iouring_ms runloom_iouring_ms_t;
static void runloom_iouring_ms_on_cqe(runloom_iouring_ms_t *h,
                                   int32_t res, uint32_t flags);

/* Per-process ring state.  Initialised lazily on first use; once set
 * up it lives for the process lifetime. */
typedef struct {
    int ring_fd;
    int initialised;       /* 0 = untried, 1 = ready, -1 = init failed */
    int eventfd_fd;        /* signaled by the kernel on each CQE post */

    /* Submission queue */
    void  *sq_mmap;        size_t sq_mmap_size;
    unsigned *sq_head;     unsigned *sq_tail;
    unsigned  sq_mask;     unsigned  sq_entries;
    unsigned *sq_array;
    unsigned *sq_flags;    /* SQ ring flags word -- IORING_SQ_CQ_OVERFLOW */
    struct io_uring_sqe *sqes;
    size_t sqe_mmap_size;
    void  *sqe_mmap;

    /* Completion queue */
    void  *cq_mmap;        size_t cq_mmap_size;
    unsigned *cq_head;     unsigned *cq_tail;
    unsigned  cq_mask;     unsigned  cq_entries;
    struct io_uring_cqe *cqes;

    /* Serialises SQE writes + io_uring_enter calls across threads. */
    runloom_mutex_t sub_lock;

    /* Provided-buffer ring for multishot recv.  Registered with bgid=0
     * once the ring is up + the kernel reports >= 5.19.  pool_base is
     * a contiguous N_BUFS * BUF_SIZE allocation; the kernel writes
     * incoming data into one of these buffers per CQE.  ring_mem
     * holds the io_uring_buf_ring structure shared with the kernel.
     * Zero pool_base means "buffer ring unavailable" (older kernel
     * or registration failed); multishot callers fall back. */
    void   *pool_base;
    size_t  pool_total;            /* N_BUFS * BUF_SIZE */
    unsigned pool_n;               /* number of buffers in the ring */
    unsigned pool_buf_size;        /* per-buffer size */
    void   *bring_mem;             /* io_uring_buf_ring + entries */
    size_t  bring_size;
    uint16_t bring_mask;           /* pool_n - 1, pool_n must be pow2 */
    runloom_mutex_t bring_lock;       /* serialises producer-side tail writes */
} runloom_iouring_state_t;

static runloom_iouring_state_t runloom_iouring_state = {0};
static volatile long runloom_iouring_lock_inited = 0;

/* Inflight counter: incremented after a successful submit, decremented
 * by drain when a CQE is consumed.  Read by runloom_iouring_inflight()
 * so the scheduler drain loop knows not to exit while a fiber is
 * parked on an iouring op. */
static volatile int runloom_iouring_inflight_count = 0;

/* Lazy initialise the submission mutex.  Same pattern as netpoll's
 * runloom_parked_lock_ensure_inited -- works for static-init absent
 * Windows but we always use it on Linux too for uniformity. */
static void runloom_iouring_lock_ensure_inited(void)
{
    long expected;
    if (__atomic_load_n(&runloom_iouring_lock_inited, __ATOMIC_ACQUIRE) == 2)
        return;
    expected = 0;
    if (__atomic_compare_exchange_n(&runloom_iouring_lock_inited, &expected, 1,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&runloom_iouring_state.sub_lock);
        __atomic_store_n(&runloom_iouring_lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&runloom_iouring_lock_inited, __ATOMIC_ACQUIRE) != 2)
            { /* spin */ }
    }
}

/* Initialise the ring.  Lazy: only runs the first time
 * runloom_iouring_available() is called and only succeeds once.
 * Returns 0 on success, -1 on init failure / no kernel support. */
static int runloom_iouring_lazy_init(void)
{
    struct io_uring_params p;
    int fd, efd;
    void *sq_map = MAP_FAILED, *cq_map = MAP_FAILED, *sqe_map = MAP_FAILED;
    size_t sq_size = 0, cq_size = 0, sqe_size = 0;
    int reg_arg;

    memset(&p, 0, sizeof(p));
    /* Setup flags we DON'T pass and why:
     *
     *  IORING_SETUP_DEFER_TASKRUN (6.1+):
     *    Defers completion task work until the user thread calls
     *    io_uring_enter(GETEVENTS).  In our pump-on-eventfd model
     *    the pump sleeps on epoll_wait(eventfd); the eventfd is
     *    signalled only when a CQE is posted, but with DEFER_TASKRUN
     *    the CQE isn't posted until we flush task work via
     *    io_uring_enter -- so the pump would sleep forever waiting
     *    for a notification that never comes.  Making it work would
     *    require us to call io_uring_enter(GETEVENTS) periodically
     *    (one extra syscall per drain), which negates the win.
     *    Worth revisiting if/when we move to a dedicated drain thread.
     *
     *  IORING_SETUP_SINGLE_ISSUER (5.18+):
     *    Asserts only the ring-creator thread submits SQEs.  Our M:N
     *    hub threads also submit to the shared ring, so this would
     *    fail on hub callers.  Could be enabled once per-hub rings
     *    land.
     *
     *  IORING_SETUP_SQPOLL:
     *    Dedicates a kernel thread to SQ polling.  Lower latency at
     *    the cost of a busy CPU.  Future opt-in.
     */
    fd = sys_io_uring_setup(64, &p);
    if (fd < 0) {
        __atomic_store_n(&runloom_iouring_state.initialised, -1, __ATOMIC_RELEASE);
        return -1;
    }

    /* SQ ring */
    sq_size = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    sq_map = mmap(NULL, sq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq_map == MAP_FAILED) goto fail;

    /* CQ ring -- on modern kernels SQ + CQ can share one mmap; we map
     * them separately for simplicity. */
    cq_size = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    cq_map = mmap(NULL, cq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
    if (cq_map == MAP_FAILED) goto fail;

    /* SQE array */
    sqe_size = p.sq_entries * sizeof(struct io_uring_sqe);
    sqe_map = mmap(NULL, sqe_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (sqe_map == MAP_FAILED) goto fail;

    /* Eventfd for completion notification.  EFD_NONBLOCK so the netpoll
     * pump's drain-read doesn't block; EFD_CLOEXEC to avoid leaking
     * across exec. */
    efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) goto fail;

    /* Register the eventfd with the ring.  Kernel writes a counter on
     * each CQE post; reader drains by reading the eventfd. */
    reg_arg = efd;
    if (sys_io_uring_register(fd, IORING_REGISTER_EVENTFD, &reg_arg, 1) < 0) {
        /* Without the eventfd we can't drive the async path; treat as
         * init failure so callers fall back to the thread-pool path. */
        close(efd);
        goto fail;
    }

    runloom_iouring_state.ring_fd       = fd;
    runloom_iouring_state.eventfd_fd    = efd;
    runloom_iouring_state.sq_mmap       = sq_map;
    runloom_iouring_state.sq_mmap_size  = sq_size;
    runloom_iouring_state.cq_mmap       = cq_map;
    runloom_iouring_state.cq_mmap_size  = cq_size;
    runloom_iouring_state.sqe_mmap      = sqe_map;
    runloom_iouring_state.sqe_mmap_size = sqe_size;

    runloom_iouring_state.sq_head    = (unsigned *)((char *)sq_map + p.sq_off.head);
    runloom_iouring_state.sq_tail    = (unsigned *)((char *)sq_map + p.sq_off.tail);
    runloom_iouring_state.sq_mask    = *(unsigned *)((char *)sq_map + p.sq_off.ring_mask);
    runloom_iouring_state.sq_entries = *(unsigned *)((char *)sq_map + p.sq_off.ring_entries);
    runloom_iouring_state.sq_array   = (unsigned *)((char *)sq_map + p.sq_off.array);
    runloom_iouring_state.sq_flags   = (unsigned *)((char *)sq_map + p.sq_off.flags);
    runloom_iouring_state.sqes       = (struct io_uring_sqe *)sqe_map;

    runloom_iouring_state.cq_head    = (unsigned *)((char *)cq_map + p.cq_off.head);
    runloom_iouring_state.cq_tail    = (unsigned *)((char *)cq_map + p.cq_off.tail);
    runloom_iouring_state.cq_mask    = *(unsigned *)((char *)cq_map + p.cq_off.ring_mask);
    runloom_iouring_state.cq_entries = *(unsigned *)((char *)cq_map + p.cq_off.ring_entries);
    runloom_iouring_state.cqes       = (struct io_uring_cqe *)((char *)cq_map + p.cq_off.cqes);

    /* initialised is published (atomic-release) only at the very END of init,
     * after the eventfd is hooked into the pump -- so a lock-free fast-path
     * reader in runloom_iouring_available() never observes a half-built ring. */

    /* Best-effort provided-buffer-ring setup for multishot recv.
     * Linux 5.19+; older kernels just leave pool_base = NULL and
     * callers fall back to single-shot recv.  Failure here is not a
     * hard error -- only the multishot path is affected. */
    {
        struct runloom_iouring_buf_reg reg;
        size_t pool_total = (size_t)RUNLOOM_IOURING_PBUF_COUNT *
                            RUNLOOM_IOURING_PBUF_SIZE;
        size_t bring_size = sizeof(struct runloom_iouring_buf) *
                            RUNLOOM_IOURING_PBUF_COUNT;
        long page = sysconf(_SC_PAGESIZE);
        void *pool = NULL, *bring = NULL;
        if (page <= 0) page = 4096;
        if (posix_memalign(&pool, (size_t)page, pool_total) != 0)
            pool = NULL;
        if (posix_memalign(&bring, (size_t)page, bring_size) != 0)
            bring = NULL;
        if (pool != NULL && bring != NULL) {
            unsigned i;
            struct runloom_iouring_buf *bufs;
            memset(bring, 0, bring_size);
            bufs = (struct runloom_iouring_buf *)bring;
            memset(&reg, 0, sizeof(reg));
            reg.ring_addr    = (uintptr_t)bring;
            reg.ring_entries = RUNLOOM_IOURING_PBUF_COUNT;
            reg.bgid         = RUNLOOM_IOURING_PBUF_BGID;
            reg.flags        = 0;
            if (sys_io_uring_register(fd, IORING_REGISTER_PBUF_RING,
                                      &reg, 1) == 0) {
                /* Populate the ring: each buffer at index i refers to
                 * pool_base + i*BUF_SIZE with bid=i.  After we fill the
                 * descriptors, bump the tail by N so the kernel sees N
                 * usable buffers immediately. */
                for (i = 0; i < RUNLOOM_IOURING_PBUF_COUNT; i++) {
                    bufs[i].addr = (uintptr_t)((char *)pool +
                                   (size_t)i * RUNLOOM_IOURING_PBUF_SIZE);
                    bufs[i].len  = RUNLOOM_IOURING_PBUF_SIZE;
                    bufs[i].bid  = (uint16_t)i;
                    bufs[i].resv = 0;
                }
                /* The kernel tail lives in bufs[0].resv (per the
                 * io_uring_buf_ring union).  Atomic store-release so
                 * the kernel observes a consistent view of all
                 * descriptors before the new tail. */
                __atomic_store_n(&bufs[0].resv,
                                 (uint16_t)RUNLOOM_IOURING_PBUF_COUNT,
                                 __ATOMIC_RELEASE);
                runloom_iouring_state.pool_base     = pool;
                runloom_iouring_state.pool_total    = pool_total;
                runloom_iouring_state.pool_n        = RUNLOOM_IOURING_PBUF_COUNT;
                runloom_iouring_state.pool_buf_size = RUNLOOM_IOURING_PBUF_SIZE;
                runloom_iouring_state.bring_mem     = bring;
                runloom_iouring_state.bring_size    = bring_size;
                runloom_iouring_state.bring_mask    = RUNLOOM_IOURING_PBUF_COUNT - 1;
                runloom_mutex_init(&runloom_iouring_state.bring_lock);
            } else {
                free(pool);  free(bring);
            }
        } else {
            free(pool);  free(bring);
        }
    }

    /* Hook the eventfd into the netpoll pump so CQEs cause the pump
     * to wake up + drain.  If this fails (epoll not on this OS, or
     * netpoll init failed), callers running on the global scheduler
     * will get no CQE delivery and park forever.  Treat as init
     * failure -- the hub path still works via spin-drain. */
    if (runloom_netpoll_add_iouring_eventfd(efd) != 0) {
        /* Don't tear down the ring; just abort init.  Mark as failed so
         * callers fall back. */
        munmap(sq_map,  sq_size);
        munmap(cq_map,  cq_size);
        munmap(sqe_map, sqe_size);
        close(efd);
        close(fd);
        runloom_iouring_state.eventfd_fd = -1;
        runloom_iouring_state.ring_fd    = -1;
        __atomic_store_n(&runloom_iouring_state.initialised, -1, __ATOMIC_RELEASE);
        return -1;
    }
    /* Fully built AND hooked into the pump: publish ready, release. */
    __atomic_store_n(&runloom_iouring_state.initialised, 1, __ATOMIC_RELEASE);
    return 0;

fail:
    if (sq_map  != MAP_FAILED) munmap(sq_map,  sq_size);
    if (cq_map  != MAP_FAILED) munmap(cq_map,  cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    __atomic_store_n(&runloom_iouring_state.initialised, -1, __ATOMIC_RELEASE);
    return -1;
}

