/* io_uring.c -- cooperative io_uring backend.
 *
 * Why we exist: pygo_core.fd_read / fd_write on regular files don't
 * work cooperatively through epoll -- regular file fds always report
 * "ready" so wait_fd is a no-op and the actual read/write blocks the
 * OS thread.  io_uring submits the read/write asynchronously to the
 * kernel; we park the goroutine and let other gs run while the kernel
 * processes the op.  When a completion is posted the kernel signals
 * an eventfd registered with the ring; the netpoll pump observes that
 * eventfd (epoll-registered), drains the CQ ring, and wakes the
 * goroutine that submitted each op.
 *
 * What we DON'T do: liburing.  Adding a build-time dependency on a
 * native library would compromise pygo's "pip install . just works"
 * story.  We talk to io_uring via the raw syscalls (io_uring_setup,
 * io_uring_enter, io_uring_register) and an mmap'd ring -- about 300
 * lines of code total.
 *
 * Backend availability is runtime-detected via the io_uring_setup
 * syscall returning -ENOSYS on old kernels (<5.1).  In that case
 * pygo_iouring_available() returns 0 and callers fall back to the
 * thread-pool path in monkey.py / pygo.sync.
 *
 * Concurrency model:
 *   - Submission is mutex-protected so multiple OS threads (the global
 *     scheduler thread and any M:N hub thread) can share the single
 *     ring.
 *   - Drain runs lock-free over the CQ ring; wakes are routed via
 *     pygo_sched_wake_safe (global sched g) or pygo_mn_wake_g (hub g)
 *     based on the per-op record's hub pointer.
 *   - The op record lives on the submitter's C stack.  The goroutine
 *     doesn't get torn down while parked, so the stack stays alive
 *     through to drain.
 *
 * Hub callers: the eventfd integration is wired into the GLOBAL netpoll
 * pump.  Within an M:N hub there's no shared pump that drains the ring
 * automatically, so hub callers take a synchronous spin-drain path
 * (block in io_uring_enter with min_complete=1 + drain inline).  This
 * regresses the hub case versus single-thread but is correct; future
 * work is one-ring-per-hub for full M:N coverage.
 */
#include "plat.h"

#if defined(__linux__)

#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "io_uring.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "plat_compat.h"
#include "pygo_sched.h"

/* IORING_REGISTER_EVENTFD opcode for io_uring_register.  Value is a
 * stable kernel ABI but some older Linux headers don't expose the
 * symbol; define a fallback. */
#ifndef IORING_REGISTER_EVENTFD
#  define IORING_REGISTER_EVENTFD 4
#endif

/* Provided-buffer-ring opcodes/symbols (Linux 5.19+).  Older kernel
 * headers don't define these; supply fallbacks so we can compile and
 * just feature-detect at runtime. */
#ifndef IORING_REGISTER_PBUF_RING
#  define IORING_REGISTER_PBUF_RING   22
#endif
#ifndef IORING_UNREGISTER_PBUF_RING
#  define IORING_UNREGISTER_PBUF_RING 23
#endif
#ifndef IOSQE_BUFFER_SELECT
#  define IOSQE_BUFFER_SELECT (1U << 5)
#endif
#ifndef IORING_CQE_F_BUFFER
#  define IORING_CQE_F_BUFFER 1U
#endif
#ifndef IORING_CQE_F_MORE
#  define IORING_CQE_F_MORE 2U
#endif
#ifndef IORING_CQE_BUFFER_SHIFT
#  define IORING_CQE_BUFFER_SHIFT 16
#endif
#ifndef IORING_RECV_MULTISHOT
#  define IORING_RECV_MULTISHOT (1U << 1)
#endif

/* Provided-buffer ring sizing.  Powers of two only; the ring uses
 * masking against (n - 1) to wrap indices.
 *
 * Sized to absorb the worst case: every armed multishot recv has one
 * CQE in flight at once.  N=4096 conns × 1 buffer each = 4096 entries.
 * At 2 KB/buffer that's 8 MB of pinned kernel-visible memory -- the
 * trade is acceptable for the high-concurrency workloads that
 * actually benefit from multishot.  Smaller pools risk -ENOBUFS
 * storms where the kernel ends a multishot mid-stream, the conn's
 * consumer goroutine has to re-arm, and another conn may grab the
 * buffer first -- with enough conns this stalls progress entirely. */
#define PYGO_IOURING_PBUF_COUNT    4096
#define PYGO_IOURING_PBUF_SIZE     2048
#define PYGO_IOURING_PBUF_BGID        0

/* Minimal kernel-shared structs for the buffer ring.  We could rely
 * on the libc UAPI header (linux/io_uring.h above), but it's been
 * around long enough on 5.19+ kernels that we just feature-test the
 * registration syscall at runtime and use these shadow declarations
 * for source compatibility with older build hosts. */
struct pygo_iouring_buf {
    uint64_t addr;
    uint32_t len;
    uint16_t bid;
    uint16_t resv;
};

struct pygo_iouring_buf_reg {
    uint64_t ring_addr;
    uint32_t ring_entries;
    uint16_t bgid;
    uint16_t flags;
    uint64_t resv[3];
};

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

/* Per-op record.  user_data on the SQE points to one of these so
 * drain can route the completion back to the right waiter.  For
 * "single" ops the record lives on the submitter's C stack across
 * the park; for "multishot" ops it lives as the first field of a
 * pygo_iouring_ms_t handle (heap-allocated, longer-lived). */
typedef enum {
    PYGO_IOURING_OP_SINGLE    = 0,
    PYGO_IOURING_OP_MULTISHOT = 1,
    PYGO_IOURING_OP_CANCEL    = 2,    /* fire-and-forget */
} pygo_iouring_op_type_t;

/* SINGLE-op park/wake handshake for hub (M:N) callers.  A hub goroutine
 * that submits a SINGLE op PARKS (coro_yield) instead of blocking the OS
 * thread; a drainer (the idle-hub netpoll pump) wakes it when the CQE
 * arrives.  Without a handshake the INLINE drain -- which runs ON the
 * submitter, synchronously, BEFORE it parks -- could wake a not-yet-parked
 * g via pygo_mn_wake_g, stranding it on the hub sub-list and double-
 * resuming it (this was "Bug 1", and the same latent hole exists in the
 * recv/send ring path).  `wait` is the single atomic commit point:
 *   INFLIGHT -> PARKED  (submitter, CAS): committed to yield; any drainer
 *                        that completes the op after this MUST wake us.
 *   * -> DONE           (drainer, exchange): record completion; wake the
 *                        submitter IFF the prior state was PARKED (i.e. it
 *                        had already committed to yielding).  A drainer that
 *                        sees INFLIGHT does NOT wake -- the submitter will
 *                        observe DONE on its own CAS and skip the park. */
#define PYGO_IOURING_WAIT_INFLIGHT 0
#define PYGO_IOURING_WAIT_PARKED   1
#define PYGO_IOURING_WAIT_DONE     2

typedef struct pygo_iouring_op {
    int       type;          /* pygo_iouring_op_type_t */
    pygo_g_t *g;             /* SINGLE: g to wake when this op completes */
    void     *hub;           /* opaque hub_t* or NULL for global sched */
    int32_t   result;        /* SINGLE: CQE res field; valid after wake */
    int       wait;          /* SINGLE hub op: park/wake commit (see above) */
} pygo_iouring_op_t;

/* Forward-declare for the drain handler. */
typedef struct pygo_iouring_ms pygo_iouring_ms_t;
static void pygo_iouring_ms_on_cqe(pygo_iouring_ms_t *h,
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
    struct io_uring_sqe *sqes;
    size_t sqe_mmap_size;
    void  *sqe_mmap;

    /* Completion queue */
    void  *cq_mmap;        size_t cq_mmap_size;
    unsigned *cq_head;     unsigned *cq_tail;
    unsigned  cq_mask;     unsigned  cq_entries;
    struct io_uring_cqe *cqes;

    /* Serialises SQE writes + io_uring_enter calls across threads. */
    pygo_mutex_t sub_lock;

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
    pygo_mutex_t bring_lock;       /* serialises producer-side tail writes */
} pygo_iouring_state_t;

static pygo_iouring_state_t pygo_iouring_state = {0};
static volatile long pygo_iouring_lock_inited = 0;

/* Inflight counter: incremented after a successful submit, decremented
 * by drain when a CQE is consumed.  Read by pygo_iouring_inflight()
 * so the scheduler drain loop knows not to exit while a goroutine is
 * parked on an iouring op. */
static volatile int pygo_iouring_inflight_count = 0;

/* Lazy initialise the submission mutex.  Same pattern as netpoll's
 * pygo_parked_lock_ensure_inited -- works for static-init absent
 * Windows but we always use it on Linux too for uniformity. */
static void pygo_iouring_lock_ensure_inited(void)
{
    long expected;
    if (__atomic_load_n(&pygo_iouring_lock_inited, __ATOMIC_ACQUIRE) == 2)
        return;
    expected = 0;
    if (__atomic_compare_exchange_n(&pygo_iouring_lock_inited, &expected, 1,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        pygo_mutex_init(&pygo_iouring_state.sub_lock);
        __atomic_store_n(&pygo_iouring_lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&pygo_iouring_lock_inited, __ATOMIC_ACQUIRE) != 2)
            { /* spin */ }
    }
}

/* Initialise the ring.  Lazy: only runs the first time
 * pygo_iouring_available() is called and only succeeds once.
 * Returns 0 on success, -1 on init failure / no kernel support. */
static int pygo_iouring_lazy_init(void)
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
        __atomic_store_n(&pygo_iouring_state.initialised, -1, __ATOMIC_RELEASE);
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

    pygo_iouring_state.ring_fd       = fd;
    pygo_iouring_state.eventfd_fd    = efd;
    pygo_iouring_state.sq_mmap       = sq_map;
    pygo_iouring_state.sq_mmap_size  = sq_size;
    pygo_iouring_state.cq_mmap       = cq_map;
    pygo_iouring_state.cq_mmap_size  = cq_size;
    pygo_iouring_state.sqe_mmap      = sqe_map;
    pygo_iouring_state.sqe_mmap_size = sqe_size;

    pygo_iouring_state.sq_head    = (unsigned *)((char *)sq_map + p.sq_off.head);
    pygo_iouring_state.sq_tail    = (unsigned *)((char *)sq_map + p.sq_off.tail);
    pygo_iouring_state.sq_mask    = *(unsigned *)((char *)sq_map + p.sq_off.ring_mask);
    pygo_iouring_state.sq_entries = *(unsigned *)((char *)sq_map + p.sq_off.ring_entries);
    pygo_iouring_state.sq_array   = (unsigned *)((char *)sq_map + p.sq_off.array);
    pygo_iouring_state.sqes       = (struct io_uring_sqe *)sqe_map;

    pygo_iouring_state.cq_head    = (unsigned *)((char *)cq_map + p.cq_off.head);
    pygo_iouring_state.cq_tail    = (unsigned *)((char *)cq_map + p.cq_off.tail);
    pygo_iouring_state.cq_mask    = *(unsigned *)((char *)cq_map + p.cq_off.ring_mask);
    pygo_iouring_state.cq_entries = *(unsigned *)((char *)cq_map + p.cq_off.ring_entries);
    pygo_iouring_state.cqes       = (struct io_uring_cqe *)((char *)cq_map + p.cq_off.cqes);

    /* initialised is published (atomic-release) only at the very END of init,
     * after the eventfd is hooked into the pump -- so a lock-free fast-path
     * reader in pygo_iouring_available() never observes a half-built ring. */

    /* Best-effort provided-buffer-ring setup for multishot recv.
     * Linux 5.19+; older kernels just leave pool_base = NULL and
     * callers fall back to single-shot recv.  Failure here is not a
     * hard error -- only the multishot path is affected. */
    {
        struct pygo_iouring_buf_reg reg;
        size_t pool_total = (size_t)PYGO_IOURING_PBUF_COUNT *
                            PYGO_IOURING_PBUF_SIZE;
        size_t bring_size = sizeof(struct pygo_iouring_buf) *
                            PYGO_IOURING_PBUF_COUNT;
        long page = sysconf(_SC_PAGESIZE);
        void *pool = NULL, *bring = NULL;
        if (page <= 0) page = 4096;
        if (posix_memalign(&pool, (size_t)page, pool_total) != 0)
            pool = NULL;
        if (posix_memalign(&bring, (size_t)page, bring_size) != 0)
            bring = NULL;
        if (pool != NULL && bring != NULL) {
            unsigned i;
            struct pygo_iouring_buf *bufs;
            memset(bring, 0, bring_size);
            bufs = (struct pygo_iouring_buf *)bring;
            memset(&reg, 0, sizeof(reg));
            reg.ring_addr    = (uintptr_t)bring;
            reg.ring_entries = PYGO_IOURING_PBUF_COUNT;
            reg.bgid         = PYGO_IOURING_PBUF_BGID;
            reg.flags        = 0;
            if (sys_io_uring_register(fd, IORING_REGISTER_PBUF_RING,
                                      &reg, 1) == 0) {
                /* Populate the ring: each buffer at index i refers to
                 * pool_base + i*BUF_SIZE with bid=i.  After we fill the
                 * descriptors, bump the tail by N so the kernel sees N
                 * usable buffers immediately. */
                for (i = 0; i < PYGO_IOURING_PBUF_COUNT; i++) {
                    bufs[i].addr = (uintptr_t)((char *)pool +
                                   (size_t)i * PYGO_IOURING_PBUF_SIZE);
                    bufs[i].len  = PYGO_IOURING_PBUF_SIZE;
                    bufs[i].bid  = (uint16_t)i;
                    bufs[i].resv = 0;
                }
                /* The kernel tail lives in bufs[0].resv (per the
                 * io_uring_buf_ring union).  Atomic store-release so
                 * the kernel observes a consistent view of all
                 * descriptors before the new tail. */
                __atomic_store_n(&bufs[0].resv,
                                 (uint16_t)PYGO_IOURING_PBUF_COUNT,
                                 __ATOMIC_RELEASE);
                pygo_iouring_state.pool_base     = pool;
                pygo_iouring_state.pool_total    = pool_total;
                pygo_iouring_state.pool_n        = PYGO_IOURING_PBUF_COUNT;
                pygo_iouring_state.pool_buf_size = PYGO_IOURING_PBUF_SIZE;
                pygo_iouring_state.bring_mem     = bring;
                pygo_iouring_state.bring_size    = bring_size;
                pygo_iouring_state.bring_mask    = PYGO_IOURING_PBUF_COUNT - 1;
                pygo_mutex_init(&pygo_iouring_state.bring_lock);
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
    if (pygo_netpoll_add_iouring_eventfd(efd) != 0) {
        /* Don't tear down the ring; just abort init.  Mark as failed so
         * callers fall back. */
        munmap(sq_map,  sq_size);
        munmap(cq_map,  cq_size);
        munmap(sqe_map, sqe_size);
        close(efd);
        close(fd);
        pygo_iouring_state.eventfd_fd = -1;
        pygo_iouring_state.ring_fd    = -1;
        __atomic_store_n(&pygo_iouring_state.initialised, -1, __ATOMIC_RELEASE);
        return -1;
    }
    /* Fully built AND hooked into the pump: publish ready, release. */
    __atomic_store_n(&pygo_iouring_state.initialised, 1, __ATOMIC_RELEASE);
    return 0;

fail:
    if (sq_map  != MAP_FAILED) munmap(sq_map,  sq_size);
    if (cq_map  != MAP_FAILED) munmap(cq_map,  cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    __atomic_store_n(&pygo_iouring_state.initialised, -1, __ATOMIC_RELEASE);
    return -1;
}

int pygo_iouring_available(void)
{
    if (__atomic_load_n(&pygo_iouring_state.initialised, __ATOMIC_ACQUIRE) == 0) {
        /* Serialize lazy init.  Concurrent first-callers -- e.g. several M:N
         * hubs each running a goroutine that touches io_uring at the same
         * instant, with no prior single-threaded available() call -- must NOT
         * each run lazy_init: that io_uring_setup()s multiple rings and races
         * the shared ring-state pointers (ring_fd / sq_tail / sqes / ...), so
         * submits land in different/short-lived rings, SQE slots get
         * overwritten, and ops vanish without ever completing (an intermittent
         * multi-hub "lost completion" hang -- masked whenever a test happens to
         * call iouring_available() once on the main thread first).
         * lock_ensure_inited gives us an initialized sub_lock; double-check
         * `initialised` under it so EXACTLY ONE caller runs lazy_init, and hold
         * it across init so no submit_sqe (also sub_lock) sees a half-built
         * ring. */
        pygo_iouring_lock_ensure_inited();
        pygo_mutex_lock(&pygo_iouring_state.sub_lock);
        if (pygo_iouring_state.initialised == 0) {
            pygo_iouring_lazy_init();
        }
        pygo_mutex_unlock(&pygo_iouring_state.sub_lock);
    }
    return __atomic_load_n(&pygo_iouring_state.initialised, __ATOMIC_ACQUIRE) == 1;
}

int pygo_iouring_eventfd(void)
{
    if (!pygo_iouring_available()) return -1;
    return pygo_iouring_state.eventfd_fd;
}

int pygo_iouring_inflight(void)
{
    return __atomic_load_n(&pygo_iouring_inflight_count, __ATOMIC_ACQUIRE);
}

int pygo_iouring_pbuf_available(void)
{
    if (!pygo_iouring_available()) return 0;
    return pygo_iouring_state.pool_base != NULL;
}

unsigned pygo_iouring_pbuf_size(void)
{
    return pygo_iouring_state.pool_buf_size;
}

unsigned pygo_iouring_pbuf_count(void)
{
    return pygo_iouring_state.pool_n;
}

void *pygo_iouring_pbuf_addr(unsigned bid)
{
    pygo_iouring_state_t *s = &pygo_iouring_state;
    if (s->pool_base == NULL) return NULL;
    if (bid >= s->pool_n) return NULL;
    return (char *)s->pool_base + (size_t)bid * s->pool_buf_size;
}

void pygo_iouring_pbuf_return(unsigned bid)
{
    pygo_iouring_state_t *s = &pygo_iouring_state;
    struct pygo_iouring_buf *bufs;
    uint16_t tail, idx;
    if (s->pool_base == NULL) return;
    if (bid >= s->pool_n) return;
    bufs = (struct pygo_iouring_buf *)s->bring_mem;

    pygo_mutex_lock(&s->bring_lock);
    /* The kernel's tail is overlaid on bufs[0].resv.  Load with
     * acquire so we observe consumer-side advancement (the kernel
     * advances head as it takes buffers, but tail is purely producer-
     * side; we're the only producer, so the load is mostly for
     * ordering against the store below). */
    tail = __atomic_load_n(&bufs[0].resv, __ATOMIC_ACQUIRE);
    idx  = tail & s->bring_mask;
    bufs[idx].addr = (uintptr_t)((char *)s->pool_base +
                     (size_t)bid * s->pool_buf_size);
    bufs[idx].len  = s->pool_buf_size;
    bufs[idx].bid  = (uint16_t)bid;
    /* idx==0 means we just overwrote bufs[0].addr/len/bid; the resv
     * field (kernel tail) is the same memory location as our local
     * `tail` variable, but we're about to bump tail by 1 in the store
     * below so the kernel sees the new entry. */
    __atomic_store_n(&bufs[0].resv,
                     (uint16_t)(tail + 1), __ATOMIC_RELEASE);
    pygo_mutex_unlock(&s->bring_lock);
}

/* Submit one SQE.  Caller has filled sqe_template (opcode/fd/addr/len/
 * off); we set user_data to the op pointer and submit without waiting.
 * Returns 0 on success, -1 with errno on failure. */
static int pygo_iouring_submit_sqe(struct io_uring_sqe sqe_template,
                                   pygo_iouring_op_t *op)
{
    pygo_iouring_state_t *s = &pygo_iouring_state;
    unsigned tail, head, idx;
    int n;

    pygo_mutex_lock(&s->sub_lock);

    /* Wait for a free SQE slot.  Ring is 64 deep; in practice we
     * never spin here for the workloads pygo targets, but if every
     * slot is in-flight we must block submitting until the kernel
     * consumes some.  io_uring_enter with submit=0,wait=0 is cheap;
     * but the kernel doesn't progress the SQ ring without an enter,
     * so we just drain CQ inline here to make room. */
    while (1) {
        tail = __atomic_load_n(s->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(s->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < s->sq_entries) break;
        /* Ring full -- spin-drain CQ to free slots. */
        pygo_mutex_unlock(&s->sub_lock);
        pygo_iouring_drain();
        pygo_mutex_lock(&s->sub_lock);
    }

    idx = tail & s->sq_mask;
    s->sqes[idx] = sqe_template;
    s->sqes[idx].user_data = (uint64_t)(uintptr_t)op;
    s->sq_array[idx] = idx;
    __atomic_store_n(s->sq_tail, tail + 1, __ATOMIC_RELEASE);

    /* Submit without waiting -- the kernel takes the SQE and we
     * return immediately.  The CQE will arrive whenever the op
     * completes; the eventfd notifies the netpoll pump. */
    n = sys_io_uring_submit_eintr(s->ring_fd, 1);   /* retry on EINTR */
    pygo_mutex_unlock(&s->sub_lock);
    if (n < 0) return -1;
    __atomic_add_fetch(&pygo_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    return 0;
}

void pygo_iouring_drain(void)
{
    pygo_iouring_state_t *s = &pygo_iouring_state;
    if (s->initialised != 1) return;

    /* Drain the eventfd counter so the kernel re-arms the EPOLLET edge
     * on the next CQE post.  Idempotent: if the eventfd isn't set
     * (manual drain call) the non-blocking read returns -EAGAIN
     * immediately. */
    if (s->eventfd_fd >= 0) {
        uint64_t scratch;
        while (read(s->eventfd_fd, &scratch, sizeof(scratch))
               == (ssize_t)sizeof(scratch))
            { /* drain */ }
    }

    /* Multi-drainer safety: the netpoll pump (whichever hub goes idle)
     * AND the inline drain after submit AND the hub spin-drain path
     * all call this without any external mutex.  On free-threaded
     * Python (3.13t) these can run truly in parallel; under the GIL
     * they only serialise at Python-level boundaries which most of
     * this function doesn't cross.  Without a CAS on cq_head two
     * drainers could load the same head, process the same CQE
     * (double-wake the goroutine, free a cancel record twice), and
     * each blindly store head+1 -- a single CQE consumed twice with
     * inflight decremented twice.  Pre-fix this manifested as
     * gradual M:N hangs (the spurious wake left the g in a "ready
     * twice" state that drove an unrelated parker into use-after-
     * free).  The CAS turns the head advance into a per-CQE claim:
     * exactly one drainer succeeds for each CQE; the loser re-loads
     * and retries. */
    for (;;) {
        unsigned head = __atomic_load_n(s->cq_head, __ATOMIC_ACQUIRE);
        unsigned ct   = __atomic_load_n(s->cq_tail, __ATOMIC_ACQUIRE);
        struct io_uring_cqe *cqe;
        pygo_iouring_op_t *op;
        int32_t  res;
        uint32_t flags;
        int      type;
        int      cqe_final;
        unsigned expected;
        if (head == ct) return;
        cqe   = &s->cqes[head & s->cq_mask];
        res   = cqe->res;
        flags = cqe->flags;
        op    = (pygo_iouring_op_t *)(uintptr_t)cqe->user_data;
        type      = (op != NULL) ? op->type : PYGO_IOURING_OP_SINGLE;
        cqe_final = (type != PYGO_IOURING_OP_MULTISHOT) ||
                    !(flags & IORING_CQE_F_MORE);
        /* Claim this CQE via CAS on cq_head.  On contention, the
         * other drainer wins -- we re-load and try the next CQE.
         * SEQ_CST so multi-thread observers see a coherent head. */
        expected = head;
        if (!__atomic_compare_exchange_n(s->cq_head, &expected, head + 1,
                                         0, __ATOMIC_ACQ_REL,
                                         __ATOMIC_ACQUIRE)) {
            continue;   /* another drainer took it; reload */
        }

        if (op != NULL) {
            switch (type) {
            case PYGO_IOURING_OP_SINGLE:
                /* Publish the result BEFORE the wake handshake so a woken
                 * (or self-recovering) submitter sees it. */
                __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
                if (op->hub != NULL) {
                    /* Hub (M:N) op: the submitter parks via coro_yield.
                     * Claim the completion with an exchange and wake ONLY if
                     * the submitter had already committed to parking
                     * (wait == PARKED).  If it is still INFLIGHT -- e.g. this
                     * is the submitter's own INLINE drain, before it parks --
                     * we must NOT wake: pygo_mn_wake_g would hub-submit a
                     * still-running g and double-resume it (Bug 1).  The
                     * submitter will instead observe DONE on its own CAS and
                     * skip the park entirely. */
                    int prev = __atomic_exchange_n(&op->wait,
                                                   PYGO_IOURING_WAIT_DONE,
                                                   __ATOMIC_ACQ_REL);
                    if (prev == PYGO_IOURING_WAIT_PARKED) {
                        pygo_mn_wake_g(op->hub, op->g);
                    }
                } else if (op->g != NULL) {
                    /* Single-thread sched: race-safe via wake_pending (if the
                     * submitter hasn't parked yet park_safe just decrements). */
                    pygo_sched_wake_safe(op->g);
                }
                break;
            case PYGO_IOURING_OP_MULTISHOT:
                pygo_iouring_ms_on_cqe((pygo_iouring_ms_t *)op, res, flags);
                break;
            case PYGO_IOURING_OP_CANCEL:
                /* Fire-and-forget: the cancel op record is heap-
                 * allocated by ms_close; once the CQE arrives the
                 * record is no longer needed. */
                free(op);
                break;
            }
        }
        if (cqe_final) {
            __atomic_sub_fetch(&pygo_iouring_inflight_count, 1,
                               __ATOMIC_ACQ_REL);
        }
    }
}

/* Common submit-and-wait path for pread/pwrite.  Returns CQE result
 * (>= 0 on success) or -errno on failure. */
static pygo_iouring_ssize_t pygo_iouring_do(struct io_uring_sqe sqe)
{
    pygo_iouring_op_t op;
    void *hub;

    if (!pygo_iouring_available()) {
        errno = ENOSYS;
        return -1;
    }

    hub = pygo_mn_current_hub_opaque();
    op.type   = PYGO_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = (hub != NULL) ? pygo_mn_tls_current_g()
                              : pygo_sched_get()->current;
    op.result = INT32_MIN;        /* sentinel: not yet completed */
    op.wait   = PYGO_IOURING_WAIT_INFLIGHT;

    if (pygo_iouring_submit_sqe(sqe, &op) != 0) return -1;

    /* Inline drain after submit.  With IORING_FEAT_FAST_POLL (5.7+)
     * the kernel completes inline when data is already ready, so the
     * CQE is in the ring before io_uring_enter even returns.
     * Draining here turns "submit + park + pump + drain + wake +
     * resume" into "submit + drain + return" for the data-ready
     * case, saving an entire scheduler round-trip per RT in the
     * common echo workload.  Runs while op.wait == INFLIGHT, so for
     * OUR op the drain just sets result + flips wait to DONE (no wake);
     * we observe that below and skip the park. */
    pygo_iouring_drain();

    if (hub != NULL) {
        /* Hub (M:N) path: PARK -- do NOT block the OS thread.  A blocking
         * io_uring_enter here holds this hub's tstate ATTACHED across the
         * wait, which deadlocks any stop-the-world (free-threaded GC, the
         * per-g-tstate attach protocol) whose completion depends on a thread
         * frozen at the STW barrier -- and it monopolizes the hub so its
         * other goroutines can't run.  Yield instead: the hub returns to its
         * (STW-safe, handoff-compatible) main loop, and when idle pumps the
         * shared ring's eventfd; a drainer wakes us via the wait handshake.
         * The inline drain above already completed us inline in the common
         * data-ready case (wait == DONE) -- skip the park then. */
        if (__atomic_load_n(&op.wait, __ATOMIC_ACQUIRE)
                != PYGO_IOURING_WAIT_DONE) {
            int prev = PYGO_IOURING_WAIT_INFLIGHT;
            if (__atomic_compare_exchange_n(&op.wait, &prev,
                                            PYGO_IOURING_WAIT_PARKED, 0,
                                            __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                /* Committed to park: any drainer completing the op now
                 * observes PARKED and wakes us.  park_current marks the g
                 * off-queue (hub_main won't re-enqueue on yield-return);
                 * the wake routes it back onto the hub sub-list, and the
                 * in_sub_queue CAS in hub_submit dedups a wake that races
                 * our coro_yield. */
                pygo_sched_park_current();
                pygo_coro_yield();
            }
            /* else prev == DONE: a concurrent drainer completed the op
             * between the load and the CAS; op.result is set, fall through. */
        }
    } else if (op.g != NULL) {
        /* Single-thread sched path: park cooperatively.  When the
         * eventfd fires the netpoll pump calls pygo_iouring_drain
         * which calls pygo_sched_wake_safe(op.g).  park_safe yields
         * via pygo_coro_yield; on resume we eat the pending wake. */
        pygo_sched_park_safe();
    } else {
        /* Not inside a goroutine (plain thread / main thread calling
         * file_read directly).  Block, but RELEASE the tstate around the
         * wait so a stop-the-world isn't held off by this syscall. */
        while (__atomic_load_n(&op.result, __ATOMIC_ACQUIRE) == INT32_MIN) {
            int n;
            Py_BEGIN_ALLOW_THREADS
            n = sys_io_uring_enter(pygo_iouring_state.ring_fd, 0, 1,
                                   IORING_ENTER_GETEVENTS, NULL, 0);
            Py_END_ALLOW_THREADS
            if (n < 0 && errno != EINTR) return -1;
            pygo_iouring_drain();
        }
    }

    {
        int32_t r = __atomic_load_n(&op.result, __ATOMIC_ACQUIRE);
        if (r < 0) { errno = -r; return -1; }
        return r;
    }
}

pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n,
                                        pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_READ;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return pygo_iouring_do(sqe);
}

pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n,
                                         pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_WRITE;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return pygo_iouring_do(sqe);
}

/* ============================================================
 * Multishot recv handle (Linux 6.0+ multishot + 5.19+ provided
 * buffer ring).
 *
 * Per fd, one in-flight multishot SQE; the kernel produces a CQE
 * each time data arrives, picking a buffer from the bgid=0 provided
 * buffer ring.  The handle queues received-but-not-yet-consumed
 * buffers and serves them to ms_recv calls.
 * ============================================================ */

typedef struct ms_entry {
    uint16_t  bid;
    uint16_t  len;
    struct ms_entry *next;
} ms_entry_t;

struct pygo_iouring_ms {
    pygo_iouring_op_t op;        /* must be first; SQE user_data */
    int               fd;
    int               closing;   /* ms_close called */
    int               err;       /* sticky errno or 0 */
    int               eof;       /* kernel signalled orderly EOF */
    int               armed;     /* multishot SQE in-flight */

    /* Ready buffer queue (head=oldest, tail=newest). */
    ms_entry_t       *ready_head;
    ms_entry_t       *ready_tail;

    /* Partially-consumed buffer carried between ms_recv calls. */
    int               inflight_bid;    /* -1 if none */
    uint32_t          inflight_off;
    uint32_t          inflight_len;

    /* Single waiter (most TCPConns have one consumer goroutine). */
    pygo_g_t         *waiter_g;
    void             *waiter_hub;

    pygo_mutex_t      lock;
};

/* Submit a multishot recv SQE for handle h.  Sets h->armed on
 * success.  Caller must hold h->lock. */
static int pygo_iouring_ms_submit(pygo_iouring_ms_t *h)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.flags     = IOSQE_BUFFER_SELECT;
    sqe.fd        = h->fd;
    /* addr/len = 0 with BUFFER_SELECT means "kernel picks one from the
     * provided-buffer ring at buf_group". */
    sqe.ioprio    = IORING_RECV_MULTISHOT;
    sqe.buf_group = PYGO_IOURING_PBUF_BGID;
    if (pygo_iouring_submit_sqe(sqe, &h->op) != 0) return -1;
    h->armed = 1;
    return 0;
}

pygo_iouring_ms_t *pygo_iouring_ms_open(int fd)
{
    pygo_iouring_ms_t *h;
    if (!pygo_iouring_pbuf_available()) return NULL;
    h = (pygo_iouring_ms_t *)calloc(1, sizeof(*h));
    if (h == NULL) return NULL;
    h->op.type = PYGO_IOURING_OP_MULTISHOT;
    h->fd      = fd;
    h->inflight_bid = -1;
    pygo_mutex_init(&h->lock);

    pygo_mutex_lock(&h->lock);
    if (pygo_iouring_ms_submit(h) != 0) {
        pygo_mutex_unlock(&h->lock);
        pygo_mutex_destroy(&h->lock);
        free(h);
        return NULL;
    }
    pygo_mutex_unlock(&h->lock);
    return h;
}

/* Drain handler.  Called from pygo_iouring_drain when a CQE arrives
 * with user_data pointing to a multishot handle.  Appends the
 * delivered buffer to the handle's ready queue and wakes any
 * parked consumer goroutine.  If the multishot ended (no F_MORE),
 * clears the armed flag so the next recv re-submits. */
static void pygo_iouring_ms_on_cqe(pygo_iouring_ms_t *h,
                                   int32_t res, uint32_t flags)
{
    int has_buffer = (flags & IORING_CQE_F_BUFFER) != 0;
    int more       = (flags & IORING_CQE_F_MORE) != 0;
    pygo_g_t *wake_g  = NULL;
    void     *wake_hub = NULL;
    int       was_closing;

    pygo_mutex_lock(&h->lock);

    if (has_buffer && res > 0) {
        ms_entry_t *e = (ms_entry_t *)malloc(sizeof(*e));
        if (e != NULL) {
            e->bid  = (uint16_t)(flags >> IORING_CQE_BUFFER_SHIFT);
            e->len  = (uint16_t)res;
            e->next = NULL;
            if (h->ready_tail) h->ready_tail->next = e;
            else               h->ready_head = e;
            h->ready_tail = e;
        } else {
            /* OOM: data is lost; return the buffer so the kernel can
             * reuse it.  Conn stays usable. */
            uint16_t bid = (uint16_t)(flags >> IORING_CQE_BUFFER_SHIFT);
            pygo_iouring_pbuf_return(bid);
        }
    } else if (res == 0 && !more) {
        h->eof = 1;
    } else if (res < 0) {
        /* -ENOBUFS = kernel ran out of buffers; re-arm on next recv.
         * -ECANCELED arrives in response to ms_close's cancel SQE. */
        if (res != -ENOBUFS && res != -ECANCELED) {
            h->err = -res;
        }
    }

    if (!more) h->armed = 0;

    /* Capture the waiter under the lock so we don't double-wake.
     * Actual wake call goes outside the lock for latency. */
    wake_g   = h->waiter_g;
    wake_hub = h->waiter_hub;
    h->waiter_g = NULL;

    was_closing = h->closing;
    pygo_mutex_unlock(&h->lock);

    if (wake_g != NULL) {
        if (wake_hub) pygo_mn_wake_g(wake_hub, wake_g);
        else          pygo_sched_wake_safe(wake_g);
    }

    /* If the handle is closing AND the multishot has ended, this
     * was the last CQE we'll ever see for it -- safe to free. */
    if (was_closing && !more) {
        ms_entry_t *e, *next;
        e = h->ready_head;
        while (e != NULL) {
            next = e->next;
            pygo_iouring_pbuf_return(e->bid);
            free(e);
            e = next;
        }
        if (h->inflight_bid >= 0)
            pygo_iouring_pbuf_return((unsigned)h->inflight_bid);
        pygo_mutex_destroy(&h->lock);
        free(h);
    }
}

pygo_iouring_ssize_t pygo_iouring_ms_recv(pygo_iouring_ms_t *h,
                                          void *buf, size_t n)
{
    char *out = (char *)buf;
    size_t out_off = 0;

    if (h == NULL) { errno = EINVAL; return -1; }
    if (n == 0)    return 0;

    pygo_mutex_lock(&h->lock);
    for (;;) {
        /* 1. Drain any partially-consumed in-flight buffer first. */
        if (h->inflight_bid >= 0) {
            size_t avail = h->inflight_len - h->inflight_off;
            size_t want  = n - out_off;
            size_t take  = (avail < want) ? avail : want;
            void *src = pygo_iouring_pbuf_addr((unsigned)h->inflight_bid);
            if (src != NULL && take > 0) {
                memcpy(out + out_off,
                       (char *)src + h->inflight_off, take);
                out_off += take;
                h->inflight_off += (uint32_t)take;
            }
            if (h->inflight_off >= h->inflight_len) {
                pygo_iouring_pbuf_return((unsigned)h->inflight_bid);
                h->inflight_bid = -1;
                h->inflight_off = 0;
                h->inflight_len = 0;
            }
            if (out_off >= n) {
                pygo_mutex_unlock(&h->lock);
                return (pygo_iouring_ssize_t)out_off;
            }
            /* User wants more.  Loop to grab another buffer. */
        }

        /* 2. Move next ready buffer into the in-flight slot. */
        if (h->ready_head != NULL) {
            ms_entry_t *e = h->ready_head;
            h->ready_head = e->next;
            if (h->ready_head == NULL) h->ready_tail = NULL;
            h->inflight_bid = e->bid;
            h->inflight_off = 0;
            h->inflight_len = e->len;
            free(e);
            continue;
        }

        /* 3. No data right now.  If we already copied some, return. */
        if (out_off > 0) {
            pygo_mutex_unlock(&h->lock);
            return (pygo_iouring_ssize_t)out_off;
        }

        /* 4. EOF / sticky error. */
        if (h->eof) {
            pygo_mutex_unlock(&h->lock);
            return 0;
        }
        if (h->err) {
            int e = h->err;
            pygo_mutex_unlock(&h->lock);
            errno = e;
            return -1;
        }

        /* 5. Re-arm if multishot ended (e.g. earlier -ENOBUFS). */
        if (!h->armed) {
            if (pygo_iouring_ms_submit(h) != 0) {
                int e = errno;
                pygo_mutex_unlock(&h->lock);
                errno = e;
                return -1;
            }
        }

        /* 6. Park.  Capture our g under the lock, release, then yield. */
        {
            void *hub = pygo_mn_current_hub_opaque();
            h->waiter_hub = hub;
            h->waiter_g   = (hub != NULL) ? pygo_mn_tls_current_g()
                                          : pygo_sched_get()->current;
            pygo_mutex_unlock(&h->lock);
            if (hub != NULL) {
                /* Hub callers can't ride wake_pending (bound to the
                 * global sched), so spin-drain via io_uring_enter. */
                for (;;) {
                    int n2;
                    pygo_mutex_lock(&h->lock);
                    if (h->ready_head || h->eof || h->err ||
                        h->inflight_bid >= 0) {
                        pygo_mutex_unlock(&h->lock);
                        break;
                    }
                    pygo_mutex_unlock(&h->lock);
                    n2 = sys_io_uring_enter(pygo_iouring_state.ring_fd,
                                            0, 1,
                                            IORING_ENTER_GETEVENTS,
                                            NULL, 0);
                    if (n2 < 0 && errno != EINTR) return -1;
                    pygo_iouring_drain();
                }
            } else {
                pygo_sched_park_safe();
            }
            pygo_mutex_lock(&h->lock);
            /* Loop back to top to consume whatever drain delivered. */
        }
    }
}

void pygo_iouring_ms_close(pygo_iouring_ms_t *h)
{
    pygo_iouring_op_t *cancel_op;
    struct io_uring_sqe sqe;
    int armed_snapshot;

    if (h == NULL) return;

    pygo_mutex_lock(&h->lock);
    h->closing = 1;
    armed_snapshot = h->armed;
    pygo_mutex_unlock(&h->lock);

    if (!armed_snapshot) {
        /* Multishot already terminated; safe to free immediately. */
        ms_entry_t *e, *next;
        e = h->ready_head;
        while (e != NULL) {
            next = e->next;
            pygo_iouring_pbuf_return(e->bid);
            free(e);
            e = next;
        }
        if (h->inflight_bid >= 0)
            pygo_iouring_pbuf_return((unsigned)h->inflight_bid);
        pygo_mutex_destroy(&h->lock);
        free(h);
        return;
    }

    /* Fire-and-forget cancel.  drain frees the cancel op record when
     * the cancel CQE arrives; the multishot's final CQE (with
     * F_MORE clear) triggers the handle free in on_cqe. */
    cancel_op = (pygo_iouring_op_t *)calloc(1, sizeof(*cancel_op));
    if (cancel_op == NULL) {
        /* OOM: leak the handle; the kernel will eventually deliver
         * -ECANCELED when the fd closes, and on_cqe with closing=1
         * will free.  Best-effort. */
        return;
    }
    cancel_op->type = PYGO_IOURING_OP_CANCEL;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_ASYNC_CANCEL;
    sqe.addr   = (uintptr_t)&h->op;     /* match SQE by its user_data */
    sqe.fd     = -1;
    if (pygo_iouring_submit_sqe(sqe, cancel_op) != 0) {
        free(cancel_op);
    }
}

pygo_iouring_ssize_t pygo_iouring_recv(int fd, void *buf, size_t n, int flags)
{
    struct io_uring_sqe sqe;
    /* Route through the hub's per-thread ring if we're inside a hub
     * and the hub created its ring successfully.  That bypasses the
     * global ring's submission mutex and the legacy spin-drain hub
     * path -- the hub g parks via coro_yield and the shared netpoll
     * pump drains the hub ring's eventfd. */
    pygo_iouring_ring_t *hub_ring = pygo_mn_current_iouring_ring();
    if (hub_ring != NULL) {
        return pygo_iouring_ring_recv(hub_ring, fd, buf, n, flags);
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;     /* MSG_* recv flags */
    return pygo_iouring_do(sqe);
}

pygo_iouring_ssize_t pygo_iouring_send(int fd, const void *buf, size_t n, int flags)
{
    struct io_uring_sqe sqe;
    pygo_iouring_ring_t *hub_ring = pygo_mn_current_iouring_ring();
    if (hub_ring != NULL) {
        return pygo_iouring_ring_send(hub_ring, fd, buf, n, flags);
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode   = IORING_OP_SEND;
    sqe.fd       = fd;
    sqe.addr     = (uintptr_t)buf;
    sqe.len      = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return pygo_iouring_do(sqe);
}

/* ============================================================
 * Per-hub rings (Linux 5.18+ SINGLE_ISSUER, 6.1+ DEFER_TASKRUN).
 *
 * One ring per M:N hub.  Hub thread is the SINGLE issuer + drainer of
 * its ring, so no submission mutex is needed and SINGLE_ISSUER + (opt-
 * in) DEFER_TASKRUN are kernel-side correct.  See io_uring.h for the
 * public API contract.
 *
 * Memory layout mirrors the global ring (sq/cq mmaps, sqe array,
 * eventfd) minus the provided-buffer ring -- multishot stays on the
 * global ring so the buffer pool isn't fragmented N ways.
 * ============================================================ */

/* Setup-flag values that older glibc UAPI headers might miss.  Fall
 * back to the kernel-stable values. */
#ifndef IORING_SETUP_SINGLE_ISSUER
#  define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_DEFER_TASKRUN
#  define IORING_SETUP_DEFER_TASKRUN (1U << 13)
#endif

struct pygo_iouring_ring {
    int ring_fd;
    int eventfd_fd;
    int defer_taskrun;          /* 1 if DEFER_TASKRUN is enabled */

    /* SQ ring */
    void  *sq_mmap;             size_t sq_mmap_size;
    unsigned *sq_head;          unsigned *sq_tail;
    unsigned  sq_mask;          unsigned  sq_entries;
    unsigned *sq_array;
    struct io_uring_sqe *sqes;
    void  *sqe_mmap;            size_t sqe_mmap_size;

    /* CQ ring */
    void  *cq_mmap;             size_t cq_mmap_size;
    unsigned *cq_head;          unsigned *cq_tail;
    unsigned  cq_mask;          unsigned  cq_entries;
    struct io_uring_cqe *cqes;

    /* Per-ring inflight counter.  Hub_main checks this when deciding
     * pump-vs-sleep so the hub keeps spinning the pump while any of
     * its iouring ops are outstanding. */
    volatile int inflight;
};

/* Hub-ring init.  Tries SINGLE_ISSUER + (optionally) DEFER_TASKRUN;
 * if the kernel rejects either flag, retries without it once.  This
 * keeps creation portable across 5.1+ kernels without per-syscall
 * feature probing. */
pygo_iouring_ring_t *pygo_iouring_ring_create(int defer_taskrun)
{
    struct io_uring_params p;
    pygo_iouring_ring_t *r;
    int fd, efd;
    void *sq_map = MAP_FAILED, *cq_map = MAP_FAILED, *sqe_map = MAP_FAILED;
    size_t sq_size = 0, cq_size = 0, sqe_size = 0;
    int reg_arg;
    unsigned flags;

    r = (pygo_iouring_ring_t *)calloc(1, sizeof(*r));
    if (r == NULL) { errno = ENOMEM; return NULL; }
    r->ring_fd = r->eventfd_fd = -1;

    /* Attempt full flag set; downgrade on EINVAL. */
    flags = IORING_SETUP_SINGLE_ISSUER;
    if (defer_taskrun) flags |= IORING_SETUP_DEFER_TASKRUN;
    memset(&p, 0, sizeof(p));
    p.flags = flags;
    fd = sys_io_uring_setup(64, &p);
    if (fd < 0 && errno == EINVAL && defer_taskrun) {
        /* DEFER_TASKRUN unsupported.  Retry without it; SINGLE_ISSUER
         * came in 5.18 so it's the more likely survivor. */
        memset(&p, 0, sizeof(p));
        p.flags = IORING_SETUP_SINGLE_ISSUER;
        fd = sys_io_uring_setup(64, &p);
        defer_taskrun = 0;
    }
    if (fd < 0 && errno == EINVAL) {
        /* SINGLE_ISSUER unsupported too (5.1-5.17 kernels).  Plain
         * setup -- still correct, just multi-issuer-tolerant. */
        memset(&p, 0, sizeof(p));
        fd = sys_io_uring_setup(64, &p);
    }
    if (fd < 0) {
        free(r);
        return NULL;
    }
    r->defer_taskrun = defer_taskrun;

    sq_size = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    sq_map = mmap(NULL, sq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq_map == MAP_FAILED) goto fail;

    cq_size = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    cq_map = mmap(NULL, cq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
    if (cq_map == MAP_FAILED) goto fail;

    sqe_size = p.sq_entries * sizeof(struct io_uring_sqe);
    sqe_map = mmap(NULL, sqe_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (sqe_map == MAP_FAILED) goto fail;

    efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) goto fail;

    reg_arg = efd;
    if (sys_io_uring_register(fd, IORING_REGISTER_EVENTFD, &reg_arg, 1) < 0) {
        close(efd);
        goto fail;
    }

    r->ring_fd       = fd;
    r->eventfd_fd    = efd;
    r->sq_mmap       = sq_map;       r->sq_mmap_size  = sq_size;
    r->cq_mmap       = cq_map;       r->cq_mmap_size  = cq_size;
    r->sqe_mmap      = sqe_map;      r->sqe_mmap_size = sqe_size;
    r->sq_head    = (unsigned *)((char *)sq_map + p.sq_off.head);
    r->sq_tail    = (unsigned *)((char *)sq_map + p.sq_off.tail);
    r->sq_mask    = *(unsigned *)((char *)sq_map + p.sq_off.ring_mask);
    r->sq_entries = *(unsigned *)((char *)sq_map + p.sq_off.ring_entries);
    r->sq_array   = (unsigned *)((char *)sq_map + p.sq_off.array);
    r->sqes       = (struct io_uring_sqe *)sqe_map;
    r->cq_head    = (unsigned *)((char *)cq_map + p.cq_off.head);
    r->cq_tail    = (unsigned *)((char *)cq_map + p.cq_off.tail);
    r->cq_mask    = *(unsigned *)((char *)cq_map + p.cq_off.ring_mask);
    r->cq_entries = *(unsigned *)((char *)cq_map + p.cq_off.ring_entries);
    r->cqes       = (struct io_uring_cqe *)((char *)cq_map + p.cq_off.cqes);
    return r;

fail:
    if (sq_map  != MAP_FAILED) munmap(sq_map,  sq_size);
    if (cq_map  != MAP_FAILED) munmap(cq_map,  cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    free(r);
    return NULL;
}

void pygo_iouring_ring_destroy(pygo_iouring_ring_t *r)
{
    if (r == NULL) return;
    if (r->sq_mmap  != NULL) munmap(r->sq_mmap,  r->sq_mmap_size);
    if (r->cq_mmap  != NULL) munmap(r->cq_mmap,  r->cq_mmap_size);
    if (r->sqe_mmap != NULL) munmap(r->sqe_mmap, r->sqe_mmap_size);
    if (r->eventfd_fd >= 0) close(r->eventfd_fd);
    if (r->ring_fd    >= 0) close(r->ring_fd);
    free(r);
}

int pygo_iouring_ring_eventfd(const pygo_iouring_ring_t *r)
{
    return r != NULL ? r->eventfd_fd : -1;
}

int pygo_iouring_ring_inflight(const pygo_iouring_ring_t *r)
{
    if (r == NULL) return 0;
    return __atomic_load_n(&r->inflight, __ATOMIC_ACQUIRE);
}

/* Submit one SQE to a hub ring.  No submission mutex: the caller is
 * the SINGLE issuer (this ring's owning hub thread).  Returns 0 on
 * success, -1 with errno on failure. */
static int pygo_iouring_ring_submit_sqe(pygo_iouring_ring_t *r,
                                        struct io_uring_sqe sqe_template,
                                        pygo_iouring_op_t *op)
{
    unsigned tail, head, idx;
    int n;
    while (1) {
        tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(r->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < r->sq_entries) break;
        /* SQ full.  Drain CQ to free SQEs (the SINGLE issuer assumption
         * also means we drain inline here without competing with anyone
         * else). */
        pygo_iouring_ring_drain(r);
    }
    idx = tail & r->sq_mask;
    r->sqes[idx] = sqe_template;
    r->sqes[idx].user_data = (uint64_t)(uintptr_t)op;
    r->sq_array[idx] = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
    n = sys_io_uring_submit_eintr(r->ring_fd, 1);   /* retry on EINTR */
    if (n < 0) return -1;
    __atomic_add_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
    /* Also bump the process-wide counter so hub_main's idle decision
     * can do a single atomic load instead of walking the registered-
     * rings list under a lock. */
    __atomic_add_fetch(&pygo_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    return 0;
}

void pygo_iouring_ring_drain(pygo_iouring_ring_t *r)
{
    if (r == NULL) return;
    if (r->eventfd_fd >= 0) {
        uint64_t scratch;
        while (read(r->eventfd_fd, &scratch, sizeof(scratch))
               == (ssize_t)sizeof(scratch))
            { /* drain */ }
    }
    /* CAS-claim per CQE: SINGLE_ISSUER only restricts SQE submission,
     * not CQ drain.  The netpoll pump (running on whichever hub goes
     * idle) can race with this ring's owning hub doing its own inline
     * drain.  Without the CAS both would process the same CQE and
     * pygo_mn_wake_g would push the goroutine onto the submission
     * list twice -> a second resume after the first one already
     * advanced the coro, corrupting the stack. */
    for (;;) {
        unsigned head = __atomic_load_n(r->cq_head, __ATOMIC_ACQUIRE);
        unsigned ct   = __atomic_load_n(r->cq_tail, __ATOMIC_ACQUIRE);
        struct io_uring_cqe *cqe;
        pygo_iouring_op_t *op;
        int32_t  res;
        unsigned expected;
        if (head == ct) return;
        cqe = &r->cqes[head & r->cq_mask];
        res = cqe->res;
        op  = (pygo_iouring_op_t *)(uintptr_t)cqe->user_data;
        expected = head;
        if (!__atomic_compare_exchange_n(r->cq_head, &expected, head + 1,
                                         0, __ATOMIC_ACQ_REL,
                                         __ATOMIC_ACQUIRE)) {
            continue;
        }
        if (op != NULL) {
            /* Hub rings only carry SINGLE ops -- multishot stays on the
             * global ring.  The op's hub field routes the wake; nominally
             * == this ring's owning hub. */
            op->result = res;
            if (op->hub != NULL) {
                pygo_mn_wake_g(op->hub, op->g);
            } else if (op->g != NULL) {
                pygo_sched_wake_safe(op->g);
            }
        }
        __atomic_sub_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
        __atomic_sub_fetch(&pygo_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    }
}

void pygo_iouring_ring_get_events(pygo_iouring_ring_t *r)
{
    if (r == NULL || !r->defer_taskrun) return;
    /* DEFER_TASKRUN: kernel only flushes task work + posts CQEs when
     * the user calls io_uring_enter(GETEVENTS).  We don't wait
     * (min_complete=0) -- just trigger the flush so the eventfd fires
     * if anything is pending.  Best-effort; ignore errors. */
    (void)sys_io_uring_enter(r->ring_fd, 0, 0,
                             IORING_ENTER_GETEVENTS, NULL, 0);
}

/* Common submit-and-park for hub-ring recv/send.  Caller must be a
 * goroutine running on the hub that owns r.  Returns bytes or -1
 * with errno. */
static pygo_iouring_ssize_t pygo_iouring_ring_do(pygo_iouring_ring_t *r,
                                                 struct io_uring_sqe sqe)
{
    pygo_iouring_op_t op;
    void *hub;

    if (r == NULL) { errno = EINVAL; return -1; }
    hub = pygo_mn_current_hub_opaque();
    if (hub == NULL) {
        /* Shouldn't happen if callers respect the contract, but fall
         * back gracefully: park via the global ring path. */
        errno = EINVAL;
        return -1;
    }
    op.type   = PYGO_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = pygo_mn_tls_current_g();
    op.result = INT32_MIN;

    if (pygo_iouring_ring_submit_sqe(r, sqe, &op) != 0) return -1;

    /* Inline drain: with FAST_POLL the CQE may already be in the ring.
     * Drain locally so the data-ready case avoids a park+wake round-
     * trip entirely.  pygo_mn_wake_g is race-safe -- if we're not yet
     * parked it just queues onto the hub's submission list, which the
     * hub will drain on the next iteration. */
    pygo_iouring_ring_drain(r);
    if (op.result != INT32_MIN) {
        if (op.result < 0) { errno = -op.result; return -1; }
        return op.result;
    }

    /* Park the hub g.  pygo_sched_park_current snaps the per-g tstate
     * slice AND marks self_queued so hub_main won't re-enqueue on
     * return from yield.  Without the snap, the g resumes with the
     * hub's tstate (not its own) and the pump's
     * PyEval_SaveThread/RestoreThread roundtrip leaves us with no
     * GIL on resume -> "PyEval_SaveThread: must be called with GIL".
     * Wake comes from drain via mn_wake_g pushing onto h->sub_head;
     * hub_main moves it onto its local FIFO on the next iteration;
     * then hub_main loads g->snap before pygo_coro_resume. */
    pygo_sched_park_current();
    pygo_coro_yield();

    if (op.result < 0) { errno = -op.result; return -1; }
    return op.result;
}

pygo_iouring_ssize_t pygo_iouring_ring_recv(pygo_iouring_ring_t *r,
                                            int fd, void *buf, size_t n,
                                            int flags)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return pygo_iouring_ring_do(r, sqe);
}

pygo_iouring_ssize_t pygo_iouring_ring_send(pygo_iouring_ring_t *r,
                                            int fd, const void *buf,
                                            size_t n, int flags)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_SEND;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return pygo_iouring_ring_do(r, sqe);
}

#else  /* !__linux__ */

#include <errno.h>
#include "io_uring.h"

int pygo_iouring_available(void) { return 0; }
int pygo_iouring_eventfd(void)   { return -1; }
void pygo_iouring_drain(void)    { /* no-op */ }
int pygo_iouring_inflight(void)  { return 0; }

int pygo_iouring_pbuf_available(void) { return 0; }
unsigned pygo_iouring_pbuf_size(void) { return 0; }
unsigned pygo_iouring_pbuf_count(void) { return 0; }
void *pygo_iouring_pbuf_addr(unsigned bid) { (void)bid; return NULL; }
void pygo_iouring_pbuf_return(unsigned bid) { (void)bid; }

pygo_iouring_ms_t *pygo_iouring_ms_open(int fd) { (void)fd; return NULL; }
pygo_iouring_ssize_t pygo_iouring_ms_recv(pygo_iouring_ms_t *h,
                                          void *buf, size_t n)
{
    (void)h; (void)buf; (void)n;
    errno = ENOSYS;
    return -1;
}
void pygo_iouring_ms_close(pygo_iouring_ms_t *h) { (void)h; }

pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n, pygo_iouring_off_t offset)
{
    (void)fd; (void)buf; (void)n; (void)offset;
    errno = ENOSYS;
    return -1;
}

pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n, pygo_iouring_off_t offset)
{
    (void)fd; (void)buf; (void)n; (void)offset;
    errno = ENOSYS;
    return -1;
}

pygo_iouring_ssize_t pygo_iouring_recv(int fd, void *buf, size_t n, int flags)
{
    (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

pygo_iouring_ssize_t pygo_iouring_send(int fd, const void *buf, size_t n, int flags)
{
    (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

/* Per-hub ring stubs (Linux-only feature; safe no-ops elsewhere). */
pygo_iouring_ring_t *pygo_iouring_ring_create(int defer_taskrun)
{
    (void)defer_taskrun;
    errno = ENOSYS;
    return NULL;
}
void pygo_iouring_ring_destroy(pygo_iouring_ring_t *r) { (void)r; }
int  pygo_iouring_ring_eventfd(const pygo_iouring_ring_t *r) { (void)r; return -1; }
int  pygo_iouring_ring_inflight(const pygo_iouring_ring_t *r) { (void)r; return 0; }
void pygo_iouring_ring_drain(pygo_iouring_ring_t *r) { (void)r; }
void pygo_iouring_ring_get_events(pygo_iouring_ring_t *r) { (void)r; }
pygo_iouring_ssize_t pygo_iouring_ring_recv(pygo_iouring_ring_t *r,
                                            int fd, void *buf, size_t n, int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}
pygo_iouring_ssize_t pygo_iouring_ring_send(pygo_iouring_ring_t *r,
                                            int fd, const void *buf, size_t n, int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

#endif
