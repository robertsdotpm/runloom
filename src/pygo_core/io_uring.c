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

/* Per-op record.  Lives on the submitter's C stack across the park.
 * user_data on the SQE is the address of this struct so drain can
 * route the completion back to the right waiter. */
typedef struct pygo_iouring_op {
    pygo_g_t *g;             /* g to wake when this op completes */
    void     *hub;           /* opaque hub_t* or NULL for global sched */
    int32_t   result;        /* CQE res field; valid after wake */
} pygo_iouring_op_t;

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
    fd = sys_io_uring_setup(64, &p);
    if (fd < 0) {
        pygo_iouring_state.initialised = -1;
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

    pygo_iouring_state.initialised = 1;

    /* Hook the eventfd into the netpoll pump so CQEs cause the pump
     * to wake up + drain.  If this fails (epoll not on this OS, or
     * netpoll init failed), callers running on the global scheduler
     * will get no CQE delivery and park forever.  Treat as init
     * failure -- the hub path still works via spin-drain. */
    if (pygo_netpoll_add_iouring_eventfd(efd) != 0) {
        /* Don't tear down the ring; just abort init.  The lazy_init
         * sentinel is set to -1 above the fail label only on hard
         * failures (mmap, syscall) -- here we have a partially-
         * functional ring.  Mark as failed so callers fall back. */
        pygo_iouring_state.initialised = -1;
        munmap(sq_map,  sq_size);
        munmap(cq_map,  cq_size);
        munmap(sqe_map, sqe_size);
        close(efd);
        close(fd);
        pygo_iouring_state.eventfd_fd = -1;
        pygo_iouring_state.ring_fd    = -1;
        return -1;
    }
    return 0;

fail:
    if (sq_map  != MAP_FAILED) munmap(sq_map,  sq_size);
    if (cq_map  != MAP_FAILED) munmap(cq_map,  cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    pygo_iouring_state.initialised = -1;
    return -1;
}

int pygo_iouring_available(void)
{
    if (pygo_iouring_state.initialised == 0) {
        pygo_iouring_lock_ensure_inited();
        pygo_iouring_lazy_init();
    }
    return pygo_iouring_state.initialised == 1;
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
    n = sys_io_uring_enter(s->ring_fd, 1, 0, 0, NULL, 0);
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

    for (;;) {
        unsigned head = __atomic_load_n(s->cq_head, __ATOMIC_RELAXED);
        unsigned ct   = __atomic_load_n(s->cq_tail, __ATOMIC_ACQUIRE);
        struct io_uring_cqe *cqe;
        pygo_iouring_op_t *op;
        if (head == ct) return;
        cqe = &s->cqes[head & s->cq_mask];
        op  = (pygo_iouring_op_t *)(uintptr_t)cqe->user_data;
        if (op != NULL) {
            op->result = cqe->res;
            if (op->hub != NULL) {
                pygo_mn_wake_g(op->hub, op->g);
            } else if (op->g != NULL) {
                /* Global sched g.  wake_safe is race-safe: if the
                 * submitter hasn't parked yet, it bumps wake_pending
                 * and the next park_safe consumes the count without
                 * yielding.  Single-thread sched can't actually race
                 * here (drain runs from the pump which only runs
                 * between gs), but the race-safe primitive is
                 * cheap. */
                pygo_sched_wake_safe(op->g);
            }
        }
        __atomic_store_n(s->cq_head, head + 1, __ATOMIC_RELEASE);
        __atomic_sub_fetch(&pygo_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
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
    op.hub = hub;
    op.g   = (hub != NULL) ? pygo_mn_tls_current_g()
                           : pygo_sched_get()->current;
    op.result = INT32_MIN;        /* sentinel: not yet completed */

    if (pygo_iouring_submit_sqe(sqe, &op) != 0) return -1;

    if (hub != NULL) {
        /* Hub path: spin-drain inline.  The hub's local pump doesn't
         * know about the io_uring eventfd, so we can't park
         * cooperatively here without more plumbing.  Block in
         * io_uring_enter with min_complete=1 and drain after each
         * wakeup until our op is done. */
        while (op.result == INT32_MIN) {
            int n = sys_io_uring_enter(pygo_iouring_state.ring_fd, 0, 1,
                                       IORING_ENTER_GETEVENTS, NULL, 0);
            if (n < 0 && errno != EINTR) return -1;
            pygo_iouring_drain();
        }
    } else if (op.g != NULL) {
        /* Single-thread sched path: park cooperatively.  When the
         * eventfd fires the netpoll pump calls pygo_iouring_drain
         * which calls pygo_sched_wake_safe(op.g).  park_safe yields
         * via pygo_coro_yield; on resume we eat the pending wake. */
        pygo_sched_park_safe();
    } else {
        /* Not inside a goroutine.  Block like the hub path. */
        while (op.result == INT32_MIN) {
            int n = sys_io_uring_enter(pygo_iouring_state.ring_fd, 0, 1,
                                       IORING_ENTER_GETEVENTS, NULL, 0);
            if (n < 0 && errno != EINTR) return -1;
            pygo_iouring_drain();
        }
    }

    if (op.result < 0) {
        errno = -op.result;
        return -1;
    }
    return op.result;
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

#else  /* !__linux__ */

#include <errno.h>
#include "io_uring.h"

int pygo_iouring_available(void) { return 0; }
int pygo_iouring_eventfd(void)   { return -1; }
void pygo_iouring_drain(void)    { /* no-op */ }

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

#endif
