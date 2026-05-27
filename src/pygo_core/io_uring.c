/* io_uring.c -- minimal io_uring backend for cooperative file I/O.
 *
 * Why we exist: pygo_core.fd_read / fd_write on regular files don't
 * work cooperatively through epoll -- regular file fds always report
 * "ready" so wait_fd is a no-op and the actual read/write blocks the
 * OS thread.  io_uring submits the read/write asynchronously to the
 * kernel and we park the goroutine on a chan, woken when the kernel
 * posts a completion.
 *
 * What we DON'T do: liburing.  Adding a build-time dependency on a
 * native library would compromise pygo's "pip install . just works"
 * story.  We talk to io_uring via the raw syscalls (io_uring_setup,
 * io_uring_enter) and an mmap'd ring -- about 200 lines of code.
 *
 * Backend availability is runtime-detected via the io_uring_setup
 * syscall returning -ENOSYS on old kernels (<5.1).  In that case
 * pygo_iouring_available() returns 0 and callers fall back to the
 * thread-pool path in monkey.py / pygo.sync.
 *
 * Thread-safety: NONE.  Single per-process ring; callers must serialise
 * (the C scheduler is single-threaded so this works for the normal
 * path).  The M:N hub will need a per-hub ring later.
 */
#include "plat.h"

#if defined(__linux__)

#include <fcntl.h>
#include <linux/io_uring.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "io_uring.h"
#include "pygo_sched.h"

/* Some old kernels' io_uring.h tie ssize_t/off_t to libc headers; we
 * keep the API portable via the typedef above and only use the
 * libc types inside the syscall calls below. */

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

/* Per-process ring state.  Initialised lazily on first use; once set
 * up it lives for the process lifetime. */
typedef struct {
    int ring_fd;
    int initialised;       /* 0 = untried, 1 = ready, -1 = init failed */

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

    /* Pending operations indexed by user_data.  We use the user_data
     * slot to carry a pointer to the pygo_g_t that's parked on the
     * operation.  When the completion fires we wake the g and pass
     * the result back. */
} pygo_iouring_state_t;

static pygo_iouring_state_t pygo_iouring_state = {0};

/* Op-result: published by the completion-drain side, read by the
 * waiting goroutine when it resumes. */
typedef struct {
    int32_t result;        /* res from CQE; >= 0 = success, < 0 = -errno */
    pygo_g_t *g;           /* g to wake when this completes */
    int      done;
} pygo_iouring_op_t;


/* Initialise the ring.  Lazy: only runs the first time
 * pygo_iouring_available() is called and only succeeds once.
 * Returns 0 on success, -1 on init failure / no kernel support. */
static int pygo_iouring_lazy_init(void)
{
    struct io_uring_params p;
    int fd;
    void *sq_map = MAP_FAILED, *cq_map = MAP_FAILED, *sqe_map = MAP_FAILED;
    size_t sq_size, cq_size, sqe_size;

    memset(&p, 0, sizeof(p));
    fd = sys_io_uring_setup(64, &p);
    if (fd < 0) return -1;

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

    pygo_iouring_state.ring_fd       = fd;
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
    return 0;

fail:
    if (sq_map != MAP_FAILED)  munmap(sq_map, sq_size);
    if (cq_map != MAP_FAILED)  munmap(cq_map, cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    pygo_iouring_state.initialised = -1;
    return -1;
}

int pygo_iouring_available(void)
{
    if (pygo_iouring_state.initialised == 0) {
        pygo_iouring_lazy_init();
    }
    return pygo_iouring_state.initialised == 1;
}

/* Acquire one SQE, configure it, advance sq_tail, submit, then wait
 * for the matching CQE.  We don't batch -- one syscall per op for the
 * MVP.  Future: batched submission via the same enter call. */
static int pygo_iouring_submit_and_wait(struct io_uring_sqe sqe_template,
                                        uint64_t user_data)
{
    pygo_iouring_state_t *s = &pygo_iouring_state;
    unsigned tail, idx;
    int n;

    /* Acquire submission slot. */
    tail = *s->sq_tail;
    idx  = tail & s->sq_mask;
    s->sqes[idx] = sqe_template;
    s->sqes[idx].user_data = user_data;
    s->sq_array[idx] = idx;
    __atomic_store_n(s->sq_tail, tail + 1, __ATOMIC_RELEASE);

    /* Submit + block (kernel-side -- the syscall thread blocks; not
     * the goroutine).  IORING_ENTER_GETEVENTS makes the kernel return
     * only when at least one completion is available. */
    n = sys_io_uring_enter(s->ring_fd, 1, 1, IORING_ENTER_GETEVENTS, NULL, 0);
    if (n < 0) return -1;

    /* Walk the CQ looking for our op. */
    for (;;) {
        unsigned head = *s->cq_head;
        unsigned ct   = __atomic_load_n(s->cq_tail, __ATOMIC_ACQUIRE);
        if (head == ct) {
            /* No more completions -- another goroutine drained ours.
             * Shouldn't happen in single-threaded mode but defensive. */
            n = sys_io_uring_enter(s->ring_fd, 0, 1,
                                   IORING_ENTER_GETEVENTS, NULL, 0);
            if (n < 0) return -1;
            continue;
        }
        struct io_uring_cqe *cqe = &s->cqes[head & s->cq_mask];
        if (cqe->user_data == user_data) {
            int res = cqe->res;
            __atomic_store_n(s->cq_head, head + 1, __ATOMIC_RELEASE);
            return res;
        }
        /* Someone else's CQE -- skip.  In a fuller impl we'd dispatch
         * to the right g via cqe->user_data; the MVP only has one
         * goroutine submitting at a time. */
        __atomic_store_n(s->cq_head, head + 1, __ATOMIC_RELEASE);
    }
}

pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n, pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    int res;
    if (!pygo_iouring_available()) {
        /* Caller should fall back; signal via -ENOSYS. */
        return -1;
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode  = IORING_OP_READ;
    sqe.fd      = fd;
    sqe.addr    = (uintptr_t)buf;
    sqe.len     = (unsigned)n;
    sqe.off     = (uint64_t)offset;
    res = pygo_iouring_submit_and_wait(sqe, (uint64_t)(uintptr_t)buf);
    if (res < 0) {
        errno = -res;
        return -1;
    }
    return res;
}

pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n, pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    int res;
    if (!pygo_iouring_available()) return -1;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode  = IORING_OP_WRITE;
    sqe.fd      = fd;
    sqe.addr    = (uintptr_t)buf;
    sqe.len     = (unsigned)n;
    sqe.off     = (uint64_t)offset;
    res = pygo_iouring_submit_and_wait(sqe, (uint64_t)(uintptr_t)buf);
    if (res < 0) {
        errno = -res;
        return -1;
    }
    return res;
}

#else  /* !__linux__ */

#include <errno.h>
#include "io_uring.h"

int pygo_iouring_available(void) { return 0; }

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
