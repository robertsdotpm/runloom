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


/* ---------------------------------------------------------------------------
 * io_uring.c is split across the io_uring_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only io_uring.c.
 * --------------------------------------------------------------------------- */
#include "io_uring_l_sys.c.inc"
#include "io_uring_l_buf.c.inc"
#include "io_uring_l_do.c.inc"
#include "io_uring_l_msclose.c.inc"
#include "io_uring_l_ring.c.inc"
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
