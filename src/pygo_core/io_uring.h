/* io_uring.h -- public interface to pygo's io_uring backend.
 *
 * On Linux 5.1+ these provide cooperative file I/O via the kernel's
 * io_uring interface.  On other OSes (or older kernels), pygo_iouring_
 * available() returns 0 and the read/write entry points return -1 with
 * errno=ENOSYS; callers should fall back to the thread-pool path.
 *
 * Cooperative model:
 *   - The caller submits one SQE referencing a per-op record on its
 *     own C stack.  user_data on the SQE is the op record pointer.
 *   - The caller parks via pygo_sched_park_safe (single-thread sched).
 *   - The kernel signals an eventfd registered with the ring on each
 *     CQE post.  The eventfd lives in pygo_iouring_eventfd(); the
 *     netpoll pump epoll-registers it and calls pygo_iouring_drain()
 *     when the eventfd fires.
 *   - Drain walks the CQ ring, writes result into each op record, and
 *     wakes the parked goroutine via pygo_sched_wake_safe.
 *
 *   Callers running inside an M:N hub take a synchronous spin-drain
 *   path instead -- the eventfd integration is wired into the global
 *   netpoll pump only, and the hub doesn't share that pump.
 */
#ifndef PYGO_IOURING_H
#define PYGO_IOURING_H

#include <stddef.h>
#include <stdint.h>

/* Portable signed-ssize_t / off_t equivalents.  Windows lacks ssize_t
 * in standard headers and we only need to compile the stubs there
 * (io_uring is Linux-only). */
typedef int64_t pygo_iouring_ssize_t;
typedef int64_t pygo_iouring_off_t;

/* 1 if io_uring is available on this system, 0 otherwise.  Lazy-
 * initialises the ring on the first call. */
int pygo_iouring_available(void);

/* Eventfd registered with the ring.  Returns -1 if io_uring is
 * unavailable.  Callers epoll-add this fd (EPOLLIN | EPOLLET) and
 * call pygo_iouring_drain() when it fires. */
int pygo_iouring_eventfd(void);

/* Walk the CQ ring, write results into per-op records, wake parked
 * goroutines.  Idempotent; safe to call when no completions are
 * pending. */
void pygo_iouring_drain(void);

/* Number of submitted ops that have not yet been drained.  Used by
 * the scheduler drain loop so it doesn't exit while a goroutine is
 * parked waiting for a CQE.  Includes ops in hub-spin-drain. */
int pygo_iouring_inflight(void);

/* Submit a pread, park the calling goroutine cooperatively, return
 * bytes read or -1 with errno set. */
pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n,
                                        pygo_iouring_off_t offset);

/* Submit a pwrite, park the calling goroutine cooperatively, return
 * bytes written or -1 with errno set. */
pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n,
                                         pygo_iouring_off_t offset);

/* Submit an IORING_OP_RECV / IORING_OP_SEND and park cooperatively
 * until completion.  Same return convention as recv()/send():
 * non-negative bytes on success, -1 with errno on failure.  flags is
 * the recv/send flags arg (MSG_*).  Used in place of the recv()/send()
 * + epoll-wait loop in the TCP hot path. */
pygo_iouring_ssize_t pygo_iouring_recv(int fd, void *buf, size_t n, int flags);
pygo_iouring_ssize_t pygo_iouring_send(int fd, const void *buf, size_t n, int flags);

#endif
