/* io_uring.h -- public interface to pygo's io_uring backend.
 *
 * On Linux 5.1+ these provide cooperative-ish file I/O via the kernel's
 * io_uring interface.  On other OSes (or older kernels), pygo_iouring_
 * available() returns 0 and pread/pwrite return -1 with errno=ENOSYS;
 * callers should fall back to the thread-pool path.
 *
 * "Cooperative-ish": the MVP version blocks the OS thread inside the
 * io_uring_enter syscall while waiting for the kernel.  A future
 * version will park the goroutine and let other gs run during the
 * wait -- the syscall returns immediately once the operation
 * completes.  Even the MVP version is a clear win over the thread-
 * pool path for file I/O because it avoids the thread-handoff cost
 * (one io_uring_enter syscall vs. thread schedule + GIL release + work
 * + GIL reacquire + thread wakeup).
 */
#ifndef PYGO_IOURING_H
#define PYGO_IOURING_H

#include <stddef.h>
#include <stdint.h>

/* Portable signed-ssize_t equivalent.  Windows lacks ssize_t in
 * standard headers and we only need to compile the stubs there
 * (io_uring is Linux-only). */
typedef int64_t pygo_iouring_ssize_t;
typedef int64_t pygo_iouring_off_t;

/* 1 if io_uring is available on this system, 0 otherwise.  Lazy-
 * initialises the ring on the first call. */
int pygo_iouring_available(void);

/* Submit a pread, block until completion, return bytes read or -1 with
 * errno set. */
pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n,
                                        pygo_iouring_off_t offset);

/* Submit a pwrite, block until completion, return bytes written or -1
 * with errno set. */
pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n,
                                         pygo_iouring_off_t offset);

#endif
