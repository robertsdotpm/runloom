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

/* ============================================================
 * Provided buffer ring (Linux 5.19+) + multishot recv (Linux 6.0+).
 *
 * The buffer ring is a process-global pool of fixed-size buffers
 * pre-registered with the kernel.  Multishot recv ops submit once
 * and produce a CQE per chunk of incoming data, each CQE referring
 * to a buffer by bid (buffer ID).  Callers consume the data, then
 * return the buffer to the ring for reuse.
 * ============================================================ */

/* 1 if the provided-buffer ring is set up (and multishot is usable),
 * 0 otherwise.  Lazy-initialised alongside the main ring. */
int pygo_iouring_pbuf_available(void);

/* Per-buffer size and total count, for callers that want to know how
 * much data a multishot CQE can deliver per buffer. */
unsigned pygo_iouring_pbuf_size(void);
unsigned pygo_iouring_pbuf_count(void);

/* Map a CQE-delivered buffer id back to its kernel-shared memory
 * address.  Returns NULL on invalid bid or no ring. */
void *pygo_iouring_pbuf_addr(unsigned bid);

/* Return a buffer to the ring so the kernel can reuse it for another
 * multishot CQE.  Must be called after copying the data out.  Safe
 * to call concurrently. */
void pygo_iouring_pbuf_return(unsigned bid);

/* Opaque handle for a per-fd multishot recv stream. */
typedef struct pygo_iouring_ms pygo_iouring_ms_t;

/* Open a multishot recv handle on fd.  Submits one IORING_OP_RECV
 * with IORING_RECV_MULTISHOT immediately.  Returns NULL if the
 * kernel doesn't support multishot + provided buffer rings, or if
 * submission fails (errno set).  Caller owns the handle and must
 * call pygo_iouring_ms_close to release it. */
pygo_iouring_ms_t *pygo_iouring_ms_open(int fd);

/* Cooperatively read up to n bytes into buf.  Returns bytes read
 * (0 on orderly EOF, -1 with errno on error).  Parks the calling
 * goroutine until data arrives or EOF/error. */
pygo_iouring_ssize_t pygo_iouring_ms_recv(pygo_iouring_ms_t *h,
                                          void *buf, size_t n);

/* Close the multishot handle.  Submits an ASYNC_CANCEL for the in-
 * flight multishot SQE if it's still armed; the handle is freed
 * asynchronously by drain once the kernel's final CQE arrives.  If
 * the multishot already terminated the handle is freed inline.
 * Returns immediately; do not touch h afterwards. */
void pygo_iouring_ms_close(pygo_iouring_ms_t *h);

/* ============================================================
 * Per-hub rings (Linux 5.18+ for SINGLE_ISSUER, 6.1+ for DEFER_TASKRUN).
 *
 * The functions above operate on a single process-global ring shared
 * by every thread.  Under M:N (each hub == one OS thread that owns its
 * goroutines) each hub can additionally own a dedicated ring; that
 * ring is the SINGLE issuer of SQEs (no submission lock needed) and
 * eventually DEFER_TASKRUN can be turned on so completion task work
 * is batched until the hub thread next enters io_uring_enter.
 *
 * Hub rings carry plain recv/send/read/write SQEs.  The multishot +
 * provided-buffer-ring path stays on the global ring (one buffer pool
 * for the process); a hub-context multishot ms_open still submits its
 * SQE through the global ring, so per-hub rings do NOT regress
 * multishot.
 *
 * Lifecycle: a hub creates its ring at hub_main entry and destroys it
 * at hub exit.  The eventfd lives in the netpoll pump's shared epoll
 * (registered via pygo_netpoll_add_iouring_ring); the pump dispatches
 * CQE-pending events to the matching ring's drain function.
 * ============================================================ */

typedef struct pygo_iouring_ring pygo_iouring_ring_t;

/* Create a per-hub ring.  Sets IORING_SETUP_SINGLE_ISSUER on 5.18+
 * kernels (silently downgrades if unsupported).  If
 * defer_taskrun != 0 AND the kernel reports support (6.1+), also
 * sets IORING_SETUP_DEFER_TASKRUN -- in that case the OWNING thread
 * must call pygo_iouring_ring_get_events(r) periodically to flush
 * task work, since CQEs (and the eventfd) won't be posted until the
 * kernel sees an io_uring_enter(GETEVENTS) call.
 *
 * Returns NULL with errno set on failure (e.g. ENOSYS on <5.1, or any
 * mmap/eventfd failure). */
pygo_iouring_ring_t *pygo_iouring_ring_create(int defer_taskrun);

/* Tear down a ring.  Caller must ensure no in-flight ops remain
 * (inflight() == 0).  Closes ring fd + eventfd. */
void pygo_iouring_ring_destroy(pygo_iouring_ring_t *r);

/* Eventfd used by the kernel to signal CQE posts on this ring.
 * Caller registers it with the netpoll pump. */
int pygo_iouring_ring_eventfd(const pygo_iouring_ring_t *r);

/* In-flight SQE count on this ring.  Hub_main uses this in the idle
 * decision: if > 0, pump (drains CQEs) instead of sleep. */
int pygo_iouring_ring_inflight(const pygo_iouring_ring_t *r);

/* Drain CQEs.  Called by the netpoll pump when the ring's eventfd
 * fires.  Walks the CQ ring, writes results into op records, wakes
 * parked goroutines.  Idempotent. */
void pygo_iouring_ring_drain(pygo_iouring_ring_t *r);

/* DEFER_TASKRUN heartbeat: call from the owner thread to flush kernel
 * task work and post any pending CQEs to the eventfd.  No-op if the
 * ring wasn't created with defer_taskrun=1.  Idempotent. */
void pygo_iouring_ring_get_events(pygo_iouring_ring_t *r);

/* Cooperative recv/send through a hub ring.  Must be called from a
 * goroutine running on the hub that owns the ring (so SINGLE_ISSUER
 * holds and the park can be woken via pygo_mn_wake_g from drain).
 *
 * Same return convention as recv()/send(): non-negative bytes on
 * success, -1 with errno on failure. */
pygo_iouring_ssize_t pygo_iouring_ring_recv(pygo_iouring_ring_t *r,
                                            int fd, void *buf, size_t n,
                                            int flags);
pygo_iouring_ssize_t pygo_iouring_ring_send(pygo_iouring_ring_t *r,
                                            int fd, const void *buf,
                                            size_t n, int flags);

#endif
