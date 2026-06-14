/* io_uring.h -- public interface to runloom's io_uring backend.
 *
 * On Linux 5.1+ these provide cooperative file I/O via the kernel's
 * io_uring interface.  On other OSes (or older kernels), runloom_iouring_
 * available() returns 0 and the read/write entry points return -1 with
 * errno=ENOSYS; callers should fall back to the thread-pool path.
 *
 * Cooperative model:
 *   - The caller submits one SQE referencing a per-op record on its
 *     own C stack.  user_data on the SQE is the op record pointer.
 *   - The caller parks via runloom_sched_park_safe (single-thread sched).
 *   - The kernel signals an eventfd registered with the ring on each
 *     CQE post.  The eventfd lives in runloom_iouring_eventfd(); the
 *     netpoll pump epoll-registers it and calls runloom_iouring_drain()
 *     when the eventfd fires.
 *   - Drain walks the CQ ring, writes result into each op record, and
 *     wakes the parked fiber via runloom_sched_wake_safe.
 *
 *   Callers running inside an M:N hub take a synchronous spin-drain
 *   path instead -- the eventfd integration is wired into the global
 *   netpoll pump only, and the hub doesn't share that pump.
 */
#ifndef RUNLOOM_IOURING_H
#define RUNLOOM_IOURING_H

#include <stddef.h>
#include <stdint.h>

/* Portable signed-ssize_t / off_t equivalents.  Windows lacks ssize_t
 * in standard headers and we only need to compile the stubs there
 * (io_uring is Linux-only). */
typedef int64_t runloom_iouring_ssize_t;
typedef int64_t runloom_iouring_off_t;

/* 1 if io_uring is available on this system, 0 otherwise.  Lazy-
 * initialises the ring on the first call. */
int runloom_iouring_available(void);

/* Eventfd registered with the ring.  Returns -1 if io_uring is
 * unavailable.  Callers epoll-add this fd (EPOLLIN | EPOLLET) and
 * call runloom_iouring_drain() when it fires. */
int runloom_iouring_eventfd(void);

/* Walk the CQ ring, write results into per-op records, wake parked
 * fibers.  Idempotent; safe to call when no completions are
 * pending. */
void runloom_iouring_drain(void);

/* Number of submitted ops that have not yet been drained.  Used by
 * the scheduler drain loop so it doesn't exit while a fiber is
 * parked waiting for a CQE.  Includes ops in hub-spin-drain. */
int runloom_iouring_inflight(void);

/* Cancel a fiber parked on a single (global-ring) io_uring op: submit an
 * ASYNC_CANCEL so the kernel completes it -ECANCELED and the drain wakes the
 * fiber.  Returns 1 if a cancel was submitted, 0 otherwise (not parked on a
 * cancellable op).  Forward-declared g to avoid a runloom_sched.h include cycle. */
struct runloom_g;
int runloom_iouring_cancel_g(struct runloom_g *g);

/* Submit an ASYNC_CANCEL for a hub-ring op (a runloom_iouring_op_t*, void* here)
 * on ITS ring.  Called by the op's OWNING hub -- the ring's single issuer --
 * after it drains its cancel mailbox (runloom_mn_hub_request_iouring_cancel). */
void runloom_iouring_submit_cancel_for_op(void *op);

/* Submit a pread, park the calling fiber cooperatively, return
 * bytes read or -1 with errno set. */
runloom_iouring_ssize_t runloom_iouring_pread(int fd, void *buf, size_t n,
                                        runloom_iouring_off_t offset);

/* Submit a pwrite, park the calling fiber cooperatively, return
 * bytes written or -1 with errno set. */
runloom_iouring_ssize_t runloom_iouring_pwrite(int fd, const void *buf, size_t n,
                                         runloom_iouring_off_t offset);

/* Submit an IORING_OP_RECV / IORING_OP_SEND and park cooperatively
 * until completion.  Same return convention as recv()/send():
 * non-negative bytes on success, -1 with errno on failure.  flags is
 * the recv/send flags arg (MSG_*).  Used in place of the recv()/send()
 * + epoll-wait loop in the TCP hot path. */
runloom_iouring_ssize_t runloom_iouring_recv(int fd, void *buf, size_t n, int flags);
runloom_iouring_ssize_t runloom_iouring_send(int fd, const void *buf, size_t n, int flags);

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
int runloom_iouring_pbuf_available(void);

/* Per-buffer size and total count, for callers that want to know how
 * much data a multishot CQE can deliver per buffer. */
unsigned runloom_iouring_pbuf_size(void);
unsigned runloom_iouring_pbuf_count(void);

/* Map a CQE-delivered buffer id back to its kernel-shared memory
 * address.  Returns NULL on invalid bid or no ring. */
void *runloom_iouring_pbuf_addr(unsigned bid);

/* Return a buffer to the ring so the kernel can reuse it for another
 * multishot CQE.  Must be called after copying the data out.  Safe
 * to call concurrently. */
void runloom_iouring_pbuf_return(unsigned bid);

/* Opaque handle for a per-fd multishot recv stream. */
typedef struct runloom_iouring_ms runloom_iouring_ms_t;

/* Open a multishot recv handle on fd.  Submits one IORING_OP_RECV
 * with IORING_RECV_MULTISHOT immediately.  Returns NULL if the
 * kernel doesn't support multishot + provided buffer rings, or if
 * submission fails (errno set).  Caller owns the handle and must
 * call runloom_iouring_ms_close to release it. */
runloom_iouring_ms_t *runloom_iouring_ms_open(int fd);

/* Cooperatively read up to n bytes into buf.  Returns bytes read
 * (0 on orderly EOF, -1 with errno on error).  Parks the calling
 * fiber until data arrives or EOF/error. */
runloom_iouring_ssize_t runloom_iouring_ms_recv(runloom_iouring_ms_t *h,
                                          void *buf, size_t n);

/* Close the multishot handle.  Submits an ASYNC_CANCEL for the in-
 * flight multishot SQE if it's still armed; the handle is freed
 * asynchronously by drain once the kernel's final CQE arrives.  If
 * the multishot already terminated the handle is freed inline.
 * Returns immediately; do not touch h afterwards. */
void runloom_iouring_ms_close(runloom_iouring_ms_t *h);

/* ============================================================
 * Per-hub rings (Linux 5.18+ for SINGLE_ISSUER, 6.1+ for DEFER_TASKRUN).
 *
 * The functions above operate on a single process-global ring shared
 * by every thread.  Under M:N (each hub == one OS thread that owns its
 * fibers) each hub can additionally own a dedicated ring; that
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
 * (registered via runloom_netpoll_add_iouring_ring); the pump dispatches
 * CQE-pending events to the matching ring's drain function.
 * ============================================================ */

typedef struct runloom_iouring_ring runloom_iouring_ring_t;

/* Create a per-hub ring.  Sets IORING_SETUP_SINGLE_ISSUER on 5.18+
 * kernels (silently downgrades if unsupported).  If
 * defer_taskrun != 0 AND the kernel reports support (6.1+), also
 * sets IORING_SETUP_DEFER_TASKRUN -- in that case the OWNING thread
 * must call runloom_iouring_ring_get_events(r) periodically to flush
 * task work, since CQEs (and the eventfd) won't be posted until the
 * kernel sees an io_uring_enter(GETEVENTS) call.
 *
 * Returns NULL with errno set on failure (e.g. ENOSYS on <5.1, or any
 * mmap/eventfd failure). */
runloom_iouring_ring_t *runloom_iouring_ring_create(int defer_taskrun);

/* Tear down a ring.  Caller must ensure no in-flight ops remain
 * (inflight() == 0).  Closes ring fd + eventfd. */
void runloom_iouring_ring_destroy(runloom_iouring_ring_t *r);

/* Eventfd used by the kernel to signal CQE posts on this ring.
 * Caller registers it with the netpoll pump. */
int runloom_iouring_ring_eventfd(const runloom_iouring_ring_t *r);

/* In-flight SQE count on this ring.  Hub_main uses this in the idle
 * decision: if > 0, pump (drains CQEs) instead of sleep. */
int runloom_iouring_ring_inflight(const runloom_iouring_ring_t *r);

/* Drain CQEs.  Called by the netpoll pump when the ring's eventfd
 * fires.  Walks the CQ ring, writes results into op records, wakes
 * parked fibers.  Idempotent. */
void runloom_iouring_ring_drain(runloom_iouring_ring_t *r);

/* DEFER_TASKRUN heartbeat: call from the owner thread to flush kernel
 * task work and post any pending CQEs to the eventfd.  No-op if the
 * ring wasn't created with defer_taskrun=1.  Idempotent. */
void runloom_iouring_ring_get_events(runloom_iouring_ring_t *r);

/* Cooperative recv/send through a hub ring.  Must be called from a
 * fiber running on the hub that owns the ring (so SINGLE_ISSUER
 * holds and the park can be woken via runloom_mn_wake_g from drain).
 *
 * Same return convention as recv()/send(): non-negative bytes on
 * success, -1 with errno on failure. */
runloom_iouring_ssize_t runloom_iouring_ring_recv(runloom_iouring_ring_t *r,
                                            int fd, void *buf, size_t n,
                                            int flags);
runloom_iouring_ssize_t runloom_iouring_ring_send(runloom_iouring_ring_t *r,
                                            int fd, const void *buf,
                                            size_t n, int flags);

/* ============================================================
 * io_uring-as-loop backend (RUNLOOM_IOURING_LOOP=1, default off).
 *
 * The hub blocks DIRECTLY in its per-hub ring (io_uring_submit_and_wait_timeout)
 * instead of epoll_wait, eliminating the eventfd->epoll->pump bridge.  Two
 * non-io_uring wake sources are bridged in as persistent multishot POLL_ADDs:
 * a per-hub wake eventfd (cross-hub submit interrupt) and the shared netpoll
 * epoll fd (so existing wait_fd readiness still works, drained via a
 * non-blocking pump(0) when the loop reports it ready).
 * ============================================================ */

/* 1 if the loop backend is enabled (RUNLOOM_IOURING_LOOP set, read once). */
int runloom_iouring_loop_enabled(void);

/* Bits set by loop_wait in *flags_out. */
#define RUNLOOM_LOOP_F_EPOLL   0x1   /* epoll fd readable: caller should pump(0) */
#define RUNLOOM_LOOP_F_WAKE    0x2   /* wake eventfd fired (sub list re-drained) */

/* Arm the loop on a hub's ring: create the wake eventfd, poll-add it + the
 * shared epoll fd (may be -1) as persistent multishot polls.  Returns the wake
 * fd (>=0) on success, -1 on failure (caller discards the ring). */
int runloom_iouring_loop_hub_arm(runloom_iouring_ring_t *r, int epoll_fd);

/* Block the owning hub in its ring until >=1 completion or timeout_ns (<0 =
 * forever), draining the whole CQE batch.  *flags_out accumulates the F_* bits. */
void runloom_iouring_loop_wait(runloom_iouring_ring_t *r, long long timeout_ns,
                               int *flags_out);

/* Stage-2 proactor I/O: submit an IORING_OP_RECV/SEND on the current hub's
 * ring (deferred -- batched into the next loop_wait), park the fiber, and
 * return bytes (0 = EOF, -1 + errno on error).  Must run on a fiber whose
 * hub owns r.  Used by the all-C serve path under the loop backend. */
runloom_iouring_ssize_t runloom_iouring_loop_recv(runloom_iouring_ring_t *r,
                                                  int fd, void *buf, size_t n,
                                                  int flags);
runloom_iouring_ssize_t runloom_iouring_loop_send(runloom_iouring_ring_t *r,
                                                  int fd, const void *buf,
                                                  size_t n, int flags);

/* Write a hub's wake eventfd to interrupt its ring wait (cross-hub submit). */
void runloom_iouring_loop_wake(int wake_fd);

/* Close the wake eventfd (the polls die with the ring at destroy). */
void runloom_iouring_loop_hub_disarm(runloom_iouring_ring_t *r);

/* ---- Stage-3 multishot recv (RUNLOOM_IOURING_MS=1, requires the loop) ----
 *
 * ONE persistent IORING_OP_RECV | MULTISHOT | BUFFER_SELECT SQE per connection
 * delivers a CQE per chunk into the owning hub's provided buffer ring, with no
 * re-submit and no submit/park per recv.  Used by the all-C serve path.  Handle
 * access is single-hub-thread (the echo fiber is hub-pinned), so it is lock-free.
 */

/* 1 if the multishot recv path is enabled (RUNLOOM_IOURING_MS set; read once). */
int runloom_iouring_loop_ms_enabled(void);

/* Open a multishot recv stream on fd on the current hub's ring r.  Returns an
 * opaque handle, or NULL if multishot isn't available (no per-hub buffer pool /
 * alloc failure) -- the caller then falls back to single-shot loop_recv.  Must
 * run on a fiber whose hub owns r; the fiber must stay on that hub for the
 * stream's lifetime (guaranteed: a woken fiber is hub-pinned). */
void *runloom_iouring_loop_ms_open(runloom_iouring_ring_t *r, int fd);

/* Cooperatively read up to n bytes from the stream into buf.  Returns bytes
 * (>0), 0 on EOF, or -1 with errno on error.  Parks the fiber until data
 * arrives. */
runloom_iouring_ssize_t runloom_iouring_loop_ms_recv(void *handle,
                                                     void *buf, size_t n);

/* Close the stream: cancel an armed SQE, wait for its terminal CQE, reclaim
 * held buffers, free the handle.  Do not touch the handle afterwards. */
void runloom_iouring_loop_ms_close(void *handle);

#endif
