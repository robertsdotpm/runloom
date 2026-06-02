/* netpoll.h -- portable I/O multiplexing for the scheduler.
 *
 *   pygo_netpoll_wait_fd(fd, READ | WRITE, timeout_ns)
 *     park the current goroutine until the fd is ready or timeout expires.
 *     Returns READY mask, 0 on timeout, -1 on error (errno set).
 *
 * Backend per OS:
 *   Linux           -> epoll_create1 + epoll_wait
 *   BSD/macOS       -> kqueue + kevent
 *   Solaris         -> event ports (fallback to select for v0)
 *   Windows         -> select for v0 (IOCP is async-completion, harder
 *                      to slot under a "wait until ready" API)
 *   everything else -> select
 *
 * v0 caveat: this is single-threaded.  Phase C M:N scheduler will need
 * to extend with thread-safe registration and per-hub poll sets.
 */
#ifndef PYGO_NETPOLL_H
#define PYGO_NETPOLL_H

#include "compat.h"
#include "plat.h"

#define PYGO_NETPOLL_READ  0x1
#define PYGO_NETPOLL_WRITE 0x2

/* Sentinel returned by pygo_netpoll_wait_fd when the parked goroutine was
 * cancelled out-of-band via pygo_netpoll_cancel_g (a task.cancel() targeting a
 * g parked in a C wait_fd, where there is no coro await-point to throw into).
 * A high positive bit that can never be a real event mask (0x1/0x2) nor the
 * 0/-1 timeout/error returns, so callers distinguish it without an errno or a
 * sign check.  pygo.aio's wait_fd wrapper turns this into CancelledError. */
#define PYGO_NETPOLL_CANCELLED 0x40000000

/* Park the current goroutine until fd is ready for any of `events`,
 * or timeout_ns nanoseconds have passed (-1 = wait forever).
 * Returns the ready events mask (subset of `events`), 0 on timeout,
 * -1 on error.  Must be called from inside a goroutine. */
int pygo_netpoll_wait_fd(int fd, int events, long long timeout_ns);

/* Drive netpoll once.  Returns the number of goroutines woken.
 * Called by the scheduler when its ready queue is empty but at
 * least one goroutine is parked. */
int pygo_netpoll_pump(long long timeout_ns);

/* How many goroutines are currently parked.  Scheduler uses this
 * to decide whether to call pump or exit. */
int pygo_netpoll_parked_count(void);

/* DIAG: dump every parked parker (fd/g/hub/commit) to stderr. */
void pygo_netpoll_dump_parkers(void);

/* Hub-idle dwell-based stack reclaim (PYGO_STACK_PARK_SWEEP).  The
 * calling hub madvises the idle stack pages of its OWN parkers whose
 * park has exceeded threshold_ns.  Safe only when called by the owning
 * hub while idle (see the netpoll.c definition).  Returns # reclaimed. */
int pygo_netpoll_sweep_idle(void *hub_opaque, long long threshold_ns);

/* Forcibly wake every parked goroutine with ready_mask=-1.  Used by
 * sched_reset() on paio.run cleanup so leftover accept loops /
 * tickers don't block the next pygo_core.run(). */
int pygo_netpoll_drain_parked(void);

/* Force-unlink a g's pending parker, if any.  Called by the hub
 * completion path before pygo_g_decref so a leaked parker (M:N race
 * where some wake path bypassed pygo_parker_unlink) cannot survive
 * into stack-pool reuse and resurrect the freed g via pump dispatch. */
struct pygo_g;
void pygo_netpoll_force_unlink_g_parker(struct pygo_g *g);

/* Cancel a goroutine parked in pygo_netpoll_wait_fd: claim its parker (the
 * same commit-CAS the pump uses, so exactly one of {pump, timeout, cancel}
 * wins), make its wait_fd return PYGO_NETPOLL_CANCELLED, and re-queue it to its
 * owner scheduler.  Returns 1 if a parked g was woken, 0 if g had no live
 * parker (not parked in wait_fd, or already woken by the pump/timeout).  This
 * is the per-g cancel primitive that lets task.cancel() interrupt a g blocked
 * in a socket recv/accept/connect with no coro await-point. */
int pygo_netpoll_cancel_g(struct pygo_g *g);

/* Clear the "fd is registered in netpoll" cache bit.  Call from the
 * socket-close hook so a future fd reuse re-registers cleanly.  No
 * syscall; the kernel auto-clears its epoll/kqueue entry when the
 * last fd reference closes, so this just keeps our bitmap honest.
 * Safe to call on unknown fds (no-op). */
void pygo_netpoll_unregister(int fd);

/* One-time init / cleanup. */
int pygo_netpoll_init(void);
void pygo_netpoll_fini(void);

/* Backend name for diagnostics: "epoll" / "kqueue" / "select". */
const char *pygo_netpoll_backend(void);

/* Test-only Windows netpoll fault-injection introspection (see netpoll.c).
 * pygo_fault_count returns how many times the named site ("WSAPOLL"/"SELECT"/
 * "IOCP_WAIT"/"IOCP_SUBMIT") injected an error (-1 for an unknown name / a
 * non-Windows build); pygo_fault_reset clears all counters + once-flags.
 * No-ops on non-Windows. */
long pygo_fault_count(const char *name);
void pygo_fault_reset(void);

/* Register an external eventfd so the pump treats EPOLLIN on that fd
 * as a "drain io_uring CQEs" signal.  Only meaningful on the epoll
 * backend (Linux); a no-op elsewhere.  Used by io_uring.c to hook
 * its completion eventfd into the global pump.  Caller retains
 * ownership of the fd. */
int pygo_netpoll_add_iouring_eventfd(int fd);

/* Like above but registers a per-hub ring instead of the global ring.
 * The pump dispatches the eventfd hit to pygo_iouring_ring_drain(ring).
 * Up to PYGO_NETPOLL_MAX_IOURING_RINGS hub rings may be registered at
 * once (sized for typical CPU counts).  Returns 0 on success, -1 on
 * "too many registered" or non-epoll backend. */
struct pygo_iouring_ring;
int pygo_netpoll_add_iouring_ring(int eventfd_fd,
                                  struct pygo_iouring_ring *ring);
void pygo_netpoll_remove_iouring_ring(int eventfd_fd);

/* Generic cross-thread pump interrupt.  Arm once (idempotent); returns 0
 * if the backend supports it (epoll today), -1 otherwise.  Any thread may
 * then call pygo_netpoll_wake_pump() to break an idle epoll_wait so a
 * scheduler blocked in the pump re-checks its ready/wake lists.  Used by
 * the blocking-offload pool to wake the single-thread scheduler. */
int  pygo_netpoll_wake_pump_arm(void);
void pygo_netpoll_wake_pump(void);

/* Does any registered iouring source (global or per-hub) have an
 * in-flight SQE?  Hub_main uses this to decide pump vs sleep when no
 * fd-parks are active. */
int pygo_netpoll_any_iouring_inflight(void);

#endif /* PYGO_NETPOLL_H */
