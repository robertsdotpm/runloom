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

/* Forcibly wake every parked goroutine with ready_mask=-1.  Used by
 * sched_reset() on paio.run cleanup so leftover accept loops /
 * tickers don't block the next pygo_core.run(). */
int pygo_netpoll_drain_parked(void);

/* One-time init / cleanup. */
int pygo_netpoll_init(void);
void pygo_netpoll_fini(void);

/* Backend name for diagnostics: "epoll" / "kqueue" / "select". */
const char *pygo_netpoll_backend(void);

#endif /* PYGO_NETPOLL_H */
