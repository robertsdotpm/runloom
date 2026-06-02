/* netpoll.c -- portable I/O multiplexing.
 *
 * Backends, picked by plat.h:
 *   Linux           -> epoll
 *   macOS / *BSD    -> kqueue
 *   Windows         -> WSAPoll (sockets) -- POSIX poll()-shaped Winsock API,
 *                      Vista+, no FD_SETSIZE cap.  IOCP would be more
 *                      efficient for the file-handle case but only sockets
 *                      flow through wait_fd today (regular files go to the
 *                      thread-pool backend in monkey.py).
 *   else            -> select() POSIX fallback
 *
 * Park mechanics:
 *   - the current goroutine snapshot tstate, gets pushed onto an internal
 *     "parked" list with (fd, events, deadline) metadata.  yields via
 *     pygo_coro_yield.
 *   - the scheduler's drain loop, when ready queue is empty, calls
 *     pygo_netpoll_pump(timeout) instead of sleeping the OS thread.
 *   - pump waits for I/O / timeout, wakes parked goroutines, returns.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "netpoll.h"
#include "coro.h"
#include "pygo_sched.h"
#include "mn_sched.h"
#include "io_uring.h"
#include "pygo_diag.h"
#include "pygo_gstate.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#if !defined(PYGO_OS_WINDOWS)
#  include <sys/resource.h>   /* getrlimit(RLIMIT_NOFILE) for fd-array sizing */
#endif

#if defined(PYGO_HAVE_EPOLL)
#  include <sys/epoll.h>
#  include <sys/eventfd.h>
#  include <stdint.h>
#  include <unistd.h>
#elif defined(PYGO_HAVE_KQUEUE)
#  include <sys/event.h>
#  include <sys/time.h>
#  include <unistd.h>
#  include <fcntl.h>      /* self-pipe wake: O_NONBLOCK / FD_CLOEXEC */
#elif defined(PYGO_OS_WINDOWS)
   /* winsock2.h, ws2tcpip.h and windows.h are already pulled in via
    * plat_compat.h.  WSAPoll + WSAPOLLFD live in winsock2.h, FD_SET /
    * FD_ISSET likewise -- no extra header needed here. */
#else
#  include <sys/select.h>
#  include <unistd.h>
#  include <fcntl.h>      /* self-pipe wake: O_NONBLOCK / FD_CLOEXEC */
#endif

/* ---- internal park record ----
 * Allocated on the parked goroutine's C stack inside pygo_netpoll_wait_fd
 * (the stack stays alive across yield because the goroutine isn't
 * destroyed until it completes).
 *
 * Two intrusive links:
 *   * `next` / `slot` into the global parked list (used by drain_parked
 *     and the timeout sweep).  Uses the "slot pointer" trick for O(1)
 *     unlink; the head pointer lives in a static, so its address is
 *     stable.
 *   * `next_by_fd` / `prev_by_fd` into the per-fd bucket (the hot path:
 *     epoll-event → matching parker → wake).  Doubly-linked because
 *     the bucket-head pointer lives inside a realloc-able heap array,
 *     and the slot-pointer trick can't survive a realloc that moves
 *     the array. */
typedef struct pygo_parked {
    int fd;
    int events;
    long long deadline_ns;     /* -1 = forever */
    int *ready_out;            /* where to store the wakeup mask */
    pygo_g_t *g;
    void *hub;                  /* M:N hub opaque; NULL = global sched */
    struct pygo_parked  *next;
    struct pygo_parked **slot;
    struct pygo_parked  *next_by_fd;
    struct pygo_parked  *prev_by_fd;
    /* Monotonic acquire generation (bumped each pool acquire).  When
     * the parker comes from the pool's freelist, the new gen
     * disambiguates this lifetime from prior ones with the same
     * heap address.  Pure defence-in-depth: pool acquire never
     * returns a parker that is currently linked into any list, so
     * the global / bucket pointers cannot alias a live entry.  The
     * gen field is exposed via the diag ring so a missed-unlink can
     * be traced to the lifetime that left the dangling reference. */
    unsigned int gen;
    /* Index into the deadline min-heap; -1 if not present (deadline
     * < 0 or never linked).  Maintained by the heap ops so unlink
     * can find and remove in O(log N). */
    int heap_index;
    /* TLS pool freelist linkage.  When released, the parker is on
     * its owning thread's freelist; pool_next chains them.  Cleared
     * on acquire. */
    struct pygo_parked *pool_next;
    /* Atomic park/wake commit (Go netpollblockcommit, adapted to pygo's
     * re-queue model).  Closes the residual lost-wake: a pump can claim
     * a still-linked parker in the window between wait_fd's last
     * readiness re-check and its commit to parking.  Exactly one of
     * {pump, parking g} CASes this away from ARMED:
     *   ARMED  - linked, g has not yet committed to parking.
     *   PARKED - g committed (yielded / about to); a pump that claims a
     *            PARKED parker re-queues the g via pygo_mn_wake_g.
     *   WOKEN  - claimed (by a pump or cancel).  A pump that claims an
     *            ARMED parker records readiness + unlinks but does NOT
     *            re-queue (the g hasn't parked); the g's commit CAS then
     *            fails, so it aborts the park and returns ready_mask
     *            instead -- no lost wake, no double-resume. */
    int commit;
    /* Dwell-based stack reclaim (PYGO_STACK_PARK_SWEEP).  park_ts is the
     * monotonic time the g committed to parking; the hub-idle sweep
     * madvises the stacks of its own parkers whose dwell exceeds a
     * threshold.  reclaimed=1 means the sweep already dropped this
     * park's idle pages, so re-sweeps skip it until the next park
     * (pool acquire zeroes both). */
    long long park_ts;
    int reclaimed;
} pygo_parked_t;

#define PYGO_PARK_ARMED  0
#define PYGO_PARK_PARKED 1
#define PYGO_PARK_WOKEN  2

/* Forcibly wake all parked goroutines with a cancelled marker.
 * Returns count of waiters woken.  Used by sched_reset() so paio.run
 * cleanup doesn't leave the next pygo_core.run() blocking on parked
 * accept loops / tickers / etc. */

/* ---------------------------------------------------------------------------
 * netpoll.c is split across the netpoll_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only netpoll.c.
 * --------------------------------------------------------------------------- */
#include "netpoll_parkers.c.inc"
#include "netpoll_parker_link.c.inc"
#include "netpoll_diag_fd.c.inc"
#include "netpoll_init.c.inc"
#include "netpoll_register.c.inc"
#include "netpoll_wake_iouring.c.inc"
#include "netpoll_wait_fd.c.inc"
#include "netpoll_pump_helpers.c.inc"
#include "netpoll_pump.c.inc"
