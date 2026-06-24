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
 *   - the current fiber snapshot tstate, gets pushed onto an internal
 *     "parked" list with (fd, events, deadline) metadata.  yields via
 *     runloom_coro_yield.
 *   - the scheduler's drain loop, when ready queue is empty, calls
 *     runloom_netpoll_pump(timeout) instead of sleeping the OS thread.
 *   - pump waits for I/O / timeout, wakes parked fibers, returns.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"
#include "netpoll.h"
#include "coro.h"
#include "runloom_sched.h"
#include "mn_sched.h"
#include "io_uring.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_fsm.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#if !defined(RUNLOOM_OS_WINDOWS)
#  include <sys/resource.h>   /* getrlimit(RLIMIT_NOFILE) for fd-array sizing */
#endif

#if defined(RUNLOOM_HAVE_EPOLL)
#  include <sys/epoll.h>
#  include <sys/eventfd.h>
#  include <stdint.h>
#  include <unistd.h>
#  include <poll.h>        /* pending_wake_consume re-checks real fd readiness */
#elif defined(RUNLOOM_HAVE_KQUEUE)
#  include <sys/event.h>
#  include <sys/time.h>
#  include <unistd.h>
#  include <fcntl.h>      /* self-pipe wake: O_NONBLOCK / FD_CLOEXEC */
#elif defined(RUNLOOM_OS_WINDOWS)
   /* winsock2.h, ws2tcpip.h and windows.h are already pulled in via
    * plat_compat.h.  WSAPoll + WSAPOLLFD live in winsock2.h, FD_SET /
    * FD_ISSET likewise -- no extra header needed here. */
   /* IOCP-AFD backend prototypes (runloom_iocp_cancel/submit/wait/...).  Pulled
    * in HERE -- before the .c.inc fragments below -- because the single
    * parker-unlink choke point in netpoll_parker_link.c.inc (the FIRST fragment
    * after the parker pool) calls runloom_iocp_cancel to tear down a released
    * parker's in-flight AFD IRP.  netpoll_diag_fd.c.inc re-includes this header
    * (idempotent via its guard) for the backend-selection statics.  */
#  include "netpoll_iocp.h"
   /* Runtime backend-selection flag; the definition (a file-scope static) lives
    * in netpoll_diag_fd.c.inc, included further down.  Forward-declared here so
    * the earlier parker-link fragment can gate its IOCP cancel on it.  A static
    * forward decl + later static definition is one internal-linkage object. */
static int runloom_win_use_iocp;
#else
#  include <sys/select.h>
#  include <unistd.h>
#  include <fcntl.h>      /* self-pipe wake: O_NONBLOCK / FD_CLOEXEC */
#endif

/* ---- internal park record ----
 * Allocated on the parked fiber's C stack inside runloom_netpoll_wait_fd
 * (the stack stays alive across yield because the fiber isn't
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
typedef struct runloom_parked {
    int fd;
    int events;
    long long deadline_ns;     /* -1 = forever */
    int *ready_out;            /* where to store the wakeup mask */
    runloom_g_t *g;
    void *hub;                  /* M:N hub opaque; NULL = global sched */
    struct runloom_parked  *next;
    struct runloom_parked **slot;
    struct runloom_parked  *next_by_fd;
    struct runloom_parked  *prev_by_fd;
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
    struct runloom_parked *pool_next;
    /* Atomic park/wake commit (Go netpollblockcommit, adapted to runloom's
     * re-queue model).  Closes the residual lost-wake: a pump can claim
     * a still-linked parker in the window between wait_fd's last
     * readiness re-check and its commit to parking.  Exactly one of
     * {pump, parking g} CASes this away from ARMED:
     *   ARMED  - linked, g has not yet committed to parking.
     *   PARKED - g committed (yielded / about to); a pump that claims a
     *            PARKED parker re-queues the g via runloom_mn_wake_g.
     *   WOKEN  - claimed (by a pump or cancel).  A pump that claims an
     *            ARMED parker records readiness + unlinks but does NOT
     *            re-queue (the g hasn't parked); the g's commit CAS then
     *            fails, so it aborts the park and returns ready_mask
     *            instead -- no lost wake, no double-resume. */
    int commit;
    /* Dwell-based stack reclaim (RUNLOOM_STACK_PARK_SWEEP).  park_ts is the
     * monotonic time the g committed to parking; the hub-idle sweep
     * madvises the stacks of its own parkers whose dwell exceeds a
     * threshold.  reclaimed=1 means the sweep already dropped this
     * park's idle pages, so re-sweeps skip it until the next park
     * (pool acquire zeroes both). */
    long long park_ts;
    int reclaimed;
    /* Windows IOCP-AFD backend ONLY.  WEAK reference to the per-park
     * runloom_poll_ctx_t whose AFD_POLL IRP this parker submitted
     * (netpoll_wait_fd's IOCP branch).  The ctx is owned and freed
     * EXCLUSIVELY by runloom_iocp_wait when its completion drains -- the
     * parker never frees it; it only needs the pointer to runloom_iocp_cancel
     * the still-in-flight IRP when the parker is released early (deadline
     * heap timeout, fd-ready dispatch on a sibling completion, cross-fiber
     * close, cancel_all).  Stored under pool->lock at submit, cancelled +
     * cleared at the single unlink choke point (runloom_parker_unlink).
     * Typed void* to keep runloom_poll_ctx_t private to netpoll_iocp.c (no
     * header cycle).  NULL on every non-Windows / non-IOCP park. */
    void *iocp_ctx;
} runloom_parked_t;

#define RUNLOOM_PARK_ARMED  0
#define RUNLOOM_PARK_PARKED 1
#define RUNLOOM_PARK_WOKEN  2

/* ---- parker-commit FSM transition table (OBSERVATIONAL) ---------------------
 * The Go-netpollblockcommit claim protocol (GenMC-proven in
 * tools/verify/genmc/netpoll_claim.c).  Three states (ARMED/PARKED/WOKEN), two events:
 *   COMMIT -- the parking g commits to parking (the sole ARMED->PARKED writer).
 *   CLAIM  -- a waker (pump fd-ready / timeout sweep / cancel / unpark) claims
 *             the parker, CASing it to WOKEN; legal from ARMED (g not yet parked
 *             -> record readiness, do NOT re-queue) or PARKED (re-queue).
 * WOKEN is terminal (a claimed parker is released back to its pool).  Exactly
 * one of {pump, parking g} ever moves commit off ARMED -- the exactly-once-wake
 * guarantee.  This table never DRIVES commit (the proven CASes still do); it is
 * consulted only by RUNLOOM_PARK_NOTE() to abort under -DRUNLOOM_FSM_VALIDATE if
 * a live CAS ever performs an edge the relation does not contain.  Zero cost in
 * a normal build. */
enum {
    RUNLOOM_PARK_EV_COMMIT = 0,   /* g commits to parking: ARMED -> PARKED       */
    RUNLOOM_PARK_EV_CLAIM,        /* a waker claims the parker -> WOKEN          */
    RUNLOOM_PARK_EV_COUNT
};
#define RUNLOOM_PARK_STATE_COUNT 3   /* ARMED, PARKED, WOKEN */

static const signed char runloom_park_table
        [RUNLOOM_PARK_STATE_COUNT][RUNLOOM_PARK_EV_COUNT]
        __attribute__((unused)) = {
    /*                      COMMIT                CLAIM */
    [RUNLOOM_PARK_ARMED]  = { RUNLOOM_PARK_PARKED, RUNLOOM_PARK_WOKEN  },
    [RUNLOOM_PARK_PARKED] = { RUNLOOM_FSM_INVALID, RUNLOOM_PARK_WOKEN  },
    [RUNLOOM_PARK_WOKEN]  = { RUNLOOM_FSM_INVALID, RUNLOOM_FSM_INVALID },
};
RUNLOOM_FSM_ASSERT_TABLE(runloom_park_table, RUNLOOM_PARK_STATE_COUNT,
                         RUNLOOM_PARK_EV_COUNT, "parker_commit");

/* Convenience: assert the (from->to) edge exists in the parker-commit relation.
 * Expands to nothing unless -DRUNLOOM_FSM_VALIDATE. */
#define RUNLOOM_PARK_NOTE(from, to)                                           \
    RUNLOOM_FSM_NOTE("parker_commit", runloom_park_table,                     \
                     RUNLOOM_PARK_STATE_COUNT, RUNLOOM_PARK_EV_COUNT,         \
                     (from), (to))

/* Forcibly wake all parked fibers with a cancelled marker.
 * Returns count of waiters woken.  Used by sched_reset() so paio.run
 * cleanup doesn't leave the next runloom_c.run() blocking on parked
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
