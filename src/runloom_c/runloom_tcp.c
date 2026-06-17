/* runloom_tcp.c -- runloom_c.TCPConn type, the thin C wrapper around a
 * socket that bypasses Python's socket.socket entirely for the hot
 * path.  See runloom_tcp.h for the API surface.
 *
 * Each method's structure is:
 *   1. try the syscall (recv / send / accept / connect)
 *   2. on EAGAIN, park on netpoll via runloom_netpoll_wait_fd
 *   3. loop
 *
 * The netpoll registration is LEVEL-triggered, armed once per direction on
 * epoll (EV_ONESHOT re-armed per park on kqueue) -- see netpoll_register.c.inc.
 * The first wait_fd call on a fd costs one epoll_ctl ADD and every subsequent
 * same-direction call is zero syscalls.
 *
 * Platform notes:
 *   POSIX: recv()/send()/accept4()/connect() with non-blocking fds.
 *   Windows: same surface; recv/send map to Winsock, the underlying
 *            wait_fd routes through IOCP-AFD / WSAPoll / select.
 *            Buffer pointers stay valid across coro yields because
 *            the syscall is synchronous from our side; the actual
 *            wait is on epoll/IOCP, not in the recv() call.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "runloom_tcp.h"
#include "plat.h"
#include "plat_compat.h"
#include "netpoll.h"
#include "io_uring.h"
#include "runloom_blockpool.h"
#include "mn_sched.h"
#include "runloom_sched.h"
#include "runloom_io_fsm.h"   /* the total (rc,errno)->event I/O classifier */

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

/* TCPConn type struct, declared up front so the linux-only iouring
 * helpers below can read the iouring_choice field directly. */
typedef struct runloom_tcpconn_s {
    PyObject_HEAD
    int fd;          /* underlying socket fd; -1 if closed */
    int family;      /* AF_INET / AF_INET6 / etc */
    int is_listener; /* True after listen() succeeds */
    int closed;
#if defined(__linux__)
    /* Lazily-allocated multishot recv handle.  NULL until the first
     * iouring recv on this conn; freed in close. */
    runloom_iouring_ms_t *ms;
    /* Per-conn backend decision, latched on first recv.  See
     * runloom_tcpconn_use_iouring for the latching rationale. */
    int iouring_choice;
#endif
} RunloomTCPConn;

#if defined(__linux__)
/* RUNLOOM_TCPCONN_IOURING controls TCPConn's recv/send backend:
 *   unset / "0" : epoll register-once + recv()/send() (default).
 *                 Fastest for N <= ~1024 concurrent conns on current
 *                 Linux after the netpoll O(1) parker-index fix.
 *   "1"         : io_uring multishot recv unconditionally.  Slower at
 *                 low N (~14% gap) but wins at very-high N.
 *   "auto"      : start in epoll mode; switch this conn over to
 *                 iouring multishot when the live TCPConn population
 *                 crosses RUNLOOM_TCPCONN_IOURING_THRESHOLD (default
 *                 2048, the empirical crossover point on echo
 *                 workloads).
 *
 * Mode is resolved once on first read.  Active-conn count is
 * maintained atomically and consulted only when mode == auto. */
enum {
    RUNLOOM_IOURING_MODE_OFF  = 0,
    RUNLOOM_IOURING_MODE_ON   = 1,
    RUNLOOM_IOURING_MODE_AUTO = 2,
};
static int runloom_tcpconn_iouring_mode = -1;
static int runloom_tcpconn_iouring_threshold = 2048;
static volatile int runloom_tcpconn_live_count = 0;

static void runloom_tcpconn_resolve_mode(void)
{
    const char *e = getenv("RUNLOOM_TCPCONN_IOURING");
    const char *t = getenv("RUNLOOM_TCPCONN_IOURING_THRESHOLD");
    int mode = RUNLOOM_IOURING_MODE_OFF;
    if (e != NULL) {
        if (e[0] == '1') mode = RUNLOOM_IOURING_MODE_ON;
        else if (strcmp(e, "auto") == 0) mode = RUNLOOM_IOURING_MODE_AUTO;
    }
    runloom_tcpconn_iouring_mode = mode;
    if (t != NULL) {
        int v = atoi(t);
        if (v > 0) runloom_tcpconn_iouring_threshold = v;
    }
}

/* Resolve the backend choice for this specific conn.  Sticky: once a
 * conn picks epoll, it stays on epoll for life (a mid-life flip to
 * iouring would leave a stale netpoll-epoll registration competing
 * with a fresh multishot SQE on the same fd).  Once a conn picks
 * iouring, it stays on iouring. */
static int runloom_tcpconn_use_iouring(RunloomTCPConn *self)
{
    int mode;
    if (self->iouring_choice >= 0) return self->iouring_choice;
    if (runloom_tcpconn_iouring_mode < 0) runloom_tcpconn_resolve_mode();
    mode = runloom_tcpconn_iouring_mode;
    if (mode == RUNLOOM_IOURING_MODE_OFF) {
        self->iouring_choice = 0;
    } else if (mode == RUNLOOM_IOURING_MODE_ON) {
        self->iouring_choice = runloom_iouring_available() ? 1 : 0;
    } else {
        /* auto */
        if (runloom_iouring_available() &&
            __atomic_load_n(&runloom_tcpconn_live_count, __ATOMIC_ACQUIRE)
                >= runloom_tcpconn_iouring_threshold) {
            self->iouring_choice = 1;
        } else {
            self->iouring_choice = 0;
        }
    }
    return self->iouring_choice;
}
#endif

#if defined(RUNLOOM_OS_WINDOWS)
   /* winsock2.h + ws2tcpip.h + windows.h pulled in by plat_compat.h. */
#  define RUNLOOM_SOCK_T   SOCKET
#  define RUNLOOM_BADSOCK  INVALID_SOCKET
#  define runloom_closesock(s) closesocket(s)
#else
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <netdb.h>
#  include <fcntl.h>
#  include <unistd.h>
#  include <arpa/inet.h>
#  define RUNLOOM_SOCK_T   int
#  define RUNLOOM_BADSOCK  (-1)
#  define runloom_closesock(s) close(s)
#endif

#define RUNLOOM_NETPOLL_READ  0x1
#define RUNLOOM_NETPOLL_WRITE 0x2

int runloom_netpoll_wait_fd(int fd, int events, long long timeout_ns);
/* Cooperative-socket wait_fd (maps the CANCELLED sentinel to errno=ECANCELED/-1
 * so the socket fast paths raise instead of re-parking on cancel).  Defined once
 * in the netpoll TU (netpoll_wait_fd.c.inc), shared with module_tcp.c.inc so the
 * monkey tcp_recv/send fast paths honour cancel too.  Audit finding B3. */
int runloom_netpoll_wait_fd_coop(int fd, int events, long long timeout_ns);

/* ============================================================
 * Type object  (struct definition is above the iouring helpers)
 * ============================================================ */
static PyTypeObject RunloomTCPConnType;

/* ---------------------------------------------------------------------------
 * runloom_tcp.c is split across the runloom_tcp_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only runloom_tcp.c.
 * --------------------------------------------------------------------------- */
#include "runloom_tcp_helpers.c.inc"
#include "runloom_tcp_conn_io.c.inc"
#include "runloom_tcp_conn_send.c.inc"
#include "runloom_tcp_conn_net.c.inc"
#include "runloom_tcp_type_init.c.inc"
