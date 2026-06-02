/* pygo_tcp.c -- pygo_core.TCPConn type, the thin C wrapper around a
 * socket that bypasses Python's socket.socket entirely for the hot
 * path.  See pygo_tcp.h for the API surface.
 *
 * Each method's structure is:
 *   1. try the syscall (recv / send / accept / connect)
 *   2. on EAGAIN, park on netpoll via pygo_netpoll_wait_fd
 *   3. loop
 *
 * The netpoll registration is ET register-once (see netpoll.c) so
 * the first wait_fd call on a fd costs one epoll_ctl ADD and every
 * subsequent call is zero syscalls.
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

#include "pygo_tcp.h"
#include "plat.h"
#include "plat_compat.h"
#include "netpoll.h"
#include "io_uring.h"
#include "pygo_blockpool.h"
#include "mn_sched.h"
#include "pygo_sched.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

/* TCPConn type struct, declared up front so the linux-only iouring
 * helpers below can read the iouring_choice field directly. */
typedef struct pygo_tcpconn_s {
    PyObject_HEAD
    int fd;          /* underlying socket fd; -1 if closed */
    int family;      /* AF_INET / AF_INET6 / etc */
    int is_listener; /* True after listen() succeeds */
    int closed;
#if defined(__linux__)
    /* Lazily-allocated multishot recv handle.  NULL until the first
     * iouring recv on this conn; freed in close. */
    pygo_iouring_ms_t *ms;
    /* Per-conn backend decision, latched on first recv.  See
     * pygo_tcpconn_use_iouring for the latching rationale. */
    int iouring_choice;
#endif
} PygoTCPConn;

#if defined(__linux__)
/* PYGO_TCPCONN_IOURING controls TCPConn's recv/send backend:
 *   unset / "0" : epoll register-once + recv()/send() (default).
 *                 Fastest for N <= ~1024 concurrent conns on current
 *                 Linux after the netpoll O(1) parker-index fix.
 *   "1"         : io_uring multishot recv unconditionally.  Slower at
 *                 low N (~14% gap) but wins at very-high N.
 *   "auto"      : start in epoll mode; switch this conn over to
 *                 iouring multishot when the live TCPConn population
 *                 crosses PYGO_TCPCONN_IOURING_THRESHOLD (default
 *                 2048, the empirical crossover point on echo
 *                 workloads).
 *
 * Mode is resolved once on first read.  Active-conn count is
 * maintained atomically and consulted only when mode == auto. */
enum {
    PYGO_IOURING_MODE_OFF  = 0,
    PYGO_IOURING_MODE_ON   = 1,
    PYGO_IOURING_MODE_AUTO = 2,
};
static int pygo_tcpconn_iouring_mode = -1;
static int pygo_tcpconn_iouring_threshold = 2048;
static volatile int pygo_tcpconn_live_count = 0;

static void pygo_tcpconn_resolve_mode(void)
{
    const char *e = getenv("PYGO_TCPCONN_IOURING");
    const char *t = getenv("PYGO_TCPCONN_IOURING_THRESHOLD");
    int mode = PYGO_IOURING_MODE_OFF;
    if (e != NULL) {
        if (e[0] == '1') mode = PYGO_IOURING_MODE_ON;
        else if (strcmp(e, "auto") == 0) mode = PYGO_IOURING_MODE_AUTO;
    }
    pygo_tcpconn_iouring_mode = mode;
    if (t != NULL) {
        int v = atoi(t);
        if (v > 0) pygo_tcpconn_iouring_threshold = v;
    }
}

/* Resolve the backend choice for this specific conn.  Sticky: once a
 * conn picks epoll, it stays on epoll for life (a mid-life flip to
 * iouring would leave a stale netpoll-epoll registration competing
 * with a fresh multishot SQE on the same fd).  Once a conn picks
 * iouring, it stays on iouring. */
static int pygo_tcpconn_use_iouring(PygoTCPConn *self)
{
    int mode;
    if (self->iouring_choice >= 0) return self->iouring_choice;
    if (pygo_tcpconn_iouring_mode < 0) pygo_tcpconn_resolve_mode();
    mode = pygo_tcpconn_iouring_mode;
    if (mode == PYGO_IOURING_MODE_OFF) {
        self->iouring_choice = 0;
    } else if (mode == PYGO_IOURING_MODE_ON) {
        self->iouring_choice = pygo_iouring_available() ? 1 : 0;
    } else {
        /* auto */
        if (pygo_iouring_available() &&
            __atomic_load_n(&pygo_tcpconn_live_count, __ATOMIC_ACQUIRE)
                >= pygo_tcpconn_iouring_threshold) {
            self->iouring_choice = 1;
        } else {
            self->iouring_choice = 0;
        }
    }
    return self->iouring_choice;
}
#endif

#if defined(PYGO_OS_WINDOWS)
   /* winsock2.h + ws2tcpip.h + windows.h pulled in by plat_compat.h. */
#  define PYGO_SOCK_T   SOCKET
#  define PYGO_BADSOCK  INVALID_SOCKET
#  define pygo_closesock(s) closesocket(s)
#else
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <netdb.h>
#  include <fcntl.h>
#  include <unistd.h>
#  include <arpa/inet.h>
#  define PYGO_SOCK_T   int
#  define PYGO_BADSOCK  (-1)
#  define pygo_closesock(s) close(s)
#endif

#define PYGO_NETPOLL_READ  0x1
#define PYGO_NETPOLL_WRITE 0x2

int pygo_netpoll_wait_fd(int fd, int events, long long timeout_ns);

/* ============================================================
 * Type object  (struct definition is above the iouring helpers)
 * ============================================================ */
static PyTypeObject PygoTCPConnType;

/* ---------------------------------------------------------------------------
 * pygo_tcp.c is split across the pygo_tcp_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only pygo_tcp.c.
 * --------------------------------------------------------------------------- */
#include "pygo_tcp_helpers.c.inc"
#include "pygo_tcp_conn_io.c.inc"
#include "pygo_tcp_conn_send.c.inc"
#include "pygo_tcp_conn_net.c.inc"
#include "pygo_tcp_type_init.c.inc"
