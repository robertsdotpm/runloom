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

/* ============================================================
 * Helpers
 * ============================================================ */

/* Set O_NONBLOCK on a fd.  Idempotent. */
static int pygo_set_nonblock(int fd)
{
#if defined(PYGO_OS_WINDOWS)
    u_long mode = 1;
    if (ioctlsocket((SOCKET)fd, FIONBIO, &mode) == SOCKET_ERROR) {
        return -1;
    }
    return 0;
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (flags & O_NONBLOCK) return 0;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
}

/* Set TCP_NODELAY.  Best effort; ignore errors. */
static void pygo_set_nodelay(int fd, int family)
{
    int on = 1;
    if (family != AF_INET && family != AF_INET6) return;
#if defined(PYGO_OS_WINDOWS)
    (void)setsockopt((SOCKET)fd, IPPROTO_TCP, TCP_NODELAY,
                     (const char *)&on, sizeof(on));
#else
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#endif
}

/* Translate the last errno/WSAGetLastError to a Python exception. */
static PyObject *pygo_raise_errno(void)
{
#if defined(PYGO_OS_WINDOWS)
    return PyErr_SetFromWindowsErr(WSAGetLastError());
#else
    return PyErr_SetFromErrno(PyExc_OSError);
#endif
}

/* ---- test-only socket-surface fault injection (kqueue/Windows) -------------
 * Linux faults the socket syscalls with strace; the kqueue/Windows backends
 * have no such tracer, so they gate in-process on PYGO_FAULT_TCP_<CALL> (see
 * netpoll.c).  pygo_tcp_fault_armed() caches whether ANY such env is set, so
 * the hot recv/send path pays only a load+branch when disarmed -- the getenv
 * scan runs once.  cached is written idempotently (always the same 0/1), so
 * the benign first-call race needs no atomic.  Compiled out on Linux, where
 * PYGO_TCP_FINJ() is a constant 0 (no overhead, no reference to the helper).
 *
 * Usage:  int injerr = PYGO_TCP_FINJ(SITE);
 *         r = injerr ? (errno = injerr, -1) : <real syscall>; */
#if defined(PYGO_OS_WINDOWS) || defined(PYGO_HAVE_KQUEUE)
static int pygo_tcp_fault_armed(void)
{
    static int cached = -1;
    if (cached < 0)
        cached = (getenv("PYGO_FAULT_TCP_SOCKET")  || getenv("PYGO_FAULT_TCP_CONNECT") ||
                  getenv("PYGO_FAULT_TCP_ACCEPT")  || getenv("PYGO_FAULT_TCP_RECV")    ||
                  getenv("PYGO_FAULT_TCP_SEND")) ? 1 : 0;
    return cached;
}
#  define PYGO_TCP_FINJ(site) (pygo_tcp_fault_armed() ? pygo_fault_inject(site) : 0)
#else
#  define PYGO_TCP_FINJ(site) 0
#endif

#if defined(PYGO_OS_WINDOWS)
/* Used only on Windows branches below; the POSIX paths inline the
 * errno comparisons.  Tagged so non-Windows builds (which include
 * this header anyway for the prototype) don't warn. */
static int pygo_is_wouldblock(void)
{
    return WSAGetLastError() == WSAEWOULDBLOCK;
}

static int pygo_is_intr(void)
{
    return WSAGetLastError() == WSAEINTR;
}
#endif

/* ============================================================
 * Address parsing -- AF_INET / AF_INET6.
 *
 * Inputs accepted:
 *   ("1.2.3.4", 8080)         -> AF_INET
 *   ("::1", 8080)             -> AF_INET6
 *   ("example.com", 8080)     -> getaddrinfo, returns first usable
 *
 * Returns 0 on success and fills `*storage` + `*addrlen` + `*family`.
 * On error sets a Python exception and returns -1.
 * ============================================================ */
/* getaddrinfo offloaded to the blocking pool.  Job lives on the
 * resolving goroutine's coroutine stack (alive across the park); the
 * worker runs getaddrinfo with no GIL. */
typedef struct {
    const char *host;
    const char *portbuf;
    const struct addrinfo *hints;
    struct addrinfo *res;
    int rc;
} pygo_resolve_job_t;

static void *pygo_resolve_worker(void *p)
{
    pygo_resolve_job_t *j = (pygo_resolve_job_t *)p;
    j->rc = getaddrinfo(j->host, j->portbuf, j->hints, &j->res);
    return NULL;
}

static int pygo_resolve(const char *host, int port, int want_passive,
                        struct sockaddr_storage *storage,
                        socklen_t *addrlen, int *family)
{
    struct addrinfo hints, *res = NULL, *p;
    char portbuf[16];
    int rc;
    pygo_sched_t *sched;
    int in_goroutine;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (want_passive) hints.ai_flags |= AI_PASSIVE;
    /* AI_NUMERICSERV: we always pass a numeric port. */
    hints.ai_flags |= AI_NUMERICSERV;

    snprintf(portbuf, sizeof(portbuf), "%d", port);

    /* getaddrinfo is a non-preemptible blocking C call.  If we are
     * running inside a goroutine, offload it to the blocking pool and
     * park -- otherwise it would wedge the goroutine's hub (or the
     * single-thread scheduler), stranding everything queued behind it.
     * The worker runs getaddrinfo with no GIL, so we must NOT bracket
     * the offload in Py_BEGIN_ALLOW_THREADS (you cannot yield a
     * coroutine across that macro pair).  Outside a goroutine there is
     * nothing to park, so fall back to the classic inline lookup with
     * the GIL released. */
    sched = pygo_sched_get();
    in_goroutine = (pygo_mn_current_hub_opaque() != NULL) ||
                   (sched != NULL && sched->current != NULL);
    if (in_goroutine) {
        pygo_resolve_job_t job;
        job.host = host; job.portbuf = portbuf; job.hints = &hints;
        job.res = NULL;  job.rc = 0;
        pygo_blocking_call(pygo_resolve_worker, &job);
        rc  = job.rc;
        res = job.res;
    } else {
        Py_BEGIN_ALLOW_THREADS
        rc = getaddrinfo(host, portbuf, &hints, &res);
        Py_END_ALLOW_THREADS
    }
    if (rc != 0 || res == NULL) {
        PyErr_Format(PyExc_OSError, "getaddrinfo: %s",
                     gai_strerror(rc));
        if (res) freeaddrinfo(res);
        return -1;
    }
    for (p = res; p != NULL; p = p->ai_next) {
        if (p->ai_addrlen <= sizeof(*storage)) {
            memcpy(storage, p->ai_addr, p->ai_addrlen);
            *addrlen = (socklen_t)p->ai_addrlen;
            *family  = p->ai_family;
            freeaddrinfo(res);
            return 0;
        }
    }
    freeaddrinfo(res);
    PyErr_SetString(PyExc_OSError, "getaddrinfo: no usable address");
    return -1;
}

/* ============================================================
 * Construction / destruction
 * ============================================================ */

/* Allocate + zero-init a TCPConn from a type.  Centralises the
 * tp_alloc + ms/fd/closed defaults + live-count increment that
 * listen/accept/connect would otherwise have to duplicate. */
static PygoTCPConn *PygoTCPConn_alloc(PyTypeObject *type)
{
    PygoTCPConn *self = (PygoTCPConn *)type->tp_alloc(type, 0);
    if (self == NULL) return NULL;
    self->fd = -1;
    self->family = 0;
    self->is_listener = 0;
    self->closed = 0;
#if defined(__linux__)
    self->ms = NULL;
    self->iouring_choice = -1;
    __atomic_add_fetch(&pygo_tcpconn_live_count, 1, __ATOMIC_RELEASE);
#endif
    return self;
}

static PyObject *PygoTCPConn_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return (PyObject *)PygoTCPConn_alloc(type);
}

static int PygoTCPConn_init(PygoTCPConn *self, PyObject *args, PyObject *kwds)
{
    /* TCPConn(fd) -- wrap an existing non-blocking fd.  The fd is
     * stolen: close() / GC will close it.  Use this for accepted
     * connections handed up from a different listener.
     *
     * If you want the high-level API, use TCPConn.connect(host, port)
     * or TCPConn.listen(host, port). */
    int fd;
    static char *kwlist[] = {"fd", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i", kwlist, &fd))
        return -1;
    if (fd < 0) {
        PyErr_SetString(PyExc_ValueError, "fd must be >= 0");
        return -1;
    }
    self->fd = fd;
    self->family = AF_INET;       /* unknown; callers can override */
    self->closed = 0;
    self->is_listener = 0;
    (void)pygo_set_nonblock(fd);
    pygo_set_nodelay(fd, self->family);
    return 0;
}

static void PygoTCPConn_dealloc(PygoTCPConn *self)
{
    if (!self->closed && self->fd >= 0) {
#if defined(__linux__)
        if (self->ms != NULL) {
            pygo_iouring_ms_close(self->ms);
            self->ms = NULL;
        }
#endif
        pygo_netpoll_unregister(self->fd);
        pygo_closesock((PYGO_SOCK_T)self->fd);
        self->fd = -1;
        self->closed = 1;
    }
#if defined(__linux__)
    __atomic_sub_fetch(&pygo_tcpconn_live_count, 1, __ATOMIC_RELEASE);
#endif
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoTCPConn_close(PygoTCPConn *self, PyObject *unused)
{
    (void)unused;
    if (!self->closed && self->fd >= 0) {
#if defined(__linux__)
        if (self->ms != NULL) {
            pygo_iouring_ms_close(self->ms);
            self->ms = NULL;
        }
#endif
        pygo_netpoll_unregister(self->fd);
        pygo_closesock((PYGO_SOCK_T)self->fd);
        self->fd = -1;
        self->closed = 1;
    }
    Py_RETURN_NONE;
}

static PyObject *PygoTCPConn_fileno(PygoTCPConn *self, PyObject *unused)
{
    (void)unused;
    return PyLong_FromLong((long)self->fd);
}

static PyObject *PygoTCPConn_is_closed(PygoTCPConn *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong((long)self->closed);
}

static PyObject *PygoTCPConn_family_get(PygoTCPConn *self, void *closure)
{
    (void)closure;
    return PyLong_FromLong((long)self->family);
}

/* ============================================================
 * recv / recv_into / send / send_all
 *
 * All four below are the hot path: a tight C loop that issues the
 * syscall, parks on EAGAIN, and resumes.  No PyObject churn except
 * the bytes-result alloc on recv() (and not even that on recv_into).
 * ============================================================ */

static PyObject *PygoTCPConn_recv(PygoTCPConn *self, PyObject *args)
{
    Py_ssize_t n_bytes;
    int flags = 0;
    Py_ssize_t got = 0;
    PyObject *result;
    char *out;
    int fd;
#if defined(__linux__)
    int use_iouring;
#endif

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "n|i", &n_bytes, &flags)) return NULL;
    if (n_bytes < 0) {
        PyErr_SetString(PyExc_ValueError, "negative bufsize");
        return NULL;
    }
    if (n_bytes == 0) return PyBytes_FromStringAndSize(NULL, 0);

    fd = self->fd;
    result = PyBytes_FromStringAndSize(NULL, n_bytes);
    if (result == NULL) return NULL;
    out = PyBytes_AS_STRING(result);

#if defined(__linux__)
    use_iouring = pygo_tcpconn_use_iouring(self);
    if (use_iouring) {
        pygo_iouring_ssize_t r;
        /* Multishot when available and the call carries no special
         * MSG_* flags (multishot SQE is fire-and-forget without
         * per-call flags). */
        if (flags == 0 && pygo_iouring_pbuf_available()) {
            if (self->ms == NULL) {
                self->ms = pygo_iouring_ms_open(fd);
            }
            if (self->ms != NULL) {
                r = pygo_iouring_ms_recv(self->ms, out, (size_t)n_bytes);
                if (r < 0) {
                    Py_DECREF(result);
                    return PyErr_SetFromErrno(PyExc_OSError);
                }
                got = (Py_ssize_t)r;
                if (got < n_bytes) {
                    if (_PyBytes_Resize(&result, got) < 0) return NULL;
                }
                return result;
            }
        }
        r = pygo_iouring_recv(fd, out, (size_t)n_bytes, flags);
        if (r < 0) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        got = (Py_ssize_t)r;
        if (got < n_bytes) {
            if (_PyBytes_Resize(&result, got) < 0) return NULL;
        }
        return result;
    }
#endif

    while (1) {
#if defined(PYGO_OS_WINDOWS)
        int r = recv((SOCKET)fd, out, (int)n_bytes, flags);
        if (r >= 0) { got = r; break; }
        if (!pygo_is_wouldblock() && !pygo_is_intr()) {
            Py_DECREF(result);
            return pygo_raise_errno();
        }
#else
        int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_RECV);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : recv(fd, out, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_READ, -1LL) < 0) {
            Py_DECREF(result);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    if (got < n_bytes) {
        if (_PyBytes_Resize(&result, got) < 0) return NULL;
    }
    return result;
}

static PyObject *PygoTCPConn_recv_into(PygoTCPConn *self, PyObject *args)
{
    Py_buffer buf;
    Py_ssize_t n_bytes = 0;
    int flags = 0;
    Py_ssize_t got = 0;
    int fd;

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "w*|ni", &buf, &n_bytes, &flags))
        return NULL;
    if (n_bytes == 0 || n_bytes > buf.len) n_bytes = buf.len;
    if (n_bytes <= 0) {
        PyBuffer_Release(&buf);
        return PyLong_FromLong(0);
    }
    fd = self->fd;

#if defined(__linux__)
    if (pygo_tcpconn_use_iouring(self)) {
        pygo_iouring_ssize_t r;
        if (flags == 0 && pygo_iouring_pbuf_available()) {
            if (self->ms == NULL) {
                self->ms = pygo_iouring_ms_open(fd);
            }
            if (self->ms != NULL) {
                r = pygo_iouring_ms_recv(self->ms, buf.buf, (size_t)n_bytes);
                PyBuffer_Release(&buf);
                if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
                return PyLong_FromSsize_t((Py_ssize_t)r);
            }
        }
        /* Single-shot IORING_OP_RECV fallback (kernel < 5.19, or
         * non-zero flags). */
        r = pygo_iouring_recv(fd, buf.buf, (size_t)n_bytes, flags);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif

    while (1) {
#if defined(PYGO_OS_WINDOWS)
        int r = recv((SOCKET)fd, (char *)buf.buf, (int)n_bytes, flags);
        if (r >= 0) { got = r; break; }
        if (!pygo_is_wouldblock() && !pygo_is_intr()) {
            PyBuffer_Release(&buf);
            return pygo_raise_errno();
        }
#else
        ssize_t r = recv(fd, (char *)buf.buf, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_READ, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
}

static PyObject *PygoTCPConn_send(PygoTCPConn *self, PyObject *args)
{
    Py_buffer buf;
    int flags = 0;
    Py_ssize_t sent = 0;
    int fd;

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "y*|i", &buf, &flags)) return NULL;
    fd = self->fd;

#if defined(__linux__)
    if (pygo_tcpconn_use_iouring(self)) {
        pygo_iouring_ssize_t r = pygo_iouring_send(fd, buf.buf,
                                                  (size_t)buf.len, flags);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif

    while (1) {
#if defined(PYGO_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf, (int)buf.len, flags);
        if (r >= 0) { sent = r; break; }
        if (!pygo_is_wouldblock() && !pygo_is_intr()) {
            PyBuffer_Release(&buf);
            return pygo_raise_errno();
        }
#else
        ssize_t r = send(fd, (const char *)buf.buf, (size_t)buf.len, flags);
        if (r >= 0) { sent = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}

static PyObject *PygoTCPConn_send_all(PygoTCPConn *self, PyObject *args)
{
    Py_buffer buf;
    int flags = 0;
    Py_ssize_t sent = 0;
    int fd;
#if defined(__linux__)
    int use_iouring;
#endif

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "y*|i", &buf, &flags)) return NULL;
    fd = self->fd;

#if defined(__linux__)
    use_iouring = pygo_tcpconn_use_iouring(self);
    if (use_iouring) {
        /* IORING_OP_SEND already handles short writes inside the
         * kernel for stream sockets -- the kernel buffers what it
         * can.  We still loop here because a single SEND op returns
         * what was accepted; if buf.len > SO_SNDBUF the kernel may
         * return partial and we must resubmit the remainder. */
        while (sent < buf.len) {
            pygo_iouring_ssize_t r =
                pygo_iouring_send(fd, (const char *)buf.buf + sent,
                                  (size_t)(buf.len - sent), flags);
            if (r < 0) {
                PyBuffer_Release(&buf);
                return PyErr_SetFromErrno(PyExc_OSError);
            }
            sent += (Py_ssize_t)r;
            if (r == 0) break;     /* defensive: avoid infinite loop */
        }
        PyBuffer_Release(&buf);
        return PyLong_FromSsize_t(sent);
    }
#endif

    while (sent < buf.len) {
#if defined(PYGO_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf + sent,
                     (int)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (!pygo_is_wouldblock() && !pygo_is_intr()) {
            PyBuffer_Release(&buf);
            return pygo_raise_errno();
        }
#else
        int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_SEND);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : send(fd, (const char *)buf.buf + sent,
                                  (size_t)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}

/* ============================================================
 * Listener / accept
 * ============================================================ */

static PyObject *PygoTCPConn_listen_cls(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    const char *host;
    int port;
    int backlog = 128;
    static char *kwlist[] = {"host", "port", "backlog", NULL};
    struct sockaddr_storage addr;
    socklen_t addrlen = 0;
    int family = 0;
    int fd;
    int on = 1;
    PygoTCPConn *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "si|i", kwlist,
                                     &host, &port, &backlog))
        return NULL;

    if (pygo_resolve(host, port, /*passive*/1, &addr, &addrlen, &family) != 0)
        return NULL;

#if defined(PYGO_OS_WINDOWS)
    pygo_winsock_init();
    {
        SOCKET s = socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (s == INVALID_SOCKET) return pygo_raise_errno();
        (void)setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
                         (const char *)&on, sizeof(on));
        if (bind(s, (struct sockaddr *)&addr, addrlen) == SOCKET_ERROR ||
            listen(s, backlog) == SOCKET_ERROR) {
            int saved = WSAGetLastError();
            closesocket(s);
            WSASetLastError(saved);
            return pygo_raise_errno();
        }
        fd = (int)s;
    }
#else
    {   int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_SOCKET);
        fd = injerr ? (errno = injerr, -1)
                    : socket(family, SOCK_STREAM, IPPROTO_TCP); }
    if (fd < 0) return PyErr_SetFromErrno(PyExc_OSError);
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (bind(fd, (struct sockaddr *)&addr, addrlen) < 0 ||
        listen(fd, backlog) < 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return PyErr_SetFromErrno(PyExc_OSError);
    }
#endif

    pygo_set_nonblock(fd);

    result = PygoTCPConn_alloc(cls);
    if (result == NULL) {
        pygo_closesock((PYGO_SOCK_T)fd);
        return NULL;
    }
    result->fd = fd;
    result->family = family;
    result->is_listener = 1;
    result->closed = 0;
    return (PyObject *)result;
}

static PyObject *PygoTCPConn_accept(PygoTCPConn *self, PyObject *unused)
{
    PygoTCPConn *result;
    int new_fd;
    (void)unused;

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!self->is_listener) {
        PyErr_SetString(PyExc_OSError, "TCPConn.accept on non-listener");
        return NULL;
    }

    while (1) {
#if defined(PYGO_OS_WINDOWS)
        SOCKET s = accept((SOCKET)self->fd, NULL, NULL);
        if (s != INVALID_SOCKET) { new_fd = (int)s; break; }
        if (!pygo_is_wouldblock() && !pygo_is_intr()) {
            return pygo_raise_errno();
        }
#else
        int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_ACCEPT);
        new_fd = injerr ? (errno = injerr, -1) : accept(self->fd, NULL, NULL);
        if (new_fd >= 0) break;
        /* ECONNABORTED: the peer reset between the SYN and our accept() -- a
         * transient, not a listener failure.  Go's netpoll and libuv both
         * retry it; surfacing it spuriously killed the accept loop. */
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR
            && errno != ECONNABORTED) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(self->fd, PYGO_NETPOLL_READ, -1LL) < 0) {
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    pygo_set_nonblock(new_fd);
    pygo_set_nodelay(new_fd, self->family);

    result = PygoTCPConn_alloc(Py_TYPE(self));
    if (result == NULL) {
        pygo_closesock((PYGO_SOCK_T)new_fd);
        return NULL;
    }
    result->fd = new_fd;
    result->family = self->family;
    result->is_listener = 0;
    result->closed = 0;
    return (PyObject *)result;
}

/* ============================================================
 * Client connect (classmethod)
 * ============================================================ */
static PyObject *PygoTCPConn_connect_cls(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    const char *host;
    int port;
    static char *kwlist[] = {"host", "port", NULL};
    struct sockaddr_storage addr;
    socklen_t addrlen = 0;
    int family = 0;
    int fd;
    PygoTCPConn *result;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "si", kwlist, &host, &port))
        return NULL;
    if (pygo_resolve(host, port, /*passive*/0, &addr, &addrlen, &family) != 0)
        return NULL;

#if defined(PYGO_OS_WINDOWS)
    pygo_winsock_init();
    {
        SOCKET s = socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (s == INVALID_SOCKET) return pygo_raise_errno();
        fd = (int)s;
    }
#else
    {   int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_SOCKET);
        fd = injerr ? (errno = injerr, -1)
                    : socket(family, SOCK_STREAM, IPPROTO_TCP); }
    if (fd < 0) return PyErr_SetFromErrno(PyExc_OSError);
#endif

    pygo_set_nonblock(fd);

    /* Non-blocking connect.  If the connect returns EINPROGRESS
     * (or WSAEWOULDBLOCK on Windows), park on WRITE and read
     * SO_ERROR to determine final status. */
#if defined(PYGO_OS_WINDOWS)
    rc = connect((SOCKET)fd, (struct sockaddr *)&addr, addrlen);
    if (rc == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err == WSAEWOULDBLOCK) {
            if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
                int saved = errno;
                closesocket((SOCKET)fd);
                errno = saved;
                /* wait_fd may have run a Python signal handler that raised (e.g. an
                 * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
                return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
            }
            int sockerr = 0; int optlen = sizeof(sockerr);
            if (getsockopt((SOCKET)fd, SOL_SOCKET, SO_ERROR,
                           (char *)&sockerr, &optlen) == 0 && sockerr != 0) {
                closesocket((SOCKET)fd);
                WSASetLastError(sockerr);
                return pygo_raise_errno();
            }
        } else {
            closesocket((SOCKET)fd);
            WSASetLastError(err);
            return pygo_raise_errno();
        }
    }
#else
    rc = connect(fd, (struct sockaddr *)&addr, addrlen);
    {   /* test-only: override the result AFTER the real connect() initiates the
         * connection, so EINTR is modelled as a signal on an in-flight connect
         * (park WRITE + SO_ERROR completes it) rather than a no-op that hangs. */
        int injerr = PYGO_TCP_FINJ(PYGO_FAULT_TCP_CONNECT);
        if (injerr) { rc = -1; errno = injerr; }
    }
    if (rc < 0) {
        /* EINTR is handled like EINPROGRESS, NOT as an error: POSIX says a
         * connect() interrupted by a signal is not aborted -- the connection
         * continues asynchronously, and the caller must wait for writability
         * and check SO_ERROR (re-issuing connect() would fail EALREADY/
         * EADDRINUSE).  Without this, a signal landing on the connect() syscall
         * spuriously failed the connection with OSError(EINTR) -- confirmed by
         * fault injection (strace -e inject=connect:error=EINTR:when=1). */
        if (errno == EINPROGRESS || errno == EAGAIN || errno == EINTR) {
            if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
                int saved = errno;
                close(fd);
                errno = saved;
                /* wait_fd may have run a Python signal handler that raised (e.g. an
                 * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
                return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
            }
            int sockerr = 0;
            socklen_t soptlen = sizeof(sockerr);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &soptlen) == 0
                && sockerr != 0) {
                close(fd);
                errno = sockerr;
                return PyErr_SetFromErrno(PyExc_OSError);
            }
        } else {
            int saved = errno;
            close(fd);
            errno = saved;
            return PyErr_SetFromErrno(PyExc_OSError);
        }
    }
#endif

    pygo_set_nodelay(fd, family);

    result = PygoTCPConn_alloc(cls);
    if (result == NULL) {
        pygo_closesock((PYGO_SOCK_T)fd);
        return NULL;
    }
    result->fd = fd;
    result->family = family;
    result->is_listener = 0;
    result->closed = 0;
    return (PyObject *)result;
}

/* ============================================================
 * setsockopt / getsockopt (light surface)
 * ============================================================ */
static PyObject *PygoTCPConn_setsockopt(PygoTCPConn *self, PyObject *args)
{
    int level, optname;
    Py_buffer val;
    int rc;
    if (!PyArg_ParseTuple(args, "iiy*", &level, &optname, &val)) return NULL;
#if defined(PYGO_OS_WINDOWS)
    rc = setsockopt((SOCKET)self->fd, level, optname,
                    (const char *)val.buf, (int)val.len);
#else
    rc = setsockopt(self->fd, level, optname, val.buf, (socklen_t)val.len);
#endif
    PyBuffer_Release(&val);
    if (rc != 0) return pygo_raise_errno();
    Py_RETURN_NONE;
}

/* ============================================================
 * Method / type table
 * ============================================================ */
static PyMethodDef PygoTCPConn_methods[] = {
    {"recv",       (PyCFunction)PygoTCPConn_recv,       METH_VARARGS,
     "recv(n, flags=0) -> bytes.  Cooperative recv that allocates a "
     "bytes object of length <= n.  b'' on orderly shutdown."},
    {"recv_into",  (PyCFunction)PygoTCPConn_recv_into,  METH_VARARGS,
     "recv_into(buf, n=0, flags=0) -> int.  Recv directly into a "
     "writable buffer; no bytes-object allocation per call."},
    {"send",       (PyCFunction)PygoTCPConn_send,       METH_VARARGS,
     "send(data, flags=0) -> int.  Single send syscall; may return "
     "fewer bytes than len(data).  Parks on EAGAIN."},
    {"send_all",   (PyCFunction)PygoTCPConn_send_all,   METH_VARARGS,
     "send_all(data, flags=0) -> int.  Loop until all bytes sent."},
    {"accept",     (PyCFunction)PygoTCPConn_accept,     METH_NOARGS,
     "accept() -> TCPConn.  Park on EAGAIN until a new connection "
     "arrives; return a new TCPConn wrapping the accepted fd."},
    {"close",      (PyCFunction)PygoTCPConn_close,      METH_NOARGS,
     "close().  Idempotent."},
    {"fileno",     (PyCFunction)PygoTCPConn_fileno,     METH_NOARGS,
     "fileno() -> int.  Underlying socket fd, or -1 if closed."},
    {"setsockopt", (PyCFunction)PygoTCPConn_setsockopt, METH_VARARGS,
     "setsockopt(level, optname, value).  Same args as socket."},
    {"connect",    (PyCFunction)PygoTCPConn_connect_cls, METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.connect(host, port) -> TCPConn.  TCP/IPv4-or-v6 "
     "non-blocking connect; cooperatively waits for completion."},
    {"listen",     (PyCFunction)PygoTCPConn_listen_cls,  METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.listen(host, port, backlog=128) -> TCPConn.  Bind + "
     "listen on the given address; returns a listener that accept() "
     "can be called on."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoTCPConn_getset[] = {
    {"closed", (getter)PygoTCPConn_is_closed, NULL,
     "True after close().", NULL},
    {"family", (getter)PygoTCPConn_family_get, NULL,
     "AF_INET / AF_INET6 / etc.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoTCPConnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core.TCPConn",
    .tp_basicsize = sizeof(PygoTCPConn),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "Thin C-side TCP connection that bypasses socket.socket.",
    .tp_new = PygoTCPConn_new,
    .tp_init = (initproc)PygoTCPConn_init,
    .tp_dealloc = (destructor)PygoTCPConn_dealloc,
    .tp_methods = PygoTCPConn_methods,
    .tp_getset  = PygoTCPConn_getset,
};

int pygo_tcpconn_register(PyObject *module)
{
    if (PyType_Ready(&PygoTCPConnType) < 0) return -1;
    Py_INCREF(&PygoTCPConnType);
    if (PyModule_AddObject(module, "TCPConn",
                           (PyObject *)&PygoTCPConnType) < 0) {
        Py_DECREF(&PygoTCPConnType);
        return -1;
    }
    return 0;
}
