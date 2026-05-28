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

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(__linux__)
/* Opt-in: PYGO_TCPCONN_IOURING=1 routes TCPConn's recv/send through
 * the io_uring backend instead of recv()/send()+epoll-wait.  Default
 * off because single-shot SQE-per-op is currently a net regression
 * versus the ET register-once path (one io_uring_enter per RT plus
 * eventfd routing through the pump beats the kernel-level cost of
 * non-blocking recv() + epoll_wait).  Flipping the default is
 * deferred until multishot recv (step 4) + DEFER_TASKRUN (step 5)
 * land and the combined path beats the legacy one.
 *
 * Probed once on first read; cache the answer to avoid getenv() in
 * the hot path. */
static int pygo_tcpconn_iouring_enabled = -1;
static int pygo_tcpconn_use_iouring(void)
{
    if (pygo_tcpconn_iouring_enabled < 0) {
        const char *e = getenv("PYGO_TCPCONN_IOURING");
        pygo_tcpconn_iouring_enabled =
            (e != NULL && e[0] == '1') ? 1 : 0;
    }
    return pygo_tcpconn_iouring_enabled && pygo_iouring_available();
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
 * Type object
 * ============================================================ */
typedef struct {
    PyObject_HEAD
    int fd;          /* underlying socket fd; -1 if closed */
    int family;      /* AF_INET / AF_INET6 / etc */
    int is_listener; /* True after listen() succeeds */
    int closed;
} PygoTCPConn;

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
static int pygo_resolve(const char *host, int port, int want_passive,
                        struct sockaddr_storage *storage,
                        socklen_t *addrlen, int *family)
{
    struct addrinfo hints, *res = NULL, *p;
    char portbuf[16];
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (want_passive) hints.ai_flags |= AI_PASSIVE;
    /* AI_NUMERICSERV: we always pass a numeric port. */
    hints.ai_flags |= AI_NUMERICSERV;

    snprintf(portbuf, sizeof(portbuf), "%d", port);

    Py_BEGIN_ALLOW_THREADS
    rc = getaddrinfo(host, portbuf, &hints, &res);
    Py_END_ALLOW_THREADS
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

static PyObject *PygoTCPConn_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PygoTCPConn *self = (PygoTCPConn *)type->tp_alloc(type, 0);
    if (self == NULL) return NULL;
    self->fd = -1;
    self->family = 0;
    self->is_listener = 0;
    self->closed = 0;
    return (PyObject *)self;
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
        pygo_netpoll_unregister(self->fd);
        pygo_closesock((PYGO_SOCK_T)self->fd);
        self->fd = -1;
        self->closed = 1;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoTCPConn_close(PygoTCPConn *self, PyObject *unused)
{
    (void)unused;
    if (!self->closed && self->fd >= 0) {
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
    use_iouring = pygo_tcpconn_use_iouring();
    if (use_iouring) {
        pygo_iouring_ssize_t r = pygo_iouring_recv(fd, out, (size_t)n_bytes, flags);
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
        ssize_t r = recv(fd, out, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_READ, -1LL) < 0) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
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
    if (pygo_tcpconn_use_iouring()) {
        /* Single-shot IORING_OP_RECV: kernel waits for data, posts CQE
         * when ready.  Replaces the recv()-EAGAIN-then-park loop with
         * one submit + park cycle. */
        pygo_iouring_ssize_t r = pygo_iouring_recv(fd, buf.buf,
                                                  (size_t)n_bytes, flags);
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
            return PyErr_SetFromErrno(PyExc_OSError);
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
    if (pygo_tcpconn_use_iouring()) {
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
            return PyErr_SetFromErrno(PyExc_OSError);
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
    use_iouring = pygo_tcpconn_use_iouring();
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
        ssize_t r = send(fd, (const char *)buf.buf + sent,
                         (size_t)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
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
    fd = socket(family, SOCK_STREAM, IPPROTO_TCP);
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

    result = (PygoTCPConn *)cls->tp_alloc(cls, 0);
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
        new_fd = accept(self->fd, NULL, NULL);
        if (new_fd >= 0) break;
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(self->fd, PYGO_NETPOLL_READ, -1LL) < 0) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    pygo_set_nonblock(new_fd);
    pygo_set_nodelay(new_fd, self->family);

    result = (PygoTCPConn *)Py_TYPE(self)->tp_alloc(Py_TYPE(self), 0);
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
    fd = socket(family, SOCK_STREAM, IPPROTO_TCP);
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
                return PyErr_SetFromErrno(PyExc_OSError);
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
    if (rc < 0) {
        if (errno == EINPROGRESS || errno == EAGAIN) {
            if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
                int saved = errno;
                close(fd);
                errno = saved;
                return PyErr_SetFromErrno(PyExc_OSError);
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

    result = (PygoTCPConn *)cls->tp_alloc(cls, 0);
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
