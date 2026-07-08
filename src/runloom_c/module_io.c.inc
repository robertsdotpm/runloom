/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state.
 *
 * runloom_c.serve(): a reusable C server scaffold.  Opens N SO_REUSEPORT
 * listeners, runs N accept-loop fibers in C, and spawns one handler
 * fiber per accepted connection -- the handler receives a TCPConn (the
 * all-C socket type, with C recv/recv_into/send_all + io_uring fast paths), so
 * the per-request I/O path is C and only the user's handler logic is Python.
 *
 * The accept loop runs as a *callable* fiber (so it has a proper tstate +
 * root frame to construct objects and spawn), and reuses the Python-level
 * TCPConn API (TCPConn.listen / .accept) because RunloomTCPConn_alloc is static
 * to runloom_tcp.c's translation unit.  Per-connection cost (accept method call
 * + partial + spawn) is paid once per connection, not per request. */

#if !defined(RUNLOOM_OS_WINDOWS)
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <unistd.h>
#  include <errno.h>
#  include <stdint.h>
#  include <fcntl.h>      /* non-Linux accept(): O_NONBLOCK via fcntl */
#  include <stdlib.h>     /* malloc/free for the custom-handler acceptor arg */
#endif

#include "runloom_tcp_capi.h"   /* RUNLOOM_C_HANDLER_CAPSULE_NAME + raw-fd capi */

/* Lazily-resolved helpers (set on first serve()). */
static PyObject *runloom_io_partial      = NULL;   /* functools.partial */
static PyObject *runloom_io_tcpconn_type = NULL;   /* runloom_c.TCPConn */
static PyObject *runloom_io_acceptor_fn  = NULL;   /* PyCFunction wrapping the accept loop */
static PyObject *runloom_io_handler_wrap = NULL;   /* _serve_handler_wrap(handler, conn) */

/* Default handler-fiber stack: user request handlers can recurse deep into
 * C (TLS, parsers), like the aio bridge's I/O fibers -- pin 512 KB
 * (virtual, pooled) rather than risk the grow-down shrinking it. */
#define RUNLOOM_SERVE_HANDLER_STACK (512u * 1024u)

/* The accept loop, run as one fiber per listener.  args = (listener,
 * handler).  Loops: accept a connection (cooperative, C inside TCPConn.accept),
 * then spawn a handler fiber running partial(handler, conn).  Exits cleanly
 * when the listener is closed (accept raises) -- that is how serve() is
 * stopped.  Never propagates a per-connection error out of the loop. */
static PyObject *m_serve_acceptor(PyObject *self, PyObject *args)
{
    PyObject *listener, *handler;
    (void)self;
    if (!PyArg_ParseTuple(args, "OO", &listener, &handler)) return NULL;

    for (;;) {
        PyObject *conn, *part, *h;
        conn = PyObject_CallMethod(listener, "accept", NULL);
        if (conn == NULL) {
            /* Listener closed (serve stopped) or a hard accept error: end the
             * loop quietly -- the run is winding down. */
            PyErr_Clear();
            break;
        }
        /* Spawn the handler fiber: partial(_serve_handler_wrap, handler, conn)().
         * The wrap ALWAYS closes conn at handler completion (normal OR raise),
         * in the goroutine's own context.  Without it, a handler that raises
         * before conn.close() strands its peer: serve() issues no close on the
         * error path, so the accepted fd is closed only when the completed
         * DETACHED handler goroutine is reaped -- which for a detached g happens
         * lazily (when the hub next runs another g) or at teardown, NOT at its
         * own completion.  So the tail handler of a quiet burst is never reaped,
         * its conn never released, and the peer hangs on a half-open socket.
         * close() is idempotent, so a handler that already closes pays nothing.
         * partial holds its own refs; we drop ours. */
        part = PyObject_CallFunctionObjArgs(runloom_io_partial,
                                            runloom_io_handler_wrap, handler,
                                            conn, NULL);
        Py_DECREF(conn);
        if (part == NULL) { PyErr_Clear(); continue; }
        h = runloom_mn_fiber(part, RUNLOOM_SERVE_HANDLER_STACK);
        Py_DECREF(part);
        if (h == NULL) PyErr_Clear();
        else Py_DECREF(h);
    }
    Py_RETURN_NONE;
}

static PyMethodDef runloom_serve_acceptor_def = {
    "_serve_acceptor", m_serve_acceptor, METH_VARARGS,
    "internal: per-listener accept loop"
};

/* Resolve functools.partial, the TCPConn type, and build the acceptor
 * PyCFunction.  module is the runloom_c module object (m_serve's self). */
static int runloom_io_lazy_init(PyObject *module)
{
    if (runloom_io_partial == NULL) {
        PyObject *ft = PyImport_ImportModule("functools");
        if (ft == NULL) return -1;
        runloom_io_partial = PyObject_GetAttrString(ft, "partial");
        Py_DECREF(ft);
        if (runloom_io_partial == NULL) return -1;
    }
    if (runloom_io_tcpconn_type == NULL) {
        runloom_io_tcpconn_type = PyObject_GetAttrString(module, "TCPConn");
        if (runloom_io_tcpconn_type == NULL) return -1;
    }
    if (runloom_io_acceptor_fn == NULL) {
        runloom_io_acceptor_fn =
            PyCFunction_NewEx(&runloom_serve_acceptor_def, module, NULL);
        if (runloom_io_acceptor_fn == NULL) return -1;
    }
    if (runloom_io_handler_wrap == NULL) {
        /* Build _serve_handler_wrap(handler, conn) once: run the handler, then
         * ALWAYS close conn (normal exit OR exception) so a raising handler
         * cannot strand its peer -- see the spawn site in m_serve_acceptor. */
        PyObject *ns = PyDict_New();
        if (ns == NULL) return -1;
        PyObject *bi = PyEval_GetBuiltins();               /* borrowed */
        if (bi != NULL) PyDict_SetItemString(ns, "__builtins__", bi);
        PyObject *r = PyRun_String(
            "def _serve_handler_wrap(handler, conn):\n"
            "    try:\n"
            "        handler(conn)\n"
            "    finally:\n"
            "        try:\n"
            "            conn.close()\n"
            "        except OSError:\n"
            "            pass\n",
            Py_file_input, ns, ns);
        Py_XDECREF(r);
        if (r == NULL) { Py_DECREF(ns); return -1; }
        runloom_io_handler_wrap = PyDict_GetItemString(ns, "_serve_handler_wrap");
        Py_XINCREF(runloom_io_handler_wrap);               /* borrowed -> own */
        Py_DECREF(ns);
        if (runloom_io_handler_wrap == NULL) return -1;
    }
    return 0;
}

/* Bound port of a TCPConn listener via fileno() + getsockname(). */
static int runloom_io_listener_port(PyObject *listener)
{
#if !defined(RUNLOOM_OS_WINDOWS)
    PyObject *fo = PyObject_CallMethod(listener, "fileno", NULL);
    int fd, port = -1;
    struct sockaddr_storage ss;
    socklen_t sl = sizeof ss;
    if (fo == NULL) return -1;
    fd = (int)PyLong_AsLong(fo);
    Py_DECREF(fo);
    if (fd < 0) return -1;
    if (getsockname(fd, (struct sockaddr *)&ss, &sl) != 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (ss.ss_family == AF_INET)
        port = ntohs(((struct sockaddr_in *)&ss)->sin_port);
    else if (ss.ss_family == AF_INET6)
        port = ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return port;
#else
    (void)listener;
    return -1;
#endif
}

/* ---- All-C echo path (handler=None): tstate-free C fibers ----
 * serve(..., handler=None) runs each connection entirely in C, spawned via
 * runloom_mn_fiber_c -- a C-entry fiber carries NO Python tstate and NO root
 * frame, so the per-swap PyThreadState save/restore that a Python-callable
 * fiber pays on every park/wake is SKIPPED.  Pure C recv/send loop,
 * cooperative via runloom_netpoll_wait_fd (same pattern module_tcp / TCPConn
 * already use).  This is the "written + integrated the right way" fast path:
 * zero PyObjects in the hot loop, zero tstate juggle per round-trip. */
#if !defined(RUNLOOM_OS_WINDOWS)
/* Close a connection fd from the all-C path, clearing its netpoll arm cache
 * first.  The readiness fallback parks via runloom_netpoll_wait_fd, which leaves
 * a process-global per-fd LEVEL arm; a bare close() does NOT clear it, so when
 * the OS reuses this fd NUMBER for the next connection, netpoll_register's
 * already-armed skip sees the stale mask and never re-ADDs it -> the new
 * fiber's wait_fd parks forever (the all-C echo deadlock under concurrent
 * connect/close churn).  release_if_idle is a no-op if anything is still parked
 * and harmless for the io_uring paths (the fd was never netpoll-registered). */
static void runloom_io_c_close(int fd)
{
    runloom_netpoll_release_if_idle(fd);
    close(fd);
}

static void runloom_io_c_echo(void *arg)
{
    int fd = (int)(intptr_t)arg;
    char buf[64];
    /* Under the io_uring-as-loop backend, drive recv/send as proactor ring ops
     * (IORING_OP_RECV/SEND on the owning hub's ring): the data op runs in the
     * kernel and the fiber parks until the CQE arrives, drained inline by the
     * hub's loop_wait -- no readiness round trip, no epoll bridge.  Off the loop
     * backend (or with no hub ring), fall back to the readiness path (raw
     * recv/send + cooperative wait_fd). */
    runloom_iouring_ring_t *ring = runloom_iouring_loop_enabled()
                                       ? runloom_mn_current_iouring_ring()
                                       : NULL;
    /* Stage-3: one persistent multishot recv per connection on the hub ring
     * (RUNLOOM_IOURING_MS=1).  Falls back to single-shot proactor recv when the
     * pool isn't available (ms==NULL).  Send stays single-shot proactor. */
    void *ms = (ring != NULL && runloom_iouring_loop_ms_enabled())
                   ? runloom_iouring_loop_ms_open(ring, fd)
                   : NULL;
    for (;;) {
        ssize_t n;
        if (ms != NULL) {
            n = runloom_iouring_loop_ms_recv(ms, buf, 8);
            if (n < 0) { runloom_iouring_loop_ms_close(ms); runloom_io_c_close(fd); return; }
        } else if (ring != NULL) {
            n = runloom_iouring_loop_recv(ring, fd, buf, 8, 0);
            if (n < 0) { runloom_io_c_close(fd); return; }    /* errno set; hard error */
        } else {
            for (;;) {
                n = recv(fd, buf, 8, 0);
                RUNLOOM_IO_NOTE(RUNLOOM_IO_RECV,
                                runloom_io_classify(RUNLOOM_IO_RECV, (long long)n, errno));
                if (n >= 0) break;
                if (errno == EINTR) continue;
                if (errno != EAGAIN && errno != EWOULDBLOCK) { runloom_io_c_close(fd); return; }
                if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_READ, -1LL) < 0) { runloom_io_c_close(fd); return; }
            }
        }
        if (n == 0) {                           /* peer closed */
            if (ms != NULL) runloom_iouring_loop_ms_close(ms);
            runloom_io_c_close(fd); return;
        }
        {
            ssize_t sent = 0;
            while (sent < n) {
                ssize_t s;
                if (ring != NULL) {
                    s = runloom_iouring_loop_send(ring, fd, buf + sent,
                                                  (size_t)(n - sent), 0);
                    if (s > 0) { sent += s; continue; }
                    if (ms != NULL) runloom_iouring_loop_ms_close(ms);
                    runloom_io_c_close(fd); return;  /* EOF/error on a proactor send */
                }
                s = send(fd, buf + sent, (size_t)(n - sent), 0);
                RUNLOOM_IO_NOTE(RUNLOOM_IO_SEND,
                                runloom_io_classify(RUNLOOM_IO_SEND, (long long)s, errno));
                if (s > 0) { sent += s; continue; }
                if (s < 0 && errno == EINTR) continue;
                if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) { runloom_io_c_close(fd); return; }
                    continue;
                }
                runloom_io_c_close(fd); return;  /* hard error */
            }
        }
    }
}

static void runloom_io_c_acceptor(void *arg)
{
    int lfd = (int)(intptr_t)arg;
    for (;;) {
#if defined(__linux__)
        int cfd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK);
#else
        /* BSD/macOS have no accept4(): plain accept() + set non-blocking
         * explicitly (matches runloom_tcp_conn_net's accept path). */
        int cfd = accept(lfd, NULL, NULL);
#endif
        RUNLOOM_IO_NOTE(RUNLOOM_IO_ACCEPT,
                        runloom_io_classify(RUNLOOM_IO_ACCEPT, (long long)cfd, errno));
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (runloom_netpoll_wait_fd_coop(lfd, RUNLOOM_NETPOLL_READ, -1LL) < 0) return;
                continue;
            }
            return;                              /* listener closed / hard error */
        }
#if !defined(__linux__)
        {
            int fl = fcntl(cfd, F_GETFL, 0);
            if (fl >= 0) (void)fcntl(cfd, F_SETFL, fl | O_NONBLOCK);
        }
#endif
        {
            int on = 1;
            (void)setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
        }
        if (runloom_mn_fiber_c(runloom_io_c_echo, (void *)(intptr_t)cfd) < 0)
            close(cfd);
    }
}

/* ---- Custom C handler path: serve(handler=<runloom_c.c_handler PyCapsule>) ----
 * Identical accept loop to runloom_io_c_acceptor, but spawns a USER C function
 * per connection instead of the built-in echo -- still a tstate-free c_entry
 * fiber (runloom_mn_fiber_c), so a Cython cdef handler gets the all-C echo's fast
 * path (no Python frame, no per-park tstate save/restore) with custom logic.
 * arg = the malloc'd {listen fd, handler}, freed when the listener closes. */
typedef struct {
    int lfd;
    void (*handler)(void *arg);   /* arg = accepted fd (via intptr_t) */
} runloom_io_c_handler_arg_t;

static void runloom_io_c_handler_acceptor(void *arg)
{
    runloom_io_c_handler_arg_t *a = (runloom_io_c_handler_arg_t *)arg;
    int lfd = a->lfd;
    void (*handler)(void *) = a->handler;
    for (;;) {
#if defined(__linux__)
        int cfd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK);
#else
        int cfd = accept(lfd, NULL, NULL);
#endif
        RUNLOOM_IO_NOTE(RUNLOOM_IO_ACCEPT,
                        runloom_io_classify(RUNLOOM_IO_ACCEPT, (long long)cfd, errno));
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (runloom_netpoll_wait_fd_coop(lfd, RUNLOOM_NETPOLL_READ, -1LL) < 0) { free(a); return; }
                continue;
            }
            free(a); return;                     /* listener closed / hard error */
        }
#if !defined(__linux__)
        { int fl = fcntl(cfd, F_GETFL, 0); if (fl >= 0) (void)fcntl(cfd, F_SETFL, fl | O_NONBLOCK); }
#endif
        { int on = 1; (void)setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)); }
        if (runloom_mn_fiber_c(handler, (void *)(intptr_t)cfd) < 0)
            close(cfd);
    }
}

/* Bound listen fd of a TCPConn listener via fileno(). */
static int runloom_io_listener_fd(PyObject *listener)
{
    PyObject *fo = PyObject_CallMethod(listener, "fileno", NULL);
    int fd;
    if (fo == NULL) return -1;
    fd = (int)PyLong_AsLong(fo);
    Py_DECREF(fo);
    return fd;
}
#endif

/* runloom_c.serve(host, port, handler, acceptors=1, backlog=128)
 *     -> (bound_port, [listeners])
 *
 * Spawns `acceptors` accept-loop fibers over SO_REUSEPORT listeners on one
 * port (port=0 picks one; the rest bind to it).  Each accepted connection is
 * handed to a fresh fiber running handler(conn), conn being a TCPConn.  To
 * stop the server, close the returned listeners (the accept loops then exit).
 * Must be called from inside the M:N runtime (runloom.run / mn_run). */
static PyObject *m_serve(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"host", "port", "handler",
                             "acceptors", "backlog", NULL};
    const char *host;
    int port, acceptors = 1, backlog = 128;
    PyObject *handler, *listeners, *ret;
    int i, bound_port;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "siO|ii", kwlist,
                                     &host, &port, &handler,
                                     &acceptors, &backlog))
        return NULL;
    if (handler != Py_None && !PyCallable_Check(handler)
#if !defined(RUNLOOM_OS_WINDOWS)
        && !PyCapsule_IsValid(handler, RUNLOOM_C_HANDLER_CAPSULE_NAME)
#endif
        ) {
        PyErr_SetString(PyExc_TypeError,
                        "serve(): handler must be callable, None for the built-in all-C "
                        "echo, or a runloom_c.c_handler PyCapsule (custom C handler)");
        return NULL;
    }
    /* serve() hosts each accept loop on an M:N hub (runloom_mn_fiber / mn_go_c).
     * Single-thread mode -- runloom.run(1, ...) -- has no hubs (hub_count==0),
     * so the spawns fail with a cryptic "mn_init must be called first".  Fail
     * fast with an actionable message instead, before any listener is bound. */
    if (runloom_mn_hub_count() < 1) {
        PyErr_SetString(PyExc_RuntimeError,
                        "serve(): requires the M:N runtime -- call runloom.run(n, ...) "
                        "with n >= 2; single-thread mode has no hubs to host the accept loops");
        return NULL;
    }
    if (acceptors < 1) acceptors = 1;
    if (runloom_io_lazy_init(self) < 0) return NULL;

    listeners = PyList_New(0);
    if (listeners == NULL) return NULL;

    bound_port = port;
    for (i = 0; i < acceptors; i++) {
        PyObject *ln, *part, *h;
        int rp = (acceptors > 1) ? 1 : 0;
        /* TCPConn.listen(host, <port>, backlog, reuseport).  First listener
         * fixes the port when port==0; the rest reuse it via SO_REUSEPORT. */
        ln = PyObject_CallMethod(runloom_io_tcpconn_type, "listen",
                                 "siii", host, bound_port, backlog, rp);
        if (ln == NULL) { Py_DECREF(listeners); return NULL; }
        if (i == 0 && port == 0) {
            bound_port = runloom_io_listener_port(ln);
            if (bound_port < 0) { Py_DECREF(ln); Py_DECREF(listeners); return NULL; }
        }
        if (PyList_Append(listeners, ln) < 0) {
            Py_DECREF(ln); Py_DECREF(listeners); return NULL;
        }
#if !defined(RUNLOOM_OS_WINDOWS)
        if (PyCapsule_IsValid(handler, RUNLOOM_C_HANDLER_CAPSULE_NAME)) {
            /* Custom C handler: spawn the user's C function per connection as a
             * tstate-free c_entry fiber (the all-C echo's fast path + custom
             * logic).  The listener stays alive in `listeners`; drop the loop ref. */
            void (*h)(void *) = (void (*)(void *))PyCapsule_GetPointer(
                handler, RUNLOOM_C_HANDLER_CAPSULE_NAME);
            int lfd = runloom_io_listener_fd(ln);
            runloom_io_c_handler_arg_t *ha;
            Py_DECREF(ln);
            if (lfd < 0 || h == NULL) { Py_DECREF(listeners); return NULL; }
            ha = (runloom_io_c_handler_arg_t *)malloc(sizeof(*ha));
            if (ha == NULL) { Py_DECREF(listeners); return PyErr_NoMemory(); }
            ha->lfd = lfd; ha->handler = h;
            if (runloom_mn_fiber_c(runloom_io_c_handler_acceptor, ha) < 0) {
                free(ha); Py_DECREF(listeners);
                PyErr_SetString(PyExc_RuntimeError, "serve(): mn_fiber_c failed");
                return NULL;
            }
            continue;
        }
        if (handler == Py_None) {
            /* All-C path: tstate-free C accept loop (mn_go_c) on the raw listen
             * fd; it spawns a C echo fiber per connection.  The listener
             * object stays alive in `listeners` (returned to Python). */
            int lfd = runloom_io_listener_fd(ln);
            Py_DECREF(ln);
            if (lfd < 0) { Py_DECREF(listeners); return NULL; }
            if (runloom_mn_fiber_c(runloom_io_c_acceptor, (void *)(intptr_t)lfd) < 0) {
                Py_DECREF(listeners);
                PyErr_SetString(PyExc_RuntimeError, "serve(): mn_fiber_c failed");
                return NULL;
            }
            continue;
        }
#endif
        /* Spawn the accept loop: partial(_serve_acceptor, ln, handler)(). */
        part = PyObject_CallFunctionObjArgs(runloom_io_partial,
                                            runloom_io_acceptor_fn,
                                            ln, handler, NULL);
        Py_DECREF(ln);
        if (part == NULL) { Py_DECREF(listeners); return NULL; }
        h = runloom_mn_fiber(part, 0);
        Py_DECREF(part);
        if (h == NULL) { Py_DECREF(listeners); return NULL; }
        Py_DECREF(h);
    }

    ret = Py_BuildValue("(iN)", bound_port, listeners);   /* steals listeners */
    return ret;
}
