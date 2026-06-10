/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state.
 *
 * runloom_c.serve(): a reusable C server scaffold.  Opens N SO_REUSEPORT
 * listeners, runs N accept-loop goroutines in C, and spawns one handler
 * goroutine per accepted connection -- the handler receives a TCPConn (the
 * all-C socket type, with C recv/recv_into/send_all + io_uring fast paths), so
 * the per-request I/O path is C and only the user's handler logic is Python.
 *
 * The accept loop runs as a *callable* goroutine (so it has a proper tstate +
 * root frame to construct objects and spawn), and reuses the Python-level
 * TCPConn API (TCPConn.listen / .accept) because RunloomTCPConn_alloc is static
 * to runloom_tcp.c's translation unit.  Per-connection cost (accept method call
 * + partial + spawn) is paid once per connection, not per request. */

#if !defined(RUNLOOM_OS_WINDOWS)
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <unistd.h>
#endif

/* Lazily-resolved helpers (set on first serve()). */
static PyObject *runloom_io_partial      = NULL;   /* functools.partial */
static PyObject *runloom_io_tcpconn_type = NULL;   /* runloom_c.TCPConn */
static PyObject *runloom_io_acceptor_fn  = NULL;   /* PyCFunction wrapping the accept loop */

/* Default handler-goroutine stack: user request handlers can recurse deep into
 * C (TLS, parsers), like the aio bridge's I/O goroutines -- pin 512 KB
 * (virtual, pooled) rather than risk the grow-down shrinking it. */
#define RUNLOOM_SERVE_HANDLER_STACK (512u * 1024u)

/* The accept loop, run as one goroutine per listener.  args = (listener,
 * handler).  Loops: accept a connection (cooperative, C inside TCPConn.accept),
 * then spawn a handler goroutine running partial(handler, conn).  Exits cleanly
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
        /* Spawn the handler goroutine: partial(handler, conn)().  partial holds
         * its own ref to conn; we drop ours. */
        part = PyObject_CallFunctionObjArgs(runloom_io_partial, handler, conn, NULL);
        Py_DECREF(conn);
        if (part == NULL) { PyErr_Clear(); continue; }
        h = runloom_mn_go(part, RUNLOOM_SERVE_HANDLER_STACK);
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

/* runloom_c.serve(host, port, handler, acceptors=1, backlog=128)
 *     -> (bound_port, [listeners])
 *
 * Spawns `acceptors` accept-loop goroutines over SO_REUSEPORT listeners on one
 * port (port=0 picks one; the rest bind to it).  Each accepted connection is
 * handed to a fresh goroutine running handler(conn), conn being a TCPConn.  To
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
    if (!PyCallable_Check(handler)) {
        PyErr_SetString(PyExc_TypeError, "serve(): handler must be callable");
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
        /* Spawn the accept loop: partial(_serve_acceptor, ln, handler)(). */
        part = PyObject_CallFunctionObjArgs(runloom_io_partial,
                                            runloom_io_acceptor_fn,
                                            ln, handler, NULL);
        Py_DECREF(ln);
        if (part == NULL) { Py_DECREF(listeners); return NULL; }
        h = runloom_mn_go(part, 0);
        Py_DECREF(part);
        if (h == NULL) { Py_DECREF(listeners); return NULL; }
        Py_DECREF(h);
    }

    ret = Py_BuildValue("(iN)", bound_port, listeners);   /* steals listeners */
    return ret;
}
