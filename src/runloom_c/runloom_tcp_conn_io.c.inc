/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ============================================================
 * Construction / destruction
 * ============================================================ */

/* Allocate + zero-init a TCPConn from a type.  Centralises the
 * tp_alloc + ms/fd/closed defaults + live-count increment that
 * listen/accept/connect would otherwise have to duplicate. */
static RunloomTCPConn *RunloomTCPConn_alloc(PyTypeObject *type)
{
    RunloomTCPConn *self = (RunloomTCPConn *)type->tp_alloc(type, 0);
    if (self == NULL) return NULL;
    self->fd = -1;
    self->family = 0;
    self->is_listener = 0;
    self->closed = 0;
#if defined(__linux__)
    self->ms = NULL;
    self->iouring_choice = -1;
    __atomic_add_fetch(&runloom_tcpconn_live_count, 1, __ATOMIC_RELEASE);
#endif
    return self;
}

static PyObject *RunloomTCPConn_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return (PyObject *)RunloomTCPConn_alloc(type);
}

static int RunloomTCPConn_init(RunloomTCPConn *self, PyObject *args, PyObject *kwds)
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
    (void)runloom_set_nonblock(fd);
    runloom_set_nodelay(fd, self->family);
    return 0;
}

static void RunloomTCPConn_dealloc(RunloomTCPConn *self)
{
    if (!self->closed && self->fd >= 0) {
#if defined(__linux__)
        if (self->ms != NULL) {
            runloom_iouring_ms_close(self->ms);
            self->ms = NULL;
        }
#endif
        /* Wake any fiber parked in accept()/recv()/send() on this fd so it
         * sees the close and exits, instead of hanging forever on a fd that
         * will never fire again (mirrors the monkey socket close hook). */
        runloom_netpoll_cancel_fd(self->fd);
        runloom_netpoll_unregister(self->fd);
        runloom_closesock((RUNLOOM_SOCK_T)self->fd);
        self->fd = -1;
        self->closed = 1;
    }
#if defined(__linux__)
    __atomic_sub_fetch(&runloom_tcpconn_live_count, 1, __ATOMIC_RELEASE);
#endif
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomTCPConn_close(RunloomTCPConn *self, PyObject *unused)
{
    (void)unused;
    if (!self->closed && self->fd >= 0) {
#if defined(__linux__)
        if (self->ms != NULL) {
            runloom_iouring_ms_close(self->ms);
            self->ms = NULL;
        }
#endif
        /* Wake any fiber parked in accept()/recv()/send() on this fd so it
         * sees the close and exits, instead of hanging forever on a fd that
         * will never fire again (mirrors the monkey socket close hook). */
        runloom_netpoll_cancel_fd(self->fd);
        runloom_netpoll_unregister(self->fd);
        runloom_closesock((RUNLOOM_SOCK_T)self->fd);
        self->fd = -1;
        self->closed = 1;
    }
    Py_RETURN_NONE;
}

static PyObject *RunloomTCPConn_fileno(RunloomTCPConn *self, PyObject *unused)
{
    (void)unused;
    return PyLong_FromLong((long)self->fd);
}

static PyObject *RunloomTCPConn_is_closed(RunloomTCPConn *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong((long)self->closed);
}

static PyObject *RunloomTCPConn_family_get(RunloomTCPConn *self, void *closure)
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

static PyObject *RunloomTCPConn_recv(RunloomTCPConn *self, PyObject *args)
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
    use_iouring = runloom_tcpconn_use_iouring(self);
    if (use_iouring) {
        runloom_iouring_ssize_t r;
        /* Multishot when available and the call carries no special
         * MSG_* flags (multishot SQE is fire-and-forget without
         * per-call flags). */
        if (flags == 0 && runloom_iouring_pbuf_available()) {
            if (self->ms == NULL) {
                self->ms = runloom_iouring_ms_open(fd);
            }
            if (self->ms != NULL) {
                r = runloom_iouring_ms_recv(self->ms, out, (size_t)n_bytes);
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
        r = runloom_iouring_recv(fd, out, (size_t)n_bytes, flags);
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
#if defined(RUNLOOM_OS_WINDOWS)
        int r = recv((SOCKET)fd, out, (int)n_bytes, flags);
        if (r >= 0) { got = r; break; }
        if (!runloom_is_wouldblock() && !runloom_is_intr()) {
            Py_DECREF(result);
            return runloom_raise_errno();
        }
#else
        int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_RECV);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : recv(fd, out, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_READ, -1LL) < 0) {
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

static PyObject *RunloomTCPConn_recv_into(RunloomTCPConn *self, PyObject *args)
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
    if (runloom_tcpconn_use_iouring(self)) {
        runloom_iouring_ssize_t r;
        if (flags == 0 && runloom_iouring_pbuf_available()) {
            if (self->ms == NULL) {
                self->ms = runloom_iouring_ms_open(fd);
            }
            if (self->ms != NULL) {
                r = runloom_iouring_ms_recv(self->ms, buf.buf, (size_t)n_bytes);
                PyBuffer_Release(&buf);
                if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
                return PyLong_FromSsize_t((Py_ssize_t)r);
            }
        }
        /* Single-shot IORING_OP_RECV fallback (kernel < 5.19, or
         * non-zero flags). */
        r = runloom_iouring_recv(fd, buf.buf, (size_t)n_bytes, flags);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif

    while (1) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = recv((SOCKET)fd, (char *)buf.buf, (int)n_bytes, flags);
        if (r >= 0) { got = r; break; }
        if (!runloom_is_wouldblock() && !runloom_is_intr()) {
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        }
#else
        ssize_t r = recv(fd, (char *)buf.buf, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_READ, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
}
