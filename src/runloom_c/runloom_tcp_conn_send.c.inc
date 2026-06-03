/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


static PyObject *RunloomTCPConn_send(RunloomTCPConn *self, PyObject *args)
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
    if (runloom_tcpconn_use_iouring(self)) {
        runloom_iouring_ssize_t r = runloom_iouring_send(fd, buf.buf,
                                                  (size_t)buf.len, flags);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif

    while (1) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf, (int)buf.len, flags);
        if (r >= 0) { sent = r; break; }
        if (!runloom_is_wouldblock() && !runloom_is_intr()) {
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        }
#else
        ssize_t r = send(fd, (const char *)buf.buf, (size_t)buf.len, flags);
        if (r >= 0) { sent = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}

static PyObject *RunloomTCPConn_send_all(RunloomTCPConn *self, PyObject *args)
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
    use_iouring = runloom_tcpconn_use_iouring(self);
    if (use_iouring) {
        /* IORING_OP_SEND already handles short writes inside the
         * kernel for stream sockets -- the kernel buffers what it
         * can.  We still loop here because a single SEND op returns
         * what was accepted; if buf.len > SO_SNDBUF the kernel may
         * return partial and we must resubmit the remainder. */
        while (sent < buf.len) {
            runloom_iouring_ssize_t r =
                runloom_iouring_send(fd, (const char *)buf.buf + sent,
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
#if defined(RUNLOOM_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf + sent,
                     (int)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (!runloom_is_wouldblock() && !runloom_is_intr()) {
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        }
#else
        int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_SEND);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : send(fd, (const char *)buf.buf + sent,
                                  (size_t)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}
