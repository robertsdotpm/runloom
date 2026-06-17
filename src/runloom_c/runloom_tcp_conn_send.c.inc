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
        /* Classify through the total I/O FSM; send emits only PROGRESS (>=0,
         * possibly partial) / WOULDBLOCK_W / INTR / ERROR. */
#if defined(RUNLOOM_OS_WINDOWS)
        long long r = (long long)send((SOCKET)fd, (const char *)buf.buf,
                                      (int)buf.len, flags);
#else
        long long r = (long long)send(fd, (const char *)buf.buf,
                                      (size_t)buf.len, flags);
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_SEND, r, runloom_sock_errno())) {
        case RUNLOOM_IO_PROGRESS:
            sent = (Py_ssize_t)r;
            goto send_done;
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_INTR:
            break;                      /* not writable -> park on WRITE below */
        case RUNLOOM_IO_ERROR:
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        case RUNLOOM_IO_READY:          /* events send cannot emit */
        case RUNLOOM_IO_EOF:
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            runloom_fsm_violation("runloom_io.send", RUNLOOM_IO_SEND, -1,
                                  __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
send_done:
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
        /* Same total-FSM classification; PROGRESS advances `sent` and the loop
         * re-checks sent < buf.len (a `continue` from inside the switch targets
         * the enclosing while, not the switch). */
#if defined(RUNLOOM_OS_WINDOWS)
        long long r = (long long)send((SOCKET)fd, (const char *)buf.buf + sent,
                                      (int)(buf.len - sent), flags);
#else
        int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_SEND);
        long long r = injerr ? (errno = injerr, -1LL)
                             : (long long)send(fd, (const char *)buf.buf + sent,
                                               (size_t)(buf.len - sent), flags);
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_SEND, r, runloom_sock_errno())) {
        case RUNLOOM_IO_PROGRESS:
            sent += (Py_ssize_t)r;
            continue;                   /* loop re-checks sent < buf.len */
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_INTR:
            break;                      /* not writable -> park on WRITE below */
        case RUNLOOM_IO_ERROR:
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        case RUNLOOM_IO_READY:          /* events send cannot emit */
        case RUNLOOM_IO_EOF:
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            runloom_fsm_violation("runloom_io.send_all", RUNLOOM_IO_SEND, -1,
                                  __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}
