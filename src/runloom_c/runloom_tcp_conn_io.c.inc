/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ------------------------------------------------------------------
 * Per-conn critical section.
 *
 * RunloomTCPConn's fields (closed/fd/ms) are plain ints/pointers with no
 * per-object lock, but the type is exposed to free-threaded Python where two
 * threads/hubs may call methods on one conn concurrently.  We serialize the
 * check-then-act sequences (close, the lazy multishot-handle open, and the
 * read+park loop) with the object's built-in per-object critical section
 * (ob_mutex).  Crucially the scheduler SUSPENDS a held critical section at a
 * cooperative park (runloom_critsec_suspend, called from the pystate snap on
 * every fiber swap) and re-acquires it on resume, so we may hold it ACROSS a
 * netpoll park without stranding the mutex -- that is what lets close() serialize
 * AFTER a reader's parker is linked (netpoll_wait_fd links before it yields),
 * closing the close-races-a-fiber-entering-recv lost wakeup.
 *
 * Gate on the SAME condition as that suspend/restore support
 * (RUNLOOM_CRITSEC_HAVE in runloom_iframe.c): free-threaded 3.13+ with the
 * iframe TU present.  Elsewhere the GIL already serializes Python-level calls
 * (a close() has no yield point between its check and closesock, and a reader
 * cannot be preempted mid-C between EAGAIN and its park), so a no-op is correct
 * -- and it avoids holding a critical section across a park on a build that
 * would NOT suspend it. */
#if PY_VERSION_HEX >= 0x030D0000 && defined(Py_GIL_DISABLED) && \
    !defined(RUNLOOM_NO_IFRAME)
#  define RUNLOOM_TCPCONN_LOCK_DECL   PyCriticalSection _conn_cs
#  define RUNLOOM_TCPCONN_LOCK(self)  PyCriticalSection_Begin(&_conn_cs, (PyObject *)(self))
#  define RUNLOOM_TCPCONN_UNLOCK()    PyCriticalSection_End(&_conn_cs)
#else
#  define RUNLOOM_TCPCONN_LOCK_DECL   ((void)0)
#  define RUNLOOM_TCPCONN_LOCK(self)  ((void)0)
#  define RUNLOOM_TCPCONN_UNLOCK()    ((void)0)
#endif

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
        /* dealloc runs only when the last reference drops -- a parked recv/send
         * pins a reference, so no method can be in flight and no lock is needed.
         * Still INVALIDATE (fd=-1, closed=1) before the syscalls, mirroring
         * close(), so the ordering matches. */
        int fd = self->fd;
#if defined(__linux__)
        runloom_iouring_ms_t *ms = self->ms;
        self->ms = NULL;
#endif
        self->fd = -1;
        self->closed = 1;
#if defined(__linux__)
        if (ms != NULL) {
            runloom_iouring_ms_close(ms);
        }
#endif
        /* Wake any fiber parked in accept()/recv()/send() on this fd so it
         * sees the close and exits, instead of hanging forever on a fd that
         * will never fire again (mirrors the monkey socket close hook). */
        runloom_netpoll_cancel_fd(fd);
        runloom_netpoll_unregister(fd);
        runloom_closesock((RUNLOOM_SOCK_T)fd);
    }
#if defined(__linux__)
    __atomic_sub_fetch(&runloom_tcpconn_live_count, 1, __ATOMIC_RELEASE);
#endif
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RunloomTCPConn_close(RunloomTCPConn *self, PyObject *unused)
{
    int fd = -1;
#if defined(__linux__)
    runloom_iouring_ms_t *ms = NULL;
#endif
    RUNLOOM_TCPCONN_LOCK_DECL;
    (void)unused;

    /* Claim the close under the conn's per-object critical section and
     * INVALIDATE the fd (closed=1, fd=-1) BEFORE any syscall.  Two effects:
     *   1. Only ONE caller ever captures a live fd, so concurrent close()es
     *      (or a close racing dealloc) cannot double-close a fd number the
     *      kernel may already have re-handed to another thread.  CPython's
     *      socketmodule stores INVALID_SOCKET before closing for exactly this
     *      reason; the old code here stored -1 only AFTER closesock while still
     *      advertising closed==0 for the whole cancel/unregister/close window.
     *   2. A fiber entering recv/send re-reads self->fd/self->closed under the
     *      SAME critical section AND holds it across its netpoll park (the
     *      scheduler suspends the section only once the parker is linked), so
     *      this close() blocks here until that parker exists -- then the
     *      cancel_fd below always reaches it, instead of racing ahead and
     *      stranding a fiber that parks on the now-dead fd forever. */
    RUNLOOM_TCPCONN_LOCK(self);
    if (!self->closed && self->fd >= 0) {
        fd = self->fd;
        self->fd = -1;
        self->closed = 1;
#if defined(__linux__)
        ms = self->ms;
        self->ms = NULL;
#endif
    }
    RUNLOOM_TCPCONN_UNLOCK();

    if (fd < 0) Py_RETURN_NONE;   /* already closed by another caller */

#if defined(__linux__)
    if (ms != NULL) {
        runloom_iouring_ms_close(ms);
    }
#endif
    /* Wake any fiber parked in accept()/recv()/send() on this fd so it sees the
     * close and exits, instead of hanging forever on a fd that will never fire
     * again (mirrors the monkey socket close hook). */
    runloom_netpoll_cancel_fd(fd);
    runloom_netpoll_unregister(fd);
    runloom_closesock((RUNLOOM_SOCK_T)fd);
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

    result = PyBytes_FromStringAndSize(NULL, n_bytes);
    if (result == NULL) return NULL;
    out = PyBytes_AS_STRING(result);

#if defined(__linux__)
    use_iouring = runloom_tcpconn_use_iouring(self);
    if (use_iouring) {
        runloom_iouring_ssize_t r;
        runloom_iouring_ms_t *ms;
        int fd;
        RUNLOOM_TCPCONN_LOCK_DECL;
        /* Serialize the lazy multishot-handle open + read against a concurrent
         * recv()/close() on this conn: two hubs must not both ms_open the same
         * fd (that arms two kernel multishots, one leaked and silently eating
         * the byte stream -> permanent data loss).  The critical section is
         * suspended by the scheduler while ms_recv is parked, so it does not
         * block close() for the whole wait -- only the non-parked spans. */
        RUNLOOM_TCPCONN_LOCK(self);
        if (self->closed || self->fd < 0) {
            RUNLOOM_TCPCONN_UNLOCK();
            Py_DECREF(result);
            PyErr_SetString(PyExc_OSError, "TCPConn is closed");
            return NULL;
        }
        fd = self->fd;
        /* Multishot when available and the call carries no special MSG_* flags
         * (the multishot SQE is fire-and-forget, without per-call flags). */
        if (self->ms == NULL && flags == 0 && runloom_iouring_pbuf_available()) {
            self->ms = runloom_iouring_ms_open(fd);
        }
        ms = self->ms;
        if (ms != NULL) {
            /* Once armed the multishot OWNS the byte stream: it drains every
             * incoming byte into its private queue, so a raw single-shot recv
             * on the fd -- even a flags-bearing one (e.g. MSG_PEEK) -- would
             * find the socket empty and surface a FALSE, out-of-order EOF (b'')
             * on a live connection.  Serve ALL reads through the handle while it
             * exists.  Holding the conn critical section across ms_recv also
             * closes the reader-vs-closer UAF: ms_recv takes its own recv-ref
             * before it parks, and close()'s ms_close cannot run until this
             * fiber has parked (hence taken that ref). */
            r = runloom_iouring_ms_recv(ms, out, (size_t)n_bytes);
            if (r < 0) {
                RUNLOOM_TCPCONN_UNLOCK();
                Py_DECREF(result);
                return PyErr_SetFromErrno(PyExc_OSError);
            }
            if (r == 0 && self->closed) {
                /* Our own close() cancelled this parked recv (ms_close): match
                 * the epoll backend, which raises OSError(ECANCELED), rather
                 * than reporting a false EOF a caller would read as peer-close. */
                RUNLOOM_TCPCONN_UNLOCK();
                Py_DECREF(result);
                errno = ECANCELED;
                return PyErr_SetFromErrno(PyExc_OSError);
            }
            RUNLOOM_TCPCONN_UNLOCK();
            got = (Py_ssize_t)r;
            if (got < n_bytes) {
                if (_PyBytes_Resize(&result, got) < 0) return NULL;
            }
            return result;
        }
        RUNLOOM_TCPCONN_UNLOCK();
        /* Single-shot IORING_OP_RECV fallback (non-zero flags, or the provided-
         * buffer ring / ms_open is unavailable). */
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

    for (;;) {
        long long r;
        int local_fd;
        int wait_rc;
        RUNLOOM_TCPCONN_LOCK_DECL;
        /* Re-read fd + re-check closed under the conn critical section on EVERY
         * iteration, and hold it across the netpoll park: this fiber keeps the
         * section from here through wait_fd's parker-link (the scheduler
         * suspends it only once linked), so a concurrent close() serializes
         * AFTER the link and its cancel_fd always reaches this parker instead
         * of racing ahead and stranding it on a dead fd.  Re-reading fd also
         * stops a stale cached fd from parking on / stealing data from an
         * unrelated reused fd number. */
        RUNLOOM_TCPCONN_LOCK(self);
        if (self->closed || self->fd < 0) {
            RUNLOOM_TCPCONN_UNLOCK();
            Py_DECREF(result);
            PyErr_SetString(PyExc_OSError, "TCPConn is closed");
            return NULL;
        }
        local_fd = self->fd;
        /* Classify (rc,errno) through the single total I/O FSM; the exhaustive
         * switch makes an unhandled return pathway a build error.  recv emits
         * only READY/EOF/WOULDBLOCK_R/INTR/ERROR (the others are impossible). */
#if defined(RUNLOOM_OS_WINDOWS)
        r = (long long)recv((SOCKET)local_fd, out, (int)n_bytes, flags);
#else
        {
            int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_RECV);
            r = injerr ? (errno = injerr, -1LL)
                       : (long long)recv(local_fd, out, (size_t)n_bytes, flags);
        }
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_RECV, r, runloom_sock_errno())) {
        case RUNLOOM_IO_READY:
        case RUNLOOM_IO_EOF:
            got = (Py_ssize_t)r;             /* >0 data, or 0 = orderly EOF */
            RUNLOOM_TCPCONN_UNLOCK();
            goto recv_done;
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_INTR:
            /* Not ready -> park on READ while still holding the section (the
             * scheduler suspends it during the park). */
            wait_rc = runloom_netpoll_wait_fd_coop(local_fd, RUNLOOM_NETPOLL_READ,
                                                   -1LL);
            RUNLOOM_TCPCONN_UNLOCK();
            if (wait_rc < 0) {
                Py_DECREF(result);
                /* wait_fd may have run a Python signal handler that raised (e.g.
                 * an alarm handler / Ctrl-C) -> propagate it; don't overwrite
                 * with OSError. */
                return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
            }
            break;                           /* re-attempt above */
        case RUNLOOM_IO_ERROR:
            RUNLOOM_TCPCONN_UNLOCK();
            Py_DECREF(result);
            return runloom_raise_errno();
        case RUNLOOM_IO_PROGRESS:            /* events recv cannot emit */
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            RUNLOOM_TCPCONN_UNLOCK();
            runloom_fsm_violation("runloom_io.recv", RUNLOOM_IO_RECV, -1,
                                  __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
    }

recv_done:
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

    if (self->closed || self->fd < 0) {
        PyErr_SetString(PyExc_OSError, "TCPConn is closed");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "w*|ni", &buf, &n_bytes, &flags))
        return NULL;
    /* Match socket.recv_into's contract: a negative nbytes and an nbytes larger
     * than the buffer are ERRORS, not a silent clamp.  The old clamp turned a
     * caller relying on "raise or read up to nbytes" (e.g. a frame size against
     * an undersized scratch buffer) into a silent short read. */
    if (n_bytes < 0) {
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "negative buffersize in recv_into");
        return NULL;
    }
    if (n_bytes == 0) {
        n_bytes = buf.len;
    } else if (n_bytes > buf.len) {
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "buffer too small for requested bytes");
        return NULL;
    }
    if (n_bytes == 0) {                 /* empty buffer: nothing to read into */
        PyBuffer_Release(&buf);
        return PyLong_FromLong(0);
    }

#if defined(__linux__)
    if (runloom_tcpconn_use_iouring(self)) {
        runloom_iouring_ssize_t r;
        runloom_iouring_ms_t *ms;
        int fd;
        RUNLOOM_TCPCONN_LOCK_DECL;
        RUNLOOM_TCPCONN_LOCK(self);
        if (self->closed || self->fd < 0) {
            RUNLOOM_TCPCONN_UNLOCK();
            PyBuffer_Release(&buf);
            PyErr_SetString(PyExc_OSError, "TCPConn is closed");
            return NULL;
        }
        fd = self->fd;
        if (self->ms == NULL && flags == 0 && runloom_iouring_pbuf_available()) {
            self->ms = runloom_iouring_ms_open(fd);
        }
        ms = self->ms;
        if (ms != NULL) {
            /* Multishot owns the stream once armed -- serve every read from it
             * (see RunloomTCPConn_recv for the false-EOF + reader-vs-closer UAF
             * rationale). */
            r = runloom_iouring_ms_recv(ms, buf.buf, (size_t)n_bytes);
            if (r < 0) {
                RUNLOOM_TCPCONN_UNLOCK();
                PyBuffer_Release(&buf);
                return PyErr_SetFromErrno(PyExc_OSError);
            }
            if (r == 0 && self->closed) {
                RUNLOOM_TCPCONN_UNLOCK();
                PyBuffer_Release(&buf);
                errno = ECANCELED;
                return PyErr_SetFromErrno(PyExc_OSError);
            }
            RUNLOOM_TCPCONN_UNLOCK();
            PyBuffer_Release(&buf);
            return PyLong_FromSsize_t((Py_ssize_t)r);
        }
        RUNLOOM_TCPCONN_UNLOCK();
        /* Single-shot IORING_OP_RECV fallback (kernel < 5.19, or
         * non-zero flags). */
        r = runloom_iouring_recv(fd, buf.buf, (size_t)n_bytes, flags);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif

    for (;;) {
        long long r;
        int local_fd;
        int wait_rc;
        RUNLOOM_TCPCONN_LOCK_DECL;
        RUNLOOM_TCPCONN_LOCK(self);
        if (self->closed || self->fd < 0) {
            RUNLOOM_TCPCONN_UNLOCK();
            PyBuffer_Release(&buf);
            PyErr_SetString(PyExc_OSError, "TCPConn is closed");
            return NULL;
        }
        local_fd = self->fd;
        /* recv_into: same total-FSM classification as recv (READY/EOF success,
         * WOULDBLOCK_R/INTR park, ERROR raise).  fd re-read + close re-checked +
         * park held under the section, as in recv(). */
#if defined(RUNLOOM_OS_WINDOWS)
        r = (long long)recv((SOCKET)local_fd, (char *)buf.buf, (int)n_bytes,
                            flags);
#else
        r = (long long)recv(local_fd, (char *)buf.buf, (size_t)n_bytes, flags);
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_RECV, r, runloom_sock_errno())) {
        case RUNLOOM_IO_READY:
        case RUNLOOM_IO_EOF:
            got = (Py_ssize_t)r;
            RUNLOOM_TCPCONN_UNLOCK();
            goto recv_into_done;
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_INTR:
            wait_rc = runloom_netpoll_wait_fd_coop(local_fd, RUNLOOM_NETPOLL_READ,
                                                   -1LL);
            RUNLOOM_TCPCONN_UNLOCK();
            if (wait_rc < 0) {
                PyBuffer_Release(&buf);
                /* wait_fd may have run a Python signal handler that raised (e.g.
                 * an alarm handler / Ctrl-C) -> propagate it; don't overwrite
                 * with OSError. */
                return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
            }
            break;                           /* re-attempt above */
        case RUNLOOM_IO_ERROR:
            RUNLOOM_TCPCONN_UNLOCK();
            PyBuffer_Release(&buf);
            return runloom_raise_errno();
        case RUNLOOM_IO_PROGRESS:            /* events recv cannot emit */
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            RUNLOOM_TCPCONN_UNLOCK();
            runloom_fsm_violation("runloom_io.recv_into", RUNLOOM_IO_RECV, -1,
                                  __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
    }
recv_into_done:
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
}

#undef RUNLOOM_TCPCONN_LOCK_DECL
#undef RUNLOOM_TCPCONN_LOCK
#undef RUNLOOM_TCPCONN_UNLOCK
