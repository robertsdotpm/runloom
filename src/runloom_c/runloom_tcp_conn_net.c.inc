/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ============================================================
 * Listener / accept
 * ============================================================ */

static PyObject *RunloomTCPConn_listen_cls(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    const char *host;
    int port;
    int backlog = 128;
    int reuseport = 0;
    static char *kwlist[] = {"host", "port", "backlog", "reuseport", NULL};
    struct sockaddr_storage addr;
    socklen_t addrlen = 0;
    int family = 0;
    int fd;
    int on = 1;
    RunloomTCPConn *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "si|ip", kwlist,
                                     &host, &port, &backlog, &reuseport))
        return NULL;

    if (runloom_resolve(host, port, /*passive*/1, &addr, &addrlen, &family) != 0)
        return NULL;

#if defined(RUNLOOM_OS_WINDOWS)
    runloom_winsock_init();
    {
        SOCKET s = socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (s == INVALID_SOCKET) return runloom_raise_errno();
        (void)setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
                         (const char *)&on, sizeof(on));
        if (bind(s, (struct sockaddr *)&addr, addrlen) == SOCKET_ERROR ||
            listen(s, backlog) == SOCKET_ERROR) {
            int saved = WSAGetLastError();
            closesocket(s);
            WSASetLastError(saved);
            return runloom_raise_errno();
        }
        fd = (int)s;
    }
#else
    {   int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_SOCKET);
        fd = injerr ? (errno = injerr, -1)
                    : socket(family, SOCK_STREAM, IPPROTO_TCP); }
    if (fd < 0) return PyErr_SetFromErrno(PyExc_OSError);
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
#if defined(SO_REUSEPORT)
    if (reuseport)
        (void)setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
#endif
    if (bind(fd, (struct sockaddr *)&addr, addrlen) < 0 ||
        listen(fd, backlog) < 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return PyErr_SetFromErrno(PyExc_OSError);
    }
#endif

    runloom_set_nonblock(fd);

    result = RunloomTCPConn_alloc(cls);
    if (result == NULL) {
        runloom_closesock((RUNLOOM_SOCK_T)fd);
        return NULL;
    }
    result->fd = fd;
    result->family = family;
    result->is_listener = 1;
    result->closed = 0;
    return (PyObject *)result;
}

#if defined(__linux__) && defined(SOCK_NONBLOCK)
static int runloom_tcp_accept4_on(void)
{
    static int v = -1;
    if (v < 0) { const char *e = getenv("RUNLOOM_TCP_ACCEPT4");
                 v = (e && e[0] == '0') ? 0 : 1; }   /* default ON */
    return v;
}
#endif

static PyObject *RunloomTCPConn_accept(RunloomTCPConn *self, PyObject *unused)
{
    RunloomTCPConn *result;
    int new_fd;
    int nb_set = 0;
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
        /* Success is detected with each platform's native idiom (a SOCKET handle
         * is not a small signed fd); the total I/O FSM then classifies a FAILURE
         * into park-and-retry (WOULDBLOCK_R / INTR / RETRY) vs raise (ERROR).
         * RETRY == ECONNABORTED: the peer reset between the SYN and our accept --
         * a transient, not a listener failure.  Go's netpoll and libuv both retry
         * it; surfacing it spuriously killed the accept loop.  Routing it through
         * the shared classifier also applies that retry on the WSA backend
         * (WSAECONNABORTED), where the old branch raised it. */
        runloom_io_event_t ev;
#if defined(RUNLOOM_OS_WINDOWS)
        SOCKET s = accept((SOCKET)self->fd, NULL, NULL);
        if (s != INVALID_SOCKET) { new_fd = (int)s; break; }
        ev = runloom_io_classify(RUNLOOM_IO_ACCEPT, -1, runloom_sock_errno());
#else
        int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_ACCEPT);
        if (injerr) { new_fd = (errno = injerr, -1); }
#if defined(__linux__) && defined(SOCK_NONBLOCK)
        /* accept4 bakes O_NONBLOCK + CLOEXEC into the accepted fd in ONE syscall,
         * removing the 2 fcntls runloom_set_nonblock did per connection (the conn/s
         * accept-path micro-win, docs/dev/conn_cpu.md).  RUNLOOM_TCP_ACCEPT4=0 reverts
         * to accept()+fcntl for an ancient kernel that lacks accept4. */
        else if (runloom_tcp_accept4_on()) {
            new_fd = accept4(self->fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (new_fd >= 0) nb_set = 1;        /* nonblock already set -> skip the fcntls */
        }
#endif
        else { new_fd = accept(self->fd, NULL, NULL); }
        if (new_fd >= 0) break;
        ev = runloom_io_classify(RUNLOOM_IO_ACCEPT, -1, errno);
#endif
        RUNLOOM_IO_SWITCH(ev) {
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_INTR:
        case RUNLOOM_IO_RETRY:
            break;                      /* park on READ + retry below */
        case RUNLOOM_IO_ERROR:
            return runloom_raise_errno();
        case RUNLOOM_IO_READY:          /* success handled above (break), not here */
        case RUNLOOM_IO_PROGRESS:       /* events accept cannot emit */
        case RUNLOOM_IO_EOF:
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_EVENT_COUNT:
            runloom_fsm_violation("runloom_io.accept", RUNLOOM_IO_ACCEPT, -1,
                                  __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
        if (runloom_netpoll_wait_fd_coop(self->fd, RUNLOOM_NETPOLL_READ, -1LL) < 0) {
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    if (!nb_set) runloom_set_nonblock(new_fd);   /* accept4 already did it on Linux */
    runloom_set_nodelay(new_fd, self->family);

    result = RunloomTCPConn_alloc(Py_TYPE(self));
    if (result == NULL) {
        runloom_closesock((RUNLOOM_SOCK_T)new_fd);
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
static PyObject *RunloomTCPConn_connect_cls(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    const char *host;
    int port;
    static char *kwlist[] = {"host", "port", NULL};
    struct sockaddr_storage addr;
    socklen_t addrlen = 0;
    int family = 0;
    int fd;
    RunloomTCPConn *result;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "si", kwlist, &host, &port))
        return NULL;
    if (runloom_resolve(host, port, /*passive*/0, &addr, &addrlen, &family) != 0)
        return NULL;

#if defined(RUNLOOM_OS_WINDOWS)
    runloom_winsock_init();
    {
        SOCKET s = socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (s == INVALID_SOCKET) return runloom_raise_errno();
        fd = (int)s;
    }
#else
    {   int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_SOCKET);
        fd = injerr ? (errno = injerr, -1)
                    : socket(family, SOCK_STREAM, IPPROTO_TCP); }
    if (fd < 0) return PyErr_SetFromErrno(PyExc_OSError);
#endif

    runloom_set_nonblock(fd);

    /* Non-blocking connect.  If the connect returns EINPROGRESS
     * (or WSAEWOULDBLOCK on Windows), park on WRITE and read
     * SO_ERROR to determine final status. */
#if defined(RUNLOOM_OS_WINDOWS)
    rc = connect((SOCKET)fd, (struct sockaddr *)&addr, addrlen);
    if (rc == SOCKET_ERROR) {
        int err = WSAGetLastError();
        /* Observational: same total I/O FSM on the WSA backend (WSAEWOULDBLOCK
         * -> WOULDBLOCK_W -> park WRITE + SO_ERROR; else ERROR). */
        RUNLOOM_IO_NOTE(RUNLOOM_IO_CONNECT,
                        runloom_io_classify(RUNLOOM_IO_CONNECT, -1, runloom_sock_errno()));
        if (err == WSAEWOULDBLOCK) {
            if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
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
                return runloom_raise_errno();
            }
        } else {
            closesocket((SOCKET)fd);
            WSASetLastError(err);
            return runloom_raise_errno();
        }
    }
#else
    rc = connect(fd, (struct sockaddr *)&addr, addrlen);
    {   /* test-only: override the result AFTER the real connect() initiates the
         * connection, so EINTR is modelled as a signal on an in-flight connect
         * (park WRITE + SO_ERROR completes it) rather than a no-op that hangs. */
        int injerr = RUNLOOM_TCP_FINJ(RUNLOOM_FAULT_TCP_CONNECT);
        if (injerr) { rc = -1; errno = injerr; }
    }
    /* Observational: route the connect result through the total I/O FSM
     * (READY | WOULDBLOCK_W | INTR | ERROR).  The one-shot SO_ERROR branch below
     * is the consumer (too intricate to restructure as a loop); the NOTE asserts
     * its understanding matches the model under -DRUNLOOM_FSM_VALIDATE.  EINTR is
     * handled like EINPROGRESS (park WRITE + SO_ERROR), NOT as an error: a
     * connect() interrupted by a signal continues asynchronously per POSIX. */
    RUNLOOM_IO_NOTE(RUNLOOM_IO_CONNECT,
                    runloom_io_classify(RUNLOOM_IO_CONNECT, rc, errno));
    if (rc < 0) {
        /* EINTR is handled like EINPROGRESS, NOT as an error: POSIX says a
         * connect() interrupted by a signal is not aborted -- the connection
         * continues asynchronously, and the caller must wait for writability
         * and check SO_ERROR (re-issuing connect() would fail EALREADY/
         * EADDRINUSE).  Without this, a signal landing on the connect() syscall
         * spuriously failed the connection with OSError(EINTR) -- confirmed by
         * fault injection (strace -e inject=connect:error=EINTR:when=1). */
        if (errno == EINPROGRESS || errno == EAGAIN || errno == EINTR) {
            if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
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

    runloom_set_nodelay(fd, family);

    result = RunloomTCPConn_alloc(cls);
    if (result == NULL) {
        runloom_closesock((RUNLOOM_SOCK_T)fd);
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
static PyObject *RunloomTCPConn_setsockopt(RunloomTCPConn *self, PyObject *args)
{
    int level, optname;
    Py_buffer val;
    int rc;
    if (!PyArg_ParseTuple(args, "iiy*", &level, &optname, &val)) return NULL;
#if defined(RUNLOOM_OS_WINDOWS)
    rc = setsockopt((SOCKET)self->fd, level, optname,
                    (const char *)val.buf, (int)val.len);
#else
    rc = setsockopt(self->fd, level, optname, val.buf, (socklen_t)val.len);
#endif
    PyBuffer_Release(&val);
    if (rc != 0) return runloom_raise_errno();
    Py_RETURN_NONE;
}
