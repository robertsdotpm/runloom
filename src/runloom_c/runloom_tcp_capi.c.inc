/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  Implements the zero-PyObject C entry points declared in
 * runloom_tcp_capi.h.  Mirrors RunloomTCPConn_recv_into / _send_all exactly
 * (same FSM classification, same io_uring branch, same netpoll park), but
 * without Py_buffer / PyArg / PyLong / PyErr -- so a Cython handler that calls
 * these has an allocation-free, PyObject-free hot loop. */

#include "runloom_tcp_capi.h"

/* recv up to n bytes into buf.  Returns bytes read (0 = EOF), -1 on error. */
Py_ssize_t runloom_tcpconn_c_recv_into(PyObject *conn, void *buf, Py_ssize_t n)
{
    RunloomTCPConn *self = (RunloomTCPConn *)conn;
    int fd;

    if (self->closed || self->fd < 0) { errno = EBADF; return -1; }
    if (n <= 0) return 0;
    fd = self->fd;

#if defined(__linux__)
    /* Stage-2 single-shot io_uring PROACTOR: when the loop backend is on, submit
     * the recv as an SQE and inline-drain -- on a data-ready socket the CQE is
     * already posted (FAST_POLL did the recv) so the fiber skips the park
     * entirely.  This is the +20%-on-loopback path the all-C echo uses; without
     * it a Cython handler on RUNLOOM_IOURING_LOOP would do a readiness recv() +
     * an io_uring-bridged park (io_uring's cost, none of its win). */
    if (runloom_iouring_loop_enabled()) {
        runloom_iouring_ring_t *ring = runloom_mn_current_iouring_ring();
        if (ring != NULL) {
            runloom_iouring_ssize_t r =
                runloom_iouring_loop_recv(ring, fd, buf, (size_t)n, 0);
            return (r < 0) ? -1 : (Py_ssize_t)r;   /* errno set by loop_recv */
        }
    }
    if (runloom_tcpconn_use_iouring(self)) {
        runloom_iouring_ssize_t r;
        if (runloom_iouring_pbuf_available()) {
            if (self->ms == NULL) self->ms = runloom_iouring_ms_open(fd);
            if (self->ms != NULL) {
                r = runloom_iouring_ms_recv(self->ms, buf, (size_t)n);
                return (r < 0) ? -1 : (Py_ssize_t)r;
            }
        }
        r = runloom_iouring_recv(fd, buf, (size_t)n, 0);
        return (r < 0) ? -1 : (Py_ssize_t)r;
    }
#endif

    for (;;) {
#if defined(RUNLOOM_OS_WINDOWS)
        long long r = (long long)recv((SOCKET)fd, (char *)buf, (int)n, 0);
#else
        long long r = (long long)recv(fd, buf, (size_t)n, 0);
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_RECV, r, runloom_sock_errno())) {
        case RUNLOOM_IO_READY:
        case RUNLOOM_IO_EOF:
            return (Py_ssize_t)r;        /* >0 data, or 0 = orderly EOF */
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_INTR:
            break;                        /* not ready -> park on READ */
        case RUNLOOM_IO_ERROR:
            return -1;                    /* errno already set by recv() */
        case RUNLOOM_IO_PROGRESS:
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            runloom_fsm_violation("runloom_tcp_capi.recv_into",
                                  RUNLOOM_IO_RECV, -1, __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_READ, -1LL) < 0) {
            if (!errno) errno = ECANCELED;
            return -1;
        }
    }
}

/* send exactly n bytes from buf.  Returns n on success, -1 on error. */
Py_ssize_t runloom_tcpconn_c_send_all(PyObject *conn, const void *buf, Py_ssize_t n)
{
    RunloomTCPConn *self = (RunloomTCPConn *)conn;
    int fd;
    Py_ssize_t sent = 0;

    if (self->closed || self->fd < 0) { errno = EBADF; return -1; }
    if (n <= 0) return 0;
    fd = self->fd;

#if defined(__linux__)
    /* Stage-2 single-shot io_uring proactor send (see recv_into above). */
    if (runloom_iouring_loop_enabled()) {
        runloom_iouring_ring_t *ring = runloom_mn_current_iouring_ring();
        if (ring != NULL) {
            while (sent < n) {
                runloom_iouring_ssize_t r =
                    runloom_iouring_loop_send(ring, fd, (const char *)buf + sent,
                                              (size_t)(n - sent), 0);
                if (r < 0) return -1;
                sent += (Py_ssize_t)r;
                if (r == 0) break;
            }
            return sent;
        }
    }
    if (runloom_tcpconn_use_iouring(self)) {
        while (sent < n) {
            runloom_iouring_ssize_t r =
                runloom_iouring_send(fd, (const char *)buf + sent,
                                     (size_t)(n - sent), 0);
            if (r < 0) return -1;
            sent += (Py_ssize_t)r;
            if (r == 0) break;            /* defensive */
        }
        return sent;
    }
#endif

    while (sent < n) {
#if defined(RUNLOOM_OS_WINDOWS)
        long long r = (long long)send((SOCKET)fd, (const char *)buf + sent,
                                      (int)(n - sent), 0);
#else
        long long r = (long long)send(fd, (const char *)buf + sent,
                                      (size_t)(n - sent), 0);
#endif
        RUNLOOM_IO_SWITCH(runloom_io_classify(RUNLOOM_IO_SEND, r, runloom_sock_errno())) {
        case RUNLOOM_IO_PROGRESS:
            sent += (Py_ssize_t)r;
            continue;                     /* re-check sent < n */
        case RUNLOOM_IO_WOULDBLOCK_W:
        case RUNLOOM_IO_INTR:
            break;                        /* not writable -> park on WRITE */
        case RUNLOOM_IO_ERROR:
            return -1;
        case RUNLOOM_IO_READY:
        case RUNLOOM_IO_EOF:
        case RUNLOOM_IO_WOULDBLOCK_R:
        case RUNLOOM_IO_RETRY:
        case RUNLOOM_IO_EVENT_COUNT:
            runloom_fsm_violation("runloom_tcp_capi.send_all",
                                  RUNLOOM_IO_SEND, -1, __FILE__, __LINE__);
        } RUNLOOM_IO_SWITCH_END
        if (runloom_netpoll_wait_fd_coop(fd, RUNLOOM_NETPOLL_WRITE, -1LL) < 0) {
            if (!errno) errno = ECANCELED;
            return -1;
        }
    }
    return sent;
}

/* The exported pointer table for the PyCapsule fallback. */
static const RunloomTCPCAPI runloom_tcp_capi_table = {
    runloom_tcpconn_c_recv_into,
    runloom_tcpconn_c_send_all,
};

/* Called once from module init (module_init.c.inc) to attach the capsule. */
int runloom_tcpconn_capi_register(PyObject *module)
{
    PyObject *cap = PyCapsule_New((void *)&runloom_tcp_capi_table,
                                  RUNLOOM_TCP_CAPI_CAPSULE_NAME, NULL);
    if (cap == NULL) return -1;
    if (PyModule_AddObject(module, "__tcp_capi__", cap) < 0) {
        Py_DECREF(cap);
        return -1;
    }
    return 0;
}
