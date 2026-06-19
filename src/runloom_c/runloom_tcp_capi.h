/* runloom_tcp_capi.h -- zero-PyObject C entry points into a runloom_c.TCPConn's
 * cooperative recv/send hot loop, for C and Cython handlers.
 *
 * Motivation: a Python (or plain Cython) handler that calls conn.recv_into() /
 * conn.send_all() still pays Python method dispatch (PyObject_Call... ), a
 * Py_buffer fill, and a PyLong result box on EVERY round trip.  These two
 * functions run the SAME epoll / io_uring core the methods run, but operate on
 * a raw buffer and return a C Py_ssize_t -- so a Cython handler that cimports
 * them compiles to a hot loop with NO PyObject traffic (verifiable in the
 * disassembly: no calls to any Py_ or _Py_ symbol between recv and send).
 *
 * Contract:
 *   conn   -- a runloom_c.TCPConn (borrowed; NOT type-checked here, the caller
 *             owns the reference for the duration of the call).
 *   recv_into(conn, buf, n) -> bytes read into buf (0 == orderly EOF),
 *                              or -1 on error (errno is set; NO Python
 *                              exception is raised).
 *   send_all(conn, buf, n)  -> n on success, or -1 on error (errno set).
 *
 * Both park the calling fiber on the netpoll on EAGAIN and therefore MUST be
 * called from inside a fiber (i.e. from a serve() handler).  They honour the
 * active loop backend (epoll or RUNLOOM_IOURING_LOOP) exactly as the methods
 * do, and the per-conn RUNLOOM_TCPCONN_IOURING multishot choice.
 *
 * Symbol export: these have default visibility in runloom_c.so.  A separately
 * compiled Cython module resolves them either (a) by importing runloom_c with
 * RTLD_GLOBAL set, or (b) via the PyCapsule runloom_c.__tcp_capi__ whose
 * pointer is a `const RunloomTCPCAPI *` (see below).
 */
#ifndef RUNLOOM_TCP_CAPI_H
#define RUNLOOM_TCP_CAPI_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

Py_ssize_t runloom_tcpconn_c_recv_into(PyObject *conn, void *buf, Py_ssize_t n);
Py_ssize_t runloom_tcpconn_c_send_all(PyObject *conn, const void *buf, Py_ssize_t n);

/* Raw-fd, tstate-free cooperative I/O -- the all-C echo's fast path, reusable by
 * a custom c_entry handler (e.g. a Cython cdef function).  No PyObject, no
 * TCPConn, no tstate.  Uses the Stage-2 io_uring proactor (loop_recv/send) when
 * the loop backend + a hub ring are active, else the readiness recv/send +
 * wait_fd path.  recv: bytes read (0 = EOF), -1 on error.  send_all: n, or -1.
 * close: clear the netpoll arm + close (avoids the fd-reuse deadlock). */
Py_ssize_t runloom_tcp_c_fd_recv(int fd, void *buf, Py_ssize_t n);
Py_ssize_t runloom_tcp_c_fd_send_all(int fd, const void *buf, Py_ssize_t n);
void       runloom_tcp_c_fd_close(int fd);

/* Function-pointer table exported as the PyCapsule runloom_c.__tcp_capi__,
 * for consumers that prefer not to rely on RTLD_GLOBAL symbol resolution. */
typedef struct {
    Py_ssize_t (*recv_into)(PyObject *conn, void *buf, Py_ssize_t n);
    Py_ssize_t (*send_all)(PyObject *conn, const void *buf, Py_ssize_t n);
    Py_ssize_t (*fd_recv)(int fd, void *buf, Py_ssize_t n);
    Py_ssize_t (*fd_send_all)(int fd, const void *buf, Py_ssize_t n);
    void       (*fd_close)(int fd);
} RunloomTCPCAPI;

#define RUNLOOM_TCP_CAPI_CAPSULE_NAME "runloom_c.__tcp_capi__"

/* serve(handler=<PyCapsule of this name>) custom C handler: a capsule wrapping a
 * void(*)(void *arg) (arg = the accepted connection fd, cast through intptr_t)
 * makes serve() spawn the handler as a tstate-free c_entry fiber per connection
 * (runloom_mn_fiber_c) -- the all-C echo's fast path with custom logic. */
#define RUNLOOM_C_HANDLER_CAPSULE_NAME "runloom_c.c_handler"

#endif /* RUNLOOM_TCP_CAPI_H */
