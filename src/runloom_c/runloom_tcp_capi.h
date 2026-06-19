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

/* Function-pointer table exported as the PyCapsule runloom_c.__tcp_capi__,
 * for consumers that prefer not to rely on RTLD_GLOBAL symbol resolution. */
typedef struct {
    Py_ssize_t (*recv_into)(PyObject *conn, void *buf, Py_ssize_t n);
    Py_ssize_t (*send_all)(PyObject *conn, const void *buf, Py_ssize_t n);
} RunloomTCPCAPI;

#define RUNLOOM_TCP_CAPI_CAPSULE_NAME "runloom_c.__tcp_capi__"

#endif /* RUNLOOM_TCP_CAPI_H */
