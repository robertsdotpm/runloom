/* runloom_tcp.h -- runloom_c.TCPConn type.
 *
 * A thin C-side TCP connection that bypasses socket.socket entirely.
 * The fd lives in the struct, the netpoll registration is cached on
 * connect/accept (LEVEL-triggered, armed once per direction), and the
 * recv/send hot path is a
 * single C call -- no BlockingIOError raise/catch, no Python frame
 * dispatch through socket.socket methods.
 *
 * Exposed as runloom_c.TCPConn:
 *   TCPConn(fd)                           -- wrap an existing fd
 *   TCPConn.connect(host, port)           -- TCP/IPv4 or v6 connect
 *   TCPConn.listen(host, port, backlog)   -- bind + listen
 *   conn.accept() -> TCPConn              -- listener accept loop
 *   conn.recv(n, flags=0) -> bytes
 *   conn.recv_into(buf, n=0, flags=0) -> int
 *   conn.send(data, flags=0) -> int
 *   conn.send_all(data, flags=0) -> int
 *   conn.fileno() -> int
 *   conn.close()
 *
 * All recv/send methods park the calling fiber via the netpoll
 * on EAGAIN.  All methods must be called from inside a fiber.
 */
#ifndef RUNLOOM_TCP_H
#define RUNLOOM_TCP_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* Type registration entry point: called once from module init.
 * Adds TCPConn as an attribute of the module. */
int runloom_tcpconn_register(PyObject *module);

/* Attach the runloom_c.__tcp_capi__ PyCapsule (zero-PyObject recv/send pointer
 * table for Cython/C handlers).  Defined in runloom_tcp_capi.c.inc. */
int runloom_tcpconn_capi_register(PyObject *module);

#endif
