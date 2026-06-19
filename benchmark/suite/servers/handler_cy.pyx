# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, freethreading_compatible=True
"""Zero-PyObject Cython echo handler for runloom_c.serve (benchmark tiers 4 & 5).

The whole point of this tier: the per-request hot loop must create NO Python
objects, so the cost of an echo round-trip is `recv()` + `send()` + a netpoll
park and nothing else -- no method dispatch, no bytes box, no Py_buffer fill.

We get there by calling the C functions runloom_tcpconn_c_recv_into /
runloom_tcpconn_c_send_all *directly* (via the runloom_c.__tcp_capi__ capsule's
function-pointer table), passing a raw stack buffer.  `disasm_check.sh` objdumps
the compiled `handler` symbol and asserts there is NO call to any Py_/_Py_/
PyObject_ symbol between the recv and send calls.

Built by build_cy.py against -I<repo>/src/runloom_c.  `freethreading_compatible`
is REQUIRED: without it, importing this module on 3.13t silently re-enables the
GIL and destroys the M:N parallelism we are trying to measure.
"""
from cpython.object cimport PyObject
from cpython.pycapsule cimport PyCapsule_GetPointer

cdef extern from "runloom_tcp_capi.h":
    ctypedef struct RunloomTCPCAPI:
        Py_ssize_t (*recv_into)(PyObject *conn, void *buf, Py_ssize_t n)
        Py_ssize_t (*send_all)(PyObject *conn, const void *buf, Py_ssize_t n)
    const char *RUNLOOM_TCP_CAPI_CAPSULE_NAME

# Resolve the pointer table once at import (no RTLD_GLOBAL games, no undefined
# symbols -- the capsule is the supported cross-module C-API hand-off).
cdef const RunloomTCPCAPI *_capi = NULL

cdef int _load_capi() except -1:
    global _capi
    import runloom_c
    cap = runloom_c.__tcp_capi__
    _capi = <const RunloomTCPCAPI *>PyCapsule_GetPointer(
        cap, RUNLOOM_TCP_CAPI_CAPSULE_NAME)
    if _capi is NULL:
        raise ImportError("runloom_c.__tcp_capi__ capsule pointer is NULL")
    return 0

_load_capi()

# Chunk size for the streaming echo.  64 KiB lives comfortably on the fiber's
# 512 KiB stack and is large enough that the 1.5 MB bandwidth test loops only
# ~24x per message while the small-payload req/s test fits in one read.
DEF CHUNK = 65536

def handler(conn):
    """serve() hands us a runloom_c.TCPConn.  Echo until EOF, zero PyObjects
    in the loop body."""
    cdef PyObject *c = <PyObject *>conn
    cdef char buf[CHUNK]
    cdef Py_ssize_t n
    while True:
        n = _capi.recv_into(c, buf, CHUNK)
        if n <= 0:
            break
        if _capi.send_all(c, buf, n) < 0:
            break
