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

# Work knob for the work curve: the SAME FNV-1a byte hash, run INLINE in this
# zero-PyObject Cython handler. So the FULL request path is native -- capi recv,
# native FNV, fold, capi send -- no interpreted recv_into/send_all/fold wrapper
# and no per-call boxing. This is the state-of-the-art optimized runloom handler
# (the line that competes with Go), not a Python def calling a compiled function.
# _work is set once via set_work() before serve() spawns any fiber. work=0 = echo.
cdef int _work = 0


def set_work(int w):
    global _work
    _work = w


cdef unsigned int _fnv(const unsigned char *buf, Py_ssize_t n, int passes) noexcept nogil:
    cdef unsigned int h = 2166136261u   # FNV-1a offset basis
    cdef Py_ssize_t i
    cdef int p
    for p in range(passes):
        for i in range(n):
            h = (h ^ buf[i]) * 16777619u   # FNV-1a prime, native wraparound
    return h


def handler(conn):
    """serve() hands us a runloom_c.TCPConn.  recv -> (optional inline FNV) ->
    send, until EOF. Zero PyObjects in the loop body either way."""
    cdef PyObject *c = <PyObject *>conn
    cdef char buf[CHUNK]
    cdef Py_ssize_t n
    cdef unsigned int h
    while True:
        n = _capi.recv_into(c, buf, CHUNK)
        if n <= 0:
            break
        if _work > 0:
            h = _fnv(<const unsigned char *>buf, n, _work)
            buf[0] = <char>(<unsigned char>buf[0] ^ <unsigned char>(h & 0xffu))   # fold -> no elision
        if _capi.send_all(c, buf, n) < 0:
            break
