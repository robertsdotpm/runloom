# cython: language_level=3, boundscheck=False, wraparound=False, freethreading_compatible=True
"""Tstate-free Cython `cdef` handler for runloom_c.serve(handler=<capsule>).

This is the *c_entry* fast path with custom logic. Unlike handler_cy (a Python
`def`, so serve() spawns it as a full Python fiber that carries a PyThreadState
and pays tstate_save/restore on every park), this handler is exposed to serve()
as a `runloom_c.c_handler` PyCapsule wrapping a `cdef` C function. serve() spawns
it via runloom_mn_fiber_c -> the `g->c_entry` path in runloom_g_entry, which skips
ALL Python-frame / tstate setup. So the request loop has:
  * zero PyObjects (like handler_cy), AND
  * zero tstate save/restore per park (unlike handler_cy) -- the all-C echo's
    advantage, now available to a custom handler.

It runs entirely `nogil` (there is no tstate to hold), calling runloom's raw-fd
cooperative recv/send via the runloom_c.__tcp_capi__ capsule (the proactor when
RUNLOOM_IOURING_LOOP is on, else readiness + wait_fd).
"""
from libc.stdint cimport intptr_t
from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_New

# Function-pointer types for the raw-fd capi (noexcept nogil: callable from the
# tstate-free, GIL-less c_entry handler).
ctypedef Py_ssize_t (*fd_recv_t)(int fd, void *buf, Py_ssize_t n) noexcept nogil
ctypedef Py_ssize_t (*fd_send_t)(int fd, const void *buf, Py_ssize_t n) noexcept nogil
ctypedef void (*fd_close_t)(int fd) noexcept nogil

cdef extern from "runloom_tcp_capi.h":
    ctypedef struct RunloomTCPCAPI:
        fd_recv_t fd_recv
        fd_send_t fd_send_all
        fd_close_t fd_close
    const char *RUNLOOM_TCP_CAPI_CAPSULE_NAME
    const char *RUNLOOM_C_HANDLER_CAPSULE_NAME

cdef fd_recv_t _fd_recv = NULL
cdef fd_send_t _fd_send_all = NULL
cdef fd_close_t _fd_close = NULL

# Work knob for the cross-runtime work curve: the SAME FNV-1a byte hash as
# work_cy / py_fnv / goFnv, but here it runs INSIDE the tstate-free nogil cdef
# handler -- so unlike srv_runloom_work.py's Python `def` handler (which wraps the
# compiled work in interpreted recv_into/send_all/fold), the ENTIRE request path
# is native C. This is the "fully-native runloom handler" line for the
# Cython-vs-cdef-vs-Go comparison. _work is set once via set_work() before serve()
# spawns any fiber, so the nogil read needs no lock. work=0 -> plain echo.
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


cdef int _load() except -1:
    global _fd_recv, _fd_send_all, _fd_close
    import runloom_c
    cdef RunloomTCPCAPI *capi = <RunloomTCPCAPI *>PyCapsule_GetPointer(
        runloom_c.__tcp_capi__, RUNLOOM_TCP_CAPI_CAPSULE_NAME)
    if capi is NULL:
        raise ImportError("runloom_c.__tcp_capi__ capsule pointer is NULL")
    _fd_recv = capi.fd_recv
    _fd_send_all = capi.fd_send_all
    _fd_close = capi.fd_close
    return 0


_load()


cdef void echo_handler(void *arg) noexcept nogil:
    """serve() hands us the accepted fd (via intptr_t). recv -> (optional FNV
    work) -> send, until EOF. No tstate, no Python objects, no GIL -- the c_entry
    fast path. With _work>0 the ENTIRE request path is native C (the fully-native
    runloom handler line vs Go); _work==0 is the plain echo."""
    cdef int fd = <int><intptr_t>arg
    cdef char buf[16384]
    cdef Py_ssize_t n
    cdef unsigned int h
    while True:
        n = _fd_recv(fd, buf, 16384)
        if n <= 0:
            break
        if _work > 0:
            h = _fnv(<const unsigned char *>buf, n, _work)
            buf[0] = <char>(<unsigned char>buf[0] ^ <unsigned char>(h & 0xffu))   # fold in -> no elision
        if _fd_send_all(fd, buf, n) < 0:
            break
    _fd_close(fd)


# The capsule serve() detects (RUNLOOM_C_HANDLER_CAPSULE_NAME = "runloom_c.c_handler").
handler = PyCapsule_New(<void *>echo_handler, RUNLOOM_C_HANDLER_CAPSULE_NAME, NULL)
