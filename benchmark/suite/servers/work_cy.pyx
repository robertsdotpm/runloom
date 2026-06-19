# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, freethreading_compatible=True
"""Compiled handler 'work' for the work-curve experiment.

An FNV-1a byte hash over the payload, repeated `passes` times -- the IDENTICAL
algorithm to py_fnv() in srv_runloom_work.py. The only difference between the two
is that this is compiled to native code, so the experiment isolates exactly one
variable: what does compiling the handler's work buy.

CRITICAL (per design): this is PURE inline arithmetic -- no stdlib, no I/O, no
call runloom could route to the blockpool/executor. It runs synchronously on the
fiber's hub, which is required for a valid per-hub CPU/throughput measurement.
A blockpool offload here would move the work to a worker thread and wreck the
per-core accounting. Unsigned wraparound is the hash; no masking needed in C.
"""


cpdef unsigned int fnv_work(const unsigned char[::1] buf, Py_ssize_t n, int passes):
    cdef unsigned int h = 2166136261u   # FNV-1a 32-bit offset basis (0x811c9dc5)
    cdef Py_ssize_t i
    cdef int p
    for p in range(passes):
        for i in range(n):
            h = (h ^ buf[i]) * 16777619u   # FNV-1a prime (0x01000193), wraps mod 2^32
    return h
