# cython: language_level=3, boundscheck=False, wraparound=False, freethreading_compatible=True
"""C-entry scheduler probe: spawn / yield fibers with NO Python eval, NO tstate,
NO shared closure cells -- the capstone that isolates runloom's pure scheduler
cost from the free-threaded-CPython interpreter contention the Python-fiber
microbenchmarks measure.

It externs runloom_c's two public scheduler entry points (both exported `T` in
runloom_c.so; resolved at runtime via RTLD_GLOBAL promotion in the driver):
  runloom_mn_fiber_c   -- spawn a g via the g->c_entry fast path (no Python frame)
  runloom_mn_yield_current -- the C-level cooperative yield

Must be called from inside a running runloom.run() (a fiber spawns more fibers).
"""

ctypedef void (*c_entry_fn)(void *) noexcept nogil

cdef extern from *:
    int runloom_mn_fiber_c(c_entry_fn fn, void *arg) nogil
    int runloom_mn_yield_current() nogil

cdef int _yk = 0   # yields per worker (set before spawning)


cdef void _noop(void *arg) noexcept nogil:
    pass


cdef void _yielder(void *arg) noexcept nogil:
    cdef int i
    for i in range(_yk):
        runloom_mn_yield_current()


def spawn_c(int n):
    """Spawn n tstate-free c_entry no-op fibers (compiled loop -- no per-spawn
    Python frame on either side). Drained by the enclosing runloom.run()."""
    cdef int i
    for i in range(n):
        runloom_mn_fiber_c(_noop, NULL)


def spawn_yielders_c(int g, int k):
    """Spawn g c_entry fibers that each yield k times via the C-level yield --
    the ctxswitch capstone (no Python eval, no shared cells)."""
    global _yk
    _yk = k
    cdef int i
    for i in range(g):
        runloom_mn_fiber_c(_yielder, NULL)
