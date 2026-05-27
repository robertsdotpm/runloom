"""pygo -- Go-style coroutines in Python.

Public API (v0):
    pygo.go(fn, *args, **kw)   spawn a goroutine
    pygo.yield_()              cooperative yield
    pygo.sleep(seconds)        sleep without blocking the OS thread
    pygo.run(main_fn=None)     drive the scheduler until idle
    pygo.backend()             "fibers" | "ucontext"
"""
import sys as _sys

# CPython's per-thread recursion counter is not swapped across our
# ucontext stack switch (v0 -- properly handled in the M:N C path
# planned for phase 3).  Each pygo.yield_() permanently decrements the
# counter on the OS thread, so a long pygo.run() eventually hits
# RecursionError.  Bumping the limit makes the leak tolerable for
# anything short of a multi-hour service; the proper fix is to
# save/restore tstate->py_recursion_remaining + c_recursion_remaining
# in the C resume/yield path.
if _sys.getrecursionlimit() < 1_000_000:
    _sys.setrecursionlimit(1_000_000)

from .runtime import (
    go,
    yield_,
    sleep,
    run,
    current,
    Goroutine,
)
import pygo_core as _core  # noqa: F401  – C extension lives at top level

backend = _core.backend
