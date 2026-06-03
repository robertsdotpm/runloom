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
    blocking,
    run,
    current,
    Goroutine,
)
import pygo_core as _core  # noqa: F401  – C extension lives at top level

backend = _core.backend

# Fork safety: after os.fork() the child keeps only the forking thread, so the
# M:N hub threads and the blocking-offload workers are gone.  Reset the C
# runtime in the child so it neither hangs (pygo_core.run / mn_run waiting on
# dead hubs) nor deadlocks on a lock a dead thread held at fork, and so the
# child gets its own netpoll fd instead of sharing the parent's.  Registered
# here once, at import, for ALL pygo use (the monkey layer adds its own,
# higher-level child handler on top).
import os as _os
if hasattr(_os, "register_at_fork"):
    _os.register_at_fork(after_in_child=_core.reset_after_fork)

# Runtime introspection -- `pygo.inspect.dump()`, goroutines(), stack(), etc.
# See pygo/inspect.py.  Exposed as a submodule plus a couple of top-level
# conveniences (the common "what are all my goroutines doing" calls).
from . import inspect  # noqa: E402,F401
goroutines = inspect.goroutines
dump = inspect.dump
