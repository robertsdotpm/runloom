"""runloom -- Go-style coroutines in Python.

Everyday API -- `import runloom` is all you need:
    runloom.go(fn, *args, **kw)   spawn a goroutine
    runloom.run(n, main_fn)       THE entry point. run main_fn with n hubs:
                                  n=1 single-thread, n>1 M:N parallel across n
                                  cores (needs 3.13t + GIL off; n>1 on a GIL
                                  build raises).  Collapses mn_init/mn_go/
                                  mn_run/mn_fini.  main_fn optional -> drain only.
    runloom.sleep(seconds)        sleep without blocking the OS thread
    runloom.yield_now()           cooperative yield (give other goroutines a turn)
    runloom.Chan(capacity=0)      Go-style channel
    runloom.select(cases, ...)    wait on multiple channel ops
    runloom.blocking(fn, ...)     offload a blocking/CPU call off the hub
    runloom.mn_init/mn_go/mn_run/mn_fini   raw M:N scheduler (run() wraps these)
    runloom.backend() / .netpoll_backend()

Feature packages (import as needed, Go-style):
    runloom.monkey  -- make blocking stdlib cooperative (manual: .patch())
    runloom.time    -- After / Tick / Timer / Ticker
    runloom.context -- Background / WithCancel / WithTimeout
    runloom.sync    -- blocking-style sockets + Lock/Event/Semaphore
    runloom.aio     -- run async/await code on the scheduler

The raw C extension stays importable as `runloom_c` for advanced use
(TCPConn, wait_fd, warmup, raw Coro/G handles, go_noyield, ...).
"""
# Distribution version, read from the installed package metadata so that
# pyproject.toml stays the single source of truth.
from importlib.metadata import version, PackageNotFoundError
try:
    __version__ = version("runloom")
except PackageNotFoundError:        # running from an uninstalled source tree
    __version__ = "0.2.0"
del version, PackageNotFoundError

import sys as _sys

# CPython's per-thread recursion counter is not swapped across our
# ucontext stack switch (v0 -- properly handled in the M:N C path
# planned for phase 3).  Each runloom.yield_now() permanently decrements the
# counter on the OS thread, so a long runloom.run() eventually hits
# RecursionError.  Bumping the limit makes the leak tolerable for
# anything short of a multi-hour service; the proper fix is to
# save/restore tstate->py_recursion_remaining + c_recursion_remaining
# in the C resume/yield path.
if _sys.getrecursionlimit() < 1_000_000:
    _sys.setrecursionlimit(1_000_000)

from .runtime import (
    go,
    yield_now,
    yield_,        # deprecated alias for yield_now (keyword-dodge name)
    sleep,
    blocking,
    run,
    current,
    Goroutine,
    set_grow_down,      # toggle the default-on function-bound stack grow-down
    grow_down_enabled,
)
import runloom_c as _core  # noqa: F401  – C extension lives at top level

backend = _core.backend
netpoll_backend = _core.netpoll_backend

# Re-export the core scheduler + channel primitives so `import runloom` is all
# everyday code needs -- no separate `import runloom_c`.  (go / run / sleep /
# yield_now / blocking / current above are the friendly wrappers from .runtime.)
Chan = _core.Chan
select = _core.select

# M:N scheduler -- real multi-core parallelism on free-threaded 3.13t.
# Everyday code uses run(n, main_fn); these raw entry points stay exposed for
# advanced use (custom spawn loops, benchmarks).  mn_hub_count() reports how
# many hubs are live and is what the runloom.go() wrapper dispatches on.
mn_init = _core.mn_init
mn_go = _core.mn_go
mn_run = _core.mn_run
mn_fini = _core.mn_fini
mn_hub_count = _core.mn_hub_count
mn_hub_states = _core.mn_hub_states   # per-hub diagnostic snapshot (see inspect.hubs)

# Lower-level C primitives, surfaced here so `runloom_c` never needs importing
# directly for normal use.  (The raw module stays available as `runloom_c` for
# deep internals: sched_*, park_self, the get_/set_ tuning knobs, etc.)
TCPConn = _core.TCPConn                # C-level TCP connection (Go-parity perf)
Coro = _core.Coro                      # raw stackful coroutine handle
G = _core.G                            # goroutine handle type
wait_fd = _core.wait_fd                # park the current goroutine on fd readiness
WAIT_FD_CANCELLED = _core.WAIT_FD_CANCELLED
tcp_recv = _core.tcp_recv
tcp_send = _core.tcp_send
go_noyield = _core.go_noyield          # faster spawn for run-to-completion work
warmup = _core.warmup                  # pre-allocate the per-thread stack pool
thread_init = _core.thread_init
thread_fini = _core.thread_fini
preempt_init = _core.preempt_init
preempt_fini = _core.preempt_fini
iouring_available = _core.iouring_available

# Fork safety: after os.fork() the child keeps only the forking thread, so the
# M:N hub threads and the blocking-offload workers are gone.  Reset the C
# runtime in the child so it neither hangs (runloom_c.run / mn_run waiting on
# dead hubs) nor deadlocks on a lock a dead thread held at fork, and so the
# child gets its own netpoll fd instead of sharing the parent's.  Registered
# here once, at import, for ALL runloom use (the monkey layer adds its own,
# higher-level child handler on top).
import os as _os
if hasattr(_os, "register_at_fork"):
    _os.register_at_fork(after_in_child=_core.reset_after_fork)

# Opt-in crash reporter: set RUNLOOM_CRASH (on/all/wait/gdb/backtrace/pystack)
# to install a fatal-signal handler at import, so a SIGSEGV (e.g. a goroutine
# stack overflow) prints a classified goroutine dump instead of dying silently.
# Off by default -- we don't hijack process-wide signal handlers unless asked.
# Installed here, before the runtime starts, so the scheduler hubs are armed as
# they spawn.  See runloom.inspect.install_crash_handler() to install in code.
if _os.environ.get("RUNLOOM_CRASH", "").strip().lower() not in ("", "0", "off"):
    try:
        _core.install_crash_handler(
            _os.environ.get("RUNLOOM_CRASH"),
            _os.environ.get("RUNLOOM_CRASH_FILE"),
        )
    except Exception:   # never let crash-reporter setup break import
        pass

# Opt-in adaptive stack auto-sizer: RUNLOOM_STACK_AUTOSIZE=1 starts each
# goroutine kind large and learns its real size down over its first runs (in
# memory only, never persisted).  Off by default -- it changes per-kind stack
# sizes.  See runloom.inspect.enable_stack_autosize().
_autosize_env = _os.environ.get("RUNLOOM_STACK_AUTOSIZE", "").strip().lower()
if _autosize_env in ("1", "on", "true", "prescan"):
    try:
        _core.enable_stack_autosize(True, _autosize_env == "prescan")
    except Exception:
        pass

# Runtime introspection -- `runloom.inspect.dump()`, goroutines(), stack(), etc.
# See runloom/inspect.py.  Exposed as a submodule plus a couple of top-level
# conveniences (the common "what are all my goroutines doing" calls).
from . import inspect  # noqa: E402,F401
goroutines = inspect.goroutines
dump = inspect.dump
hubs = inspect.hubs

# Feature packages, imported eagerly so that `import runloom` is the ONLY
# import statement you ever need.  Importing them has no side effects -- in
# particular monkey patches the stdlib only when you call runloom.monkey.patch().
# (monkey first: runloom.sync builds its Lock/Event on monkey's primitives.)
from . import monkey   # noqa: E402,F401  – cooperative stdlib (manual .patch())
from . import time     # noqa: E402,F401  – After / Tick / Timer / Ticker
from . import context  # noqa: E402,F401  – Background / WithCancel / WithTimeout
from . import sync     # noqa: E402,F401  – blocking-style sockets + sync prims
from .sync import WaitGroup, Future, gather  # noqa: E402,F401  – fan-in primitives
from . import aio      # noqa: E402,F401  – run async/await on the scheduler

__all__ = [
    # scheduler
    "go", "run", "sleep", "yield_now", "yield_", "blocking", "current",
    "Goroutine", "go_noyield", "warmup", "thread_init", "thread_fini",
    "preempt_init", "preempt_fini",
    # channels
    "Chan", "select",
    # fan-in primitives
    "WaitGroup", "Future", "gather",
    # M:N (free-threaded 3.13t)
    "mn_init", "mn_go", "mn_run", "mn_fini", "mn_hub_count", "mn_hub_states",
    "hubs",
    # low-level I/O primitives
    "TCPConn", "Coro", "G", "wait_fd", "WAIT_FD_CANCELLED",
    "tcp_recv", "tcp_send", "iouring_available",
    # introspection
    "backend", "netpoll_backend", "goroutines", "dump", "inspect",
    # feature packages
    "monkey", "time", "context", "sync", "aio",
    "__version__",
]
