"""Shared foundation for the runloom asyncio bridge: stdlib re-exports,
_go_io (roomy-stack fiber spawn), module-root capture, blocking
helpers, cooperative _wait_fd, the lazy CoLock, and the current-task
registry.  Every submodule does `from ._base import *`."""

import asyncio
import collections as _collections
import contextvars as _contextvars
import errno as _errno
import inspect as _inspect
import os as _os
import socket as _socket
import ssl as _ssl
import subprocess as _subprocess
import sys
import threading as _threading
import time as _time
import warnings as _warnings
import weakref as _weakref

import runloom_c
from .. import runtime as _runtime


# Security: the bridge runs user protocol callbacks -- including TLS handshakes
# and OpenSSL key material -- on fiber stacks that are pooled and reused.
# By default those stacks are not scrubbed on recycle, so a later connection's
# fiber could read the previous one's leftovers off a shared stack (see
# tools/security/FINDINGS.md, S1). Enable scrubbing by default for the bridge:
# its fibers are long-lived (per connection / per task), so the per-recycle
# cost (MADV_DONTNEED, ~8 us) is negligible. Opt out with RUNLOOM_STACK_SCRUB=0.
# No-op on a runloom_c too old to expose the API.
if _os.environ.get("RUNLOOM_STACK_SCRUB") != "0":
    try:
        runloom_c.set_stack_scrub(True)
    except AttributeError:
        pass


def _signal_wakeup_noop(signum, frame):
    # A Python-level handler must be installed for CPython to write the signum
    # to set_wakeup_fd()'s pipe; the real dispatch happens loop-side off that
    # pipe (see RunloomEventLoop.add_signal_handler), so this is intentionally a
    # no-op.  A server may temporarily replace it with its own handler -- the
    # wakeup-fd write happens regardless, so loop-side dispatch survives.
    pass


# Per-task driver stack size (bytes).  RunloomTask drivers run arbitrary user
# code, including deep C-recursive first-time imports (pydantic etc.) that
# overflow the scheduler's default 32 KB g-stack and SEGV.  512 KB clears
# every real-world import chain seen so far while staying cheap relative to
# the CPython object tax per task.  Set RUNLOOM_AIO_TASK_STACK=0 to disable and
# use the scheduler default; set a custom byte count to tune.
try:
    _TASK_STACK = int(_os.environ.get("RUNLOOM_AIO_TASK_STACK", 512 * 1024))
except ValueError:
    _TASK_STACK = 512 * 1024

# Per-connection I/O fiber stack size (bytes).  The transport read /
# datagram / accept fibers invoke arbitrary user protocol callbacks
# (data_received, connection_made, datagram_received, ...) SYNCHRONOUSLY on
# their own swapped C stack -- and those callbacks can run deep C-recursive
# code (e.g. asyncssh runs a full crypto key-exchange + chacha20/OpenSSL chain
# inside data_received).  That overflows the scheduler's default 32 KB g-stack
# and SEGVs, exactly like the task-driver case above.  Give them the same
# roomier stack.  Set RUNLOOM_AIO_IO_STACK=0 to use the scheduler default; set a
# custom byte count to tune.
try:
    _IO_STACK = int(_os.environ.get("RUNLOOM_AIO_IO_STACK", _TASK_STACK or 512 * 1024))
except ValueError:
    _IO_STACK = _TASK_STACK or 512 * 1024


def _go_io(fn):
    """Spawn a fiber that synchronously runs user protocol callbacks,
    on the roomier _IO_STACK (falls back to the scheduler default if disabled).

    fifo=True marks the fiber so the PCT controlled scheduler (RUNLOOM_PCT_SEED)
    keeps it in spawn order relative to other aio fibers -- the aio bridge
    delivers call_soon callbacks / task steps as fibers, and asyncio
    guarantees them call_soon-FIFO, so permuting them is a false positive, not a
    bug.  No effect when PCT is off; PCT still freely interleaves raw fibers."""
    if _IO_STACK:
        return runloom_c.go(fn, stack_size=_IO_STACK, fifo=True)
    return runloom_c.go(fn, fifo=True)


# ------------------------------------------------------------------
# Module-root frame for task-driver fibers.
#
# A RunloomTask drives its coroutine on the fiber's own swapped C stack,
# whose Python frame chain runloom_c deliberately severs at the fiber
# root (so tracebacks / recursion don't bleed across fibers).  Stock
# asyncio instead runs a Task's coro synchronously nested under
# _run_once -> run_forever -> ... -> "<module>" on ONE stack, so a library
# that derives its module name by walking frame.f_back to the first
# co_name == "<module>" -- aiohttp's web.AppKey (helpers.py) -- finds it.
# Under runloom the walk dead-ends at the driver and AppKey raises
# UnboundLocalError (test_web_app subapp tests; pass under stock asyncio).
#
# Fix: run the driver coroutine *underneath* a real "<module>"-named frame.
# compile(src, name, "exec") yields a top code object whose co_name is
# literally "<module>"; exec'ing it with a globals dict carrying the right
# __name__ seats a genuine, lifecycle-correct module frame at the fiber
# root.  No hand-built _PyInterpreterFrame, and crucially no cross-stack
# f_back link to the spawner (that would dangle the moment the spawner's
# stack is swapped away or returns, and would be a lie -- the spawner is
# concurrent, not on the fiber's call stack).  Only task-driver
# fibers are wrapped; raw runloom_c.go() fibers (netpoll pump,
# keepalive, timers) are untouched, so the per-fiber cost stays off the
# scale-out path.  Disable with RUNLOOM_AIO_MODULE_ROOT=0.
_PG_MODULE_ROOT_ON = _os.environ.get("RUNLOOM_AIO_MODULE_ROOT", "1") != "0"
_PG_ROOT_CODE = compile("__runloom_body__()", "<runloom-task-root>", "exec")


def _pg_capture_module_name(default="__main__"):
    """Walk the CREATOR's live stack (RunloomTask.__init__ runs synchronously on
    it) to the nearest "<module>" frame and copy its __name__ -- the same
    module asyncio's frame walk would reach.  Holds only the string, never the
    frame.  A nested create_task() finds the parent task's own module-root
    frame first, so the name self-propagates down the task tree."""
    f = _inspect.currentframe()
    try:
        f = f.f_back if f is not None else None     # skip our own frame
        while f is not None:
            if f.f_code.co_name == "<module>":
                name = f.f_globals.get("__name__")
                if name is not None:
                    return name
            f = f.f_back
    finally:
        del f                                       # don't strand a frame ref
    return default


def _pg_run_with_module_root(body, module_name):
    exec(_PG_ROOT_CODE, {"__name__": module_name, "__runloom_body__": body})


# ------------------------------------------------------------------
# CONCURRENT event loops: one scheduler PER OS THREAD (runloom "Phase C").
#
# runloom_c.run() drains the CALLING thread's own (thread-local) scheduler, so
# each asyncio loop runs on its thread and is fully independent of loops on
# other threads -- exactly like stock asyncio.  A thread blocking synchronously
# inside a coroutine (run_coroutine_threadsafe().result(), anyio
# BlockingPortal, a threaded server controller with a blocking client) freezes
# only its own sched, never the others'.  No single-driver election, no global
# bootstrap queue: each loop just drives itself.
#
# The only cross-thread rule: runloom_c.go() (create_task/call_soon/call_later/
# keepalive) must run on the LOOP'S thread -- a foreign thread's go() would land
# on ITS thread's sched, which this loop never drains.  So a foreign-thread
# spawn is marshalled onto the loop's thread via call_soon_threadsafe (the
# loop's lock-guarded ts queue, drained by its keepalive on its own thread).
# ------------------------------------------------------------------


def _blocking(fn, *args):
    """runloom_c.blocking (offload fn to the blocking-pool), but deliver a
    cancellation requested WHILE we were in the call.

    runloom_c.blocking parks the fiber in C with no driver await-point, so
    task.cancel() cannot interrupt it -- it only sets the task's one-shot
    _pgmustcancel and wakes us (which runloom_c.blocking now ignores until the
    worker is done, to avoid freeing the in-flight job).  Stock asyncio resolves
    via run_in_executor, an await that raises CancelledError on cancel; mirror
    that here so a cancel during DNS doesn't silently get swallowed and let the
    caller go on to park uncancellably (e.g. in the connect wait_fd -> hang)."""
    r = runloom_c.blocking(fn, *args)
    task = asyncio.current_task()
    if task is not None and getattr(task, "_pgmustcancel", False):
        task._pgmustcancel = False
        raise asyncio.CancelledError()
    return r


def _resolve(host, port, family, type_, proto, flags):
    """getaddrinfo via the blocking-offload pool, so DNS doesn't wedge the
    fiber's hub (it is a non-preemptible blocking C call).  Runs inline
    when not on a fiber -- safe in either context."""
    return _blocking(_socket.getaddrinfo, host, port,
                     family, type_, proto, flags)


def _close_sock(sock):
    """Close a socket and tell the netpoll backend to forget about it.

    Without the netpoll_unregister, the per-fd "already registered"
    bitmap stays sticky for the closed fd.  When the OS later reuses
    that fd number for a new socket, netpoll skips re-registering and
    no edge ever fires -- the new socket's wait_fd parks forever.
    Manifests as test-run hangs after fast socket churn. """
    if sock is None:
        return
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        fd = -1
    if fd >= 0:
        try: runloom_c.netpoll_unregister(fd)
        except (AttributeError, OSError): pass
    try: sock.close()
    except OSError: pass


def _reject_subprocess_text_mode(kwargs):
    """Mirror asyncio.base_events.subprocess_{exec,shell}: an asyncio
    subprocess is always binary and unbuffered.  Reject text-mode / buffering
    kwargs with ValueError (CPython's test_subprocess asserts these raises),
    and pop bufsize so it can't collide with our hardcoded bufsize=0 Popen."""
    if kwargs.get("universal_newlines"):
        raise ValueError("universal_newlines must be False")
    if kwargs.get("text"):
        raise ValueError("text must be False")
    if kwargs.get("encoding") is not None:
        raise ValueError("encoding must be None")
    if kwargs.get("errors") is not None:
        raise ValueError("errors must be None")
    if kwargs.pop("bufsize", 0) != 0:
        raise ValueError("bufsize must be 0")


# A cooperative mutex (parks the fiber, not the OS thread) imported lazily
# to keep the import graph acyclic.  Used to serialise access to one SSLSocket
# shared by a connection's recv fiber and concurrent writers under M:N.
_CoLock = None


def _get_colock():
    global _CoLock
    if _CoLock is None:
        from ..monkey import CoLock as _CL
        _CoLock = _CL
    return _CoLock


# wait_fd direction flags (match the literals used throughout this file).
_WAIT_READ = 1
_WAIT_WRITE = 2

# Sentinel runloom_c.wait_fd returns when the parked fiber was cancelled
# out-of-band via G.cancel_wait_fd() -- a task.cancel() that targets a g blocked
# in a socket recv/accept/connect, where there's no coro await-point to throw
# CancelledError into.  _wait_fd turns it back into CancelledError so it unwinds
# the recv loop -> the coro -> the driver, which settles the task cancelled.
_WAIT_FD_CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", 0x40000000)


# asyncio's non-raising "loop running on this thread, or None" accessor (C fn);
# used on the hot socket-I/O path in _wait_fd to keep current_task() correct.
_PG_GET_RUNNING_LOOP = asyncio.events._get_running_loop


def _wait_fd(fd, events, timeout_ms=-1):
    """runloom_c.wait_fd, but a cancellation (G.cancel_wait_fd) raises
    CancelledError instead of returning the raw sentinel.  Every aio I/O loop
    parks through this, so cancelling a task blocked in any socket wait works.

    Also preserves asyncio's "current task" across the park.  The RunloomTask
    driver sets _CURRENT_TASKS[loop] = self around each coro.send and restores
    it in a finally -- but a coroutine that parks HERE for socket I/O suspends
    the fiber MID-send, so that finally can't run until the send eventually
    returns.  While we're parked, other tasks run and their drivers mutate the
    single shared per-loop slot (and pop it back to whatever preceded them), so
    on resume the slot may name the wrong task or be empty -- breaking
    asyncio.current_task() and hence asyncio.timeout()/wait_for() ("Timeout
    should be used inside a task").  Snapshot our task before the C park and
    re-establish it on resume so current_task() is correct across a socket wait.
    (Future-based awaits don't need this: they park BETWEEN sends, after the
    driver's finally has already run.)"""
    loop = _PG_GET_RUNNING_LOOP()
    saved = _CURRENT_TASKS.get(loop) if loop is not None else None
    try:
        r = runloom_c.wait_fd(fd, events, timeout_ms)
    finally:
        if saved is not None and _CURRENT_TASKS.get(loop) is not saved:
            _CURRENT_TASKS[loop] = saved
    if r == _WAIT_FD_CANCELLED:
        raise asyncio.CancelledError()
    return r



# asyncio's private "currently-running task per loop" registry.  This is
# what asyncio.current_task() reads, and several stdlib helpers
# (asyncio.timeouts, asyncio.shield, taskgroups) bail with
# "must be used inside a task" if the entry is missing.  We update it
# from RunloomTask._driver around every send/throw.
try:
    _CURRENT_TASKS = asyncio.tasks._current_tasks
except AttributeError:
    # Very old Python -- fall back to a no-op dict; current_task() will
    # return None and asyncio.timeouts won't work, but the rest does.
    _CURRENT_TASKS = {}


# Re-export every name defined above so a section module gets the
# whole foundation with a single `from ._base import *`.
__all__ = [name for name in list(globals()) if not name.startswith("__")]
