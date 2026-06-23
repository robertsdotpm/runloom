"""Shared helpers for the adversarial QA suite (tests/test_adv_*.py).

The adversarial suite deliberately drives the runtime toward its failure
modes: lost wakes, teardown hangs, refcount UAF, fd-reuse staleness,
foreign-OS-thread re-entry, guard-page overflow, and *slow returns* on
non-blocking I/O.  Two infrastructure problems follow from that goal and
this module solves both:

  1. A real hang (a lost wake inside C `run()`/`mn_run()` with no timeout
     argument) cannot be interrupted from Python.  `hang_guard()` arms
     `faulthandler.dump_traceback_later(..., exit=True)`, so a wedged test
     prints every thread's C+Python stack and `_exit`s instead of blocking
     forever.  Under tests/run_isolated.py that surfaces as a per-file
     TIMEOUT-ish crash with a pinpointed traceback, not a dead suite.

  2. "Slow return" is part of the assessment: a cooperative op that *does*
     return but only after starving its siblings is a bug.  `Stopwatch` /
     `assert_faster_than` make an upper-bound wall-clock assertion a
     first-class check, not a flaky afterthought.

`raw_thread()` spawns a **real** OS thread captured from the unpatched
`threading` module, so foreign-OS-thread tests keep a genuine non-fiber
thread even after `runloom.monkey.patch()` has replaced `threading`.
"""
import faulthandler
import os
import sys
import time
import threading
import contextlib

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

# Captured BEFORE any monkey.patch() in any test could run -- a genuine OS
# thread class + primitives a "foreign thread" test needs to stay foreign.
_RealThread = threading.Thread
_real_sleep = time.sleep


@contextlib.contextmanager
def hang_guard(seconds, label=""):
    """Dump all stacks and _exit if the body does not finish in `seconds`.

    The only reliable watchdog for a hang that lives inside the C scheduler
    with the GIL off: faulthandler runs its timer on a dedicated thread that
    does not need the interpreter to be responsive.
    """
    if label:
        sys.stderr.write("[hang_guard] arming {0}s for {1}\n".format(seconds, label))
        sys.stderr.flush()
    faulthandler.dump_traceback_later(seconds, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()


class Stopwatch(object):
    def __enter__(self):
        self.t0 = time.monotonic()
        return self

    def __exit__(self, *a):
        self.elapsed = time.monotonic() - self.t0
        return False


@contextlib.contextmanager
def assert_faster_than(seconds, what="operation"):
    """Fail if the body takes longer than `seconds` of wall-clock.

    A 'slow return' guard: the op completes, but cooperative overlap broke
    and it took far longer than the work warranted.
    """
    sw = Stopwatch().__enter__()
    try:
        yield
    finally:
        sw.__exit__()
    assert sw.elapsed < seconds, (
        "{0} took {1:.3f}s, expected < {2:.3f}s (slow return / lost overlap)"
        .format(what, sw.elapsed, seconds))


def raw_thread(target, *args, **kwargs):
    """A genuine OS thread from the pre-patch threading module."""
    t = _RealThread(target=target, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t


def free_tcp_port_pair():
    """Return (listen_sock, port) for a bound-but-not-accepted loopback listener."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    s.listen(128)
    return s, s.getsockname()[1]


def needs_free_threading():
    """True iff this interpreter has the GIL disabled (real M:N parallelism)."""
    return hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()


def pollable_pipe():
    """Return (rfd, wfd, keepalive) -- a pair of fds usable as a wait_fd target.

    On POSIX the netpoll backend (epoll/kqueue/select) can poll a pipe, so this
    is just os.pipe() and `keepalive` is None.

    On Windows the readiness backend is iocp-afd, which can ONLY poll Winsock
    sockets -- a wait_fd on an os.pipe() read end fails (AFD has no IRP path for
    a non-socket HANDLE).  A loopback socket.socketpair() IS pollable by AFD and
    is the same substitute monkey/_base.py + runloom.aio already use, so return
    its fds there.  The socket objects MUST stay referenced or Python closes the
    fds out from under the parked fiber, so the caller keeps `keepalive` alive.

    Use this only for tests that PARK on the fd (timeout / cancel / never-ready /
    park-forever) -- they never os.read()/os.write() the fds, which would not
    work on a Windows SOCKET handle.  Tests that drive readiness by writing a
    byte, or that probe pipe-/epoll-specific semantics, are gated off Windows
    instead.
    """
    if sys.platform == "win32":
        import socket
        s1, s2 = socket.socketpair()
        return s1.fileno(), s2.fileno(), (s1, s2)
    r, w = os.pipe()
    return r, w, None
