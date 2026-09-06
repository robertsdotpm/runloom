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
# Bound at import, BEFORE any monkeypatching: OverlapTracker records spans from
# blockpool worker threads, so its lock must be a real OS lock rather than a
# cooperative one that would park a thread the scheduler does not manage.
_RealLock = threading.Lock
# Same reason: a rendezvous between genuine executor threads must not be a
# cooperative barrier, which would never release them.
RealBarrier = threading.Barrier
_real_sleep = time.sleep
# The wedge-capture watchdog reuses _RealThread (above) -- it MUST run on a real
# OS thread, since a cooperative thread can't run while the scheduler is wedged,
# which is exactly when we need the dump.  Event is captured pre-patch too.
_real_event = threading.Event


def dump_cooperative_state(label=""):
    """Dump the runloom COOPERATIVE state for a wedge / lost-wake post-mortem.

    faulthandler shows only the OS-thread (hub) stacks; it cannot show the
    PARKED FIBERS -- which is exactly what a lost-wake wedge looks like.  This
    dumps:
      * dump_fibers   -- every live fiber + its state + the fd it waits on
                         (e.g. ``g1025 io-wait fd=5 ev=R``);
      * _dump_parkers -- the netpoll parkers.  A nonzero ``readyParked`` means an
                         fd was READY but its parker was NOT woken == a LOST WAKE
                         (the smoking gun: data present, fiber still parked);
      * print_hubs    -- per-hub running_g / dwell / pending.
    Safe to call from a foreign OS thread while the scheduler is wedged.
    """
    import runloom_c as _rc
    tag = " (" + label + ")" if label else ""
    sys.stderr.write("\n[wedge-capture] cooperative-state dump{0}:\n".format(tag))
    sys.stderr.flush()
    for fn in ("dump_fibers", "_dump_parkers"):
        try:
            getattr(_rc, fn)()
        except Exception as e:               # noqa: BLE001
            sys.stderr.write("[wedge-capture] {0} failed: {1!r}\n".format(fn, e))
    # The aio task layer, which dump_fibers cannot see: a WAITING task has no
    # live fiber (its driver returned into a future), so a task wedged above the
    # scheduler appears in the fiber dump only as an absence.  Only meaningful
    # if the aio bridge is actually loaded -- importing it here otherwise would
    # be a surprising side effect mid-wedge.
    _aio = sys.modules.get("runloom.aio.tasks")
    if _aio is not None:
        try:
            _aio.print_tasks(file=sys.stderr)
        except Exception as e:                   # noqa: BLE001
            sys.stderr.write("[wedge-capture] task dump failed: {0!r}\n".format(e))
    try:
        from runloom import inspect as _gi
        _gi.print_hubs(file=sys.stderr)
    except Exception as e:                    # noqa: BLE001
        sys.stderr.write("[wedge-capture] print_hubs failed: {0!r}\n".format(e))
    sys.stderr.flush()


@contextlib.contextmanager
def wedge_capture(seconds, label=""):
    """Real-OS-thread watchdog: if the body does not finish within `seconds`,
    dump_cooperative_state(label).  Unlike hang_guard it does NOT abort -- the
    body keeps running (the outer test/run_isolated timeout is the backstop), so
    a recoverable slow path is merely annotated, while a true wedge is captured
    with WHICH fiber parked on WHICH fd instead of an opaque timeout.
    """
    _done = _real_event()

    def _watch():
        if not _done.wait(seconds):
            dump_cooperative_state(label)

    _RealThread(target=_watch, name="wedge_capture", daemon=True).start()
    try:
        yield
    finally:
        _done.set()


@contextlib.contextmanager
def hang_guard(seconds, label="", capture=True):
    """Dump all stacks and _exit if the body does not finish in `seconds`.

    The only reliable watchdog for a hang that lives inside the C scheduler
    with the GIL off: faulthandler runs its timer on a dedicated thread that
    does not need the interpreter to be responsive.  With ``capture`` (default
    on), also dump the runloom COOPERATIVE state (dump_cooperative_state) just
    before the faulthandler exit, so a lost-wake wedge shows which fiber parked
    on which fd -- not just the opaque OS-thread dump.

    Also forces UNRAISABLE exceptions to be reported the instant they happen.
    A fiber whose body raises does not propagate anywhere -- runloom captures it
    into g->error and reports it through sys.unraisablehook (see
    RUNLOOM_GOROUTINE_PANIC).  That report is the single most useful artifact
    when a hang is caused by a fiber dying, because it names the line.  But
    pytest's `unraisableexception` plugin replaces the hook to COLLECT
    unraisables and re-raise them at test TEARDOWN -- and this guard exits via
    faulthandler's exit=True, i.e. _exit(), so teardown never runs and the
    report is discarded exactly when it mattered.  Measured: the same dying
    driver prints a full traceback under `-p no:unraisableexception` and
    nothing at all under stock pytest.  So write it to fd 2 immediately, then
    still delegate to whatever hook was installed.
    """
    if label:
        sys.stderr.write("[hang_guard] arming {0}s for {1}\n".format(seconds, label))
        sys.stderr.flush()
    _done = _real_event()
    if capture:
        def _cap():
            if not _done.wait(max(2.0, seconds * 0.8)):
                dump_cooperative_state(label)
        _RealThread(target=_cap, name="hang_guard_capture", daemon=True).start()

    _prev_hook = sys.unraisablehook

    def _immediate_unraisable(unraisable, _prev=_prev_hook):
        # os.write to fd 2 directly: no buffering to lose if we are _exit()ed
        # moments later, and safe from a foreign thread mid-wedge.
        try:
            import traceback as _tb
            txt = "".join(_tb.format_exception(
                unraisable.exc_type, unraisable.exc_value, unraisable.exc_traceback))
            obj = getattr(unraisable, "object", None)
            os.write(2, ("\n[hang_guard] UNRAISABLE in %r%s:\n%s"
                         % (obj,
                            (" -- " + unraisable.err_msg) if getattr(
                                unraisable, "err_msg", None) else "",
                            txt)).encode("utf-8", "replace"))
        except Exception:
            pass
        try:
            _prev(unraisable)          # keep pytest's collection working too
        except Exception:
            pass

    sys.unraisablehook = _immediate_unraisable
    faulthandler.dump_traceback_later(seconds, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()
        _done.set()
        if sys.unraisablehook is _immediate_unraisable:
            sys.unraisablehook = _prev_hook


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


class OverlapTracker(object):
    """Records when each unit of work actually ran, so a test can assert that
    they OVERLAPPED rather than that they finished by some deadline.

    Wall-clock overlap bounds ("N jobs of 0.15s must finish in under 0.9s")
    read like parallelism assertions but are really machine-speed assertions:
    they pass on a fast box and fail on a loaded CI runner, and they fail by
    milliseconds, which is the signature of a bad instrument rather than a bug.
    That bound flaked seven times on macOS before it was replaced -- once by
    8ms.  Peak concurrency is what the tests actually mean, and it is invariant
    to how fast the machine is: if the work serialises, the peak is 1 no matter
    how quick each unit was; if it overlaps, the peak is >1 no matter how slow.

    Usage:
        ov = OverlapTracker()
        def work():
            with ov.span():
                time.sleep(NAP)
        ...
        ov.assert_peak_at_least(2, "concurrent offload (mn)")
    """

    def __init__(self):
        self.spans = []                      # (t_enter, t_exit) per unit
        self._lock = _RealLock()

    @contextlib.contextmanager
    def span(self):
        t_in = time.monotonic()
        try:
            yield
        finally:
            t_out = time.monotonic()
            with self._lock:
                self.spans.append((t_in, t_out))

    def peak(self):
        """Max number of spans open at once (a sweep over the endpoints)."""
        events = []
        for t_in, t_out in self.spans:
            events.append((t_in, 1))
            events.append((t_out, -1))
        # close before open at equal timestamps: never credit overlap to two
        # spans that merely touched at an endpoint.
        events.sort(key=lambda e: (e[0], e[1]))
        cur = best = 0
        for _, delta in events:
            cur += delta
            if cur > best:
                best = cur
        return best

    def assert_peak_at_least(self, want, what="work"):
        got = self.peak()
        assert got >= want, (
            "{0}: peak concurrency {1}, expected >= {2} over {3} span(s) "
            "-- the work serialised (spans={4!r})"
            .format(what, got, want, len(self.spans), self.spans[:8]))
        return got


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


def ensure_fd_budget(n, what="this test"):
    """Raise the soft RLIMIT_NOFILE to cover `n` descriptors, or skip.

    macOS ships a soft limit of 256 with an effectively unlimited hard limit
    (capped by kern.maxfilesperproc, 10240 on the CI box), so a test wanting a
    few hundred socketpairs dies with EMFILE on a stock mac while passing on
    Linux, where the soft default is 1024+.  That is an environment
    assumption, not a runtime bug, and it is invisible in the failure -- the
    traceback points at socket.socketpair(), several frames from anything the
    test is actually about.

    Raising the SOFT limit toward the hard one needs no privileges; it is
    exactly what `ulimit -n` does.  Restoring is deliberately NOT attempted:
    lowering it again could break an unrelated later test in the same
    interpreter, and a higher soft limit harms nothing.
    """
    try:
        import resource
    except ImportError:                      # pragma: no cover - Windows
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= n:
        return
    want = n if hard == resource.RLIM_INFINITY else min(n, hard)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except (ValueError, OSError):
        want = soft
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < n:
        import pytest
        pytest.skip("%s needs %d fds; soft RLIMIT_NOFILE is %d and could not "
                    "be raised (hard limit %s)" % (what, n, want, hard))


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
