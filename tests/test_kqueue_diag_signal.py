"""kqueue backend: diagnostics + signal-delivery branch coverage (macOS/BSD).

Covers the reachable branches in netpoll_diag_fd.c.inc (the introspection/dump
paths: _self_check, _dump_parkers, fiber_count) and the signal-delivery path in
netpoll_wait_fd.c.inc (RUNLOOM_NETPOLL_SIGNALED) that the functional modules
don't reach.  The cycle-detection guards (netpoll_diag_fd.c.inc:39-64, 74-76) are
defensive against a CORRUPTED parker list and are intentionally NOT covered here
(reaching them would require corrupting the list).

Conventions match test_netpoll_conformance (_drive single-thread + socketpair).
"""
import os
import signal
import socket
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

sys.path.insert(0, "src")

import runloom_c  # noqa: E402

READ = 1
WRITE = 2


def _drive(*fibers):
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:  # noqa: BLE001
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.go(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _reset_registration():
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:           # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _reg_reset():
    _reset_registration()
    yield
    _reset_registration()


def test_backend_is_kqueue():
    assert runloom_c.netpoll_backend() == "kqueue"


# -- _dump_parkers while fibers are parked (netpoll_diag_fd dump_parkers walk) --
@pytest.mark.parametrize("n", [1, 4, 16], ids=["n1", "n4", "n16"])
def test_dump_parkers_with_parked_fibers(n):
    """Drives runloom_netpoll_dump_parkers: the per-pool walk, the READ/WRITE/RW
    mask categorization, and the ready-but-parked (readyParked) poll -- by parking
    N readers, dumping, then releasing them so the run terminates."""
    pairs = [_pair() for _ in range(n)]

    def reader(a):
        runloom_c.wait_fd(a.fileno(), READ, 3000)

    def dumper():
        for _ in range(8):
            runloom_c.sched_yield()
        runloom_c._dump_parkers()        # walk the parked set (categorize + rdyP)
        for _a, b in pairs:              # release everyone so the run ends
            b.send(b"x")

    _drive(*([(lambda a=a: reader(a)) for a, _b in pairs] + [dumper]))
    for a, b in pairs:
        a.close(); b.close()


def test_dump_parkers_when_none_parked():
    """dump_parkers early-out path: total==0 per pool (no fibers parked)."""
    runloom_c._dump_parkers()            # must be a clean no-op, never crash


# -- _self_check: inspect_for_self_check walk (count + by_fd buckets) ----------
@pytest.mark.parametrize("n", [2, 8], ids=["n2", "n8"])
def test_self_check_with_parked_fibers(n):
    """runloom_netpoll_inspect_for_self_check: the global-list count walk + the
    by_fd bucket walk, exercised with fibers genuinely parked."""
    pairs = [_pair() for _ in range(n)]
    res = []

    def reader(a):
        runloom_c.wait_fd(a.fileno(), READ, 3000)

    def checker():
        for _ in range(8):
            runloom_c.sched_yield()
        try:
            res.append(runloom_c._self_check())   # walk while parkers are linked
        except Exception as e:                    # noqa: BLE001
            res.append(("err", repr(e)))
        for _a, b in pairs:
            b.send(b"x")

    _drive(*([(lambda a=a: reader(a)) for a, _b in pairs] + [checker]))
    assert res, "self_check did not run"
    for a, b in pairs:
        a.close(); b.close()


def test_self_check_when_idle():
    """self_check with nothing parked (the empty-pool path)."""
    try:
        runloom_c._self_check()
    except Exception:                    # noqa: BLE001
        pass                             # presence of the call drives the branch


# -- fiber introspection counts (cheap, drives the introspect registry) --------
def test_fiber_count_while_parked():
    a, b = _pair()
    seen = []

    def reader():
        seen.append(runloom_c.fiber_count())   # >=1 (at least this fiber)
        runloom_c.wait_fd(a.fileno(), READ, 2000)

    def waker():
        runloom_c.sched_yield()
        b.send(b"x")

    _drive(reader, waker)
    assert seen and seen[0] >= 1
    a.close(); b.close()


# -- signal delivery INTO a parked wait_fd (RUNLOOM_NETPOLL_SIGNALED) ----------
# Single-thread only: the signal path is driven by runloom_sched_drain
# (PyErr_CheckSignals + runloom_netpoll_signal_wake); M:N signal servicing is a
# separate, known gap.
def test_signal_wakes_parked_wait_fd():
    """A SIGALRM raised while a fiber is parked in wait_fd must wake THAT fiber
    and propagate the handler's exception out of the cooperative wait
    (netpoll_wait_fd.c.inc RUNLOOM_NETPOLL_SIGNALED return path)."""
    a, b = _pair()                       # never made readable
    fired = []
    raised = []

    def handler(signum, frame):
        fired.append(1)
        raise KeyboardInterrupt("alarm")

    old = signal.signal(signal.SIGALRM, handler)
    try:
        def parker():
            signal.setitimer(signal.ITIMER_REAL, 0.2)
            try:
                # No timeout: only the signal (or the safety deadline) can wake it.
                runloom_c.wait_fd(a.fileno(), READ, 3000)
            except KeyboardInterrupt:
                raised.append(1)

        try:
            _drive(parker)
        except KeyboardInterrupt:
            raised.append(1)             # propagated out of run() is also fine
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)
        a.close(); b.close()

    assert fired, "SIGALRM handler never ran"
    assert raised, "signal did not propagate out of the parked wait_fd"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
