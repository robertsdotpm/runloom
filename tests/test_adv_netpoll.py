"""Adversarial QA: netpoll (epoll/kqueue/select) + wait_fd.

The netpoll layer carries the project's worst historical bugs: the
edge-drop "Hang A", the level-triggered register-per-direction-once arm
cache, and the fd-reuse stale-arm hang.  Backend-independent: runs on epoll
(Linux) and kqueue (macOS) alike.

A hard-won lesson is baked into these tests: the per-fd arm cache
(`runloom_fd_armed`) is **process-global and is NOT cleared when an fd is
closed** -- only `netpoll_unregister(fd)` clears it.  A raw `wait_fd` user who
closes a socket/pipe without unregistering POISONS that fd *number* for the
whole process: any later fiber that is handed the same fd number and parks on
it skips the EPOLL_CTL_ADD (register-once) and hangs.  The monkey/aio close
hooks call unregister for you; direct `wait_fd` callers must too.  Every fd
here is closed through `_drop()` (unregister-then-close) for exactly that
reason -- without it these tests are order-dependent and flaky, which is
itself the finding (see test_fd_reuse_without_unregister_should_still_wake).
"""
import os
import socket
import sys
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, assert_faster_than, needs_free_threading

READ, WRITE = 1, 2
CANCELLED = rc.WAIT_FD_CANCELLED
FT = needs_free_threading()


def _drop(fd):
    """Close an fd the way every real close path must: clear its netpoll arm
    cache first, THEN close, so the fd number is clean for reuse."""
    try:
        rc.netpoll_unregister(fd)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _drop_sock(s):
    try:
        rc.netpoll_unregister(s.fileno())
    except Exception:
        pass
    s.close()


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# basics
# --------------------------------------------------------------------------
def test_netpoll_backend_is_known():
    assert rc.netpoll_backend() in ("epoll", "kqueue", "select")


def test_wait_fd_timeout_returns_zero():
    def f():
        r, w = os.pipe()
        try:
            t0 = time.monotonic()
            rv = rc.wait_fd(r, READ, 30)
            return rv, time.monotonic() - t0
        finally:
            _drop(r); _drop(w)
    rv, el = _run_single(f)
    assert rv == 0
    assert 0.02 < el < 1.0, "timeout fired at %.3fs, expected ~0.03s" % el


def test_wait_fd_readable_and_writable():
    def f():
        a, b = socket.socketpair()
        try:
            assert rc.wait_fd(a.fileno(), WRITE, 1000) & WRITE
            def writer():
                rc.sched_yield(); rc.sched_yield()
                b.send(b"x")
            rc.go(writer)
            assert rc.wait_fd(a.fileno(), READ, 2000) & READ
            assert a.recv(1) == b"x"
            return "ok"
        finally:
            _drop_sock(a); _drop_sock(b)
    with hang_guard(15, "wait_fd r/w"):
        assert _run_single(f) == "ok"


def test_wait_fd_invalid_fd_raises_not_hangs():
    # Regression (was findings): the select backend used to FD_SET(-1) -> glibc
    # _FORTIFY_SOURCE __fdelt_chk -> process abort; and a huge fd grew the per-fd
    # parker array to fd-size (a multi-GB zero-fill that HUNG).  wait_fd now
    # rejects fd<0 (EBADF), fd>=FD_SETSIZE on select (EINVAL), and fd>=RLIMIT_NOFILE
    # (EBADF) up front -- on every backend, no abort, no hang.
    def f():
        with pytest.raises(OSError):
            rc.wait_fd(-1, READ, 1000)
        with pytest.raises(OSError):
            rc.wait_fd(2048, READ, 1000)         # out-of-range: EBADF / EINVAL
        with pytest.raises(OSError):
            rc.wait_fd(1 << 30, READ, 1000)      # huge fd: must error fast, not hang
        return "ok"
    with hang_guard(10, "wait_fd invalid fd"):
        assert _run_single(f) == "ok"


# --------------------------------------------------------------------------
# cancellation
# --------------------------------------------------------------------------
def test_netpoll_cancel_fd_wakes_with_sentinel():
    def f():
        r, w = os.pipe()
        try:
            def canceller():
                rc.sched_yield(); rc.sched_yield()
                rc.netpoll_cancel_fd(r)
            rc.go(canceller)
            return rc.wait_fd(r, READ, -1)
        finally:
            _drop(r); _drop(w)
    with hang_guard(15, "netpoll_cancel_fd"):
        assert _run_single(f) == CANCELLED


def test_g_cancel_wait_fd_wakes_target():
    res = {}
    hold = {}
    def waiter():
        r, w = os.pipe()
        hold["fds"] = (r, w)
        res["rv"] = rc.wait_fd(r, READ, -1)
    def main():
        g = rc.go(waiter)
        rc.sched_yield(); rc.sched_yield()
        res["cancel_ret"] = g.cancel_wait_fd()
    with hang_guard(15, "G.cancel_wait_fd"):
        rc.go(main)
        rc.run()
    r, w = hold["fds"]; _drop(r); _drop(w)
    assert res["cancel_ret"] is True
    assert res["rv"] == CANCELLED


# --------------------------------------------------------------------------
# fd-reuse stale-arm (the register-once hazard)
# --------------------------------------------------------------------------
def test_fd_reuse_after_unregister_is_clean():
    def f():
        r, w = os.pipe()
        rc.wait_fd(r, READ, 5)           # arm fd r in netpoll
        rc.netpoll_unregister(r)         # the close-hook's job
        os.close(r); os.close(w)
        r2, w2 = os.pipe()
        try:
            if r2 != r:
                return "skip"
            def writer():
                rc.sched_yield(); rc.sched_yield()
                os.write(w2, b"y")
            rc.go(writer)
            t0 = time.monotonic()
            rv = rc.wait_fd(r2, READ, 3000)
            return rv, time.monotonic() - t0
        finally:
            _drop(r2); _drop(w2)
    with hang_guard(15, "fd reuse after unregister"):
        out = _run_single(f)
    if out == "skip":
        pytest.skip("fd number was not reused on this run")
    rv, el = out
    assert rv & READ, "reused fd never woke (stale arm survived unregister!)"
    assert el < 1.0, "reused fd took %.3fs to wake (stale arm)" % el


def test_release_if_idle_enables_clean_reuse():
    def f():
        r, w = os.pipe()
        rc.wait_fd(r, READ, 5)
        rc.netpoll_release_if_idle(r)    # DEL + clear arm iff no g parked (it isn't)
        os.close(r); os.close(w)
        r2, w2 = os.pipe()
        try:
            if r2 != r:
                return "skip"
            def writer():
                rc.sched_yield(); rc.sched_yield()
                os.write(w2, b"y")
            rc.go(writer)
            return rc.wait_fd(r2, READ, 3000)
        finally:
            _drop(r2); _drop(w2)
    with hang_guard(15, "release_if_idle reuse"):
        out = _run_single(f)
    if out == "skip":
        pytest.skip("fd number was not reused on this run")
    assert out & READ


@pytest.mark.xfail(strict=False, reason=(
    "SHARP EDGE: the per-fd netpoll arm cache is PROCESS-GLOBAL and is not "
    "cleared when an fd is closed -- only netpoll_unregister clears it. Closing "
    "an armed fd at the raw wait_fd level WITHOUT unregister leaves the arm "
    "stale; the next time that fd NUMBER is reused and parked on, the "
    "register-once skip suppresses the EPOLL_CTL_ADD and wait_fd parks until its "
    "ceiling even though data is ready. The aio sock_* / monkey close hooks "
    "paper over this; a raw wait_fd user who forgets to unregister gets a silent "
    "hang, and a process that leaks the arm makes UNRELATED later fds on the "
    "same number hang. Encoded as the ideal so the hazard stays visible."))
def test_fd_reuse_without_unregister_should_still_wake():
    def f():
        r, w = os.pipe()
        rc.wait_fd(r, READ, 5)           # arm
        os.close(r); os.close(w)         # NO unregister -> poisons fd number r
        r2, w2 = os.pipe()
        try:
            if r2 != r:
                return "skip"
            def writer():
                rc.sched_yield(); rc.sched_yield()
                os.write(w2, b"y")
            rc.go(writer)
            t0 = time.monotonic()
            rv = rc.wait_fd(r2, READ, 1200)
            return rv, time.monotonic() - t0
        finally:
            _drop(r2); _drop(w2)
    with hang_guard(15, "fd reuse no unregister"):
        out = _run_single(f)
    if out == "skip":
        pytest.skip("fd number was not reused")
    rv, el = out
    assert rv & READ and el < 1.0, "stale arm: reused fd parked %.3fs (data ready)" % el


# --------------------------------------------------------------------------
# slow return / cooperative overlap
# --------------------------------------------------------------------------
def test_never_ready_park_does_not_block_siblings():
    progress = []
    def parker():
        r, w = os.pipe()
        try:
            rc.wait_fd(r, READ, 200)     # never-ready, 200ms ceiling
            progress.append("parker_returned")
        finally:
            _drop(r); _drop(w)
    def burner():
        for i in range(50):
            progress.append(("burn", i))
            rc.sched_yield()
    def main():
        rc.go(parker)
        rc.go(burner)
    with hang_guard(15, "slow-return overlap"):
        with assert_faster_than(1.5, "park+burn overlap"):
            rc.go(main)
            rc.run()
    burns = sum(1 for p in progress if isinstance(p, tuple))
    assert burns == 50, "burner starved by the parked fiber (%d/50)" % burns
    assert "parker_returned" in progress


# --------------------------------------------------------------------------
# wake-storm edge-drop (the "Hang A" class) -- single thread and M:N
# --------------------------------------------------------------------------
def _wake_storm(spawn, drive, n):
    pairs = [socket.socketpair() for _ in range(n)]
    woke = bytearray(n)          # one writer slot per reader -> no RMW race
    def reader(i):
        rd = pairs[i][0]
        rv = rc.wait_fd(rd.fileno(), READ, 5000)
        if rv & READ and rd.recv(1) == b"!":
            woke[i] = 1
    def main():
        for i in range(n):
            spawn(lambda i=i: reader(i))
        rc.sched_yield()         # let readers park
        for i in range(n):
            pairs[i][1].send(b"!")   # make every fd ready ~simultaneously
    drive(main)
    total = sum(woke)
    for rd, wr in pairs:
        _drop_sock(rd); _drop_sock(wr)
    return total


def test_wake_storm_single_thread():
    N = 300
    with hang_guard(40, "wake storm single-thread"):
        total = _wake_storm(rc.go, lambda m: (rc.go(m), rc.run()), N)
    assert total == N, "edge-drop: only %d/%d readers woke" % (total, N)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_wake_storm_across_mn_hubs():
    N = 400
    def drive(main):
        rc.mn_init(4)
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()
    with hang_guard(60, "wake storm M:N"):
        total = _wake_storm(rc.mn_go, drive, N)
    assert total == N, "edge-drop under M:N: only %d/%d readers woke" % (total, N)


def test_two_waiters_same_fd_distinct_directions():
    # Register-per-direction-once: a READ waiter parks while a WRITE waiter on
    # the SAME fd returns ~immediately (socketpair is writable).  The WRITE
    # waiter's completion must NOT clear the READ direction's arm.  Crucially
    # the sockets are closed only AFTER run() drains -- closing them out from
    # under the parked waiters would itself time them out (a self-inflicted
    # false failure).
    results = {}
    holder = {}
    def setup():
        a, b = socket.socketpair()
        holder["socks"] = (a, b)
        def write_waiter():
            results["w"] = bool(rc.wait_fd(a.fileno(), WRITE, 2000) & WRITE)
        def read_waiter():
            results["r"] = bool(rc.wait_fd(a.fileno(), READ, 3000) & READ)
        def sender():
            rc.sched_yield(); rc.sched_yield()
            b.send(b"z")         # makes READ ready
        rc.go(write_waiter)
        rc.go(read_waiter)
        rc.go(sender)
    with hang_guard(20, "same fd distinct directions"):
        rc.go(setup)
        rc.run()
    a, b = holder["socks"]
    _drop_sock(a); _drop_sock(b)
    assert results.get("w") is True, "WRITE waiter did not wake on a writable fd"
    assert results.get("r") is True, "READ waiter's arm was clobbered by the WRITE waiter"


# --------------------------------------------------------------------------
# the RUNLOOM_DBG_NETPOLL stale-arm tripwire (must run in a subprocess: the
# flag is read once at process start)
# --------------------------------------------------------------------------
_TRIPWIRE_SCRIPT = r'''
import sys, socket, gc; sys.path.insert(0, "src")
import runloom.monkey as monkey
monkey.patch()
import runloom_c as rc
READ = 1
out = {}
def main():
    a, b = socket.socketpair(); fd = a.fileno()
    def s1():
        rc.sched_yield(); rc.sched_yield(); b.send(b"X")
    rc.go(s1); a.recv(1)                       # arm fd
    b.close(); del a; gc.collect()            # GC-close WITHOUT unregister -> stale arm
    c, d = socket.socketpair()
    if c.fileno() != fd:
        out["skip"] = True
        rc.netpoll_unregister(c.fileno()); c.close()
        rc.netpoll_unregister(d.fileno()); d.close(); return
    def reader():
        out["rv"] = rc.wait_fd(c.fileno(), READ, 1500)
        rc.netpoll_unregister(c.fileno()); c.close()
    def late():
        for _ in range(6): rc.sched_yield()
        d.send(b"Y"); rc.netpoll_unregister(d.fileno()); d.close()
    rc.go(reader); rc.go(late)
rc.go(main); rc.run()
sys.stdout.write("SKIP\n" if out.get("skip") else ("HEALED\n" if out.get("rv") else "HUNG\n"))
'''


@pytest.mark.skipif(rc.netpoll_backend() != "epoll", reason="tripwire is epoll-only")
def test_dbg_netpoll_tripwire_detects_and_heals_stale_arm():
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, RUNLOOM_DBG_NETPOLL="1", PYTHON_GIL="0", PYTHONPATH="src")
    for _ in range(5):     # retry until the fd number actually reuses
        p = subprocess.run([sys.executable, "-c", _TRIPWIRE_SCRIPT],
                           cwd=repo, env=env, capture_output=True, text=True, timeout=30)
        if "SKIP" in p.stdout:
            continue
        assert "STALE ARM on fd" in p.stderr, (
            "tripwire did not fire on a GC-poisoned fd\nstderr=%r" % p.stderr)
        assert "HEALED" in p.stdout, (
            "tripwire fired but did not self-heal the wait\nout=%r err=%r"
            % (p.stdout, p.stderr))
        return
    pytest.skip("fd number never reused across 5 attempts")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
