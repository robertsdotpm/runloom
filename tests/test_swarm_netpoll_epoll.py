"""Adversarial swarm: the Linux default backend -- epoll netpoll + io_uring file
I/O + TCPConn / serve.

This file goes DEEPER than tests/test_adv_netpoll.py + tests/test_cov_netpoll.py
(which already cover the basics: backend name, timeout, r/w, invalid-fd reject,
cancel_fd/cancel_wait_fd, fd-reuse after-unregister, the DBG tripwire, the
register MOD-widen, the EPOLLHUP/ERR fold, a 300/400 wake-storm, a 40-deadline
heap, and the io_uring-eventfd drain).  Here the focus is the conditions that
actually break a lock-free netpoll under free-threaded 3.13t:

  - the process-global arm-cache + fd-number-reuse poison, exercised through BOTH
    a raw close-without-unregister AND a GC'd-socket close, with the
    RUNLOOM_DBG_NETPOLL tripwire self-heal asserted in a subprocess;
  - EPOLLERR/EPOLLHUP folding into BOTH directions (a peer RST must wake a WRITE
    waiter, a half-close must wake a READ waiter) -- not just "something woke";
  - register MOD-widen on a single live fd in BOTH orders (READ-then-WRITE and
    WRITE-then-READ), asserting neither direction's arm is clobbered;
  - a wake-storm of HUNDREDS of simultaneously-ready fds, single-thread AND
    across M:N hubs, checked by SET EQUALITY of the payload each reader saw
    (so a reorder / cross-wire / dropped-edge surfaces as wrong data, not just
    a count);
  - argument-validation edge values (negative / >=RLIMIT_NOFILE / huge fd) that
    must raise OSError and NEVER abort or hang -- proven in a subprocess so an
    abort is contained as a signal, not a wedged suite;
  - the deadline heap with many staggered timeouts whose RELATIVE firing ORDER
    is asserted (earliest deadline first), interleaved with a ready fd;
  - netpoll FAULT injection (FD_READ / FD_WRITE EAGAIN-park-recover AND a hard
    errno; TCP_RECV / TCP_SEND / TCP_ACCEPT / TCP_CONNECT / TCP_SOCKET) mid
    workload, asserting a clean Python error or a recovered result, never a crash;
  - a signal (SIGALRM) delivered INTO a fiber parked in wait_fd / tcp_recv,
    which must raise out of the cooperative call through that fiber's stack;
  - slow-return: a never-ready park must not starve siblings (assert_faster_than);
  - the io_uring global-ring eventfd drain under CONCURRENT file I/O and under
    RUNLOOM_IOURING_LOOP=1 (subprocess);
  - TCPConn connection-refused / EOF / large framed transfer / many concurrent
    connections, single-thread AND M:N; serve() M:N echo + its single-thread
    refusal.

Every fd parked-on at the raw wait_fd level is closed through _drop()
(unregister-then-close) so it does not poison its fd NUMBER for a later test --
that staleness is itself a documented hazard (the without-unregister case is
encoded as a dedicated subprocess test, not leaked into the rest of the file).
"""
import os
import socket
import struct
import subprocess
import sys
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      needs_free_threading)

READ, WRITE = 1, 2
CANCELLED = rc.WAIT_FD_CANCELLED
FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

pytestmark = pytest.mark.skipif(rc.netpoll_backend() != "epoll",
                                reason="epoll-backend (Linux default) coverage")


# --------------------------------------------------------------------------
# fd hygiene helpers -- every raw park must unregister before close so the
# process-global arm cache never poisons a later test's reused fd number.
# --------------------------------------------------------------------------
def _drop(fd):
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
    try:
        s.close()
    except OSError:
        pass


def _run_single(fn):
    box = {}

    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


def _subproc(script, env_extra=None, timeout=40):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_GOROUTINE_PANIC="silent")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _assert_no_signal_crash(p, label):
    # A negative returncode is a signal (SIGSEGV/SIGABRT/...): the runtime
    # crashed.  Contained here as an observable, not a dead suite.
    assert p.returncode is None or p.returncode >= 0, (
        "%s crashed with signal %d\nstdout=%r\nstderr=%r"
        % (label, -p.returncode, p.stdout, p.stderr[-2000:]))


# ==========================================================================
# 1. argument-validation edge values -- must raise OSError, never abort/hang.
#    Run in a subprocess so a FD_SET(-1) glibc abort or a multi-GB array
#    zero-fill hang is CONTAINED as a signal/timeout, not a wedged suite.
# ==========================================================================
_FD_VALIDATION_SCRIPT = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
RLIMIT = __import__("resource").getrlimit(__import__("resource").RLIMIT_NOFILE)
hard = RLIMIT[1]
bad = []
def main():
    cases = [-1, -2, 1 << 30, 1 << 24]
    if hard not in (-1,) and hard < (1 << 28):
        cases.append(int(hard))        # exactly at the hard limit
        cases.append(int(hard) + 5)    # above it
    for fd in cases:
        try:
            rc.wait_fd(fd, 1, 100)
            bad.append(("no-raise", fd))
        except OSError:
            pass
        except Exception as e:
            bad.append(("wrong-exc", fd, type(e).__name__))
rc.go(main); rc.run()
sys.stdout.write("BAD=%r\n" % bad if bad else "ALL_RAISED\n")
'''


def test_fd_validation_edges_raise_not_abort_subprocess():
    p = _subproc(_FD_VALIDATION_SCRIPT, timeout=30)
    _assert_no_signal_crash(p, "fd-validation")
    assert "ALL_RAISED" in p.stdout, (
        "an out-of-range fd did not raise OSError cleanly: %r / %r"
        % (p.stdout, p.stderr[-800:]))


def test_wait_fd_small_fd_numbers_do_not_corrupt():
    # Small fd numbers (the low end of the per-fd parker/arm arrays) must index
    # cleanly -- no off-by-one corruption at fd 0/1/2/3.  Build pipes, dup their
    # write ends down to small numbers, and WRITE-wait (a pipe write end is
    # writable -> immediate WRITE), never an abort or a hang.
    def f():
        out = {}
        made = []
        try:
            for _ in range(4):
                r, w = os.pipe()
                made.append((r, w))
                out[w] = rc.wait_fd(w, WRITE, 200)   # pipe write end is writable
        finally:
            for r, w in made:
                _drop(r); _drop(w)
        return out
    with hang_guard(10, "small fds"):
        out = _run_single(f)
    assert all(v & WRITE for v in out.values()), "a small-numbered fd lost its WRITE arm: %r" % out


# ==========================================================================
# 2. EPOLLERR / EPOLLHUP folding into BOTH directions.
#    The pump folds ERR/HUP into READ *and* WRITE (netpoll_pump.c.inc): a peer
#    RST must wake a WRITE-waiter, a write-end close must wake a READ-waiter.
# ==========================================================================
def test_epoll_hup_wakes_a_write_waiter():
    # Closing the peer of a connected socket -> EPOLLHUP, which the pump folds
    # into WRITE as well as READ.  A fiber parked WRITE-only must still wake.
    res = {}
    hold = {}

    def write_waiter():
        # Fill the send buffer so the socket is NOT trivially writable, forcing
        # a real park; then the peer-HUP is what must wake it.
        a = hold["a"]
        try:
            while True:
                a.send(b"\0" * 65536)
        except (BlockingIOError, OSError):
            pass
        res["rv"] = rc.wait_fd(a.fileno(), WRITE, 3000)

    def closer():
        rc.sched_yield(); rc.sched_yield(); rc.sched_yield()
        b = hold["b"]
        # RST so the local send-side reports HUP/ERR promptly.
        b.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                     struct.pack("ii", 1, 0))
        b.close()

    a, b = socket.socketpair()
    a.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(20, "hup wakes write waiter"):
        rc.go(write_waiter); rc.go(closer); rc.run()
    _drop_sock(a)
    assert res.get("rv", 0) != 0, "peer HUP/RST did not wake the WRITE waiter"


def test_epoll_half_close_read_then_drain():
    # shutdown(SHUT_WR) on the peer gives our read end EPOLLIN+EPOLLRDHUP; the
    # reader must wake, read the queued bytes, then see EOF -- not a lost wake.
    res = {}
    hold = {}

    def reader():
        a = hold["a"]
        rv = rc.wait_fd(a.fileno(), READ, 3000)
        res["rv"] = rv
        got = b""
        while True:
            try:
                chunk = a.recv(64)
            except BlockingIOError:
                break
            if not chunk:
                res["eof"] = True
                break
            got += chunk
        res["got"] = got

    def peer():
        rc.sched_yield(); rc.sched_yield()
        b = hold["b"]
        b.send(b"tail-bytes")
        b.shutdown(socket.SHUT_WR)

    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(20, "half-close read"):
        rc.go(reader); rc.go(peer); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert res.get("rv", 0) & READ, "RDHUP did not wake the reader"
    assert res.get("got") == b"tail-bytes", "wrong data after half-close: %r" % res.get("got")
    assert res.get("eof") is True, "EOF not observed after draining"


# ==========================================================================
# 3. register MOD-widen -- both orders, neither arm clobbered.
#    cov already does WRITE-then-READ; add READ-then-WRITE, and assert the
#    *value* each direction saw (not just "woke").
# ==========================================================================
@pytest.mark.parametrize("first", ["read_first", "write_first"])
def test_mod_widen_preserves_both_arms(first):
    res = {}
    hold = {}

    def read_waiter():
        res["r"] = rc.wait_fd(hold["fd"], READ, 3000)

    def write_waiter():
        res["w"] = rc.wait_fd(hold["fd"], WRITE, 2000)

    def sender():
        rc.sched_yield(); rc.sched_yield(); rc.sched_yield()
        hold["b"].send(b"Q")

    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    hold["fd"], hold["b"] = a.fileno(), b

    def main():
        # Spawn deterministically: the FIRST direction's wait_fd ADDs the fd;
        # the second yields then widens it to IN|OUT via EPOLL_CTL_MOD.  Neither
        # direction's arm may be clobbered by the other's register/completion.
        if first == "read_first":
            rc.go(read_waiter)
            rc.sched_yield()
            rc.go(write_waiter)
        else:
            rc.go(write_waiter)
            rc.sched_yield()
            rc.go(read_waiter)
        rc.go(sender)
    with hang_guard(20, "mod widen " + first):
        rc.go(main); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert res.get("w", 0) & WRITE, "WRITE arm lost under MOD-widen (%s)" % first
    assert res.get("r", 0) & READ, "READ arm clobbered by MOD-widen (%s)" % first


# ==========================================================================
# 4. wake-storm by SET EQUALITY -- a reorder / cross-wire / edge-drop shows up
#    as wrong data, not just a missing count.  Single-thread + M:N.
# ==========================================================================
def _wake_storm_payload(spawn, drive, n):
    """Each writer sends a DISTINCT 4-byte payload; each reader records what it
    actually received into its own slot.  Correct == the multiset of received
    payloads equals the multiset sent.  A cross-wired wake (reader i woken for
    fd j) or a dropped edge corrupts that set."""
    pairs = [socket.socketpair() for _ in range(n)]
    for a, b in pairs:
        a.setblocking(False); b.setblocking(False)
    got = [None] * n          # one writer slot per reader -> no RMW race

    def reader(i):
        rd = pairs[i][0]
        # clean-reuse: a fresh socket can land on a previously-used (and possibly
        # stale-armed) fd NUMBER in this long-lived process; clear any sticky arm
        # first so we measure edge-drop, not the separate fd-poison hazard.
        rc.netpoll_release_if_idle(rd.fileno())
        rv = rc.wait_fd(rd.fileno(), READ, 8000)
        if rv & READ:
            try:
                got[i] = rd.recv(4)
            except BlockingIOError:
                got[i] = b"EAGAIN"
        else:
            got[i] = b"TMOUT"

    def main():
        for i in range(n):
            spawn(lambda i=i: reader(i))
        rc.sched_yield()
        for i in range(n):
            pairs[i][1].send(struct.pack(">I", i))   # distinct per fd
    drive(main)
    expected = {struct.pack(">I", i) for i in range(n)}
    received = {g for g in got if g is not None}
    for a, b in pairs:
        _drop_sock(a); _drop_sock(b)
    return expected, received, got


def test_wake_storm_set_equality_single_thread():
    N = 350
    with hang_guard(40, "wake storm set-eq single"):
        expected, received, got = _wake_storm_payload(
            rc.go, lambda m: (rc.go(m), rc.run()), N)
    missing = expected - received
    assert not missing, "%d readers got wrong/dropped payload: missing %r" % (
        len(missing), list(missing)[:5])
    # each reader saw exactly its OWN fd's payload (no cross-wire)
    for i, g in enumerate(got):
        assert g == struct.pack(">I", i), "reader %d cross-wired: saw %r" % (i, g)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_wake_storm_set_equality_across_mn_hubs():
    N = 450

    def drive(main):
        rc.mn_init(4)
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()
    with hang_guard(60, "wake storm set-eq M:N"):
        expected, received, got = _wake_storm_payload(rc.mn_go, drive, N)
    missing = expected - received
    assert not missing, "M:N edge-drop/cross-wire: missing %d payloads %r" % (
        len(missing), list(missing)[:5])
    for i, g in enumerate(got):
        assert g == struct.pack(">I", i), "M:N reader %d cross-wired: saw %r" % (i, g)


# ==========================================================================
# 5. deadline heap -- RELATIVE firing order, interleaved with a ready fd.
# ==========================================================================
def test_deadline_heap_fires_in_order():
    # Many staggered deadlines: assert the order they FIRE is non-decreasing in
    # their deadline (the heap must pop earliest-first), and that a ready fd
    # mixed in wakes immediately rather than waiting on its own (huge) deadline.
    N = 30
    order = []          # (deadline_ms, monotonic_at_fire)
    pipes = [os.pipe() for _ in range(N)]

    def waiter(i, ms):
        rc.wait_fd(pipes[i][0], READ, ms)
        order.append((ms, time.monotonic()))

    ready = {}

    def ready_waiter():
        # this fd is made ready ~immediately; its 100000ms deadline must NOT
        # gate it -- it must wake on the data, proving the heap isn't blocking
        # the pump on the far-future min-deadline.
        a, b = socket.socketpair(); a.setblocking(False)
        ready["socks"] = (a, b)
        t0 = time.monotonic()
        rv = rc.wait_fd(a.fileno(), READ, 100000)
        ready["dt"] = time.monotonic() - t0
        ready["rv"] = rv

    def feeder():
        rc.sched_yield(); rc.sched_yield()
        ready["socks"][1].send(b"x")

    def main():
        # deadlines from 20ms..(20+N*6)ms, spawned in REVERSE so spawn order
        # != deadline order (the heap, not insertion order, must dominate).
        for i in reversed(range(N)):
            rc.go(lambda i=i: waiter(i, 20 + i * 6))
        rc.go(ready_waiter)
        rc.go(feeder)
    with hang_guard(20, "deadline order"):
        rc.go(main); rc.run()
    a, b = ready["socks"]; _drop_sock(a); _drop_sock(b)
    for r, w in pipes:
        _drop(r); os.close(w)
    assert len(order) == N, "%d/%d deadlines fired" % (len(order), N)
    # Sort by fire-time; the deadlines must come out monotonically non-decreasing.
    by_fire = [ms for ms, _ in sorted(order, key=lambda t: t[1])]
    inversions = sum(1 for i in range(1, len(by_fire)) if by_fire[i] < by_fire[i - 1] - 10)
    assert inversions == 0, "deadline heap fired out of order: %r" % by_fire
    assert ready.get("rv", 0) & READ, "ready fd did not wake on data"
    assert ready.get("dt", 99) < 1.0, (
        "ready fd waited %.3fs -- pump gated on the far min-deadline" % ready.get("dt", 99))


# ==========================================================================
# 6. fd-number-reuse poison: raw close-without-unregister AND GC'd-socket.
#    Both in subprocesses (the poison is process-global; isolate it).  The
#    DBG tripwire variant asserts the self-heal.
# ==========================================================================
_RAW_POISON_SCRIPT = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
READ = 1
out = {}
def main():
    r, w = os.pipe()
    rc.wait_fd(r, READ, 5)            # arm fd number `r`
    os.close(r); os.close(w)          # NO unregister -> poisons the number
    r2, w2 = os.pipe()
    if r2 != r:
        out["skip"] = True
        rc.netpoll_unregister(r2); os.close(r2); os.close(w2); return
    def writer():
        rc.sched_yield(); rc.sched_yield(); os.write(w2, b"y")
    rc.go(writer)
    out["rv"] = rc.wait_fd(r2, READ, 1200)   # bounded so a stale arm = timeout 0
    rc.netpoll_unregister(r2); os.close(r2); os.close(w2)
rc.go(main); rc.run()
if out.get("skip"): sys.stdout.write("SKIP\n")
elif out.get("rv"): sys.stdout.write("WOKE\n")
else: sys.stdout.write("HUNG\n")
'''


def test_raw_close_without_unregister_poisons_fd_subprocess():
    # FINDING (known SHARP EDGE, mirrors test_adv's xfail): a raw wait_fd user
    # who closes WITHOUT unregister leaves the global arm cache stale, so the
    # next fiber handed that fd NUMBER skips EPOLL_CTL_ADD and parks until its
    # ceiling even though data is ready.  We assert the CURRENT (buggy) behavior:
    # the reused fd times out to 0 ("HUNG") rather than waking.  Contained in a
    # subprocess (the poison must not leak into the rest of the suite); bounded
    # timeout so it never actually hangs.  No crash/abort either way.
    for _ in range(8):
        p = _subproc(_RAW_POISON_SCRIPT, timeout=20)
        _assert_no_signal_crash(p, "raw-poison")
        if "SKIP" in p.stdout:
            continue
        # The hazard reproduces deterministically on the first reuse: the reused
        # fd does NOT wake (the stale arm suppressed the re-ADD).
        assert "HUNG" in p.stdout, (
            "stale-arm poison did NOT reproduce (got %r); if the runtime now "
            "self-heals this raw path that is a *fix*, update this assertion"
            % p.stdout)
        return
    pytest.skip("fd number never reused across 8 attempts")


@pytest.mark.skipif(rc.netpoll_backend() != "epoll", reason="tripwire is epoll-only")
def test_dbg_netpoll_tripwire_heals_gc_poison_subprocess():
    # RUNLOOM_DBG_NETPOLL turns the silent stale-arm hang into a loud,
    # self-healing diagnostic: the register skip validates with EPOLL_CTL_MOD,
    # sees ENOENT, warns, and re-ADDs.  Drive a GC-closed socket (bypasses the
    # monkey close hook) so the arm goes stale, then prove the wait HEALS.
    script = r'''
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
    rc.go(s1); a.recv(1)
    b.close(); del a; gc.collect()        # GC-close WITHOUT unregister
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
    for _ in range(6):
        p = _subproc(script, env_extra={"RUNLOOM_DBG_NETPOLL": "1"}, timeout=30)
        _assert_no_signal_crash(p, "dbg-tripwire")
        if "SKIP" in p.stdout:
            continue
        assert "STALE ARM on fd" in p.stderr, (
            "tripwire did not fire on a GC-poisoned fd\nstderr=%r" % p.stderr[-1500:])
        assert "HEALED" in p.stdout, (
            "tripwire fired but did not self-heal\nout=%r err=%r"
            % (p.stdout, p.stderr[-1500:]))
        return
    pytest.skip("fd number never reused across 6 attempts")


# ==========================================================================
# 7. fault injection mid-workload -- clean error or recovery, never a crash.
# ==========================================================================
def test_fault_fd_read_eagain_parks_and_recovers():
    # once:EAGAIN forces the first read() to "fail" EAGAIN -> park -> the writer
    # makes it ready -> recover and read the data.  Exercises the wait_fd park
    # path under fault injection rather than a trivially-ready read.
    script = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
out = {}
def main():
    r, w = os.pipe()
    def writer():
        # write, then let main's fd_read be the only runnable thing -> the
        # scheduler goes idle and pumps the netpoll (a sched_yield spin here
        # would starve the pump; that is by design, see netpoll_poll docs).
        rc.sched_yield()
        os.write(w, b"abc")
    rc.go(writer)
    buf = bytearray(3)
    n = rc.fd_read(r, buf, 3)       # injected EAGAIN -> park -> wake -> read
    out["n"] = n; out["buf"] = bytes(buf)
    rc.netpoll_unregister(r); os.close(r); os.close(w)
rc.go(main); rc.run()
sys.stdout.write("OK %d %r\n" % (out.get("n", -1), out.get("buf")))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_FD_READ": "once:11"}, timeout=20)
    _assert_no_signal_crash(p, "fault fd_read eagain")
    assert "OK 3 b'abc'" in p.stdout, (p.stdout, p.stderr[-800:])


def test_fault_fd_write_eagain_parks_and_recovers():
    # Inject EAGAIN-once on the first write() so fd_write parks on WRITE; the
    # pipe write end is writable, so the park wakes immediately and the write
    # completes.  Exercises the wait_fd(WRITE) park path under fault injection
    # (a trivially-ready write would skip the park).  No crash, all bytes written.
    script = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
out = {}
def main():
    r, w = os.pipe()
    n = rc.fd_write(w, b"Z" * 256)   # injected EAGAIN -> park WRITE -> wake -> write
    out["n"] = n
    # drain so nothing is left dangling; then unregister + close cleanly
    buf = bytearray(256)
    os.set_blocking(r, False)
    try:
        os.read(r, 256)
    except BlockingIOError:
        pass
    rc.netpoll_unregister(r); os.close(r)
    rc.netpoll_unregister(w); os.close(w)
rc.go(main); rc.run()
sys.stdout.write("OK %d\n" % out.get("n", -1))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_FD_WRITE": "once:11"}, timeout=20)
    _assert_no_signal_crash(p, "fault fd_write eagain")
    assert "OK 256" in p.stdout, (p.stdout, p.stderr[-800:])


def test_fault_fd_read_hard_errno_raises_clean():
    # A hard errno (EIO) is not EAGAIN/EINTR -> must surface as OSError, no crash.
    script = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
out = {}
def main():
    r, w = os.pipe(); os.write(w, b"abc"); buf = bytearray(3)
    try:
        rc.fd_read(r, buf, 3)
        out["res"] = "no-raise"
    except OSError as e:
        out["res"] = ("OSError", e.errno)
    rc.netpoll_unregister(r); os.close(r); os.close(w)
rc.go(main); rc.run()
sys.stdout.write("RES=%r\n" % (out.get("res"),))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_FD_READ": "always:5"}, timeout=20)
    _assert_no_signal_crash(p, "fault fd_read hard")
    assert "RES=('OSError', 5)" in p.stdout, (p.stdout, p.stderr[-800:])


@pytest.mark.parametrize("site,errno_code", [
    ("RUNLOOM_FAULT_TCP_RECV", 104),    # ECONNRESET
    ("RUNLOOM_FAULT_TCP_SEND", 32),     # EPIPE
    ("RUNLOOM_FAULT_TCP_SOCKET", 24),   # EMFILE
    ("RUNLOOM_FAULT_TCP_ACCEPT", 104),
    ("RUNLOOM_FAULT_TCP_CONNECT", 111), # ECONNREFUSED
])
def test_fault_tcp_sites_no_crash(site, errno_code):
    # Drive a TCPConn echo round-trip under each TCP_* fault (forced onto the
    # readiness path so the inject site is reached -- iouring_choice=-1 path).
    # The result is allowed to be a clean OSError OR a recovered round-trip; the
    # ONLY hard requirement is no segfault/abort.
    script = r'''
import sys, os, socket; sys.path.insert(0, "src")
import runloom_c as rc
out = {"err": None, "echo": None}
def main():
    lstn = rc.TCPConn.listen("127.0.0.1", 0, 64, 0)
    # discover bound port via getsockname on a dup of the raw fd
    ss = socket.socket(fileno=os.dup(lstn.fileno()))
    bound = ss.getsockname()[1]; ss.detach()
    def server():
        try:
            conn = lstn.accept()
            data = conn.recv(16)
            conn.send_all(data)
            conn.close()
        except OSError as e:
            out["err"] = ("server", e.errno)
        lstn.close()
    def client():
        rc.sched_yield()
        try:
            c = rc.TCPConn.connect("127.0.0.1", bound)
            c.send_all(b"ping")
            out["echo"] = c.recv(4)
            c.close()
        except OSError as e:
            out["err"] = ("client", e.errno)
    rc.go(server); rc.go(client)
rc.go(main); rc.run()
sys.stdout.write("DONE err=%r echo=%r\n" % (out["err"], out["echo"]))
'''
    p = _subproc(script, env_extra={site: "once:%d" % errno_code}, timeout=25)
    _assert_no_signal_crash(p, "fault " + site)
    assert "DONE" in p.stdout, (
        "%s: workload did not complete cleanly: %r / %r"
        % (site, p.stdout, p.stderr[-800:]))


# ==========================================================================
# 8. signal (SIGALRM) into a fiber parked in wait_fd / tcp_recv.
# ==========================================================================
def test_signal_interrupts_parked_wait_fd():
    # An alarm handler that raises must propagate out of the cooperative wait_fd
    # through the parked fiber's own stack (CLAUDE.md "signals deliver INTO the
    # parked goroutine").  Subprocess: setitimer is process-global.
    script = r'''
import sys, os, signal; sys.path.insert(0, "src")
import runloom_c as rc
out = {}
class Boom(Exception): pass
def handler(signum, frame): raise Boom()
signal.signal(signal.SIGALRM, handler)
def main():
    r, w = os.pipe()
    signal.setitimer(signal.ITIMER_REAL, 0.1)
    try:
        rc.wait_fd(r, 1, 5000)       # park 5s; alarm at 0.1s must interrupt
        out["res"] = "no-raise"
    except Boom:
        out["res"] = "boom"
    except BaseException as e:
        out["res"] = ("other", type(e).__name__)
    rc.netpoll_unregister(r); os.close(r); os.close(w)
rc.go(main); rc.run()
sys.stdout.write("RES=%s\n" % (out.get("res"),))
'''
    p = _subproc(script, timeout=20)
    _assert_no_signal_crash(p, "signal wait_fd")
    assert "RES=boom" in p.stdout, (
        "alarm did not interrupt the parked wait_fd: %r / %r"
        % (p.stdout, p.stderr[-800:]))


def test_signal_interrupts_parked_tcp_recv():
    # Same, through tcp_recv's wait_fd: a SIGALRM during a never-arriving recv
    # raises out of the call, not swallowed, not carried out of run().
    script = r'''
import sys, os, socket, signal; sys.path.insert(0, "src")
import runloom_c as rc
out = {}
class Boom(Exception): pass
def handler(signum, frame): raise Boom()
signal.signal(signal.SIGALRM, handler)
def main():
    a, b = socket.socketpair(); a.setblocking(False)
    signal.setitimer(signal.ITIMER_REAL, 0.1)
    buf = bytearray(8)
    try:
        rc.tcp_recv(a.fileno(), buf, 8)   # nothing sent -> park forever
        out["res"] = "no-raise"
    except Boom:
        out["res"] = "boom"
    except BaseException as e:
        out["res"] = ("other", type(e).__name__)
    rc.netpoll_unregister(a.fileno()); a.close()
    rc.netpoll_unregister(b.fileno()); b.close()
rc.go(main); rc.run()
sys.stdout.write("RES=%s\n" % (out.get("res"),))
'''
    p = _subproc(script, timeout=20)
    _assert_no_signal_crash(p, "signal tcp_recv")
    assert "RES=boom" in p.stdout, (p.stdout, p.stderr[-800:])


# ==========================================================================
# 9. slow-return: a never-ready park must not starve siblings.
# ==========================================================================
def test_many_never_ready_parks_dont_starve_burner():
    # 60 fibers parked forever (bounded by a 300ms ceiling); a burner does 200
    # yields.  If the parked fibers serialized the scheduler, the burner would
    # be gated to the 300ms ceiling per park (>>1.5s).  Cooperative overlap must
    # keep the whole thing well under the ceiling.
    progress = {"burns": 0, "returned": 0}
    holders = []

    def parker(i):
        r, w = os.pipe()
        holders.append((r, w))
        rc.wait_fd(r, READ, 300)
        progress["returned"] += 1     # single-thread: no RMW race here

    def burner():
        for _ in range(200):
            progress["burns"] += 1
            rc.sched_yield()

    def main():
        for i in range(60):
            rc.go(lambda i=i: parker(i))
        rc.go(burner)
    with hang_guard(15, "no-starve overlap"):
        with assert_faster_than(2.0, "60 parks + 200 burns overlap"):
            rc.go(main); rc.run()
    for r, w in holders:
        _drop(r); os.close(w)
    assert progress["burns"] == 200, "burner starved (%d/200)" % progress["burns"]
    assert progress["returned"] == 60, "%d/60 parkers returned" % progress["returned"]


def test_ready_fd_returns_fast_amid_idle_parkers():
    # One fd is made ready immediately while many siblings park on a far
    # deadline.  The ready fiber must return promptly (the pump must not gate on
    # the idle parkers' far min-deadline).  After it returns, cancel the idle
    # parkers so run() can drain (otherwise run() waits out their ceiling).
    res = {}
    holders = []     # idle pipe read fds, so quick() can cancel them

    def idle(i):
        r, w = os.pipe()
        holders.append((r, w))
        rc.wait_fd(r, READ, 60000)     # far deadline; cancelled below

    def quick():
        a, b = socket.socketpair(); a.setblocking(False)
        res["socks"] = (a, b)

        def feeder():
            rc.sched_yield(); rc.sched_yield()
            b.send(b"!")
        rc.go(feeder)
        # let the idle fibers park first so their deadlines are live in the heap
        for _ in range(6):
            rc.sched_yield()
        t0 = time.monotonic()
        rv = rc.wait_fd(a.fileno(), READ, 60000)
        res["dt"] = time.monotonic() - t0
        res["rv"] = rv
        # release the idle parkers so run() can finish
        for r, w in holders:
            rc.netpoll_cancel_fd(r)

    def main():
        for i in range(40):
            rc.go(lambda i=i: idle(i))
        rc.go(quick)
    with hang_guard(15, "quick amid idle"):
        with assert_faster_than(3.0, "ready fd return amid idle parkers"):
            rc.go(main); rc.run()
    a, b = res["socks"]; _drop_sock(a); _drop_sock(b)
    for r, w in holders:
        _drop(r); os.close(w)
    assert res.get("rv", 0) & READ
    assert res.get("dt", 99) < 1.0, "ready fd took %.3fs amid idle parkers" % res.get("dt", 99)


# ==========================================================================
# 10. io_uring global-ring eventfd drain -- concurrent file I/O + loop mode.
# ==========================================================================
@pytest.mark.skipif(not rc.iouring_available(), reason="io_uring not available")
def test_iouring_concurrent_file_io_drains_eventfd():
    import tempfile
    N = 24
    ok = bytearray(N)

    def one(i):
        fd, path = tempfile.mkstemp()
        try:
            payload = bytes((i * 7 + k) & 0xFF for k in range(8192))
            rc.file_write(fd, payload, 0)
            buf = bytearray(8192)
            n = rc.file_read(fd, buf, 8192, 0)
            if n == 8192 and bytes(buf) == payload:
                ok[i] = 1
        finally:
            os.close(fd); os.unlink(path)

    def main():
        for i in range(N):
            rc.go(lambda i=i: one(i))
    with hang_guard(40, "iouring concurrent file io"):
        rc.go(main); rc.run()
    assert sum(ok) == N, "%d/%d file round-trips ok (eventfd drain lost a CQE?)" % (sum(ok), N)


@pytest.mark.skipif(not (FT and rc.iouring_available()),
                    reason="io_uring loop mode needs M:N + io_uring")
def test_iouring_loop_mode_file_io_subprocess():
    # RUNLOOM_IOURING_LOOP=1: file_read parks on the global ring whose eventfd is
    # EPOLLEXCLUSIVE in the shared epoll (the documented hang hazard -- the loop
    # idle path must drain the global ring after loop_wait).  Bounded; assert it
    # completes, no hang.
    script = r'''
import sys, os, tempfile; sys.path.insert(0, "src")
import runloom
import runloom_c as rc
ok = [0]
def main():
    for i in range(12):
        def one(i=i):
            fd, path = tempfile.mkstemp()
            try:
                rc.file_write(fd, b"u" * 4096, 0)
                buf = bytearray(4096)
                if rc.file_read(fd, buf, 4096, 0) == 4096:
                    ok[0] += 1
            finally:
                os.close(fd); os.unlink(path)
        rc.mn_go(one)
runloom.run(3, main)
sys.stdout.write("LOOP_OK %d\n" % ok[0])
'''
    p = _subproc(script, env_extra={"RUNLOOM_IOURING_LOOP": "1"}, timeout=40)
    _assert_no_signal_crash(p, "iouring loop")
    assert "LOOP_OK 12" in p.stdout, (
        "io_uring loop mode lost a file completion / hung: %r / %r"
        % (p.stdout, p.stderr[-1000:]))


# ==========================================================================
# 11. TCPConn -- refused / EOF / large framed transfer / many conns.
# ==========================================================================
def test_tcpconn_connection_refused_raises():
    def f():
        s = socket.socket(); s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()                      # nothing listening on `port`
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.close()
            return "connected?!"
        except OSError as e:
            return ("refused", e.errno)
    with hang_guard(15, "tcpconn refused"):
        out = _run_single(f)
    assert isinstance(out, tuple) and out[0] == "refused", "expected refusal, got %r" % (out,)
    assert out[1] in (111, 113, 110), "unexpected errno on refusal: %r" % (out[1],)


def test_tcpconn_eof_returns_empty():
    res = {}
    hold = {}

    def conn_side():
        a = hold["a"]
        c = rc.TCPConn(a.fileno())
        res["eof"] = c.recv(64)        # peer closed -> b""
        # do NOT c.close(): it owns a.fileno(); unregister handled in teardown

    def peer():
        rc.sched_yield(); rc.sched_yield()
        hold["b"].close()

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(15, "tcpconn eof"):
        rc.go(conn_side); rc.go(peer); rc.run()
    rc.netpoll_unregister(a.fileno())   # a.fileno() owned by the TCPConn (now GC'd)
    assert res.get("eof") == b"", "EOF did not return empty bytes: %r" % res.get("eof")


def test_tcpconn_large_framed_transfer():
    # A 4 MB framed transfer over a loopback TCPConn pair: send_all on one side,
    # recv_into in a loop on the other; assert byte-exactness (no short-read
    # corruption, no lost wake mid-stream).
    SIZE = 4 * 1024 * 1024
    payload = bytes((i * 131 + 7) & 0xFF for i in range(0, SIZE, 997))  # cheap pseudo-random
    payload = (payload * ((SIZE // len(payload)) + 1))[:SIZE]
    res = {}
    hold = {}

    def server():
        lstn = hold["lstn"]
        conn = lstn.accept()
        buf = bytearray(SIZE)
        view = memoryview(buf)
        off = 0
        while off < SIZE:
            n = conn.recv_into(view[off:], SIZE - off)
            if n == 0:
                break
            off += n
        res["recv_len"] = off
        res["recv_ok"] = (bytes(buf[:off]) == payload)
        conn.close()
        lstn.close()

    def client():
        rc.sched_yield()
        c = rc.TCPConn.connect("127.0.0.1", hold["port"])
        c.send_all(payload)
        c.close()

    lstn = rc.TCPConn.listen("127.0.0.1", 0, 128, 0)
    # bound port from the raw fd
    raw = socket.socket(fileno=os.dup(lstn.fileno()))
    hold["port"] = raw.getsockname()[1]; raw.detach()
    hold["lstn"] = lstn
    with hang_guard(40, "tcpconn large transfer"):
        rc.go(server); rc.go(client); rc.run()
    assert res.get("recv_len") == SIZE, "short transfer: %r/%d" % (res.get("recv_len"), SIZE)
    assert res.get("recv_ok") is True, "4MB transfer corrupted"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_tcpconn_many_concurrent_connections_mn():
    # serve() with a PYTHON echo handler + many concurrent TCPConn clients under
    # M:N: each client sends a distinct frame and must read it back verbatim
    # (set equality of the echoed payloads -- a cross-wire / dropped wake shows
    # as a missing or wrong echo).  The Python-handler path is the robust one;
    # the all-C echo path (handler=None) hangs intermittently and is encoded as
    # a finding below (test_serve_all_c_echo_concurrent_hangs_subprocess).
    N = 80
    got = [None] * N

    def main():
        def handler(conn):
            try:
                data = conn.recv(8)
                if data:
                    conn.send_all(data)
            finally:
                conn.close()
        port, listeners = rc.serve("127.0.0.1", 0, handler, 3)
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)

        def client(i):
            try:
                c = rc.TCPConn.connect("127.0.0.1", port)
                c.send_all(struct.pack(">Q", i))
                got[i] = c.recv(8)
                c.close()
            finally:
                wg.done()
        for i in range(N):
            rc.mn_go(lambda i=i: client(i))
        wg.wait()
        for ln in listeners:
            ln.close()
    with hang_guard(60, "tcpconn many conns M:N"):
        runloom.run(4, main)
    expected = {struct.pack(">Q", i) for i in range(N)}
    received = {g for g in got if g is not None}
    missing = expected - received
    assert not missing, "%d echoes wrong/missing under M:N: %r" % (
        len(missing), list(missing)[:5])
    for i, g in enumerate(got):
        assert g == struct.pack(">Q", i), "client %d got wrong echo %r" % (i, g)


# FINDING: serve(handler=None) -- the tstate-free all-C echo path (mn_go_c C
# accept loop + per-conn runloom_io_c_echo fibers, with the io_uring multishot
# recv) -- DEADLOCKS/lost-wakes intermittently under concurrent M:N connections,
# while the Python-handler serve path with the IDENTICAL client load never does.
# Reproduces standalone roughly 4-of-6 runs at N=20 acceptors=2 hubs=4 (and at
# N=80 acceptors=3): wg.wait() never reaches N because some connections' echo
# never returns, so runloom.run()/mn_run() hangs forever (the main thread sits in
# mn_run; every hub thread is parked with no Python frame).  Run in a SUBPROCESS
# with a hard timeout so the hang is CONTAINED + OBSERVED as a non-zero
# returncode, never a wedged suite.  The xfail asserts the CORRECT behavior (all
# clients echoed within the timeout); it currently fails (the subprocess times
# out / under-counts), recording the finding without touching the C source.
_ALL_C_ECHO_SCRIPT = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup
N = 40
got = [None] * N
def main():
    port, listeners = rc.serve("127.0.0.1", 0, None, 2)   # handler=None: all-C echo
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in listeners:
        ln.close()
runloom.run(4, main)
sys.stdout.write("ALL_C_ECHO_OK %d\n" % sum(1 for g in got if g is not None))
'''


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.xfail(strict=False, reason=(
    "FINDING: serve(handler=None) all-C echo path (mn_go_c C echo fibers + "
    "io_uring multishot recv) deadlocks/lost-wakes intermittently under "
    "concurrent M:N connections -- wg.wait() never reaches N and mn_run() hangs; "
    "the Python-handler serve path with the same load never hangs. Asserts the "
    "correct (all clients echoed) behavior, which currently fails."))
def test_serve_all_c_echo_concurrent_completes_subprocess():
    # Try a few times: the hang is intermittent, so a single lucky pass would
    # mask it.  If ANY attempt hangs (times out) or under-counts, the finding is
    # confirmed and we fail (-> xfail).  All attempts must complete fully for the
    # path to be considered fixed.
    failures = []
    for attempt in range(4):
        try:
            p = _subproc(_ALL_C_ECHO_SCRIPT, timeout=15)
        except subprocess.TimeoutExpired:
            failures.append(("hang", attempt))
            continue
        _assert_no_signal_crash(p, "all-C echo concurrent")
        if "ALL_C_ECHO_OK 40" not in p.stdout:
            failures.append(("incomplete", attempt, p.stdout.strip()))
    assert not failures, (
        "serve(handler=None) all-C echo hung/under-counted on %d/4 attempts: %r"
        % (len(failures), failures))


# ==========================================================================
# 12. serve() -- single-thread refusal + M:N python handler.
# ==========================================================================
def test_serve_requires_mn_single_thread_refuses():
    res = {}

    def main():
        try:
            rc.serve("127.0.0.1", 0, None, 1)
            res["res"] = "served?!"
        except RuntimeError as e:
            res["res"] = ("RuntimeError", str(e))
    with hang_guard(10, "serve single-thread refuse"):
        rc.go(main); rc.run()
    assert isinstance(res.get("res"), tuple), "serve() did not refuse single-thread: %r" % res.get("res")
    assert "M:N" in res["res"][1] or "hub" in res["res"][1]


def test_serve_rejects_non_callable_handler():
    res = {}

    def main():
        try:
            rc.serve("127.0.0.1", 0, 12345)        # not callable, not None
            res["res"] = "accepted?!"
        except TypeError as e:
            res["res"] = "TypeError"
        except RuntimeError:
            # single-thread refusal may fire first depending on arg-check order;
            # either way a non-callable must not silently serve.
            res["res"] = "RuntimeError"
    with hang_guard(10, "serve non-callable"):
        rc.go(main); rc.run()
    assert res.get("res") in ("TypeError", "RuntimeError"), (
        "non-callable handler not rejected: %r" % res.get("res"))


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_serve_python_handler_echo_mn():
    box = {}

    def main():
        def handler(conn):
            try:
                data = conn.recv(64)
                if data:
                    conn.send_all(b"echo:" + data)
            finally:
                conn.close()
        port, listeners = rc.serve("127.0.0.1", 0, handler, 2)
        box["port"] = port

        def client():
            try:
                c = rc.TCPConn.connect("127.0.0.1", port)
                c.send_all(b"hello")
                box["reply"] = c.recv(64)
                c.close()
            finally:
                for ln in listeners:
                    ln.close()
        rc.mn_go(client)
    with hang_guard(40, "serve python handler M:N"):
        runloom.run(3, main)
    assert box.get("reply") == b"echo:hello", "serve python handler echo: %r" % box.get("reply")


# ==========================================================================
# 13. teardown / cancel-while-parked races -- close fd, cancel_fd, drain.
# ==========================================================================
def test_close_socket_while_parked_wakes_recv():
    # A fiber parked in TCPConn.recv; another fiber close()s the conn. The close
    # hook (cancel_fd) must wake the parked recv with a cancelled return, not
    # strand it forever.
    res = {}
    hold = {}

    def reader():
        c = hold["c"]
        try:
            res["r"] = c.recv(16)      # parks; close() wakes it
        except OSError as e:
            res["r"] = ("OSError", e.errno)

    def closer():
        rc.sched_yield(); rc.sched_yield()
        hold["c"].close()

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    c = rc.TCPConn(a.fileno())
    hold["c"], hold["b"] = c, b
    with hang_guard(15, "close while parked"):
        rc.go(reader); rc.go(closer); rc.run()
    _drop_sock(b)
    # recv returns b"" (cancel_fd-woken recv re-reads -> EOF/empty) or raises
    # OSError; either is acceptable, the point is it did NOT hang.
    assert "r" in res, "parked recv was not woken by close()"


def test_cancel_fd_wakes_all_waiters_on_one_fd():
    # Multiple fibers parked READ on the SAME fd; cancel_fd must wake EVERY one
    # of them with the CANCELLED sentinel (not just the head of the bucket list).
    K = 12
    res = [None] * K
    hold = {}

    def waiter(i):
        res[i] = rc.wait_fd(hold["r"], READ, -1)

    def canceller():
        for _ in range(4):
            rc.sched_yield()
        rc.netpoll_cancel_fd(hold["r"])

    r, w = os.pipe()
    hold["r"], hold["w"] = r, w
    with hang_guard(15, "cancel_fd all waiters"):
        def main():
            for i in range(K):
                rc.go(lambda i=i: waiter(i))
            rc.go(canceller)
        rc.go(main); rc.run()
    _drop(r); os.close(w)
    woken = sum(1 for v in res if v == CANCELLED)
    assert woken == K, "cancel_fd woke only %d/%d waiters on the shared fd" % (woken, K)


def test_drain_parked_cancels_leftover_loops():
    # sched_reset's drain wakes leftover parked fibers with -1 so they exit; a
    # subsequent run() must not inherit a wedged parker.  Park a fiber forever,
    # let run() return (it can't -- so we cancel it), then re-run cleanly.
    res = {}
    hold = {}

    def parker():
        r, w = os.pipe()
        hold["fds"] = (r, w)
        rv = rc.wait_fd(r, READ, -1)
        res["rv"] = rv

    def waker():
        rc.sched_yield(); rc.sched_yield()
        # cancel the parker so run() can finish (no external drain hook from
        # Python besides cancel_fd / cancel_g)
        rc.netpoll_cancel_fd(hold["fds"][0])

    with hang_guard(15, "drain leftover"):
        rc.go(parker); rc.go(waker); rc.run()
    r, w = hold["fds"]; _drop(r); os.close(w)
    assert res.get("rv") == CANCELLED
    # a fresh run() afterward must work (no inherited wedge)
    out = _run_single(lambda: "clean")
    assert out == "clean"


# ==========================================================================
# 14. resource growth -- many distinct fds growing the per-fd arrays, mixed
#     ready and timed-out, single pass.
# ==========================================================================
def test_many_distinct_fds_mixed_ready_and_timeout():
    N = 256
    pairs = [socket.socketpair() for _ in range(N)]
    for a, b in pairs:
        a.setblocking(False); b.setblocking(False)
    outcome = [None] * N    # "ready" or "tmout"

    def reader(i):
        rd = pairs[i][0]
        # Clear any stale arm inherited from a reused fd NUMBER (the documented
        # clean-reuse path; mirrors what every correct close hook does).  This
        # process is long-lived across the whole suite, so these fresh socket
        # numbers will have been used-and-closed before -- without the clear a
        # reused number's sticky arm would suppress the EPOLL_CTL_ADD and the
        # reader would hang, which is the SEPARATE fd-poison hazard exercised by
        # test_raw_close_without_unregister_poisons_fd_subprocess, not the
        # large-array lost-wake we measure here.
        rc.netpoll_release_if_idle(rd.fileno())
        # even fds get data (long ceiling so they never spuriously time out);
        # odd fds get NOTHING and a short ceiling so they exercise heap-expire.
        # Exercises both heap-expire and level-ready wake on a large per-fd
        # array in one run.  Level-triggered: even readers that park AFTER the
        # send still wake (the ADD synthesizes the edge).
        ms = 20000 if (i % 2 == 0) else 120
        rv = rc.wait_fd(rd.fileno(), READ, ms)
        if rv & READ:
            outcome[i] = ("ready", rd.recv(1))
        else:
            outcome[i] = ("tmout", None)

    def main():
        for i in range(N):
            rc.go(lambda i=i: reader(i))
        # Yield enough times that every reader has parked before we send, so the
        # test measures level-ready delivery, not a park-after-send race.
        for _ in range(8):
            rc.sched_yield()
        for i in range(0, N, 2):     # make every even fd ready
            pairs[i][1].send(b"!")
    with hang_guard(40, "many fds mixed"):
        rc.go(main); rc.run()
    for a, b in pairs:
        _drop_sock(a); _drop_sock(b)
    # The lost-wake property: EVERY even (data-bearing) reader must have woken
    # READ-ready -- a dropped edge on a large per-fd array would leave one timed
    # out instead.  (Odd readers all time out: they never received data.)
    even_ready = sum(1 for i in range(0, N, 2)
                     if outcome[i] and outcome[i][0] == "ready")
    odd_tmout = sum(1 for i in range(1, N, 2)
                    if outcome[i] and outcome[i][0] == "tmout")
    assert even_ready == N // 2, (
        "lost wake on a large per-fd array: %d/%d data-bearing readers woke"
        % (even_ready, N // 2))
    assert odd_tmout == N // 2, (
        "%d/%d never-fed readers timed out (a spurious wake?)" % (odd_tmout, N // 2))


# ==========================================================================
# 15. foreign OS thread: netpoll_unregister / release_if_idle must be safe to
#     call from a non-goroutine thread (the GC / close-hook can run anywhere).
# ==========================================================================
def test_netpoll_unregister_from_foreign_thread_safe():
    # netpoll_unregister / release_if_idle are pure cache/epoll_ctl ops with no
    # park; a foreign OS thread (a GC dealloc, a close hook on a worker thread)
    # may call them.  Must not crash or wedge.
    errors = []
    done = {"flag": False}

    def foreign():
        try:
            for _ in range(200):
                r, w = os.pipe()
                rc.netpoll_unregister(r)
                rc.netpoll_release_if_idle(r)
                os.close(r); os.close(w)
        except Exception as e:
            errors.append(repr(e))
        finally:
            done["flag"] = True

    t = raw_thread(foreign)
    deadline = time.monotonic() + 10
    while not done["flag"] and time.monotonic() < deadline:
        time.sleep(0.01)
    t.join(timeout=5)
    assert done["flag"], "foreign-thread netpoll cache ops hung"
    assert not errors, "foreign-thread netpoll ops raised: %r" % errors


# ==========================================================================
# 16. env-gated scheduler modes over a netpoll workload (subprocess) -- the
#     mode's detector paths must run a netpoll-heavy workload without crash.
# ==========================================================================
_NETPOLL_WORKLOAD = r'''
import sys, os, socket; sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup
def main():
    wg = WaitGroup()
    pairs = [socket.socketpair() for _ in range(40)]
    for a, b in pairs:
        a.setblocking(False); b.setblocking(False)
    woke = bytearray(40)
    wg.add(40)
    def reader(i):
        try:
            rd = pairs[i][0]
            if rc.wait_fd(rd.fileno(), 1, 5000) & 1 and rd.recv(1) == b"!":
                woke[i] = 1
        finally:
            wg.done()
    def cpu():
        x = 0
        for k in range(300000): x += k
        return x
    for i in range(40):
        rc.mn_go(lambda i=i: reader(i))
    for _ in range(4):
        rc.mn_go(cpu)
    rc.sched_yield()
    for i in range(40):
        pairs[i][1].send(b"!")
    wg.wait()
    for a, b in pairs:
        rc.netpoll_unregister(a.fileno()); a.close()
        rc.netpoll_unregister(b.fileno()); b.close()
    sys.stdout.write("WOKE %d\n" % sum(woke))
runloom.run(4, main)
'''


@pytest.mark.skipif(not FT, reason="M:N env modes need GIL-disabled build")
@pytest.mark.parametrize("mode_env", [
    {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "8"},
    {"RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "8"},
    {"RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2"},
    {"RUNLOOM_HUB_IDLE_WAKE": "0"},
    {"RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1"},
    {"RUNLOOM_DEADLOCK_MS": "50"},
    {"RUNLOOM_READY_STARVE_BOUND": "2"},
])
def test_netpoll_workload_under_env_mode_subprocess(mode_env):
    p = _subproc(_NETPOLL_WORKLOAD, env_extra=mode_env, timeout=50)
    _assert_no_signal_crash(p, "env-mode %r" % sorted(mode_env))
    assert "WOKE 40" in p.stdout, (
        "netpoll workload under %r lost wakes / hung: %r / %r"
        % (sorted(mode_env), p.stdout, p.stderr[-1000:]))


def test_netpoll_workload_under_gated_off_unsafe_flag_subprocess():
    # RUNLOOM_PER_G_TSTATE is KNOWN-CRASH at hub-count>=2; set it WITHOUT
    # RUNLOOM_ALLOW_UNSAFE_MIGRATION -> the runtime must WARN to stderr and run
    # the default (safe) scheduler.  Assert: no crash, work completes, the warn
    # path was taken (default scheduler).  NEVER set the allow flag.
    p = _subproc(_NETPOLL_WORKLOAD, env_extra={"RUNLOOM_PER_G_TSTATE": "1"}, timeout=50)
    _assert_no_signal_crash(p, "gated-off per_g_tstate")
    assert "WOKE 40" in p.stdout, (
        "gated-off unsafe-flag workload did not complete: %r / %r"
        % (p.stdout, p.stderr[-1000:]))


# ==========================================================================
# 17. netpoll_poll() drains ready fds without running the parked fibers.
# ==========================================================================
def test_netpoll_poll_delivers_readiness_on_sleep0():
    # netpoll_poll() (the aio sleep(0) drain) must enqueue a ready parked fiber
    # WITHOUT a blocking pump.  Park a reader, make it ready, call netpoll_poll,
    # then yield -> the reader runs and sees the data.
    res = {}
    hold = {}

    def reader():
        a = hold["a"]
        res["rv"] = rc.wait_fd(a.fileno(), READ, 5000)
        if res["rv"] & READ:
            res["data"] = a.recv(1)

    def driver():
        rc.sched_yield()                  # let reader park
        hold["b"].send(b"P")              # make it ready
        rc.netpoll_poll()                 # non-blocking drain -> enqueue reader
        rc.sched_yield()                  # reader runs

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(15, "netpoll_poll sleep0"):
        rc.go(reader); rc.go(driver); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert res.get("rv", 0) & READ, "netpoll_poll did not deliver readiness"
    assert res.get("data") == b"P"


# ==========================================================================
# 18. AUGMENTATION (adversarial critic pass).  The first pass covered the big
#     hazards (fd-validation, EPOLLHUP-into-WRITE, MOD-widen, wake-storm set-eq,
#     deadline order, fault injection, signals, slow-return, io_uring, serve).
#     The gaps below are the ones it skipped or tested too shallowly:
#       (a) the timeout==0 / events==0 / events-out-of-range ARG edges of wait_fd
#           -- distinct from the negative/huge-fd edges -- the immediate-expiry
#           and "no direction requested" branches;
#       (b) netpoll_cancel_fd on an fd nobody is parked on / a bogus or negative
#           fd -- the no-op + bad-input branches that must not crash;
#       (c) release_if_idle WHILE a fiber is parked -- the GUARDED path that must
#           NOT EPOLL_CTL_DEL a live arm (cov only covers the no-parker case);
#       (d) EPOLLERR folding into the READ direction specifically (a reader
#           parked READ-only on a socket whose peer RSTs) -- the first pass only
#           proved the fold into WRITE;
#       (e) cancel_fd waking waiters in BOTH directions (READ + WRITE) on one fd;
#       (f) the raw tcp_send/tcp_recv primitives' real-EAGAIN park on a multi-MB
#           transfer (byte-exact) -- the first pass exercised TCPConn, not the
#           bare module_tcp primitives, and only fault-INJECTED fd_read/fd_write;
#       (g) TCPConn / raw-primitive argument validation: recv(0)/recv(-1),
#           recv_into on a 0-len buffer, ops on a CLOSED conn, double-close
#           idempotency, fileno()==-1 after close, accept on a non-listener,
#           tcp_recv n<=0, tcp_recv_alloc(0)/negative;
#       (h) netpoll_poll() delivering readiness to MANY parked fibers in one
#           non-blocking drain (set-equality), and netpoll_poll being safe to
#           call when nothing is parked;
#       (i) a deadline park interleaved with a netpoll_poll drain (the timed
#           park must still expire; the poll must not eat its deadline);
#       (j) fd_read partial/short read and fd_write of a large buffer that parks
#           WRITE for real (no fault injection);
#       (k) a connect/accept/recv full round-trip through the raw module_tcp
#           primitives under SIGALRM is already covered; add the gated-OFF
#           RUNLOOM_STEAL_WOKEN unsafe-flag warn path over a netpoll workload
#           (the sibling of PER_G_TSTATE -- both KNOWN-CRASH at hub>=2, both must
#           warn + run the default scheduler WITHOUT the allow flag).
# ==========================================================================

def test_wait_fd_timeout_zero_nonready_expires_immediately():
    # timeout==0 is the immediate-expiry boundary (deadline == now): a NON-ready
    # fd must return 0 without parking past ~now, and a READY fd must return its
    # mask.  Distinct from the negative (infinite) and positive cases the first
    # pass covered.
    def f():
        out = {}
        r, w = os.pipe()
        try:
            t0 = time.monotonic()
            out["nonready"] = rc.wait_fd(r, READ, 0)     # nothing to read -> 0 now
            out["dt"] = time.monotonic() - t0
        finally:
            _drop(r); _drop(w)
        a, b = socket.socketpair(); a.setblocking(False)
        try:
            out["ready"] = rc.wait_fd(a.fileno(), WRITE, 0)   # writable -> WRITE now
        finally:
            _drop_sock(a); _drop_sock(b)
        return out
    with hang_guard(10, "wait_fd timeout==0"):
        out = _run_single(f)
    assert out["nonready"] == 0, "timeout==0 on a non-ready fd did not expire to 0: %r" % out["nonready"]
    assert out["dt"] < 0.5, "timeout==0 parked %.3fs (should be immediate)" % out["dt"]
    assert out["ready"] & WRITE, "timeout==0 on a writable fd lost its WRITE mask: %r" % out["ready"]


def test_wait_fd_events_zero_and_out_of_range_no_crash():
    # events==0 (no direction) and events with stray high bits (events & 3 == 0)
    # must not abort/crash: register computes need=events&(READ|WRITE)==0 and the
    # park just rides its deadline to a 0 timeout.  Bounded ceiling so it can't
    # hang.  This is the "no arm requested" branch the first pass never hit.
    def f():
        out = {}
        a, b = socket.socketpair(); a.setblocking(False)
        try:
            t0 = time.monotonic()
            out["ev0"] = rc.wait_fd(a.fileno(), 0, 60)      # no direction -> timeout 0
            out["dt0"] = time.monotonic() - t0
            out["ev_hi"] = rc.wait_fd(a.fileno(), 8, 60)    # 8&3==0 -> same
        finally:
            _drop_sock(a); _drop_sock(b)
        return out
    with hang_guard(10, "wait_fd events edge"):
        out = _run_single(f)
    assert out["ev0"] == 0, "events==0 did not ride to a timeout-0: %r" % out["ev0"]
    assert 0.04 < out["dt0"] < 1.0, "events==0 deadline mis-fired at %.3fs" % out["dt0"]
    assert out["ev_hi"] == 0, "out-of-range events bit did not ride to 0: %r" % out["ev_hi"]


def test_cancel_fd_noop_on_idle_and_bad_fd():
    # netpoll_cancel_fd on (a) an fd nobody is parked on, (b) a never-registered
    # high fd, (c) a negative fd -- all must be clean no-ops, never an abort or a
    # corrupted per-fd table.  The first pass only cancelled fds that HAD parkers.
    def f():
        r, w = os.pipe()
        rc.netpoll_cancel_fd(r)             # registered? no.  parked? no.  -> no-op
        rc.netpoll_cancel_fd(999999)        # never-seen high fd -> no growth, no-op
        rc.netpoll_cancel_fd(-1)            # negative -> rejected/no-op, never FD_SET(-1)
        os.close(r); os.close(w)
        # a real wait still works right afterward (table not corrupted)
        a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
        res = {}
        def parker():
            res["rv"] = rc.wait_fd(a.fileno(), READ, 2000)
        def feeder():
            rc.sched_yield(); rc.sched_yield()
            b.send(b"k")
        rc.go(parker); rc.go(feeder)
        return res, a, b
    with hang_guard(10, "cancel_fd no-op"):
        box = {}
        def main():
            box["v"] = f()
        rc.go(main); rc.run()
    res, a, b = box["v"]
    _drop_sock(a); _drop_sock(b)
    assert res.get("rv", 0) & READ, "cancel_fd no-ops corrupted the fd table (later wait lost)"


def test_release_if_idle_is_noop_while_parked():
    # The GUARDED path: release_if_idle must NOT EPOLL_CTL_DEL the arm of an fd
    # that has a LIVE parker.  Park a reader, call release_if_idle on its fd from
    # a sibling fiber (so the scheduler idle-pumps), then send -- if the arm had
    # been DEL'd the wake would be lost.  cov only covers the no-parker DEL path.
    box = {}
    hold = {}

    def parker():
        a = hold["a"]
        rv = rc.wait_fd(a.fileno(), READ, 4000)
        box["rv"] = rv
        if rv & READ:
            box["data"] = a.recv(1)

    def feeder():
        rc.sched_yield(); rc.sched_yield()
        rc.netpoll_release_if_idle(hold["a"].fileno())   # parker is live -> no-op
        hold["b"].send(b"L")                              # arm must survive -> wake

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(15, "release_if_idle while parked"):
        rc.go(parker); rc.go(feeder); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert box.get("rv", 0) & READ, (
        "release_if_idle DEL'd a LIVE arm -> the parked reader lost its wake")
    assert box.get("data") == b"L", "wrong data after release_if_idle no-op: %r" % box.get("data")


def test_epoll_err_wakes_a_read_only_waiter():
    # The fold-into-READ direction (the first pass proved fold-into-WRITE via a
    # send-buffer-full WRITE waiter).  A reader parked READ-ONLY on a connected
    # socket whose peer RSTs (SO_LINGER 0) must wake on the EPOLLERR/HUP fold,
    # not strand forever.  Byte-exact is not the point here (a RST may drop
    # queued data); the point is the READ arm wakes on a pure error event.
    res = {}
    hold = {}

    def reader():
        a = hold["a"]
        res["rv"] = rc.wait_fd(a.fileno(), READ, 3000)

    def resetter():
        rc.sched_yield(); rc.sched_yield()
        b = hold["b"]
        b.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        b.close()                       # RST -> EPOLLERR|EPOLLHUP on a's read end

    a, b = socket.socketpair()
    a.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(20, "err wakes read-only waiter"):
        rc.go(reader); rc.go(resetter); rc.run()
    _drop_sock(a)
    assert res.get("rv", 0) & READ, "RST/ERR did not wake the READ-only waiter (rv=%r)" % res.get("rv")


def test_cancel_fd_wakes_both_directions_on_one_fd():
    # cancel_fd must wake EVERY parker on the fd regardless of direction: a READ
    # waiter AND a WRITE waiter on the same fd (the WRITE arm widened the reg via
    # MOD) must BOTH return CANCELLED.  Distinct from the first pass's
    # same-direction K-waiter cancel.  Use a send-buffer-full socket so the WRITE
    # waiter genuinely parks instead of returning writable immediately.
    res = {}
    hold = {}

    def read_waiter():
        res["r"] = rc.wait_fd(hold["a"].fileno(), READ, -1)

    def write_waiter():
        a = hold["a"]
        try:
            while True:
                a.send(b"\0" * 65536)       # fill the send buffer -> not writable
        except (BlockingIOError, OSError):
            pass
        res["w"] = rc.wait_fd(a.fileno(), WRITE, -1)

    def canceller():
        for _ in range(5):
            rc.sched_yield()
        rc.netpoll_cancel_fd(hold["a"].fileno())

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(15, "cancel both directions"):
        def main():
            rc.go(read_waiter)
            rc.sched_yield()
            rc.go(write_waiter)
            rc.go(canceller)
        rc.go(main); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert res.get("r") == CANCELLED, "READ waiter not cancelled: %r" % res.get("r")
    assert res.get("w") == CANCELLED, "WRITE waiter not cancelled: %r" % res.get("w")


def test_raw_tcp_send_recv_large_real_eagain_byte_exact():
    # The bare module_tcp primitives (tcp_send / tcp_recv) on a 2 MB transfer:
    # tcp_send MUST hit a real EAGAIN and park WRITE (the buffer can't hold 2 MB),
    # tcp_recv MUST park READ between bursts.  Byte-exact end-to-end proves
    # neither direction's park dropped or mis-ordered a chunk.  The first pass
    # only exercised TCPConn (which can route through io_uring) and only
    # fault-injected the fd primitives -- this is the readiness-path tcp_* loop.
    SIZE = 2 * 1024 * 1024
    payload = bytes((i * 97 + 3) & 0xFF for i in range(SIZE))
    res = {}
    hold = {}

    def sender():
        res["sent"] = rc.tcp_send(hold["a"].fileno(), payload)

    def receiver():
        got = bytearray()
        buf = bytearray(65536)
        while len(got) < SIZE:
            n = rc.tcp_recv(hold["b"].fileno(), buf, 65536)
            if n == 0:
                break
            got += buf[:n]
        res["recv_len"] = len(got)
        res["ok"] = (bytes(got) == payload)

    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    hold["a"], hold["b"] = a, b
    with hang_guard(30, "raw tcp_send/recv 2MB"):
        rc.go(sender); rc.go(receiver); rc.run()
    _drop_sock(a); _drop_sock(b)
    assert res.get("sent") == SIZE, "tcp_send short: %r/%d" % (res.get("sent"), SIZE)
    assert res.get("recv_len") == SIZE, "tcp_recv short: %r/%d" % (res.get("recv_len"), SIZE)
    assert res.get("ok") is True, "raw tcp_send/recv 2MB corrupted (park dropped/reordered a chunk)"


def test_tcpconn_and_raw_arg_validation():
    # Argument-validation edges that must raise cleanly (never crash / never hang
    # / never silently misbehave): recv(0)->b"", recv(-1)->ValueError, recv_into
    # on a 0-len buffer->0, ops on a CLOSED conn->OSError, double-close is
    # idempotent, fileno()==-1 after close, accept on a non-listener->OSError,
    # tcp_recv n<=0 -> 0, tcp_recv_alloc(0)->b"" / negative->ValueError.
    def f():
        out = {}
        a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
        c = rc.TCPConn(a.fileno())
        out["recv0"] = c.recv(0)
        try:
            c.recv(-1); out["recvneg"] = "no-raise"
        except ValueError:
            out["recvneg"] = "ValueError"
        out["recv_into_0"] = c.recv_into(bytearray(0))
        c.close()
        try:
            c.recv(4); out["recv_closed"] = "no-raise"
        except OSError:
            out["recv_closed"] = "OSError"
        try:
            c.send(b"x"); out["send_closed"] = "no-raise"
        except OSError:
            out["send_closed"] = "OSError"
        out["fileno_after_close"] = c.fileno()
        out["closed_prop"] = bool(c.closed)
        c.close()                              # double close -> idempotent
        out["double_close"] = "ok"
        # b's fd is owned by nobody now (a's fd went through TCPConn.close which
        # unregistered+closed it).  close b's python wrapper directly.
        try:
            b.close()
        except OSError:
            pass
        # accept on a non-listener TCPConn
        a2, b2 = socket.socketpair(); a2.setblocking(False)
        c2 = rc.TCPConn(a2.fileno())
        try:
            c2.accept(); out["accept_nonlistener"] = "no-raise"
        except OSError:
            out["accept_nonlistener"] = "OSError"
        c2.close()
        try:
            b2.close()
        except OSError:
            pass
        # raw primitive edges on a fresh pair
        a3, b3 = socket.socketpair(); a3.setblocking(False); b3.setblocking(False)
        try:
            out["tcp_recv_neg"] = rc.tcp_recv(a3.fileno(), bytearray(8), 0)
            out["recv_alloc_0"] = rc.tcp_recv_alloc(a3.fileno(), 0)
            try:
                rc.tcp_recv_alloc(a3.fileno(), -1); out["recv_alloc_neg"] = "no-raise"
            except ValueError:
                out["recv_alloc_neg"] = "ValueError"
        finally:
            _drop_sock(a3); _drop_sock(b3)
        return out
    with hang_guard(15, "tcpconn arg validation"):
        out = _run_single(f)
    assert out["recv0"] == b"", "recv(0) != empty: %r" % out["recv0"]
    assert out["recvneg"] == "ValueError", "recv(-1) not ValueError: %r" % out["recvneg"]
    assert out["recv_into_0"] == 0, "recv_into(0-len) != 0: %r" % out["recv_into_0"]
    assert out["recv_closed"] == "OSError", "recv on closed conn: %r" % out["recv_closed"]
    assert out["send_closed"] == "OSError", "send on closed conn: %r" % out["send_closed"]
    assert out["fileno_after_close"] == -1, "fileno after close: %r" % out["fileno_after_close"]
    assert out["closed_prop"] is True, "closed prop wrong: %r" % out["closed_prop"]
    assert out["double_close"] == "ok"
    assert out["accept_nonlistener"] == "OSError", "accept on non-listener: %r" % out["accept_nonlistener"]
    assert out["tcp_recv_neg"] == 0, "tcp_recv(n<=0) != 0: %r" % out["tcp_recv_neg"]
    assert out["recv_alloc_0"] == b"", "tcp_recv_alloc(0) != empty: %r" % out["recv_alloc_0"]
    assert out["recv_alloc_neg"] == "ValueError", "tcp_recv_alloc(-1): %r" % out["recv_alloc_neg"]


def test_netpoll_poll_drains_many_parked_set_equality():
    # netpoll_poll() (the aio sleep(0) drain) must enqueue ALL ready parked
    # fibers in one non-blocking pass, not just one -- checked by SET EQUALITY of
    # the distinct per-fd payloads so a dropped/cross-wired enqueue surfaces as
    # wrong data.  The first pass only proved netpoll_poll delivers to a SINGLE
    # reader; this proves the fan-out.
    N = 48
    pairs = [socket.socketpair() for _ in range(N)]
    for a, b in pairs:
        a.setblocking(False); b.setblocking(False)
    got = [None] * N

    def reader(i):
        rd = pairs[i][0]
        rc.netpoll_release_if_idle(rd.fileno())
        rv = rc.wait_fd(rd.fileno(), READ, 6000)
        if rv & READ:
            try:
                got[i] = rd.recv(4)
            except BlockingIOError:
                got[i] = b"EAGAIN"

    def driver():
        for _ in range(8):
            rc.sched_yield()                       # let every reader park
        for i in range(N):
            pairs[i][1].send(struct.pack(">I", i))  # distinct per fd
        rc.netpoll_poll()                          # ONE non-blocking drain
        for _ in range(N + 8):
            rc.sched_yield()                       # let woken readers run

    def main():
        for i in range(N):
            rc.go(lambda i=i: reader(i))
        rc.go(driver)
    with hang_guard(30, "netpoll_poll fan-out"):
        rc.go(main); rc.run()
    expected = {struct.pack(">I", i) for i in range(N)}
    received = {g for g in got if g is not None}
    for a, b in pairs:
        _drop_sock(a); _drop_sock(b)
    missing = expected - received
    assert not missing, "netpoll_poll dropped %d readers in one drain: %r" % (
        len(missing), list(missing)[:5])
    for i, g in enumerate(got):
        assert g == struct.pack(">I", i), "netpoll_poll reader %d cross-wired: %r" % (i, g)


def test_netpoll_poll_safe_when_nothing_parked():
    # netpoll_poll() called repeatedly with NO parked fibers must be a clean
    # no-op (it drives runloom_netpoll_pump(0)) -- never block, never crash, even
    # outside any park.  Boundary robustness for the sleep(0) path.
    def f():
        for _ in range(50):
            rc.netpoll_poll()
        return "ok"
    with hang_guard(10, "netpoll_poll empty"):
        with assert_faster_than(2.0, "50 empty netpoll_poll calls"):
            out = _run_single(f)
    assert out == "ok"


def test_deadline_park_survives_a_netpoll_poll_drain():
    # A timed (deadline) park interleaved with netpoll_poll on an UNRELATED ready
    # fd: the timed park must still EXPIRE at its deadline (the poll drain must
    # not eat its deadline / re-arm the wrong timer), and the ready fd must wake.
    res = {}
    hold = {}

    def timed():
        r, w = os.pipe()
        hold["timed"] = (r, w)
        t0 = time.monotonic()
        rv = rc.wait_fd(r, READ, 120)      # never fed -> must expire ~120ms
        res["timed_rv"] = rv
        res["timed_dt"] = time.monotonic() - t0

    def ready():
        a, b = socket.socketpair(); a.setblocking(False)
        hold["ready"] = (a, b)
        for _ in range(2):
            rc.sched_yield()
        b.send(b"R")
        rv = rc.wait_fd(a.fileno(), READ, 5000)
        res["ready_rv"] = rv

    def driver():
        for _ in range(3):
            rc.sched_yield()
        rc.netpoll_poll()                   # drain the ready fd's readiness
        for _ in range(4):
            rc.sched_yield()

    def main():
        rc.go(timed); rc.go(ready); rc.go(driver)
    with hang_guard(15, "deadline survives poll"):
        rc.go(main); rc.run()
    r, w = hold["timed"]; _drop(r); os.close(w)
    a, b = hold["ready"]; _drop_sock(a); _drop_sock(b)
    assert res.get("timed_rv") == 0, "timed park did not expire to 0 across a poll: %r" % res.get("timed_rv")
    assert 0.08 < res.get("timed_dt", 0) < 2.0, (
        "timed park deadline shifted by the poll: %.3fs (want ~0.12s)" % res.get("timed_dt", 0))
    assert res.get("ready_rv", 0) & READ, "ready fd did not wake across the poll drain"


def test_fd_read_partial_and_large_fd_write_real_park():
    # fd_read of MORE than is available returns a SHORT read (what's there), and a
    # large fd_write to a pipe with a small buffer must hit a real EAGAIN, park
    # WRITE, and complete once the reader drains -- byte-exact.  No fault
    # injection: the real readiness park on a pipe.  The first pass only
    # fault-injected fd_read/fd_write.
    res = {}

    def main():
        # (a) short read: write 3 bytes, ask for 16 -> get 3
        r, w = os.pipe()
        os.write(w, b"abc")
        buf = bytearray(16)
        n = rc.fd_read(r, buf, 16)
        res["short_n"] = n
        res["short_buf"] = bytes(buf[:n])
        rc.netpoll_unregister(r); os.close(r); os.close(w)

        # (b) large write parks WRITE for real; a reader drains it byte-exact
        r2, w2 = os.pipe()
        os.set_blocking(r2, False)
        payload = bytes((i * 13 + 1) & 0xFF for i in range(512 * 1024))
        sub = {}

        def writer():
            sub["written"] = rc.fd_write(w2, payload)

        def reader():
            got = bytearray()
            rbuf = bytearray(65536)
            while len(got) < len(payload):
                k = rc.fd_read(r2, rbuf, 65536)
                if k == 0:
                    break
                got += rbuf[:k]
            sub["read_ok"] = (bytes(got) == payload)
            sub["read_len"] = len(got)

        rc.go(writer); rc.go(reader)
        res["sub"] = sub
        res["_fds"] = (r2, w2)

    with hang_guard(20, "fd_read/write real park"):
        rc.go(main); rc.run()
    r2, w2 = res["_fds"]
    rc.netpoll_unregister(r2); os.close(r2)
    rc.netpoll_unregister(w2); os.close(w2)
    assert res["short_n"] == 3, "fd_read short read returned %r (want 3)" % res["short_n"]
    assert res["short_buf"] == b"abc", "fd_read wrong data: %r" % res["short_buf"]
    sub = res["sub"]
    assert sub.get("written") == 512 * 1024, "fd_write short: %r" % sub.get("written")
    assert sub.get("read_len") == 512 * 1024, "pipe transfer short: %r" % sub.get("read_len")
    assert sub.get("read_ok") is True, "fd_write WRITE-park corrupted the pipe transfer"


def test_netpoll_workload_under_gated_off_steal_woken_subprocess():
    # RUNLOOM_STEAL_WOKEN is the sibling of RUNLOOM_PER_G_TSTATE: KNOWN-CRASH at
    # hub-count>=2.  Set it WITHOUT RUNLOOM_ALLOW_UNSAFE_MIGRATION -> the runtime
    # must WARN to stderr and run the DEFAULT (safe) scheduler over the netpoll
    # workload.  Assert: no crash, all wakes delivered.  NEVER set the allow flag.
    # The first pass covered the PER_G_TSTATE gated-off path; this is its twin.
    p = _subproc(_NETPOLL_WORKLOAD, env_extra={"RUNLOOM_STEAL_WOKEN": "1"}, timeout=50)
    _assert_no_signal_crash(p, "gated-off steal_woken")
    assert "WOKE 40" in p.stdout, (
        "gated-off STEAL_WOKEN workload did not complete: %r / %r"
        % (p.stdout, p.stderr[-1000:]))


def test_wait_fd_at_rlimit_minus_one_high_fd_no_crash_subprocess():
    # The HIGH end of the valid fd range, just below RLIMIT_NOFILE: a wait_fd on
    # an fd at (hard_limit - 1) that is NOT actually open must reject cleanly
    # (EBADF) without growing the per-fd arrays to multi-GB.  Complements the
    # first pass's "exactly at / above the limit" cases with the "valid index but
    # closed fd" case at the top of the range.  Subprocess so any abort/hang is
    # contained.
    script = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
out = {}
def main():
    # an fd one below the cap: a valid INDEX but no open fd there.
    if hard in (-1, resource.RLIM_INFINITY) or hard > (1 << 26):
        out["skip"] = True
        return
    probe = int(hard) - 1
    try:
        rc.wait_fd(probe, 1, 50)
        out["res"] = "no-raise"
    except OSError as e:
        out["res"] = ("OSError", e.errno)
    except Exception as e:
        out["res"] = ("wrong", type(e).__name__)
rc.go(main); rc.run()
sys.stdout.write("SKIP\n" if out.get("skip") else "RES=%r\n" % (out.get("res"),))
'''
    p = _subproc(script, timeout=25)
    _assert_no_signal_crash(p, "high-fd at rlimit-1")
    if "SKIP" in p.stdout:
        pytest.skip("RLIMIT_NOFILE unbounded/huge -- no representable top-of-range fd")
    assert "OSError" in p.stdout, (
        "a closed fd just below RLIMIT_NOFILE did not OSError cleanly (abort/hang?): %r / %r"
        % (p.stdout, p.stderr[-800:]))


def test_tcpconn_accept_loop_recv_send_roundtrip_single_thread():
    # A full TCPConn listen/accept/recv/send_all round-trip driven on the
    # SINGLE-THREAD scheduler (serve() refuses single-thread, so this is the only
    # way to exercise the C accept loop + recv + send_all off M:N).  Distinct
    # data each direction; byte-exact.  Exercises the readiness-path accept park
    # (the listener parks READ until the SYN) the first pass never drove outside
    # serve()/M:N.
    res = {}
    hold = {}

    def server():
        lstn = hold["lstn"]
        conn = lstn.accept()                # parks READ on the listen fd
        data = conn.recv(16)
        res["server_got"] = data
        conn.send_all(b"reply:" + data)
        conn.close()
        lstn.close()

    def client():
        rc.sched_yield()
        c = rc.TCPConn.connect("127.0.0.1", hold["port"])
        c.send_all(b"hello-rt")
        res["client_got"] = c.recv(64)
        c.close()

    lstn = rc.TCPConn.listen("127.0.0.1", 0, 64, 0)
    raw = socket.socket(fileno=os.dup(lstn.fileno()))
    hold["port"] = raw.getsockname()[1]; raw.detach()
    hold["lstn"] = lstn
    with hang_guard(20, "tcpconn single-thread roundtrip"):
        rc.go(server); rc.go(client); rc.run()
    assert res.get("server_got") == b"hello-rt", "server recv wrong: %r" % res.get("server_got")
    assert res.get("client_got") == b"reply:hello-rt", "client recv wrong: %r" % res.get("client_got")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
