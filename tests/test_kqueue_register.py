"""kqueue runloom_netpoll_register / unregister branch coverage (macOS/BSD).

The kqueue branch of runloom_netpoll_register lives in
src/runloom_c/netpoll_register.c.inc:93-179.  Unlike the epoll branch (LEVEL,
register-PER-DIRECTION-once, skip the syscall when the direction is already
armed), the kqueue branch arms ONLY the requested direction(s), EV_ADD|
EV_ONESHOT (NOT EV_CLEAR), and RE-ARMS on EVERY park.  The per-fd bitmap is kept
in sync for unregister but NO LONGER gates the kevent -- a second waiter / a
re-park on a still-set bit ORs the bit and falls through to kevent anyway.

This module exercises each register/unregister branch through BEHAVIOUR on real
socketpairs + runloom_c.wait_fd, on the single-thread scheduler (_drive), never a
backend internal -- exactly the proven convention from test_netpoll_conformance.

Branch map (file:line in netpoll_register.c.inc), with >=2-3 instances each:
  * 139-143  EV_SET per requested direction (READ / WRITE / READ|WRITE), and the
             n == 0 early return when events == 0.
  * 94-118   EV_ONESHOT delivers once then auto-disables, RE-ARMED on every park
             (repeated rapid re-arm on the same fd; re-arm in the other dir).
  * 122-128  runloom_fd_bit_set ORs the bit; a second waiter / a re-park whose
             bit is ALREADY set is NOT gated -- both still issue kevent + arm.
  * 107-111  EV_ADD re-checks readiness NOW: data present before the park is
             still delivered (ready-at-EV_ADD level recheck).
  * 163-179  unregister clears the bit + arm mask so a REUSED fd number
             re-registers cleanly (close + reopen reusing the fd, must wake).

wait_fd(fd, events, timeout_ms): returns the ready mask (1=READ,2=WRITE,3=both)
when ready, 0 on the deadline, raises OSError on a hard error.  timeout_ms<0 or
omitted blocks forever (we always pass a small bounded timeout for determinism).
"""
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


# ---- single-thread driver + socketpair helpers (proven convention) ----------
def _drive(*fibers):
    """Spawn each callable as a fiber, run the single-thread scheduler, re-raise
    the first exception any fiber hit so asserts surface in the test body."""
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
    """Clear the per-fd registered bit + arm mask around each test.

    These tests raw-``close()`` their socketpairs, bypassing the
    ``netpoll_unregister`` that all real runloom close paths run.  A reused fd
    number with a stale bit would (on the OLD scheme) skip its kevent and hang;
    mimic the close hook so tests don't leak registration into each other.
    Internal scheduler fds (kqueue fd, self-pipe) are not in the bit path, so
    this never touches them."""
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:           # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _registration_reset():
    _reset_registration()
    yield
    _reset_registration()


def test_backend_is_kqueue():
    """Sanity: every assertion below is about the kqueue branch, so prove we are
    actually on it (the skipif guards the platform; this guards the build)."""
    assert runloom_c.netpoll_backend() == "kqueue"


# =============================================================================
# 139-143  EV_SET per requested direction: READ / WRITE / READ|WRITE arm the
#          matching EVFILT, and only that one fires.
# =============================================================================
@pytest.mark.parametrize(
    "events, make_ready, expect",
    [
        # READ-only: arm EVFILT_READ; peer write -> READ.
        (READ, "peer_write", READ),
        # WRITE-only: arm EVFILT_WRITE; an empty send buffer is writable now.
        (WRITE, "already", WRITE),
        # READ|WRITE on a not-readable socket: both filters armed, only WRITE
        # ready -> a non-empty subset of the requested set (WRITE).
        (READ | WRITE, "already", WRITE),
    ],
    ids=["read_only", "write_only", "rw_subset_write_ready"])
def test_ev_set_per_direction(events, make_ready, expect):
    """netpoll_register.c.inc:139-143 -- one EV_SET per requested direction."""
    a, b = _pair()
    out = []

    def reader():
        out.append(runloom_c.wait_fd(a.fileno(), events, 2000))

    def writer():
        runloom_c.sched_yield()      # let the reader park + arm first
        if make_ready == "peer_write":
            b.send(b"x")
        # "already" cases need no action (the socket is born writable)

    _drive(reader, writer)
    assert out == [expect]
    a.close()
    b.close()


@pytest.mark.parametrize("timeout_ms", [120, 200, 300],
                         ids=["t120", "t200", "t300"])
def test_events_zero_arms_nothing_and_times_out(timeout_ms):
    """netpoll_register.c.inc:143 -- the n == 0 early return when events == 0.

    A wait_fd with events==0 arms NEITHER EVFILT (n stays 0, register returns 0
    without a kevent), so the fd can never wake the parker; it must wake only via
    its deadline (return 0).  This reaches register's n==0 path through wait_fd:
    the parker still links + commits, no kevent is issued, and the deadline heap
    delivers the timeout.  The socket is born WRITABLE, proving events==0 does
    not accidentally arm the always-ready WRITE filter."""
    a, b = _pair()                  # a is writable right now, never made readable
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), 0, timeout_ms)))
    assert out == [0], "events==0 should arm nothing and wake only on deadline"
    a.close()
    b.close()


# =============================================================================
# 107-111  EV_ADD re-checks readiness NOW: data that arrived BEFORE the parker
#          linked is still delivered (the level-recheck-at-add).
# =============================================================================
@pytest.mark.parametrize("payload", [b"x", b"hello", b"A" * 4000],
                         ids=["1B", "5B", "4000B"])
def test_ready_at_ev_add_recheck_read(payload):
    """netpoll_register.c.inc:107-111 -- ready-before-park returns immediately.

    The peer writes BEFORE the reader parks, so the fd is already readable when
    EV_ADD arms it.  kqueue reports a level-ready fd at add time, so EV_ADD
    synthesizes the readiness and wait_fd returns READ without ever blocking on
    the next kevent()."""
    a, b = _pair()
    b.send(payload)                 # readable before the single fiber parks
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), READ, 1000)))
    assert out == [READ]
    assert a.recv(len(payload)) == payload
    a.close()
    b.close()


@pytest.mark.parametrize("events", [WRITE, READ | WRITE],
                         ids=["write", "rw"])
def test_ready_at_ev_add_recheck_write(events):
    """netpoll_register.c.inc:107-111 -- a born-writable fd is delivered at the
    EV_ADD readiness recheck (single fiber, no peer action)."""
    a, b = _pair()
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), events, 1000)))
    # Writable now -> a non-empty subset of the requested set containing WRITE.
    assert out[0] in (WRITE, READ | WRITE)
    a.close()
    b.close()


# =============================================================================
# 94-118  EV_ONESHOT delivers once then auto-disables; RE-ARMED on every park.
#          A re-park MUST re-register or the second wait hangs.
# =============================================================================
@pytest.mark.parametrize("rounds", [3, 10, 50], ids=["r3", "r10", "r50"])
def test_oneshot_rearm_storm_same_fd(rounds):
    """netpoll_register.c.inc:94-118 -- EV_ONESHOT re-arm on EVERY park.

    Many park->ready->consume cycles on the SAME fd: EV_ONESHOT fires once and
    auto-disables, so each new park MUST re-issue EV_ADD|EV_ONESHOT (the bit is
    already set after round 1, proving the bit does NOT gate the re-arm).  A
    register that leaked / dropped the re-arm after the first delivery would
    hang from the 2nd round on."""
    a, b = _pair()
    ready = runloom_c.Chan()
    out = []

    def reader():
        n = 0
        for _ in range(rounds):
            if runloom_c.wait_fd(a.fileno(), READ, 2000) == READ:
                a.recv(1)
                n += 1
            ready.send(1)           # "consumed + about to re-park"
        out.append(n)

    def writer():
        for _ in range(rounds):
            runloom_c.sched_yield()
            b.send(b"x")
            ready.recv()            # one cycle done; reader re-armed

    _drive(reader, writer)
    assert out == [rounds], "EV_ONESHOT re-arm dropped mid-storm"
    a.close()
    b.close()


@pytest.mark.parametrize("first, second",
                         [(READ, WRITE), (WRITE, READ), (READ, READ)],
                         ids=["read_then_write", "write_then_read",
                              "read_then_read"])
def test_oneshot_rearm_switches_direction(first, second):
    """netpoll_register.c.inc:139-142 + 94-118 -- a re-park arms whatever
    direction is requested THIS time (per-direction one-shot), proving the
    re-register picks up the new events mask rather than reusing the first."""
    a, b = _pair()
    out = []

    def reader():
        # Make the first direction ready, wait it, consume, then re-park on the
        # second direction.  WRITE is always ready; READ needs a peer write.
        if first == READ:
            runloom_c.sched_yield()
        r1 = runloom_c.wait_fd(a.fileno(), first, 2000)
        if first == READ:
            a.recv(3)                # exact payload ("one"); don't over-consume
        # Re-park on the second direction.
        if second == READ:
            runloom_c.sched_yield()
        r2 = runloom_c.wait_fd(a.fileno(), second, 2000)
        if second == READ:
            a.recv(3)                # exact payload ("two"); don't over-consume
        out.append((r1, r2))

    def feeder():
        # Provide data whenever a READ direction is needed.
        if first == READ:
            runloom_c.sched_yield()
            runloom_c.sched_yield()
            b.send(b"one")
        if second == READ:
            # let the reader consume the first + re-park on READ
            for _ in range(6):
                runloom_c.sched_yield()
            b.send(b"two")

    _drive(reader, feeder)
    assert out == [(first, second)], "re-arm did not honour the new direction"
    a.close()
    b.close()


# =============================================================================
# 122-128  runloom_fd_bit_set ORs the bit; a SECOND waiter / a re-park on an
#          already-set bit is NOT gated -- both fall through to kevent + arm.
# =============================================================================
@pytest.mark.parametrize("n_waiters", [2, 3, 5], ids=["w2", "w3", "w5"])
def test_two_parks_one_fd_both_arm(n_waiters):
    """netpoll_register.c.inc:122-128 -- the bit ORs and the kevent is NOT gated.

    Multiple fibers park READ on the SAME fd.  The first sets the fd bit; every
    later register sees the bit already set, ORs it (returns 0 from the bit-set,
    NOT an early return), and STILL issues EV_ADD|EV_ONESHOT.  When the peer
    writes, the pump's kqueue path wakes EVERY matching parker (finding B2,
    wake_all=1), so all n waiters must wake -- a register that early-returned on
    the set bit would leave the later waiters un-armed and they'd time out."""
    a, b = _pair()
    woke = []

    def make_waiter(i):
        def run():
            r = runloom_c.wait_fd(a.fileno(), READ, 2500)
            if r == READ:
                woke.append(i)
        return run

    def writer():
        # Let all waiters park + arm on the one fd, then make it readable.
        for _ in range(n_waiters + 2):
            runloom_c.sched_yield()
        b.send(b"!")

    waiters = [make_waiter(i) for i in range(n_waiters)]
    _drive(*waiters, writer)
    assert sorted(woke) == list(range(n_waiters)), (
        "not all parkers on the shared fd armed (bit gated the re-arm?)")
    a.close()
    b.close()


def test_repark_after_partial_consume_rearms_on_set_bit():
    """netpoll_register.c.inc:122-128 -- a re-park whose bit is STILL set (the
    fd was never unregistered between parks) re-arms and re-reports buffered
    data.  Peer sends two bytes; the reader consumes one, re-parks (bit still
    set), and must be re-armed to see the remaining byte."""
    a, b = _pair()
    ready = runloom_c.Chan()
    out = []

    def reader():
        r1 = runloom_c.wait_fd(a.fileno(), READ, 2000)
        a.recv(1)                   # consume ONE of two -- still buffered
        ready.send(1)               # consumed one + re-parking (bit still set)
        r2 = runloom_c.wait_fd(a.fileno(), READ, 2000)
        a.recv(1)
        out.append((r1, r2))

    def writer():
        runloom_c.sched_yield()
        b.send(b"AB")               # two bytes
        ready.recv()

    _drive(reader, writer)
    assert out == [(READ, READ)], "re-park on a set bit was not re-armed"
    a.close()
    b.close()


# =============================================================================
# 163-179  unregister clears the bit + arm mask so a REUSED fd NUMBER
#          re-registers cleanly.
# =============================================================================
@pytest.mark.parametrize("attempt", [0, 1, 2], ids=["once", "twice", "thrice"])
def test_unregister_then_fd_reuse_rearms(attempt):
    """netpoll_register.c.inc:163-179 -- unregister clears the bit so a reused
    fd number re-registers cleanly.

    Open a socketpair, park+wake on `a` (sets `a`'s bit), then RAW close both
    and call netpoll_unregister(a.fileno()) (the close hook's job) to clear the
    bit + arm mask.  Open a NEW socketpair -- the OS very likely hands back the
    SAME fd number -- and park on it.  If unregister failed to clear the stale
    bit, the OLD scheme would skip the kevent for the reused fd and the new
    reader would hang/time out; the current scheme re-arms on every park, but we
    STILL exercise unregister's clear and assert the reused fd wakes.

    `attempt` runs the whole churn cycle 1..3 times so a leak that only bites on
    the 2nd/3rd reuse is caught (the historical fast-socket-churn hang)."""
    for _ in range(attempt + 1):
        a, b = _pair()
        out1 = []

        def reader1(a=a, b=b, out1=out1):
            def feed():
                runloom_c.sched_yield()
                b.send(b"x")
            runloom_c.go(feed)
            out1.append(runloom_c.wait_fd(a.fileno(), READ, 2000))

        _drive(reader1)
        assert out1 == [READ]
        old_fd = a.fileno()

        # Raw close (bypasses the runloom unregister hook) then mimic the hook.
        a.close()
        b.close()
        runloom_c.netpoll_unregister(old_fd)    # clears bit + arm mask

        # Reopen; likely reuses old_fd.  Must re-register + wake cleanly.
        c, d = _pair()
        out2 = []

        def reader2(c=c, d=d, out2=out2):
            def feed():
                runloom_c.sched_yield()
                d.send(b"y")
            runloom_c.go(feed)
            out2.append(runloom_c.wait_fd(c.fileno(), READ, 2000))

        _drive(reader2)
        assert out2 == [READ], (
            "reused fd %d (was %d) did not re-arm after unregister"
            % (c.fileno(), old_fd))
        c.close()
        d.close()
        runloom_c.netpoll_unregister(c.fileno())


def test_unregister_clears_write_arm_for_reuse():
    """netpoll_register.c.inc:163-179 -- the unregister clear also covers a fd
    that was last armed for WRITE: after unregister + fd reuse, a fresh WRITE
    wait on the reused number still fires immediately (born writable)."""
    a, b = _pair()
    out1 = []
    _drive(lambda: out1.append(runloom_c.wait_fd(a.fileno(), WRITE, 1000)))
    assert out1[0] in (WRITE, READ | WRITE)
    old_fd = a.fileno()
    a.close()
    b.close()
    runloom_c.netpoll_unregister(old_fd)

    c, d = _pair()
    out2 = []
    _drive(lambda: out2.append(runloom_c.wait_fd(c.fileno(), WRITE, 1000)))
    assert out2[0] in (WRITE, READ | WRITE), (
        "reused fd did not re-arm WRITE after unregister")
    c.close()
    d.close()
    runloom_c.netpoll_unregister(c.fileno())


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
