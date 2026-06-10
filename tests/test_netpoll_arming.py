"""Deterministic arming-discipline torture test for the netpoll backend.

This mirrors the Linux kernel's own approach to verifying epoll wakeup
semantics (``tools/testing/selftests/filesystems/epoll/epoll_wakeup_test.c``):
drive an fd's readiness from *outside* the scheduler -- a real OS thread doing
real socket I/O -- and assert the EXACT number of times a goroutine parked in
``runloom_c.wait_fd`` wakes, not merely "does it eventually make progress".

That exact-wake-count level is where the historical arming bugs lived.  runloom's
netpoll once registered fds ``EPOLLET | EPOLLEXCLUSIVE`` once and never re-armed;
a consumed-then-readable-again fd produced an edge the kernel never refired, so
the *second* ``wait_fd`` on that fd hung forever (the "hung 96/96" finding that
drove the switch to LEVEL + ``EPOLLONESHOT`` + re-arm-on-every-park).  A model
checker can't see that class of bug -- it's kernel epoll firing semantics, not a
memory-model race -- so it belongs in a deterministic wake-count test.

Determinism without timing guesses
-----------------------------------
A stream socket coalesces: two quick ``send``\\s before the reader consumes look
like a single readable state, and a naive test would then "see" a dropped edge
that never happened.  We remove the race with a cross-thread *handshake*: a
second socketpair carries an ACK from the waiter goroutine back to the feeder
thread, so the feeder only produces edge N+1 after the waiter has consumed edge
N and is about to re-arm.  One feeder send == exactly one readable edge ==
exactly one expected wake.

Whether that edge happens to arrive *before* the re-arm (data pending at arm
time -> the level-triggered / pending-bitmap path must report it) or *after*
(the kernel must fire the freshly armed oneshot) is left to the natural race;
both orderings MUST wake, and over many edges the test samples both.  The old
EPOLLET scheme dropped at least one of them.

Every ``wait_fd`` carries a finite timeout, so a *broken* arm surfaces as a 0
(timeout) return -- a bounded, assertable failure -- never a suite-wedging hang.
"""
import os
import socket
import sys
import threading

import pytest

# conftest.py already prepends <repo>/src to sys.path.
import runloom_c

READ = 1   # RUNLOOM_NETPOLL_READ
WRITE = 2  # RUNLOOM_NETPOLL_WRITE

# A healthy arm wakes in microseconds; this ceiling is only ever paid when an
# arm is BROKEN (the wake never comes), turning a hang into a 0 return we can
# assert on.  Overridable so the fault-injection harness can shorten it.
TIMEOUT_MS = int(os.environ.get("RUNLOOM_ARMING_TIMEOUT_MS", "4000"))

BACKEND = runloom_c.netpoll_backend()


@pytest.fixture(autouse=True)
def _reset_netpoll_registration():
    """Clear the per-fd 'registered' cache between tests.

    These tests drive raw ``runloom_c.wait_fd`` on raw ``socket.socketpair``s and
    close them with a bare ``socket.close()`` -- which bypasses the
    ``netpoll_unregister`` that ALL real runloom close paths (monkey sockets,
    the aio bridge, TCPConn, osio/polling/sync) perform.  Under EPOLLET
    register-once that unregister is load-bearing: a reused fd NUMBER whose
    stale registration bit is still set would skip its ``EPOLL_CTL_ADD`` and the
    new socket would never be armed (a hang).  Real code never leaks this (it
    unregisters on close); these tests must mimic that, so reset the residue
    between tests.  (Internal scheduler fds -- the epoll fd, self-pipe,
    io_uring eventfd -- are added to epoll directly, not through the
    registration-bit path, so clearing the bit range does not touch them.)
    """
    yield
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:       # noqa: BLE001
            pass


def _drain(sock):
    """Read everything currently readable from a non-blocking socket."""
    total = 0
    try:
        while True:
            b = sock.recv(65536)
            if not b:          # peer closed (EOF) -- stop, still "readable"
                break
            total += len(b)
    except BlockingIOError:
        pass
    return total


def _pair_nonblocking_read():
    r, w = socket.socketpair()
    r.setblocking(False)
    return r, w


# ---------------------------------------------------------------------------
# Re-arm after consume: the headline regression (EPOLLET edge-drop).
# ---------------------------------------------------------------------------

def _run_rearm(n_edges, timeout_ms=TIMEOUT_MS):
    """Wake a single goroutine exactly `n_edges` times, one readable edge each,
    sequenced by a cross-thread ACK so edges never coalesce.  Returns the list
    of wait_fd return values (expected: [READ] * n_edges)."""
    data_r, data_w = _pair_nonblocking_read()
    ack_r, ack_w = socket.socketpair()        # waiter -> feeder handshake
    woke = []
    feeder_err = []

    def feeder():
        try:
            for _ in range(n_edges):
                data_w.send(b"x")             # produce one readable edge
                if ack_r.recv(1) != b"a":     # block until waiter consumed it
                    raise RuntimeError("bad ack")
        except Exception as e:                # noqa: BLE001 - reported to test
            feeder_err.append(e)

    t = threading.Thread(target=feeder, daemon=True)
    t.start()

    def waiter():
        for _ in range(n_edges):
            m = runloom_c.wait_fd(data_r.fileno(), READ, timeout_ms)
            woke.append(m)
            if m & READ:
                _drain(data_r)
            ack_w.send(b"a")                  # release the feeder for edge N+1

    runloom_c.go(waiter)
    runloom_c.run()
    t.join(timeout=10)
    try:
        assert not t.is_alive(), "feeder thread did not finish"
        assert not feeder_err, "feeder error: %r" % (feeder_err,)
    finally:
        for s in (data_r, data_w, ack_r, ack_w):
            s.close()
    return woke


@pytest.mark.parametrize("n_edges", [1, 2, 8, 32])
def test_rearm_after_consume(n_edges):
    """Each consumed-then-readable-again edge must re-fire.  The EPOLLET
    register-once scheme dropped these -> a 0 (timeout) would appear."""
    woke = _run_rearm(n_edges)
    assert woke == [READ] * n_edges, (
        "dropped/extra edge on %s backend: %r" % (BACKEND, woke))


# ---------------------------------------------------------------------------
# Readiness already present at arm time (level / pending-bitmap path).
# ---------------------------------------------------------------------------

def test_ready_before_park_returns_immediately():
    """Data written before wait_fd is ever called must be reported on arm --
    not block waiting for a fresh edge that already happened."""
    data_r, data_w = _pair_nonblocking_read()
    data_w.send(b"x")                         # readable BEFORE any park/arm
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(data_r.fileno(), READ, TIMEOUT_MS))

    runloom_c.go(waiter)
    runloom_c.run()
    data_r.close(); data_w.close()
    assert got == [READ], "ready-at-arm not reported on %s: %r" % (BACKEND, got)


def test_write_side_is_ready_immediately():
    """A fresh stream socket is writable; wait_fd(WRITE) must return at once."""
    a, b = socket.socketpair()
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(a.fileno(), WRITE, TIMEOUT_MS))

    runloom_c.go(waiter)
    runloom_c.run()
    a.close(); b.close()
    assert got and (got[0] & WRITE), "writable arm not reported: %r" % got


def test_combined_read_write_returns_write_subset():
    """wait_fd(READ|WRITE) on a writable-but-not-readable socket returns a
    subset that includes WRITE and excludes READ (no spurious READ)."""
    a, b = socket.socketpair()
    a.setblocking(False)
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(a.fileno(), READ | WRITE, TIMEOUT_MS))

    runloom_c.go(waiter)
    runloom_c.run()
    a.close(); b.close()
    assert got, "no return"
    assert got[0] & WRITE, "WRITE not reported: %r" % got
    assert not (got[0] & READ), "spurious READ on idle socket: %r" % got


# ---------------------------------------------------------------------------
# Negative control: never-ready fd must TIME OUT, not wake.
# ---------------------------------------------------------------------------

def test_never_ready_times_out():
    """A fd that never becomes readable must return 0 at the timeout -- this is
    the control that proves the suite can tell 'woke' from 'did not wake'."""
    data_r, data_w = _pair_nonblocking_read()
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(data_r.fileno(), READ, 200))

    runloom_c.go(waiter)
    runloom_c.run()
    data_r.close(); data_w.close()
    assert got == [0], "expected timeout (0), got %r on %s" % (got, BACKEND)


# ---------------------------------------------------------------------------
# Peer close while armed: EOF is a readable edge and must wake (EPOLLRDHUP/HUP).
# ---------------------------------------------------------------------------

def test_peer_close_wakes_reader():
    """Closing the write end while a goroutine is parked on the read end is a
    readable (EOF) edge -- the parked wait_fd must wake, not hang to timeout."""
    data_r, data_w = _pair_nonblocking_read()
    woke = []
    ready = threading.Event()

    def closer():
        ready.wait(5)
        data_w.close()                        # peer close -> read end gets EOF

    t = threading.Thread(target=closer, daemon=True)
    t.start()

    def waiter():
        ready.set()                           # tell the closer we're about to park
        m = runloom_c.wait_fd(data_r.fileno(), READ, TIMEOUT_MS)
        woke.append(m)

    runloom_c.go(waiter)
    runloom_c.run()
    t.join(5)
    data_r.close()
    assert woke == [READ], (
        "peer-close (EOF) did not wake reader on %s: %r" % (BACKEND, woke))


# ---------------------------------------------------------------------------
# Closing the *armed* fd itself: the footgun must degrade to a clean timeout,
# never a crash or a leaked parker.
# ---------------------------------------------------------------------------

def test_close_armed_fd_degrades_to_timeout():
    """If the very fd a goroutine is parked on is closed out from under it, the
    kernel drops it from the poll set and no event ever comes; the park must
    still terminate via its timeout (bounded), not hang or crash.

    Unlike the readiness-edge tests above, the close runs from a SECOND
    GOROUTINE rather than an OS thread.  The reason is a registration race: an
    OS thread that closes data_r can win against wait_fd's fd registration, so
    the register kevent() hits EBADF and wait_fd *raises* instead of degrading
    to a timeout (the bug this used to flake on -- got == []).  The cooperative
    scheduler removes the race: the closer yields once, the waiter runs and
    parks (its fd now registered), then the closer resumes and closes -- so the
    close is always "fd closed while parked", which is exactly the contract
    under test.
    """
    data_r, data_w = _pair_nonblocking_read()
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(data_r.fileno(), READ, 400))

    def closer():
        runloom_c.sched_yield()               # let the waiter register its fd + park
        data_r.close()                        # close the fd we're parked on

    runloom_c.go(waiter)
    runloom_c.go(closer)
    runloom_c.run()
    data_w.close()
    assert got == [0], "closing the armed fd should time out cleanly: %r" % got


# ---------------------------------------------------------------------------
# Many waiters on distinct fds: no missed wake, no cross-talk.
# ---------------------------------------------------------------------------

def test_concurrent_distinct_fds_thundering():
    """N goroutines each parked on their own fd, all made readable at once.
    Every waiter must wake exactly once -- batch delivery + per-fd bucket
    dispatch must not miss or cross-deliver."""
    N = 16
    pairs = [_pair_nonblocking_read() for _ in range(N)]
    woke = [[] for _ in range(N)]
    started = threading.Event()

    def feeder():
        started.wait(5)
        for _, w in pairs:
            w.send(b"x")                      # all N fds readable at once

    t = threading.Thread(target=feeder, daemon=True)
    t.start()

    def make_waiter(i):
        def waiter():
            r, _ = pairs[i]
            if i == 0:
                started.set()                 # release the feeder once gs exist
            m = runloom_c.wait_fd(r.fileno(), READ, TIMEOUT_MS)
            woke[i].append(m)
            if m & READ:
                _drain(r)
        return waiter

    for i in range(N):
        runloom_c.go(make_waiter(i))
    runloom_c.run()
    t.join(5)
    for r, w in pairs:
        r.close(); w.close()
    assert all(woke[i] == [READ] for i in range(N)), (
        "missed/cross wake on %s: %r" % (BACKEND, woke))


def test_concurrent_distinct_fds_staggered():
    """Same N waiters, but woken one at a time with a per-fd handshake: waking
    one parker must leave the other N-1 parked and intact."""
    N = 8
    pairs = [_pair_nonblocking_read() for _ in range(N)]
    acks = [socket.socketpair() for _ in range(N)]
    woke = [[] for _ in range(N)]
    feeder_err = []

    def feeder():
        try:
            for i, (_, w) in enumerate(pairs):
                w.send(b"x")
                if acks[i][0].recv(1) != b"a":
                    raise RuntimeError("bad ack %d" % i)
        except Exception as e:                # noqa: BLE001
            feeder_err.append(e)

    t = threading.Thread(target=feeder, daemon=True)
    t.start()

    def make_waiter(i):
        def waiter():
            r, _ = pairs[i]
            m = runloom_c.wait_fd(r.fileno(), READ, TIMEOUT_MS)
            woke[i].append(m)
            if m & READ:
                _drain(r)
            acks[i][1].send(b"a")
        return waiter

    for i in range(N):
        runloom_c.go(make_waiter(i))
    runloom_c.run()
    t.join(10)
    for r, w in pairs:
        r.close(); w.close()
    for ar, aw in acks:
        ar.close(); aw.close()
    assert not feeder_err, "feeder error: %r" % (feeder_err,)
    assert all(woke[i] == [READ] for i in range(N)), (
        "staggered wake missed a parker on %s: %r" % (BACKEND, woke))
