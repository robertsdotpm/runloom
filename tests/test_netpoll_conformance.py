"""Backend-agnostic netpoll readiness-conformance suite.

These are the universal event-notification semantics that EVERY pygo netpoll
backend must satisfy -- epoll (Linux), kqueue (FreeBSD/macOS), and the three
Windows backends (iocp-afd / wsapoll / select).  The scenarios are the
distilled "standardized" set from the Linux kernel's own epoll selftest
(tools/testing/selftests/filesystems/epoll/epoll_wakeup_test.c) and libkqueue's
regression suite (read/write/EOF/timer): ready-before-park, park-then-ready,
write readiness, R|W subset, deadline/timeout, peer-close EOF, re-arm after
consume (the edge-triggered drop class), and many concurrent waiters.

This is the "are we doing it right?" suite: run it on any OS, force any backend
with PYGO_NETPOLL=epoll|kqueue|iocp|wsapoll|select, and the SAME assertions must
hold.  Backend-portable on purpose -- it asserts BEHAVIOUR through real sockets
(socketpair) + pygo_core.wait_fd, never a backend-specific internal.

wait_fd(fd, events, timeout_ms) contract (verified against netpoll.c):
  returns the ready mask (1=READ, 2=WRITE, 3=both) when fd becomes ready,
  0 on the timeout/deadline, raises OSError on a hard error.
"""
import socket
import sys
import unittest

sys.path.insert(0, "src")

import pygo_core

READ = 1
WRITE = 2


def _drive(*goroutines):
    """Spawn each callable as a goroutine, run the single-thread scheduler,
    re-raise the first exception any goroutine hit (so asserts surface)."""
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:  # noqa: BLE001
                box.append(e)
        return runner

    for g in goroutines:
        pygo_core.go(wrap(g))
    pygo_core.run()
    if box:
        raise box[0]


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


class TestNetpollConformance(unittest.TestCase):
    """Every test_* here must pass identically on every backend."""

    def setUp(self):
        # Surface which backend is under test in failure output.
        self.backend = pygo_core.netpoll_backend()

    # -- ready BEFORE park: an already-readable fd returns immediately --------
    def test_ready_before_park(self):
        a, b = _pair()
        b.send(b"x")                       # a is readable before we park
        out = []
        _drive(lambda: out.append(pygo_core.wait_fd(a.fileno(), READ, 1000)))
        self.assertEqual(out, [READ], "backend=%s" % self.backend)
        a.close(); b.close()

    # -- park THEN ready: parked reader wakes when the peer writes ------------
    def test_park_then_ready(self):
        a, b = _pair()
        out = []

        def reader():
            out.append(pygo_core.wait_fd(a.fileno(), READ, 2000))

        def writer():
            pygo_core.sched_yield()        # let the reader park first
            b.send(b"hello")

        _drive(reader, writer)
        self.assertEqual(out, [READ], "backend=%s" % self.backend)
        self.assertEqual(a.recv(16), b"hello")
        a.close(); b.close()

    # -- write readiness: an empty-send-buffer socket is writable ------------
    def test_write_ready(self):
        a, b = _pair()
        out = []
        _drive(lambda: out.append(pygo_core.wait_fd(a.fileno(), WRITE, 1000)))
        self.assertEqual(out, [WRITE], "backend=%s" % self.backend)
        a.close(); b.close()

    # -- R|W subset: request both, only WRITE is ready -> get WRITE only ------
    def test_rw_subset_returns_write_only(self):
        a, b = _pair()                     # a: writable, not readable (no data)
        out = []
        _drive(lambda: out.append(
            pygo_core.wait_fd(a.fileno(), READ | WRITE, 1000)))
        self.assertEqual(out, [WRITE], "backend=%s" % self.backend)
        a.close(); b.close()

    # -- deadline/timeout: a never-ready fd wakes via its deadline (==0) ------
    #    (This is the scenario that caught the Windows wsapoll deadline hang.)
    def test_timeout_deadline_wakes(self):
        a, b = _pair()                     # nothing ever written to a
        out = []
        _drive(lambda: out.append(pygo_core.wait_fd(a.fileno(), READ, 250)))
        self.assertEqual(out, [0], "deadline did not fire, backend=%s"
                         % self.backend)
        a.close(); b.close()

    # -- peer-close EOF: a closed peer makes the fd readable (EOF) ------------
    def test_peer_close_eof(self):
        a, b = _pair()
        out = []

        def reader():
            out.append(pygo_core.wait_fd(a.fileno(), READ, 2000))

        def closer():
            pygo_core.sched_yield()
            b.close()                      # EOF -> a readable

        _drive(reader, closer)
        self.assertEqual(out, [READ], "backend=%s" % self.backend)
        self.assertEqual(a.recv(16), b"")   # EOF
        a.close()

    # -- re-arm after consume: ready, consume, re-park, ready again ----------
    #    The edge-triggered drop regression (epoll EPOLLET / kqueue EV_CLEAR):
    #    a backend that armed once and never refired would hang the 2nd park.
    def test_rearm_after_consume(self):
        a, b = _pair()
        ready = pygo_core.Chan()            # reader -> writer handshake
        out = []

        def reader():
            r1 = pygo_core.wait_fd(a.fileno(), READ, 2000)
            a.recv(16)                      # consume -> not readable
            ready.send(1)                   # "consumed; re-parking now"
            r2 = pygo_core.wait_fd(a.fileno(), READ, 2000)
            a.recv(16)
            out.append((r1, r2))

        def writer():
            pygo_core.sched_yield()
            b.send(b"one")
            ready.recv()                    # wait until reader re-armed
            b.send(b"two")                  # second edge -> must refire

        _drive(reader, writer)
        self.assertEqual(out, [(READ, READ)],
                         "re-arm dropped, backend=%s" % self.backend)
        a.close(); b.close()

    # -- many concurrent waiters: N fds, N parked readers, all wake ----------
    def test_many_waiters(self):
        N = 16
        pairs = [_pair() for _ in range(N)]
        woke = []

        def make_reader(i, a):
            def run():
                r = pygo_core.wait_fd(a.fileno(), READ, 3000)
                if r == READ:
                    woke.append(i)
            return run

        def writer():
            pygo_core.sched_yield()         # let all readers park
            for _a, b in pairs:
                b.send(b"!")

        gs = [make_reader(i, a) for i, (a, _b) in enumerate(pairs)]
        _drive(*gs, writer)
        self.assertEqual(sorted(woke), list(range(N)),
                         "not all waiters woke, backend=%s" % self.backend)
        for a, b in pairs:
            a.close(); b.close()

    # ======================================================================
    # Additional edge cases drawn from wepoll's + mio's + libuv's poll test
    # suites -- the corners where the kqueue fd-reuse and iocp AFD-timeout
    # bugs lived.  Same contract: identical result on every backend.
    # ======================================================================

    # -- LEVEL readiness persists across a PARTIAL consume (wepoll level vs.
    #    edge; mio re-register).  Peer sends TWO messages; the reader consumes
    #    only ONE, then re-parks -- a level-triggered backend (or a correct
    #    one-shot re-arm) must report the still-buffered data again.  An
    #    edge-triggered drop (the kqueue bug class) would hang the 2nd park.
    def test_level_readiness_persists_after_partial_consume(self):
        a, b = _pair()
        ready = pygo_core.Chan()
        out = []

        def reader():
            r1 = pygo_core.wait_fd(a.fileno(), READ, 2000)
            a.recv(1)                       # consume only ONE byte of two
            ready.send(1)
            r2 = pygo_core.wait_fd(a.fileno(), READ, 2000)
            a.recv(1)
            out.append((r1, r2))

        def writer():
            pygo_core.sched_yield()
            b.send(b"AB")                   # two bytes; reader takes one at a time
            ready.recv()                    # reader consumed one + re-parked

        _drive(reader, writer)
        self.assertEqual(out, [(READ, READ)],
                         "buffered data not re-reported on re-park (edge drop), "
                         "backend=%s" % self.backend)
        a.close(); b.close()

    # -- re-arm in a DIFFERENT direction on the same fd (mio reregister): park
    #    READ (wake on data), then park WRITE on the same fd -> WRITE.  A
    #    per-direction one-shot must switch direction on re-arm.
    def test_reregister_different_direction(self):
        a, b = _pair()
        out = []

        def reader():
            r1 = pygo_core.wait_fd(a.fileno(), READ, 2000)
            a.recv(16)
            r2 = pygo_core.wait_fd(a.fileno(), WRITE, 2000)   # now ask WRITE
            out.append((r1, r2))

        def writer():
            pygo_core.sched_yield()
            b.send(b"x")

        _drive(reader, writer)
        self.assertEqual(out, [(READ, WRITE)],
                         "direction not switched on re-arm, backend=%s"
                         % self.backend)
        a.close(); b.close()

    # -- NO spurious wake on the wrong direction (mio/wepoll): a socket is
    #    always writable but not readable; a READ-only wait must NOT fire on
    #    writability -- it wakes only via its deadline (0).  (Inverse of the
    #    R|W-subset test: proves the backend masks to the REQUESTED direction.)
    def test_no_spurious_wake_on_unrequested_direction(self):
        a, b = _pair()                      # a writable, never readable
        out = []
        _drive(lambda: out.append(pygo_core.wait_fd(a.fileno(), READ, 250)))
        self.assertEqual(out, [0],
                         "READ-only wait fired on writability (wrong direction), "
                         "backend=%s" % self.backend)
        a.close(); b.close()

    # -- BOTH directions ready at once: request R|W on a socket that is both
    #    readable (peer wrote) and writable.  A backend that COALESCES readiness
    #    (epoll / iocp-afd / wsapoll / select) reports both at once (R|W);
    #    kqueue delivers EVFILT_READ and EVFILT_WRITE as SEPARATE events, so a
    #    combined wait legitimately returns just one direction (the caller gets
    #    the other on its next wait).  The universal contract is therefore: a
    #    non-empty subset of the requested directions, all of which are ready.
    def test_both_directions_ready(self):
        a, b = _pair()
        b.send(b"x")                        # a now readable AND writable
        out = []
        _drive(lambda: out.append(
            pygo_core.wait_fd(a.fileno(), READ | WRITE, 1000)))
        self.assertIn(out[0], (READ, WRITE, READ | WRITE),
                      "both-ready reported nothing/garbage (%r), backend=%s"
                      % (out, self.backend))
        a.close(); b.close()

    # -- repeated rapid re-arm on the SAME fd (mio oneshot storm): many
    #    park->ready->consume cycles must each fire -- a re-arm that leaks or
    #    drops after the first delivery hangs partway through.
    def test_repeated_rearm_storm(self):
        a, b = _pair()
        ready = pygo_core.Chan()
        ROUNDS = 50
        out = []

        def reader():
            n = 0
            for _ in range(ROUNDS):
                if pygo_core.wait_fd(a.fileno(), READ, 2000) == READ:
                    a.recv(1)
                    n += 1
                ready.send(1)
            out.append(n)

        def writer():
            for _ in range(ROUNDS):
                pygo_core.sched_yield()
                b.send(b"x")
                ready.recv()                # one cycle done; reader re-parked

        _drive(reader, writer)
        self.assertEqual(out, [ROUNDS],
                         "re-arm dropped mid-storm, backend=%s" % self.backend)
        a.close(); b.close()

    # -- many fds, MIXED directions (libuv multi-poll): half park READ (peer
    #    writes), half park WRITE (already writable); every one wakes with its
    #    own correct direction.
    def test_many_fds_mixed_directions(self):
        N = 12
        pairs = [_pair() for _ in range(N)]
        got = {}

        def make_reader(i, a):
            def run():
                got[i] = pygo_core.wait_fd(a.fileno(), READ, 3000)
            return run

        def make_writer_waiter(i, a):
            def run():
                got[i] = pygo_core.wait_fd(a.fileno(), WRITE, 3000)
            return run

        def feeder():
            pygo_core.sched_yield()
            for i, (_a, b) in enumerate(pairs):
                if i % 2 == 0:
                    b.send(b"!")            # make the even ones readable

        gs = []
        for i, (a, _b) in enumerate(pairs):
            gs.append((make_reader if i % 2 == 0 else make_writer_waiter)(i, a))
        _drive(*gs, feeder)
        expect = {i: (READ if i % 2 == 0 else WRITE) for i in range(N)}
        self.assertEqual(got, expect,
                         "mixed-direction multi-poll wrong, backend=%s"
                         % self.backend)
        for a, b in pairs:
            a.close(); b.close()


if __name__ == "__main__":
    print("netpoll backend under test:", pygo_core.netpoll_backend())
    unittest.main()
