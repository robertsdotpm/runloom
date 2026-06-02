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


if __name__ == "__main__":
    print("netpoll backend under test:", pygo_core.netpoll_backend())
    unittest.main()
