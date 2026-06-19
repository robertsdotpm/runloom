"""Cooperative `selectors` / select.poll / select.epoll / select.kqueue.

Adapted from CPython's Lib/test/test_selectors.py (BaseSelectorTestCase)
and the readiness-event matrix in libuv's test/test-poll.c.  The point is to
prove that runloom.monkey's `selectors` category makes the high-level selector
API cooperative *without changing its observable contract*: the same
(key, events) return shape, the same EVENT_READ / EVENT_WRITE / POLLHUP
return codes, the same KeyError / ValueError fault behaviour -- but a
fiber blocked in select() now yields the OS thread to its siblings
instead of freezing the scheduler.

Run under the C scheduler (runloom_c.fiber / runloom_c.run), which is the path
the monkey-patches target.
"""
import errno
import os
import platform
import select
import selectors
import socket
import time
import unittest

import runloom
import runloom.monkey
import runloom_c

_IS_WINDOWS = platform.system() == "Windows"


def _drive(fn):
    """Run fn() as a fiber, return its value (or re-raise)."""
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001 - propagate to the test
            box[1] = e

    runloom_c.fiber(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    runloom.monkey.patch()


def tearDownModule():
    runloom.monkey.unpatch()


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


class TestSelectorsContract(unittest.TestCase):
    """Register / modify / unregister / get_key surface (CPython parity)."""

    def test_register_returns_selectorkey(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            try:
                key = sel.register(a, selectors.EVENT_READ, "the-data")
                self.assertEqual(key.fileobj, a)
                self.assertEqual(key.fd, a.fileno())
                self.assertEqual(key.events, selectors.EVENT_READ)
                self.assertEqual(key.data, "the-data")
                self.assertIs(sel.get_key(a), key)
            finally:
                sel.close(); a.close(); b.close()
        _drive(body)

    def test_reregister_raises_keyerror(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            try:
                sel.register(a, selectors.EVENT_READ)
                with self.assertRaises(KeyError):
                    sel.register(a, selectors.EVENT_READ)
            finally:
                sel.close(); a.close(); b.close()
        _drive(body)

    def test_unregister_unknown_raises_keyerror(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            try:
                with self.assertRaises(KeyError):
                    sel.unregister(a)
            finally:
                sel.close(); a.close(); b.close()
        _drive(body)

    def test_register_no_events_raises_valueerror(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            try:
                with self.assertRaises(ValueError):
                    sel.register(a, 0)
            finally:
                sel.close(); a.close(); b.close()
        _drive(body)

    def test_modify_switches_interest(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            try:
                sel.register(a, selectors.EVENT_READ)
                key = sel.modify(a, selectors.EVENT_WRITE)
                self.assertEqual(key.events, selectors.EVENT_WRITE)
                # a fresh, writable, empty socket -> WRITE ready, not READ.
                ready = sel.select(timeout=1.0)
                self.assertEqual(len(ready), 1)
                k, mask = ready[0]
                self.assertTrue(mask & selectors.EVENT_WRITE)
                self.assertFalse(mask & selectors.EVENT_READ)
            finally:
                sel.close(); a.close(); b.close()
        _drive(body)


class TestSelectorsReadiness(unittest.TestCase):
    """select() return codes for each readiness state (libuv poll matrix)."""

    def test_read_ready_after_peer_write(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            sel.register(a, selectors.EVENT_READ, "a")
            order = []

            def writer():
                time.sleep(0.02)
                order.append("write")
                b.send(b"payload")

            runloom_c.fiber(writer)
            t0 = time.monotonic()
            ready = sel.select(timeout=2.0)
            dt = time.monotonic() - t0
            order.append("woke")
            data = a.recv(16)
            sel.close(); a.close(); b.close()
            self.assertEqual(len(ready), 1)
            key, mask = ready[0]
            self.assertEqual(key.data, "a")
            self.assertEqual(mask, selectors.EVENT_READ)
            self.assertEqual(data, b"payload")
            # Woke on the event, not on a busy-poll timeout.
            self.assertLess(dt, 1.5)
            # The writer fiber ran *while* select() was parked.
            self.assertEqual(order, ["write", "woke"])
        _drive(body)

    def test_timeout_returns_empty(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            sel.register(a, selectors.EVENT_READ)
            t0 = time.monotonic()
            ready = sel.select(timeout=0.05)   # nobody writes
            dt = time.monotonic() - t0
            sel.close(); a.close(); b.close()
            self.assertEqual(ready, [])
            self.assertGreaterEqual(dt, 0.04)
        _drive(body)

    def test_nonblocking_zero_timeout(self):
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            sel.register(a, selectors.EVENT_READ)
            self.assertEqual(sel.select(timeout=0), [])   # nothing ready
            b.send(b"x")
            # epoll level-triggered: ready immediately on the next poll.
            ready = sel.select(timeout=0)
            sel.close(); a.close(); b.close()
            self.assertEqual(len(ready), 1)
        _drive(body)

    def test_read_and_write_mask(self):
        def body():
            a, b = _pair()
            b.send(b"hi")          # make `a` readable
            time.sleep(0.005)
            sel = selectors.DefaultSelector()
            sel.register(a, selectors.EVENT_READ | selectors.EVENT_WRITE)
            ready = sel.select(timeout=1.0)
            sel.close(); a.close(); b.close()
            return ready
        ready = _drive(body)
        # epoll coalesces a fd's read+write readiness into one (key, READ|WRITE)
        # entry; kqueue uses separate EVFILT_READ/EVFILT_WRITE filters and the
        # stdlib KqueueSelector returns ONE entry per filter (it does NOT
        # coalesce -- verified against stock selectors on macOS).  So assert the
        # union of masks across all entries has both bits, backend-agnostically.
        self.assertGreaterEqual(len(ready), 1)
        combined = 0
        for _, mask in ready:
            combined |= mask
        self.assertTrue(combined & selectors.EVENT_READ)
        self.assertTrue(combined & selectors.EVENT_WRITE)

    def test_many_fds_only_ready_returned(self):
        def body():
            pairs = [_pair() for _ in range(16)]
            sel = selectors.DefaultSelector()
            for i, (a, _b) in enumerate(pairs):
                sel.register(a, selectors.EVENT_READ, i)
            # Make exactly fds 3, 7, 11 readable.
            for i in (3, 7, 11):
                pairs[i][1].send(b"z")
            time.sleep(0.01)
            ready = sel.select(timeout=1.0)
            got = sorted(k.data for k, _ in ready)
            sel.close()
            for a, b in pairs:
                a.close(); b.close()
            self.assertEqual(got, [3, 7, 11])
        _drive(body)


class TestSelectorsConcurrency(unittest.TestCase):
    """Two fibers parked in select() must overlap, not serialize."""

    def test_parallel_selects_overlap(self):
        def body():
            results = []

            def one(idx):
                a, b = _pair()
                sel = selectors.DefaultSelector()
                sel.register(a, selectors.EVENT_READ)

                def w():
                    time.sleep(0.05)
                    b.send(b"go")

                runloom_c.fiber(w)
                sel.select(timeout=2.0)
                a.recv(4)
                sel.close(); a.close(); b.close()
                results.append(idx)

            t0 = time.monotonic()
            g_done = []
            for i in range(2):
                runloom_c.fiber(lambda i=i: (one(i), g_done.append(1)))
            # Spin the driving fiber until both children finish.
            while len(g_done) < 2:
                runloom.sleep(0.005)
            return time.monotonic() - t0

        elapsed = _drive(body)
        # Two independent 0.05s waits, run cooperatively, finish in ~0.05s,
        # nowhere near the 0.10s a serial implementation would take.
        self.assertLess(elapsed, 0.09)


class TestSelectPollDirect(unittest.TestCase):
    """select.poll() directly -- POLLIN / POLLOUT / POLLHUP return codes."""

    @unittest.skipUnless(hasattr(select, "poll"), "no select.poll on this OS")
    def test_pollin_after_write(self):
        def body():
            a, b = _pair()
            p = select.poll()
            p.register(a.fileno(), select.POLLIN)

            def w():
                time.sleep(0.02)
                b.send(b"data")

            runloom_c.fiber(w)
            evts = p.poll(2000)         # milliseconds
            d = a.recv(8)
            a.close(); b.close()
            self.assertEqual(len(evts), 1)
            fd, revents = evts[0]
            self.assertTrue(revents & select.POLLIN)
            self.assertEqual(d, b"data")
        _drive(body)

    @unittest.skipUnless(hasattr(select, "poll"), "no select.poll on this OS")
    def test_pollout_ready_immediately(self):
        def body():
            a, b = _pair()
            p = select.poll()
            p.register(a.fileno(), select.POLLOUT)
            evts = p.poll(1000)
            a.close(); b.close()
            self.assertEqual(len(evts), 1)
            self.assertTrue(evts[0][1] & select.POLLOUT)
        _drive(body)

    @unittest.skipUnless(hasattr(select, "poll"), "no select.poll on this OS")
    def test_poll_timeout_empty(self):
        def body():
            a, b = _pair()
            p = select.poll()
            p.register(a.fileno(), select.POLLIN)
            t0 = time.monotonic()
            evts = p.poll(50)
            dt = time.monotonic() - t0
            a.close(); b.close()
            self.assertEqual(evts, [])
            self.assertGreaterEqual(dt, 0.04)
        _drive(body)

    @unittest.skipUnless(hasattr(select, "poll"), "no select.poll on this OS")
    def test_pollhup_on_peer_close(self):
        """Fault injection: peer closes -> POLLHUP/POLLIN surfaces."""
        def body():
            a, b = _pair()
            p = select.poll()
            p.register(a.fileno(), select.POLLIN)
            b.close()                  # half the connection vanishes
            time.sleep(0.01)
            evts = p.poll(1000)
            a.close()
            self.assertEqual(len(evts), 1)
            # A closed peer surfaces as readable (EOF) and/or HUP.
            self.assertTrue(evts[0][1] & (select.POLLIN | select.POLLHUP))
        _drive(body)


@unittest.skipUnless(hasattr(select, "epoll"), "epoll is Linux-only")
class TestSelectEpollDirect(unittest.TestCase):
    """select.epoll() directly -- the event-driven backing-fd path."""

    def test_epoll_wakes_on_data(self):
        def body():
            a, b = _pair()
            ep = select.epoll()
            ep.register(a.fileno(), select.EPOLLIN)

            def w():
                time.sleep(0.02)
                b.send(b"epoll")

            runloom_c.fiber(w)
            evts = ep.poll(timeout=2.0)   # seconds
            d = a.recv(8)
            ep.close(); a.close(); b.close()
            self.assertEqual(len(evts), 1)
            self.assertTrue(evts[0][1] & select.EPOLLIN)
            self.assertEqual(d, b"epoll")
        _drive(body)

    def test_epoll_context_manager_and_fileno(self):
        def body():
            with select.epoll() as ep:
                self.assertIsInstance(ep.fileno(), int)
                self.assertGreaterEqual(ep.fileno(), 0)
            return True
        self.assertTrue(_drive(body))

    def test_epoll_timeout_empty(self):
        def body():
            a, b = _pair()
            ep = select.epoll()
            ep.register(a.fileno(), select.EPOLLIN)
            t0 = time.monotonic()
            evts = ep.poll(timeout=0.05)
            dt = time.monotonic() - t0
            ep.close(); a.close(); b.close()
            self.assertEqual(evts, [])
            self.assertGreaterEqual(dt, 0.04)
        _drive(body)


class TestSelectorsFaultInjection(unittest.TestCase):
    """Closed / bad fds must fail the same way the stdlib selector does."""

    def test_select_after_close_is_safe(self):
        """Registering then closing the fileobj, then select(), must not
        wedge the scheduler -- it returns or raises, but always returns
        control (the bug class this guards: parking forever on a dead fd)."""
        def body():
            a, b = _pair()
            sel = selectors.DefaultSelector()
            sel.register(a, selectors.EVENT_READ)
            a.close()                  # fd pulled out from under the selector
            raised = False
            try:
                sel.select(timeout=0.1)
            except OSError:
                raised = True
            sel.close(); b.close()
            return raised
        # Either it raises OSError (EBADF) or returns -- both are acceptable;
        # the test passing at all proves it did not hang.
        _drive(body)

    @unittest.skipUnless(hasattr(select, "poll"), "no select.poll on this OS")
    def test_poll_negative_fd_rejected(self):
        def body():
            p = select.poll()
            with self.assertRaises((OSError, ValueError)):
                p.register(-1, select.POLLIN)
                p.poll(0)
        _drive(body)


if __name__ == "__main__":
    unittest.main()
