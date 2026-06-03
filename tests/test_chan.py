"""Tests for runloom_c.Chan -- Go-style channels."""
import sys
import time
import unittest

sys.path.insert(0, "src")

import runloom_c


def _run_in_sched(*goroutines):
    """Spawn each callable, run scheduler to completion."""
    for g in goroutines:
        runloom_c.go(g)
    runloom_c.run()


class TestUnbufferedBasic(unittest.TestCase):
    def test_send_then_recv(self):
        ch = runloom_c.Chan()
        out = []

        def producer():
            ch.send(7)

        def consumer():
            v, ok = ch.recv()
            out.append((v, ok))

        _run_in_sched(producer, consumer)
        self.assertEqual(out, [(7, True)])

    def test_recv_then_send_blocks_in_right_order(self):
        ch = runloom_c.Chan()
        log = []

        def consumer():
            log.append("c-wait")
            v, ok = ch.recv()
            log.append(("c-got", v))

        def producer():
            log.append("p-start")
            ch.send("hi")
            log.append("p-done")

        _run_in_sched(consumer, producer)
        # Consumer should park first (no sender yet); producer runs,
        # hands off the value, consumer wakes with it.
        self.assertEqual(log[0], "c-wait")
        self.assertIn(("c-got", "hi"), log)


class TestBuffered(unittest.TestCase):
    def test_buffer_fills_drains(self):
        ch = runloom_c.Chan(3)
        out = []

        def writer():
            for i in range(5):
                ch.send(i)

        def reader():
            for _ in range(5):
                v, ok = ch.recv()
                out.append(v)

        _run_in_sched(writer, reader)
        self.assertEqual(out, [0, 1, 2, 3, 4])

    def test_len_capacity(self):
        ch = runloom_c.Chan(4)
        out = []
        def runner():
            out.append(ch.capacity)
            ch.send("a")
            ch.send("b")
            out.append(len(ch))
            v, _ = ch.recv()
            out.append(v)
            out.append(len(ch))
        _run_in_sched(runner)
        self.assertEqual(out, [4, 2, "a", 1])


class TestClose(unittest.TestCase):
    def test_recv_after_close_returns_ok_false(self):
        ch = runloom_c.Chan()
        out = []
        def runner():
            ch.close()
            v, ok = ch.recv()
            out.append((v, ok))
        _run_in_sched(runner)
        self.assertEqual(out, [(None, False)])

    def test_buffered_drains_after_close(self):
        ch = runloom_c.Chan(2)
        out = []
        def runner():
            ch.send(10)
            ch.send(20)
            ch.close()
            for _ in range(3):
                v, ok = ch.recv()
                out.append((v, ok))
        _run_in_sched(runner)
        self.assertEqual(out, [(10, True), (20, True), (None, False)])

    def test_send_on_closed_raises(self):
        ch = runloom_c.Chan()
        err = []
        def runner():
            ch.close()
            try:
                ch.send(1)
            except ValueError as e:
                err.append(str(e))
        _run_in_sched(runner)
        self.assertEqual(err, ["send on closed channel"])

    def test_double_close_raises(self):
        ch = runloom_c.Chan()
        err = []
        def runner():
            ch.close()
            try:
                ch.close()
            except ValueError as e:
                err.append(str(e))
        _run_in_sched(runner)
        self.assertEqual(err, ["close on closed channel"])

    def test_close_wakes_parked_senders(self):
        ch = runloom_c.Chan()      # unbuffered
        log = []

        def sender():
            try:
                ch.send("x")
                log.append("sent")
            except ValueError:
                log.append("closed")

        def closer():
            # Yield so sender gets to park first.
            runloom_c.sched_yield()
            ch.close()

        _run_in_sched(sender, closer)
        self.assertEqual(log, ["closed"])

    def test_close_wakes_parked_receivers(self):
        ch = runloom_c.Chan()
        log = []
        def receiver():
            v, ok = ch.recv()
            log.append((v, ok))
        def closer():
            runloom_c.sched_yield()
            ch.close()
        _run_in_sched(receiver, closer)
        self.assertEqual(log, [(None, False)])


class TestNonBlocking(unittest.TestCase):
    def test_try_send_full_returns_false(self):
        ch = runloom_c.Chan(1)
        out = []
        def runner():
            out.append(ch.try_send(1))   # True (room)
            out.append(ch.try_send(2))   # False (full)
        _run_in_sched(runner)
        self.assertEqual(out, [True, False])

    def test_try_recv_empty_returns_none(self):
        ch = runloom_c.Chan(1)
        out = []
        def runner():
            out.append(ch.try_recv())    # None (empty, would-block)
            ch.send(42)
            out.append(ch.try_recv())    # (42, True)
            out.append(ch.try_recv())    # None again
            ch.close()
            out.append(ch.try_recv())    # (None, False)
        _run_in_sched(runner)
        self.assertEqual(out, [None, (42, True), None, (None, False)])


class TestPingPong(unittest.TestCase):
    """End-to-end test of the actual concurrency: two goroutines
    bouncing values through a channel."""
    def test_ping_pong(self):
        a = runloom_c.Chan()
        b = runloom_c.Chan()
        log = []
        N = 5

        def pinger():
            for i in range(N):
                a.send(i)
                v, _ = b.recv()
                log.append(("p", v))

        def ponger():
            for _ in range(N):
                v, _ = a.recv()
                b.send(v * 10)

        _run_in_sched(pinger, ponger)
        self.assertEqual(log, [("p", 0), ("p", 10), ("p", 20),
                               ("p", 30), ("p", 40)])

    def test_fan_in(self):
        """N producers, 1 consumer, buffered channel."""
        ch = runloom_c.Chan(4)
        out = []
        N = 4

        def make_producer(i):
            def prod():
                for j in range(3):
                    ch.send((i, j))
            return prod

        def consumer():
            for _ in range(N * 3):
                v, _ = ch.recv()
                out.append(v)

        gs = [make_producer(i) for i in range(N)]
        _run_in_sched(*gs, consumer)
        # All items delivered, regardless of interleaving.
        self.assertEqual(len(out), N * 3)
        self.assertEqual(sorted(out), sorted((i, j) for i in range(N) for j in range(3)))


class TestIteration(unittest.TestCase):
    """Channels support Go's `for v := range ch { ... }` via Python's
    `for v in ch:` -- iteration ends when the channel is closed."""

    def test_range_basic(self):
        ch = runloom_c.Chan(8)
        out = []

        def producer():
            for i in range(5):
                ch.send(i * 11)
            ch.close()

        def consumer():
            for v in ch:
                out.append(v)

        _run_in_sched(producer, consumer)
        self.assertEqual(out, [0, 11, 22, 33, 44])

    def test_range_empty_after_close(self):
        ch = runloom_c.Chan()
        out = []
        def runner():
            ch.close()
            for v in ch:
                out.append(v)
            out.append("done")
        _run_in_sched(runner)
        self.assertEqual(out, ["done"])


class TestSelect(unittest.TestCase):
    def test_default_no_case_ready(self):
        a = runloom_c.Chan()
        b = runloom_c.Chan(1)
        out = []
        def runner():
            r = runloom_c.select([
                ("recv", a),
                ("recv", b),
            ], default=True)
            out.append(r)
        _run_in_sched(runner)
        self.assertEqual(out, [-1])

    def test_immediate_recv(self):
        ch = runloom_c.Chan(1)
        out = []
        def runner():
            ch.send("ready")
            i, payload = runloom_c.select([("recv", ch)])
            out.append((i, payload))
        _run_in_sched(runner)
        self.assertEqual(out, [(0, ("ready", True))])

    def test_immediate_send_into_buffer(self):
        ch = runloom_c.Chan(2)
        out = []
        def runner():
            i, _ = runloom_c.select([("send", ch, 99)])
            out.append(i)
            v, ok = ch.recv()
            out.append((v, ok))
        _run_in_sched(runner)
        self.assertEqual(out, [0, (99, True)])

    def test_blocking_two_chans(self):
        """One goroutine selects on two channels; another writes to
        the second one.  The select should fire on case 1."""
        a = runloom_c.Chan()
        b = runloom_c.Chan()
        log = []

        def selector():
            r = runloom_c.select([
                ("recv", a),
                ("recv", b),
            ])
            log.append(("fired", r[0], r[1]))

        def writer_b():
            runloom_c.sched_yield()       # let selector park first
            b.send("from-b")

        _run_in_sched(selector, writer_b)
        self.assertEqual(log, [("fired", 1, ("from-b", True))])

    def test_select_send_on_one_recv_on_other(self):
        a = runloom_c.Chan()
        b = runloom_c.Chan()
        log = []

        def selector():
            r = runloom_c.select([
                ("send", a, "to-a"),
                ("recv", b),
            ])
            log.append(("fired", r[0]))

        def take_a():
            v, _ = a.recv()
            log.append(("got-a", v))

        _run_in_sched(selector, take_a)
        # selector's SEND on a fires when take_a recvs.
        self.assertIn(("fired", 0), log)
        self.assertIn(("got-a", "to-a"), log)


if __name__ == "__main__":
    unittest.main()
