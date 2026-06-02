"""Cooperative queue.SimpleQueue / Queue / LifoQueue / PriorityQueue.

Adapted from CPython's Lib/test/test_queue.py.  queue.Queue and its
subclasses only need the `threading` patch (they build on
threading.Condition); queue.SimpleQueue is a C type whose blocking get()
needs the dedicated CoSimpleQueue replacement.  These tests cover:

  * the Empty / Full return-code contract (get_nowait / put_nowait,
    timeouts);
  * FIFO / LIFO / priority ordering preserved by the cooperative shims;
  * blocking get()/put() actually parking the goroutine and being woken by
    the matching put()/get() in a sibling goroutine (cooperative, not a
    busy-poll, not a scheduler freeze);
  * conservation under many producers + many consumers: every item is
    delivered exactly once (no loss, no duplication, no lost wakeups).
"""
import queue
import threading
import time
import unittest

import pygo
import pygo.monkey
import pygo_core


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    pygo_core.go(runner)
    pygo_core.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    pygo.monkey.patch()


def tearDownModule():
    pygo.monkey.unpatch()


class TestSimpleQueueContract(unittest.TestCase):
    def test_fifo_order(self):
        def body():
            q = queue.SimpleQueue()
            for i in range(5):
                q.put(i)
            self.assertEqual(q.qsize(), 5)
            self.assertFalse(q.empty())
            return [q.get() for _ in range(5)]
        self.assertEqual(_drive(body), [0, 1, 2, 3, 4])

    def test_get_nowait_empty_raises(self):
        def body():
            q = queue.SimpleQueue()
            with self.assertRaises(queue.Empty):
                q.get_nowait()
        _drive(body)

    def test_get_timeout_raises_empty(self):
        def body():
            q = queue.SimpleQueue()
            t0 = time.monotonic()
            with self.assertRaises(queue.Empty):
                q.get(timeout=0.05)
            return time.monotonic() - t0
        dt = _drive(body)
        self.assertGreaterEqual(dt, 0.04)

    def test_put_never_blocks_unbounded(self):
        def body():
            q = queue.SimpleQueue()
            for i in range(10000):     # unbounded: must not block/raise
                q.put(i)
            q.put_nowait("tail")
            return q.qsize()
        self.assertEqual(_drive(body), 10001)

    def test_blocking_get_woken_by_put(self):
        def body():
            q = queue.SimpleQueue()
            order = []

            def producer():
                time.sleep(0.02)
                order.append("put")
                q.put("item")

            pygo_core.go(producer)
            val = q.get()              # parks until producer runs
            order.append("got")
            self.assertEqual(val, "item")
            self.assertEqual(order, ["put", "got"])
        _drive(body)


class TestSimpleQueueConservation(unittest.TestCase):
    def test_many_producers_consumers(self):
        """N producers each emit M items; C consumers drain.  Every item is
        delivered exactly once."""
        def body():
            q = queue.SimpleQueue()
            N_PROD, M, N_CONS = 4, 50, 3
            total = N_PROD * M
            received = []
            done = threading.Event()

            def producer(base):
                for i in range(M):
                    q.put(base + i)
                    if i % 7 == 0:
                        pygo.yield_()

            def consumer():
                while True:
                    if len(received) >= total:
                        return
                    try:
                        received.append(q.get(timeout=1.0))
                    except queue.Empty:
                        if len(received) >= total:
                            return

            for p in range(N_PROD):
                pygo_core.go(lambda base=p * 1000: producer(base))
            for _ in range(N_CONS):
                pygo_core.go(consumer)

            # Wait for all items, cooperatively.
            t0 = time.monotonic()
            while len(received) < total and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return sorted(received)

        got = _drive(body)
        expected = sorted(p * 1000 + i for p in range(4) for i in range(50))
        self.assertEqual(got, expected)


class TestQueueReturnCodes(unittest.TestCase):
    """queue.Queue Empty/Full contract via the cooperative Condition."""

    def test_get_nowait_empty(self):
        def body():
            q = queue.Queue()
            with self.assertRaises(queue.Empty):
                q.get_nowait()
        _drive(body)

    def test_put_nowait_full(self):
        def body():
            q = queue.Queue(maxsize=2)
            q.put_nowait(1)
            q.put_nowait(2)
            self.assertTrue(q.full())
            with self.assertRaises(queue.Full):
                q.put_nowait(3)
        _drive(body)

    def test_get_timeout(self):
        def body():
            q = queue.Queue()
            t0 = time.monotonic()
            with self.assertRaises(queue.Empty):
                q.get(timeout=0.05)
            return time.monotonic() - t0
        self.assertGreaterEqual(_drive(body), 0.04)

    def test_put_timeout_when_full(self):
        def body():
            q = queue.Queue(maxsize=1)
            q.put(1)
            t0 = time.monotonic()
            with self.assertRaises(queue.Full):
                q.put(2, timeout=0.05)
            return time.monotonic() - t0
        self.assertGreaterEqual(_drive(body), 0.04)


class TestQueueBlockingHandoff(unittest.TestCase):
    def test_bounded_put_blocks_until_get(self):
        """A full bounded Queue parks the producer until a consumer drains
        one slot -- proving the cooperative Condition wakeup works both
        ways (the Go buffered-channel back-pressure pattern)."""
        def body():
            q = queue.Queue(maxsize=1)
            q.put("first")             # fills the queue
            order = []

            def consumer():
                time.sleep(0.02)
                order.append("get:" + q.get())

            pygo_core.go(consumer)
            order.append("put-start")
            q.put("second")            # blocks until consumer takes "first"
            order.append("put-done")
            self.assertEqual(order, ["put-start", "get:first", "put-done"])
            self.assertEqual(q.get(), "second")
        _drive(body)

    def test_task_done_join(self):
        def body():
            q = queue.Queue()
            for i in range(5):
                q.put(i)
            drained = []

            def worker():
                while True:
                    try:
                        item = q.get(timeout=0.5)
                    except queue.Empty:
                        return
                    drained.append(item)
                    q.task_done()

            pygo_core.go(worker)
            q.join()                   # parks until task_done called 5x
            return sorted(drained)
        self.assertEqual(_drive(body), [0, 1, 2, 3, 4])


class TestQueueOrdering(unittest.TestCase):
    def test_lifo(self):
        def body():
            q = queue.LifoQueue()
            for i in range(4):
                q.put(i)
            return [q.get() for _ in range(4)]
        self.assertEqual(_drive(body), [3, 2, 1, 0])

    def test_priority(self):
        def body():
            q = queue.PriorityQueue()
            for v in (5, 1, 4, 2, 3):
                q.put(v)
            return [q.get() for _ in range(5)]
        self.assertEqual(_drive(body), [1, 2, 3, 4, 5])


if __name__ == "__main__":
    unittest.main()
