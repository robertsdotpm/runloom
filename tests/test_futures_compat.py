"""Cooperative concurrent.futures: fiber-backed ThreadPoolExecutor and the
Future.result() / wait() / as_completed() surface.

Adapted from CPython Lib/test/test_concurrent_futures (ExecutorTest,
ThreadPoolExecutorTest, WaitTests, AsCompletedTests, FutureTests).

The stock ThreadPoolExecutor runs work on OS threads and notifies each
Future's threading.Condition from the worker thread; after patch() that
Condition is cooperative and a cross-thread notify of a fiber waiter would
deadlock.  runloom.monkey makes ThreadPoolExecutor fiber-backed, so submitted
work runs on the cooperative scheduler and Future.result()/wait()/
as_completed() resolve in-domain.  These tests pin down result delivery,
exception propagation, map ordering, wait()/as_completed() semantics, cancel,
and that a blocked result() yields to siblings.
"""
import time
import unittest

import runloom
import runloom.monkey
import runloom_c

import concurrent.futures as cf


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
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


class TestThreadPoolExecutor(unittest.TestCase):
    def test_is_fiber_backed(self):
        self.assertEqual(cf.ThreadPoolExecutor.__name__, "CoThreadPoolExecutor")

    def test_submit_result(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                fut = ex.submit(pow, 2, 10)
                return fut.result()
        self.assertEqual(_drive(body), 1024)

    def test_many_results(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                futs = [ex.submit(lambda x: x * x, i) for i in range(16)]
                return [f.result() for f in futs]
        self.assertEqual(_drive(body), [i * i for i in range(16)])

    def test_exception_propagates(self):
        def body():
            with cf.ThreadPoolExecutor() as ex:
                fut = ex.submit(lambda: 1 / 0)
                try:
                    fut.result()
                except ZeroDivisionError:
                    return ("raised", fut.exception().__class__.__name__)
            return ("no-raise", None)
        self.assertEqual(_drive(body), ("raised", "ZeroDivisionError"))

    def test_map_preserves_order(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                return list(ex.map(lambda x: x + 100, range(10)))
        self.assertEqual(_drive(body), [i + 100 for i in range(10)])

    def test_submit_after_shutdown_raises(self):
        def body():
            ex = cf.ThreadPoolExecutor()
            ex.shutdown()
            try:
                ex.submit(lambda: 1)
            except RuntimeError:
                return True
            return False
        self.assertTrue(_drive(body))

    def test_max_workers_bounds_concurrency(self):
        """No more than max_workers tasks run at once."""
        def body():
            live = {"now": 0, "peak": 0}

            def task():
                live["now"] += 1
                live["peak"] = max(live["peak"], live["now"])
                for _ in range(5):
                    runloom.sleep(0.002)
                live["now"] -= 1

            with cf.ThreadPoolExecutor(max_workers=3) as ex:
                futs = [ex.submit(task) for _ in range(12)]
                for f in futs:
                    f.result()
            return live["peak"]
        self.assertLessEqual(_drive(body), 3)

    def test_result_blocks_then_yields(self):
        """A fiber blocked in future.result() lets a sibling run."""
        def body():
            ticks = []
            done = {"v": False}

            def ticker():
                while not done["v"]:
                    ticks.append(1)
                    runloom.sleep(0.002)

            runloom_c.fiber(ticker)
            with cf.ThreadPoolExecutor() as ex:
                fut = ex.submit(lambda: (
                    [runloom.sleep(0.005) for _ in range(6)], 99)[1])
                val = fut.result()
            done["v"] = True
            return val, len(ticks)
        val, ticks = _drive(body)
        self.assertEqual(val, 99)
        self.assertGreaterEqual(ticks, 1)


class TestWait(unittest.TestCase):
    def test_wait_all_completed(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                futs = [ex.submit(lambda i=i: (runloom.sleep(0.005), i)[1])
                        for i in range(6)]
                done, not_done = cf.wait(futs)
                return len(done), len(not_done)
        self.assertEqual(_drive(body), (6, 0))

    def test_wait_first_completed(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                fast = ex.submit(lambda: 1)
                slow = ex.submit(lambda: ([runloom.sleep(0.01) for _ in range(10)],
                                          2)[1])
                done, not_done = cf.wait(
                    [fast, slow], return_when=cf.FIRST_COMPLETED)
                ok = fast in done
                slow.result()        # drain
                return ok
        self.assertTrue(_drive(body))

    def test_wait_timeout(self):
        def body():
            with cf.ThreadPoolExecutor() as ex:
                slow = ex.submit(lambda: ([runloom.sleep(0.01) for _ in range(20)],
                                          1)[1])
                done, not_done = cf.wait([slow], timeout=0.02)
                got = (len(done), len(not_done))
                slow.result()
                return got
        self.assertEqual(_drive(body), (0, 1))


class TestAsCompleted(unittest.TestCase):
    def test_as_completed_yields_all(self):
        def body():
            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                futs = [ex.submit(lambda i=i: (runloom.sleep(0.003 * (i % 3)), i)[1])
                        for i in range(8)]
                return sorted(f.result() for f in cf.as_completed(futs))
        self.assertEqual(_drive(body), list(range(8)))


class TestFutureCancel(unittest.TestCase):
    def test_cancel_before_run(self):
        """A future cancelled before its task starts must report cancelled and
        never run the callable."""
        def body():
            ran = {"v": False}

            def slow_marker():
                ran["v"] = True
                return 1

            with cf.ThreadPoolExecutor(max_workers=1) as ex:
                blocker = ex.submit(
                    lambda: ([runloom.sleep(0.01) for _ in range(5)], 0)[1])
                target = ex.submit(slow_marker)
                cancelled = target.cancel()      # still queued behind blocker
                blocker.result()
                return cancelled, target.cancelled(), ran["v"]
        cancelled, is_cancelled, ran = _drive(body)
        self.assertTrue(cancelled)
        self.assertTrue(is_cancelled)
        self.assertFalse(ran)


if __name__ == "__main__":
    unittest.main()
