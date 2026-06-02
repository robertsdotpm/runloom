"""Cooperative threading primitives: Lock / RLock / Event / Condition /
Semaphore / BoundedSemaphore / Thread.join.

Adapted from CPython's Lib/test/lock_tests.py + Lib/test/test_threading.py,
with mutual-exclusion and lost-wakeup stress patterns in the spirit of the
Linux kernel's locking self-tests (lib/locking-selftest, the litmus idea of
"a lock must serialise a racing read-modify-write to an exact total, and a
wait/wake pair must never drop a wakeup").

These run the cooperative shims under the C scheduler.  Where the contract
differs from real OS-thread locks it is because the single-thread
cooperative model is the design target (e.g. a goroutine that takes a lock
never preempts mid-critical-section), which is exactly the property the
mutual-exclusion tests assert.
"""
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


class TestLock(unittest.TestCase):
    def test_acquire_release_locked(self):
        def body():
            lk = threading.Lock()
            self.assertFalse(lk.locked())
            self.assertTrue(lk.acquire())
            self.assertTrue(lk.locked())
            lk.release()
            self.assertFalse(lk.locked())
        _drive(body)

    def test_non_blocking_acquire_fails_when_held(self):
        def body():
            lk = threading.Lock()
            lk.acquire()
            self.assertFalse(lk.acquire(blocking=False))
            lk.release()
            self.assertTrue(lk.acquire(blocking=False))
            lk.release()
        _drive(body)

    def test_release_unlocked_raises(self):
        def body():
            lk = threading.Lock()
            with self.assertRaises(RuntimeError):
                lk.release()
        _drive(body)

    def test_mutual_exclusion_no_interleave(self):
        """Linux-selftest-style: a critical section must not interleave.
        Each holder logs (in, out); the log must be strictly paired."""
        def body():
            lk = threading.Lock()
            log = []

            def worker(name):
                with lk:
                    log.append((name, "in"))
                    pygo.sleep(0.005)        # yield while holding the lock
                    log.append((name, "out"))

            for n in "ABCD":
                pygo_core.go(lambda n=n: worker(n))
            t0 = time.monotonic()
            while len(log) < 8 and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return log

        log = _drive(body)
        self.assertEqual(len(log), 8)
        for i in range(0, 8, 2):
            self.assertEqual(log[i][1], "in")
            self.assertEqual(log[i + 1][1], "out")
            self.assertEqual(log[i][0], log[i + 1][0])

    def test_counter_no_lost_updates(self):
        """N goroutines each do M lock-guarded increments of a shared
        counter; the total must be exact (no lost read-modify-write)."""
        def body():
            lk = threading.Lock()
            state = {"n": 0}
            N, M = 8, 200

            def bump():
                for _ in range(M):
                    with lk:
                        v = state["n"]
                        pygo.yield_()        # widen the race window
                        state["n"] = v + 1

            for _ in range(N):
                pygo_core.go(bump)
            t0 = time.monotonic()
            while state["n"] < N * M and time.monotonic() - t0 < 10:
                pygo.sleep(0.005)
            return state["n"]

        self.assertEqual(_drive(body), 8 * 200)


class TestRLock(unittest.TestCase):
    def test_reentrant_same_goroutine(self):
        def body():
            rl = threading.RLock()
            rl.acquire()
            rl.acquire()                     # reentrant: must not deadlock
            rl.acquire()
            rl.release()
            rl.release()
            rl.release()
            return True
        self.assertTrue(_drive(body))

    def test_excludes_other_goroutine(self):
        def body():
            rl = threading.RLock()
            log = []

            def a():
                with rl:
                    log.append("A-in")
                    pygo.sleep(0.02)
                    log.append("A-out")

            def b():
                pygo.sleep(0.005)
                got = rl.acquire(blocking=False)  # A holds it -> False
                log.append(("B-nonblock", got))
                with rl:                          # now block until A frees
                    log.append("B-in")

            pygo_core.go(a)
            pygo_core.go(b)
            t0 = time.monotonic()
            while "B-in" not in log and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return log

        log = _drive(body)
        self.assertIn(("B-nonblock", False), log)
        self.assertLess(log.index("A-out"), log.index("B-in"))


class TestEvent(unittest.TestCase):
    def test_set_wakes_all_waiters(self):
        def body():
            ev = threading.Event()
            woke = []

            def waiter(i):
                ev.wait()
                woke.append(i)

            for i in range(3):
                pygo_core.go(lambda i=i: waiter(i))

            def setter():
                pygo.sleep(0.02)
                ev.set()

            pygo_core.go(setter)
            t0 = time.monotonic()
            while len(woke) < 3 and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return sorted(woke), ev.is_set()

        woke, isset = _drive(body)
        self.assertEqual(woke, [0, 1, 2])
        self.assertTrue(isset)

    def test_wait_timeout_returns_false(self):
        def body():
            ev = threading.Event()
            t0 = time.monotonic()
            r = ev.wait(timeout=0.05)        # never set
            return r, time.monotonic() - t0
        r, dt = _drive(body)
        self.assertFalse(r)
        self.assertGreaterEqual(dt, 0.04)

    def test_wait_returns_true_if_already_set(self):
        def body():
            ev = threading.Event()
            ev.set()
            return ev.wait(timeout=0.01)
        self.assertTrue(_drive(body))


class TestCondition(unittest.TestCase):
    def test_notify_one(self):
        def body():
            cv = threading.Condition()
            woke = []

            def waiter(i):
                with cv:
                    cv.wait()
                    woke.append(i)

            for i in range(2):
                pygo_core.go(lambda i=i: waiter(i))

            def notifier():
                pygo.sleep(0.02)
                with cv:
                    cv.notify(1)            # wake exactly one
                pygo.sleep(0.02)
                with cv:
                    cv.notify(1)            # then the other

            pygo_core.go(notifier)
            t0 = time.monotonic()
            while len(woke) < 2 and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return sorted(woke)

        self.assertEqual(_drive(body), [0, 1])

    def test_wait_for_predicate(self):
        def body():
            cv = threading.Condition()
            state = {"ready": False}
            got = []

            def waiter():
                with cv:
                    cv.wait_for(lambda: state["ready"])
                    got.append(True)

            def setter():
                pygo.sleep(0.02)
                with cv:
                    state["ready"] = True
                    cv.notify_all()

            pygo_core.go(waiter)
            pygo_core.go(setter)
            t0 = time.monotonic()
            while not got and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            return got

        self.assertEqual(_drive(body), [True])

    def test_producer_consumer_no_lost_wakeup(self):
        """Bounded buffer over a Condition; every produced item is consumed
        exactly once with no lost wakeup (kernel-style wait/wake litmus)."""
        def body():
            cv = threading.Condition()
            buf = []
            CAP, TOTAL = 4, 200
            produced = {"n": 0}
            consumed = []

            def producer():
                for i in range(TOTAL):
                    with cv:
                        while len(buf) >= CAP:
                            cv.wait()
                        buf.append(i)
                        cv.notify()

            def consumer():
                while len(consumed) < TOTAL:
                    with cv:
                        while not buf:
                            if len(consumed) >= TOTAL:
                                return
                            cv.wait(timeout=0.5)
                            if not buf:
                                continue
                        consumed.append(buf.pop(0))
                        cv.notify()

            pygo_core.go(producer)
            pygo_core.go(consumer)
            t0 = time.monotonic()
            while len(consumed) < TOTAL and time.monotonic() - t0 < 10:
                pygo.sleep(0.005)
            return consumed

        consumed = _drive(body)
        self.assertEqual(consumed, list(range(200)))


class TestSemaphore(unittest.TestCase):
    def test_bounds(self):
        def body():
            s = threading.Semaphore(2)
            self.assertTrue(s.acquire())
            self.assertTrue(s.acquire())
            self.assertFalse(s.acquire(blocking=False))   # exhausted
            s.release()
            self.assertTrue(s.acquire(blocking=False))
            s.release(); s.release()
        _drive(body)

    def test_bounded_over_release_raises(self):
        def body():
            s = threading.BoundedSemaphore(1)
            s.acquire()
            s.release()
            with self.assertRaises(ValueError):
                s.release()                  # over the initial bound
        _drive(body)

    def test_limits_concurrency(self):
        """A Semaphore(K) must cap the number of goroutines in the region to
        K at any instant."""
        def body():
            K = 3
            s = threading.Semaphore(K)
            inside = {"now": 0, "max": 0}

            def worker():
                with s:
                    inside["now"] += 1
                    inside["max"] = max(inside["max"], inside["now"])
                    pygo.sleep(0.01)
                    inside["now"] -= 1

            for _ in range(12):
                pygo_core.go(worker)
            t0 = time.monotonic()
            while inside["now"] != 0 or time.monotonic() - t0 < 0.2:
                if time.monotonic() - t0 > 5:
                    break
                pygo.sleep(0.005)
            return inside["max"]

        self.assertLessEqual(_drive(body), 3)


class TestThreadJoin(unittest.TestCase):
    def test_join_cooperative(self):
        """A goroutine joining a real worker thread keeps siblings running."""
        def body():
            log = []

            def work():
                time.sleep(0.03)
                log.append("worker-done")

            th = threading.Thread(target=work)
            th.start()

            def sib():
                for _ in range(3):
                    time.sleep(0.005)
                    log.append("sib")

            pygo_core.go(sib)
            th.join()
            log.append("joined")
            return log

        log = _drive(body)
        self.assertIn("worker-done", log)
        self.assertEqual(log[-1], "joined")
        # The sibling made progress while we were parked in join().
        self.assertGreaterEqual(log.count("sib"), 1)
        self.assertLess(log.index("sib"), log.index("joined"))

    def test_join_not_started_raises(self):
        def body():
            th = threading.Thread(target=lambda: None)
            with self.assertRaises(RuntimeError):
                th.join()                    # never started
        _drive(body)

    def test_join_timeout_returns(self):
        def body():
            ev = threading.Event()

            def work():
                ev.wait()                    # blocks until we let it finish

            th = threading.Thread(target=work)
            th.start()
            t0 = time.monotonic()
            th.join(timeout=0.05)            # times out, thread still alive
            dt = time.monotonic() - t0
            alive = th.is_alive()
            ev.set()                         # release the worker
            th.join()
            return dt, alive

        dt, alive = _drive(body)
        self.assertGreaterEqual(dt, 0.04)
        self.assertTrue(alive)


if __name__ == "__main__":
    unittest.main()
