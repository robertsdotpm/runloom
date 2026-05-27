"""Smoke tests: Coro primitive + go()/run() scheduler."""
import sys
import time
import unittest

sys.path.insert(0, "src")

import pygo
import pygo_core


class TestCoreBackend(unittest.TestCase):
    def test_backend_name(self):
        b = pygo_core.backend()
        self.assertIn(b, ("ucontext", "fibers", "fcontext-asm"))


class TestCoroPrimitive(unittest.TestCase):
    def test_yield_resume_chain(self):
        log = []
        def child():
            log.append("a")
            pygo_core.yield_()
            log.append("b")
            pygo_core.yield_()
            log.append("c")
            return "done"

        c = pygo_core.Coro(child)
        self.assertFalse(c.done)
        c.resume(); self.assertEqual(log, ["a"]); self.assertFalse(c.done)
        c.resume(); self.assertEqual(log, ["a", "b"]); self.assertFalse(c.done)
        c.resume(); self.assertEqual(log, ["a", "b", "c"]); self.assertTrue(c.done)
        self.assertEqual(c.result, "done")

    def test_exception_propagates_on_resume(self):
        def child():
            raise ValueError("boom")
        c = pygo_core.Coro(child)
        with self.assertRaises(ValueError):
            c.resume()
        self.assertTrue(c.done)

    def test_nested_coros_separate_state(self):
        def outer():
            inner_log = []
            def inner():
                inner_log.append("inner-1")
                pygo_core.yield_()
                inner_log.append("inner-2")
            c = pygo_core.Coro(inner)
            c.resume(); c.resume()
            return inner_log

        c = pygo_core.Coro(outer)
        c.resume()
        self.assertTrue(c.done)
        self.assertEqual(c.result, ["inner-1", "inner-2"])


class TestScheduler(unittest.TestCase):
    def test_three_goroutines_interleave(self):
        log = []
        def worker(name, n):
            for i in range(n):
                log.append((name, i))
                pygo.yield_()
        pygo.go(worker, "A", 3)
        pygo.go(worker, "B", 3)
        pygo.go(worker, "C", 3)
        pygo.run()
        # Should round-robin A0 B0 C0 A1 B1 C1 ...
        self.assertEqual(log, [
            ("A", 0), ("B", 0), ("C", 0),
            ("A", 1), ("B", 1), ("C", 1),
            ("A", 2), ("B", 2), ("C", 2),
        ])

    def test_sleep_lets_others_run(self):
        log = []
        def sleeper():
            log.append("s1-start")
            pygo.sleep(0.05)
            log.append("s1-end")
        def burner():
            for i in range(3):
                log.append(("b", i))
                pygo.yield_()
        pygo.go(sleeper)
        pygo.go(burner)
        t0 = time.monotonic()
        pygo.run()
        elapsed = time.monotonic() - t0
        # burner finished long before sleeper woke
        self.assertEqual(log[:4], ["s1-start", ("b", 0), ("b", 1), ("b", 2)])
        self.assertEqual(log[-1], "s1-end")
        self.assertGreaterEqual(elapsed, 0.04)


if __name__ == "__main__":
    unittest.main()
