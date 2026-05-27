"""Tests for pygo.context (Go-style cancellation)."""
import time as _time
import unittest

import pygo_core
import pygo.context as ctxmod


class TestBackground(unittest.TestCase):
    def test_background_never_errs(self):
        ctx = ctxmod.Background()
        self.assertIsNone(ctx.err())
        deadline, has = ctx.deadline()
        self.assertIsNone(deadline)
        self.assertFalse(has)


class TestWithCancel(unittest.TestCase):
    def test_cancel_closes_done_channel(self):
        ctx, cancel = ctxmod.WithCancel(ctxmod.Background())
        self.assertIsNone(ctx.err())

        got = []

        def waiter():
            idx, _ = pygo_core.select([("recv", ctx.done)])
            got.append(("woken", idx, ctx.err()))

        def canceller():
            pygo_core.sched_sleep(0.01)
            cancel()

        pygo_core.go(waiter)
        pygo_core.go(canceller)
        pygo_core.run()

        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][2], ctxmod.CANCELED)

    def test_cancel_propagates_to_child(self):
        parent, p_cancel = ctxmod.WithCancel(ctxmod.Background())
        child, _c_cancel = ctxmod.WithCancel(parent)

        got = []

        def child_waiter():
            pygo_core.select([("recv", child.done)])
            got.append(child.err())

        def cancel_parent():
            pygo_core.sched_sleep(0.01)
            p_cancel()

        pygo_core.go(child_waiter)
        pygo_core.go(cancel_parent)
        pygo_core.run()

        self.assertEqual(got, [ctxmod.CANCELED])

    def test_double_cancel_is_idempotent(self):
        ctx, cancel = ctxmod.WithCancel(ctxmod.Background())
        cancel()
        cancel()  # should not raise


class TestWithTimeout(unittest.TestCase):
    def test_deadline_fires(self):
        ctx, _cancel = ctxmod.WithTimeout(ctxmod.Background(), 0.02)

        outcome = []

        def waiter():
            pygo_core.select([("recv", ctx.done)])
            outcome.append(ctx.err())

        pygo_core.go(waiter)
        pygo_core.run()

        self.assertEqual(outcome, [ctxmod.DEADLINE_EXCEEDED])

    def test_explicit_cancel_beats_timeout(self):
        ctx, cancel = ctxmod.WithTimeout(ctxmod.Background(), 1.0)

        outcome = []

        def waiter():
            pygo_core.select([("recv", ctx.done)])
            outcome.append(ctx.err())

        def early_cancel():
            pygo_core.sched_sleep(0.01)
            cancel()

        pygo_core.go(waiter)
        pygo_core.go(early_cancel)
        pygo_core.run()

        self.assertEqual(outcome, [ctxmod.CANCELED])

    def test_parent_deadline_wins_if_tighter(self):
        # Parent has 0.01s; child asks for 1.0s -- should still fire at 0.01s.
        parent, _ = ctxmod.WithTimeout(ctxmod.Background(), 0.01)
        child, _  = ctxmod.WithTimeout(parent, 1.0)

        t0 = _time.monotonic()
        outcome = []

        def waiter():
            pygo_core.select([("recv", child.done)])
            outcome.append((child.err(), _time.monotonic() - t0))

        pygo_core.go(waiter)
        pygo_core.run()

        err, elapsed = outcome[0]
        self.assertEqual(err, ctxmod.DEADLINE_EXCEEDED)
        self.assertLess(elapsed, 0.5)  # nowhere near 1.0s


if __name__ == "__main__":
    unittest.main()
