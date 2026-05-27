"""Tests for pygo.time (After, Timer, Ticker)."""
import time as _time
import unittest

import pygo_core
import pygo.time as ptime


class TestAfter(unittest.TestCase):
    def test_after_fires_once(self):
        out = []
        ch = ptime.After(0.02)

        def waiter():
            v, ok = ch.recv()
            out.append((ok, v))
            # second recv should see channel closed
            _v2, ok2 = ch.recv()
            out.append(ok2)

        pygo_core.go(waiter)
        pygo_core.run()

        self.assertEqual(len(out), 2)
        self.assertTrue(out[0][0])     # first recv: ok=True
        self.assertFalse(out[1])       # second recv: ok=False (closed)


class TestTimer(unittest.TestCase):
    def test_timer_fires(self):
        out = []
        t = ptime.NewTimer(0.02)

        def waiter():
            _v, ok = t.c.recv()
            out.append(ok)

        pygo_core.go(waiter)
        pygo_core.run()
        self.assertEqual(out, [True])

    def test_stop_prevents_fire(self):
        out = []
        t = ptime.NewTimer(0.05)
        # Stop before it fires.

        def stopper():
            pygo_core.sched_sleep(0.01)
            stopped = t.Stop()
            out.append(("stopped", stopped))
            # Drain in case of race; with-timeout to avoid hanging
            ok = t.c.try_recv()
            out.append(("try_recv", ok))

        pygo_core.go(stopper)
        pygo_core.run()
        self.assertEqual(out[0], ("stopped", True))
        # try_recv should be None (no value) since Stop fired before timer.
        self.assertIsNone(out[1][1])


class TestTicker(unittest.TestCase):
    def test_ticker_fires_repeatedly(self):
        out = []
        t = ptime.NewTicker(0.01)

        def collector():
            for _ in range(3):
                v, ok = t.c.recv()
                if not ok:
                    break
                out.append(v)
            t.Stop()

        pygo_core.go(collector)
        pygo_core.run()

        self.assertEqual(len(out), 3)

    def test_negative_interval_rejected(self):
        with self.assertRaises(ValueError):
            ptime.NewTicker(0)
        with self.assertRaises(ValueError):
            ptime.NewTicker(-1.0)


class TestSleep(unittest.TestCase):
    def test_sleep_alias(self):
        out = []
        def g():
            t0 = _time.monotonic()
            ptime.Sleep(0.02)
            out.append(_time.monotonic() - t0)
        pygo_core.go(g)
        pygo_core.run()
        self.assertGreaterEqual(out[0], 0.015)


if __name__ == "__main__":
    unittest.main()
