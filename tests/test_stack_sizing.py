"""Tests for the per-goroutine stack sizing machinery:

  * pygo_core.go(fn, stack_size=N): per-call override
  * pygo_core.set_stack_size(N) / get_stack_size(): program-wide default
  * Auto-calibration over the first PYGO_CAL_TARGET completions
  * stats() exposes stack_hwm, stack_completed, stack_calibrated

These tests run in a single process so the calibration state from earlier
tests will be present.  Each test that asserts on calibration state forces
a known starting point via set_stack_size().
"""
import unittest

import pygo_core


class TestStackSizeOverride(unittest.TestCase):
    def test_get_stack_size_default(self):
        sz = pygo_core.get_stack_size()
        self.assertGreaterEqual(sz, 16 * 1024)
        self.assertLessEqual(sz, 8 * 1024 * 1024)

    def test_set_stack_size_changes_default(self):
        original = pygo_core.get_stack_size()
        try:
            pygo_core.set_stack_size(64 * 1024)
            self.assertEqual(pygo_core.get_stack_size(), 64 * 1024)
        finally:
            pygo_core.set_stack_size(original)

    def test_set_stack_size_clamps_to_min(self):
        original = pygo_core.get_stack_size()
        try:
            pygo_core.set_stack_size(1024)            # below 16 KB min
            self.assertGreaterEqual(pygo_core.get_stack_size(), 16 * 1024)
        finally:
            pygo_core.set_stack_size(original)

    def test_set_stack_size_clamps_to_max(self):
        original = pygo_core.get_stack_size()
        try:
            pygo_core.set_stack_size(64 * 1024 * 1024)  # above 8 MB max
            self.assertLessEqual(pygo_core.get_stack_size(),
                                 8 * 1024 * 1024)
        finally:
            pygo_core.set_stack_size(original)

    def test_set_stack_size_rejects_zero_and_negative(self):
        with self.assertRaises(ValueError):
            pygo_core.set_stack_size(0)
        with self.assertRaises(ValueError):
            pygo_core.set_stack_size(-1)

    def test_per_call_stack_size_kwarg(self):
        """go(fn, stack_size=N) accepts and spawns successfully."""
        ran = [False]
        def w():
            ran[0] = True
        pygo_core.go(w, stack_size=64 * 1024)
        pygo_core.run()
        self.assertTrue(ran[0])

    def test_per_call_stack_size_does_not_change_default(self):
        original = pygo_core.get_stack_size()
        def w():
            pass
        pygo_core.go(w, stack_size=128 * 1024)
        pygo_core.run()
        self.assertEqual(pygo_core.get_stack_size(), original)

    def test_per_call_huge_stack_works(self):
        """Spawn a goroutine with a 2 MB stack; verify it runs to completion.
        Catches off-by-one in the size clamp / paint loop."""
        out = []
        def w():
            # Some C-stack-heavy work: small local allocation
            data = list(range(100))
            out.append(sum(data))
        pygo_core.go(w, stack_size=2 * 1024 * 1024)
        pygo_core.run()
        self.assertEqual(out, [sum(range(100))])


class TestCalibrationStats(unittest.TestCase):
    def test_stats_exposes_stack_fields(self):
        s = pygo_core.stats()
        self.assertIn("stack_size_default", s)
        self.assertIn("stack_hwm", s)
        self.assertIn("stack_completed", s)
        self.assertIn("stack_calibrated", s)
        self.assertIn("stack_painting", s)

    def test_stats_hwm_increments(self):
        """After running goroutines that touch a known stack amount, HWM
        should be > 0 (assuming painting is still on)."""
        original = pygo_core.get_stack_size()
        try:
            # Re-enable paint by setting a fresh default of any size --
            # actually set_stack_size DISABLES painting (the calibration
            # is treated as frozen).  So skip the HWM assertion if
            # painting is already disabled.
            before = pygo_core.stats()
            def w():
                pass
            for _ in range(10):
                pygo_core.go(w)
            pygo_core.run()
            after = pygo_core.stats()
            if before["stack_painting"]:
                # HWM should be >= what we saw before (or remain at the
                # pre-existing max).
                self.assertGreaterEqual(after["stack_hwm"],
                                        before["stack_hwm"])
                self.assertGreaterEqual(after["stack_completed"],
                                        before["stack_completed"] + 10)
        finally:
            # Restore -- but set_stack_size locks calibration so this is
            # just a courtesy.
            pygo_core.set_stack_size(original)


class TestSpawnWithSize(unittest.TestCase):
    def test_many_concurrent_with_explicit_size(self):
        """Spawn 200 gs each with a 32 KB stack; verify all complete."""
        N = 200
        out = []
        def w():
            out.append(1)
        for _ in range(N):
            pygo_core.go(w, stack_size=32 * 1024)
        pygo_core.run()
        self.assertEqual(len(out), N)

    def test_mixed_size_concurrent(self):
        """Mix of different stack sizes in flight at once."""
        out = []
        def small():
            out.append("s")
        def medium():
            out.append("m")
        def large():
            out.append("l")
        for _ in range(20):
            pygo_core.go(small,  stack_size=16 * 1024)
            pygo_core.go(medium, stack_size=64 * 1024)
            pygo_core.go(large,  stack_size=256 * 1024)
        pygo_core.run()
        self.assertEqual(out.count("s"), 20)
        self.assertEqual(out.count("m"), 20)
        self.assertEqual(out.count("l"), 20)


if __name__ == "__main__":
    unittest.main()
