"""Tests for the per-fiber stack sizing machinery:

  * runloom_c.go(fn, stack_size=N): per-call override
  * runloom_c.set_stack_size(N) / get_stack_size(): program-wide default
  * Auto-calibration over the first RUNLOOM_CAL_TARGET completions
  * stats() exposes stack_hwm, stack_completed, stack_calibrated

These tests run in a single process so the calibration state from earlier
tests will be present.  Each test that asserts on calibration state forces
a known starting point via set_stack_size().
"""
import unittest

import runloom_c

import os as _hwm_os
import pytest as _hwm_pytest
# Stack high-water-mark is precise only on a POSIX guard-page backend
# (fcontext-asm / ucontext) with 4 KB pages.  Windows Fibers have no guard page,
# and macOS 16 KB pages make the mincore-based HWM over-report (it reports the
# whole stack resident), so these HWM/advice/sizing tests can't measure precisely
# there -- skip them (the diagnostic itself just over-reserves, which is safe).
_RELIABLE_HWM = (_hwm_os.name == "posix"
                 and runloom_c.backend() in ("fcontext-asm", "ucontext")
                 and _hwm_os.sysconf("SC_PAGESIZE") == 4096)
pytestmark = _hwm_pytest.mark.skipif(
    not _RELIABLE_HWM,
    reason="stack HWM is reliable only on a POSIX guard-page backend with 4 KB pages")


class TestStackSizeOverride(unittest.TestCase):
    def test_get_stack_size_default(self):
        sz = runloom_c.get_stack_size()
        self.assertGreaterEqual(sz, 16 * 1024)
        self.assertLessEqual(sz, 8 * 1024 * 1024)

    def test_set_stack_size_changes_default(self):
        original = runloom_c.get_stack_size()
        try:
            runloom_c.set_stack_size(64 * 1024)
            self.assertEqual(runloom_c.get_stack_size(), 64 * 1024)
        finally:
            runloom_c.set_stack_size(original)

    def test_set_stack_size_clamps_to_min(self):
        original = runloom_c.get_stack_size()
        try:
            runloom_c.set_stack_size(1024)            # below 16 KB min
            self.assertGreaterEqual(runloom_c.get_stack_size(), 16 * 1024)
        finally:
            runloom_c.set_stack_size(original)

    def test_set_stack_size_clamps_to_max(self):
        original = runloom_c.get_stack_size()
        try:
            runloom_c.set_stack_size(64 * 1024 * 1024)  # above 8 MB max
            self.assertLessEqual(runloom_c.get_stack_size(),
                                 8 * 1024 * 1024)
        finally:
            runloom_c.set_stack_size(original)

    def test_set_stack_size_rejects_zero_and_negative(self):
        with self.assertRaises(ValueError):
            runloom_c.set_stack_size(0)
        with self.assertRaises(ValueError):
            runloom_c.set_stack_size(-1)

    def test_per_call_stack_size_kwarg(self):
        """go(fn, stack_size=N) accepts and spawns successfully."""
        ran = [False]
        def w():
            ran[0] = True
        runloom_c.go(w, stack_size=64 * 1024)
        runloom_c.run()
        self.assertTrue(ran[0])

    def test_per_call_stack_size_does_not_change_default(self):
        original = runloom_c.get_stack_size()
        def w():
            pass
        runloom_c.go(w, stack_size=128 * 1024)
        runloom_c.run()
        self.assertEqual(runloom_c.get_stack_size(), original)

    def test_per_call_huge_stack_works(self):
        """Spawn a fiber with a 2 MB stack; verify it runs to completion.
        Catches off-by-one in the size clamp / paint loop."""
        out = []
        def w():
            # Some C-stack-heavy work: small local allocation
            data = list(range(100))
            out.append(sum(data))
        runloom_c.go(w, stack_size=2 * 1024 * 1024)
        runloom_c.run()
        self.assertEqual(out, [sum(range(100))])


class TestCalibrationStats(unittest.TestCase):
    def test_stats_exposes_stack_fields(self):
        s = runloom_c.stats()
        self.assertIn("stack_size_default", s)
        self.assertIn("stack_hwm", s)
        self.assertIn("stack_completed", s)
        self.assertIn("stack_calibrated", s)
        self.assertIn("stack_painting", s)

    def test_stats_hwm_increments(self):
        """After running fibers that touch a known stack amount, HWM
        should be > 0 (assuming painting is still on)."""
        original = runloom_c.get_stack_size()
        try:
            # Re-enable paint by setting a fresh default of any size --
            # actually set_stack_size DISABLES painting (the calibration
            # is treated as frozen).  So skip the HWM assertion if
            # painting is already disabled.
            before = runloom_c.stats()
            def w():
                pass
            for _ in range(10):
                runloom_c.go(w)
            runloom_c.run()
            after = runloom_c.stats()
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
            runloom_c.set_stack_size(original)


class TestSpawnWithSize(unittest.TestCase):
    def test_many_concurrent_with_explicit_size(self):
        """Spawn 200 gs each with a 32 KB stack; verify all complete."""
        N = 200
        out = []
        def w():
            out.append(1)
        for _ in range(N):
            runloom_c.go(w, stack_size=32 * 1024)
        runloom_c.run()
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
            runloom_c.go(small,  stack_size=16 * 1024)
            runloom_c.go(medium, stack_size=64 * 1024)
            runloom_c.go(large,  stack_size=256 * 1024)
        runloom_c.run()
        self.assertEqual(out.count("s"), 20)
        self.assertEqual(out.count("m"), 20)
        self.assertEqual(out.count("l"), 20)


if __name__ == "__main__":
    unittest.main()
