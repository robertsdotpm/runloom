"""Regression guard for the soak leak-oracle's retain-forever ratchet forgiveness
(tools/soak/oracle.py).

A retain-forever pool (freed g-structs / cached coro stacks kept reusable in
per-thread slabs) climbs to a high-water-mark on a DECELERATING curve, then
plateaus.  When its ramp bleeds PAST the warmup cutoff the whole-window OLS slope
stays large-positive long after the pool has locally flattened -- a naive
single-line slope test then false-FAILs a converged ratchet.  The oracle forgives
such a metric only with POSITIVE evidence of convergence:

  * flat-tail span (final quarter moved <= abs_floor), or
  * SATURATING shape (a log model beats the linear fit, rising), or
  * CURRENT-RATE (the most-recent eighth is itself flat: slope CI includes 0 or
    |slope| <= eps) AND a genuine earlier ramp existed -- robust to a BUMPY ramp
    (bursty connection arrival) that defeats the smooth log fit.

This test pins the CURRENT-RATE path (the one real 6h cserve iterations needed:
they converge to a ~36/h final-eighth rate while the whole-window fit still reads
+1200/h) and confirms that genuine leaks -- constant, accelerating, and a
constant leak hiding under a one-time ramp -- are STILL failed.
"""
import csv
import os
import sys
import tempfile
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "soak"))
import oracle  # noqa: E402

DT = 30.0                 # sample interval (s)
WARMUP_S = 2160.0         # drops the first 72 samples, matching the real soak
N = 720                   # 6 h at 30 s
WARMUP_SAMPLES = 72
POST = N - WARMUP_SAMPLES  # 648 post-warmup samples analysed


def _series(post_eighth_rates, warmup_rate=8000.0, y0=0.0):
    """Integrate a g_structs_total trajectory: a steep warmup ramp (so the
    whole-window slope stays high) followed by 8 equal post-warmup eighths, each
    rising at its given per-HOUR rate.  Returns (ts, values)."""
    assert len(post_eighth_rates) == 8
    ts, ys, v = [], [], y0
    per_s = DT / 3600.0
    e = POST // 8
    for i in range(N):
        ts.append(i * DT)
        ys.append(v)
        if i < WARMUP_SAMPLES:
            rate = warmup_rate
        else:
            k = min(7, (i - WARMUP_SAMPLES) // e)
            rate = post_eighth_rates[k]
        v += rate * per_s
    return ts, ys


def _verdict(post_eighth_rates, **kw):
    ts, ys = _series(post_eighth_rates, **kw)
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="oracle_ratchet_")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t", "g_structs_total"])
            for t, y in zip(ts, ys):
                w.writerow(["%.1f" % t, "%.1f" % y])
        verdict, rows = oracle.analyze(path, WARMUP_S)
        g = next(r for r in rows if r["metric"] == "g_structs_total")
        return verdict, g
    finally:
        os.unlink(path)


class TestRatchetForgiveness(unittest.TestCase):
    def test_bumpy_converged_ratchet_forgiven_via_current_rate(self):
        # The real iter5/worker0 shape: a bumpy, NON-monotone decelerating ramp
        # (defeats the smooth log fit) whose final quarter still spans > floor
        # (defeats the flat-tail test) but whose final EIGHTH has decayed below
        # eps (64/h).  Only the current-rate path can forgive it -- and must.
        verdict, g = _verdict([3921, 4255, 814, 1119, 104, 446, 554, 36])
        self.assertGreater(g["slope_per_h"], 500.0,
                           "sanity: whole-window slope should look scary")
        self.assertTrue(g["ok"],
                        "converged ratchet (final-8th 36/h) not forgiven: %r" % g["reason"])
        self.assertIn("final-8th", g["reason"])
        self.assertEqual(verdict, "PASS")

    def test_constant_leak_still_fails(self):
        # A constant +400/h leak: final-eighth stays at 400/h (>> eps, CI clear of
        # 0) and the final-quarter span (~540) clears the floor -> FAIL.
        verdict, g = _verdict([400] * 8)
        self.assertFalse(g["ok"], "constant +400/h leak wrongly forgiven: %r" % g["reason"])
        self.assertEqual(verdict, "FAIL")

    def test_accelerating_leak_still_fails(self):
        verdict, g = _verdict([200, 400, 800, 1200, 1600, 2000, 2400, 2800])
        self.assertFalse(g["ok"], "accelerating leak wrongly forgiven: %r" % g["reason"])
        self.assertEqual(verdict, "FAIL")

    def test_leak_hiding_under_ramp_still_fails(self):
        # A one-time fill (looks like a ratchet) masking a persistent +300/h leak
        # whose final eighth never decays below eps -> the current-rate path must
        # NOT forgive it (300/h > 64 eps, CI clear of 0).
        verdict, g = _verdict([6000, 6000, 300, 300, 300, 300, 300, 300])
        self.assertFalse(g["ok"], "leak-under-ramp wrongly forgiven: %r" % g["reason"])
        self.assertEqual(verdict, "FAIL")

    def test_flat_plateau_passes(self):
        # No post-warmup growth at all -> passes the ordinary flat tests.
        verdict, g = _verdict([0] * 8, warmup_rate=2000.0)
        self.assertTrue(g["ok"], "flat plateau wrongly failed: %r" % g["reason"])
        self.assertEqual(verdict, "PASS")


if __name__ == "__main__":
    unittest.main()
