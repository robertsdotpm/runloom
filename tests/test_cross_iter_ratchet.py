"""Cross-iteration ratchet-leak backstop (tools/soak/cross_iter_ratchet.py).

Closes the per-run oracle's one single-window blind spot: a slow constant leak on
a retain-forever pool whose 6h movement stays under the metric floor is
indistinguishable from slow pool-creep WITHIN a window, but across ITERATIONS a
pool plateaus (fixed HWM) while a leak climbs without bound.  This pins the
verdict classifier (PLATEAU / FILLING / LEAK / SHORT) and the record->check
round-trip.
"""
import csv
import os
import sys
import tempfile
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "soak"))
import cross_iter_ratchet as cir  # noqa: E402
import oracle  # noqa: E402

FLOOR = oracle.ABSOLUTE_FLOOR["g_structs_total"]  # 320


def _judge(ends):
    return cir.judge_metric(list(enumerate(ends, start=1)), FLOOR)[0]


class TestJudge(unittest.TestCase):
    def test_settled_pool_is_plateau(self):
        # END plateaus ~11000 with slab-batch noise (+-50): a settled pool.
        self.assertEqual(
            _judge([10950, 11010, 10980, 11020, 10990, 11005, 10970, 11015,
                    10995, 11008, 10985, 11002]),
            "PLATEAU")

    def test_filling_pool_decelerating_is_not_a_leak(self):
        # A pool still reaching its HWM climbs but decelerates -> FILLING, not LEAK.
        self.assertEqual(
            _judge([3000, 6000, 8000, 9500, 10300, 10700, 10900, 11000,
                    11050, 11080, 11095, 11100]),
            "FILLING")

    def test_slow_constant_leak_is_caught(self):
        # +1200/iter == a +200/h leak over 6h iterations: the exact single-window
        # residual the per-run oracle forgives.  Must surface cross-iteration.
        self.assertEqual(_judge([5000 + 1200 * i for i in range(12)]), "LEAK")

    def test_fast_and_accelerating_leaks_caught(self):
        self.assertEqual(_judge([5000 + 6000 * i for i in range(12)]), "LEAK")
        self.assertEqual(
            _judge([3000, 3400, 4200, 5800, 9000, 15400, 28200, 53800,
                    105000, 207000, 410000, 815000]), "LEAK")

    def test_oscillation_hidden_leak_is_caught(self):
        # THE ADVERSARIAL BLINDING (was PLATEAU under an OLS slope+CI test): a
        # genuine +500/iter unbounded leak buried under +-2400 run-to-run HWM
        # jitter.  Difference-of-block-means cancels the symmetric jitter and
        # recovers the trend -> LEAK.
        series = [100000 + 500 * i + 2400 * ((-1) ** i) for i in range(20)]
        self.assertEqual(_judge(series), "LEAK")

    def test_very_slow_fill_not_false_flagged(self):
        # A genuinely slow-filling (still decelerating) pool must not read as LEAK.
        self.assertIn(
            _judge([2000, 3500, 4600, 5400, 6000, 6450, 6790, 7050,
                    7250, 7405, 7520, 7600]),
            ("FILLING", "PLATEAU"))

    def test_sub_floor_leak_is_accepted_limit(self):
        # A leak at exactly the floor rate (~53/h) reads PLATEAU -- the documented
        # accepted limit (below the per-run oracle's harmless-drift epsilon).
        self.assertEqual(_judge([1000 + 320 * i for i in range(12)]), "PLATEAU")

    def test_short_history_is_undecided(self):
        self.assertEqual(_judge([3000, 6000, 9000, 12000]), "SHORT")


class TestRecordAndCheck(unittest.TestCase):
    def _worker_csv(self, path, metric, ramp_to, n=160):
        # A decelerating ramp to `ramp_to` so the final-eighth median ~= ramp_to.
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t", metric])
            for i in range(n):
                frac = 1.0 - (1.0 - i / (n - 1)) ** 2   # concave, saturating
                w.writerow(["%.1f" % (i * 30.0), "%.1f" % (ramp_to * frac)])

    def test_record_then_check_flags_leak(self):
        d = tempfile.mkdtemp(prefix="cir_")
        ledger = os.path.join(d, "ledger.csv")
        # 12 iterations whose pool END climbs +1500 each: an unbounded leak.
        for it in range(1, 13):
            outdir = os.path.join(d, "iter%d" % it)
            os.mkdir(outdir)
            end = 4000 + 1500 * it
            self._worker_csv(os.path.join(outdir, "worker0.csv"), "g_structs_total", end)
            self._worker_csv(os.path.join(outdir, "worker1.csv"), "g_structs_total", end)
            cir.append_ledger(ledger, it, cir.read_end_values(outdir))
        ok, results = cir.check(ledger)
        states = {m: s for m, s, _ in results}
        self.assertEqual(states.get("g_structs_total"), "LEAK", results)
        self.assertFalse(ok)

    def test_record_then_check_plateau_ok(self):
        d = tempfile.mkdtemp(prefix="cir_")
        ledger = os.path.join(d, "ledger.csv")
        for it in range(1, 13):
            outdir = os.path.join(d, "iter%d" % it)
            os.mkdir(outdir)
            end = 11000 + ((it * 37) % 90) - 45   # bounded +-45 noise, no trend
            self._worker_csv(os.path.join(outdir, "worker0.csv"), "g_structs_total", end)
            cir.append_ledger(ledger, it, cir.read_end_values(outdir))
        ok, results = cir.check(ledger)
        states = {m: s for m, s, _ in results}
        self.assertEqual(states.get("g_structs_total"), "PLATEAU", results)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
