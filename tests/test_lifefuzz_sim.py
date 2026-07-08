"""lifefuzz `sim` kind wiring (tools/lifefuzz/lifefuzz.py -> tools/dst/simnet.py).

The deterministic sim-network workload folded into the lifefuzz fleet as a `sim`
kind, so the existing seed-iterating soak (rr chaos, millions of seeds) exercises
it -- with the INSTANT count_deadlocked lost-wake oracle instead of a wall-clock
timeout, and a one-integer repro key.  This pins the dispatch (build_spec ->
kind=sim -> simnet.sim_program) and that a sim run is a clean, deterministic,
pure-function-of-seed unit.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "lifefuzz"))
sys.path.insert(0, REPO)
import lifefuzz as lf  # noqa: E402


class TestSimKindWiring(unittest.TestCase):
    def test_forced_kind_sim(self):
        old = os.environ.get("LIFEFUZZ_KIND")
        os.environ["LIFEFUZZ_KIND"] = "sim"
        try:
            spec = lf.build_spec(123)
        finally:
            if old is None:
                os.environ.pop("LIFEFUZZ_KIND", None)
            else:
                os.environ["LIFEFUZZ_KIND"] = old
        self.assertEqual(spec["kind"], "sim")
        self.assertEqual(spec["seed"], 123)

    def test_sim_runs_clean_and_deterministic(self):
        for seed in (1, 2, 3, 7, 11):
            spec = {"seed": seed, "kind": "sim"}
            ok1, r1 = lf.run_program(spec, timeout=20.0)
            ok2, r2 = lf.run_program(spec, timeout=20.0)
            self.assertTrue(ok1, "sim seed %d not clean: %s" % (seed, r1))
            self.assertEqual((ok1, r1), (ok2, r2),
                             "sim seed %d not deterministic" % seed)

    def test_sim_is_in_default_rotation(self):
        kinds = set(lf.build_spec(s)["kind"] for s in range(1, 400))
        self.assertIn("sim", kinds, "sim kind never drawn in the default rotation")


if __name__ == "__main__":
    unittest.main()
