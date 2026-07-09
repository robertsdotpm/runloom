"""lifefuzz `simfd` kind wiring (the socketpair byte-plane soak).

The deterministic socketpair workload (tools/dst/simnet_fd.simfd_program) folded
into the lifefuzz fleet as a `simfd` kind, so the existing seed-iterating soak (rr
chaos, millions of seeds) exercises the REAL sim netpoll pump -- the C park/commit/
deadline/wake path as a function of the seed, coverage the Chan-based `sim` kind
cannot reach.  Pins the dispatch (build_spec -> kind=simfd -> simnet_fd.simfd_program)
and that a simfd run is a clean, deterministic, pure-function-of-seed unit.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "lifefuzz"))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))
sys.path.insert(0, os.path.join(REPO, "src"))
os.environ["RUNLOOM_SIM"] = "1"          # before any runloom_c import in run_program
import lifefuzz as lf  # noqa: E402


class TestSimfdKindWiring(unittest.TestCase):
    def test_forced_kind_simfd(self):
        old = os.environ.get("LIFEFUZZ_KIND")
        os.environ["LIFEFUZZ_KIND"] = "simfd"
        try:
            spec = lf.build_spec(123)
        finally:
            if old is None:
                os.environ.pop("LIFEFUZZ_KIND", None)
            else:
                os.environ["LIFEFUZZ_KIND"] = old
        self.assertEqual(spec["kind"], "simfd")
        self.assertEqual(spec["seed"], 123)

    def test_simfd_runs_clean_and_deterministic(self):
        for seed in (2, 8, 21, 58):
            spec = {"seed": seed, "kind": "simfd"}
            ok1, r1 = lf.run_program(spec, timeout=20.0)
            ok2, r2 = lf.run_program(spec, timeout=20.0)
            self.assertTrue(ok1, "simfd seed %d not clean: %s" % (seed, r1))
            self.assertEqual((ok1, r1), (ok2, r2),
                             "simfd seed %d not deterministic" % seed)

    def test_simfd_is_in_default_rotation(self):
        kinds = set(lf.build_spec(s).get("kind", "core") for s in range(1, 400))
        self.assertIn("simfd", kinds, "simfd kind never drawn in the default rotation")


if __name__ == "__main__":
    unittest.main()
