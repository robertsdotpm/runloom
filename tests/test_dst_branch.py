"""Antithesis-style branch-seed decision-replay in the DST harness
(tools/dst/dst.py, QA-steal-V2 #13).

From ONE seed, replaying both branches (yield / no-yield) of a single decision
point -- with the shared rng stream held aligned so only that decision differs --
reaches executions the seed's own run pinned one way.  This pins that the
mechanism has TEETH: on the strict-FIFO negative control, seeds whose OWN run is
clean still reveal the bug under branch-replay; a bug-free scenario reveals none.
The seed IS the snapshot and replay IS the fork -- no os.fork (forking mid
runloom_c.run() on an fcontext stack would hang/SEGV).
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))
import dst  # noqa: E402


def _clean_base_seed(scen, horizon, strat_factory, limit=200):
    """First seed whose own base run does NOT trip the invariant."""
    for seed in range(1, limit):
        _, err = dst.run_once(scen, seed, strat_factory(), horizon)
        if not err:
            return seed
    return None


class TestBranchSeeds(unittest.TestCase):
    def test_branch_reveals_bug_a_clean_seed_misses(self):
        scen = dst.scenario_BUG_strict_order
        horizon = dst.calibrate(scen)
        seed = _clean_base_seed(scen, horizon, lambda: dst.UniformYield(0.5))
        self.assertIsNotNone(seed, "no clean base seed found (scenario always fails?)")
        # The seed's own run is clean...
        _, base_err = dst.run_once(scen, seed, dst.UniformYield(0.5), horizon)
        self.assertIsNone(base_err, "expected a clean base run for seed %s" % seed)
        # ...but forcing some single decision reveals the bug.
        found = None
        for k in range(1, horizon + 1):
            for forced in (True, False):
                _, err = dst.run_once(
                    scen, seed, dst.ForcedAt(dst.UniformYield(0.5), k, forced), horizon)
                if err:
                    found = (k, forced, err)
                    break
            if found:
                break
        self.assertIsNotNone(
            found, "branch-replay revealed no bug from clean seed %s" % seed)

    def test_forcedat_aligns_rng_stream_off_k(self):
        # Two branches from one seed must differ ONLY at k: forcing k to the value
        # the base already chose there must reproduce the base execution exactly.
        scen = dst.scenario_BUG_strict_order
        horizon = dst.calibrate(scen)
        seed = 1
        base_sig, base_err = dst.run_once(scen, seed, dst.UniformYield(0.5), horizon)
        # Determine the base decision at step 1 by trying both and matching sig.
        matched = False
        for forced in (True, False):
            sig, err = dst.run_once(
                scen, seed, dst.ForcedAt(dst.UniformYield(0.5), 1, forced), horizon)
            if sig == base_sig and err == base_err:
                matched = True
        self.assertTrue(
            matched, "forcing step 1 to its base value did not reproduce the base run"
                     " -- rng stream not aligned")

    def test_bugfree_scenario_yields_no_branch_bug(self):
        # A scenario with no invariant a yield can break must reveal no bug.
        scen = dst.scenario_unbuffered_handoff
        horizon = dst.calibrate(scen)
        for seed in range(1, 6):
            for k in range(1, horizon + 1):
                for forced in (True, False):
                    _, err = dst.run_once(
                        scen, seed, dst.ForcedAt(dst.UniformYield(0.5), k, forced), horizon)
                    self.assertIsNone(
                        err, "branch spuriously flagged a bug in a sound scenario "
                             "(seed=%s k=%s forced=%s): %s" % (seed, k, forced, err))

    def test_branchsweep_teeth(self):
        # The teeth metric: at least one clean seed's bug is found only by branching.
        rc = dst.cmd_branchsweep(40, 0)
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
