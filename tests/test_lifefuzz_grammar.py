"""Resource-typed grammar fuzzer for lifefuzz (tools/lifefuzz/lifefuzz.py,
QA-steal-V2 #17).

build_grammar_spec emits a syzlang-style typed op sequence (Chan + G resources,
each op referencing earlier handles) that is WELL-FORMED BY CONSTRUCTION -- every
channel drained + closed, so it terminates and the exact conserved token multiset
is known.  This pins that (a) generated programs are well-formed (run clean under
the full oracle net), (b) the op list is genuinely typed/dataflow-shaped, and (c)
the conservation + completion oracles keep their teeth (a corrupted expectation is
caught) so the reused oracle net is not silently slack on grammar programs.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "lifefuzz"))
sys.path.insert(0, REPO)
import lifefuzz as lf  # noqa: E402


class TestGrammarSpec(unittest.TestCase):
    def test_spec_is_typed_op_list_with_dataflow(self):
        spec = lf.build_grammar_spec(7)
        self.assertEqual(spec["kind"], "grammar")
        ops = spec["ops"]
        kinds = set(o["t"] for o in ops)
        self.assertIn("chan", kinds)
        self.assertTrue({"range_cons", "select_cons"} & kinds, "no consumer op emitted")
        chan_ids = set(o["id"] for o in ops if o["t"] == "chan")
        # every producer/consumer references a channel that a chan op created
        for o in ops:
            if o["t"] == "producer":
                self.assertIn(o["chan"], chan_ids)
            if o["t"] == "range_cons":
                self.assertIn(o["chan"], chan_ids)
            if o["t"] == "select_cons":
                self.assertTrue(set(o["chans"]) <= chan_ids)
        # every channel is covered by at least one draining consumer (well-formed)
        covered = set()
        for o in ops:
            if o["t"] == "range_cons":
                covered.add(o["chan"])
            elif o["t"] == "select_cons":
                covered.update(o["chans"])
        self.assertTrue(chan_ids <= covered, "a channel has no draining consumer")

    def test_deterministic(self):
        self.assertEqual(lf.build_grammar_spec(11), lf.build_grammar_spec(11))


class TestGrammarWellFormed(unittest.TestCase):
    def test_seeds_run_clean(self):
        for seed in (1, 2, 3, 5, 8):
            spec = lf.build_grammar_spec(seed)
            ok, reason = lf.run_program(spec, timeout=20.0)
            self.assertTrue(ok, "grammar seed %d not well-formed: %s" % (seed, reason))


class TestGrammarOracleTeeth(unittest.TestCase):
    def test_conservation_teeth(self):
        # A corrupted token expectation must be caught (the reused conservation
        # oracle is not slack on grammar programs).
        spec = lf.build_grammar_spec(3)
        spec["exp_count"] += 1
        spec["exp_sum"] += 999983
        ok, reason = lf.run_program(spec, timeout=20.0)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("CONSERVATION"), reason)

    def test_sumsq_teeth_independent_of_count_and_sum(self):
        # The sum-of-squares side-channel must be checked INDEPENDENTLY: corrupt
        # only exp_sumsq (count + sum intact) -- this is the count+sum-preserving
        # multiset-corruption class (e.g. {0,1,2,3} delivered as {0,0,3,3}).
        spec = lf.build_grammar_spec(4)
        spec["exp_sumsq"] += 4         # count and sum untouched
        ok, reason = lf.run_program(spec, timeout=20.0)
        self.assertFalse(ok, "sum-of-squares dimension not actually checked")
        self.assertTrue(reason.startswith("CONSERVATION"), reason)

    def test_completion_teeth(self):
        # A wrong spawned-count expectation must be caught by the completion oracle
        # (only meaningful under mn, which returns a completed count).
        spec = None
        for seed in range(1, 40):
            s = lf.build_grammar_spec(seed)
            if s["mode"] == "mn":
                spec = s
                break
        self.assertIsNotNone(spec, "no mn-mode grammar seed found")
        spec["exp_spawned"] += 1
        ok, reason = lf.run_program(spec, timeout=20.0)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("COMPLETION"), reason)


if __name__ == "__main__":
    unittest.main()
