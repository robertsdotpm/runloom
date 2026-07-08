"""Cross-hub alias-pair coverage for the greybox interleaving fuzzer
(tools/mn_controlled/chess_greybox.py, QA-steal-V2 #12).

The preemption-EDGE fingerprint rewards new hub-switch positions but is blind to
WHICH shared objects the switched segments touch.  alias_pairs adds a second
coverage dimension -- the ordered (objA, objB) cells where object objA is followed
by objB on a DIFFERENT hub -- so the fuzzer is steered toward diverse cross-object
interleavings (the KRACE/CONZZER idea).  This pins the extraction logic (hub
attribution, obj==0 collapse, same-hub exclusion), the namespacing of the two
coverage dimensions, and that a channel workload actually yields alias-pairs.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "mn_controlled"))
import chess_greybox as gb  # noqa: E402
import chess_explore as ce  # noqa: E402


def _rec(hub, obj):
    return {"hub": hub, "obj": obj, "k": 0, "def": 0, "cnt": 1}


class TestAliasPairs(unittest.TestCase):
    def test_cross_hub_pairs_extracted(self):
        # obj at index i is attributed to hub[i-1]; consecutive object-touchers on
        # DIFFERENT hubs emit a pair.
        trace = [_rec(0, 0), _rec(1, 1), _rec(0, 2), _rec(1, 1)]
        # i=1 obj1@hub0; i=2 obj2@hub1 (0!=1 -> (1,2)); i=3 obj1@hub0 (1!=0 -> (2,1))
        self.assertEqual(gb.alias_pairs(trace), {(1, 2), (2, 1)})

    def test_same_hub_touchers_emit_no_pair(self):
        # two consecutive object-touchers on the SAME hub are not a cross-hub race
        trace = [_rec(0, 0), _rec(9, 1), _rec(9, 2)]
        # i=1 obj1@hub0; i=2 obj2@hub9 -> hubs 0 vs 9 differ -> (1,2)
        self.assertEqual(gb.alias_pairs(trace), {(1, 2)})
        # now make both touchers land on the same attributed hub
        trace2 = [_rec(5, 0), _rec(5, 1), _rec(5, 2)]
        # i=1 obj1@hub5; i=2 obj2@hub5 -> same hub -> no pair
        self.assertEqual(gb.alias_pairs(trace2), set())

    def test_obj0_segments_collapsed(self):
        # obj==0 (CPU/sleep/lock) segments are independent: skipped, and they do
        # not break the pairing of the object-touchers around them.
        trace = [_rec(0, 0), _rec(1, 1), _rec(3, 0), _rec(0, 2)]
        # i=1 obj1@hub0; i=2 obj0 -> skipped; i=3 obj2@hub3 -> prev(hub0,obj1) vs hub3 -> (1,2)
        self.assertEqual(gb.alias_pairs(trace), {(1, 2)})

    def test_schedule_cover_namespaced_and_disjoint(self):
        trace = [_rec(0, 0), _rec(1, 1), _rec(0, 2)]
        cov = gb.schedule_cover(trace)
        tags = set(c[0] for c in cov)
        self.assertTrue(tags <= {"edge", "pair"})
        # the pair dimension is present and tagged
        self.assertIn(("pair", 1, 2), cov)
        # edge cells and pair cells never collide
        edges = set(c for c in cov if c[0] == "edge")
        pairs = set(c for c in cov if c[0] == "pair")
        self.assertEqual(edges & pairs, set())


class TestOnRealWorkload(unittest.TestCase):
    def test_chess_chan_yields_cross_hub_alias_pairs(self):
        # A two-independent-channel workload must produce cross-hub alias-pairs
        # (objects 1 and 2 interleaved across hubs); a bounded enumeration suffices.
        wl = os.path.join(REPO, "tools", "mn_controlled", "chess_chan.py")
        env = {"CHESS_M": "2"}
        pairs = set()
        stack = [[]]
        runs = 0
        while stack and runs < 120:
            prefix = stack.pop()
            runs += 1
            tr, outcome, last, hub = ce.run_prefix(wl, prefix, 20, env)
            pairs |= gb.alias_pairs(tr)
            fi = None
            for i in range(len(prefix), len(tr)):
                if tr[i]["cnt"] >= 2:
                    fi = i
                    break
            if fi is None:
                continue
            base = [tr[i]["k"] for i in range(fi)]
            for j in range(tr[fi]["cnt"]):
                stack.append(base + [j])
        self.assertTrue(pairs, "no cross-hub alias-pairs from a two-channel workload")
        # objects are the two channels' dpor ids (1, 2); pairs are among {1,2}
        objs = set(a for a, b in pairs) | set(b for a, b in pairs)
        self.assertTrue(objs <= {1, 2}, objs)


if __name__ == "__main__":
    unittest.main()
