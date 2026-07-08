"""Snowboard PMC candidate extractor (tools/snowboard/pmc_candidates.py,
QA-steal-V2 #11).

Reframes each tsan-gold `data race` report as a same-address write/read PMC
candidate on two symbolized sites (the honest, buildable slice of Snowboard's
candidate step).  Pins the tsan-gold-aware parsing (the FIRST access is
capitalized, the second lowercased "Previous read" -- the case bug that made the
extractor silently return nothing), the in-scope filtering (deque / handle /
grace-reclaim), and candidate canonicalization.
"""
import os
import sys
import tempfile
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "snowboard"))
import pmc_candidates as pmc  # noqa: E402

SYNTH_RACE = """\
WARNING: ThreadSanitizer: data race (pid=1)
  Write of size 8 at 0xdeadbeef by thread T5:
    #0 cldeque_push src/runloom_c/cldeque.c:48 (mod+0x1) (BuildId: x)
    #1 hub_run src/runloom_c/mn_sched.c:10 (mod+0x2) (BuildId: x)
  Previous read of size 8 at 0xdeadbeef by thread T1 (mutexes: write M0):
    #0 cldeque_steal src/runloom_c/cldeque.c:98 (mod+0x3) (BuildId: x)
    #1 thief src/runloom_c/mn_sched.c:20 (mod+0x4) (BuildId: x)
  Location is heap block of size 64 at 0xdeadbe00 allocated by thread T2:
    #0 calloc x:1 (libtsan.so.2+0x1)
SUMMARY: ThreadSanitizer: data race src/runloom_c/cldeque.c:48
"""


class TestParse(unittest.TestCase):
    def _log(self, text):
        fd, path = tempfile.mkstemp(suffix=".tsan")
        with os.fdopen(fd, "w") as f:
            f.write(text)
        return path

    def test_parses_capitalized_and_lowercase_accesses(self):
        # The second access is "Previous read" (lowercase) -- both must parse.
        path = self._log(SYNTH_RACE)
        try:
            races = list(pmc.parse_log(path))
        finally:
            os.unlink(path)
        self.assertEqual(len(races), 1)
        a, b = races[0]["a"], races[0]["b"]
        self.assertEqual(a["site"][:2], ("cldeque.c", 48))
        self.assertEqual(b["site"][:2], ("cldeque.c", 98))
        self.assertEqual(a["addr"], b["addr"])          # same-address communication
        self.assertNotEqual(a["thread"], b["thread"])   # cross-thread

    def test_candidate_in_scope_and_kinds(self):
        path = self._log(SYNTH_RACE)
        try:
            c = pmc.candidate(next(pmc.parse_log(path)))
        finally:
            os.unlink(path)
        self.assertTrue(c["in_scope"], "a cldeque.c race must be flagged in-scope")
        self.assertEqual(c["kinds"], "RW")             # a Read and a Write, sorted
        self.assertTrue(c["same_addr"] and c["cross_thread"])
        # canonical (A,B) ordering so the pair dedupes regardless of report order
        self.assertLessEqual(c["siteA"], c["siteB"])

    def test_in_scope_predicate(self):
        self.assertTrue(pmc._in_scope(("cldeque.c", 48, "cldeque_push")))
        self.assertTrue(pmc._in_scope(("rl_handle.c", 141, "rl_handle_reclaim")))
        self.assertTrue(pmc._in_scope(("runloom_sched_core.c.inc", 750, "runloom_g_slab_alloc")))
        self.assertFalse(pmc._in_scope(("runloom_introspect.c", 627, "runloom_fiber_snapshot")))
        self.assertFalse(pmc._in_scope(None))


class TestRealCorpus(unittest.TestCase):
    def test_gold_smoke_log_yields_one_candidate(self):
        log = os.path.join(REPO, "docs", "dev", "soak",
                           "matrix_tsan-gold-smoke", "tsan.2137531")
        if not os.path.exists(log):
            self.skipTest("gold smoke log not present")
        races = list(pmc.parse_log(log))
        # the known introspect-vs-sched race on a g-struct (not in-scope)
        self.assertGreaterEqual(len(races), 1)
        c = pmc.candidate(races[0])
        self.assertFalse(c["in_scope"])   # introspect/sched, not deque/handle/grace


if __name__ == "__main__":
    unittest.main()
