"""Offload (blocking-pool) scaling + correctness regression.

The blocking-offload pool was rebuilt for scale (see
docs/dev/OFFLOAD_REDESIGN_FINDINGS.md, "LANDED: blockpool scaling"):

  1. a SHARDED submit queue (per-hub lock+cond+workers) replacing the single
     global mutex that convoyed every submit + dequeue, and
  2. a PERSISTENT per-worker PyThreadState (attached only around the Python
     call) replacing the per-job PyGILState_Ensure/Release that created +
     destroyed a tstate every offload -- each create/destroy taking the runtime
     HEAD_LOCK, which serialized all workers (~20k offloads/s ceiling).

Together: ~20k -> ~670k offloads/s on 8 hubs (~33x), free-threaded 3.13t.

These tests pin the CORRECTNESS the rewrite must keep: every offload runs and
returns the right value across many hubs, results survive back-to-back run()
cycles (so a worker's cached tstate is reused soundly), and exceptions still
propagate.  Throughput is exercised but not asserted (machine-dependent).
"""
import unittest

import runloom


class TestOffloadScaling(unittest.TestCase):
    def test_results_correct_at_scale(self):
        """Every one of N offloads runs on a pool thread and returns its own
        value, with N >> hubs so work spreads across all hubs and workers.
        Distinct list slots per fiber => race-free check on free-threaded."""
        N = 8000
        res = [None] * N

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: res.__setitem__(i, runloom.blocking(lambda n=i: n ^ 0x5a5a)))

        runloom.run(8, main)
        missing = [i for i in range(N) if res[i] is None]
        wrong = [i for i in range(N) if res[i] != (i ^ 0x5a5a)]
        self.assertEqual(missing, [], "%d offloads never completed" % len(missing))
        self.assertEqual(wrong, [], "%d offloads returned wrong values" % len(wrong))

    def test_multi_run_tstate_reuse(self):
        """Back-to-back run() cycles: each pool worker creates its persistent
        tstate once and REUSES it across runs -- all offloads must still
        complete every cycle (no lost wakeup, no stale-tstate crash)."""
        N = 3000
        for cycle in range(4):
            done = [False] * N

            def main():
                for i in range(N):
                    runloom.fiber(lambda i=i: (runloom.blocking(lambda: None),
                                               done.__setitem__(i, True)))

            runloom.run(8, main)
            missing = [i for i in range(N) if not done[i]]
            self.assertEqual(missing, [], "cycle %d: %d offloads lost" % (cycle, len(missing)))

    def test_exception_propagates(self):
        """An exception raised inside the offloaded call surfaces at the
        blocking() call site (captured + normalised on the worker)."""
        seen = {}

        def boom():
            raise ValueError("kaboom-42")

        def main():
            def f():
                try:
                    runloom.blocking(boom)
                except ValueError as e:
                    seen["msg"] = str(e)

            runloom.fiber(f)

        runloom.run(4, main)
        self.assertEqual(seen.get("msg"), "kaboom-42")


if __name__ == "__main__":
    unittest.main()
