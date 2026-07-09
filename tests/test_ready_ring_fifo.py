"""Per-sched ready FIFO ring: order preserved across grow and wraparound.

The ready ring (runloom_sched_datastack.c.inc: runloom_sched_ready_push /
_pop / runloom_ready_grow) is the single-thread scheduler's FIFO of runnable gs
(freshly spawned, yielded, or woken).  It is a power-of-two ring buffer: cap
starts at 64 and DOUBLES on overflow (runloom_ready_grow: `old_cap * 2`), with
head/tail indices masked (`& ready_mask`) so they wrap past the buffer end.

Previously this was exercised only incidentally by the deque-overflow stress
(thousands of fresh gs), which would still complete every fiber even if the ring
reordered them -- so a FIFO-violating reorder across a grow or a wraparound would
pass.  These assert the ordering directly.

Runs on the single-thread scheduler (runloom_c.run); no special env needed.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
os.environ["PYTHON_GIL"] = "0"
import runloom_c            # noqa: E402


class TestReadyRingFifo(unittest.TestCase):
    def test_fifo_preserved_across_grows(self):
        """Spawn far more than the initial cap (64) BEFORE run(), so the ring
        grows 64->128->256 during the spawn burst (each runloom_c.fiber() pushes
        immediately).  run() then pops FIFO: the fibers must execute in exact
        spawn order, proving the grow's order-preserving copy is correct."""
        N = 200                      # 64 -> 128 -> 256: two doublings
        order = []
        for k in range(N):
            runloom_c.fiber((lambda k=k: order.append(k)))
        runloom_c.run()
        self.assertEqual(len(order), N)
        self.assertEqual(order, list(range(N)),
                         "ready ring must stay FIFO across capacity grows")
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_fifo_preserved_across_wraparound(self):
        """A small set of fibers (<= cap, so NO grow) that each yield many times
        cycles head/tail far past the ring end -- 8 fibers x 25 yields = 200
        push/pop pairs over a 64-slot ring, wrapping the masked indices ~3x.  The
        run sequence must be exact round-robin (0..7 repeated): any masking /
        wraparound bug would drop, duplicate, or reorder a fiber."""
        NF, ROUNDS = 8, 25
        seq = []

        def mk(k):
            def w():
                for _ in range(ROUNDS):
                    seq.append(k)
                    runloom_c.sched_yield()
            return w

        for k in range(NF):
            runloom_c.fiber(mk(k))
        runloom_c.run()

        self.assertEqual(len(seq), NF * ROUNDS)
        self.assertEqual(seq, list(range(NF)) * ROUNDS,
                         "ready ring must stay round-robin FIFO across index wraparound")
        self.assertEqual(runloom_c._self_check(0), 0)


if __name__ == "__main__":
    unittest.main()
