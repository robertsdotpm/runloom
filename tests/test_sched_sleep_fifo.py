"""Sleep-heap equal-deadline FIFO tiebreak (sleep_seq).

The sleep heap is a min-heap keyed on (wake_at, sleep_seq); sleep_seq is a
monotonic counter assigned at push time so that fibers sleeping to the SAME
deadline wake in insertion (FIFO) order -- matching asyncio's (when, seq)
TimerHandle order (runloom_sched_datastack.c.inc: runloom_sleep_before /
sleep_seq_ctr, runloom_sched.h:251/584).  Without the tiebreak, equal-deadline
sleepers would wake in arbitrary heap order (siblings with an equal primary key
land in sift-decided, not insertion, order).

This was previously exercised only by an asyncio-bridge integration repro
(bughunt_repros/r04_timer_fifo.py) that has no `def test_` and is NOT collected
by the suite -- so the raw sched_sleep tiebreak had zero running coverage.

To make the tiebreak the SOLE discriminator we run under RUNLOOM_LOGICAL_CLOCK:
the logical clock does not advance while there is runnable work, so every fiber
that sleeps the same duration during the initial pass computes the identical
wake_at.  Wake order is then decided purely by sleep_seq.  RUNLOOM_LOGICAL_CLOCK
is lazy-read from env on first use; set it before import.  run_isolated gives
this file its own subprocess.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
os.environ["PYTHON_GIL"] = "0"
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")
import runloom_c            # noqa: E402


class TestSleepSeqFifo(unittest.TestCase):
    def test_equal_deadline_wakes_in_insertion_order(self):
        """N fibers sleep the SAME duration (identical wake_at under the logical
        clock), so ONLY the sleep_seq FIFO tiebreak can order their wake.  N is
        chosen > the initial heap cap (16) so the heap grows (16->32->64) and the
        sift paths run -- a min-heap without the tiebreak would not preserve
        insertion order among equal keys across those sifts."""
        N = 48
        order = []

        def mk(k):
            def w():
                runloom_c.sched_sleep(0.05)
                order.append(k)
            return w

        for k in range(N):
            runloom_c.fiber(mk(k))
        runloom_c.run()

        self.assertEqual(len(order), N)
        self.assertEqual(order, list(range(N)),
                         "equal-deadline sleepers must wake in insertion (FIFO) order")
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_distinct_deadlines_still_earliest_first(self):
        """Sanity companion: with DISTINCT deadlines the heap must still wake
        earliest-first regardless of insertion order (the primary key), so the
        tiebreak does not disturb ordinary min-heap ordering.  Spawn in reverse
        deadline order; assert they wake in deadline order."""
        order = []

        def mk(k, secs):
            def w():
                runloom_c.sched_sleep(secs)
                order.append(k)
            return w

        # spawn latest-deadline first; nearest deadline (k=0) must wake first.
        n = 20
        for k in reversed(range(n)):
            runloom_c.fiber(mk(k, 0.01 + k * 0.001))
        runloom_c.run()

        self.assertEqual(order, list(range(n)),
                         "distinct deadlines must wake earliest-first")
        self.assertEqual(runloom_c._self_check(0), 0)


if __name__ == "__main__":
    unittest.main()
