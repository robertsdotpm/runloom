"""Free-threaded cyclic GC x parked goroutines.

A parked goroutine holds its Python locals on a SWAPPED-OUT stack: the
suspended interpreter frames are NOT on the scheduler thread's current-frame
chain, so the cyclic GC never traverses them as roots.  If the GC's
reachability accounting (which on free-threaded 3.13t also involves deferred
/ biased refcounting) failed to keep alive a reference cycle held only by
such a parked goroutine, gc.collect() would free live objects -> a
use-after-free when the goroutine resumes.

This hammers gc.collect() while many goroutines are parked (across every
park type: sleep, channel, park_self) holding reference cycles on their
stacks, then verifies every cycle survived intact.  Run it under ASan to
turn any premature free into a hard error.
"""
import gc
import sys
import weakref
import unittest

sys.path.insert(0, "src")

import pygo
import pygo_core


class _Node(object):
    """Plain class so instances support weakref (dicts/lists do not)."""
    pass


def _make_cycle(tag):
    """A self-referential cycle (uncollectable except by the cyclic GC),
    reachable only through the caller's local `a`."""
    a = _Node()
    b = _Node()
    a.tag = tag
    a.fwd = b
    b.back = a            # a -> b -> a, a real cycle
    return a, weakref.ref(a)


def _verify(a, wr, tag):
    return (wr() is a and a.tag == tag and a.fwd.back is a)


class TestGCWithParkedGoroutines(unittest.TestCase):
    def _run(self, park):
        N = 200
        results = {}

        def worker(i, done):
            a, wr = _make_cycle(i)
            park(i)                       # park holding the cycle on our stack
            results[i] = _verify(a, wr, i)
            done.send(1)

        def main():
            done = pygo_core.Chan(N)
            for i in range(N):
                pygo.go(worker, i, done)
            pygo.sleep(0.01)              # let them all park holding cycles
            for _ in range(30):           # hammer the cyclic GC
                gc.collect()
            for _ in range(N):
                done.recv()

        pygo.run(main)
        self.assertEqual(len(results), N)
        self.assertTrue(all(results.values()),
                        "a cycle held by a parked goroutine was collected")

    def test_control_unheld_cycle_is_collected(self):
        # Sanity: the cycles we build ARE collectable (so the tests above are
        # not vacuously passing).  An unheld cycle must die at gc.collect().
        _a, wr = _make_cycle(0)
        del _a
        gc.collect()
        self.assertIsNone(wr(), "cycle should have been collected once unheld")

    def test_sleep_parked(self):
        self._run(lambda i: pygo.sleep(0.06))

    def test_chan_parked(self):
        # each worker parks recv-ing on its own channel; main sends after GC
        chans = {}

        def park(i):
            ch = pygo_core.Chan(0)
            chans[i] = ch
            ch.recv()                     # parks here holding the cycle

        N = 150
        results = {}

        def worker(i, done):
            a, wr = _make_cycle(i)
            park(i)
            results[i] = _verify(a, wr, i)
            done.send(1)

        def main():
            done = pygo_core.Chan(N)
            for i in range(N):
                pygo.go(worker, i, done)
            pygo.sleep(0.02)              # let them park on their chans
            for _ in range(30):
                gc.collect()
            for i in range(N):            # wake each
                chans[i].send(1)
            for _ in range(N):
                done.recv()

        pygo.run(main)
        self.assertEqual(len(results), N)
        self.assertTrue(all(results.values()),
                        "a cycle held by a chan-parked goroutine was collected")

    def test_park_self(self):
        handles = {}

        def worker(i, done):
            a, wr = _make_cycle(i)
            handles[i] = pygo_core.current_g()
            pygo_core.park_self()         # parks holding the cycle
            results_ok = _verify(a, wr, i)
            done.send(1 if results_ok else 0)

        def main():
            N = 150
            done = pygo_core.Chan(N)
            for i in range(N):
                pygo.go(worker, i, done)
            pygo.sleep(0.02)
            for _ in range(30):
                gc.collect()
            for i in range(N):
                handles[i].wake()
            oks = 0
            for _ in range(N):
                oks += done.recv()[0]
            self.assertEqual(oks, N, "a cycle held by a park_self goroutine was collected")

        pygo.run(main)


if __name__ == "__main__":
    unittest.main()
