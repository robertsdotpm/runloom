"""GC checkmark: a parked fiber's frame locals MUST stay GC roots across collect.

The one correctness surface runloom's 80-model formal stack structurally cannot
cover: CPython owns the GC, and ~94% of a parked fiber is its swapped-out C stack
/ suspended frame.  If the free-threaded GC's stop-the-world mark misses a root
that lives only on a PARKED fiber's frame, an object still reachable from that
fiber gets freed -> on resume the fiber holds a dangling reference (the arm64 GC
heisenbug class).  `test_gc_fibers.py` only checks PRE-CONSTRUCTED cycles survive;
it cannot detect a root the test never pre-registered.

This builds the ground-truth reachable set the hard way and checks it:
  * each of N goroutines creates a uniquely-stamped Sentinel held ONLY in a
    frame local (the sole strong reference), publishes a *weakref* to it, then
    PARKS (so its frame is suspended / stack swapped out);
  * a collector goroutine waits until all N are parked, runs gc.collect()
    several times (STW under M:N), and after each collect counts how many of the
    published weakrefs were CLEARED -- a cleared weakref means the GC freed an
    object that was still reachable from a parked fiber = a missed root;
  * on wake each goroutine re-reads its Sentinel's stamp (data-integrity: a
    freed-then-reused slot shows here too).

Oracle: zero weakrefs cleared while parked, every stamp intact.  Runs under M:N
churn at several hub counts.  (Run additionally under an ASan-built ext for
deterministic UAF detection -- the weakref oracle here needs no sanitizer.)
"""
import gc
import sys
import threading
import unittest
import weakref

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0] + "/src")
import runloom
import runloom.sync as rsync


class Sentinel(object):
    """Weakref-able, stamped; reachable ONLY via a parked fiber's local."""
    __slots__ = ("tag", "__weakref__")

    def __init__(self, tag):
        self.tag = tag


def _run_checkmark(n, hubs, rounds):
    refs = [None] * n            # published weakrefs (do NOT keep the objects alive)
    cleared_max = [0]
    bad_stamp = [0]
    ready = runloom.WaitGroup()
    ready.add(n)
    done = runloom.WaitGroup()
    done.add(n)
    release = rsync.Event()

    def holder(i):
        obj = Sentinel(i)                 # sole strong ref = this frame local
        refs[i] = weakref.ref(obj)
        ready.done()
        release.wait()                    # PARK: frame suspended, stack swapped out
        # resumed: obj must still be the same live, intact object
        if obj is None or obj.tag != i:
            bad_stamp[0] += 1
        del obj
        done.done()

    def collector():
        ready.wait()                      # all N created + (about to) park
        runloom.sleep(0.01)               # let the last few reach the park
        for _ in range(rounds):
            gc.collect()                  # STW mark over the parked-fiber roots
            c = sum(1 for wr in refs if wr is not None and wr() is None)
            if c > cleared_max[0]:
                cleared_max[0] = c
            runloom.sleep(0)
        release.set()

    def main():
        for i in range(n):
            runloom.fiber(holder, i)
        runloom.fiber(collector)
        done.wait()

    runloom.run(hubs, main)
    return cleared_max[0], bad_stamp[0]


class GCCheckmark(unittest.TestCase):

    def test_parked_fiber_locals_survive_collect(self):
        for hubs in (2, 4):
            cleared, bad = _run_checkmark(n=400, hubs=hubs, rounds=5)
            self.assertEqual(cleared, 0,
                             "GC freed {0} object(s) still reachable from a PARKED "
                             "fiber's frame (missed root, hubs={1})".format(cleared, hubs))
            self.assertEqual(bad, 0,
                             "{0} parked fiber(s) resumed with a corrupted/freed "
                             "local (hubs={1})".format(bad, hubs))

    def test_oracle_has_teeth(self):
        """Negative control: an object with NO strong ref (only a weakref) MUST be
        collected -- proving the weakref oracle above would actually FIRE if the GC
        freed a parked-fiber root.  If this didn't clear, the oracle is blind."""
        refs = [weakref.ref(Sentinel(i)) for i in range(200)]  # no strong refs kept
        gc.collect()
        cleared = sum(1 for wr in refs if wr() is None)
        self.assertGreater(cleared, 0,
                           "oracle blind: a strongly-unreferenced object was not "
                           "collected, so the checkmark test could not detect a "
                           "missed root")


if __name__ == "__main__":
    unittest.main()
