"""Chan tp_traverse / tp_clear GC-integration audit (QA-steal-V2 #5).

Chan is the ONLY GC-tracked runloom C type (Py_TPFLAGS_HAVE_GC).  Its tp_traverse
(RunloomChan_traverse -> runloom_chan_gc_traverse) exposes the strong refs its C
ring buffer holds, so the free-threaded cyclic collector can see a cycle that
reaches back through a buffered value; tp_clear drops them to break it.  A
traverse that OMITS a buffered value -> the collector can't break the cycle -> an
uncollectable leak; the inverse (clearing/freeing a value still buffered) is a
UAF.  Nothing else audits this: test_gc_fibers only checks cycles held by parked
fiber STACKS, never through a Chan buffer.  The g/task types are deliberately NOT
GC-tracked (manual refcounting), so Chan is the whole GC-trackable surface.
"""
import gc
import os
import sys
import unittest
import weakref

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import runloom_c


class Node(object):
    """Weakref-able buffered value (object() has no __weakref__)."""
    __slots__ = ("ref", "__weakref__")


class TestChanGCTraverse(unittest.TestCase):
    def test_get_referents_reflects_buffer(self):
        # tp_traverse completeness: gc.get_referents(chan) walks tp_traverse and
        # must expose EXACTLY the buffered strong refs.
        ch = runloom_c.Chan(4)
        vals = [Node() for _ in range(3)]
        for v in vals:
            self.assertTrue(ch.try_send(v))
        refs = gc.get_referents(ch)
        for v in vals:
            self.assertTrue(any(r is v for r in refs),
                            "tp_traverse did not expose a buffered value")
        # After draining, the buffer holds nothing.
        for _ in vals:
            ch.try_recv()
        refs2 = gc.get_referents(ch)
        for v in vals:
            self.assertFalse(any(r is v for r in refs2),
                             "tp_traverse still exposes a drained value")

    def test_buffered_cycle_is_collected(self):
        # chan <-> obj cycle THROUGH the buffer: obj references chan, chan buffers
        # obj.  Drop all external refs; only a weakref remains.  The cyclic
        # collector must break it via Chan.tp_traverse (see the buffered obj) +
        # tp_clear (drop it).  A traverse-completeness regression leaks it.
        ch = runloom_c.Chan(2)
        obj = Node()
        obj.ref = ch                          # obj -> ch
        self.assertTrue(ch.try_send(obj))     # ch buffer -> obj  (cycle closed)
        wr = weakref.ref(obj)
        del obj, ch
        gc.collect()
        self.assertIsNone(wr(), "a cycle through a Chan buffer was NOT collected "
                                "-- Chan.tp_traverse/tp_clear is incomplete")

    def test_control_unheld_cycle_is_collected(self):
        # Sanity so the test above is not vacuous: a plain a<->b cycle (no Chan)
        # is collected, i.e. the collector is running and Node cycles die.
        a, b = Node(), Node()
        a.ref = b
        b.ref = a
        wr = weakref.ref(a)
        del a, b
        gc.collect()
        self.assertIsNone(wr(), "control cycle should have been collected")

    def test_dealloc_drops_buffered_refs(self):
        # tp_dealloc path (not just the cyclic collector): dropping the only ref
        # to a buffered Chan must free its buffered values.
        ch = runloom_c.Chan(2)
        v = Node()
        self.assertTrue(ch.try_send(v))
        wr = weakref.ref(v)
        del v, ch
        gc.collect()
        self.assertIsNone(wr(), "buffered value not freed when the Chan was")


if __name__ == "__main__":
    unittest.main()
