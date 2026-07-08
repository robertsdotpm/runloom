"""Channel FIFO + exactly-once history checker (QA-steal-V2 #24, Porcupine/Elle).

The big_100 conservation oracles fold an order-INSENSITIVE checksum (XOR/sum), so
they are blind to REORDERING and to a compensating lost+duplicated pair.  Here
each sender goroutine emits frames tagged (sender_gid, seq) into a shared channel;
a SINGLE receiver drains them, giving a total receive order.  A linear pass then
asserts the two linearizability properties a Go-style channel must hold:

  * exactly-once: the multiset of received (gid, seq) equals the multiset sent --
    no lost frame (a dropped wake), no duplicate (a double-wake delivering a value
    twice), no corrupted tag (a torn/cross-delivered frame); and
  * per-sender FIFO: in the receiver's total order, each sender's seqs are
    strictly increasing -- a steal/wake that reordered one sender's frames breaks
    it, invisible to a sum/XOR checksum.

Run buffered and unbuffered, single- and multi-hub, so a cross-hub steal that
reorders or double-delivers is exercised.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import runloom
import runloom_c


class TestChanFifoExactlyOnce(unittest.TestCase):
    def _run(self, nsenders, per_sender, cap, hubs):
        total = nsenders * per_sender
        ch = runloom_c.Chan(cap)
        received = []                    # single receiver -> a real total order

        def sender(gid):
            for seq in range(per_sender):
                ch.send((gid, seq))

        def receiver():
            for _ in range(total):
                received.append(ch.recv()[0])

        def root():
            runloom.fiber(receiver)
            for g in range(nsenders):
                runloom.fiber(lambda g=g: sender(g))

        runloom.run(hubs, main_fn=root)

        # exactly-once: multiset equality with the sent set.
        self.assertEqual(len(received), total,
                         "wrong count: got {0} want {1} (loss or dup)".format(
                             len(received), total))
        sent = {(g, s) for g in range(nsenders) for s in range(per_sender)}
        got = set(received)
        self.assertEqual(len(got), len(received),
                         "duplicate delivery: {0} unique of {1} received".format(
                             len(got), len(received)))
        self.assertEqual(got, sent,
                         "lost/corrupted tag(s): {0} missing, {1} unexpected".format(
                             len(sent - got), len(got - sent)))
        # per-sender FIFO: each sender's seqs strictly increasing in receive order.
        last = {g: -1 for g in range(nsenders)}
        for gid, seq in received:
            self.assertGreater(seq, last[gid],
                               "FIFO violation: sender {0} seq {1} arrived after "
                               "{2} (reordered)".format(gid, seq, last[gid]))
            last[gid] = seq

    def test_buffered_single_hub(self):
        self._run(nsenders=8, per_sender=500, cap=16, hubs=1)

    def test_unbuffered_single_hub(self):
        self._run(nsenders=8, per_sender=500, cap=0, hubs=1)

    def test_buffered_multi_hub(self):
        self._run(nsenders=16, per_sender=500, cap=32, hubs=4)

    def test_unbuffered_multi_hub(self):
        self._run(nsenders=16, per_sender=400, cap=0, hubs=4)


if __name__ == "__main__":
    unittest.main()
