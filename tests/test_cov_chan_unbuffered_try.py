"""Coverage: Chan.try_send / try_recv on an UNBUFFERED (cap-0) channel.

Every existing try_* test (tests/test_chan.py::TestNonBlocking,
tests/test_swarm_chan_select_sync.py Section 2) drives a BUFFERED channel, so
they only exercise the `ch->cap > 0` slot-push / slot-pop branches of
chan_send_locked / chan_recv_locked (chan_ops.c.inc).  A cap-0 channel has NO
buffer: its only non-blocking success path is a DIRECT RENDEZVOUS with an
already-parked peer --

  * try_send  -> waiter_pop_claimable(&ch->receivers): hand the value straight
                 to a parked receiver, wake it, return True.  (chan_ops.c.inc:41)
  * try_recv  -> waiter_pop_claimable(&ch->senders): steal a parked sender's
                 value, wake it (send_result=0), return (v, True). (line 144)

and the peer-less case must be a clean would-block (True->False / (v,True)->None)
WITHOUT buffering anything (cap==0 so the `cap>0 && len<cap` branch is skipped).

Conventions copied from tests/test_chan.py (`_run_in_sched` over runloom_c.fiber
+ runloom_c.run, runloom_c.sched_yield to force a peer to park first) and from
tests/test_swarm_chan_select_sync.py (adv_util.hang_guard so a lost-wake shows as
a timeout with a cooperative-state dump, not a silent wedge).
"""
import sys
import unittest

sys.path.insert(0, "src")

import runloom_c

from adv_util import hang_guard


def _run_in_sched(*fibers):
    """Spawn each callable as a fiber, run the scheduler to completion."""
    for g in fibers:
        runloom_c.fiber(g)
    runloom_c.run()


class TestUnbufferedTrySend(unittest.TestCase):
    def test_try_send_to_parked_receiver_returns_true_and_delivers(self):
        # A receiver parks on the empty cap-0 channel; a later try_send must find
        # it via the receivers queue, hand off the value, wake it, and report True
        # (the non-blocking DIRECT-RENDEZVOUS success path, chan_ops.c.inc:41-48).
        ch = runloom_c.Chan(0)
        out = []

        def receiver():
            v, ok = ch.recv()            # empty + no sender -> parks
            out.append(("recv", v, ok))

        def sender():
            runloom_c.sched_yield()      # let the receiver park first
            out.append(("try", ch.try_send(7)))

        with hang_guard(15, "try_send to parked receiver"):
            _run_in_sched(receiver, sender)

        # try_send saw the parked receiver -> True (not a would-block False),
        # and the value actually arrived at the receiver.
        self.assertIn(("try", True), out)
        self.assertIn(("recv", 7, True), out)
        # ordering: the try_send resolves before the receiver wakes with the value
        self.assertLess(out.index(("try", True)), out.index(("recv", 7, True)))

    def test_try_send_peerless_returns_false_and_buffers_nothing(self):
        # cap-0 + no parked receiver: try_send must NOT block and must NOT invent a
        # buffer slot (the `cap>0 && len<cap` branch is skipped) -> would-block False.
        ch = runloom_c.Chan(0)
        out = []

        def runner():
            out.append(ch.try_send(1))   # no receiver, no buffer -> False
            out.append(len(ch))          # nothing was buffered
            out.append(ch.closed)

        _run_in_sched(runner)
        self.assertEqual(out, [False, 0, False])


class TestUnbufferedTryRecv(unittest.TestCase):
    def test_try_recv_from_parked_sender_returns_value_and_wakes_sender(self):
        # A sender parks on the cap-0 channel (no receiver); try_recv must steal its
        # value, return (v, True), AND wake the sender so its send() completes
        # (send_result=0) rather than raising or hanging (chan_ops.c.inc:144-152).
        ch = runloom_c.Chan(0)
        out = []

        def sender():
            ch.send(9)                   # unbuffered + no receiver -> parks
            out.append("sent")           # only reached if try_recv woke us cleanly

        def receiver():
            runloom_c.sched_yield()      # let the sender park first
            out.append(("try", ch.try_recv()))

        with hang_guard(15, "try_recv from parked sender"):
            _run_in_sched(sender, receiver)

        # try_recv pulled the parked sender's value...
        self.assertIn(("try", (9, True)), out)
        # ...and woke the sender so its blocking send() returned (no raise/hang).
        self.assertIn("sent", out)
        self.assertLess(out.index(("try", (9, True))), out.index("sent"))

    def test_try_recv_peerless_open_returns_none(self):
        # cap-0, open, empty, no parked sender: would-block -> None (NOT the
        # (None, False) closed sentinel -- would-block and closed are distinct).
        ch = runloom_c.Chan(0)
        out = []

        def runner():
            out.append(ch.try_recv())

        _run_in_sched(runner)
        self.assertEqual(out, [None])

    def test_try_send_then_try_recv_peerless_are_independent_noops(self):
        # Symmetric peer-less pair in one fiber: try_send -> False leaves the
        # channel empty, so the following try_recv sees would-block -> None.
        ch = runloom_c.Chan(0)
        out = []

        def runner():
            out.append(ch.try_send(1))   # False (no receiver)
            out.append(ch.try_recv())    # None (nothing buffered by the failed send)

        _run_in_sched(runner)
        self.assertEqual(out, [False, None])


if __name__ == "__main__":
    unittest.main()
