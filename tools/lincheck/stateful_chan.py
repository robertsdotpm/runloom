"""Stateful (model-based) Hypothesis testing of pygo channel semantics.

The existing tests/test_chan_properties.py uses only @given (stateless).  This
is a RuleBasedStateMachine: Hypothesis generates random *sequences* of
send/recv/close/select operations and checks each step against a reference
FIFO queue, shrinking any failure to a minimal counterexample.  Preconditions
keep every single op non-blocking (send only when not full, recv only when
non-empty, select only when some case is ready), so each op runs as one short
goroutine on a persistent buffered channel -- exercising the real Chan
buffer/wraparound/close paths and the multi-case select install/abort path.

Run as a test:   pytest tools/lincheck/stateful_chan.py
Run standalone:  python tools/lincheck/stateful_chan.py
"""
import os
import sys
from collections import deque

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import pygo_core


def go1(fn):
    """Run one short, non-blocking goroutine to completion."""
    pygo_core.go(fn)
    pygo_core.run()


class ChannelStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super(ChannelStateMachine, self).__init__()
        self.cap = 8
        self.ch = pygo_core.Chan(self.cap)
        self.ref = deque()       # reference FIFO model of the buffer
        self.closed = False
        # Second channel, used only to give select() a genuine multi-case
        # choice.  Kept open for the whole run (never closed) to bound the
        # state space; ch is the one that exercises the close paths.
        self.ch2 = pygo_core.Chan(self.cap)
        self.ref2 = deque()

    @rule(v=st.integers(min_value=0, max_value=10 ** 6))
    @precondition(lambda self: not self.closed and len(self.ref) < self.cap)
    def send(self, v):
        go1(lambda: self.ch.send(v))
        self.ref.append(v)

    @rule(v=st.integers(min_value=10 ** 6 + 1, max_value=2 * 10 ** 6))
    @precondition(lambda self: len(self.ref2) < self.cap)
    def send2(self, v):
        # Disjoint value range from ch so a select result is unambiguous about
        # which channel actually delivered it.
        go1(lambda: self.ch2.send(v))
        self.ref2.append(v)

    @rule()
    @precondition(lambda self: len(self.ref) > 0 or self.closed or len(self.ref2) > 0)
    def select_recv(self):
        # select() over BOTH channels.  Precondition guarantees at least one
        # case is ready, so the single goroutine never blocks.  The runtime is
        # free to pick ANY ready case; we accept whichever it returns as long
        # as it's consistent with the model, then apply the matching pop.  This
        # is the multi-waiter install/abort/cleanup path -- chan.c's four
        # historical select bugs all lived here.
        box = []
        go1(lambda: box.append(pygo_core.select([("recv", self.ch), ("recv", self.ch2)])))
        idx, res = box[0]
        v, ok = res
        assert idx in (0, 1), "select returned bad index {0}".format(idx)
        if idx == 0:
            if ok:
                assert len(self.ref) > 0, "select fired ch recv but model is empty"
                want = self.ref.popleft()
                assert v == want, "FIFO violation on ch: got {0} want {1}".format(v, want)
            else:
                assert self.closed and len(self.ref) == 0, \
                    "select returned ch-closed but ch not closed/drained"
        else:  # idx == 1: ch2 is never closed, so it must be a real value
            assert ok, "select returned ch2 not-ok but ch2 is never closed"
            assert len(self.ref2) > 0, "select fired ch2 recv but model is empty"
            want = self.ref2.popleft()
            assert v == want, "FIFO violation on ch2: got {0} want {1}".format(v, want)

    @rule()
    @precondition(lambda self: len(self.ref) > 0)
    def recv(self):
        box = []
        go1(lambda: box.append(self.ch.recv()))
        v, ok = box[0]
        assert ok, "recv returned not-ok with {0} buffered".format(len(self.ref))
        want = self.ref.popleft()
        assert v == want, "FIFO violation: got {0} want {1}".format(v, want)

    @rule()
    @precondition(lambda self: not self.closed)
    def close(self):
        self.ch.close()
        self.closed = True

    @rule()
    @precondition(lambda self: self.closed and len(self.ref) == 0)
    def recv_after_drain(self):
        box = []
        go1(lambda: box.append(self.ch.recv()))
        v, ok = box[0]
        assert not ok, "recv after close+drain must be (None, False), got {0}".format((v, ok))

    @invariant()
    def runtime_consistent(self):
        assert pygo_core._self_check(0) == 0, "self_check failed"


ChannelStateMachine.TestCase.settings = settings(max_examples=200, stateful_step_count=50)
TestChannelStateMachine = ChannelStateMachine.TestCase


if __name__ == "__main__":
    from hypothesis import seed as hyp_seed  # noqa: F401
    import unittest
    unittest.main(argv=[sys.argv[0], "-v"])
