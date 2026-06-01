"""Stateful (model-based) Hypothesis testing of pygo channel semantics.

The existing tests/test_chan_properties.py uses only @given (stateless).  This
is a RuleBasedStateMachine: Hypothesis generates random *sequences* of
send/recv/close operations and checks each step against a reference FIFO
queue, shrinking any failure to a minimal counterexample.  Preconditions keep
every single op non-blocking (send only when not full, recv only when
non-empty), so each op runs as one short goroutine on a persistent buffered
channel -- exercising the real Chan buffer/wraparound/close paths.

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

    @rule(v=st.integers(min_value=0, max_value=10 ** 6))
    @precondition(lambda self: not self.closed and len(self.ref) < self.cap)
    def send(self, v):
        go1(lambda: self.ch.send(v))
        self.ref.append(v)

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
