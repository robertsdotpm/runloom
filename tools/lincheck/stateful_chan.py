"""Stateful (model-based) Hypothesis testing of runloom channel semantics.

The existing tests/test_chan_properties.py uses only @given (stateless).  This
is a RuleBasedStateMachine: Hypothesis generates random *sequences* over the op
alphabet {send, recv, try_send, try_recv, close, select-recv, select-default}
and checks each step against a reference FIFO queue, shrinking any failure to a
minimal counterexample.  Preconditions keep every op non-blocking (send only when
not full, recv only when non-empty) so each runs as one short goroutine on a
persistent buffered channel -- exercising the real Chan buffer/wraparound/close
paths, the non-blocking try_* would-block detection, and the multi-case select
install/abort path.

Hypothesis's stateful engine requires DETERMINISTIC transitions on replay; select
randomizes its winner among ready cases, so the select rules are preconditioned so
exactly one case is ready (deterministic outcome, install/abort of the other still
covered).  A multi-ready select would trip FlakyStrategyDefinition.

Run as a test:   pytest tools/lincheck/stateful_chan.py
Run standalone:  python tools/lincheck/stateful_chan.py
"""
import os
import sys
from collections import deque

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import runloom_c


def go1(fn):
    """Run one short, non-blocking goroutine to completion."""
    runloom_c.fiber(fn)
    runloom_c.run()


class ChannelStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super(ChannelStateMachine, self).__init__()
        self.cap = 8
        self.ch = runloom_c.Chan(self.cap)
        self.ref = deque()       # reference FIFO model of the buffer
        self.closed = False
        # Second channel, used only to give select() a genuine multi-case
        # choice.  Kept open for the whole run (never closed) to bound the
        # state space; ch is the one that exercises the close paths.
        self.ch2 = runloom_c.Chan(self.cap)
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
    @precondition(lambda self: (
        ((len(self.ref) > 0 or self.closed) and len(self.ref2) == 0)
        or (len(self.ref2) > 0 and len(self.ref) == 0 and not self.closed)))
    def select_recv(self):
        # select() over BOTH channels with EXACTLY ONE case ready.  This keeps the
        # multi-waiter install/abort/cleanup coverage (both cases are installed;
        # the non-ready one's waiter is aborted) -- chan.c's four historical select
        # bugs all lived here -- while making the OUTCOME DETERMINISTIC: select
        # randomizes its winner among ready cases, and a non-deterministic state
        # transition breaks Hypothesis's stateful replay (FlakyStrategyDefinition).
        # We apply the matching pop for whichever case the runtime returns.
        box = []
        go1(lambda: box.append(runloom_c.select([("recv", self.ch), ("recv", self.ch2)])))
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

    # ---- non-blocking ops (the would-block detection paths) ----------------
    @rule(v=st.integers(min_value=0, max_value=10 ** 6))
    @precondition(lambda self: not self.closed)
    def try_send(self, v):
        # try_send never blocks: True iff there is room, False (would-block) when
        # the buffer is full.  Exercises the non-blocking send fast-path.
        box = []
        go1(lambda: box.append(self.ch.try_send(v)))
        ok = box[0]
        if len(self.ref) < self.cap:
            assert ok is True, "try_send should deliver with room ({0}/{1})".format(
                len(self.ref), self.cap)
            self.ref.append(v)
        else:
            assert ok is False, "try_send should would-block (False) when full"

    @rule()
    @precondition(lambda self: not (self.closed and len(self.ref) == 0))
    def try_recv(self):
        # try_recv: (value, ok) on success, None when it would block.  Skip the
        # ambiguous closed+empty case (covered by recv_after_drain).
        box = []
        go1(lambda: box.append(self.ch.try_recv()))
        got = box[0]
        if len(self.ref) > 0:
            assert got is not None, "try_recv should succeed with {0} buffered".format(
                len(self.ref))
            v, ok = got
            assert ok, "try_recv ok=False with buffered data"
            want = self.ref.popleft()
            assert v == want, "FIFO violation try_recv: got {0} want {1}".format(v, want)
        else:  # empty + open -> would-block
            assert got is None, "try_recv on empty+open should be None, got {0}".format(got)

    @rule()
    @precondition(lambda self: len(self.ref) == 0 and len(self.ref2) == 0 and not self.closed)
    def select_default_notready(self):
        # select(default=True) with NOTHING ready returns a bare -1 (the
        # non-blocking default path).  Preconditioned on all-empty-and-open so the
        # outcome is DETERMINISTIC: with no ready case, select can't randomly pick
        # a winner, so stateful replay stays consistent (a multi-ready select
        # randomizes its winner -> non-deterministic transitions Hypothesis's
        # stateful model can't replay; that ready-case coverage is select_recv's).
        box = []
        go1(lambda: box.append(runloom_c.select(
            [("recv", self.ch), ("recv", self.ch2)], default=True)))
        assert box[0] == -1, "select(default) with nothing ready must be -1, got {0}".format(box[0])

    @invariant()
    def runtime_consistent(self):
        assert runloom_c._self_check(0) == 0, "self_check failed"


ChannelStateMachine.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=50,
    # Each step spawns a real goroutine + runloom_c.run(); up to 200*50 = 1e4
    # of them.  Under a loaded gate that trips Hypothesis's per-example deadline
    # and the too_slow HealthCheck -- pure wall-clock flake, not a channel bug.
    # Disable both so only a genuine linearizability counterexample reds step 4.
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestChannelStateMachine = ChannelStateMachine.TestCase


if __name__ == "__main__":
    from hypothesis import seed as hyp_seed  # noqa: F401
    import unittest
    unittest.main(argv=[sys.argv[0], "-v"])
