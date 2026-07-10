"""Stateful (model-based) Hypothesis testing of runloom's sync primitives.

The companion to tools/lincheck/stateful_chan.py (which covers the channel): here
Hypothesis generates random op sequences over a Lock and a weighted Semaphore and
checks each transition against a reference model, shrinking any failure to a
minimal counterexample.  Where stateful_chan drives the buffer/close/select paths,
these drive the lock truth-table (acquire / try-acquire / release / over-release)
and the weighted-permit accounting (acquire(n) / release(n) / try_acquire(n) /
over-release-raises).

Each rule runs its op as one short goroutine on a persistent primitive via go1();
preconditions keep every op's OUTCOME deterministic (acquire only when free, etc.)
so Hypothesis's stateful replay stays consistent -- exactly the discipline
stateful_chan uses.  This is API-semantics coverage (does each op return/raise the
right thing from each state); genuine real-time overlap is the linearizability
battery's job (tools/lincheck/linz/battery.py).

Run as a test:   pytest tools/lincheck/linz/stateful_sync.py
Run standalone:  python tools/lincheck/linz/stateful_sync.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import runloom_c
from runloom import sync as rsync


def go1(fn):
    """Run one short, non-blocking goroutine to completion, return its value."""
    box = []
    runloom_c.fiber(lambda: box.append(fn()))
    runloom_c.run()
    return box[0]


def raised(fn, exc):
    """Run fn in a goroutine; return True iff it raised `exc`."""
    def wrap():
        try:
            fn()
            return False
        except exc:
            return True
    return go1(wrap)


# ------------------------------------------------------------------- Lock

class LockStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super(LockStateMachine, self).__init__()
        self.lock = rsync.Lock()          # == runloom.monkey.CoLock, non-reentrant
        self.held = False

    @rule()
    @precondition(lambda self: not self.held)
    def acquire(self):
        ok = go1(lambda: self.lock.acquire())
        assert ok is True, "acquire() on a free lock must return True"
        self.held = True

    @rule()
    @precondition(lambda self: not self.held)
    def try_acquire_free(self):
        ok = go1(lambda: self.lock.acquire(False))
        assert ok is True, "acquire(blocking=False) on a free lock must be True"
        self.held = True

    @rule()
    @precondition(lambda self: self.held)
    def try_acquire_held(self):
        ok = go1(lambda: self.lock.acquire(False))
        assert ok is False, "acquire(blocking=False) on a held lock must be False"

    @rule()
    @precondition(lambda self: self.held)
    def release(self):
        go1(lambda: self.lock.release())
        self.held = False

    @rule()
    @precondition(lambda self: not self.held)
    def release_unlocked_raises(self):
        assert raised(lambda: self.lock.release(), RuntimeError), \
            "release() on an unlocked lock must raise RuntimeError"

    @invariant()
    def locked_matches_model(self):
        assert self.lock.locked() == self.held, \
            "locked()={0} but model held={1}".format(self.lock.locked(), self.held)
        assert runloom_c._self_check(0) == 0, "self_check failed"


# ------------------------------------------------------------------- Semaphore

CAP = 4


class SemaphoreStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super(SemaphoreStateMachine, self).__init__()
        self.sem = rsync.Semaphore(CAP)   # weighted, goroutine-only
        self.free = CAP                   # reference count of free permits

    @rule(n=st.integers(min_value=1, max_value=CAP))
    @precondition(lambda self: True)
    def acquire(self, n):
        if n <= self.free:
            ok = go1(lambda: self.sem.acquire(n))
            assert ok is True, "acquire({0}) with {1} free must succeed".format(n, self.free)
            self.free -= n
        # (n > free would block -- not exercised here; that is the battery's job)

    @rule(n=st.integers(min_value=1, max_value=CAP))
    def try_acquire(self, n):
        ok = go1(lambda: self.sem.try_acquire(n))
        if n <= self.free:
            assert ok is True, "try_acquire({0}) with {1} free must be True".format(n, self.free)
            self.free -= n
        else:
            assert ok is False, "try_acquire({0}) with {1} free must be False".format(n, self.free)

    @rule(n=st.integers(min_value=1, max_value=CAP))
    @precondition(lambda self: True)
    def release(self, n):
        held = CAP - self.free
        if n <= held:
            go1(lambda: self.sem.release(n))
            self.free += n
        else:
            # releasing more than held over-issues past the bound -> ValueError
            assert raised(lambda: self.sem.release(n), ValueError), \
                "release({0}) past held={1} must raise ValueError".format(n, held)

    @invariant()
    def within_capacity(self):
        assert 0 <= self.free <= CAP, "free={0} out of [0,{1}]".format(self.free, CAP)
        assert runloom_c._self_check(0) == 0, "self_check failed"


STATEFUL_SETTINGS = settings(
    max_examples=150,
    stateful_step_count=40,
    deadline=None,                        # each step spawns a real goroutine+run()
    suppress_health_check=[HealthCheck.too_slow],
)
LockStateMachine.TestCase.settings = STATEFUL_SETTINGS
SemaphoreStateMachine.TestCase.settings = STATEFUL_SETTINGS
TestLockStateMachine = LockStateMachine.TestCase
TestSemaphoreStateMachine = SemaphoreStateMachine.TestCase


if __name__ == "__main__":
    import unittest
    unittest.main(argv=[sys.argv[0], "-v"])
