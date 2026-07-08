"""Introspection *predicates* -- the decidable runtime queries, not the fiber
registry dump.  Three gaps the existing introspect suites don't pin:

  1. enable_migration() SAFETY CONTRACT on the shipped (unpatched) build.
     migration_available() is False without the alloc-home CPython patch, so
     enable_migration() must RAISE RuntimeError *and must not leave the process
     armed* -- i.e. it must NOT set os.environ['RUNLOOM_MIGRATION'] on the way
     out (a half-applied flag would silently enable the crash-prone per-g-tstate
     path at the next run()).  Mirrors src/patches/README.md's stated contract.

  2. _quiescent() NEGATIVE predicate.  A busy ping-pong over UNBUFFERED channels
     always has one fiber runnable (the other parked on the rendezvous), so an
     observer-thread sample must catch quiescent==False with inflight>0.  If it
     ever read quiescent==True mid-ping-pong the predicate would be broken (it
     would mask a lost wake).

  3. _quiescent() SETTLED predicate.  N fibers that all sched_sleep() park on a
     timer with nothing runnable -> the runtime genuinely settles: a daemon
     observer must see quiescent==True with live==parked==N (Go synctest /
     Tokio pause).  Plus the idle baseline outside run() is the trivially-settled
     {quiescent:True, live:0, parked:0, inflight:0}.

Model: tests/test_introspect.py, tests/test_hub_introspect.py,
tools/verify/quiescence_check.py.  _quiescent() must be sampled from an OBSERVER
OS thread -- the caller's own RUNNING state would otherwise keep inflight>0
(runloom_introspect.c:m_quiescent docstring).  The observer uses the stdlib
threading module directly: these tests never monkey.patch(), so Thread/Event are
the genuine OS primitives.
"""
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, "src")

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading


# --------------------------------------------------------------------------
# Gap 1 -- enable_migration() safety contract on the shipped build
# --------------------------------------------------------------------------
class TestEnableMigrationRefusesOnShippedBuild:
    """On a build WITHOUT the alloc-home patch, enable_migration() must refuse
    loudly and leave no residue in the environment."""

    def setup_method(self):
        # Snapshot the flag so a refusal (or a bug that sets it) can't leak
        # into any other test in the process.
        self.had = "RUNLOOM_MIGRATION" in os.environ
        self.prev = os.environ.get("RUNLOOM_MIGRATION")
        os.environ.pop("RUNLOOM_MIGRATION", None)

    def teardown_method(self):
        os.environ.pop("RUNLOOM_MIGRATION", None)
        if self.had:
            os.environ["RUNLOOM_MIGRATION"] = self.prev

    @pytest.mark.skipif(
        runloom.migration_available(),
        reason="alloc-home-patched build: enable_migration() is allowed to "
               "succeed here, so the 'must refuse' contract doesn't apply")
    def test_raises_and_leaves_env_unset(self):
        # Precondition: this is the shipped build (no patch).
        assert runloom.migration_available() is False
        assert "RUNLOOM_MIGRATION" not in os.environ

        with pytest.raises(RuntimeError) as ei:
            runloom.enable_migration()          # allow_unsafe defaults to False

        # The refusal must name the alloc-home patch (the actionable remedy).
        assert "alloc-home" in str(ei.value)

        # THE contract: a refused call must NOT arm the flag.  A half-applied
        # RUNLOOM_MIGRATION=1 would enable the unsafe per-g-tstate path at the
        # next runtime start.
        assert "RUNLOOM_MIGRATION" not in os.environ
        # ...and the higher-level view agrees nothing was requested.
        assert runloom.migration_enabled() is False


# --------------------------------------------------------------------------
# Gap 2 -- _quiescent() NEGATIVE: busy ping-pong is never "settled"
# --------------------------------------------------------------------------
@pytest.mark.skipif(not needs_free_threading(),
                    reason="observer-thread sampling of _quiescent() needs the "
                           "GIL disabled to run in parallel with the scheduler")
def test_quiescent_false_during_unbuffered_ping_pong():
    from runloom.sync import WaitGroup

    ROUNDS = 200000                     # ~0.8s of continuous rendezvous work
    ping = rc.Chan(0)                   # unbuffered: send blocks for a receiver
    pong = rc.Chan(0)

    running = threading.Event()
    cap = {"busy": None, "samples": 0}

    def observer():
        running.wait()
        while running.is_set():
            q = rc._quiescent()
            cap["samples"] += 1
            if (not q["quiescent"]) and q["inflight"] > 0 and cap["busy"] is None:
                cap["busy"] = dict(q)   # a genuine "work in flight" reading

    def main():
        wg = WaitGroup(); wg.add(2)

        def a():
            try:
                for i in range(ROUNDS):
                    ping.send(i)
                    pong.recv()
            finally:
                wg.done()

        def b():
            try:
                for i in range(ROUNDS):
                    ping.recv()
                    pong.send(i)
            finally:
                wg.done()

        runloom.fiber(a)
        runloom.fiber(b)
        wg.wait()

    t = threading.Thread(target=observer, name="q-observer", daemon=True)
    t.start()
    running.set()
    with hang_guard(60, "unbuffered ping-pong quiescent-false"):
        runloom.run(1, main)
    running.clear()
    t.join(5)

    assert cap["samples"] > 0, "observer never sampled _quiescent()"
    assert cap["busy"] is not None, (
        "no non-quiescent sample during a busy unbuffered ping-pong "
        "(%d samples) -- _quiescent() would be masking runnable work"
        % cap["samples"])
    assert cap["busy"]["quiescent"] is False
    assert cap["busy"]["inflight"] > 0


# --------------------------------------------------------------------------
# Gap 3 -- _quiescent() SETTLED: N sleepers all park -> quiescent, live==parked==N
# --------------------------------------------------------------------------
@pytest.mark.skipif(not needs_free_threading(),
                    reason="observer-thread sampling of _quiescent() needs the "
                           "GIL disabled to run in parallel with the scheduler")
def test_quiescent_settled_when_all_fibers_sleep():
    N = 20
    NAP = 0.30                          # wide window for the observer to land in

    running = threading.Event()
    cap = {"settled": None, "samples": 0, "max_live": 0}

    def observer():
        running.wait()
        while running.is_set():
            q = rc._quiescent()
            cap["samples"] += 1
            cap["max_live"] = max(cap["max_live"], q["live"])
            if (q["quiescent"] and q["live"] == N
                    and q["parked"] == N and q["inflight"] == 0):
                cap["settled"] = dict(q)

    def main():
        # Spawn N pure timer-parkers, then return.  run() keeps the runtime
        # alive until they all wake, so there is a long all-parked window in
        # which only the N sleepers are live (main itself is DONE).
        for _ in range(N):
            runloom.fiber(lambda: rc.sched_sleep(NAP))

    # Idle baseline: no runtime -> trivially settled, nothing live.
    base = rc._quiescent()
    assert base == {"quiescent": True, "live": 0, "parked": 0, "inflight": 0}, base

    t = threading.Thread(target=observer, name="q-observer", daemon=True)
    t.start()
    running.set()
    with hang_guard(30, "all-sleeping quiescent settle"):
        runloom.run(1, main)
    running.clear()
    t.join(5)

    assert cap["samples"] > 0, "observer never sampled _quiescent()"
    assert cap["max_live"] >= N, (
        "observer never saw the N sleepers live (max_live=%d, want >=%d)"
        % (cap["max_live"], N))
    assert cap["settled"] is not None, (
        "never observed a settled sample with live==parked==%d "
        "(samples=%d, max_live=%d) -- the runtime never reported quiescent "
        "while every fiber was timer-parked" % (N, cap["samples"], cap["max_live"]))
    assert cap["settled"]["quiescent"] is True
    assert cap["settled"]["live"] == N
    assert cap["settled"]["parked"] == N
    assert cap["settled"]["inflight"] == 0

    # Fully drained afterwards -> back to the trivially-settled idle baseline.
    after = rc._quiescent()
    assert after == {"quiescent": True, "live": 0, "parked": 0, "inflight": 0}, after


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
