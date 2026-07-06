"""Tests for the liveness auditor + stall watchdog (item 5).

Covers the pure blame logic with synthetic snapshots (deterministic teeth), the
watchdog firing on a simulated stall, and -- the important negative -- that a
healthy running workload is never falsely flagged.
"""
import os
import sys
import time

sys.path.insert(0, "src")
sys.path.insert(0, "tools/introspect")

import runloom_c
import liveness


# ---- pure blame logic: synthetic snapshots (teeth) -------------------------

def _f(id, state, blocked_on="runnable", fd=None, wake_in=None):
    return {"id": id, "state": state, "blocked_on": blocked_on, "refcount": 1,
            "noyield": False, "owner": 0, "fd": fd, "events": None,
            "wake_in": wake_in, "age": None}


def test_hard_deadlock_is_flagged():
    # two fibers both parked on chan, none runnable, no timers, no hub pending,
    # no I/O waiters -> a hard cooperative deadlock.
    snap = {"fibers": [_f(1, "chan-wait", "chan"), _f(2, "chan-wait", "chan")],
            "hubs": [{"id": 0, "pending": 0}]}
    b = liveness.deadlock_blame(snap)
    assert b is not None and b["verdict"] == "HARD-DEADLOCK", b
    assert len(b["parked"]) == 2


def test_runnable_present_is_not_deadlock():
    snap = {"fibers": [_f(1, "chan-wait", "chan"), _f(2, "runnable")],
            "hubs": []}
    assert liveness.deadlock_blame(snap) is None


def test_timer_waiter_is_not_deadlock():
    # a parked fiber with a pending timer will be woken -> not a deadlock.
    snap = {"fibers": [_f(1, "sleep", "timer", wake_in=0.5)], "hubs": []}
    assert liveness.deadlock_blame(snap) is None


def test_io_waiter_is_not_a_proven_deadlock():
    # a fiber parked on an fd may still be woken by the kernel -> the snapshot
    # alone cannot prove a deadlock (that is the watchdog's job).
    snap = {"fibers": [_f(1, "netpoll", "io", fd=7)], "hubs": []}
    assert liveness.deadlock_blame(snap) is None


def test_pending_hub_work_is_not_deadlock():
    snap = {"fibers": [_f(1, "chan-wait", "chan")],
            "hubs": [{"id": 0, "pending": 1}]}
    assert liveness.deadlock_blame(snap) is None


# ---- watchdog fires on a simulated stall -----------------------------------

def test_watchdog_fires_on_stall(monkeypatch):
    # freeze progress and present a parked snapshot -> the watchdog must fire
    # on_stall with the deadlock blame.
    parked = {"fibers": [_f(1, "chan-wait", "chan"), _f(2, "chan-wait", "chan")],
              "hubs": [{"id": 0, "pending": 0}]}
    monkeypatch.setattr(liveness, "snapshot", lambda: parked)
    fired = {}

    def on_stall(snap, blame):
        fired["snap"] = snap
        fired["blame"] = blame

    wd = liveness.StallWatchdog(progress=lambda: 0, timeout=0.4, poll=0.05,
                                on_stall=on_stall)
    wd.start()
    deadline = time.monotonic() + 5
    while not fired and time.monotonic() < deadline:
        time.sleep(0.05)
    wd.stop()
    assert fired, "watchdog did not fire on a frozen-progress parked snapshot"
    assert fired["blame"] and fired["blame"]["verdict"] == "HARD-DEADLOCK"


def test_watchdog_silent_while_progressing(monkeypatch):
    # progress keeps advancing -> the watchdog must NOT fire even though a fiber
    # is parked (a normal busy server has parked I/O fibers all the time).
    parked = {"fibers": [_f(1, "netpoll", "io", fd=7)], "hubs": [{"id": 0, "pending": 0}]}
    monkeypatch.setattr(liveness, "snapshot", lambda: parked)
    counter = {"n": 0}
    fired = {}
    wd = liveness.StallWatchdog(progress=lambda: counter["n"], timeout=0.4,
                                poll=0.05, on_stall=lambda s, b: fired.setdefault("x", 1))
    wd.start()
    for _ in range(20):
        counter["n"] += 1
        time.sleep(0.05)
    wd.stop()
    assert not fired, "watchdog false-fired while progress was advancing"


# ---- integration: no false positive on a healthy workload ------------------

def test_no_false_positive_on_healthy_run():
    # a chan ping-pong workload snapshotted repeatedly from a monitor fiber must
    # never register a hard deadlock while it is making progress.
    violations = []

    def body():
        ch = runloom_c.Chan(1)
        state = {"n": 0, "stop": False}

        def producer():
            for i in range(200):
                ch.send(i); state["n"] += 1
            state["stop"] = True

        def consumer():
            while not state["stop"] or True:
                v, ok = ch.recv()
                if not ok:
                    break

        def monitor():
            for _ in range(30):
                snap = liveness.snapshot()
                b = liveness.deadlock_blame(snap)
                if b is not None:
                    violations.append(b)
                runloom_c.sched_yield()

        runloom_c.fiber(producer)
        runloom_c.fiber(consumer)
        runloom_c.fiber(monitor)
        # let producer finish, then close so consumer exits cleanly
        while not state["stop"]:
            runloom_c.sched_yield()
        ch.close()

    runloom_c.fiber(body)
    runloom_c.run()
    assert not violations, "false-positive deadlock verdict on a healthy run: %r" % violations[:2]


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
