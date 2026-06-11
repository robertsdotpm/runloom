"""Timed waits on Event / Condition / Semaphore must time out and wake
correctly WITHOUT spawning a per-wait waker goroutine.

Before: each timed Event.wait/Condition.wait/Semaphore.acquire spawned a helper
goroutine that slept to the deadline then unparked the waiter.  Now _Parker.park
passes the deadline straight to wait_fd (which wakes on the unpark byte OR at the
timeout), so a timed wait costs no extra goroutine + heap timer.

NOTE: kept modest-scale on purpose -- a SEPARATE, pre-existing spurious-wake race
in the _Parker primitives shows up only under repeated high-fan-in set()/notify()
(see /tmp/t3_iso.py-style stress; present in the pre-timeout-change code too).
That is out of scope here; these tests exercise the timeout path's correctness.
"""
import sys
import time

import pytest

import runloom
import runloom_c
from runloom import monkey

monkey.patch()
import threading  # noqa: E402  (after patch -> cooperative primitives)


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:  # noqa: BLE001
            box[1] = e

    runloom_c.go(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def test_event_timeout_then_wake():
    def body():
        ev = threading.Event()
        t0 = time.monotonic()
        r = ev.wait(0.15)
        dt = time.monotonic() - t0
        assert r is False and 0.12 < dt < 0.6, (r, dt)
        out = []
        runloom.go(lambda: out.append(ev.wait(2.0)))
        runloom.sleep(0.03)
        ev.set()
        runloom.sleep(0.05)
        assert out == [True], out
        return True
    assert _drive(body)


def test_condition_timeout_then_wake():
    def body():
        cond = threading.Condition()
        with cond:
            t0 = time.monotonic()
            r = cond.wait(0.15)
        dt = time.monotonic() - t0
        assert r is False and 0.12 < dt < 0.6, (r, dt)
        out = []

        def w():
            with cond:
                out.append(cond.wait(2.0))
        runloom.go(w)
        runloom.sleep(0.03)
        with cond:
            cond.notify()
        runloom.sleep(0.05)
        assert out == [True], out
        return True
    assert _drive(body)


def test_semaphore_timeout_then_wake():
    def body():
        sem = threading.Semaphore(0)
        t0 = time.monotonic()
        r = sem.acquire(timeout=0.15)
        dt = time.monotonic() - t0
        assert r is False and 0.12 < dt < 0.6, (r, dt)
        out = []
        runloom.go(lambda: out.append(sem.acquire(timeout=2.0)))
        runloom.sleep(0.03)
        sem.release()
        runloom.sleep(0.05)
        assert out == [True], out
        return True
    assert _drive(body)


def test_timed_waits_spawn_no_waker_goroutines():
    """N concurrent timed waits must not balloon the live-goroutine count with
    one waker goroutine each (the old design spawned ~N extra)."""
    def body():
        ev = threading.Event()
        base = runloom_c.live_goroutines()
        n = 200
        for _ in range(n):
            runloom.go(lambda: ev.wait(5.0))
        runloom.sleep(0.2)            # all parked on their timed wait
        peak = runloom_c.live_goroutines()
        ev.set()
        runloom.sleep(0.2)
        # Old: ~n waiters + ~n wakers.  New: ~n.  Allow slack but well under 2n.
        assert peak - base < n * 1.5, (base, peak, n)
        return True
    assert _drive(body)
