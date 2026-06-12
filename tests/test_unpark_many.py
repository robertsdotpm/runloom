"""runloom_c.unpark_many + the _unpark_all batched fan-in wake path.

unpark_many wakes a batch of fibers parked in wait_fd DIRECTLY (claim the
parker + re-queue the g, bypassing the per-waiter os.write -> epoll -> drain
round-trip that dominates fan-in cost).  events.py routes Event.set /
Condition.notify_all / Semaphore.release through it (via _unpark_all), with two
safety fallbacks that these tests pin down:

  * EDGE-BEFORE-PARK: a waiter that appended itself but has not yet committed its
    wait_fd park has a NULL parker -> unpark_many reports its index "missed" and
    the caller pipe-writes it instead.
  * FOREIGN-THREAD SETTER: a real OS thread calling set()/notify can't issue a
    race-safe direct wake (it would race run()'s drain-loop exit), so _unpark_all
    falls back to os.write for every waiter.
"""
import os
import threading as _real_threading_preimport
import time  # noqa: F401

import pytest

import runloom
import runloom_c
from runloom import monkey

monkey.patch()
import threading  # noqa: E402  (cooperative after patch)

READ = 1
UNPARKED = 0x10000000


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


# ---- direct C API ---------------------------------------------------------

def test_unpark_many_wakes_all_parked():
    """N fibers parked in wait_fd are all woken by one unpark_many, each
    returning the UNPARKED sentinel, with nothing reported missed."""
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        handles = []
        woke = []

        def waiter(i):
            handles.append((i, runloom_c.current_g()))
            rv = runloom_c.wait_fd(r, READ, 5000)
            woke.append((i, rv))

        n = 300
        for i in range(n):
            runloom.go(waiter, i)
        runloom.sleep(0.15)                    # all parked
        handles.sort()
        missed = runloom_c.unpark_many([h for _, h in handles])
        runloom.sleep(0.15)
        os.close(r); os.close(w)
        return n, len(woke), missed, sorted(set(rv for _, rv in woke))

    n, woke, missed, rvs = _drive(main)
    assert woke == n
    assert missed == []
    assert rvs == [UNPARKED]


def test_unpark_many_reports_unparked_g_as_missed():
    """A handle whose fiber is NOT parked in wait_fd (it is running) cannot
    be direct-woken -> unpark_many returns its index as missed."""
    def main():
        # current_g() of the running main fiber: it is RUNNING, not parked,
        # so its netpoll_parker is NULL -> must be reported missed.
        me = runloom_c.current_g()
        return runloom_c.unpark_many([me])

    missed = _drive(main)
    assert missed == [0], missed


def test_unpark_many_empty_and_nonhandle():
    def main():
        assert runloom_c.unpark_many([]) == []
        with pytest.raises(TypeError):
            runloom_c.unpark_many([object()])
        return True
    assert _drive(main)


# ---- via the cooperative primitives --------------------------------------

def test_event_set_fiber_setter_wakes_all():
    """Event.set() from a GOROUTINE wakes every fiber waiter True (the
    batched direct path)."""
    def main():
        ev = threading.Event()
        out = bytearray(250)

        def waiter(i):
            out[i] = 1 if ev.wait(5.0) else 0

        for i in range(250):
            runloom.go(waiter, i)
        runloom.sleep(0.15)
        ev.set()
        runloom.sleep(0.2)
        return sum(out)
    assert _drive(main) == 250


def test_event_set_foreign_thread_setter_wakes_all():
    """Event.set() from a REAL worker thread wakes fiber waiters (the
    foreign-setter os.write fallback -- a direct wake here would race run()'s
    exit and be lost; this is the test_join_cooperative failure, generalized)."""
    def main():
        ev = threading.Event()
        out = bytearray(50)

        def waiter(i):
            out[i] = 1 if ev.wait(5.0) else 0

        for i in range(50):
            runloom.go(waiter, i)
        runloom.sleep(0.15)
        # set() from a real OS thread (foreign): must still wake every waiter.
        t = _real_threading_preimport.Thread(target=ev.set)
        t.start()
        t.join()
        runloom.sleep(0.25)
        return sum(out)
    assert _drive(main) == 50


def test_event_mixed_fiber_and_foreign_waiters():
    """One Event with BOTH fiber waiters (direct-woken) and a foreign-thread
    waiter (os.write-woken): set() wakes them all."""
    box = {"foreign": None, "gor": None}

    def main():
        ev = threading.Event()
        gout = bytearray(40)

        def gwaiter(i):
            gout[i] = 1 if ev.wait(5.0) else 0

        for i in range(40):
            runloom.go(gwaiter, i)

        fresult = []
        # A real OS thread also waits on the same Event (foreign waiter ->
        # _g_handle is None -> os.write path inside _unpark_all).
        def fwaiter():
            fresult.append(ev.wait(5.0))
        ft = _real_threading_preimport.Thread(target=fwaiter)
        ft.start()

        runloom.sleep(0.2)
        ev.set()                                # fiber setter: batch + write
        runloom.sleep(0.2)
        ft.join()
        box["gor"] = sum(gout)
        box["foreign"] = fresult

    _drive(main)
    assert box["gor"] == 40
    assert box["foreign"] == [True]


def test_condition_notify_all_batched():
    def main():
        cond = threading.Condition()
        out = bytearray(120)

        def w(i):
            with cond:
                got = cond.wait(5.0)
            out[i] = 1 if got else 0

        for i in range(120):
            runloom.go(w, i)
        runloom.sleep(0.2)
        with cond:
            cond.notify_all()
        runloom.sleep(0.2)
        return sum(out)
    assert _drive(main) == 120


def test_semaphore_release_n_batched():
    """release(k) hands exactly k permits; the woken acquirers are batched."""
    def main():
        sem = threading.Semaphore(0)
        got = bytearray(60)

        def acq(i):
            got[i] = 1 if sem.acquire(timeout=5.0) else 0

        for i in range(60):
            runloom.go(acq, i)
        runloom.sleep(0.2)
        sem.release(60)                         # one batched wake of all 60
        runloom.sleep(0.2)
        return sum(got)
    assert _drive(main) == 60


def test_repeated_fanin_no_lost_or_double_wake():
    """Repeated high fan-in set()/clear cycles: every cycle must wake all waiters
    exactly (no lost wake -> count stays N; no spurious False; no double-resume
    -> no crash)."""
    def main():
        total = 0
        for _ in range(8):
            ev = threading.Event()
            out = bytearray(200)

            def waiter(i, ev=ev, out=out):
                out[i] = 1 if ev.wait(5.0) else 0

            for i in range(200):
                runloom.go(waiter, i)
            runloom.sleep(0.1)
            ev.set()
            runloom.sleep(0.12)
            total += sum(out)
        return total
    assert _drive(main) == 8 * 200
