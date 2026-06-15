"""Adversarial QA: runloom.sync primitives (WaitGroup / Future / gather /
Semaphore / RWMutex / Once).

These are pure-Python fan-in/synchronisation primitives built on the
park()/g.wake() handshake + a runloom_c.Mutex guard.  Their two failure modes
are (1) lost/duplicated wakes -> a hang or a wrong count, and (2) a WAKE-side
op running on a FOREIGN OS thread, whose wake path (mn_wake_g) is not
foreign-safe and would SIGSEGV -- every such op must REJECT the foreign caller
with a clean RuntimeError *before* touching the guard.  We probe both, plus
the documented error branches (negative counters, double-resolve, over-release,
timeout), and an M:N bound-concurrency integrity check on Semaphore.
"""
import sys
import time

import pytest

import runloom
import runloom_c as rc
from runloom.sync import WaitGroup, Future, gather, Semaphore, RWMutex, Once
from adv_util import hang_guard, raw_thread, needs_free_threading

FT = needs_free_threading()


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


def _foreign_call(callable_):
    """Run callable_ on a genuine OS thread (no fiber); return its exception type
    name or 'ok'."""
    box = {}
    def body():
        try:
            callable_()
            box["r"] = "ok"
        except BaseException as e:  # noqa: BLE001
            box["r"] = type(e).__name__
    t = raw_thread(body)
    t.join(5)
    return box.get("r", "TIMEOUT")


# --------------------------------------------------------------------------
# WaitGroup
# --------------------------------------------------------------------------
def test_waitgroup_basic_and_reuse():
    def f():
        wg = WaitGroup()
        seen = []
        for cycle in range(2):                 # reusable once back at zero
            wg.add(3)
            for i in range(3):
                rc.go(lambda i=i: (seen.append(i), wg.done()))
            wg.wait()
        return sorted(seen)
    with hang_guard(15, "waitgroup reuse"):
        assert _run_single(f) == [0, 0, 1, 1, 2, 2]


def test_waitgroup_negative_counter_raises():
    def f():
        wg = WaitGroup()
        wg.add(1)
        wg.done()
        with pytest.raises(ValueError):
            wg.done()                          # below zero
        return "ok"
    assert _run_single(f) == "ok"


def test_waitgroup_done_from_foreign_thread_rejected_cleanly():
    # The wake side from a non-goroutine must raise, NOT crash.
    wg = WaitGroup()
    wg.add(1)
    assert _foreign_call(lambda: wg.done()) == "RuntimeError"


# --------------------------------------------------------------------------
# Future
# --------------------------------------------------------------------------
def test_future_set_result_once_and_double_resolve_raises():
    def f():
        fut = Future()
        fut.set_result(42)
        assert fut.done() is True
        assert fut.result() == 42
        with pytest.raises(RuntimeError):
            fut.set_result(1)                  # already resolved
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError("x"))
        return "ok"
    assert _run_single(f) == "ok"


def test_future_set_exception_propagates_to_all_waiters():
    def f():
        fut = Future()
        outcomes = []
        def waiter():
            try:
                fut.result()
                outcomes.append("no-exc")
            except KeyError:
                outcomes.append("keyerror")
        for _ in range(5):
            rc.go(waiter)
        rc.sched_yield()                        # all 5 park on the future
        fut.set_exception(KeyError("boom"))     # type form -> instantiated
        return outcomes
    with hang_guard(15, "future fan-in exception"):
        out = _run_single(f)
    assert out == ["keyerror"] * 5


def test_future_result_timeout_raises_timeouterror():
    def f():
        fut = Future()
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            fut.result(timeout=0.05)
        return time.monotonic() - t0
    with hang_guard(15, "future timeout"):
        el = _run_single(f)
    assert 0.04 < el < 1.0


def test_future_resolve_from_foreign_thread_rejected():
    fut = Future()
    assert _foreign_call(lambda: fut.set_result(1)) == "RuntimeError"


# --------------------------------------------------------------------------
# gather
# --------------------------------------------------------------------------
def test_gather_preserves_order_and_runs_concurrently():
    def f():
        def slow(i):
            runloom.sleep(0.02)
            return i * 10
        return gather(*[(lambda i=i: slow(i)) for i in range(5)])
    with hang_guard(15, "gather order"):
        with_overlap_start = time.monotonic()
        out = _run_single(f)
        el = time.monotonic() - with_overlap_start
    assert out == [0, 10, 20, 30, 40]
    assert el < 0.2, "gather serialised (%.3fs for 5x20ms)" % el


def test_gather_first_exception_by_argument_order():
    def f():
        def ok(): return "ok"
        def bad(tag): raise ValueError(tag)
        with pytest.raises(ValueError) as ei:
            gather(ok, lambda: bad("first"), lambda: bad("second"))
        return str(ei.value)
    assert _run_single(f) == "first"


def test_gather_empty_is_empty_list():
    assert _run_single(lambda: gather()) == []


# --------------------------------------------------------------------------
# Semaphore
# --------------------------------------------------------------------------
def test_semaphore_argument_validation():
    def f():
        with pytest.raises(ValueError):
            Semaphore(-1)
        s = Semaphore(2)
        with pytest.raises(ValueError):
            s.acquire(-1)
        with pytest.raises(ValueError):
            s.acquire(5)                       # exceeds limit
        s.acquire(2)
        with pytest.raises(ValueError):
            s.release(5)                       # more than held
        return "ok"
    assert _run_single(f) == "ok"


def test_semaphore_acquire_timeout_returns_false():
    def f():
        s = Semaphore(1)
        s.acquire(1)                           # exhaust
        t0 = time.monotonic()
        got = s.acquire(1, timeout=0.05)
        return got, time.monotonic() - t0
    with hang_guard(15, "semaphore timeout"):
        got, el = _run_single(f)
    assert got is False
    assert 0.04 < el < 1.0


def test_semaphore_acquire_from_foreign_thread_rejected():
    s = Semaphore(1)
    assert _foreign_call(lambda: s.acquire(1)) == "RuntimeError"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_semaphore_bounds_concurrency_under_mn():
    LIMIT, N = 4, 200
    sem = Semaphore(LIMIT)
    concurrent = bytearray(1)           # peak tracker, guarded below
    peak = [0]
    guard = rc.Mutex()
    cur = [0]

    def main():
        wg = WaitGroup(); wg.add(N)
        def worker():
            try:
                sem.acquire(1)
                guard.lock()
                cur[0] += 1
                if cur[0] > peak[0]:
                    peak[0] = cur[0]
                guard.unlock()
                runloom.sleep(0.001)
                guard.lock(); cur[0] -= 1; guard.unlock()
                sem.release(1)
            finally:
                wg.done()
        for _ in range(N):
            rc.mn_go(worker)
        wg.wait()
    with hang_guard(60, "semaphore mn bound"):
        runloom.run(4, main)
    assert peak[0] <= LIMIT, "semaphore admitted %d > limit %d concurrently" % (peak[0], LIMIT)
    assert peak[0] >= 1


# --------------------------------------------------------------------------
# RWMutex
# --------------------------------------------------------------------------
def test_rwmutex_runlock_not_held_raises():
    def f():
        rw = RWMutex()
        with pytest.raises(RuntimeError):
            rw.runlock()
        return "ok"
    assert _run_single(f) == "ok"


def test_rwmutex_writer_is_exclusive():
    def f():
        rw = RWMutex()
        log = []
        active = [0]
        def reader(i):
            rw.rlock()
            active[0] += 1
            log.append(("r+", active[0]))
            rc.sched_yield()
            active[0] -= 1
            rw.runlock()
        def writer():
            rw.lock()
            log.append(("w", active[0]))       # must see 0 readers active
            rw.unlock()
        for i in range(4):
            rc.go(lambda i=i: reader(i))
        rc.go(writer)
        return log
    with hang_guard(15, "rwmutex exclusivity"):
        log = _run_single(f)
    for entry in log:
        if entry[0] == "w":
            assert entry[1] == 0, "writer ran while %d readers active" % entry[1]


# --------------------------------------------------------------------------
# Once
# --------------------------------------------------------------------------
def test_once_runs_exactly_once_under_concurrency():
    def f():
        once = Once()
        runs = [0]
        def fn():
            runs[0] += 1
        def caller():
            once.do(fn)
        for _ in range(10):
            rc.go(caller)
        return runs
    with hang_guard(15, "once concurrency"):
        runs = _run_single(f)
    assert runs[0] == 1, "Once ran %d times" % runs[0]


def test_once_first_caller_sees_exception_later_callers_do_not():
    def f():
        once = Once()
        outcomes = []
        def fn():
            raise ValueError("only first sees this")
        def caller():
            try:
                once.do(fn)
                outcomes.append("clean")
            except ValueError:
                outcomes.append("raised")
        for _ in range(5):
            rc.go(caller)
        return outcomes
    with hang_guard(15, "once exception"):
        outcomes = _run_single(f)
    # Go semantics: the executor sees the panic, the rest return clean.
    assert outcomes.count("raised") == 1, outcomes
    assert outcomes.count("clean") == 4, outcomes


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
