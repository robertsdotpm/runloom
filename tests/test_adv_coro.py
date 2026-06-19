"""Adversarial QA: the raw Coro primitive + introspection under churn.

Coro is the bare stackful-coroutine building block (resume / done / result).  We
attack its argument validation (a negative stack_size segfaulted before the
guard added in this branch), its resume-after-done / yield-outside-coro edges,
and we hammer the introspection surface (fibers / stats / _self_check /
fiber_stack / dump_fibers) WHILE fibers churn -- it must stay crash-free and
self-consistent.
"""
import os
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
_DEVNULL = os.open(os.devnull, os.O_WRONLY)


# --------------------------------------------------------------------------
# Coro primitive
# --------------------------------------------------------------------------
def test_coro_resume_yield_chain():
    log = []
    def body():
        log.append("a"); rc.yield_()
        log.append("b"); rc.yield_()
        log.append("c"); return "done"
    c = rc.Coro(body)
    c.resume(); assert log == ["a"] and not c.done
    c.resume(); assert log == ["a", "b"] and not c.done
    c.resume(); assert log == ["a", "b", "c"] and c.done
    assert c.result == "done"


def test_coro_exception_propagates_on_resume():
    c = rc.Coro(lambda: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(ValueError):
        c.resume()
    assert c.done


def test_coro_resume_after_done_is_idempotent():
    c = rc.Coro(lambda: 42)
    c.resume()
    assert c.done and c.result == 42
    c.resume()                       # resuming a finished coro must not crash
    assert c.done and c.result == 42


def test_yield_outside_coro_is_noop():
    rc.yield_()                      # must be a harmless no-op, not a crash


def test_coro_tiny_positive_stack_runs():
    c = rc.Coro(lambda: sum(range(100)), 4096)
    c.resume()
    assert c.done and c.result == 4950


def test_coro_negative_stack_raises_not_segfaults():
    # Regression: Coro(fn, -1) cast the negative size to ~SIZE_MAX, overflowed
    # the guard-page arithmetic to an undersized mapping, and SIGSEGV'd.  It now
    # validates the argument like fiber() / set_stack_size().
    with pytest.raises(ValueError):
        rc.Coro(lambda: 1, -1)
    with pytest.raises(ValueError):
        rc.Coro(lambda: 1, -1 << 40)


def test_coro_absurd_stack_raises_memoryerror_not_crash():
    with pytest.raises((MemoryError, OverflowError)):
        c = rc.Coro(lambda: 1, (1 << 62))
        c.resume()


# --------------------------------------------------------------------------
# introspection under churn -- must not crash, must stay consistent
# --------------------------------------------------------------------------
def test_introspection_outside_fiber_is_safe():
    assert rc.fiber_count() >= 0
    assert isinstance(rc.stats(), dict)
    assert rc.current_g() is None
    assert rc._self_check(0) == 0
    rc.dump_fibers(_DEVNULL)          # async-signal-safe dump, no Python objects


def test_introspection_during_single_thread_churn():
    snap = {}
    def main():
        ch = rc.Chan(0)
        # spawn workers that PARK on a channel (live + parked state)
        for _ in range(40):
            rc.fiber(lambda: ch.recv())
        rc.sched_yield()              # let them park
        snap["count"] = rc.fiber_count()
        snap["fibers_len"] = len(rc.fibers())
        snap["self_check"] = rc._self_check(0)
        # fiber_stack on a real live fiber id must not crash
        fl = rc.fibers()
        if fl:
            snap["stack_ok"] = rc.fiber_stack(fl[0]["id"]) is not None
        rc.dump_fibers(_DEVNULL)
        ch.close()                    # release the parked workers (clean teardown)
    with hang_guard(20, "introspect churn"):
        rc.fiber(main); rc.run()
    assert snap.get("count", 0) >= 40
    assert snap.get("fibers_len", 0) >= 40
    assert snap.get("self_check") == 0
    assert snap.get("stack_ok") is True


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_introspection_during_mn_churn():
    # Poll the introspection surface from inside hubs while many gs run/park.
    from runloom.sync import WaitGroup
    errors = []
    def main():
        wg = WaitGroup(); wg.add(200)
        def worker():
            try:
                runloom.sleep(0.001)
            finally:
                wg.done()
        for _ in range(200):
            rc.mn_go(worker)
        # hammer introspection while they churn
        for _ in range(50):
            try:
                rc.fibers(); rc.stats(); rc.mn_hub_states()
                if rc._self_check(0) != 0:
                    errors.append("self_check")
            except BaseException as e:  # noqa: BLE001
                errors.append(type(e).__name__)
            rc.sched_yield()
        wg.wait()
    with hang_guard(60, "mn introspect churn"):
        runloom.run(4, main)
    assert not errors, "introspection raced under M:N churn: %r" % errors


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
