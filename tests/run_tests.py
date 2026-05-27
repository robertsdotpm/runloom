"""pygo plain-script test driver.

We don't use unittest here.  CPython tracks frame chain + recursion
counters in thread-state; the legacy `pygo.runtime` Python scheduler
only swaps recursion counters (the C scheduler in `pygo_core.go`
does the full Phase B snap).  Multi-goroutine tests therefore go
through `pygo_core.*` directly so the production path is exercised.

The two single-coro Coro tests still use `pygo_core.Coro` + the raw
`pygo_core.yield_` since those primitives are the building blocks
under both schedulers.
"""
import sys
import time
import traceback

sys.path.insert(0, "src")

import pygo_core


def eq(actual, expected, name):
    if actual != expected:
        raise AssertionError("{0}:\n  got      {1!r}\n  expected {2!r}".format(
            name, actual, expected))


# ── Test 1: backend identifies itself ──────────────────────────────
def test_backend_name():
    b = pygo_core.backend()
    assert b in ("ucontext", "fibers", "fcontext-asm"), "unexpected backend: " + b


# ── Test 2: yield/resume chain on raw Coro ─────────────────────────
def test_yield_resume_chain():
    log = []
    def child():
        log.append("a")
        pygo_core.yield_()
        log.append("b")
        pygo_core.yield_()
        log.append("c")
        return "done"
    c = pygo_core.Coro(child)
    c.resume(); eq(log, ["a"], "after resume 1")
    c.resume(); eq(log, ["a", "b"], "after resume 2")
    c.resume(); eq(log, ["a", "b", "c"], "after resume 3")
    assert c.done
    eq(c.result, "done", "result")


# ── Test 3: exception propagates ───────────────────────────────────
def test_exception_propagates():
    def child():
        raise ValueError("boom")
    c = pygo_core.Coro(child)
    try:
        c.resume()
    except ValueError as e:
        assert str(e) == "boom"
    else:
        raise AssertionError("expected ValueError")
    assert c.done


# ── Test 4: three goroutines interleave round-robin ────────────────
def test_three_goroutines_interleave():
    log = []
    def make_worker(name, n):
        def w():
            for i in range(n):
                log.append((name, i))
                pygo_core.sched_yield()
        return w
    pygo_core.go(make_worker("A", 3))
    pygo_core.go(make_worker("B", 3))
    pygo_core.go(make_worker("C", 3))
    pygo_core.run()
    expected = [("A", 0), ("B", 0), ("C", 0),
                ("A", 1), ("B", 1), ("C", 1),
                ("A", 2), ("B", 2), ("C", 2)]
    eq(log, expected, "round-robin order")


# ── Test 5: sleep yields the OS thread ─────────────────────────────
def test_sleep_lets_others_run():
    log = []
    def sleeper():
        log.append("s1-start")
        pygo_core.sched_sleep(0.05)
        log.append("s1-end")
    def burner():
        for i in range(3):
            log.append(("b", i))
            pygo_core.sched_yield()
    pygo_core.go(sleeper)
    pygo_core.go(burner)
    t0 = time.monotonic()
    pygo_core.run()
    elapsed = time.monotonic() - t0
    eq(log[:4], ["s1-start", ("b", 0), ("b", 1), ("b", 2)], "early order")
    eq(log[-1], "s1-end", "sleeper finished last")
    assert elapsed >= 0.04, "elapsed too short: " + str(elapsed)


# ── Driver ─────────────────────────────────────────────────────────
def main():
    tests = [
        test_backend_name,
        test_yield_resume_chain,
        test_exception_propagates,
        test_three_goroutines_interleave,
        test_sleep_lets_others_run,
    ]
    failed = 0
    for t in tests:
        name = t.__name__
        try:
            t()
            print("  PASS  {0}".format(name))
        except Exception:
            failed += 1
            print("  FAIL  {0}".format(name))
            traceback.print_exc()
    print()
    print("{0} passed / {1} failed".format(len(tests) - failed, failed))
    return failed


if __name__ == "__main__":
    sys.exit(main())
