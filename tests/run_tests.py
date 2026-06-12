"""runloom plain-script test driver.

We don't use unittest here.  CPython tracks frame chain + recursion
counters in thread-state; the legacy `runloom.runtime` Python scheduler
only swaps recursion counters (the C scheduler in `runloom_c.go`
does the full Phase B snap).  Multi-fiber tests therefore go
through `runloom_c.*` directly so the production path is exercised.

The two single-coro Coro tests still use `runloom_c.Coro` + the raw
`runloom_c.yield_` since those primitives are the building blocks
under both schedulers.
"""
import sys
import time
import traceback

sys.path.insert(0, "src")

import runloom_c


def eq(actual, expected, name):
    if actual != expected:
        raise AssertionError("{0}:\n  got      {1!r}\n  expected {2!r}".format(
            name, actual, expected))


# ── Test 1: backend identifies itself ──────────────────────────────
def test_backend_name():
    b = runloom_c.backend()
    assert b in ("ucontext", "fibers", "fcontext-asm"), "unexpected backend: " + b


# ── Test 2: yield/resume chain on raw Coro ─────────────────────────
def test_yield_resume_chain():
    log = []
    def child():
        log.append("a")
        runloom_c.yield_()
        log.append("b")
        runloom_c.yield_()
        log.append("c")
        return "done"
    c = runloom_c.Coro(child)
    c.resume(); eq(log, ["a"], "after resume 1")
    c.resume(); eq(log, ["a", "b"], "after resume 2")
    c.resume(); eq(log, ["a", "b", "c"], "after resume 3")
    assert c.done
    eq(c.result, "done", "result")


# ── Test 3: exception propagates ───────────────────────────────────
def test_exception_propagates():
    def child():
        raise ValueError("boom")
    c = runloom_c.Coro(child)
    try:
        c.resume()
    except ValueError as e:
        assert str(e) == "boom"
    else:
        raise AssertionError("expected ValueError")
    assert c.done


# ── Test 4: three fibers interleave round-robin ────────────────
def test_three_fibers_interleave():
    log = []
    def make_worker(name, n):
        def w():
            for i in range(n):
                log.append((name, i))
                runloom_c.sched_yield()
        return w
    runloom_c.go(make_worker("A", 3))
    runloom_c.go(make_worker("B", 3))
    runloom_c.go(make_worker("C", 3))
    runloom_c.run()
    expected = [("A", 0), ("B", 0), ("C", 0),
                ("A", 1), ("B", 1), ("C", 1),
                ("A", 2), ("B", 2), ("C", 2)]
    eq(log, expected, "round-robin order")


# ── Test 5: sleep yields the OS thread ─────────────────────────────
def test_sleep_lets_others_run():
    log = []
    def sleeper():
        log.append("s1-start")
        runloom_c.sched_sleep(0.05)
        log.append("s1-end")
    def burner():
        for i in range(3):
            log.append(("b", i))
            runloom_c.sched_yield()
    runloom_c.go(sleeper)
    runloom_c.go(burner)
    t0 = time.monotonic()
    runloom_c.run()
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
        test_three_fibers_interleave,
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
