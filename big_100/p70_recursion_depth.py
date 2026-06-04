"""big_100 / 70 -- recursion depth test.

Goroutines measure how deep they can recurse before RecursionError, recursing a
LITTLE under that ceiling (verifying the accumulator is exact, with a yield at
each level), and confirm that crossing the ceiling raises a clean
RecursionError -- never a SIGSEGV from running off the goroutine stack.

This surfaces FINDINGS BUG #6: a goroutine's recursion ceiling is only ~50-80
frames (the hub's C-recursion budget is nearly spent before the goroutine
runs), despite sys.getrecursionlimit()==1,000,000 and a 4 MB stack.  The project
reports the observed ceiling and verifies the failure mode is RecursionError,
not a crash.

Stresses: recursion counters / C-recursion budget, frame state across yields.
"""
import harness
import runloom
import runloom_c


def deep_sum(n):
    if n == 0:
        return 0
    runloom.yield_now()
    return n + deep_sum(n - 1)


def measure_ceiling():
    """Largest depth that does NOT raise (no yields -> pure depth)."""
    def plain(n):
        if n == 0:
            return 0
        return 1 + plain(n - 1)
    d = 8
    last = 0
    while d < 100000:
        try:
            plain(d)
            last = d
            d += 8
        except RecursionError:
            return last
    return last


def worker(H, wid, rng, state):
    ceiling = measure_ceiling()
    state["min_ceiling"][wid & 1023] = (
        min(state["min_ceiling"][wid & 1023], ceiling)
        if state["min_ceiling"][wid & 1023] else ceiling)
    if not H.check(ceiling >= 8,
                   "goroutine could barely recurse: ceiling={0} wid={1}".format(
                       ceiling, wid)):
        return
    # Recurse safely under the ceiling, yielding at every level; verify exact.
    safe = max(1, min(ceiling - 4, rng.randint(4, 40)))
    expected = safe * (safe + 1) // 2
    while H.running():
        got = deep_sum(safe)
        if not H.check(got == expected,
                       "recursion+yield wrong sum wid={0}: {1} != {2}".format(
                           wid, got, expected)):
            return
        # Crossing the ceiling must raise RecursionError, never crash.
        try:
            deep_sum(ceiling + 500)
        except RecursionError:
            pass
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"min_ceiling": [0] * 1024}


def body(H):
    runloom_c.set_stack_size(4 * 1024 * 1024)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ceilings = [c for c in H.state["min_ceiling"] if c]
    if ceilings:
        H.log("goroutine recursion ceiling: min={0} max={1} (sys limit is "
              "1,000,000 -- FINDINGS BUG #6)".format(min(ceilings),
                                                     max(ceilings)))


if __name__ == "__main__":
    harness.main("p70_recursion_depth", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="measure goroutine recursion ceiling; clean RecursionError")
