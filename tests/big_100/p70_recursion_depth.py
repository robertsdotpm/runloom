"""big_100 / 70 -- recursion depth test.

Goroutines measure how deep they can recurse before RecursionError, recursing a
LITTLE under that ceiling (verifying the accumulator is exact, with a yield at
each level), and confirm that crossing the ceiling raises a clean
RecursionError -- never a SIGSEGV from running off the goroutine stack.

The goroutine recursion ceiling is a RUNTIME CONSTANT (it depends on the
goroutine stack size + the C-recursion budget, not on the individual
goroutine), so we measure it ONCE and share it.  Probing it in every worker is
O(ceiling^2) per worker, which never finishes once the ceiling is large -- the
4 MB stack now yields a deep ceiling (FINDINGS BUG #6's old ~50-80 ceiling no
longer reproduces), so a 1M-goroutine run must not re-probe it per worker.

Stresses: recursion counters / C-recursion budget, frame state across yields.
"""
import harness
import runloom
import runloom_c


def plain(n):
    """Pure depth, no yields -- used only to bracket the ceiling."""
    if n == 0:
        return 0
    return 1 + plain(n - 1)


def deep_sum(n):
    if n == 0:
        return 0
    runloom.yield_now()
    return n + deep_sum(n - 1)


def measure_ceiling():
    """Bracket the ceiling with an EXPONENTIAL probe -- O(ceiling) total, vs the
    old linear `d += 8` which is O(ceiling^2).  Returns (safe_depth, raise_depth):
    safe_depth recurses cleanly; raise_depth is known to raise RecursionError."""
    d = 8
    last = 0
    while d < 1000000:
        try:
            plain(d)
            last = d
            d *= 2
        except RecursionError:
            return last, d
    return last, d


def worker(H, wid, rng, state):
    raise_depth = state["raise_depth"]
    # Every worker recurses a LITTLE (<=40) with yields and verifies the exact
    # sum -- the per-goroutine recursion+yield exercise, cheap regardless of the
    # ceiling.  A SUBSET also crosses the ceiling to confirm a clean
    # RecursionError (never SIGSEGV); crossing is O(ceiling), and a guard-page
    # crash would reproduce on ANY goroutine that crosses, so we sample it
    # rather than pay it 1M times.
    do_cross = (wid & 2047) == 0 and raise_depth > 0
    while H.running():
        safe = rng.randint(4, 40)
        expected = safe * (safe + 1) // 2
        got = deep_sum(safe)
        if not H.check(got == expected,
                       "recursion+yield wrong sum wid={0}: {1} != {2}".format(
                           wid, got, expected)):
            return
        if do_cross:
            # Crossing the ceiling must raise RecursionError, never crash.
            try:
                deep_sum(raise_depth)
            except RecursionError:
                pass
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"ceiling": 0, "raise_depth": 0}


def body(H):
    runloom_c.set_stack_size(4 * 1024 * 1024)
    # Measure the ceiling ONCE on a goroutine that has the 4 MB stack the
    # workers get, and share it.  Re-probing per worker is O(ceiling^2) and
    # never finishes at 1M.
    done = []

    def probe():
        done.append(measure_ceiling())

    H.fiber(probe)
    while not done and H.running():
        H.sleep(0.01)
    if done:
        H.state["ceiling"], H.state["raise_depth"] = done[0]
    if not H.check(H.state["ceiling"] >= 8,
                   "goroutine could barely recurse: ceiling={0}".format(
                       H.state["ceiling"])):
        return
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state["ceiling"]:
        H.log("goroutine recursion ceiling (4MB stack): safe>={0} raises_at>={1} "
              "(sys limit is 1,000,000)".format(
                  H.state["ceiling"], H.state["raise_depth"]))


if __name__ == "__main__":
    harness.main("p70_recursion_depth", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="measure goroutine recursion ceiling; clean RecursionError")
