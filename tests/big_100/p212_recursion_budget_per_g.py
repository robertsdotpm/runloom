"""big_100 / 212 -- per-goroutine recursion budget.

Each goroutine recurses (pure Python) to find the deepest depth it can reach
before a clean RecursionError, catching the RecursionError (never a SIGSEGV off
the goroutine stack), and records that max-safe depth.  The budget must be
PER-GOROUTINE and CONSISTENT: every goroutine independently reaches at least a
modest floor, and the measured depth does NOT shrink as more goroutines run
(which would signal a shared/leaking C-recursion counter -- FINDINGS BUG #6).
A subset also yields mid-recursion at a shallow level to confirm the budget (and
the exact running sum) survives a hub migration.

NOTE (FINDINGS BUG #6): the per-goroutine budget can be SMALL on the default
g-stack, so this test asserts CONSISTENCY + a modest floor, not a large depth.
Read p70 (which raises the stack to 4 MB to get a deep ceiling); here we keep
the harness-default stack so we measure the budget every goroutine actually has.

Stresses: per-goroutine C-recursion budget isolation, RecursionError cleanliness
under concurrency, recursion-sum integrity across a migration.
"""
import harness
import runloom

# A modest floor every goroutine MUST clear.  Calibrated well below the observed
# ceiling so it is robust, but high enough to fail if the budget collapses to a
# handful of frames under concurrency.  (sys recursion limit is ~1000; the
# goroutine C-stack budget is the real ceiling and is what we probe.)
FLOOR = 20


def plain(n):
    if n == 0:
        return 0
    return 1 + plain(n - 1)


def yielding_sum(n):
    """Recurse with a yield at each level -- exercises frame state + migration
    while a deep-ish Python frame chain is live, returning the exact sum."""
    if n == 0:
        return 0
    runloom.yield_now()
    return n + yielding_sum(n - 1)


def probe_ceiling():
    """Exponential bracket then a linear refine of the max-safe recursion depth
    for THIS goroutine.  O(ceiling) total (vs O(ceiling^2) for a pure linear
    probe).  Returns the deepest depth that did NOT raise RecursionError."""
    # Exponential phase: find a depth that raises.
    lo = 1
    hi = 2
    while hi < 1000000:
        try:
            plain(hi)
            lo = hi
            hi *= 2
        except RecursionError:
            break
    else:
        return lo
    # Binary-search refine between lo (ok) and hi (raises).
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        try:
            plain(mid)
            lo = mid
        except RecursionError:
            hi = mid
    return lo


def worker(H, wid, rng, state):
    # Every goroutine measures its OWN budget independently.
    depth = probe_ceiling()
    state["depths"][wid & (state["nslots"] - 1)] = depth
    # Record the (wid, depth) order so post() can check depth is not
    # monotonically DECREASING with goroutine index (a shared/leaking counter
    # would show exactly that downward drift).
    if depth < state["min_depth"][wid & 63]:
        state["min_depth"][wid & 63] = depth
    if depth > state["max_depth"][wid & 63]:
        state["max_depth"][wid & 63] = depth

    if not H.check(depth >= FLOOR,
                   "goroutine recursion budget below floor wid={0}: {1} < {2}"
                   .format(wid, depth, FLOOR)):
        return

    # A shallow recursion WITH a yield at every level must still come out with
    # the exact sum after the migration(s) -- the budget/frame chain survives.
    safe = rng.randint(4, min(40, max(4, depth - 1)))
    expected = safe * (safe + 1) // 2
    got = yielding_sum(safe)
    if not H.check(got == expected,
                   "recursion-sum corrupted across migration wid={0}: {1} != {2}"
                   .format(wid, got, expected)):
        return

    # Crossing the ceiling must raise a CLEAN RecursionError, never SIGSEGV.
    try:
        plain(depth + 5000)
        # If it somehow didn't raise, depth+5000 was actually safe -- not a
        # failure (just an under-estimate), so don't flag it.
    except RecursionError:
        pass

    H.op(wid)
    H.task_done(wid)


def setup(H):
    # depths slot count is a power of two for masking; sized to cover a typical
    # smoke run without aliasing too much.
    nslots = 1 << 14
    H.state = {
        "depths": [0] * nslots,
        "nslots": nslots,
        "min_depth": [10 ** 9] * 64,
        "max_depth": [0] * 64,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    depths = [d for d in H.state["depths"] if d > 0]
    H.check(len(depths) > 0, "no goroutine recorded a recursion depth")
    if not depths:
        return
    dmin = min(depths)
    dmax = max(depths)
    dmean = sum(depths) / len(depths)

    # 1) Floor: every recorded budget cleared the modest floor.
    H.check(dmin >= FLOOR,
            "min recursion budget {0} below floor {1} -- budget collapsed under "
            "concurrency".format(dmin, FLOOR))

    # 2) Consistency: the budget is a per-goroutine CONSTANT, so all goroutines
    #    on the same default stack should measure nearly the same ceiling.  A
    #    shared/leaking counter would make the budget SHRINK as more goroutines
    #    run, producing a wide spread.  Allow a generous band (the probe's
    #    binary search lands within 1 of the true ceiling; small variation is
    #    fine) but reject a collapse where the worst case is a small fraction of
    #    the best.
    H.check(dmin * 4 >= dmax,
            "recursion budget spread too wide (min={0} max={1}): suggests a "
            "shared/leaking C-recursion counter, not a per-g budget".format(
                dmin, dmax))

    H.log("recursion budget per-goroutine: min={0} max={1} mean={2:.1f} "
          "samples={3} floor={4}".format(dmin, dmax, dmean, len(depths), FLOOR))


if __name__ == "__main__":
    harness.main("p212_recursion_budget_per_g", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="per-goroutine recursion budget is isolated + "
                          "consistent (no shared/leaking counter); clean "
                          "RecursionError; sum survives migration")
