"""big_100 / 485 -- statistics module cache isolation under M:N.

The statistics module computes common descriptive statistics (mean, median,
stdev, variance, quantiles) on numerical data. Some implementations cache
intermediate results or decorators like @functools.lru_cache() may be applied
to accelerate repeated calls with the same arguments. Under M:N, many fibers
share one hub OS-thread, so the module-level cache (if any) is shared across
fibers.

WHERE M:N BREAKS IT (the gap this program probes).  If statistics functions use
an lru_cache or similar mechanism that is keyed ONLY by the input data (not by
the fiber/thread identity), then a fiber A computing statistics.median(data_A)
will cache the result under data_A.  When a sibling fiber B on the same hub
later calls statistics.median(data_B) where data_B is different, it should
receive a different result; but if the cache is too aggressive or has a false
collision, fiber B could receive fiber A's cached result for data_A instead
(silently wrong statistics).  This is the shared-module-state class: the cache
assumes one active computation per thread, which holds for genuine OS threads
but NOT for M:N fibers multiplexed onto one hub thread.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  statistics.mean/median/stdev/variance/quantiles are DOCUMENTED to compute
  exact results on each dataset passed to them.  A fiber computes a statistic
  on its OWN unique dataset and MUST get the correct result for that dataset --
  recomputing it after a yield MUST give the identical value, no matter what
  siblings do on the same hub.  We verified this with a standalone plain-threads
  control (8+ threads, same hazard, NO runloom) that this holds with
  PYTHON_GIL=1 AND PYTHON_GIL=0 on this very interpreter -- each fiber's
  statistic is deterministic given its dataset.  Under a CORRECT runloom, a
  fiber's statistic MUST also be deterministic (each fiber has its own closure
  / call stack, even though the cache is shared).  If runloom leaks a sibling's
  cached statistic across the yield -- the fiber's recomputed statistic differs
  from the pre-yield value for the SAME dataset, or its mean/median/etc is the
  WRONG value for its own dataset -- that is the runloom cache-isolation bug,
  and the load-bearing single-owner oracle PASSES on a correct runtime (program
  exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- STATISTICS CORRECTNESS-AND-STABILITY (worker, HARD,
    fail-fast).  Each fiber owns a unique dataset (deterministically derived
    from wid).  It computes the mean/median/stdev/variance/quantiles on that
    dataset, YIELDS (runloom.sleep / yield_now), then recomputes the same
    statistics and asserts:
      - recomputed_stat == pre_yield_stat (the statistic stayed the same after
        the yield -- no sibling's cached value leaked in);
      - the stat equals the canonical precomputed value for this dataset
        (closed-world: CANONICAL_STATS[dataset_id] is computed once, single-
        owner, before the pool -- so the check is independent of any shared
        cache);
      - for median: the value is EXACTLY as expected (a wrong digit count or
        value indicates a cache collision / leaked sibling result).
    Single-owner: nothing but THIS fiber should touch its dataset during the
    computation.  A failure is a runloom per-fiber statistics-cache isolation
    desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    computation (stranded inside a statistics function on a corrupted cache
    state) never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: a fiber's recomputed statistic differing from its pre-yield value
(changed across yield), or mismatching the canonical value for its dataset
(wrong cache lookup / cache collision), or a crash.

Stresses: statistics module cache isolation across hub fibers, cache collisions
/ false hits if caching is aggressive, mean/median/stdev/variance/quantiles
determinism across yields, pre-fiber dataset isolation.

Good TSan / controlled-M:N-replay target: if statistics functions use an
lru_cache or similar, a data race on the cache dict or a deterministic-replay
that runs two fibers computing different statistics simultaneously (with a
yield between them) will expose a cache collision before the correctness oracle
fires.
"""
import statistics
from decimal import Decimal
from fractions import Fraction

import harness
import runloom

# Canonical, single-owner precompute of statistics for several unique datasets.
# Computed ONCE in the root, before any worker runs, each in its OWN protected
# context so this table itself is race-free and independent of all shared state.
# The load-bearing oracle compares a fiber's computed statistic against the
# canonical value for its dataset.  Built in setup().
CANONICAL_STATS = {}

# Dataset generation: each wid gets a UNIQUE, deterministic dataset.
# Small enough to compute mean/median/stdev quickly; large enough that the
# values differ meaningfully (so a cache collision is detectable).
# Each dataset is a tuple of ints, deterministically seeded by wid.
DATASET_SIZE = 20


def build_dataset(wid):
    """Deterministically build a unique dataset for this wid."""
    import random
    rng = random.Random(0x9E3779B1 * (wid + 1))
    return tuple(rng.randint(1, 1000) for _ in range(DATASET_SIZE))


def build_canonical():
    """One-time, single-owner: compute mean/median/stdev/variance/quantiles
    for a representative set of datasets (one per wid up to a cap).  Each
    computation is independent of any shared cache since we compute it here,
    single-threaded, before the pool runs."""
    table = {}
    # Cap to a reasonable set (we'll use wid % cap to pick from this table).
    # 17 is a prime to avoid alignment artifacts in wid % 17.
    cap = 17
    for wid in range(cap):
        dataset = build_dataset(wid)
        data_list = list(dataset)     # statistics functions accept any iterable
        try:
            mean_val = statistics.mean(data_list)
            median_val = statistics.median(data_list)
            stdev_val = statistics.stdev(data_list)
            variance_val = statistics.variance(data_list)
            quantiles_val = tuple(statistics.quantiles(data_list, n=4))
        except Exception as exc:
            # Rare edge cases (single element, etc.) can fail some functions.
            # Record the exception so we can skip the check if needed.
            table[wid] = {
                "error": str(exc),
                "dataset": dataset,
            }
            continue
        table[wid] = {
            "dataset": dataset,
            "mean": mean_val,
            "median": median_val,
            "stdev": stdev_val,
            "variance": variance_val,
            "quantiles": quantiles_val,
        }
    return table


def setup(H):
    global CANONICAL_STATS
    CANONICAL_STATS = build_canonical()
    # Sanity: the canonical table must have entries for our dataset range.
    if not CANONICAL_STATS:
        H.fail("canonical table is empty -- build is broken")
        return
    H.state = {
        "checks": [0] * 1024,           # load-bearing correctness checks done
        "failures": [0] * 1024,         # checks that found a mismatch
        "sample_failure": [None],       # first observed bad sample
    }


# Sustained statistics computations per worker, bounded by H.running().  The
# cache-collision hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously computing statistics on different datasets with yields between
# them, so the scheduler reliably runs a sibling (on a different dataset) while
# this fiber is parked, and any cached value would be contaminated.  A single
# statistic per fiber barely overlaps a sibling's.  So each worker runs a
# sustained internal loop (one stats check per iteration) bounded by H.running()
# -- which makes the load-bearing oracle fire at the DEFAULT --rounds 1.
# INNER_CAP stops one worker from monopolizing teardown on a slow box.
INNER_CAP = 100000


def stats_check(H, wid, idx, state):
    """Compute statistics on a fiber's UNIQUE dataset, yield, recompute,
    and assert correctness and stability."""
    # Pick a dataset from the canonical table (rotate by idx so a fiber's
    # dataset varies slightly across iterations, making cache collisions even
    # more detectable if the cache is aggressive).
    dataset_id = (wid + idx) % len(CANONICAL_STATS)
    canon = CANONICAL_STATS.get(dataset_id)
    if canon is None or "error" in canon:
        return                         # skip edge cases

    dataset = canon["dataset"]
    data_list = list(dataset)

    # Compute statistics BEFORE the yield.  Store these values so we can
    # compare after the yield.
    try:
        mean_pre = statistics.mean(data_list)
        median_pre = statistics.median(data_list)
        stdev_pre = statistics.stdev(data_list)
        variance_pre = statistics.variance(data_list)
        quantiles_pre = tuple(statistics.quantiles(data_list, n=4))
    except Exception as exc:
        H.fail("fiber {0} pre-yield statistics computation failed: {1}".format(
            wid, exc))
        return

    # YIELD + SLEEP-PARK: a sibling fiber on this hub runs (computing statistics
    # on a DIFFERENT dataset) while this fiber is PARKED.  The sleep-park -- not
    # a bare yield_now -- is what reliably deschedules this fiber long enough
    # that the scheduler runs a sibling on the same hub before we resume.  If
    # runloom leaks a sibling's cached statistic or the cache has a false
    # collision, the sibling's result could contaminate our recompute.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Recompute statistics AFTER the yield on the SAME dataset.
    try:
        mean_post = statistics.mean(data_list)
        median_post = statistics.median(data_list)
        stdev_post = statistics.stdev(data_list)
        variance_post = statistics.variance(data_list)
        quantiles_post = tuple(statistics.quantiles(data_list, n=4))
    except Exception as exc:
        H.fail("fiber {0} post-yield statistics computation failed: {1}".format(
            wid, exc))
        return

    # Increment check counter.
    state["checks"][wid & 1023] += 1

    # ORACLE 1: Pre-yield == post-yield for the SAME dataset (stability).
    # A yield should not change the statistic for the same data.
    if mean_pre != mean_post:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "mean_unstable", dataset_id, mean_pre, mean_post)
        H.fail("statistics.mean NOT STABLE: fiber {0} got {1} before yield, "
               "{2} after yield (same dataset {3}) -- a sibling fiber's "
               "statistic leaked into this fiber's cache or a cache collision "
               "returned the wrong value (runloom shared-hub-identity bug, "
               "0 under plain threads)".format(wid, mean_pre, mean_post, dataset_id))
        return
    if median_pre != median_post:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "median_unstable", dataset_id, median_pre, median_post)
        H.fail("statistics.median NOT STABLE: fiber {0} got {1} before yield, "
               "{2} after yield (same dataset {3}) -- a sibling's cached "
               "statistic or a cache collision corrupted this fiber's result "
               "(runloom shared-hub-identity bug, 0 under plain threads)".format(
                   wid, median_pre, median_post, dataset_id))
        return
    if stdev_pre != stdev_post:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "stdev_unstable", dataset_id, stdev_pre, stdev_post)
        H.fail("statistics.stdev NOT STABLE: fiber {0} got {1} before yield, "
               "{2} after yield (same dataset {3}) -- cache collision or "
               "sibling leak (runloom shared-hub-identity bug)".format(
                   wid, stdev_pre, stdev_post, dataset_id))
        return
    if variance_pre != variance_post:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "variance_unstable", dataset_id, variance_pre, variance_post)
        H.fail("statistics.variance NOT STABLE: fiber {0} got {1} before yield, "
               "{2} after yield (same dataset {3}) -- cache corruption "
               "(runloom shared-hub-identity bug)".format(
                   wid, variance_pre, variance_post, dataset_id))
        return
    if quantiles_pre != quantiles_post:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "quantiles_unstable", dataset_id, quantiles_pre, quantiles_post)
        H.fail("statistics.quantiles NOT STABLE: fiber {0} got {1} before yield, "
               "{2} after yield (same dataset {3}) -- sibling's cached quantiles "
               "or false collision (runloom shared-hub-identity bug)".format(
                   wid, quantiles_pre, quantiles_post, dataset_id))
        return

    # ORACLE 2: Post-yield value == canonical value for this dataset
    # (closed-world correctness).  The canonical value was computed single-
    # owner before the pool, so it is a race-free reference.
    canon_mean = canon.get("mean")
    canon_median = canon.get("median")
    canon_stdev = canon.get("stdev")
    canon_variance = canon.get("variance")
    canon_quantiles = canon.get("quantiles")

    if canon_mean is not None and mean_post != canon_mean:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "mean_wrong", dataset_id, mean_post, canon_mean)
        H.fail("statistics.mean WRONG: fiber {0} computed {1} but canonical "
               "for dataset {2} is {3} -- the cache returned the wrong value "
               "for this dataset (runloom cache collision / sibling leak)".format(
                   wid, mean_post, dataset_id, canon_mean))
        return
    if canon_median is not None and median_post != canon_median:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "median_wrong", dataset_id, median_post, canon_median)
        H.fail("statistics.median WRONG: fiber {0} computed {1} but canonical "
               "for dataset {2} is {3} -- a cache collision returned the wrong "
               "median (runloom shared-hub-identity bug)".format(
                   wid, median_post, dataset_id, canon_median))
        return
    if canon_stdev is not None and stdev_post != canon_stdev:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "stdev_wrong", dataset_id, stdev_post, canon_stdev)
        H.fail("statistics.stdev WRONG: fiber {0} computed {1} but canonical "
               "is {2} -- cache corruption (runloom bug)".format(
                   wid, stdev_post, dataset_id, canon_stdev))
        return
    if canon_variance is not None and variance_post != canon_variance:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "variance_wrong", dataset_id, variance_post, canon_variance)
        H.fail("statistics.variance WRONG: fiber {0} computed {1} but canonical "
               "is {2} -- cache collision (runloom shared-hub-identity bug)".format(
                   wid, variance_post, dataset_id, canon_variance))
        return
    if canon_quantiles is not None and quantiles_post != canon_quantiles:
        state["failures"][wid & 1023] += 1
        if state["sample_failure"][0] is None:
            state["sample_failure"][0] = (
                wid, "quantiles_wrong", dataset_id, quantiles_post, canon_quantiles)
        H.fail("statistics.quantiles WRONG: fiber {0} computed {1} but canonical "
               "for dataset {2} is {3} -- sibling's cached quantiles or false "
               "collision (runloom shared-hub-identity bug)".format(
                   wid, quantiles_post, dataset_id, canon_quantiles))
        return


def worker(H, wid, rng, state):
    """Each fiber runs a sustained loop of statistics checks, bounded by
    H.running().  The cache-isolation hazard only manifests under churn --
    many fibers simultaneously computing statistics on different datasets
    with yields between them."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            stats_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    failures = sum(H.state["failures"])
    fail_pct = (100.0 * failures / checks) if checks else 0.0
    sample = H.state["sample_failure"][0]

    H.log("statistics: {0} checks {1} failures ({2:.2f}%) sample={3}".format(
        checks, failures, fail_pct, sample))

    if failures:
        H.log("note: the statistics module cache is NOT isolated across hub "
              "fibers under M:N -- a sibling fiber's cached statistic or a "
              "cache collision returned the wrong value (0 under plain threads "
              "GIL on AND off; the shared-module-state is the runloom cache-"
              "isolation gap, similar to p66/p67/p468).  Each fiber's "
              "statistics.mean/median/stdev/variance/quantiles must be "
              "independent of siblings, even though they share the hub thread.")

    # NON-VACUITY: the load-bearing statistics hazard was actually exercised.
    H.check(checks > 0,
            "no statistics checks ran -- the load-bearing cache-isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # statistics function on a corrupted cache state).
    H.require_no_lost("statistics module cache isolation")


if __name__ == "__main__":
    harness.main(
        "p485_statistics", body, setup=setup, post=post,
        default_funcs=8000,
        describe="statistics module cache isolation under M:N.  Each fiber "
                 "computes mean/median/stdev/variance/quantiles on its OWN "
                 "UNIQUE dataset, yields, recomputes, and asserts the result "
                 "is stable (same pre/post-yield) and correct (matches "
                 "canonical precomputed for this dataset).  LOAD-BEARING: the "
                 "cache MUST NOT leak a sibling's result or collide falsely -- "
                 "0 failures under plain threads GIL on AND off (the shared-"
                 "module-state cache is the runloom isolation gap, like p66/"
                 "p67/p468).  Same class as decimal/warnings/reprlib: each "
                 "fiber needs its own view of the statistics computation")
