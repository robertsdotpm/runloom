"""big_100 / 560 -- cProfile single-owner Stats object isolation under M:N.

cProfile is a PROCESS-GLOBAL module: on 3.14 cProfile.Profile.enable() registers
the profiler as the single sys.monitoring PROFILER tool for the WHOLE process, so
two fibers that call enable() concurrently collide with
"ValueError: Another profiling tool is already active" (verified: 8 GIL-off OS
threads each with their OWN independent Profile() still raise this thousands of
times -- the tool slot is one-per-process, NOT one-per-thread).  The active
profiler hook is therefore NOT single-owner and NOT fiber-local; it cannot itself
be the oracle (that would test documented process-global-hook semantics, not
runloom).

WHAT IS single-owner, per the contract for process-global modules: the OBJECT the
module PRODUCES.  A cProfile.Profile, once disabled, plus the pstats.Stats built
from it, is an ordinary Python object owned by ONE fiber -- a frozen snapshot of
call/time accounting.  That object is the load-bearing oracle: a correct runtime
must keep a single-owner object BIT-IDENTICAL and INTERNALLY CONSISTENT across a
yield / hub migration, no matter what siblings do on other hubs.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber produces its
own pstats.Stats snapshot, records a full serialization of it + its closed-world
totals, then YIELDS (so the scheduler migrates it to another hub and runs
siblings that build their own snapshots), then re-reads the SAME single-owner
object.  If runloom corrupts a single-owner object's fields across the yield (a
torn dict entry, a cross-fiber leak of another fiber's Stats state, a value/
identity change), the re-read serialization differs or the closed-world law
breaks.  On a correct runtime the object is untouched and every check passes
(program exits 0).

THE PROCESS-GLOBAL TOOL SLOT is serialized by a cooperative runloom Lock (created
in the root).  Only the enable -> workload -> disable region is held; this makes
production the classic ONE-profiler-at-a-time usage that the sys.monitoring slot
requires (never a runloom-thread-safety claim about cProfile, which has none).
The load-bearing oracle -- object stability across the yield -- runs OUTSIDE the
lock, concurrently across fibers each holding its own single-owner Stats.

CLOSED-WORLD LAWS on the single-owner Stats object (robust to the process-global
hook counting a stray sibling call -- both sides of each identity move together,
so contamination never breaks them; only a corrupted object does):
  * sum over entries of nc (total call count incl. recursion) == Stats.total_calls
  * sum over entries of cc (primitive call count)            == Stats.prim_calls
  * the full repr serialization of the stats dict is identical before and after
    the yield (every key, every (cc, nc, tt, ct, callers) tuple unchanged)
  * total_calls and prim_calls are individually unchanged across the yield
A deterministic fiber-local workload (leaf() called LEAF_CALLS times + a small
recursion) makes the snapshot non-trivial: total_calls != prim_calls because of
the recursion, so a corruption that conflates the two is caught.

ORACLES:
  * LOAD-BEARING -- SINGLE-OWNER Stats STABILITY (worker, HARD, fail-fast).  Each
    fiber builds its own pstats.Stats, checks the closed-world laws, yields, then
    asserts the object is bit-identical + still self-consistent.  Single-owner:
    the Profile and Stats are fiber-local, never shared.  A failure is a runloom
    single-owner-object desync across hub migration.

  * MEASURED (report-ONLY, NEVER fails): process-global-hook contention.  Because
    the sys.monitoring PROFILER slot fires for calls on ANY thread while a fiber's
    profiler is active, a fiber's leaf() count can differ from LEAF_CALLS if a
    sibling's execution leaked into the window (documented process-global-hook
    behavior).  We MEASURE the deviation and the "monitoring slot busy" skips; we
    NEVER fail on them (serializing production keeps them near zero, which is the
    CORRECT use of a one-per-process tool -- not a runloom bug either way).

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-produce
    (e.g. parked forever on the profiler lock, or vanished inside a Stats build)
    never returns; the watchdog + require_no_lost catch it.

FAIL ON: a single-owner Stats snapshot whose serialization changes across a yield,
whose total_calls/prim_calls change, or whose closed-world sums stop matching --
i.e. a torn/leaked single-owner object under M:N.  The process-global-hook
contention arm is report-only (documented sys.monitoring semantics, not a bug).

Stresses: cProfile.Profile enable/disable over the process-global sys.monitoring
PROFILER slot under a cooperative lock, pstats.Stats snapshot construction,
single-owner produced-object stability across yield + hub migration, closed-world
call-accounting conservation (sum nc == total_calls, sum cc == prim_calls) on a
snapshot with recursion.
"""
import cProfile
import pstats

import harness
import runloom

# Deterministic fiber-local workload.  leaf() is called exactly LEAF_CALLS times
# per snapshot; rec() recurses REC_DEPTH deep so the snapshot has an entry whose
# nc (total calls incl. recursion) exceeds its cc (primitive calls) -- this makes
# Stats.total_calls != Stats.prim_calls, so a corruption conflating the two totals
# is caught by the closed-world laws.
LEAF_CALLS = 40
REC_DEPTH = 6


def leaf(x):
    """The hot leaf; called LEAF_CALLS times per snapshot (cc == nc == LEAF_CALLS
    in an uncontaminated window)."""
    return x * x


def rec(n):
    """Small recursion so the snapshot has an nc>cc entry (total_calls != prim)."""
    if n <= 0:
        return 0
    return 1 + rec(n - 1)


def driver(n):
    """Deterministic fiber-local workload profiled into the single-owner snapshot.
    Contains NO runloom yield -- the profiler-enabled region never cooperatively
    hands off, so the process-global tool slot is held only briefly."""
    s = 0
    for i in range(n):
        s += leaf(i)
    rec(REC_DEPTH)
    return s


# The pstats key for leaf() -- (filename, first line no, function name) -- so the
# MEASURED (report-only) arm can look up leaf's recorded call count.
LEAF_KEY = (leaf.__code__.co_filename,
            leaf.__code__.co_firstlineno,
            leaf.__code__.co_name)


def serialize(stats):
    """Canonical full serialization of a pstats stats dict: every key with its
    complete (cc, nc, tt, ct, callers) tuple, key-sorted so the string is stable.
    Captures ALL fields (counts, cumulative/total time, caller map) so ANY
    cross-yield corruption of the single-owner object shows as a diff."""
    return repr([(k, stats[k]) for k in sorted(stats)])


# Sustained snapshots per worker, bounded by H.running().  The single-owner-object
# hazard only manifests under sustained churn -- many fibers holding their own
# frozen Stats and sleep-parked across a yield while siblings build more -- so a
# sibling reliably interleaves before this fiber re-reads its snapshot.
INNER_CAP = 100000


def profile_check(H, wid, rng, idx, state):
    """Produce a single-owner pstats.Stats snapshot, verify the closed-world laws,
    yield, then assert the snapshot is bit-identical + still self-consistent.
    A cross-yield change to this fiber's private object is a runloom desync."""
    lock = state["lock"]
    pr = cProfile.Profile()

    # ---- produce the snapshot under the process-global tool lock --------------
    # Only enable -> workload -> disable is serialized (the sys.monitoring PROFILER
    # slot is one-per-process; concurrent enable() otherwise raises ValueError).
    # No runloom yield inside the held region, so the lock is released promptly.
    with lock:
        try:
            pr.enable()
        except ValueError:
            # The single per-process PROFILER slot was momentarily still owned
            # (documented sys.monitoring semantics under interleave) -- report
            # only, skip this iteration.  NEVER a failure.
            state["busy"][wid & 1023] += 1
            return
        try:
            driver(LEAF_CALLS)
        finally:
            pr.disable()

    # pr is now disabled and single-owner.  Build THIS fiber's Stats snapshot
    # (an ordinary Python object owned only by this fiber).
    st = pstats.Stats(pr)
    stats = st.stats
    total_calls = st.total_calls
    prim_calls = st.prim_calls

    # Baseline: full serialization + closed-world sums BEFORE the yield.
    sig0 = serialize(stats)
    sum_nc0 = sum(v[1] for v in stats.values())
    sum_cc0 = sum(v[0] for v in stats.values())

    # Closed-world law (self-consistency of the produced object).  These hold on
    # any correct CPython snapshot regardless of whether a stray sibling call was
    # counted (both the per-entry sum and the reported total move together).
    if sum_nc0 != total_calls:
        H.fail("cProfile Stats self-inconsistent BEFORE yield: sum(nc)={0} != "
               "total_calls={1} (wid {2}) -- the produced single-owner snapshot's "
               "per-function call counts disagree with its reported total".format(
                   sum_nc0, total_calls, wid))
        return
    if sum_cc0 != prim_calls:
        H.fail("cProfile Stats self-inconsistent BEFORE yield: sum(cc)={0} != "
               "prim_calls={1} (wid {2}) -- primitive-call counts disagree with "
               "the reported primitive total".format(sum_cc0, prim_calls, wid))
        return

    # ---- MEASURED (report-only): process-global-hook contention ---------------
    # leaf's recorded count can exceed LEAF_CALLS if a sibling's execution leaked
    # into this fiber's profiler window via the process-global monitoring slot.
    # Documented sys.monitoring semantics -- MEASURE, never fail.
    lv = stats.get(LEAF_KEY)
    if lv is not None:
        state["measured"][wid & 1023] += 1
        if lv[1] != LEAF_CALLS:
            state["contention"][wid & 1023] += 1

    # ---- YIELD: hazard boundary -- migrate hubs, let siblings build snapshots --
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # ---- re-read the SAME single-owner object; must be untouched --------------
    sig1 = serialize(st.stats)
    if sig1 != sig0:
        H.fail("cProfile single-owner Stats CHANGED across a yield (wid {0}): "
               "the frozen snapshot's serialization differs before vs after a "
               "hub migration -- a torn entry or cross-fiber leak of another "
               "fiber's Stats state into this fiber's private object".format(wid))
        return
    if st.total_calls != total_calls:
        H.fail("cProfile Stats total_calls CHANGED across a yield: {0} -> {1} "
               "(wid {2}) -- a single-owner object's field mutated during hub "
               "migration".format(total_calls, st.total_calls, wid))
        return
    if st.prim_calls != prim_calls:
        H.fail("cProfile Stats prim_calls CHANGED across a yield: {0} -> {1} "
               "(wid {2}) -- a single-owner object's field mutated during hub "
               "migration".format(prim_calls, st.prim_calls, wid))
        return
    sum_nc1 = sum(v[1] for v in st.stats.values())
    sum_cc1 = sum(v[0] for v in st.stats.values())
    if sum_nc1 != st.total_calls or sum_cc1 != st.prim_calls:
        H.fail("cProfile Stats self-consistency BROKE across a yield (wid {0}): "
               "sum(nc)={1} total_calls={2} sum(cc)={3} prim_calls={4} -- the "
               "closed-world law held before the yield but not after, so a "
               "sibling corrupted this fiber's single-owner snapshot".format(
                   wid, sum_nc1, st.total_calls, sum_cc1, st.prim_calls))
        return

    state["checks"][wid] += 1              # single-writer-per-slot (race-free)


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            profile_check(H, wid, rng, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # lock serializes the process-global sys.monitoring PROFILER slot so only one
    # fiber's profiler is registered at a time (concurrent enable() otherwise
    # raises ValueError).  Built here, inside the root, where cooperative
    # primitives are valid.
    H.state = {
        "lock": runloom.sync.Lock(),
        "checks": [0] * H.funcs,        # LOAD-BEARING single-owner checks (wid-indexed)
        "measured": [0] * 1024,         # MEASURED leaf-count observations (report-only)
        "contention": [0] * 1024,       # process-global-hook leaks (report-only)
        "busy": [0] * 1024,             # monitoring-slot-busy skips (report-only)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    measured = sum(H.state["measured"])
    contention = sum(H.state["contention"])
    busy = sum(H.state["busy"])
    cpct = (100.0 * contention / measured) if measured else 0.0

    H.log("cProfile[single-owner LOAD-BEARING]: {0} Stats-stability checks (all "
          "passed fail-fast) | [process-global-hook MEASURED]: {1} leaf-count "
          "observations, {2} contended ({3:.1f}%, documented sys.monitoring "
          "semantics -- REPORT ONLY), {4} monitoring-slot-busy skips".format(
              checks, measured, contention, cpct, busy))

    if contention:
        H.log("note: {0} of {1} profiler windows counted a leaf() call that was "
              "NOT this fiber's -- the sys.monitoring PROFILER slot is one-per-"
              "PROCESS and fires for any thread's calls while a fiber's profiler "
              "is active, so a sibling's execution leaked into the window.  This "
              "is documented process-global-hook behavior, NOT a runloom bug, and "
              "never reaches the load-bearing single-owner oracle".format(
                  contention, measured))

    # NON-VACUITY: the load-bearing single-owner arm was actually exercised.
    H.check(checks > 0,
            "no single-owner cProfile Stats-stability checks ran -- the load-"
            "bearing produced-object hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded on the profiler
    # lock or inside a Stats build).
    H.require_no_lost("cProfile Stats isolation")


if __name__ == "__main__":
    harness.main(
        "p560_cProfile_stats_isolation", body, setup=setup, post=post,
        default_funcs=2000,
        describe="cProfile is process-global (3.14 sys.monitoring PROFILER slot is "
                 "one-per-process; concurrent enable() raises ValueError), so the "
                 "active hook is not single-owner.  LOAD-BEARING oracle lives on "
                 "the OBJECT it PRODUCES: each fiber serializes production under a "
                 "cooperative lock, builds its own pstats.Stats snapshot, records "
                 "a full serialization + the closed-world laws (sum nc == "
                 "total_calls, sum cc == prim_calls, total!=prim via recursion), "
                 "yields (hub migration), then asserts the single-owner snapshot "
                 "is bit-identical + still self-consistent.  A cross-yield change "
                 "to the private object is the runloom desync.  Process-global-"
                 "hook contention (leaf-count leak, slot-busy skips) is MEASURED "
                 "report-only (documented sys.monitoring semantics)")
