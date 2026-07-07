"""big_100 / 589 -- pstats.Stats aggregation conservation + isolation under M:N.

pstats is a REPORTING module over profiler output.  Unlike cProfile/profile it
carries NO process-global hook of its own: everything lives on a pstats.Stats
INSTANCE.  A Stats object owns a `stats` dict {func: (cc, nc, tt, ct, callers)}
(func == (filename, line, name)) plus derived scalars total_calls / prim_calls /
total_tt computed once in get_top_level_stats().  Its transforming methods --
sort_stats() (builds fcn_list, a permutation of the keys), strip_dirs() (relabels
funcs to their basename and MERGES colliding entries via add_func_stats()), and
get_stats_profile() (projects the dict to a StatsProfile) -- are ALL pure
functions of the instance's own dict.

So the correct oracle here is the guidance's rule for a process-global module:
do NOT test the (nonexistent) global hook -- test a SINGLE-OWNER object the
module PRODUCES.  Each fiber builds its OWN Stats from a fiber-local synthetic
stats dict with KNOWN closed-form aggregate totals, then asserts pstats' derived
quantities equal those totals -- exactly, and stably across a yield.

WHERE M:N COULD BREAK IT.  runloom runs these fibers in parallel across hubs with
the GIL off, migrating a fiber's frame between hubs at its yield points.  A
Stats instance is single-owner (built, read and mutated by ONE fiber, never
shared), so on a CORRECT runtime every law below holds deterministically.  A
failure would mean a real runtime bug: a torn read of the instance dict across a
hub migration, a lost/duplicated entry inside the C dict operations backing
sort_stats()/strip_dirs(), a scalar (total_calls/total_tt) that changed value
across a yield although nothing in this fiber mutated it, or a SIGSEGV inside the
sort/merge over a dict another hub's scheduler perturbed.  All of these are
runloom faults, not documented Python semantics (the object is never shared).

Verified against plain threads: 8 OS threads each building their own Stats from
a private synthetic dict and running the identical laws (GIL on AND off) show
0 violations -- the totals are a closed-form function of the fiber-local input,
so a correct M:N runtime must also be clean; the load-bearing oracle PASSES (exit
0) when there is no bug.

ORACLES:
  * LOAD-BEARING -- STATS AGGREGATION CONSERVATION + STABILITY (worker, HARD,
    fail-fast).  Single-owner.  Each fiber:
      - Builds a fiber-local stats dict of KNOWN funcs with integer cc/nc/tt/ct
        (integers so every sum and every f8() "%8.3f" round-trip is EXACT, no
        float slop), recording the closed-form sums sum_nc / sum_cc / sum_tt.
      - Constructs its OWN pstats.Stats (via a create_stats() shim -- no file,
        no profiler hook, no I/O) and asserts, as a CONSERVATION law:
            Stats.total_calls == sum_nc
            Stats.prim_calls  == sum_cc
            Stats.total_tt    == sum_tt
      - YIELDs (yield_now / sleep) so siblings interleave and the fiber may
        migrate hubs, then RE-reads the three scalars and asserts they are
        UNCHANGED (single-owner stability: nothing this fiber did could alter
        them; a change is a cross-fiber leak / torn instance read).
      - sort_stats(SortKey.CUMULATIVE): asserts fcn_list is a PERMUTATION of the
        stats keys (same multiset, same length -- no key dropped or duplicated by
        the C list-sort) AND that it is correctly ordered (cumtime ct
        non-increasing -- a real ordering-correctness check on the owned object).
      - get_stats_profile(): asserts StatsProfile.total_tt == float(f8(total_tt))
        (the documented projection identity).
      - strip_dirs(): asserts MERGE conservation -- summing (cc,nc,tt,ct) over the
        post-strip dict equals the pre-strip sums (strip_dirs only relabels to
        basename and add_func_stats-merges collisions, so the four component sums
        are invariant regardless of how many merges happened) AND every surviving
        key's filename is a bare basename.
    Every scalar/law is a closed-form function of the fiber-local input, so a
    violation is a runloom instance-isolation / torn-object desync, never Python
    semantics.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a C
    sort/merge over the instance dict never returns; the watchdog + require_no_lost
    catch it.

Stresses: pstats.Stats construction + get_top_level_stats aggregation, sort_stats
cmp_to_key list-sort permutation stability, strip_dirs func_strip_path relabel +
add_func_stats/add_callers merge conservation, get_stats_profile projection, and
the stability of instance-owned derived scalars across a hub-migrating yield.

Good TSan / controlled-M:N-replay target: sort_stats builds a Python list from a
live dict and sorts it; strip_dirs iterates the old dict while filling a new one;
under the single-owner arm exactly one fiber touches each dict, so a data-race
report on that instance dict -- or a replay that drops/dupes a key across a hub
migration -- is the cleanest signal before the permutation/conservation sums fire.
"""
import os

import pstats

import harness
import runloom

# Fiber-local func universe.  Filenames deliberately share BASENAMES across
# distinct directories so strip_dirs() collapses some entries (exercising the
# add_func_stats / add_callers merge path); the four component sums must stay
# invariant across the merge regardless.
DIRS = ("/pkg/aaa/", "/pkg/bbb/", "/pkg/ccc/", "/opt/x/", "/opt/y/")
BASENAMES = ("engine.py", "router.py", "codec.py", "pump.py", "sched.py",
             "netpoll.py", "hub.py", "slab.py")
NAMES = ("run", "step", "poll", "flush", "drain", "wake", "park", "submit",
         "close", "read", "write", "arm")

# Number of funcs per fiber-local Stats.  Enough to push the backing dict through
# a couple of growth boundaries (where a buggy C sort/merge would move entries)
# while keeping each round cheap under load.
NFUNCS_MIN = 24
NFUNCS_MAX = 64


class StatsShim:
    """Minimal object satisfying pstats.Stats.load_stats' create_stats path.

    Stats.load_stats does: arg.create_stats(); self.stats = arg.stats;
    arg.stats = {}.  This lets us build a Stats from an in-memory synthetic dict
    with NO file, NO marshal, and -- crucially -- NO profiler hook / sys.setprofile
    (which would be process-global and NOT single-owner).  The resulting Stats
    owns a fresh per-fiber dict."""

    def __init__(self, stats):
        self.stats = stats

    def create_stats(self):
        pass


def build_stats(rng):
    """Build one fiber-local synthetic stats dict + its closed-form aggregate
    sums.  All numeric fields are INTEGERS so total_tt sums and f8() "%8.3f"
    round-trips are exact (no float slop can masquerade as a torn read).

    Returns (stats_dict, sum_cc, sum_nc, sum_tt).  Keys (filename,line,name) are
    unique within the dict (they are dict keys); some SHARE a (basename,line,name)
    across dirs so strip_dirs merges them."""
    nfuncs = rng.randint(NFUNCS_MIN, NFUNCS_MAX)
    stats = {}
    sum_cc = 0
    sum_nc = 0
    sum_tt = 0
    seen = set()
    attempts = 0
    while len(stats) < nfuncs and attempts < nfuncs * 8:
        attempts += 1
        d = DIRS[rng.randrange(len(DIRS))]
        b = BASENAMES[rng.randrange(len(BASENAMES))]
        line = rng.randint(1, 40)
        name = NAMES[rng.randrange(len(NAMES))]
        func = (d + b, line, name)
        if func in seen:
            continue
        seen.add(func)
        # Primitive calls cc <= total calls nc (documented profiler invariant;
        # keep it so, though the conservation law does not depend on it).
        cc = rng.randint(1, 20)
        nc = cc + rng.randint(0, 30)
        tt = rng.randint(0, 90)
        ct = tt + rng.randint(0, 60)
        # A few callers in the cProfile (tuple) format so strip_dirs exercises the
        # add_callers tuple-merge branch.  Never the ("jprofile",0,"profiler")
        # sentinel, so top_level stays empty and deterministic.
        callers = {}
        ncall = rng.randint(0, 3)
        for _ in range(ncall):
            cd = DIRS[rng.randrange(len(DIRS))]
            cb = BASENAMES[rng.randrange(len(BASENAMES))]
            ck = (cd + cb, rng.randint(1, 40), NAMES[rng.randrange(len(NAMES))])
            callers[ck] = (rng.randint(1, 5), rng.randint(1, 5),
                           rng.randint(0, 9), rng.randint(0, 9))
        stats[func] = (cc, nc, tt, ct, callers)
        sum_cc += cc
        sum_nc += nc
        sum_tt += tt
    return stats, sum_cc, sum_nc, sum_tt


def f8_float(x):
    """Mirror pstats.get_stats_profile's projection of a scalar: float(f8(x))."""
    return float(pstats.f8(x))


def stats_oracle(H, wid, rng, state):
    """Single-owner pstats.Stats conservation + stability check (fail-fast)."""
    stats, sum_cc, sum_nc, sum_tt = build_stats(rng)

    st = pstats.Stats(StatsShim(dict(stats)))

    # ---- CONSERVATION: derived aggregates equal the closed-form input sums ----
    if st.total_calls != sum_nc:
        H.fail("pstats aggregation broken: Stats.total_calls={0} != sum(nc)={1} "
               "(wid {2}) -- get_top_level_stats mis-summed the fiber-local dict "
               "or the instance was perturbed by a sibling".format(
                   st.total_calls, sum_nc, wid))
        return
    if st.prim_calls != sum_cc:
        H.fail("pstats aggregation broken: Stats.prim_calls={0} != sum(cc)={1} "
               "(wid {2})".format(st.prim_calls, sum_cc, wid))
        return
    if st.total_tt != sum_tt:
        H.fail("pstats aggregation broken: Stats.total_tt={0} != sum(tt)={1} "
               "(wid {2})".format(st.total_tt, sum_tt, wid))
        return

    # ---- YIELD: allow sibling interleave + possible hub migration -------------
    runloom.yield_now()
    if state["tick"][wid] & 1:
        runloom.sleep(0.0002)

    # ---- STABILITY: single-owner scalars are UNCHANGED across the yield -------
    if st.total_calls != sum_nc or st.prim_calls != sum_cc or st.total_tt != sum_tt:
        H.fail("pstats instance DESYNC across a yield: (total_calls,prim_calls,"
               "total_tt)=({0},{1},{2}) but expected ({3},{4},{5}) (wid {6}) -- "
               "a single-owner Stats scalar changed with no mutation by this "
               "fiber; a torn instance read or cross-fiber leak".format(
                   st.total_calls, st.prim_calls, st.total_tt,
                   sum_nc, sum_cc, sum_tt, wid))
        return

    # ---- sort_stats: PERMUTATION + ORDERING law -------------------------------
    st.sort_stats(pstats.SortKey.CUMULATIVE)
    fcn_list = st.fcn_list
    if len(fcn_list) != len(st.stats):
        H.fail("sort_stats dropped/duped a key: len(fcn_list)={0} != "
               "len(stats)={1} (wid {2}) -- the C list-sort over the instance "
               "dict lost or duplicated an entry".format(
                   len(fcn_list), len(st.stats), wid))
        return
    if set(fcn_list) != set(st.stats.keys()):
        H.fail("sort_stats fcn_list is not a permutation of the stats keys "
               "(wid {0}) -- a key outside the fiber-local universe appeared or "
               "one vanished under M:N".format(wid))
        return
    prev_ct = None
    for func in fcn_list:
        ct = st.stats[func][3]
        if prev_ct is not None and ct > prev_ct:
            H.fail("sort_stats(CUMULATIVE) mis-ordered: cumtime {0} follows {1} "
                   "(ascending run in a descending sort) (wid {2}) -- the sort "
                   "over the owned dict produced a wrong order".format(
                       ct, prev_ct, wid))
            return
        prev_ct = ct

    # ---- get_stats_profile: projection identity -------------------------------
    sp = st.get_stats_profile()
    if sp.total_tt != f8_float(st.total_tt):
        H.fail("get_stats_profile projection broken: StatsProfile.total_tt={0} "
               "!= float(f8(total_tt))={1} (wid {2})".format(
                   sp.total_tt, f8_float(st.total_tt), wid))
        return

    # ---- strip_dirs: MERGE CONSERVATION ---------------------------------------
    pre = [0, 0, 0, 0]
    for cc, nc, tt, ct, callers in st.stats.values():
        pre[0] += cc
        pre[1] += nc
        pre[2] += tt
        pre[3] += ct

    runloom.yield_now()                 # migrate hubs across the merge boundary

    st.strip_dirs()

    post = [0, 0, 0, 0]
    for cc, nc, tt, ct, callers in st.stats.values():
        post[0] += cc
        post[1] += nc
        post[2] += tt
        post[3] += ct
    if post != pre:
        H.fail("strip_dirs MERGE non-conservation: component sums {0} != pre {1} "
               "(wid {2}) -- func_strip_path relabel + add_func_stats merge "
               "dropped or doubled a (cc,nc,tt,ct) unit under M:N".format(
                   post, pre, wid))
        return
    # The pre-strip sums must also still equal the original closed-form sums
    # (nothing between build and strip mutated the top-level component totals).
    if pre[0] != sum_cc or pre[1] != sum_nc or pre[2] != sum_tt:
        H.fail("pre-strip component sums {0} disagree with closed-form "
               "(cc={1},nc={2},tt={3}) (wid {4}) -- the instance dict was "
               "perturbed before strip_dirs".format(
                   pre, sum_cc, sum_nc, sum_tt, wid))
        return
    for func in st.stats:
        fn = func[0]
        if os.path.basename(fn) != fn:
            H.fail("strip_dirs left a directory in a filename: {0!r} (wid {1}) "
                   "-- func_strip_path did not reduce to basename".format(fn, wid))
            return

    state["checks"][wid] += 1           # ONE slot per wid (single-writer, race-free)


# Sustained inner loop so many fibers hold Stats instances simultaneously while
# sleep-parked across their yields -- the scheduler reliably interleaves a sibling
# (and a hub migration) before this fiber resumes.  A single check per fiber
# barely overlaps and does not reproduce a migration-window desync.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            state["tick"][wid] += 1     # single-writer-per-slot parity source
            stats_oracle(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Both tables are one slot per worker (wid-indexed, single-writer) -- race-free
    # under GIL-off M:N per HARD RULE 1.  Allocated here where H.funcs is known.
    H.state = {
        "checks": [0] * H.funcs,        # LOAD-BEARING oracle completions (non-vacuity)
        "tick": [0] * H.funcs,          # per-fiber parity for varied yield shape
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("pstats single-owner Stats conservation checks (all passed fail-fast): "
          "{0}; ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing conservation/stability hazard actually ran.
    H.check(checks > 0,
            "no pstats Stats conservation checks ran -- the load-bearing "
            "aggregation/stability oracle was never exercised (would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside a C sort/merge.
    H.require_no_lost("pstats stats conservation")


if __name__ == "__main__":
    harness.main(
        "p589_pstats_stats_conservation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each fiber builds its OWN pstats.Stats from a fiber-local "
                 "synthetic profile dict with KNOWN integer aggregate sums, then "
                 "asserts pstats' derived quantities are a closed-form function of "
                 "that input: total_calls==sum(nc), prim_calls==sum(cc), "
                 "total_tt==sum(tt) -- stable across a hub-migrating yield "
                 "(single-owner), sort_stats' fcn_list a correctly-ordered "
                 "permutation of the keys, get_stats_profile's projection "
                 "identity, and strip_dirs' merge conserving all four component "
                 "sums.  A scalar that shifts across a yield, a dropped/duped key, "
                 "or a broken merge sum is a runloom instance-isolation bug")
