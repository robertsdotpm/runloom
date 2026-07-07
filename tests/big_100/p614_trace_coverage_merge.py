"""big_100 / 614 -- trace.CoverageResults merge conservation + _find_lines purity under M:N.

The `trace` module's PROCESS-GLOBAL machinery (Trace, sys.settrace) is NOT
single-owner -- the trace hook is a per-thread/per-hub global, so wiring the
oracle to a live Trace run would race exactly like any shared-across-threads
sys.settrace and prove nothing about runloom.  Instead we build the oracle on the
SINGLE-OWNER objects the module PRODUCES:

  * trace.CoverageResults -- a plain result object whose `counts` dict maps
    (filename, lineno) -> hit-count.  Its update() method MERGES another
    CoverageResults in with a per-key READ-MODIFY-WRITE:
        counts[key] = counts.get(key, 0) + other_counts[key]
    That is a textbook lost-count RMW loop over a live dict -- but when the
    CoverageResults is OWNED BY ONE FIBER (never shared), the merge is race-free
    by construction and the closed-world sum MUST hold exactly.  If it does not,
    the fiber's own single-owner dict was corrupted across a hub migration / yield
    (a torn entry, a cross-fiber leak of another fiber's counts, a dropped RMW on
    a single-owner object) -- a real runloom bug.

  * trace._find_lines_from_code(code, strs) -- a PURE function: given a compiled
    code object and a set of string-literal line numbers to skip, it walks
    dis.findlinestarts(code) and returns a dict {lineno: 1} of the executable
    lines.  Deterministic: same code + same strs -> bit-identical dict.  Computed
    on a fiber-local compiled source, recomputed across a yield, it must be
    IDENTICAL both times and match the closed-form line set from findlinestarts.

WHERE M:N COULD BREAK IT (the gap this probes).  Each fiber owns a fresh
CoverageResults and a fiber-local key universe (filenames + linenos derived from
its wid, disjoint from every sibling's).  It merges several KNOWN donor
CoverageResults into its own result, YIELDING between merges so a sibling reliably
interleaves and (if isolation is broken) could scribble on this fiber's counts
dict, migrate the fiber to another hub mid-RMW, or leak a sibling's (filename,
lineno) key into this fiber's universe.  On a CORRECT runtime the single-owner
merge conserves every unit exactly and the pure line-finder is bit-stable.

WHICH ORACLE IS LOAD-BEARING (verified semantics):

  * CONSERVATION (single-owner, HARD, fail-fast).  A fiber merges DONORS donor
    CoverageResults, each carrying a KNOWN multiset of counts over the fiber's
    OWN key universe, into ITS OWN CoverageResults.  The closed-form expected
    count per key (computed in a fiber-local dict, independent of trace) MUST
    equal the merged counts[key] exactly; sum(counts.values()) MUST equal total
    units offered; NO key outside this fiber's universe may appear.  Because the
    CoverageResults is single-owner, a mismatch is NOT documented shared-dict
    behaviour -- it is a corruption of this fiber's private object.

  * PURITY (single-owner, HARD, fail-fast).  A fiber compiles its OWN source,
    computes the executable-line dict via trace._find_lines_from_code, yields,
    recomputes, and asserts the two dicts are bit-identical and match the
    closed-form line set.  A pure function on fiber-local input must be stable
    across a hub migration.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (merge_checks>0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the
    update() RMW loop or the findlinestarts walk never returns; the watchdog +
    require_no_lost catch it.

There is deliberately NO shared/report-only arm: a SHARED CoverageResults merged
by many fibers without a lock would lose counts exactly as any shared dict does
GIL-off -- documented Python behaviour, not a runloom bug -- so we never build one
and never risk mislabelling it.  The whole design keeps every mutated object
single-owner, so any FAIL is a real runtime fault.

FAIL ON: a single-owner CoverageResults whose merged count for a key != the
closed-form sum (dropped/doubled/torn RMW on a private dict), an out-of-universe
key leaking into a fiber's result, or the pure line-finder returning a different
dict across a yield.

Stresses: trace.CoverageResults.update per-key RMW over a single-owner dict across
hub migration + yield, trace._find_lines_from_code purity (dis.findlinestarts walk)
across a yield, per-fiber result-object isolation under M:N churn.
"""
import trace

import harness
import runloom

# Per-fiber key universe.  Each fiber's (filename, lineno) keys are derived from
# its wid so no two fibers share a key -- the single-owner property.  UNIVERSE_LINES
# is sized to push the CoverageResults.counts dict through several growth/rehash
# boundaries (rehash is what moves entries under a live RMW if isolation breaks).
UNIVERSE_LINES = 96

# Donor CoverageResults merged into each fiber's own result per check.  Several
# merges with a yield between them give the scheduler many interleave points at
# which a sibling could (if broken) corrupt this fiber's private counts dict.
DONORS = 6

# Sustained checks per worker, bounded by H.running().  Like p490, the isolation
# hazard only manifests under SUSTAINED churn: many fibers simultaneously merging
# into their own results while yield-parked, so a sibling reliably interleaves
# before this fiber resumes.  A single check per fiber barely overlaps a sibling.
INNER_CAP = 100000


def fiber_filename(wid, idx):
    """A (filename) unique to this fiber+check.  Distinct across fibers so keys
    never alias a sibling's -- the single-owner property of the counts dict."""
    return "trace_fiber_W{0}_I{1}.py".format(wid, idx)


def donor_count(wid, donor_idx, lineno):
    """Closed-form count donor `donor_idx` contributes to `lineno` for this fiber.

    Non-uniform (varies with wid, donor, lineno) so a dropped/doubled/torn merge
    on any single key moves that key's total by a detectable, key-specific amount
    -- a uniform pattern could mask a swap between two keys."""
    return 1 + ((wid * 31 + donor_idx * 7 + lineno * 13) % 5)


def build_source(wid, idx):
    """A small, VALID, fiber-local Python source whose executable-line set is
    non-trivial (branches + nested def) so _find_lines_from_code has real work.

    The exact text varies with wid so distinct fibers compile distinct code -- the
    pure-function oracle then proves the line-finder is bit-stable per fiber, not
    accidentally sharing a cached result across fibers."""
    n = 3 + (wid % 5)
    lines = ["def outer_{0}_{1}(a):".format(wid, idx)]
    for i in range(n):
        lines.append("    if a > {0}:".format(i))
        lines.append("        a = a + {0}".format(i + 1))
    lines.append("    def inner(b):")
    lines.append("        return b * {0}".format((wid % 7) + 1))
    lines.append("    return inner(a)")
    return "\n".join(lines) + "\n"


# ---- LOAD-BEARING arm A: single-owner CoverageResults merge conservation ----
def merge_check(H, wid, idx):
    """Merge DONORS known donor CoverageResults into a fiber-local result and
    assert the closed-world counting law.  Single-owner: the result and every
    donor are created in this fiber and never shared."""
    fname = fiber_filename(wid, idx)

    # This fiber's own result object (single-owner, never shared).
    result = trace.CoverageResults()

    # Closed-form expected counts, computed independently of trace in a fiber-local
    # dict (race-free -- one writer, this fiber).
    expected = {}
    total_offered = 0

    for d in range(DONORS):
        donor_counts = {}
        for lineno in range(1, UNIVERSE_LINES + 1):
            c = donor_count(wid, d, lineno)
            key = (fname, lineno)
            donor_counts[key] = c
            expected[key] = expected.get(key, 0) + c
            total_offered += c
        donor = trace.CoverageResults(counts=donor_counts)

        # Merge this donor into our own result via trace's per-key RMW loop.
        result.update(donor)

        # YIELD: allow siblings to run mid-merge-sequence.  If the result's counts
        # dict is not fiber-isolated, a sibling merging into ITS result on this hub
        # could scribble here, migrate us mid-RMW, or leak a key.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0003)

    counts = result.counts

    # Conservation: every key holds exactly the closed-form sum of donor counts.
    for key, exp in expected.items():
        got = counts.get(key, None)
        if got != exp:
            H.fail("trace.CoverageResults merge conservation broken: key {0!r} "
                   "== {1!r} but closed-form expected {2} after merging {3} "
                   "donors into a SINGLE-OWNER result (wid {4}) -- a per-key RMW "
                   "was {5} on this fiber's private counts dict across a yield".format(
                       key, got, exp, DONORS, wid,
                       "DROPPED/torn" if (got is None or got < exp) else "DOUBLED"))
            return False

    # No key outside this fiber's universe leaked in (all keys share our filename).
    for key in counts:
        fn, lineno = key
        if fn != fname or not (1 <= lineno <= UNIVERSE_LINES):
            H.fail("trace.CoverageResults holds OUT-OF-UNIVERSE key {0!r} after a "
                   "single-owner merge (wid {1}, own file {2!r}) -- a sibling "
                   "fiber's counts leaked into this fiber's private result".format(
                       key, wid, fname))
            return False

    # Total conservation: the whole result equals the units offered.
    got_total = sum(counts.values())
    if got_total != total_offered:
        H.fail("trace.CoverageResults total conservation broken: sum(counts)={0} "
               "!= units offered {1} across {2} donors (wid {3}) -- a merge "
               "increment was lost or doubled on a single-owner result".format(
                   got_total, total_offered, DONORS, wid))
        return False

    return True


# ---- LOAD-BEARING arm B: _find_lines_from_code purity ------------------------
def purity_check(H, wid, idx):
    """Compile fiber-local source, compute the executable-line dict via the pure
    trace._find_lines_from_code, yield, recompute, and assert bit-identical +
    matching the closed-form findlinestarts set.  Single-owner input."""
    src = build_source(wid, idx)
    code = compile(src, fiber_filename(wid, idx), "exec")

    # Closed-form line set straight from dis.findlinestarts (what the trace helper
    # is defined to return, given an empty string-line set): every distinct lineno
    # that starts a line-table entry.
    expected_lines = set()
    for _off, lineno in trace.dis.findlinestarts(code):
        if lineno is not None:
            expected_lines.add(lineno)

    baseline = trace._find_lines_from_code(code, frozenset())

    # YIELD: allow siblings to run; a pure function on fiber-local input must be
    # unaffected by any concurrent fiber.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    again = trace._find_lines_from_code(code, frozenset())

    # Purity: bit-identical across the yield.
    if again != baseline:
        H.fail("trace._find_lines_from_code NOT PURE across a yield (wid {0}): "
               "recomputed {1!r} != baseline {2!r} on the SAME fiber-local code "
               "object -- a hub migration corrupted a pure computation".format(
                   wid, again, baseline))
        return False

    # Correctness: the dict keys are exactly the executable linenos.
    if set(baseline.keys()) != expected_lines:
        H.fail("trace._find_lines_from_code WRONG (wid {0}): line set {1!r} != "
               "findlinestarts closed form {2!r} on fiber-local code".format(
                   wid, set(baseline.keys()), expected_lines))
        return False

    # Every value must be the sentinel 1 (the module's documented marker).
    for lineno, v in baseline.items():
        if v != 1:
            H.fail("trace._find_lines_from_code produced non-1 value {0!r} for "
                   "line {1} (wid {2}) -- torn dict value".format(v, lineno, wid))
            return False

    return True


def worker(H, wid, rng, state):
    """Each fiber runs BOTH single-owner arms per iteration: the CoverageResults
    merge conservation (fail-fast) and the _find_lines_from_code purity check
    (fail-fast).  Neither shares any object with a sibling."""
    merges = state["merge_checks"]
    purities = state["purity_checks"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not merge_check(H, wid, idx):
                return
            merges[wid] += 1                 # single-writer-per-slot, race-free
            if not purity_check(H, wid, idx):
                return
            purities[wid] += 1               # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-worker non-vacuity tallies: one slot per wid (single writer -> race-free),
    # allocated here where H.funcs is known.
    H.state = {
        "merge_checks": [0] * H.funcs,
        "purity_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    merges = sum(H.state["merge_checks"])
    purities = sum(H.state["purity_checks"])
    H.log("trace[single-owner LOAD-BEARING]: {0} CoverageResults.update merge-"
          "conservation checks + {1} _find_lines_from_code purity checks (all "
          "passed fail-fast); ops={2}".format(merges, purities, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner arms actually ran.
    H.check(merges > 0,
            "no CoverageResults merge-conservation checks ran -- the single-owner "
            "merge hazard was never exercised (oracle would be vacuous)")
    H.check(purities > 0,
            "no _find_lines_from_code purity checks ran -- the pure-function "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # update() RMW loop or the findlinestarts walk).
    H.require_no_lost("trace coverage merge conservation")


if __name__ == "__main__":
    harness.main(
        "p614_trace_coverage_merge", body, setup=setup, post=post,
        default_funcs=6000,
        describe="trace.CoverageResults.update merges another result with a per-key "
                 "read-modify-write over a live counts dict, and "
                 "trace._find_lines_from_code is a pure dis.findlinestarts walk. "
                 "LOAD-BEARING single-owner: each fiber merges KNOWN donor "
                 "CoverageResults into ITS OWN result across yields (closed-world "
                 "counting law: counts[key]==closed-form sum, total==units offered, "
                 "no out-of-universe key) and recomputes the pure line-finder on "
                 "fiber-local code (bit-identical across a yield).  A dropped/"
                 "doubled/torn merge on a private dict, a cross-fiber key leak, or "
                 "a non-pure line-finder result is the runloom bug")
