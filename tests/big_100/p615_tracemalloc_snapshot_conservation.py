"""big_100 / 615 -- tracemalloc.Snapshot aggregation conservation under M:N.

tracemalloc's process-global HOOK (start()/stop()/take_snapshot()) is NOT single-
owner -- it is a per-interpreter tracer, so it cannot host a fail-fast oracle.  But
the module PRODUCES a single-owner object that is entirely pure-Python data: a
`tracemalloc.Snapshot`.  A Snapshot is built from a plain tuple of TRACE tuples

    (domain: int, size: int, traceback: tuple-of-(filename, lineno), total_nframe)

and every analytic it offers -- Snapshot.statistics(key_type[, cumulative]),
Snapshot.filter_traces([DomainFilter/Filter]), Snapshot.compare_to(old, key_type),
plus pickle dump/load -- is a DETERMINISTIC, side-effect-free reduction over that
frozen tuple.  We NEVER call tracemalloc.start(): the whole oracle is fabricated
fiber-local data run through the module's grouping/filter/diff/pickle machinery.
So the Snapshot and every Statistic/StatisticDiff it yields are single-owner, and
the result is a CLOSED-FORM CONSERVATION law we can check exactly.

WHERE M:N COULD BREAK IT (the gap this program probes).  Snapshot._group_by builds
a `stats` dict and a `tracebacks` cache dict, mutates Statistic.size/.count via
read-modify-write while iterating the trace tuple, then statistics() sorts the
grouped list; filter_traces rebuilds a trace list under a comprehension;
compare_to runs _group_by over TWO snapshots and pops from one group dict while
iterating the other; pickle walks the object graph.  If runloom's M:N scheduler
leaked another fiber's grouping dict / Statistic accumulator into this fiber across
a yield, or tore the frozen trace tuple, the reduction would stop conserving:
group sizes would no longer sum to the total, a domain filter would admit an alien
byte, a diff would not telescope, or the pickled round-trip would disagree.  Under
a CORRECT runtime every reduction is a pure function of this fiber's own frozen
tuple and MUST reproduce the closed form bit-for-bit, before and after a yield.

CLOSED-WORLD / CONSERVATION oracle (single-owner, fail-fast).  Each fiber
fabricates its OWN trace tuple T (fiber-local, never shared) with KNOWN sizes,
domains, and per-traceback frame sets, and independently computes the closed form:
total_size = sum size, total_count = len(T), per-domain size/count subtotals, and
the cumulative size sum(size * nframes).  It then builds a Snapshot and asserts:

  * statistics('lineno'|'traceback'|'filename') : sum of group sizes == total_size
    and sum of group counts == total_count  (every block lands in exactly one
    group -- no byte created or destroyed by the grouping RMW);
  * statistics(cumulative=True)               : sum of group sizes == sum(size*
    nframes) and sum of counts == sum(nframes)  (frames within a traceback are
    kept DISTINCT so the cumulative fan-out is exact);
  * filter_traces(DomainFilter(d)) for every domain d partitions T exactly: each
    filtered snapshot's byte/trace subtotal == the domain subtotal, and the
    domains sum back to total_size / total_count  (a filter admitting an out-of-
    domain byte, or dropping one, breaks the partition);
  * compare_to(old) telescopes: sum(size_diff) == total_size - old_size and
    sum(count_diff) == total_count - old_count, and sum(size) == total_size;
  * ACROSS A YIELD the frozen trace tuple is identical (same object, same value)
    and statistics('lineno') recomputes to a list EQUAL to the pre-yield list;
  * pickle.loads(pickle.dumps(snapshot)).statistics('lineno') == the pre-yield
    list  (serialization round-trip preserves the reduction).

Single-owner: T, the Snapshot, the old Snapshot, and every Statistic are created
and read by ONE fiber.  A FAIL means a byte was created/destroyed by the grouping
RMW, the frozen tuple was torn, a filter mispartitioned, a diff failed to
telescope, a cross-fiber grouping dict leaked in across the yield, or the pickled
graph disagreed -- a real runtime desync, never documented Python semantics (the
Snapshot is immutable and unshared, so plain-threads behavior is identical: verified
by construction, the reduction is a pure function of a frozen tuple).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-_group_by
    (inside the grouping-dict RMW or the sort) on a desynced object never returns;
    the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Stresses: Snapshot._group_by grouping-dict + Statistic RMW accumulation, the
statistics() sort over the grouped list, filter_traces list comprehension +
DomainFilter._match, compare_to's dual _group_by with pop-while-iterate, and the
pickle round-trip of the Snapshot object graph -- all across hub migration + a
yield under tens of thousands of fibers.

Good TSan / controlled-M:N-replay target: the per-key `stat.size += size` inside
_group_by over a fresh grouping dict is a read-modify-write; under the single-owner
arm it is touched by ONE fiber, so a TSan report on a Statistic slot or the
grouping dict -- or a replay that sees a group total off by one block's size --
localizes the desync before the conservation sum even closes.
"""
import pickle

import tracemalloc

import harness
import runloom

# Synthetic frame universe.  16 files x lineno 1..49 => 784 possible frames, so a
# traceback of up to 4 DISTINCT frames is always constructible (the cumulative law
# requires distinct frames per traceback so the fan-out count is exact).
FILENAMES = tuple("/syn/mod{0}.py".format(i) for i in range(16))
LINENO_MAX = 50
NDOM = 4                                # distinct allocation domains
LIMIT = 8                               # Snapshot.traceback_limit (metadata only)

# Traces per fabricated snapshot.  Enough that the backing grouping dict grows past
# a rehash boundary and many first-frames collide into shared groups (so grouping
# is non-trivial), small enough that many iterations complete under the timeout.
TRACES_MIN = 12
TRACES_MAX = 48

# Sustained churn per worker, bounded by H.running().  The grouping/sort/filter/
# diff/pickle hazard only manifests under many fibers simultaneously reducing their
# own snapshots while sleep-PARKED across the yield; a single reduction per fiber
# barely overlaps a sibling's and does not reproduce.
INNER_CAP = 100000


def build_traces(rng):
    """Fabricate ONE fiber-local trace tuple with KNOWN sizes/domains/frames.

    Returns (traces_tuple, total_size, total_count, cum_size, cum_count,
             per_domain_size, per_domain_count).  Frames within any single
    traceback are DISTINCT so the cumulative fan-out (size counted once per frame)
    is an exact closed form.  Every value is drawn locally -- nothing is shared."""
    ntr = rng.randint(TRACES_MIN, TRACES_MAX)
    traces = []
    total_size = 0
    cum_size = 0
    cum_count = 0
    per_domain_size = [0] * NDOM
    per_domain_count = [0] * NDOM
    for _ in range(ntr):
        domain = rng.randrange(NDOM)
        size = rng.randint(1, 4096)
        nframes = rng.randint(1, 4)
        frames = []
        used = set()
        while len(frames) < nframes:
            fn = FILENAMES[rng.randrange(len(FILENAMES))]
            ln = rng.randrange(1, LINENO_MAX)
            key = (fn, ln)
            if key in used:
                continue
            used.add(key)
            frames.append(key)
        traceback = tuple(frames)
        traces.append((domain, size, traceback, nframes))
        total_size += size
        cum_size += size * nframes
        cum_count += nframes
        per_domain_size[domain] += size
        per_domain_count[domain] += 1
    return (tuple(traces), total_size, len(traces), cum_size, cum_count,
            per_domain_size, per_domain_count)


def check_conserved(H, stats, exp_size, exp_count, label, wid):
    """Non-cumulative grouping conserves: every block lands in exactly one group,
    so the group sizes sum to the total byte count and the group counts sum to the
    number of traces.  A mismatch means the grouping RMW created or destroyed a
    block's size/count -- a torn accumulator or a cross-fiber grouping-dict leak."""
    got_size = sum(s.size for s in stats)
    got_count = sum(s.count for s in stats)
    if got_size != exp_size:
        H.fail("statistics({0}) size NOT CONSERVED: group sizes sum to {1}, "
               "expected total {2} (wid {3}) -- a block's size was lost/created in "
               "the _group_by RMW or a sibling's grouping dict leaked in".format(
                   label, got_size, exp_size, wid))
        return False
    if got_count != exp_count:
        H.fail("statistics({0}) count NOT CONSERVED: group counts sum to {1}, "
               "expected {2} traces (wid {3}) -- a trace was dropped/doubled in "
               "the _group_by accumulation".format(label, got_count, exp_count, wid))
        return False
    return True


def snapshot_oracle(H, wid, idx, rng, state):
    """Single-owner Snapshot conservation check (fail-fast).

    Fabricate a fiber-local trace tuple + a second (old) one, build Snapshots, and
    assert the closed-form conservation laws hold for statistics/filter/compare_to,
    are STABLE across a yield, and survive a pickle round-trip."""
    (traces, total_size, total_count, cum_size, cum_count,
     dom_size, dom_count) = build_traces(rng)
    (old_traces, old_size, old_count, _oc, _occ, _ods, _odc) = build_traces(rng)

    snap = tracemalloc.Snapshot(traces, LIMIT)
    old_snap = tracemalloc.Snapshot(old_traces, LIMIT)

    # ---- BEFORE the yield: baseline reductions --------------------------------
    stats_lineno = snap.statistics('lineno')
    if not check_conserved(H, stats_lineno, total_size, total_count,
                           "'lineno' pre-yield", wid):
        return
    if not check_conserved(H, snap.statistics('traceback'), total_size,
                           total_count, "'traceback' pre-yield", wid):
        return
    if not check_conserved(H, snap.statistics('filename'), total_size,
                           total_count, "'filename' pre-yield", wid):
        return

    # Record identity of the frozen trace tuple so we can prove it is untouched
    # across the yield (a torn/replaced tuple is a hard fault).
    frozen = snap.traces._traces
    frozen_id = id(frozen)

    # ---- YIELD: let siblings reduce their own snapshots on other hubs ---------
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # ---- AFTER the yield: stability + the remaining conservation laws ---------
    # The frozen trace tuple must be the SAME object and value.
    if id(snap.traces._traces) != frozen_id:
        H.fail("Snapshot frozen trace tuple IDENTITY CHANGED across a yield "
               "(wid {0}) -- the immutable traces object was replaced, a cross-"
               "fiber Snapshot leak".format(wid))
        return
    if snap.traces._traces != frozen:
        H.fail("Snapshot frozen trace tuple VALUE CHANGED across a yield (wid "
               "{0}) -- the immutable traces were mutated under M:N".format(wid))
        return

    # Recompute statistics('lineno'); it must equal the pre-yield list exactly
    # (deterministic grouping + sort of an unchanged frozen tuple).
    stats_lineno_2 = snap.statistics('lineno')
    if stats_lineno_2 != stats_lineno:
        H.fail("statistics('lineno') CHANGED across a yield (wid {0}) -- the "
               "grouping/sort of an unchanged frozen Snapshot returned a different "
               "list; a sibling's grouping dict or Statistic accumulator leaked "
               "in".format(wid))
        return
    if not check_conserved(H, stats_lineno_2, total_size, total_count,
                           "'lineno' post-yield", wid):
        return

    # Cumulative grouping: each block's size is counted once per (distinct) frame.
    stats_cum = snap.statistics('lineno', cumulative=True)
    got_cum_size = sum(s.size for s in stats_cum)
    got_cum_count = sum(s.count for s in stats_cum)
    if got_cum_size != cum_size:
        H.fail("statistics('lineno', cumulative) size NOT CONSERVED: {0} != "
               "sum(size*nframes)={1} (wid {2}) -- the cumulative fan-out "
               "miscounted a frame under M:N".format(got_cum_size, cum_size, wid))
        return
    if got_cum_count != cum_count:
        H.fail("statistics('lineno', cumulative) count NOT CONSERVED: {0} != "
               "sum(nframes)={1} (wid {2})".format(got_cum_count, cum_count, wid))
        return

    # Domain partition: filter_traces(DomainFilter(d)) must split the trace tuple
    # into exact per-domain subtotals that sum back to the whole.
    part_size = 0
    part_count = 0
    for d in range(NDOM):
        sub = snap.filter_traces([tracemalloc.DomainFilter(True, d)])
        sub_traces = sub.traces._traces
        sub_size = sum(tr[1] for tr in sub_traces)
        sub_count = len(sub_traces)
        if sub_size != dom_size[d]:
            H.fail("filter_traces(DomainFilter({0})) byte subtotal {1} != "
                   "expected domain subtotal {2} (wid {3}) -- the domain filter "
                   "admitted an out-of-domain byte or dropped one".format(
                       d, sub_size, dom_size[d], wid))
            return
        if sub_count != dom_count[d]:
            H.fail("filter_traces(DomainFilter({0})) trace count {1} != expected "
                   "{2} (wid {3}) -- the domain partition lost/gained a "
                   "trace".format(d, sub_count, dom_count[d], wid))
            return
        # Every admitted trace really carries domain d.
        for tr in sub_traces:
            if tr[0] != d:
                H.fail("filter_traces(DomainFilter({0})) admitted a trace with "
                       "domain {1} (wid {2}) -- the filter matched the wrong "
                       "domain".format(d, tr[0], wid))
                return
        part_size += sub_size
        part_count += sub_count
    if part_size != total_size or part_count != total_count:
        H.fail("domain partition does not sum back to the whole: sizes {0}!={1} "
               "or counts {2}!={3} (wid {4}) -- filter_traces is not a "
               "partition".format(part_size, total_size, part_count, total_count,
                                   wid))
        return

    # Telescoping diff: compare_to(old) size sums to the new total and the diffs
    # sum to (new - old) for both size and count.
    diffs = snap.compare_to(old_snap, 'lineno')
    d_size = sum(x.size for x in diffs)
    d_size_diff = sum(x.size_diff for x in diffs)
    d_count_diff = sum(x.count_diff for x in diffs)
    if d_size != total_size:
        H.fail("compare_to size NOT CONSERVED: sum(diff.size)={0} != new total "
               "{1} (wid {2})".format(d_size, total_size, wid))
        return
    if d_size_diff != total_size - old_size:
        H.fail("compare_to does not telescope: sum(size_diff)={0} != new-old={1} "
               "(wid {2}) -- the dual _group_by / pop-while-iterate desynced "
               "under M:N".format(d_size_diff, total_size - old_size, wid))
        return
    if d_count_diff != total_count - old_count:
        H.fail("compare_to count does not telescope: sum(count_diff)={0} != "
               "new-old={1} (wid {2})".format(d_count_diff,
                                              total_count - old_count, wid))
        return

    # Pickle round-trip: the serialized Snapshot's reduction must reproduce the
    # pre-yield statistics exactly.
    loaded = pickle.loads(pickle.dumps(snap, pickle.HIGHEST_PROTOCOL))
    if loaded.statistics('lineno') != stats_lineno:
        H.fail("pickle round-trip changed the Snapshot reduction: loaded "
               "statistics('lineno') != original (wid {0}) -- the pickled object "
               "graph desynced under M:N".format(wid))
        return

    state["checks"][wid] += 1           # single-writer-per-slot (race-free)


def worker(H, wid, rng, state):
    """Each fiber sustains its own single-owner Snapshot conservation checks; the
    fabricated trace tuple, both Snapshots, and every Statistic are fiber-local, so
    nothing crosses to a sibling except through the runtime we are testing."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            snapshot_oracle(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE checks slot per worker (wid-indexed, single-writer-per-slot => race-free
    # even GIL-off).  Allocated here where H.funcs is known.
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("tracemalloc.Snapshot conservation checks (single-owner, all passed "
          "fail-fast): {0}; ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing arm actually reduced snapshots.
    H.check(checks > 0,
            "no Snapshot conservation checks ran -- the grouping/filter/diff/"
            "pickle reduction hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-_group_by/sort/pickle.
    H.require_no_lost("tracemalloc snapshot conservation")


if __name__ == "__main__":
    harness.main(
        "p615_tracemalloc_snapshot_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="tracemalloc's global tracer is not single-owner, but the "
                 "Snapshot it produces is: a frozen trace tuple whose "
                 "statistics()/filter_traces()/compare_to()/pickle reductions are "
                 "pure functions.  LOAD-BEARING: each fiber fabricates its own "
                 "fiber-local trace tuple with KNOWN sizes/domains/frames and "
                 "asserts a closed-world conservation law -- grouped sizes sum to "
                 "the total, domain filters partition exactly, diffs telescope, "
                 "the frozen tuple + statistics are stable across a yield, and a "
                 "pickle round-trip reproduces the reduction.  A byte "
                 "created/destroyed by the grouping RMW, a mispartition, a "
                 "non-telescoping diff, or a torn frozen tuple is the runtime bug")
