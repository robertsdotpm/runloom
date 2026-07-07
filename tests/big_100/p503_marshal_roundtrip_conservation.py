"""big_100 / 503 -- marshal.dumps/loads round-trip conservation under M:N.

marshal is CPython's internal object-serialization codec (the .pyc format).
marshal.dumps(value, version) walks the object graph in C, writing it into a
per-call WFILE scratch struct.  At version >= 3 the WFILE additionally carries a
REFERENCE TABLE (a _Py_hashtable keyed by object identity): the first time a
shared/interned object is emitted it is assigned an incrementing ref index and
written in full; every LATER occurrence of that same object is emitted as a
compact TYPE_REF back-reference to that index (this is the FLAG_REF path, so a
.pyc stays small and structure-sharing round-trips).  loads() replays the stream,
rebuilding the ref table so a TYPE_REF resolves back to the object created at that
index -- reconstructing the ORIGINAL sharing (identity) of the graph.

WHERE M:N COULD BREAK IT (the gap this program probes).  The dumps() ref table
and the loads() ref array are the interesting per-call scratch.  If any of that
scratch were keyed off the hub's PyThreadState (rather than being a clean per-call
stack/heap allocation owned by the calling fiber), then a sibling fiber that runs
its OWN dumps()/loads() on a different graph while THIS fiber is parked across a
hub migration -- between building its bytes and decoding them -- could corrupt
this fiber's ref indices.  The visible symptom would be:
  * a TYPE_REF resolving to the WRONG earlier object, so loads() rebuilds a graph
    that no longer equals the original (a value corruption), or
  * dumps() emitting different bytes for the SAME graph on either side of a yield
    (a non-deterministic ref-index assignment -- torn scratch), or
  * a recovered leaf carrying a SIBLING's wid (a cross-fiber object leak).

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Every fiber owns its ENTIRE value graph
(built fresh in a fiber-local variable, never shared) and every leaf is tagged
with the fiber's wid + idx (the tag string literally embeds "W{wid}_I{idx}"), so
no two fibers can ever construct equal graphs.  The graph deliberately REUSES a
handful of objects at several positions (a shared tuple, a shared inner dict, the
shared tag string) so their refcount exceeds 1 -- which is exactly what makes
marshal's w_ref() emit FLAG_REF back-references at version >= 3.  The oracle is a
strict single-owner CONSERVATION law:

  (1) DETERMINISM across the hazard boundary: dumps(graph) called BEFORE a yield
      and AGAIN AFTER the yield must produce BYTE-IDENTICAL output (marshal is
      deterministic for a fixed object graph; the ref indices are assigned in a
      fixed traversal order).  A difference means the WFILE ref-table scratch was
      disturbed by a sibling while this fiber was parked -- the torn-scratch bug.

  (2) VALUE round-trip: loads(dumps(graph)) == graph, value-for-value (dicts and
      frozensets compare order-independently, so this is a true structural law).

  (3) wid PROVENANCE: the recovered graph's known wid-bearing leaves each equal
      THIS fiber's wid (a recovered leaf holding a sibling's wid is a cross-fiber
      object leak -- impossible under a correct runtime because the graph is
      single-owner).

  (4) FLAG_REF identity reconstruction (version >= 3 only): the reused object that
      appears at several positions of the graph must come back as the SAME object
      (rec_a IS rec_b) -- proving the TYPE_REF back-reference resolved to the
      correct ref-table index and not a sibling's.  At version 2 (no FLAG_REF) the
      occurrences are independent copies, so identity is NOT asserted there.

  Versions are round-robined 2 vs 4 by (wid+idx) so BOTH the no-ref path
  (version 2, every object emitted in full) and the FLAG_REF back-ref path
  (version 4) are exercised across the run.

Single-owner: the graph, its bytes, and every intermediate are fiber-local; there
is no shared mutable object anywhere, so ANY cross-fiber observation (non-
deterministic dumps, a broken round-trip, a sibling's wid, a mis-resolved back-
ref) is a genuine runloom object/scratch-isolation bug, never documented Python
semantics.  On a correct runtime the oracle PASSES (program exits 0).

ORACLES:
  * LOAD-BEARING -- MARSHAL ROUND-TRIP CONSERVATION (worker, HARD, fail-fast):
    checks (1)-(4) above, per fiber, per iteration.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    corrupted loads() (or parked and never re-woken) never returns; caught.
  * NON-VACUITY (post, HARD): rt_checks > 0 -- the round-trip arm actually ran.

FAIL ON: dumps() non-determinism across a yield, loads(dumps(g)) != g, a
recovered leaf carrying the wrong wid, or a FLAG_REF back-ref that fails to
reconstruct shared identity at version >= 3.

Stresses: marshal.dumps/loads WFILE + RFILE per-call scratch, the version>=3
FLAG_REF object-reference table (w_ref hashtable on write, ref array on read),
nested tuple/list/dict/frozenset serialization, structure-sharing reconstruction,
all across a hub migration + yield while the fiber holds its bytes single-owner.

Good TSan / controlled-M:N-replay target: marshal's ref-table scratch is walked
per-call; a data-race report on it, or a replay that assigns a ref index while a
sibling's dumps() is mid-traversal, localizes a torn back-reference before the
value/identity conservation law even closes.
"""
import marshal

import harness
import runloom

# Number of member "num" leaves per graph -- enough nesting that the marshal
# traversal is non-trivial and the ref table has real work at version 4.
NUMS = 6


def build_graph(wid, idx):
    """Build a DISTINCT, fiber-local, wid-tagged value graph.

    The graph is nested tuple/list/dict/frozenset over int/str/float/bool/None.
    Three objects are REUSED at multiple positions -- a shared tuple, a shared
    inner dict, and the shared tag string -- so their refcount exceeds 1 while
    they sit in the graph.  That is exactly the condition under which marshal's
    w_ref() emits a FLAG_REF back-reference at version >= 3, so the version-4
    dumps exercises the object-reference table (and loads must reconstruct the
    shared identity).

    Returns (graph, tag, shared_tuple, inner) so the checker can probe the known
    wid-bearing leaves and the shared-identity positions after the round trip."""
    # Tag embeds wid + idx, so no two fibers (nor two iterations) can build equal
    # graphs -- a recovered tag carrying a different wid is a hard cross-fiber leak.
    tag = "W{0}_I{1}_MARSHALTAG".format(wid, idx)
    base = wid * 100003 + idx

    # Reused objects (refcount > 1 in the graph -> FLAG_REF at version >= 3).
    shared_tuple = (wid, tag, base * 7 + 1)
    inner = {
        "wid": wid,
        "idx": idx,
        "tag": tag,                        # reuses the shared tag string
        "nums": [base + i for i in range(NUMS)],
        "shared_a": shared_tuple,          # first occurrence of shared_tuple
        "shared_b": shared_tuple,          # reused -> back-ref at version 4
        "fs": frozenset({base, base + 1, base + 2, base + 3}),
        "flag": True,
        "none": None,
        "f": float(base),
    }

    graph = (
        wid,                               # graph[0] -- wid leaf
        tag,                               # graph[1] -- wid-bearing tag
        inner,                             # graph[2] -- first occurrence of inner
        [shared_tuple, shared_tuple, tag], # graph[3] -- reuses shared_tuple + tag
        {"nested": inner,                  # graph[4]["nested"] reuses inner
         "wid": wid,
         "pair": (tag, wid),
         "empty": ()},
    )
    return graph, tag, shared_tuple, inner


# Sustained round-trip checks per worker, bounded by H.running().  The isolation
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# dumping/loading distinct graphs while sleep-PARKED across their yield, so the
# scheduler reliably interleaves a sibling's marshal call before this fiber
# resumes.  A single check per fiber barely overlaps a sibling's and does NOT
# reproduce a scratch-sharing bug.
INNER_CAP = 100000


def rt_check(H, wid, idx, state):
    """Single-owner marshal round-trip conservation check.

    Builds a fiber-local wid-tagged graph, dumps it (before AND after a yield to
    probe scratch determinism), decodes it, and asserts value + wid-provenance +
    (at version >= 3) shared-identity reconstruction.  Every object is fiber-
    local, so any failure is a runtime object/scratch-isolation bug."""
    graph, tag, shared_tuple, inner = build_graph(wid, idx)
    # Round-robin the no-ref (v2) and FLAG_REF (v4) paths so both are exercised.
    version = 4 if ((wid + idx) & 1) else 2

    # dumps() BEFORE the hazard boundary.
    data_before = marshal.dumps(graph, version)

    # YIELD across the hazard boundary: the fiber parks (possibly migrating hubs)
    # while holding its own bytes; siblings run their own marshal calls here.  If
    # marshal's WFILE/RFILE scratch is keyed off the hub PyThreadState, a sibling
    # dumping/loading now would corrupt this fiber's ref indices.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # dumps() AFTER the yield -- must be byte-identical (marshal is deterministic
    # for a fixed graph; ref indices are assigned in a fixed traversal order).
    data_after = marshal.dumps(graph, version)
    if data_after != data_before:
        H.fail("marshal.dumps NON-DETERMINISTIC across a yield: version {0} "
               "produced {1} bytes before the yield and {2} bytes after for the "
               "SAME fiber-local graph (wid {3}) -- the WFILE ref-table scratch "
               "was disturbed by a sibling marshal call while this fiber was "
               "parked (torn per-call scratch)".format(
                   version, len(data_before), len(data_after), wid))
        return

    # VALUE round-trip: loads(dumps(g)) == g, value-for-value.
    rec = marshal.loads(data_before)
    if rec != graph:
        H.fail("marshal round-trip BROKEN: loads(dumps(graph)) != graph at "
               "version {0} (wid {1}) -- a leaf, container, or FLAG_REF back-"
               "reference decoded to the wrong value, so the recovered graph "
               "does not equal this fiber's single-owner original".format(
                   version, wid))
        return

    # wid PROVENANCE: the known wid-bearing leaves must each carry THIS fiber's
    # wid/tag.  (Value equality above already implies this because the tag embeds
    # wid+idx and is unique per fiber; these explicit probes make a cross-fiber
    # leak unmistakable and independent of the structural compare.)
    rec_inner = rec[2]
    if rec[0] != wid or rec[1] != tag or rec[3][2] != tag:
        H.fail("marshal recovered a CROSS-FIBER leaf: top-level wid/tag do not "
               "match (got wid={0!r} tag={1!r}, expected wid={2} tag={3!r}) -- "
               "a sibling's marshalled object bled into this fiber's graph".format(
                   rec[0], rec[1], wid, tag))
        return
    if (rec_inner["wid"] != wid or rec_inner["idx"] != idx
            or rec_inner["tag"] != tag):
        H.fail("marshal recovered a CROSS-FIBER inner leaf: inner wid/idx/tag "
               "mismatch (got wid={0!r} idx={1!r}, expected wid={2} idx={3}) -- "
               "a sibling's inner dict leaked into this fiber's graph".format(
                   rec_inner["wid"], rec_inner["idx"], wid, idx))
        return
    if rec_inner["shared_a"] != shared_tuple or rec[3][0] != shared_tuple:
        H.fail("marshal recovered a wrong shared tuple: expected {0!r} (wid {1}) "
               "-- the reused object decoded to a different value across the "
               "round trip".format(shared_tuple, wid))
        return

    # FLAG_REF identity reconstruction (version >= 3 only): the reused objects
    # must come back as the SAME object, proving the TYPE_REF back-reference
    # resolved to the correct ref-table index (not a sibling's).  At version 2
    # there is no FLAG_REF, so the occurrences are independent copies and identity
    # is NOT asserted.
    if version >= 3:
        if rec_inner["shared_a"] is not rec_inner["shared_b"]:
            H.fail("marshal FLAG_REF identity BROKEN at version {0}: the reused "
                   "shared tuple decoded to two DISTINCT objects (shared_a is "
                   "not shared_b) for wid {1} -- a TYPE_REF back-reference failed "
                   "to reconstruct the graph's structure sharing".format(
                       version, wid))
            return
        if rec_inner["shared_a"] is not rec[3][0] or rec[3][0] is not rec[3][1]:
            H.fail("marshal FLAG_REF identity BROKEN at version {0}: the shared "
                   "tuple is not identity-unified across the inner dict and the "
                   "list positions for wid {1} -- back-references resolved to "
                   "distinct ref-table indices".format(version, wid))
            return
        # The reused inner dict must also unify (graph[2] IS graph[4]["nested"]).
        if rec[2] is not rec[4]["nested"]:
            H.fail("marshal FLAG_REF identity BROKEN at version {0}: the reused "
                   "inner dict decoded to two DISTINCT objects for wid {1} -- the "
                   "dict-level back-reference did not reconstruct".format(
                       version, wid))
            return

    state["rt_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber sustains the single-owner marshal round-trip conservation check
    (fail-fast).  No shared state anywhere -- the graph, its bytes, and every
    intermediate are fiber-local -- so the hub stays busy with mixed marshal churn
    without any object reaching a sibling's oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            rt_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "rt_checks": [0] * 1024,           # LOAD-BEARING single-owner checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rchecks = sum(H.state["rt_checks"])
    H.log("marshal[single-owner LOAD-BEARING]: {0} round-trip conservation "
          "checks (dumps-determinism + value round-trip + wid-provenance + "
          "FLAG_REF identity, all passed fail-fast); ops={1}".format(
              rchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip arm actually ran.
    H.check(rchecks > 0,
            "no marshal round-trip checks ran -- the load-bearing marshal "
            "round-trip conservation hazard was never exercised (oracle would "
            "be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # corrupted loads() or lost across a hub migration mid-round-trip).
    H.require_no_lost("marshal round-trip conservation")


if __name__ == "__main__":
    harness.main(
        "p503_marshal_roundtrip_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="marshal.dumps(version>=3) builds a per-call WFILE with an "
                 "object-reference table (FLAG_REF back-refs for reused/interned "
                 "objects); loads() replays it, rebuilding the graph's structure "
                 "sharing.  Under M:N, if that per-call ref-table scratch were "
                 "keyed off the hub PyThreadState, a sibling dumping/loading mid-"
                 "park could corrupt this fiber's ref indices.  LOAD-BEARING: "
                 "each fiber marshals its OWN wid-tagged nested tuple/list/dict/"
                 "frozenset graph (one object reused so FLAG_REF fires), yields, "
                 "and asserts (1) dumps is byte-identical across the yield, (2) "
                 "loads(dumps(g))==g, (3) every recovered leaf carries this "
                 "fiber's wid, and (4) at version>=3 the reused object comes back "
                 "as the SAME object.  Versions 2 (no-ref) and 4 (FLAG_REF) are "
                 "round-robined by wid.  Any cross-yield non-determinism, broken "
                 "round-trip, wrong wid, or mis-resolved back-ref is the runloom "
                 "marshal-scratch isolation bug")
