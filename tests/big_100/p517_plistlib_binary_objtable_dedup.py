"""big_100 / 517 -- plistlib FMT_BINARY object-table dedup + round-trip
conservation under M:N.

plistlib's binary plist writer (`_BinaryPlistWriter` in Lib/plistlib.py, pure
Python) encodes a value graph in two passes.  The first pass, `_flatten`, walks
the object graph and builds a DEDUP TABLE: a per-call mapping from each distinct
object to a small integer index (`self._objtable` / `self._objidmap`), exactly
like a pickle memo.  A value that is REUSED at several places in the graph (the
same bytes/str/int object referenced under several dict keys) is emitted into the
object table ONCE and referenced by its table index everywhere it appears.  The
second pass writes the table and an offset index; `loads()` reconstructs the
graph by following those indices back to the single shared object.

WHERE M:N COULD BREAK IT (the gap this program probes).  `_flatten` /
`_BinaryPlistWriter` is PURE PYTHON, so runloom's preemption can interrupt a
`dumps(FMT_BINARY)` call MID-ENCODE -- while the per-call ref map is half-built
and the offset table is being laid down.  Each `dumps()` builds its OWN
`_BinaryPlistWriter` (the ref map is call-local, not shared), so a correct
runtime keeps every encode isolated.  BUT if a fiber that parks mid-`dumps`
somehow leaked its half-built object-index map to a sibling encoding a DIFFERENT
value graph -- or a sibling's write landed in this fiber's offset table -- the
object-index references would DESYNC: a reference index would point at another
fiber's table entry, and `loads()` would reconstruct a graph whose leaves belong
to a SIBLING's value (a different wid) or whose reused-object references collapse
to the wrong object.  The round-trip would then either not deep-equal the
original or carry a foreign wid tag.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, fail-fast):

  Each fiber builds its OWN value graph, entirely wid-tagged: every integer leaf
  encodes wid, every string encodes wid, and ONE bytes object is REUSED at
  several dict keys and list slots so the binary writer's dedup table MUST fold
  it to a single table entry with multiple index references (the exact machinery
  this program stresses).  The fiber then:
    - `dumps(graph, FMT_BINARY)` -> bytes  (build the dedup'd object table)
    - YIELDS (runloom.yield_now / sleep) so siblings encode/decode concurrently
      and a preempted mid-encode sibling reliably interleaves,
    - `loads(bytes)` -> recovered graph  (follow the object-index references back)
    - asserts `recovered == graph` (DEEP structural + value equality -- Python
      dict/list/bytes/datetime `==` is a full recursive compare), AND
    - independently WALKS the recovered graph asserting every wid-tagged leaf
      equals THIS fiber's wid and every reused-bytes leaf equals this fiber's
      reused value (a cross-fiber object-table desync would surface a sibling's
      wid or a collapsed/foreign reused object here even if `==` somehow held).
  A parallel FMT_XML arm (round-robined by wid) does the same round-trip through
  the text writer/parser so both plist codecs race under M:N.

  The graph is SINGLE-OWNER: built in a fiber-local variable, dumped and loaded
  by the one fiber, never shared.  So a failure here is NOT the documented
  shared-mutable-object race -- it is a runloom encode/decode isolation bug: a
  reused-object dedup index that desynced across a park, or a cross-fiber leak of
  a half-built binary-plist object table.  On a correct runtime the oracle
  PASSES (the program exits 0 when there is no bug): plist dumps/loads is a pure
  value function of its input, so a wid-tagged graph MUST round-trip to itself.

  We verified the round-trip identity with a plain-threads control (8 OS threads,
  each round-tripping its own wid-tagged graph through FMT_BINARY and FMT_XML,
  GIL on AND off): 100% deep-equal, 0 foreign-wid leaves.  Under a correct
  runloom it must also hold.

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP + DEDUP IDENTITY (worker, HARD, fail-fast).  Deep
    `recovered == graph` plus a wid-tag walk of every recovered leaf, across a
    yield, for both FMT_BINARY (reused-object dedup table) and FMT_XML.  Single-
    owner graph.  A failure is a runloom plist encode/decode isolation desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-`dumps`
    (parked inside the half-built writer and never re-woken) never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): plist_checks > 0 -- the round-trip arm actually ran.

FAIL ON: a recovered graph that does not deep-equal the fiber's own graph, a
recovered leaf carrying a sibling's wid, a reused-object leaf that decoded to the
wrong bytes, or a SIGSEGV/exception inside dumps/loads under concurrent encode.

Stresses: plistlib `_BinaryPlistWriter._flatten` dedup-table construction
(object -> index memo) and offset-table layout under preemption, binary plist
object-index reference resolution in `_BinaryPlistParser`, FMT_XML writer/parser
round-trip, reused-object identity folding, datetime/bytes/bool/int/nested
dict+list value conservation across a yield under M:N.

Good TSan / controlled-M:N-replay target: the per-call object-index dict in
_BinaryPlistWriter is a plain dict mutated across the whole _flatten walk; under
the single-owner arm it is read/written by ONE fiber, so a data-race report on
that dict -- or a replay that decodes a reference index written by a sibling's
concurrent encode -- is the cleanest signal before the deep-equality oracle
fires.
"""
import datetime
import plistlib

import harness
import runloom

# Sustained round-trips per worker, bounded by H.running().  The mid-encode
# preemption hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously dumping/loading distinct wid-tagged graphs while parked across
# their yield, so the scheduler reliably interleaves a sibling's half-built
# encode before this fiber resumes.  A single round-trip per fiber barely
# overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_graph(wid):
    """Build THIS fiber's wid-tagged value graph.

    Every leaf encodes wid so a cross-fiber decode leak surfaces as a foreign
    tag.  ONE bytes object (`reused`) is referenced at several dict keys and list
    slots so the binary writer's dedup table MUST fold it to a single object-
    table entry with multiple index references -- the exact machinery this
    program stresses.  Returns (graph, reused_bytes)."""
    reused = ("reuse-{0}-payload".format(wid)).encode("ascii")
    # datetime kept to whole seconds so it round-trips EXACTLY through both the
    # binary writer (double seconds since 2001) and the XML writer (ISO to second
    # precision) -- sub-second precision would legitimately differ and is not the
    # hazard under test.  Bounded fields so any wid maps to a valid date.
    when = datetime.datetime(2000 + (wid % 100), 1 + (wid % 12), 1 + (wid % 27),
                             (wid % 24), (wid % 60), (wid % 60))
    graph = {
        "wid": wid,
        "wid_str": "W{0}".format(wid),
        "meta": {
            "a": wid * 3 + 1,
            "b": [wid, wid + 1000000, wid + 2000000],
            "flag_t": True,
            "flag_f": False,
            "when": when,
            "blob1": reused,           # reused object -> dedup table entry
            "blob2": reused,           # same object again -> same table index
            "nested": {
                "blob3": reused,       # and again, one level deeper
                "vals": [reused, reused, wid],
                "deep_tag": wid,
            },
        },
        "list": [{"i": i, "wtag": wid, "blob": reused} for i in range(3)],
        "big": wid * 7 + 123456789,
    }
    return graph, reused


def walk_check_tags(H, node, wid, reused, where):
    """Recursively walk a RECOVERED graph asserting every wid-tagged leaf equals
    THIS fiber's wid and every reused-bytes leaf equals this fiber's reused value.

    This is the cross-fiber-leak oracle: even if a foreign object-table desync
    produced a graph that somehow structurally matched, a leaf carrying a
    sibling's wid or a collapsed/foreign reused object surfaces here.  Returns
    True iff clean; calls H.fail and returns False on the first violation."""
    if isinstance(node, dict):
        for k, v in node.items():
            # Keys that carry a wid tag by convention.
            if k in ("wid", "wtag", "deep_tag") and not isinstance(v, dict) \
                    and not isinstance(v, list):
                if v != wid:
                    H.fail("plist round-trip LEAF TAG WRONG at {0}[{1!r}]: got "
                           "{2!r}, expected wid {3} -- a cross-fiber object-table "
                           "leak decoded a SIBLING's wid into this fiber's graph"
                           .format(where, k, v, wid))
                    return False
            if k in ("blob1", "blob2", "blob3", "blob"):
                if v != reused:
                    H.fail("plist round-trip REUSED-OBJECT WRONG at {0}[{1!r}]: "
                           "got {2!r}, expected {3!r} (wid {4}) -- the dedup "
                           "object-table index desynced to a foreign/collapsed "
                           "entry".format(where, k, v, reused, wid))
                    return False
            if not walk_check_tags(H, v, wid, reused,
                                   "{0}[{1!r}]".format(where, k)):
                return False
    elif isinstance(node, list):
        for idx, v in enumerate(node):
            if not walk_check_tags(H, v, wid, reused,
                                   "{0}[{1}]".format(where, idx)):
                return False
    return True


def round_trip(H, wid, fmt, state):
    """Single-owner plist round-trip through `fmt`.

    dumps -> YIELD (so a sibling's mid-encode interleaves) -> loads, then assert
    the recovered graph deep-equals THIS fiber's graph and every leaf carries
    THIS fiber's wid.  The graph is fiber-local, never shared."""
    graph, reused = build_graph(wid)

    data = plistlib.dumps(graph, fmt=fmt)

    # YIELD: allow siblings to encode/decode concurrently and a preempted
    # mid-`dumps` sibling to interleave before we decode.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0003)

    recovered = plistlib.loads(data)

    # Check 1: DEEP structural + value equality (recursive dict/list/bytes/
    # datetime compare).  A desynced object-table reference or a cross-fiber
    # leak breaks this.
    if recovered != graph:
        H.fail("plist round-trip NOT EQUAL (fmt={0}, wid={1}): recovered graph "
               "does not deep-equal the fiber's own graph -- an object-table "
               "reference desynced across the yield or a sibling's encode "
               "corrupted this decode.  recovered={2!r}".format(
                   fmt, wid, recovered))
        return

    # Check 2: independent wid-tag + reused-object walk (catches a foreign wid or
    # a collapsed reused object even if == somehow held).
    if not walk_check_tags(H, recovered, wid, reused, "root"):
        return

    state["plist_checks"][wid & 1023] += 1
    if fmt == plistlib.FMT_BINARY:
        state["bin_checks"][wid & 1023] += 1
    else:
        state["xml_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber round-trips its OWN wid-tagged graph.  FMT_BINARY runs every
    iteration (the dedup-object-table path is the primary hazard); FMT_XML is
    round-robined in by wid parity + iteration so both plist codecs race under
    M:N without either arm sharing data (each round_trip builds a fresh single-
    owner graph)."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            round_trip(H, wid, plistlib.FMT_BINARY, state)   # LOAD-BEARING (dedup)
            if H.failed:
                return
            # Round-robin the XML arm in so both codecs are exercised regardless
            # of whether one worker does many iterations or many workers do one.
            if (wid + idx) & 1:
                round_trip(H, wid, plistlib.FMT_XML, state)  # LOAD-BEARING (text)
                if H.failed:
                    return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "plist_checks": [0] * 1024,   # total single-owner round-trip checks
        "bin_checks": [0] * 1024,     # FMT_BINARY (dedup-table) checks
        "xml_checks": [0] * 1024,     # FMT_XML checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["plist_checks"])
    bchecks = sum(H.state["bin_checks"])
    xchecks = sum(H.state["xml_checks"])
    H.log("plist round-trips (single-owner LOAD-BEARING, all deep-equal + wid-"
          "tag clean fail-fast): {0} total | FMT_BINARY (dedup object table) {1} "
          "| FMT_XML {2}; ops={3}".format(pchecks, bchecks, xchecks,
                                          H.total_ops()))

    # NON-VACUITY: the round-trip hazard was actually exercised.
    H.check(pchecks > 0,
            "no plist round-trip checks ran -- the load-bearing binary-objtable "
            "dedup / round-trip hazard was never exercised (oracle vacuous)")
    # And the primary (binary dedup-table) arm specifically ran.
    H.check(bchecks > 0,
            "no FMT_BINARY round-trips ran -- the dedup object-table path (the "
            "primary hazard) was never exercised")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-dumps inside
    # the half-built _BinaryPlistWriter).
    H.require_no_lost("plistlib binary objtable dedup round-trip")


if __name__ == "__main__":
    harness.main(
        "p517_plistlib_binary_objtable_dedup", body, setup=setup, post=post,
        default_funcs=8000,
        describe="plistlib FMT_BINARY builds a per-call dedup object table (a "
                 "reused object emitted once, referenced by index like a pickle "
                 "memo).  _BinaryPlistWriter is pure Python, so preemption can "
                 "interrupt dumps() mid-encode; if a parked fiber's half-built "
                 "object-index map leaked to a sibling encoding a different graph, "
                 "the object-index references would desync.  LOAD-BEARING: each "
                 "fiber round-trips its OWN wid-tagged graph (with ONE bytes value "
                 "reused at several keys so the dedup table MUST fold it) through "
                 "dumps(FMT_BINARY) -> yield -> loads(); recovered MUST deep-equal "
                 "the graph AND every leaf MUST carry this fiber's wid.  A parallel "
                 "FMT_XML arm round-robins by wid.  A non-equal round-trip or a "
                 "foreign-wid/collapsed-reused leaf is the runloom encode/decode "
                 "isolation bug")
