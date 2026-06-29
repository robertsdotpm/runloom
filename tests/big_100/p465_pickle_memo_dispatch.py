"""big_100 / 465 -- C _pickle per-instance memo + copyreg dispatch isolation
across an M:N park.

The subjects are the C ``_pickle`` Pickler/Unpickler PER-INSTANCE memo dict +
reducer dispatch, and the ``copyreg`` MODULE-GLOBAL ``dispatch_table`` /
``_extension_registry``.  No other program in the suite drives a custom
``_pickle.Pickler`` / ``_pickle.Unpickler`` whose reduction YIELDS mid-operation
(p438 is about the protocol-5 PickleBuffer out-of-band export COUNT, a disjoint
hazard).

THE EXACT C-LEVEL STATE UNDER ATTACK.  A ``_pickle.Pickler`` instance keeps a
PER-INSTANCE memo (object-id -> memo index) so a re-seen object is emitted as a
back-reference instead of re-serialized; the matching ``_pickle.Unpickler`` keeps
a PER-INSTANCE memo (index -> reconstructed object) so a back-reference resolves
to the SAME object.  Reducer DISPATCH is partly module-global: ``copyreg.pickle``
populates ``copyreg.dispatch_table[type] = reduce_func``, and the pickler consults
that shared table (plus its own ``Pickler.dispatch_table`` if set, plus the
type's ``__reduce_ex__``) to find how to serialize a custom type.  So the memo is
PER-FIBER state that MUST NOT bleed across fibers, while the dispatch table is
SHARED read-mostly state every fiber's pickler reads concurrently.

WHY M:N MAKES IT REACHABLE.  Under runloom each fiber runs its OWN
``_pickle.Pickler``/``_pickle.Unpickler``, but many fibers share one hub OS-thread
(and its ``PyThreadState``).  If the C pickler keyed ANY of its memo / dispatch
scratch off the OS thread (a per-thread cache, a thread-local scratch buffer), or
if a fiber's reduction YIELDS mid-pickle and the scheduler runs a sibling's
pickler on the same hub before it resumes, a memo index or a reduced value could
bleed from the sibling -- the recovered graph would then be the WRONG fiber's, or
a back-reference would resolve to a sibling's object.  We make the yield real: a
custom type's reducer ``__reduce__`` calls ``runloom.yield_now()`` /
``runloom.sleep`` WHILE the pickler is mid-dump, so a sibling fiber's pickler runs
on the shared hub thread between this fiber's memo writes.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber builds a DISTINCT nested object graph tagged with its ``wid`` (a deep
  chain of custom ``Graph`` containers, each holding a per-wid ``Leaf`` plus ONE
  ``shared`` Leaf reused at every level so the memo MUST dedup it).  The fiber
  pickles the graph with its OWN ``_pickle.Pickler`` (protocol 5) and unpickles it
  with its OWN ``_pickle.Unpickler``, with the reducer YIELDING mid-dump, then
  asserts it recovered ITS OWN graph EXACTLY:
    - ``g2 == g`` (the whole tagged graph survived the round-trip);
    - ``g2.owner == wid`` at every level (no sibling's graph bled in);
    - the ``shared`` Leaf deduped to ONE object across all levels on unpickle
      (the per-instance memo back-reference resolved to a single object, not N
      copies and not a sibling's) -- and that object's owner is ``wid``.
  We verified with a standalone plain-threads control (64 threads, the SAME hazard
  incl. a reducer that sleeps mid-reduction, NO runloom) that this holds with
  PYTHON_GIL=1 AND PYTHON_GIL=0: 0 mismatches in 25600 checks each.  Stock CPython
  gives each thread its own Pickler/Unpickler (own memo), and ``copyreg``'s
  dispatch_table is read-only after the one-time registration, so the round-trip
  identity holds for ANY GIL setting -- an oracle that fired there would be a
  false-positive detector; it does NOT fire there.  Under a CORRECT runloom it
  must ALSO hold (each fiber owns its memo).  If a memo index leaks across the
  yield (a back-reference resolves to a SIBLING's object), or a wrong reducer from
  the shared dispatch_table serializes the wrong type, or the recovered owner is
  not ``wid`` -- that is the runloom bug, and the single-owner arm PASSES on a
  correct runtime (the program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP GRAPH IDENTITY across a mid-dump yield (worker,
    HARD, fail-fast).  Single-owner: each fiber owns its Pickler, Unpickler, graph,
    and shared Leaf.  ``g2 == g`` AND ``g2.owner == wid`` at every level AND the
    shared Leaf deduped to ONE wid-owned object.  A failure is a runloom per-fiber
    memo / dispatch isolation desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-dump
    (stranded inside the reducer's yield, or inside the C pickler holding a memo
    write) never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (rt_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): the SHARED ``copyreg.dispatch_table``
    register/unregister CONTENTION path.  A minority of fibers repeatedly
    register+unregister a reducer for a fiber-private throwaway type on the SHARED
    module-global ``copyreg.dispatch_table`` across a yield (``copyreg.pickle`` /
    ``del dispatch_table[type]``), then read the table back.  Because the table is
    a single SHARED dict mutated in place, a sibling's register/unregister is
    visible mid-window -- a documented shared-global-dict interleave under M:N
    (exactly like p67's threading.local / p66's contextvar leak: 0 under plain
    threads only because each OS thread serializes via the GIL or runs truly
    parallel-but-uncontended at this rate; under M:N many fibers hammer the one
    dict on one hub thread).  We MEASURE the observed-interleave rate; we do NOT
    assert on it.  It is fully separate from the load-bearing round-trip oracle
    (which registers its types ONCE in setup, single-owner, and never mutates the
    table after) so the measured contention cannot poison the oracle.  A torn /
    garbage table entry (a value that is not a callable or not one we registered)
    IS flagged as corruption -- that would be a real shared-dict tear, not a
    benign interleave.

FAIL ON: a recovered graph that is not the fiber's own (wrong owner, sibling's
object via a leaked memo back-reference, wrong reducer dispatch, shared Leaf not
deduped to one wid-owned object), or a crash.  NEVER fail on the measured
dispatch_table register/unregister interleave (report only).

Stresses: C ``_pickle.Pickler``/``_pickle.Unpickler`` per-instance memo dict +
reducer dispatch keyed off the hub PyThreadState, a reducer ``__reduce__`` that
YIELDS mid-dump so a sibling's pickler runs on the shared hub thread between memo
writes, ``copyreg`` module-global ``dispatch_table`` shared-dict mutation under
M:N, protocol-5 framing, per-instance memo back-reference identity across hub
migration + preempt-mid-dump.

Good TSan / controlled-M:N-replay target: the C pickler's memo dict writes + the
shared ``copyreg.dispatch_table`` dict reads/writes happen across hubs while a
reducer is parked mid-dump -- a data race on the memo or the dispatch dict, or a
replay that migrates a hub between a graph's first memo write and the
back-reference resolve, localizes the leak before the round-trip identity oracle
fires.
"""
import copyreg
import io
import pickle
import _pickle

import harness
import runloom

# Per-fiber graph depth.  Deep enough that the per-instance memo holds many
# entries (one per Graph level + the shared Leaf back-reference) so a leaked memo
# index has many opportunities to resolve to the wrong object, but bounded so a
# single round-trip stays cheap under sustained churn.
GRAPH_DEPTH = 6

# Sustained round-trips per worker, bounded by H.running().  The memo/dispatch
# leak hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# mid-dump and PARKED across their reducer's yield, so the scheduler reliably runs
# a sibling's pickler on the shared hub thread before this fiber resumes.  A single
# round-trip per fiber barely overlaps a sibling's and does NOT reproduce.  So each
# worker runs a sustained internal loop (one round-trip per iteration) until the
# deadline (H.running()) or INNER_CAP.  Bounding by H.running() makes the
# load-bearing oracle fire at the DEFAULT --rounds 1.
INNER_CAP = 100000

# Fraction of workers assigned to the MEASURED dispatch_table contention arm.
# Small: a minority amply demonstrates the documented shared-dict interleave
# without dominating the population or starving the load-bearing arm.
MEASURED_FRACTION = 0.1


# --------------------------------------------------------------------------
# Custom types whose reducers are registered ONCE via copyreg.pickle in setup()
# (single-owner registration: the load-bearing arm never mutates the dispatch
# table after this).  Both reducers YIELD mid-reduction so a sibling fiber's
# pickler runs on the shared hub thread while this fiber is parked mid-dump --
# the exact window a memo/dispatch leak would corrupt.
# --------------------------------------------------------------------------
class Leaf(object):
    """A leaf tagged with its OWNER fiber's wid.  Reused (the `shared` leaf) at
    every graph level so the per-instance memo MUST dedup it to ONE object on
    unpickle -- a leaked memo index would resolve the back-reference to the wrong
    (sibling's) object."""

    __slots__ = ("owner", "payload")

    def __init__(self, owner, payload):
        self.owner = owner
        self.payload = payload

    def __eq__(self, o):
        return (isinstance(o, Leaf) and self.owner == o.owner
                and self.payload == o.payload)

    def __hash__(self):
        return hash((self.owner, self.payload))


class Graph(object):
    """A nested container tagged with its OWNER fiber's wid.  A deep chain of
    these (each holding a per-level Leaf plus the ONE shared Leaf) is the
    distinct, wid-tagged object graph each fiber round-trips."""

    __slots__ = ("owner", "leaves", "child")

    def __init__(self, owner, leaves, child=None):
        self.owner = owner
        self.leaves = leaves
        self.child = child

    def __eq__(self, o):
        return (isinstance(o, Graph) and self.owner == o.owner
                and self.leaves == o.leaves and self.child == o.child)


def reduce_leaf(leaf):
    """Reducer registered via copyreg.pickle(Leaf, reduce_leaf).  YIELDS mid-
    reduction so the shared dispatch_table lookup + this fiber's per-instance memo
    write are exercised WHILE a sibling fiber is also mid-pickle on the hub."""
    runloom.yield_now()
    return (Leaf, (leaf.owner, leaf.payload))


def reduce_graph(g):
    """Reducer registered via copyreg.pickle(Graph, reduce_graph).  YIELDS (a
    sleep-park on a coin flip) mid-reduction so this fiber is descheduled long
    enough that the scheduler runs a sibling's pickler mid-dump before we resume --
    the cadence that reproduces a memo/dispatch leak if one exists."""
    if (g.owner + len(g.leaves)) & 1:
        runloom.sleep(0.0002)
    else:
        runloom.yield_now()
    return (Graph, (g.owner, g.leaves, g.child))


def build_graph(wid):
    """Build THIS fiber's distinct, wid-tagged nested graph.  The same `shared`
    Leaf is placed at every level, so a correct per-instance memo dedups it to ONE
    object on unpickle; the returned `shared` is the identity the oracle checks the
    recovered back-reference against."""
    shared = Leaf(wid, ("shared", wid))
    g = None
    for d in range(GRAPH_DEPTH):
        g = Graph(wid, [shared, Leaf(wid, ("lvl", wid, d))], g)
    return g, shared


def roundtrip(H, wid, state):
    """LOAD-BEARING: build this fiber's wid-tagged graph, pickle it with this
    fiber's OWN _pickle.Pickler (reducer yields mid-dump), unpickle with this
    fiber's OWN _pickle.Unpickler, assert it recovered ITS OWN graph EXACTLY.
    Single-owner: only this fiber touches its Pickler/Unpickler/graph."""
    g, shared = build_graph(wid)

    # Pickle with OUR OWN Pickler instance (per-instance memo).  The reducer yields
    # mid-dump, so a sibling fiber's pickler runs on the shared hub thread here.
    buf = io.BytesIO()
    P = _pickle.Pickler(buf, protocol=5)
    P.dump(g)
    data = buf.getvalue()

    # Unpickle with OUR OWN Unpickler instance (per-instance memo index->object).
    U = _pickle.Unpickler(io.BytesIO(data))
    g2 = U.load()

    # (1) the whole tagged graph survived the round-trip.
    if g2 != g:
        H.fail("pickle round-trip CORRUPTED: recovered graph != original (wid "
               "{0}) -- a sibling fiber's memo/reducer bled into this fiber's "
               "_pickle.Pickler/Unpickler across the mid-dump yield (runloom "
               "shares the hub PyThreadState across fibers)".format(wid))
        return
    # (2) owner is OURS at every level (no sibling graph leaked in via the memo).
    node = g2
    levels = 0
    recovered_shared = None
    while node is not None:
        if node.owner != wid:
            H.fail("pickle memo LEAK: recovered Graph.owner == {0} != {1} this "
                   "fiber built (wid {1}) -- a sibling fiber's object resolved "
                   "from this fiber's Unpickler memo (cross-fiber back-reference)"
                   .format(node.owner, wid))
            return
        bref = node.leaves[0]                # the shared Leaf at this level
        if bref.owner != wid:
            H.fail("pickle memo LEAK: recovered shared Leaf.owner == {0} != {1} "
                   "(wid {1}) -- the per-instance memo back-reference resolved to "
                   "a SIBLING's object".format(bref.owner, wid))
            return
        if recovered_shared is None:
            recovered_shared = bref
        elif bref is not recovered_shared:
            # (3) the shared Leaf must dedup to ONE object across all levels: the
            # per-instance memo emitted a back-reference that resolves to a single
            # object.  Distinct objects here = the memo failed to dedup (or a
            # leaked index split it) -- a per-instance-memo isolation break.
            H.fail("pickle memo DEDUP BROKEN: the shared Leaf reused at every "
                   "level resolved to MULTIPLE distinct objects on unpickle (wid "
                   "{0}) -- the per-instance Unpickler memo back-reference did not "
                   "resolve to ONE object (memo index leaked/torn under M:N)"
                   .format(wid))
            return
        node = node.child
        levels += 1
    # (4) the graph had the depth we built (the reducer actually ran the full
    # chain, so the oracle is non-vacuous for this round-trip).
    if levels != GRAPH_DEPTH:
        H.fail("pickle round-trip SHAPE wrong: recovered {0} levels != {1} built "
               "(wid {2}) -- the reducer dispatch truncated/extended this fiber's "
               "graph (wrong reducer from the shared dispatch_table?)".format(
                   levels, GRAPH_DEPTH, wid))
        return
    state["rt_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: the SHARED copyreg.dispatch_table register/unregister contention
# path.  Report-ONLY, NEVER fails (except on a TORN/garbage entry, which is a real
# shared-dict tear).  copyreg.dispatch_table is a single module-global dict mutated
# in place; a sibling's register/unregister is visible mid-window under M:N -- a
# documented shared-global-dict interleave (like p67/p66).  We measure the
# interleave rate; we do NOT assert on it.  It is fully separate from the
# load-bearing round-trip oracle (which registers its types ONCE in setup and never
# mutates the table after), so the measured contention cannot poison the oracle.
# --------------------------------------------------------------------------
def dispatch_check(H, wid, idx, state):
    # A fiber-private throwaway type registered+unregistered on the SHARED
    # module-global dispatch_table across a yield.  We never pickle through it
    # (so it cannot touch the load-bearing arm); we only observe the shared dict.
    priv_type = state["priv_types"][wid & 1023]
    reducer = state["priv_reducer"]
    copyreg.pickle(priv_type, reducer)           # mutate the SHARED global dict
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)
    state["disp_checks"][wid & 1023] += 1
    # Observe the shared table.  Under M:N a sibling may have unregistered OUR type
    # (its priv_type differs, but the dict is shared and mutated concurrently), or
    # our own entry may be present -- either way the value, IF present, must be a
    # callable we registered, never garbage.  A garbage/torn value is a real
    # shared-dict tear -> corruption.
    got = copyreg.dispatch_table.get(priv_type, "absent")
    if got != "absent":
        if not callable(got):
            H.fail("copyreg.dispatch_table CORRUPTION: entry for a registered "
                   "type is not callable ({0!r}, wid {1}) -- the shared module-"
                   "global dispatch dict is torn under M:N".format(got, wid))
            return
        if got is not reducer:
            # A different callable than the one we registered would mean a sibling's
            # entry aliased our key -- count as an observed interleave (report only;
            # the keys are distinct per wid so this is rare/benign, not a tear).
            state["disp_interleave"][wid & 1023] += 1
    else:
        # Our entry vanished before we read it back: a sibling's concurrent
        # mutation of the shared dict reordered with our register -- a documented
        # shared-global-dict interleave under M:N.  Report only.
        state["disp_interleave"][wid & 1023] += 1
    # Clean up our entry (best-effort; a sibling may already have removed it).
    try:
        del copyreg.dispatch_table[priv_type]
    except KeyError:
        pass


def worker(H, wid, rng, state):
    """LOAD-BEARING worker: sustains a round-trip-identity churn loop bounded by
    H.running().  A minority of fibers (by wid) ALSO run the MEASURED dispatch_table
    contention arm; the two do not interact -- the load-bearing arm registers its
    types once in setup and pickles through them, while the measured arm only
    mutates+reads the shared dict with throwaway private types it never pickles
    through -- so running both keeps the hub busy with mixed pickler + dispatch
    churn without the measured contention reaching the round-trip oracle."""
    do_measured = (wid % state["measured_mod"]) == 0 if state["measured_mod"] else False
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip(H, wid, state)            # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            if do_measured:
                dispatch_check(H, wid, idx, state)   # MEASURED (report only)
                if H.failed:
                    return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Single-owner, ONE-TIME registration of the load-bearing custom types on the
    # SHARED copyreg.dispatch_table.  After this the load-bearing arm never mutates
    # the table -- it only READS it (every fiber's pickler dispatches Graph/Leaf
    # through these stable entries), so the read-mostly dispatch is exactly the
    # stock-CPython condition the plain-threads control verified clean.
    copyreg.pickle(Leaf, reduce_leaf)
    copyreg.pickle(Graph, reduce_graph)

    # Sanity (single-owner, race-free): a graph round-trips correctly in isolation,
    # so the hazard is real and the oracle is not vacuous before any fiber runs.
    g, shared = build_graph(-1)
    buf = io.BytesIO()
    _pickle.Pickler(buf, protocol=5).dump(g)
    g2 = _pickle.Unpickler(io.BytesIO(buf.getvalue())).load()
    if g2 != g or g2.owner != -1 or g2.leaves[0] is not g2.child.leaves[0]:
        H.fail("setup self-test: pickle round-trip / memo dedup broken in "
               "isolation -- the test scaffold is wrong, not runloom")
        return

    nworkers = max(2, H.funcs)
    # The MEASURED arm runs on every `measured_mod`-th fiber.  measured_mod=0 (no
    # measured arm) when the fraction rounds to nothing.
    measured_mod = int(round(1.0 / MEASURED_FRACTION)) if MEASURED_FRACTION > 0 else 0

    # Fiber-private throwaway types for the MEASURED arm, one per wid slot (1024
    # slots, reused by wid&1023).  Distinct from the load-bearing Leaf/Graph so the
    # measured register/unregister churn never aliases the load-bearing entries.
    priv_types = [type("Priv{0}".format(i), (object,), {}) for i in range(1024)]

    def priv_reducer(obj):
        return (object, ())

    H.state = {
        "rt_checks": [0] * 1024,        # load-bearing round-trip checks done
        "disp_checks": [0] * 1024,      # measured dispatch-table observations
        "disp_interleave": [0] * 1024,  # measured shared-dict interleaves seen
        "measured_mod": measured_mod,
        "priv_types": priv_types,
        "priv_reducer": priv_reducer,
        "nworkers": nworkers,
    }


def body(H):
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state)


def post(H):
    rt = sum(H.state["rt_checks"])
    dchecks = sum(H.state["disp_checks"])
    dinter = sum(H.state["disp_interleave"])
    dpct = (100.0 * dinter / dchecks) if dchecks else 0.0
    H.log("pickle: round-trip graph-identity checks={0} (LOAD-BEARING, all passed "
          "fail-fast) | dispatch_table register/unregister observations={1} "
          "interleaves={2} ({3:.1f}%, documented shared-global-dict interleave "
          "under M:N -- REPORT ONLY, like p67/p66)".format(
              rt, dchecks, dinter, dpct))
    if dinter:
        H.log("note: the dispatch_table arm observed {0} shared-dict interleaves "
              "across {1} observations -- runloom hub fibers hammer the ONE "
              "module-global copyreg.dispatch_table dict on a shared hub thread, "
              "so a sibling's register/unregister is visible mid-window.  This is "
              "documented M:N shared-global-dict behavior, NOT a runloom bug, and "
              "never reaches the load-bearing round-trip oracle (which registers "
              "its types ONCE in setup and only READS the table)".format(
                  dinter, dchecks))
    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rt > 0,
            "no pickle round-trip checks ran -- the load-bearing per-instance "
            "memo / reducer-dispatch isolation hazard was never exercised (oracle "
            "would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # reducer's yield mid-dump, or in the C pickler holding a memo write).
    H.require_no_lost("pickle per-instance memo / copyreg dispatch isolation")


if __name__ == "__main__":
    harness.main(
        "p465_pickle_memo_dispatch", body, setup=setup, post=post,
        default_funcs=8000,
        describe="the C _pickle Pickler/Unpickler keep a PER-INSTANCE memo + "
                 "reducer dispatch, and copyreg has a MODULE-GLOBAL dispatch_table; "
                 "runloom shares one hub PyThreadState across fibers.  LOAD-BEARING: "
                 "each fiber builds a DISTINCT nested graph tagged with its wid (a "
                 "custom type whose reducer is registered via copyreg.pickle and "
                 "YIELDS mid-dump), pickles+unpickles it with its OWN Pickler/"
                 "Unpickler across the yield, and MUST recover ITS OWN graph "
                 "exactly -- right owner at every level + the shared Leaf deduped "
                 "to ONE wid-owned object (0 mismatches under plain threads GIL on "
                 "AND off; a leaked memo back-reference / wrong reducer is the "
                 "runloom bug).  The SHARED copyreg.dispatch_table register/"
                 "unregister interleave is documented M:N shared-dict behavior -- "
                 "measured, report-only")
