"""big_100 / 549 -- C _pickle persistent_id / persistent_load external-object
namespace isolation across an M:N park.

The subject is the C ``_pickle`` Pickler/Unpickler PERSISTENT-OBJECT protocol:
``Pickler.persistent_id(obj)`` EXTERNALIZES an object by returning an opaque id
string (a PERSID/BINPERSID opcode is emitted instead of serializing the object),
and ``Unpickler.persistent_load(pid)`` RESOLVES that id back to a live object from
a caller-supplied table.  This is disjoint from the two existing pickle programs:
p465 attacks the per-instance memo + copyreg module-global dispatch_table, and
p438 attacks the protocol-5 PickleBuffer out-of-band export COUNT.  No other
program drives persistent_id/persistent_load, whose resolution table is
FIBER-PRIVATE and whose id namespace is the load-bearing isolation boundary.

THE EXACT C-LEVEL STATE UNDER ATTACK.  Each fiber owns:
  * a FIBER-PRIVATE registry (a dict mapping key -> External object, every
    External tagged with this fiber's ``owner == wid``);
  * its OWN ``_pickle.Pickler`` subclass whose ``persistent_id`` returns a
    WID-NAMESPACED id string ("W{wid}:{key}") for every External it owns and
    ``None`` for everything else (so ordinary nodes pickle normally);
  * its OWN ``_pickle.Unpickler`` subclass whose ``persistent_load`` parses the
    id, ASSERTS the "W{wid}" namespace matches THIS fiber, and resolves ONLY from
    THIS fiber's registry.
The persistent id namespace is the isolation invariant: a fiber must only ever be
asked to resolve ITS OWN externalized ids.  ``persistent_load`` raising on an
out-of-namespace id is the hard-fail tripwire for ANY cross-fiber leak.

WHY M:N MAKES IT REACHABLE.  Under runloom each fiber runs its OWN
Pickler/Unpickler, but many fibers share one hub OS-thread (and its
``PyThreadState``).  ``persistent_id`` is a Python method the C pickler calls
mid-dump for every object; we make it YIELD (``runloom.yield_now`` /
``runloom.sleep``) WHILE the pickler is parked mid-dump, so the scheduler runs a
sibling fiber's Pickler on the SAME hub thread between this fiber's persid
emissions.  If the C pickler keyed the persistent-object scratch (the pending
PERSID buffer, a persid-in-progress flag) off the OS thread rather than the
Pickler instance, or if a sibling's persistent_id result bled into this fiber's
frame while parked, an externalized reference could resolve to a SIBLING's object
-- or this fiber's persistent_load could be handed a SIBLING's "W{other}:{key}"
id, which its namespace assertion rejects.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber builds a DISTINCT nested object graph tagged with its ``wid`` (a deep
  chain of custom ``Graph`` containers; each level references SEVERAL External
  objects drawn from THIS fiber's private registry, including ONE external reused
  at every level).  It pickles the graph with its OWN Pickler (persistent_id
  yields mid-dump, returns "W{wid}:{key}" for its Externals), unpickles with its
  OWN Unpickler (persistent_load asserts the "W{wid}" namespace and resolves from
  this fiber's registry), then asserts:
    - ``g2 == g`` (the whole tagged graph survived the round-trip);
    - ``g2.owner == wid`` at every level (no sibling graph node bled in);
    - every resolved External ``is`` the EXACT registry object this fiber owns
      (identity, not just equality) AND its ``owner == wid`` -- a resolved
      external that is a sibling's object, or a persistent_load handed an
      out-of-namespace id, is the cross-fiber leak.
  Stock CPython gives each thread its own Pickler/Unpickler with its own
  persistent_id/persistent_load bound to its own private table, so the round-trip
  identity + namespace hold for ANY GIL setting -- an oracle that fired there
  would be a false-positive detector; it does NOT fire on plain threads GIL on OR
  off (each thread resolves only its own namespaced ids).  Under a CORRECT
  runloom it must ALSO hold (each fiber owns its Pickler/Unpickler/registry).  If
  a persid resolves to a SIBLING's object, or persistent_load is handed a
  "W{other}:..." id (rejected by the namespace assertion), or the recovered owner
  is not ``wid`` -- that is the runloom bug, and the single-owner arm PASSES on a
  correct runtime (the program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP GRAPH IDENTITY + PERSID NAMESPACE across a mid-dump
    yield (worker, HARD, fail-fast).  Single-owner: each fiber owns its Pickler,
    Unpickler, registry, graph, and Externals.  ``g2 == g`` AND ``g2.owner == wid``
    at every level AND every resolved External ``is`` this fiber's registry object
    with ``owner == wid`` AND persistent_load never saw an out-of-namespace id.  A
    failure is a runloom per-fiber persistent-object isolation desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-dump
    (stranded inside persistent_id's yield, or inside the C pickler mid-PERSID)
    never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (persid_checks>0).

FAIL ON: a recovered graph that is not the fiber's own (wrong owner, a resolved
External that is a sibling's object or not the fiber's registry object, an
out-of-namespace persistent id reaching persistent_load), or a crash.  There is no
shared-mutable arm: the persistent-object table is fiber-private by construction,
so every observable is single-owner and every failure is a real leak.

Stresses: C ``_pickle.Pickler.persistent_id`` / ``Unpickler.persistent_load``
external-object externalization keyed off the hub PyThreadState, a persistent_id
that YIELDS mid-dump so a sibling's Pickler runs on the shared hub thread between
PERSID emissions, protocol-5 framing, per-fiber persistent-id namespace + private
resolution table identity across hub migration + preempt-mid-dump.

Good TSan / controlled-M:N-replay target: the C pickler's persistent-object
scratch is written while persistent_id is parked mid-dump on one hub and a
sibling's Pickler runs on the same hub -- a data race on the persid scratch, or a
replay that migrates a hub between a persid emission and its resolution, localizes
the leak before the round-trip identity / namespace oracle fires.
"""
import io
import _pickle

import harness
import runloom

# Per-fiber graph depth.  Deep enough that persistent_id is invoked many times
# per dump (several External references per level) so a leaked persid has many
# opportunities to resolve to the wrong object, but bounded so a single round-trip
# stays cheap under sustained churn.
GRAPH_DEPTH = 6

# Number of distinct External objects in each fiber's private registry.  The graph
# references all of them (plus ONE reused at every level) so persistent_id emits a
# rich set of namespaced ids per dump.
NEXTERNALS = 4

# Sustained round-trips per worker, bounded by H.running().  The persid/persload
# leak hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# mid-dump and PARKED across persistent_id's yield, so the scheduler reliably runs
# a sibling's Pickler on the shared hub thread before this fiber resumes.  A single
# round-trip per fiber barely overlaps a sibling's and does NOT reproduce.  So each
# worker runs a sustained internal loop (one round-trip per iteration) until the
# deadline (H.running()) or INNER_CAP.  Bounding by H.running() makes the
# load-bearing oracle fire at the DEFAULT --rounds 1.
INNER_CAP = 100000


class PersidLeak(Exception):
    """Raised inside persistent_load when it is handed an id OUTSIDE this fiber's
    "W{wid}" namespace -- a cross-fiber leak of an externalized reference.  Caught
    in roundtrip() and turned into H.fail (a real runloom isolation bug)."""


class External(object):
    """An object EXTERNALIZED via persistent_id (never serialized inline; emitted
    as a PERSID id and resolved via persistent_load from the owning fiber's private
    registry).  Tagged with its OWNER fiber's wid so a resolved external can be
    identity- and owner-checked."""

    __slots__ = ("owner", "key")

    def __init__(self, owner, key):
        self.owner = owner
        self.key = key

    def __eq__(self, o):
        return isinstance(o, External) and self.owner == o.owner and self.key == o.key

    def __hash__(self):
        return hash((self.owner, self.key))


class Graph(object):
    """A nested container tagged with its OWNER fiber's wid.  Each level holds a
    list of External references (from this fiber's registry) plus a plain payload,
    and a child; the deep chain is the distinct, wid-tagged graph each fiber
    round-trips.  Externals pickle as PERSID ids, so equality alone is not enough
    -- the oracle also identity-checks each resolved External against the registry."""

    __slots__ = ("owner", "refs", "payload", "child")

    def __init__(self, owner, refs, payload, child=None):
        self.owner = owner
        self.refs = refs
        self.payload = payload
        self.child = child

    def __eq__(self, o):
        return (isinstance(o, Graph) and self.owner == o.owner
                and self.refs == o.refs and self.payload == o.payload
                and self.child == o.child)


def make_registry(wid):
    """Build THIS fiber's FIBER-PRIVATE registry: key -> External(owner=wid).  The
    persistent-object resolution table; never shared with any sibling."""
    reg = {}
    for i in range(NEXTERNALS):
        key = "ext{0}".format(i)
        reg[key] = External(wid, key)
    return reg


def build_graph(wid, reg):
    """Build THIS fiber's distinct, wid-tagged nested graph.  Every level
    references a rotating subset of this fiber's Externals plus the SAME shared
    External ("ext0") reused at every level, so persistent_id emits many namespaced
    ids per dump and the round-trip must resolve each back to THIS fiber's object.
    Returns the head Graph."""
    shared = reg["ext0"]
    g = None
    for d in range(GRAPH_DEPTH):
        # Rotate which externals this level references so the persid set varies by
        # depth; always include the shared external so it recurs across the dump.
        a = reg["ext{0}".format(1 + (d % (NEXTERNALS - 1)))]
        b = reg["ext{0}".format(1 + ((d + 1) % (NEXTERNALS - 1)))]
        g = Graph(wid, [shared, a, b], ("lvl", wid, d), g)
    return g


def make_pickler(wid, reg, buf):
    """This fiber's OWN Pickler.  persistent_id externalizes THIS fiber's Externals
    as "W{wid}:{key}" ids (yielding mid-dump so a sibling's Pickler runs on the
    shared hub thread between PERSID emissions) and returns None for everything
    else (ordinary nodes pickle inline)."""
    ns = "W{0}:".format(wid)

    class FiberPickler(_pickle.Pickler):
        def persistent_id(self, obj):
            if isinstance(obj, External):
                # Externalize by wid-namespaced id.  YIELD WHILE mid-dump: the C
                # pickler is parked here holding its PERSID scratch, so the
                # scheduler runs a sibling fiber's Pickler on this hub thread.
                runloom.yield_now()
                if (obj.owner + len(obj.key)) & 1:
                    runloom.sleep(0.0002)
                return ns + obj.key
            return None

    return FiberPickler(buf, protocol=5)


def make_unpickler(wid, reg, buf):
    """This fiber's OWN Unpickler.  persistent_load asserts the "W{wid}" namespace
    (raising PersidLeak on any out-of-namespace id -- a cross-fiber leak) and
    resolves ONLY from THIS fiber's private registry."""
    ns = "W{0}".format(wid)

    class FiberUnpickler(_pickle.Unpickler):
        def persistent_load(self, pid):
            got_ns, key = pid.split(":", 1)
            if got_ns != ns:
                # An id from OUTSIDE this fiber's namespace reached this fiber's
                # persistent_load -- a sibling's externalized reference leaked in.
                raise PersidLeak(
                    "persistent_load handed out-of-namespace id {0!r} "
                    "(this fiber is {1}) -- a sibling fiber's externalized "
                    "persistent reference leaked across the mid-dump yield".format(
                        pid, ns))
            obj = reg.get(key)
            if obj is None:
                raise PersidLeak(
                    "persistent_load could not resolve id {0!r} from this "
                    "fiber's private registry -- the externalized key is not "
                    "one this fiber owns (cross-fiber persid leak)".format(pid))
            return obj

    return FiberUnpickler(buf)


def roundtrip(H, wid, state):
    """LOAD-BEARING: build this fiber's wid-tagged graph over its PRIVATE registry,
    pickle it with this fiber's OWN Pickler (persistent_id yields mid-dump and
    emits "W{wid}:{key}" ids), unpickle with this fiber's OWN Unpickler
    (persistent_load asserts the namespace and resolves from this fiber's
    registry), assert it recovered ITS OWN graph EXACTLY with every External
    resolving to the fiber's OWN object.  Single-owner: only this fiber touches its
    registry / Pickler / Unpickler / graph."""
    reg = make_registry(wid)
    g = build_graph(wid, reg)

    # Pickle with OUR OWN Pickler (persistent_id yields mid-dump).
    buf = io.BytesIO()
    make_pickler(wid, reg, buf).dump(g)
    data = buf.getvalue()

    # Unpickle with OUR OWN Unpickler.  An out-of-namespace persid trips PersidLeak.
    try:
        g2 = make_unpickler(wid, reg, io.BytesIO(data)).load()
    except PersidLeak as e:
        H.fail("pickle PERSID NAMESPACE LEAK (wid {0}): {1}".format(wid, e))
        return

    # (1) the whole tagged graph survived the round-trip.
    if g2 != g:
        H.fail("pickle persid round-trip CORRUPTED: recovered graph != original "
               "(wid {0}) -- a sibling fiber's persistent-object scratch bled into "
               "this fiber's _pickle.Pickler/Unpickler across the mid-dump yield "
               "(runloom shares the hub PyThreadState across fibers)".format(wid))
        return

    # (2) owner is OURS at every level AND every resolved External is THIS fiber's
    #     exact registry object (identity), not merely an equal one, and its owner
    #     is wid.  A resolved external that is a sibling's object -- or not the
    #     registry object -- is the cross-fiber persid leak the namespace assertion
    #     did not already catch (equal id string, different resolution).
    node = g2
    levels = 0
    while node is not None:
        if node.owner != wid:
            H.fail("pickle persid LEAK: recovered Graph.owner == {0} != {1} this "
                   "fiber built -- a sibling fiber's graph node resolved into this "
                   "fiber's round-trip".format(node.owner, wid))
            return
        for ext in node.refs:
            registry_obj = reg.get(ext.key)
            if ext is not registry_obj:
                H.fail("pickle persid LEAK: resolved External key {0!r} is NOT "
                       "this fiber's registry object (wid {1}) -- persistent_load "
                       "returned a sibling's / a foreign object for an externalized "
                       "reference".format(ext.key, wid))
                return
            if ext.owner != wid:
                H.fail("pickle persid LEAK: resolved External.owner == {0} != {1} "
                       "(key {2!r}) -- a persistent reference resolved to a "
                       "SIBLING's externalized object".format(
                           ext.owner, wid, ext.key))
                return
        node = node.child
        levels += 1

    # (3) the graph had the depth we built (persistent_id actually ran the full
    #     chain, so the oracle is non-vacuous for this round-trip).
    if levels != GRAPH_DEPTH:
        H.fail("pickle persid round-trip SHAPE wrong: recovered {0} levels != {1} "
               "built (wid {2}) -- the persistent-object round-trip truncated or "
               "extended this fiber's graph".format(levels, GRAPH_DEPTH, wid))
        return

    state["persid_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """LOAD-BEARING worker: sustains a persid round-trip-identity churn loop bounded
    by H.running().  Single-owner throughout -- each iteration builds a fresh
    private registry + graph and round-trips it through this fiber's own
    Pickler/Unpickler; nothing is shared, so every failure is a real cross-fiber
    leak, never documented shared-object semantics."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip(H, wid, state)             # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Self-test (single-owner, race-free): a graph round-trips correctly in
    # isolation via persistent_id/persistent_load, so the hazard is real and the
    # oracle is not vacuous before any fiber runs.
    reg = make_registry(-1)
    g = build_graph(-1, reg)
    buf = io.BytesIO()
    make_pickler(-1, reg, buf).dump(g)
    g2 = make_unpickler(-1, reg, io.BytesIO(buf.getvalue())).load()
    ok = (g2 == g and g2.owner == -1
          and g2.refs[0] is reg["ext0"] and g2.refs[0] is g2.child.refs[0])
    if not ok:
        H.fail("setup self-test: persid round-trip / external resolution broken in "
               "isolation -- the test scaffold is wrong, not runloom")
        return

    H.state = {
        "persid_checks": [0] * 1024,    # load-bearing round-trip checks done
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["persid_checks"])
    H.log("pickle persid: round-trip graph-identity + namespace checks={0} "
          "(LOAD-BEARING, single-owner private registry, all passed fail-fast); "
          "ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing persid round-trip hazard was actually exercised.
    H.check(checks > 0,
            "no pickle persid round-trip checks ran -- the load-bearing "
            "persistent_id / persistent_load namespace-isolation hazard was never "
            "exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # persistent_id's yield mid-dump, or in the C pickler mid-PERSID).
    H.require_no_lost("pickle persistent_id / persistent_load namespace isolation")


if __name__ == "__main__":
    harness.main(
        "p549_pickle_persid_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="the C _pickle Pickler.persistent_id externalizes objects by id "
                 "and Unpickler.persistent_load resolves them from a FIBER-PRIVATE "
                 "table; runloom shares one hub PyThreadState across fibers.  "
                 "LOAD-BEARING: each fiber builds a DISTINCT wid-tagged graph "
                 "referencing several Externals from its OWN private registry, "
                 "pickles with its OWN Pickler (persistent_id YIELDS mid-dump, "
                 "emits 'W{wid}:{key}' ids), unpickles with its OWN Unpickler "
                 "(persistent_load asserts the 'W{wid}' namespace and resolves from "
                 "this fiber's registry), and MUST recover ITS OWN graph exactly -- "
                 "right owner at every level + every External resolving to the "
                 "fiber's OWN object.  An out-of-namespace persid reaching "
                 "persistent_load, or a resolved external that is a sibling's "
                 "object, is the runloom persistent-object isolation bug.  Fully "
                 "single-owner (private table); no shared-mutable arm")
