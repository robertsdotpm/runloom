"""big_100 / 500 -- operator.itemgetter/attrgetter/methodcaller identity+value
isolation under M:N.

operator.itemgetter(i, j, k), operator.attrgetter('a.b.c') and
operator.methodcaller('m', x) are all C-implemented callables that store their
selection spec (an index tuple, a pre-split list of attribute-name components,
or a method name plus a frozen args/kwargs bundle) in C struct fields at
construction time and read it back on every __call__.  The callable is a small
immutable C object; applying it to a target does a C-level fetch --
PyObject_GetItem / a chained PyObject_GetAttr walk / a PyObject_GetAttr +
PyObject_Call.  In 3.14t with free-threading, if the getter's C-stored spec is
torn across a hub migration -- or if the target sequence/namespace this fiber
built is observed half-constructed by the C fetch after the fiber resumes on a
different hub -- the getter would return the WRONG element (a different index, a
different attribute, or a stale object) than the closed-form spec demands.

WHERE M:N BREAKS IT (the gap this program probes).  runloom gives each fiber its
own Python frame stack, but the itemgetter/attrgetter/methodcaller objects are
plain C objects and the target sequences/namespaces are plain Python containers.
Nothing about them is fiber-aware.  A fiber that builds a fiber-LOCAL getter over
a fiber-LOCAL target with unique per-wid values, snapshots the result, YIELDS
(so a sibling reliably interleaves on another hub -- possibly building its own
conflicting getters/targets or migrating this fiber to a different hub), then
re-applies the SAME getter to the SAME target, MUST get back the byte-identical
result: same object identity for every selected member, same value equal to the
closed-form arithmetic expectation.  If the getter's index tuple / attr-name
list / method spec were torn by a concurrent hub, or the C fetch returned a
sibling's object, the identity or the value would change across the yield.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  operator.itemgetter(2,5,0,7)(seq) is DEFINED to return
  (seq[2], seq[5], seq[0], seq[7]) -- the EXACT objects stored at those indices.
  operator.attrgetter('a.b.c')(root) is DEFINED to return root.a.b.c -- the exact
  leaf object.  operator.methodcaller('combine', 3, 4)(t) is DEFINED to return
  t.combine(3, 4).  When the getter and its target are OWNED by one fiber and the
  values are unique per wid, the result is a closed-form function of wid alone.
  We verified with a standalone plain-threads control (8 OS threads, each building
  its own getters+targets with unique values, GIL on AND off) that 100% of
  applications return the closed-form object/value -- 0 cross-thread bleed and 0
  identity/value drift across a sched-yield-equivalent.  Under a CORRECT runloom
  it must also hold: the single-owner arm PASSES on a correct runtime (program
  exits 0 when there is no bug).  A member whose IDENTITY changes across the yield
  (the getter returned a different object), or whose VALUE no longer equals the
  closed-form per-wid expectation, is a torn-callable / torn-target-fetch bug in
  the runtime -- not documented Python semantics (the objects are single-owner).

ORACLES:
  * LOAD-BEARING -- GETTER IDENTITY+VALUE ISOLATION (worker, HARD, fail-fast).
    Each fiber, per inner iteration, builds THREE fiber-local structures with a
    unique per-wid base value:
      - itemgetter: a fiber-local list of distinct large-int objects
        seq = [base+0 .. base+N-1]; getter = operator.itemgetter(i, j, k, l).
        Snapshot snap = getter(seq); the result members are the EXACT objects in
        seq at those indices.
      - attrgetter: a fiber-local nested namespace root.a.b.c (a chain of plain
        instances) with a distinct leaf int; getter = attrgetter('a.b.c') and a
        multi-target attrgetter('a.b.c', 'a.b').
      - methodcaller: a fiber-local target object with a method computing a
        closed-form function of its unique value; caller = methodcaller('combine',
        x, y).
    Then it YIELDS (runloom.yield_now / a tiny sleep) so a sibling interleaves,
    re-applies every getter to the SAME target, and asserts for every selected
    member: identity stable across the yield (same object, itemgetter/attrgetter),
    value stable across the yield, and value == the closed-form per-wid
    expectation (not a cross-fiber leak).  Single-owner: getter + target are
    fiber-local, never shared.  A failure is a runtime torn-getter/torn-fetch
    desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-fetch
    (stranded inside the C attr-chain walk or the method call on a desynced
    reference) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (getter_checks>0).

  * SECONDARY (report-ONLY, NEVER fails): MEASURED cross-fiber bleed on a SHARED
    getter over a SHARED, concurrently-mutated sequence.  A small pool of shared
    lists is hammered: each fiber writes its unique value at a wid-owned slot and
    reads a shared itemgetter that also selects OTHER fibers' slots, so the read
    naturally observes siblings' concurrent writes.  A shared mutable container
    under M:N races EXACTLY like shared-across-threads -- DOCUMENTED Python
    behavior, NOT a runtime bug.  We MEASURE + REPORT the observed-drift rate (a
    read whose selected non-owned slot changed value between two reads) like p67's
    threading.local leak rate, to prove the hazard is real (the C fetch DOES see
    concurrent mutation), and NEVER call H.fail on it.

FAIL ON: a single-owner getter member whose identity changes across a yield, a
value that changes across a yield, or a value that is not the closed-form
per-wid expectation (a cross-fiber object leak / torn getter spec).  The shared-
pool MEASURED arm is report-only and is expected to show drift (documented M:N
shared-object behavior) -- the load-bearing oracle must stay clean.

Stresses: operator.itemgetter index-tuple fetch, attrgetter chained
PyObject_GetAttr walk over a nested namespace, methodcaller name+args bundle and
PyObject_Call, all across hub migration + a yield, per-fiber getter/target
isolation vs shared-getter behavior, C-level GetItem/GetAttr/Call under M:N
concurrency.

Good TSan / controlled-M:N-replay target: itemgetter/attrgetter store their spec
in C fields read on every call; under the single-owner arm the getter and its
target are touched by ONE fiber, so a data-race report on the getter's C spec or
the target container -- or a deterministic replay that re-applies the getter
mid-migration and returns a sibling's object -- is the cleanest signal before the
identity/value oracle fires.
"""
import operator

import harness
import runloom

# Per-fiber unique value band.  Each wid gets base = wid * VALUE_SCALE so the
# int objects a fiber places in its sequence/namespace never numerically overlap
# a sibling's -- a value drawn from the wrong fiber is instantly recognizable.
# VALUE_SCALE is large enough that every value is well past CPython's small-int
# cache (-5..256), so each is a DISTINCT heap int object and identity checks (is)
# are meaningful, not aliased by the small-int singletons.
VALUE_SCALE = 1000000
SEQ_LEN = 8                                    # itemgetter source length
# The itemgetter selection: fixed index tuple, deliberately out-of-order and
# repeating so a torn index tuple is detectable.  All indices < SEQ_LEN.
ITEM_INDICES = (2, 5, 0, 7, 5, 3)


class NsNode(object):
    """A plain attribute-holder for building fiber-local nested namespaces.

    Single-owner: every instance is created inside one fiber's check and never
    escapes it, so the attribute writes/reads are race-free by construction."""
    __slots__ = ("a", "b", "c")


class MethodTarget(object):
    """Fiber-local method-call target with a closed-form method.

    combine(x, y) == self.v + x*y is a pure function of the object's unique per-
    wid value and the (fixed) args, so methodcaller('combine', x, y)(t) has a
    closed-form expectation."""
    __slots__ = ("v",)

    def __init__(self, v):
        self.v = v

    def combine(self, x, y):
        return self.v + x * y


def build_seq(base):
    """A fiber-local list of SEQ_LEN DISTINCT large-int objects base+0..base+N-1.

    The comprehension's additions each produce a fresh int object (values well
    past the small-int cache), so itemgetter returns the EXACT object stored at
    each index and `result_member is seq[index]` is a real identity check."""
    return [base + i for i in range(SEQ_LEN)]


def build_namespace(base):
    """A fiber-local nested namespace root.a.b.c with a distinct leaf int.

    root.a -> mid, mid.b -> leaf, leaf.c -> (base + 100) [a distinct heap int].
    Returns (root, leaf_value_object) so the caller can identity-check the leaf."""
    leaf = NsNode()
    leaf_val = base + 100
    leaf.c = leaf_val
    mid = NsNode()
    mid.b = leaf
    root = NsNode()
    root.a = mid
    return root, mid, leaf, leaf_val


# ---- LOAD-BEARING arm: single-owner fiber-local getters ------------------
def getter_check(H, wid, base, state):
    """Single-owner operator-getter identity+value isolation check.

    Builds three fiber-local structures with unique per-wid values, snapshots
    every getter's result, yields so a sibling interleaves on another hub, then
    re-applies each getter to the SAME target and verifies identity + value are
    stable across the yield and equal the closed-form per-wid expectation.  A
    torn getter spec or a cross-fiber object fetch would break identity or value."""
    # ---- itemgetter over a fiber-local distinct-int sequence -------------
    seq = build_seq(base)
    ig = operator.itemgetter(*ITEM_INDICES)
    snap_items = ig(seq)                        # (seq[2], seq[5], seq[0], ...)

    # ---- attrgetter over a fiber-local nested namespace ------------------
    root, mid, leaf, leaf_val = build_namespace(base)
    ag_leaf = operator.attrgetter("a.b.c")
    ag_multi = operator.attrgetter("a.b.c", "a.b")
    snap_leaf = ag_leaf(root)                   # is leaf.c
    snap_multi = ag_multi(root)                 # (leaf.c, mid.b) == (leaf.c, leaf)

    # ---- methodcaller over a fiber-local target --------------------------
    tgt = MethodTarget(base)
    mc = operator.methodcaller("combine", 3, 4)
    snap_mc = mc(tgt)                           # base + 12
    expected_mc = base + 12

    # YIELD: let siblings run on other hubs and (maybe) migrate this fiber.
    runloom.yield_now()
    if base & 1:
        runloom.sleep(0.0003)

    # ---- re-apply itemgetter and verify identity + value -----------------
    got_items = ig(seq)
    for n, idx in enumerate(ITEM_INDICES):
        # Identity: the same object stored at seq[idx] (before AND after yield).
        if got_items[n] is not snap_items[n] or got_items[n] is not seq[idx]:
            H.fail("itemgetter IDENTITY CHANGED: member {0} (index {1}) id "
                   "{2} before yield, {3} after / seq-slot id {4} (wid {5}) -- "
                   "the getter's index tuple was torn or the C GetItem returned "
                   "a different object across a hub migration".format(
                       n, idx, id(snap_items[n]), id(got_items[n]),
                       id(seq[idx]), wid))
            return
        expected_val = base + idx
        if got_items[n] != expected_val or snap_items[n] != expected_val:
            H.fail("itemgetter VALUE WRONG: member {0} (index {1}) == {2} "
                   "(snap {3}), expected closed-form {4} (wid {5}) -- a cross-"
                   "fiber object leak or torn index tuple".format(
                       n, idx, got_items[n], snap_items[n], expected_val, wid))
            return

    # ---- re-apply attrgetter and verify identity + value -----------------
    got_leaf = ag_leaf(root)
    if got_leaf is not snap_leaf or got_leaf is not leaf.c:
        H.fail("attrgetter('a.b.c') IDENTITY CHANGED: leaf id {0} before yield, "
               "{1} after / root.a.b.c id {2} (wid {3}) -- the attr-name chain "
               "was torn or the C GetAttr walk returned a different object".format(
                   id(snap_leaf), id(got_leaf), id(leaf.c), wid))
        return
    if got_leaf != leaf_val:
        H.fail("attrgetter('a.b.c') VALUE WRONG: {0}, expected closed-form {1} "
               "(wid {2}) -- a cross-fiber namespace leak".format(
                   got_leaf, leaf_val, wid))
        return

    got_multi = ag_multi(root)
    # got_multi == (root.a.b.c, root.a.b) == (leaf.c, leaf)
    if got_multi[0] is not leaf.c or got_multi[1] is not leaf:
        H.fail("attrgetter('a.b.c','a.b') IDENTITY CHANGED across yield: "
               "leaf-val is-leaf.c={0}, mid.b is-leaf={1} (wid {2}) -- the "
               "multi-target attr fetch returned a wrong/sibling object".format(
                   got_multi[0] is leaf.c, got_multi[1] is leaf, wid))
        return
    if got_multi[0] != leaf_val or got_multi != snap_multi:
        H.fail("attrgetter multi VALUE CHANGED across yield: got {0!r}, snap "
               "{1!r}, expected leaf {2} (wid {3})".format(
                   got_multi, snap_multi, leaf_val, wid))
        return

    # ---- re-apply methodcaller and verify value -------------------------
    got_mc = mc(tgt)
    if got_mc != expected_mc or snap_mc != expected_mc:
        H.fail("methodcaller('combine',3,4) VALUE WRONG: {0} (snap {1}), "
               "expected closed-form {2} = base({3})+12 (wid {4}) -- the method "
               "name/args bundle was torn or the call bound a sibling's self".format(
                   got_mc, snap_mc, expected_mc, base, wid))
        return

    state["getter_checks"][wid & 1023] += 1


# ---- MEASURED arm: shared getter over a shared sequence (report-only) -----
def shared_getter_check(H, wid, r, state):
    """Shared-getter cross-fiber drift (MEASURED, report-only).

    A small pool of SHARED lists is hammered by all fibers: each fiber writes its
    unique value at its wid-owned slot, and a SHARED itemgetter reads slots owned
    by OTHER fibers too.  Because the list is a shared mutable container, a read
    naturally observes siblings' concurrent writes -- DOCUMENTED M:N shared-object
    behavior, exactly like p67's threading.local leak.  We read the same shared
    getter twice around a yield and MEASURE when a selected non-owned slot changed
    value in between; we NEVER fail on it."""
    pool = state["shared_pool"]
    shared = pool[wid % len(pool)]
    plen = len(shared)
    # This fiber owns exactly one slot in the shared list; write its unique value.
    own = wid % plen
    shared[own] = wid * VALUE_SCALE + (r & 0xFFFF)
    ig = state["shared_getter"]                 # selects several fixed slots
    first = ig(shared)
    runloom.yield_now()
    second = ig(shared)
    state["shared_checks"][wid & 1023] += 1
    if first != second:
        # A selected slot (owned by some other fiber) changed value between the
        # two reads: documented shared-mutable-container drift, NOT a bug.
        state["shared_leaks"][wid & 1023] += 1


# Sustained checks per worker, bounded by H.running().  The torn-getter hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously building /
# applying getters while sleep-PARKED across their yield, so the scheduler
# reliably interleaves a sibling (and possibly a hub migration) before this fiber
# resumes.  A single check per fiber barely overlaps a sibling's and does NOT
# reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per inner iteration: the LOAD-BEARING single-
    owner getter check (fail-fast) and the MEASURED shared-getter check (report
    only).  The two share no data (fiber-local getters/targets vs a shared pool)
    so running them together keeps the hub busy with mixed churn without the
    shared mutations ever reaching the single-owner oracle."""
    base = wid * VALUE_SCALE
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            getter_check(H, wid, base, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_getter_check(H, wid, idx, state)  # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED lists for the MEASURED arm.  Sized so many wids map
    # onto each list (wid % len(pool)) and each list has enough slots that the
    # shared itemgetter selects slots owned by OTHER fibers -- the drift source.
    POOL = 8
    SLOTS = 64
    shared_pool = [[i for i in range(SLOTS)] for _ in range(POOL)]
    # A fixed shared itemgetter selecting several spread-out slots; most of the
    # selected slots are owned by fibers other than any given reader.
    shared_getter = operator.itemgetter(1, 7, 13, 29, 47, 61)

    H.state = {
        "getter_checks": [0] * 1024,      # LOAD-BEARING single-owner checks
        "shared_pool": shared_pool,       # small shared list pool
        "shared_getter": shared_getter,   # shared itemgetter (drift source)
        "shared_checks": [0] * 1024,      # MEASURED shared-getter reads
        "shared_leaks": [0] * 1024,       # observed cross-fiber drift
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gchecks = sum(H.state["getter_checks"])
    schecks = sum(H.state["shared_checks"])
    sleaks = sum(H.state["shared_leaks"])
    spct = (100.0 * sleaks / schecks) if schecks else 0.0

    H.log("operator[single-owner LOAD-BEARING]: {0} getter identity+value "
          "checks (all passed fail-fast) | operator[shared pool MEASURED]: {1} "
          "reads {2} drift ({3:.1f}%, documented shared-object behavior -- "
          "REPORT ONLY)".format(gchecks, schecks, sleaks, spct))

    if sleaks:
        H.log("note: the shared list pool observed {0} cross-fiber value drifts "
              "across {1} reads -- a shared itemgetter over a shared, "
              "concurrently-mutated list sees siblings' writes (the list is a "
              "shared Python object, like p67's threading.local shared "
              "container).  This is documented M:N shared-object behavior, NOT a "
              "runtime bug, and never reaches the load-bearing single-owner "
              "oracle".format(sleaks, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(gchecks > 0,
            "no single-owner operator-getter checks ran -- the load-bearing "
            "torn-getter hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the C
    # attr-chain walk or the method call on a desynced reference).
    H.require_no_lost("operator getter isolation")


if __name__ == "__main__":
    harness.main(
        "p500_operator_getters_identity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="operator.itemgetter/attrgetter/methodcaller are C callables "
                 "holding an index-tuple / attr-name list / method+args bundle "
                 "read on every call.  Under M:N, a torn getter spec or a C "
                 "GetItem/GetAttr/Call that returns a sibling's object across a "
                 "hub migration would return the WRONG element.  LOAD-BEARING: "
                 "each fiber builds fiber-local getters over fiber-local targets "
                 "with unique per-wid values, snapshots the result, yields, and "
                 "re-applies -- identity AND value must stay stable and equal the "
                 "closed-form per-wid expectation.  MEASURED shared-pool arm "
                 "(expected drift on a shared mutable list, like p67) proves the "
                 "hazard exists.  An identity/value change across the yield on the "
                 "single-owner getter is the runtime torn-getter bug")
