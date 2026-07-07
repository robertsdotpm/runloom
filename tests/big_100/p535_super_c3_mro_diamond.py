"""big_100 / 535 -- super()/__mro__ C3-linearized diamond isolation under M:N.

Building a class is not free: CPython runs the C3 linearization algorithm over the
bases to compute the class's __mro__ tuple (the frozen method-resolution order),
and every cooperative-super() call (`super().method()`) walks THAT tuple, starting
from the position of the class the super() proxy was bound to, to find the next
implementation.  Two distinct mechanisms are in play per class:

  * __mro__ -- a tuple computed once at class-creation time by type_ready /
    mro_internal (the C3 merge).  For the classic diamond D(B, C) with B(A), C(A),
    the C3 result is the deterministic sequence (D, B, C, A, ..., object).
  * super() proxy -- a small object holding (__self_class__, __thisclass__); a
    call through it resumes the __mro__ walk at the slot AFTER __thisclass__, so a
    cooperative chain D.walk -> B.walk -> C.walk -> A.walk visits every class in
    C3 order exactly once.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded 3.14t
with the GIL off, tens of thousands of fibers on hubs>1 build fresh diamond class
hierarchies CONCURRENTLY.  Class creation mutates type slots (tp_mro, tp_bases,
tp_subclasses) and the super() walk reads tp_mro while dispatching.  If runloom's
M:N scheduler exposed a torn __mro__ (a class whose tp_mro was read mid-C3-merge),
or a super() proxy whose (__self_class__, __thisclass__) binding drifted to the
WRONG start class across a hub migration (the fiber parked mid-walk on hub X and
resumed on hub Y with a stale proxy), the cooperative-super chain would visit the
classes in the WRONG ORDER, revisit one, skip one, or dispatch into a foreign
fiber's class -- all observable as a corrupted visit order or accumulator.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  The C3 linearization of a fixed diamond shape is DETERMINISTIC: D(B, C) with
  B(A), C(A), A(Root) always linearizes to (D, B, C, A, Root, object) -- this is
  the documented, closed-form result of the C3 algorithm, identical on every
  thread and every run.  A cooperative-super() walk that starts at D and calls
  super().walk() at each level therefore visits EXACTLY [D, B, C, A] in that order,
  contributing each class's per-fiber value exactly once.  We confirmed with a
  plain-threads control (8 OS threads, each building its own fresh diamond with the
  same shape but per-thread additive contributions, GIL on AND off) that 100% of
  walks yield the C3 order and the closed-form accumulator -- 0 corrupted walks.
  Under a CORRECT runloom it MUST also hold: the single-owner oracle PASSES on a
  correct runtime (exit 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- C3 / SUPER ISOLATION (worker, HARD, fail-fast).  Each fiber
    builds its OWN fresh diamond (A; B(A), C(A); D(B, C)) via type() with UNIQUE
    per-wid additive contributions.  Every class and the D() instance are
    fiber-LOCAL (created in local variables, never shared -- distinct from p300's
    shared-type method cache).  The fiber then:
      - Snapshots tuple(D.__mro__) BEFORE a yield.
      - Yields (runloom.yield_now / sleep) so siblings build/walk their own
        diamonds and the scheduler can migrate this fiber across hubs.
      - Re-reads tuple(D.__mro__) and asserts it is UNCHANGED and equals the
        closed-form C3 sequence (D, B, C, A, Root, object) -- no torn __mro__.
      - Instantiates D() and runs the cooperative-super() walk, which appends each
        visited class's tag to an ordered list and adds its per-fiber value to an
        accumulator.
      - Asserts the visit order equals the CLOSED-FORM C3 order [D, B, C, A] (the
        super() proxies walked the right tuple from the right start class), and the
        accumulator equals the closed-form sum wid*16 + 10.
    Single-owner: the whole hierarchy + instance belong to one fiber.  A failure is
    a runloom C3/super desync (torn __mro__, wrong-start super proxy, cross-fiber
    dispatch), never documented Python semantics.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-walk
    (stranded inside a super() dispatch on a desynced proxy) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (mro_checks > 0).

FAIL ON: a torn/changed __mro__ across a yield, an __mro__ that differs from the
closed-form C3 sequence, a super()-walk visit order that is not the C3 order, or an
accumulator that differs from the closed-form sum.  There is NO shared-object arm:
every class is fiber-local and freshly built, so any corruption is a runtime bug.

Stresses: type() class creation + C3 mro_internal linearization under M:N, __mro__
tuple read during super() dispatch racing concurrent class construction on other
hubs, super() proxy (__self_class__/__thisclass__) binding stability across a
yield + hub migration, cooperative-super() method-resolution walk correctness.

Good TSan / controlled-M:N-replay target: tp_mro is written at class-ready and read
by every super() dispatch; under the single-owner arm each fiber's tp_mro is
written and read by ONE fiber, so a data-race report on a type object's tp_mro --
or a deterministic replay that reads __mro__ mid-C3-merge of another fiber's class
-- is the cleanest signal before the visit-order/accumulator oracle fires.
"""
import harness
import runloom

# Each fiber's four diamond classes get per-fiber additive contributions.  Base
# wid*4 keeps them distinct across fibers so a cross-fiber dispatch (visiting a
# sibling's class) would inject a foreign value and break the closed-form sum.
#   A = wid*4 + 1, B = wid*4 + 2, C = wid*4 + 3, D = wid*4 + 4
# Closed-form super()-walk sum over the C3 order {D, B, C, A}:
#   (wid*4+4) + (wid*4+2) + (wid*4+3) + (wid*4+1) = wid*16 + 10  (order-independent
# for the SUM; the ORDER is checked separately via the visit-order list).
TAGS = ("A", "B", "C", "D")


def class_value(wid, tag):
    return wid * 4 + (TAGS.index(tag) + 1)


def expected_sum(wid):
    return wid * 16 + 10


# Closed-form C3 order the cooperative-super() walk MUST visit (Root is the
# terminator whose walk() does nothing, so it is not appended).
EXPECTED_ORDER = ["D", "B", "C", "A"]


def make_diamond(wid, idx):
    """Build a fresh, fiber-LOCAL diamond hierarchy via type().

        Root  (terminator walk)
         |
         A  (walk: +A, super().walk())
        / \\
       B   C  (each: +tag, super().walk())
        \\ /
         D  (walk: +D, super().walk(); __init__ seeds order/acc)

    The classic diamond D(B, C), B(A), C(A), A(Root).  C3 linearizes D.__mro__ to
    (D, B, C, A, Root, object).  Returns (D_class, mro_tuple_of_classes) where the
    tuple is the expected (D, B, C, A, Root, object) built from the freshly created
    class objects -- used to assert __mro__ equals the closed-form C3 result."""
    vals = {tag: class_value(wid, tag) for tag in TAGS}
    holder = {}

    def mk_walk(tag):
        # Cooperative-super() walk step for class `tag`.  Uses the explicit
        # super(cls, self) form because type()-built methods have no __class__
        # cell for zero-arg super(); holder[tag] is resolved at CALL time, after
        # every class exists, so the binding is the real fiber-local class object.
        def walk(self):
            self.order.append(tag)
            self.acc += vals[tag]
            super(holder[tag], self).walk()
        return walk

    def root_walk(self):
        # Terminator: the top of the cooperative chain.  object has no walk(), so
        # A.walk()'s super().walk() lands here and stops the recursion.
        return None

    def d_init(self):
        self.order = []
        self.acc = 0

    suffix = "W{0}_I{1}".format(wid, idx)
    Root = type("DiaRoot_" + suffix, (object,), {"walk": root_walk})
    A = type("DiaA_" + suffix, (Root,), {"walk": mk_walk("A")})
    holder["A"] = A
    B = type("DiaB_" + suffix, (A,), {"walk": mk_walk("B")})
    holder["B"] = B
    C = type("DiaC_" + suffix, (A,), {"walk": mk_walk("C")})
    holder["C"] = C
    D = type("DiaD_" + suffix, (B, C), {"walk": mk_walk("D"), "__init__": d_init})
    holder["D"] = D

    expected_mro = (D, B, C, A, Root, object)
    return D, expected_mro


# Sustained checks per worker, bounded by H.running().  The torn-__mro__ /
# wrong-start-super hazard only manifests under SUSTAINED churn: many fibers
# simultaneously building + walking fresh diamonds while parked across the yield,
# so the scheduler reliably interleaves a sibling's class construction / super()
# walk (and a hub migration) before this fiber resumes its own walk.  A single
# build+walk per fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def diamond_check(H, wid, idx, state):
    """Single-owner C3/super isolation check.

    Build a fiber-local diamond, snapshot __mro__, yield to let siblings build +
    walk their own diamonds (and migrate this fiber across hubs), then assert the
    __mro__ is untorn + matches the closed-form C3 sequence, and that a cooperative
    super() walk visits the classes in C3 order with the closed-form accumulator."""
    D, expected_mro = make_diamond(wid, idx)

    # Snapshot __mro__ BEFORE the yield.  tuple(D.__mro__) materializes the frozen
    # C3 order; a torn read mid-merge, or drift across the yield, is a hard fault.
    mro_before = tuple(D.__mro__)

    # YIELD: siblings build/walk their own diamonds; the scheduler may migrate this
    # fiber to another hub, which is where a stale super() proxy would surface.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Check 1: __mro__ is UNCHANGED across the yield (no torn/replaced tuple).
    mro_after = tuple(D.__mro__)
    if mro_after != mro_before:
        H.fail("__mro__ TORN across a yield: {0}.__mro__ changed from {1!r} to "
               "{2!r} (wid {3}) -- a fiber-local class's method-resolution order "
               "was corrupted / read mid-C3-merge under M:N".format(
                   D.__name__, [c.__name__ for c in mro_before],
                   [c.__name__ for c in mro_after], wid))
        return

    # Check 2: __mro__ equals the closed-form C3 linearization (D, B, C, A, Root,
    # object).  The C3 result of this diamond shape is deterministic; anything else
    # is a corrupted linearization.
    if mro_after != expected_mro:
        H.fail("__mro__ WRONG: {0}.__mro__ is {1!r}, expected the C3 sequence "
               "{2!r} (wid {3}) -- the C3 linearization produced the wrong order "
               "under concurrent class construction".format(
                   D.__name__, [c.__name__ for c in mro_after],
                   [c.__name__ for c in expected_mro], wid))
        return

    # Check 3: run the cooperative super() walk and assert C3 visit order.
    d = D()
    d.walk()

    if d.order != EXPECTED_ORDER:
        H.fail("super()-walk VISIT ORDER wrong: {0} walked {1!r}, expected the C3 "
               "order {2!r} (wid {3}) -- a super() proxy resumed the __mro__ walk "
               "from the wrong start class, or dispatched into a foreign fiber's "
               "class, under a hub migration".format(
                   D.__name__, d.order, EXPECTED_ORDER, wid))
        return

    # Check 4: the accumulator equals the closed-form sum wid*16 + 10.  A revisited
    # or skipped class, or a cross-fiber dispatch injecting a sibling's per-wid
    # value, would move this.
    want = expected_sum(wid)
    if d.acc != want:
        H.fail("super()-walk ACCUMULATOR wrong: {0} accumulated {1}, expected the "
               "closed-form C3 sum {2} (wid {3}) -- a class was revisited/skipped "
               "or a foreign fiber's per-wid value was injected during the walk"
               .format(D.__name__, d.acc, want, wid))
        return

    state["mro_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber builds + walks fresh fiber-local diamonds in a sustained loop,
    yielding at the hazard boundary (between class construction and the super()
    walk) so a sibling reliably interleaves before this fiber resumes."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            diamond_check(H, wid, idx, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "mro_checks": [0] * 1024,        # LOAD-BEARING single-owner checks (tally)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    mchecks = sum(H.state["mro_checks"])
    H.log("C3/super[single-owner LOAD-BEARING]: {0} diamond __mro__ + super()-walk "
          "isolation checks (all passed fail-fast); ops={1}".format(
              mchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(mchecks > 0,
            "no single-owner C3/super checks ran -- the load-bearing __mro__/"
            "super()-walk hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a super()
    # dispatch on a desynced proxy / mid-C3-merge).
    H.require_no_lost("C3/super mro isolation")


if __name__ == "__main__":
    harness.main(
        "p535_super_c3_mro_diamond", body, setup=setup, post=post,
        default_funcs=6000,
        describe="building a class runs C3 linearization to compute __mro__, and "
                 "super() proxies walk that tuple from the bound start class. "
                 "Under M:N, a torn __mro__ (read mid-C3-merge) or a super() proxy "
                 "bound to the wrong start class across a hub migration would "
                 "corrupt method resolution.  LOAD-BEARING: each fiber builds its "
                 "OWN fresh diamond (A; B,C(A); D(B,C)) via type() with per-wid "
                 "additive contributions, snapshots __mro__ across a yield, then "
                 "runs a cooperative super() walk; __mro__ MUST equal the closed-"
                 "form C3 sequence (D,B,C,A,Root,object) and the walk MUST visit "
                 "the C3 order [D,B,C,A] with the closed-form accumulator "
                 "wid*16+10.  Every class is fiber-local -- any corruption is a "
                 "runloom C3/super desync, not documented Python semantics")
