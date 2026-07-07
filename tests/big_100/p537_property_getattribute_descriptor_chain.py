"""big_100 / 537 -- property/__get__ data-descriptor + __getattribute__ chain isolation under M:N.

Attribute access `inst.value` in CPython runs a fixed protocol implemented in
`type.__getattribute__` (or the instance's overriding `__getattribute__`):

  1. the class MRO is searched for `value`; if it is found and is a DATA
     descriptor (defines `__set__`/`__delete__` -- a `property` with a setter
     qualifies), its `__get__` WINS and the instance `__dict__` is never
     consulted;
  2. otherwise the instance `__dict__` is consulted;
  3. otherwise a non-data descriptor / class attribute is used.

Both the property getter and a custom `__getattribute__` run ARBITRARY PYTHON
CODE on every access.  Under free-threaded 3.14t with hubs>1, that Python code
executes on whichever hub the owning fiber currently runs on, and a cooperative
yield can migrate the fiber to a different hub mid-workload.  The hazard this
program probes:

  * the getter computes its result from the instance's own backing state
    (`self.backing`).  If a hub migration or a torn frame let the getter read a
    SIBLING fiber's instance state (or a half-written `self`), the computed
    property value would be wrong -- built from a torn frame or another fiber's
    backing;
  * `__getattribute__` maintains a per-instance access counter.  If the descriptor
    lookup / instance-dict fallthrough desynced under M:N, the data descriptor
    could lose to a decoy planted in the instance `__dict__` (returning the decoy
    instead of the computed value), or the counter could see a value inconsistent
    with the number of accesses THIS fiber performed.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each fiber
its own Python frame stack; a correctly-implemented descriptor protocol reads
`self` from the frame's locals and resolves the MRO/instance-dict deterministically
regardless of which hub runs the frame.  But if `self` were torn across a
hub-migration yield inside the getter, or the C-level attribute-lookup fast path
cached a resolution across a fiber switch, a fiber accessing its OWN single-owner
instance could observe another fiber's backing value, a stale decoy from the
instance dict, or a counter that does not match its own access tally.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  A fiber OWNS one `Probe` instance (created in a fiber-local variable, never
  shared).  It sets that instance's `backing` to a UNIQUE per-fiber value and
  plants a DECOY in the instance `__dict__` under the property name `value`.  The
  property `value` is a DATA descriptor (it has both a getter and a setter), so by
  the documented protocol `inst.value` MUST return the getter's computed result
  (`backing * MULT + ADD`), NEVER the decoy, and NEVER a sibling's value -- both
  before and after a yield that can migrate the fiber across hubs.  We verified
  with a plain-threads control (8 OS threads, each owning its own Probe with a
  distinct backing + a planted decoy, GIL on AND off) that 100% of accesses return
  the correct per-thread computed value and never the decoy, and the per-instance
  `__getattribute__` counter always equals that thread's own access count -- 0
  cross-thread leaks.  Under a CORRECT runloom it must hold identically: the
  single-owner load-bearing oracle PASSES on a correct runtime (exit 0 when there
  is no bug).

ORACLES:
  * LOAD-BEARING -- DESCRIPTOR-CHAIN ISOLATION (worker, HARD, fail-fast).  Each
    fiber creates its OWN `Probe`, sets `backing` to `wid*VALUE_SCALE + idx`, and
    plants a decoy `value` entry in the instance `__dict__`.  It then:
      - reads `inst.value` (baseline): asserts it equals the computed function of
        backing (data descriptor beats instance dict -- NOT the decoy);
      - yields (yield_now / sleep) to let siblings run and migrate this fiber;
      - re-reads `inst.value`: asserts SAME computed value (stable across the
        yield, still not the decoy, still the fiber's own unique value);
      - asserts the instance `__dict__` STILL literally holds the decoy under
        `value` (proving the descriptor -- not a dict mutation -- is what won);
      - asserts the per-instance `__getattribute__` counter equals EXACTLY the
        number of `value` accesses THIS fiber performed (self-consistent count).
    Single-owner: the Probe, its backing, its decoy, and its access counter are
    all fiber-local, never shared.  A failure is a runloom descriptor-chain /
    attribute-protocol desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the
    getter or `__getattribute__` (parked on a desynced `self`) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (prop_checks > 0).

  * SECONDARY (report-ONLY, NEVER fails): MEASURED shared-instance race.  A small
    pool of SHARED Probe instances is hammered by all fibers: a fiber writes the
    shared instance's backing then reads `value`, and a sibling that wrote a
    different backing in between makes the read disagree with what this fiber set.
    Cross-fiber disagreement is EXPECTED and DOCUMENTED here (a shared mutable
    object under M:N races exactly like shared-across-threads).  We MEASURE the
    disagreement rate and REPORT it -- proving the hazard exists so the
    load-bearing single-owner arm is truly testing isolation -- but NEVER fail on
    it (failing would mislabel documented shared-object semantics as a bug).

FAIL ON: a single-owner property read returning the decoy, a sibling's value, or
a value that changes across a yield; the instance dict decoy vanishing; or the
per-instance access counter disagreeing with this fiber's own access tally.  The
shared-pool MEASURED arm is report-only.

Stresses: `property.__get__` (data-descriptor) resolution beating the instance
`__dict__`, custom `__getattribute__` running Python code per access, the C-level
attribute-lookup fast path across a hub-migration yield, per-instance state
(`self.backing`, access counter) read/written by one fiber under M:N churn.
"""
import harness
import runloom

# The property getter computes a PURE FUNCTION of the instance's backing slot.
# Distinct fibers get distinct backings (wid*VALUE_SCALE + idx), so the computed
# value is unique per fiber and a cross-fiber leak returns a visibly wrong number.
MULT = 1000003
ADD = 12345
VALUE_SCALE = 100000
VALUE_SPAN = 16                     # distinct backings a fiber cycles through

# A sentinel planted in the instance __dict__ under the property name.  The data
# descriptor MUST win, so a correct read NEVER returns this.  Chosen far outside
# the computed-value band (always positive) so a decoy leak is unmistakable.
DECOY = -999999


class Probe(object):
    """A single-owner instance whose `value` is a DATA descriptor (getter+setter)
    computed as a pure function of a fiber-local `backing` slot, plus a custom
    `__getattribute__` that counts accesses to `value` per instance.

    All object.__getattribute__ / object.__setattr__ uses below bypass this class's
    overridden `__getattribute__` (and the property descriptor) to avoid infinite
    recursion -- they are the raw C-level attribute machinery, so reading `backing`
    or bumping `gac` inside the protocol never re-enters it."""

    def __init__(self):
        # Plain assignment (no __setattr__ override) seeds the instance dict.
        self.gac = 0                # __getattribute__ call count for `value`
        self.backing = 0            # fiber-local backing slot the getter reads

    @property
    def value(self):
        # Read backing through the RAW machinery so it doesn't re-trigger this
        # class's __getattribute__.  Pure function of the single-owner backing.
        b = object.__getattribute__(self, "backing")
        return b * MULT + ADD

    @value.setter
    def value(self, v):
        # Having a setter is what makes `value` a DATA descriptor (so it beats the
        # instance __dict__).  We never call it in the load-bearing arm (we set
        # `backing` directly), but it must exist for the protocol under test.
        object.__setattr__(self, "backing", v)

    def __getattribute__(self, name):
        # Count only accesses to `value`; use the RAW machinery for gac so this
        # method never recurses into itself.  Single writer (the owning fiber).
        if name == "value":
            g = object.__getattribute__(self, "gac")
            object.__setattr__(self, "gac", g + 1)
        return object.__getattribute__(self, name)


# ---- LOAD-BEARING arm: single-owner descriptor-chain check ----------------
def prop_check(H, wid, idx, state):
    """Single-owner property/__getattribute__ descriptor-chain isolation check.

    The Probe, its backing, the planted decoy, and the access counter are all
    fiber-local.  A cross-fiber leak, a lost descriptor-vs-dict resolution, or a
    torn `self` would surface as a wrong value / vanished decoy / inconsistent
    counter."""
    p = Probe()
    backing = wid * VALUE_SCALE + (idx % VALUE_SPAN)
    p.backing = backing
    expected = backing * MULT + ADD

    # Plant a DECOY in the instance __dict__ under the property name.  Accessing
    # p.__dict__ triggers __getattribute__("__dict__") (name != "value", so it is
    # NOT counted) and returns the raw dict; the data descriptor must still win.
    p.__dict__["value"] = DECOY

    # Baseline read: data descriptor beats the instance-dict decoy.  (gac: 0 -> 1)
    baseline = p.value
    if baseline == DECOY:
        H.fail("descriptor-chain broken: inst.value returned the instance-dict "
               "DECOY {0} instead of the data-descriptor getter value {1} (wid "
               "{2}) -- the data descriptor lost to the instance __dict__".format(
                   DECOY, expected, wid))
        return
    if baseline != expected:
        H.fail("property getter WRONG at baseline: inst.value == {0}, expected "
               "{1} (wid {2}, backing {3}) -- the getter computed from a torn "
               "frame or a sibling's backing".format(
                   baseline, expected, wid, backing))
        return

    # YIELD: let siblings run and potentially migrate this fiber across hubs.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Re-read after the yield: value must be stable, still the descriptor's, still
    # this fiber's own unique value.  (gac: 1 -> 2)
    again = p.value
    if again == DECOY:
        H.fail("descriptor-chain broken across yield: inst.value returned the "
               "instance-dict DECOY {0} instead of {1} (wid {2}) -- a hub "
               "migration flipped the descriptor-vs-dict resolution".format(
                   DECOY, expected, wid))
        return
    if again != baseline:
        H.fail("property VALUE CHANGED across a yield: inst.value went {0} -> {1} "
               "(wid {2}, backing {3}) -- a sibling's instance state leaked into "
               "this fiber's getter or `self` was torn across the hub "
               "migration".format(baseline, again, wid, backing))
        return
    if again != expected:
        H.fail("property VALUE WRONG after yield: inst.value == {0}, expected {1} "
               "(wid {2}) -- a cross-fiber backing leak".format(
                   again, expected, wid))
        return

    # The instance __dict__ STILL literally holds the decoy -- proving it was the
    # descriptor (not a dict mutation) that produced the correct value.
    dict_val = p.__dict__["value"]
    if dict_val != DECOY:
        H.fail("instance __dict__ decoy VANISHED: p.__dict__['value'] == {0}, "
               "expected the planted decoy {1} (wid {2}) -- the instance dict "
               "under the property name was corrupted".format(
                   dict_val, DECOY, wid))
        return

    # The per-instance __getattribute__ counter must equal EXACTLY the number of
    # `value` accesses this fiber performed (baseline + again == 2).  Single
    # writer, single owner: any other value is a torn/leaked counter.
    accesses = object.__getattribute__(p, "gac")
    if accesses != 2:
        H.fail("__getattribute__ counter INCONSISTENT: gac == {0}, expected 2 "
               "(this fiber accessed inst.value exactly twice) (wid {1}) -- the "
               "per-instance access counter saw a sibling's access or a torn "
               "write".format(accesses, wid))
        return

    state["prop_checks"][wid & 1023] += 1


# ---- MEASURED arm: shared instance (report-only) --------------------------
def shared_probe_check(H, wid, idx, state):
    """Shared Probe backing read-after-write (MEASURED, report-only).

    A small pool of SHARED Probe instances is hammered by all fibers.  A fiber
    writes the shared instance's backing then reads `value`; a sibling that wrote a
    different backing in between makes the read disagree with what this fiber set.
    Cross-fiber disagreement is EXPECTED and DOCUMENTED (a shared mutable object
    under M:N races exactly like shared-across-threads).  We MEASURE the rate; we
    NEVER fail on it."""
    pool = state["shared_pool"]
    p = pool[idx % len(pool)]
    mine = (wid * VALUE_SCALE + idx) % 2147480000
    p.backing = mine                       # racy shared write
    runloom.yield_now()                    # sibling may overwrite backing here
    got = p.value                          # racy shared read
    expected_if_uncontended = mine * MULT + ADD
    state["shared_checks"][wid & 1023] += 1
    if got != expected_if_uncontended:
        # A sibling wrote a different backing between our write and read.  Its
        # value is still a VALID computed value (getter is a pure function of
        # whatever backing currently sits in the shared instance) -- this is
        # documented shared-object behavior, not corruption.
        state["shared_leaks"][wid & 1023] += 1


# Sustained checks per worker, bounded by H.running().  The descriptor-chain
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# creating single-owner Probes and reading `value` while sleep-PARKED across their
# yield, so the scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner
    descriptor-chain check (fail-fast) and the MEASURED shared-instance check
    (report only).  The two do not share data (fiber-local Probe vs shared pool),
    so running them in one fiber keeps the hub busy with mixed churn without the
    shared mutations ever reaching the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            prop_check(H, wid, idx, state)             # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_probe_check(H, wid, idx, state)     # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED Probe instances for the MEASURED arm.
    shared_pool = [Probe() for _ in range(8)]
    H.state = {
        "prop_checks": [0] * 1024,        # LOAD-BEARING single-owner checks
        "shared_pool": shared_pool,       # small shared Probe pool (MEASURED)
        "shared_checks": [0] * 1024,      # MEASURED shared-instance checks
        "shared_leaks": [0] * 1024,       # cross-fiber disagreements on shared
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["prop_checks"])
    schecks = sum(H.state["shared_checks"])
    sleaks = sum(H.state["shared_leaks"])
    spct = (100.0 * sleaks / schecks) if schecks else 0.0

    H.log("descriptor-chain[single-owner LOAD-BEARING]: {0} isolation checks "
          "(all passed fail-fast) | shared-instance[MEASURED]: {1} checks {2} "
          "cross-fiber disagreements ({3:.1f}%, documented shared-object behavior "
          "-- REPORT ONLY)".format(pchecks, schecks, sleaks, spct))

    if sleaks:
        H.log("note: the shared Probe pool observed {0} cross-fiber backing "
              "disagreements across {1} checks -- fibers write a shared instance's "
              "backing and a sibling overwrites it before the read.  The getter "
              "still returns a VALID computed value of whatever backing is present; "
              "this is documented M:N shared-object behavior, NOT a runloom bug, "
              "and never reaches the load-bearing single-owner oracle".format(
                  sleaks, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(pchecks > 0,
            "no single-owner descriptor-chain checks ran -- the load-bearing "
            "property/__getattribute__ hazard was never exercised (oracle would "
            "be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the getter
    # or __getattribute__ on a desynced `self`).
    H.require_no_lost("descriptor-chain isolation")


if __name__ == "__main__":
    harness.main(
        "p537_property_getattribute_descriptor_chain", body, setup=setup, post=post,
        default_funcs=8000,
        describe="a property (DATA descriptor: getter+setter) plus a custom "
                 "__getattribute__ run Python code on every attribute access.  "
                 "LOAD-BEARING: each fiber owns a Probe with a unique per-fiber "
                 "backing and a decoy planted in its instance __dict__ under the "
                 "property name; inst.value MUST return the getter's computed "
                 "function-of-backing (data descriptor beats the dict), stable "
                 "across a hub-migration yield, and the per-instance access "
                 "counter MUST equal this fiber's own access tally.  A decoy leak, "
                 "a value that changes across a yield, a vanished dict decoy, or "
                 "an inconsistent counter is the runloom descriptor-chain bug.  "
                 "MEASURED shared-instance arm (expected cross-fiber disagreement, "
                 "report-only) proves the hazard exists")
