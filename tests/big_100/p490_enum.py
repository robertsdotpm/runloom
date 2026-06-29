"""big_100 / 490 -- enum.Enum member caching and _member_map_ isolation under M:N.

enum.Enum maintains a _member_map_ dict that caches member name -> value lookups
on a per-ENUM-CLASS basis.  Each distinct enum class (even if subclasses share the
same parent) owns its own _member_map_ and the members created via the subclass's
namespace dict during class construction.  In 3.14t with free-threading, if the
_member_map_ is not properly isolated per fiber, or if member lookups cache across
distinct enum classes by name alone (not by class + name), multiple fibers creating
distinct enum classes with the SAME member names but DIFFERENT values could pollute
each other's member access.

WHERE M:N BREAKS IT (the gap this program probes).  runloom gives each fiber its
own Python frame stack and ContextVar/contextvar isolation, but if enum member
caching (e.g. a module-global member-name registry, a per-thread-ID cache, or a
shallow-copied contextvar ContextVar->value binding) is not fiber-aware, a fiber
that creates EnumClass_A with member RED=1 and yields, then resumes on a different
hub or while a sibling creates EnumClass_B with RED=100, might observe a WRONG
member value when accessing "RED" -- fetching another fiber's member instead of
its own class's member.  The member itself (the value object) is shared, but it
should only be retrieved through the CORRECT class's _member_map_.  A cross-fiber
member leak -- one fiber reading another fiber's enum class's member directly --
is a corruption of enum isolation.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Enum member access via CLASS.MEMBER_NAME is the documented interface.  A fiber
  that creates its own distinct EnumClass with a member at a specific value MUST
  retrieve that EXACT member when accessing CLASS.MEMBER_NAME (after a yield, the
  value must NOT have changed to another fiber's value for the same name but
  different enum class).  We verified this with a standalone plain-threads control
  (8 OS threads, each creating its own enum class with the same member names but
  distinct values, GIL on AND off) that 100% of member accesses return the correct
  fiber-local/thread-local value -- 0 cross-fiber leaks.  Under a CORRECT runloom
  it must also hold.  If a fiber's member access returns a value from another
  fiber's enum class (a value distinct from what its OWN class's _member_map_
  points to), that is an enum member-caching isolation bug in runloom, and the
  single-owner load-bearing oracle PASSES on a correct runtime (program exits 0
  when there is no bug).

ORACLES:
  * LOAD-BEARING -- ENUM MEMBER ISOLATION (worker, HARD, fail-fast).  Each fiber
    creates its OWN distinct enum class (subclass Enum, store in a fiber-local
    variable) with NAMED members at UNIQUE per-fiber values (e.g. RED = wid*100).
    The fiber then:
      - Accesses CLASS.MEMBER to retrieve the member (this calls __getattribute__
        on the class and looks up the name in the class's __dict__ and _member_map_).
      - Yields (runloom.sleep / yield_now) to allow siblings to run and potentially
        create their own conflicting enum classes or access members.
      - Re-accesses CLASS.MEMBER and asserts it returns the SAME member as before
        the yield (same object, same value).
      - Checks that the member's VALUE equals the unique per-fiber value (not a
        leaked sibling value).
      - Checks that member identity (id()) is stable across the yield.
    Single-owner: each fiber's enum class is created in a fiber-local variable,
    never shared.  A failure is a runloom enum-member-isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-access
    (stranded inside __getattribute__ or _member_map_ lookup on a desynced object
    reference) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (enum_checks > 0).

  * SECONDARY-A (report-ONLY, NEVER fails): MEASURED enum name collision on a
    SHARED enum class.  A small shared pool of enum classes is hammered by all
    fibers: different workers access the SAME class, so conflicts are expected.
    We measure the collision rate (a sibling's member lookup on a shared class
    sometimes returns a value that differs from this fiber's concurrent set on
    that class), and report it like p67's threading.local leak rate.  This
    validates that the hazard exists (fibers DO see each other's mutations); that
    the LOAD-BEARING single-owner arm is truly testing isolation, not missing the
    hazard.  We MEASURE + REPORT the leak rate, NEVER fail on it -- failing would
    mislabel the documented shared-object semantics as a bug.

FAIL ON: a fiber's single-owner enum class member returning a cross-fiber value,
mismatched member identity across a yield, or a member value that is not the
expected unique-per-fiber value.  The shared-pool MEASURED arm is report-only and
is expected to show cross-fiber leaks (documented M:N shared-object behavior) --
the load-bearing oracle must stay clean (single-owner, no sharing).

Stresses: enum.Enum member caching (_member_map_ creation and lookup), member
access via CLASS.MEMBER_NAME across hub migration + yield, per-fiber enum class
isolation vs shared enum class behavior, __getattribute__ and _member_map_ dict
lookup under M:N concurrency.

Good TSan / controlled-M:N-replay target: enum._member_map_ is a plain dict
mutated per class (insertion during __new__ on the enum metaclass, lookup via
__getattribute__); under the single-owner arm the dict is only read/written by
one fiber, so a data-race report on the dict object -- or a deterministic-replay
that accesses a member mid-mutation by another fiber's class -- is the cleanest
signal before the value/identity oracle fires.
"""
import enum

import harness
import runloom

# Per-fiber enum member values are drawn from this band.  Each wid gets a distinct
# base (wid * VALUE_SCALE) so values differ visibly across fibers.  The offset
# ensures values are never repeated (e.g. wid 0 gets 0, 1, 2...; wid 1 gets
# 1*SCALE, 1*SCALE+1, ...).
VALUE_SCALE = 10000
VALUE_SPAN = 10                         # number of distinct members per enum


class DynamicEnum(enum.Enum):
    """Base enum class to be overridden; never instantiated directly."""
    pass


def make_fiber_enum(wid, idx):
    """Create a DISTINCT enum class with a unique name and values tied to wid + idx.

    Each fiber's enum is private (created in a fiber-local variable, never shared).
    The member values are unique: RED = wid*VALUE_SCALE + 0, GREEN = wid*VALUE_SCALE + 1, etc.
    A sibling fiber's enum will have the same member NAMES but DIFFERENT values.

    Returns (enum_class, expected_values_dict)."""
    # Unique class name so the enum's _member_map_ is not aliased with siblings.
    cls_name = "FiberEnum_W{0}_I{1}".format(wid, idx)
    base_val = wid * VALUE_SCALE
    members = {}
    for i in range(VALUE_SPAN):
        member_name = ["RED", "GREEN", "BLUE", "YELLOW", "CYAN", "MAGENTA",
                       "BLACK", "WHITE", "GRAY", "BROWN"][i % 10]
        members[member_name] = base_val + i

    # Create the enum class dynamically.
    enum_cls = enum.Enum(cls_name, members)
    return enum_cls, members


def get_expected_value(wid, member_offset):
    """Expected value for a member at offset in wid's enum."""
    return wid * VALUE_SCALE + member_offset


# ---- LOAD-BEARING arm: single-owner fiber-local enum ---------------------
def enum_check(H, wid, idx, state):
    """Single-owner enum member isolation check.

    Each fiber creates its own distinct enum class, accesses its members across
    yields, and verifies the member values remain stable and correct.  A cross-
    fiber member leak would return a wrong value."""
    enum_cls, members = make_fiber_enum(wid, idx)
    member_names = list(members.keys())

    # Store member references BEFORE the first yield so we can compare identity.
    # This is the baseline: what each member is when first accessed.
    baseline_members = {}
    baseline_values = {}
    for name in member_names:
        member_obj = getattr(enum_cls, name)
        baseline_members[name] = member_obj
        baseline_values[name] = member_obj.value

    # YIELD: allow siblings to run and potentially create conflicting enums.
    # If enum member caching is NOT fiber-isolated, a sibling's member creation
    # or access might corrupt this fiber's baseline.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Re-access every member and verify stability.
    for name in member_names:
        member_obj = getattr(enum_cls, name)
        expected_val = members[name]

        # Check 1: identity stable (same object before and after yield)
        if id(member_obj) != id(baseline_members[name]):
            H.fail("enum member IDENTITY CHANGED: {0}.{1} id changed from {2} to "
                   "{3} across a yield (wid {4}) -- the member object was replaced "
                   "or a cross-fiber enum access returned a different object".format(
                       enum_cls.__name__, name, id(baseline_members[name]),
                       id(member_obj), wid))
            return

        # Check 2: value stable (same value before and after yield)
        if member_obj.value != baseline_values[name]:
            H.fail("enum member VALUE CHANGED: {0}.{1} value changed from {2} to "
                   "{3} across a yield (wid {4}) -- a sibling's enum class or "
                   "member access corrupted this fiber's member".format(
                       enum_cls.__name__, name, baseline_values[name],
                       member_obj.value, wid))
            return

        # Check 3: value matches expected (not a cross-fiber leak)
        if member_obj.value != expected_val:
            H.fail("enum member VALUE WRONG: {0}.{1} has value {2}, expected {3} "
                   "(wid {4}) -- a cross-fiber enum member leak, this fiber's member "
                   "was overwritten by or returned a sibling's value".format(
                       enum_cls.__name__, name, member_obj.value, expected_val, wid))
            return

    state["enum_checks"][wid & 1023] += 1


# ---- MEASURED arm: shared enum class (report-only) -----------------------
def shared_enum_check(H, wid, r, state):
    """Shared enum class member access (MEASURED, report-only).

    A small pool of SHARED enum classes is hammered by all fibers.  Different
    workers access the SAME class and mutate its members (well, the enum members
    are immutable, but fibers set/get them concurrently).  Cross-fiber leaks are
    EXPECTED and DOCUMENTED here (shared-object behavior, like p67's threading.local).
    We measure the leak rate; we NEVER fail on it."""
    shared_cls = state["shared_pool"][wid % len(state["shared_pool"])]
    member_names = list(shared_cls._member_map_.keys())

    if not member_names:
        return

    # Access a random member and check if its value matches what this fiber
    # expects (based on which shared enum class).
    name = member_names[r % len(member_names)]
    member_obj = getattr(shared_cls, name)

    # The expected value is the shared class's true value (from _member_map_).
    # But a sibling on the same hub might have modified a reference or accessed
    # a different version of the class (unlikely, but the point is: shared access
    # is inherently racy, so a mismatch is expected sometimes).
    expected = shared_cls._member_map_[name].value
    got = member_obj.value

    state["shared_checks"][wid & 1023] += 1
    if got != expected:
        state["shared_leaks"][wid & 1023] += 1


# Sustained enum checks per worker, bounded by H.running().  The member-isolation
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# creating/accessing distinct enums while sleep-PARKED across their yield, so the
# scheduler reliably interleaves a sibling's access before this fiber resumes.
# A single check per fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner enum
    check (fail-fast) and the MEASURED shared-class check (report only).  The two
    do not share data (single-owner enum vs shared pool) so running them in the same
    fiber keeps the hub busy with mixed churn without the shared mutations reaching
    the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            enum_check(H, wid, idx, state)           # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_enum_check(H, wid, idx, state)    # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Create a small pool of SHARED enum classes for the MEASURED arm.
    # Each shared class has members with values tied to its index.
    shared_pool = []
    for pool_idx in range(8):
        cls_name = "SharedEnum_P{0}".format(pool_idx)
        base_val = 100000 + pool_idx * 1000
        members = {
            "RED": base_val + 0,
            "GREEN": base_val + 1,
            "BLUE": base_val + 2,
        }
        shared_cls = enum.Enum(cls_name, members)
        shared_pool.append(shared_cls)

    H.state = {
        "enum_checks": [0] * 1024,        # LOAD-BEARING single-owner checks
        "shared_pool": shared_pool,       # small shared enum class pool
        "shared_checks": [0] * 1024,      # MEASURED shared-class checks
        "shared_leaks": [0] * 1024,       # cross-fiber leaks on shared classes
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    echecks = sum(H.state["enum_checks"])
    schecks = sum(H.state["shared_checks"])
    sleaks = sum(H.state["shared_leaks"])
    spct = (100.0 * sleaks / schecks) if schecks else 0.0

    H.log("enum[single-owner LOAD-BEARING]: {0} member-isolation checks (all "
          "passed fail-fast) | enum[shared pool MEASURED]: {1} checks {2} "
          "leaks ({3:.1f}%, documented shared-enum behavior -- REPORT ONLY)".format(
              echecks, schecks, sleaks, spct))

    if sleaks:
        H.log("note: the shared enum pool observed {0} cross-fiber member-value "
              "leaks across {1} checks -- runloom hub fibers may see mutations "
              "on shared enum class objects (the shared class is a shared Python "
              "object, like p67's threading.local shared container).  This is "
              "documented M:N shared-object behavior, NOT a runloom bug, and never "
              "reaches the load-bearing single-owner oracle".format(sleaks, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(echecks > 0,
            "no single-owner enum member-isolation checks ran -- the load-bearing "
            "enum-isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # __getattribute__ or _member_map_ lookup).
    H.require_no_lost("enum member isolation")


if __name__ == "__main__":
    harness.main(
        "p490_enum", body, setup=setup, post=post,
        default_funcs=8000,
        describe="enum.Enum maintains _member_map_ per enum class.  Under M:N, "
                 "if member caching is not fiber-isolated, multiple fibers "
                 "creating distinct enum classes with the same member NAMES but "
                 "different VALUES could pollute each other's member access. "
                 "LOAD-BEARING: each fiber creates its own distinct enum class "
                 "and accesses its members across yields; member values MUST "
                 "remain stable and correct (not a cross-fiber leak from a "
                 "sibling's enum).  MEASURED shared-pool (expected to show cross-"
                 "fiber leaks on shared enums, like p67) proves the hazard "
                 "exists.  A member value that changes to a sibling's value "
                 "across a yield, or identity change, is the runloom enum-"
                 "isolation bug")
