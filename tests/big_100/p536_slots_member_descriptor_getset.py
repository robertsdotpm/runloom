"""big_100 / 536 -- __slots__ member_descriptor __get__/__set__ offset integrity under M:N.

A class with __slots__ has NO per-instance __dict__.  Instead the type layout
reserves a fixed run of PyObject* cells in the instance's C struct, and for every
declared slot name the class __dict__ holds a `member_descriptor` object whose
job is a single job: it stores a fixed byte OFFSET into the instance and, on
__get__/__set__, reads/writes the PyObject* cell at exactly that offset.  There is
no name lookup at access time -- `inst.s3 = x` is "store x at offset(s3)" and
`inst.s3` is "load the object at offset(s3)".  The correctness of every attribute
access on a __slots__ instance therefore rests entirely on:
  (a) the member_descriptor's stored offset being the RIGHT offset for that name,
  (b) the instance's layout matching the type's expected layout, and
  (c) the instance genuinely having no __dict__ fallback (so a mistyped/undeclared
      name has nowhere to hide and MUST raise AttributeError, never silently land
      in a phantom dict).

WHERE M:N COULD BREAK IT (the gap this program probes).  The member_descriptor is
a shared, immutable, module-level object (one per slot name, installed once at
class-creation time).  The INSTANCE, by contrast, is fiber-local.  Every fiber
creates its own instance, writes a unique per-wid value into each slot, yields to
let siblings run on other hubs, and then reads every slot back.  If a hub
migration across the yield were to (1) tear the shared member_descriptor's offset
field so a later __get__ reads the WRONG slot cell, (2) tear the instance's
layout so slot cells shift under the descriptor's fixed offset, or (3) resurrect a
phantom __dict__ on the __slots__ instance so an undeclared write silently
succeeds -- then a fiber would read back a value that is NOT the exact one it
wrote, or an undeclared attribute would fail to raise, or __dict__ would appear.
On a correct runtime none of these can happen: the descriptor offset is a plain C
size_t set once, the instance layout is fixed at allocation, and __slots__
suppresses __dict__ structurally.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  A __slots__ instance is a SINGLE-OWNER object: exactly one fiber allocates it,
  writes to it, and reads from it.  There is no sharing, so the ordinary "shared
  mutable container races GIL-off" caveat does not apply -- this is genuine
  isolation, exactly like p490's fiber-local enum.  We verified the oracle with a
  standalone plain-threads control (8 OS threads, each building its own __slots__
  instance, writing unique per-thread values into every slot, yielding via
  time.sleep(0), reading back, GIL on AND off): 100% of slot reads return the
  exact written value, every undeclared-attribute set raises AttributeError, and
  no instance ever grows a __dict__ -- 0 anomalies.  Under a CORRECT runloom it
  must also hold: the single-owner load-bearing oracle PASSES on a correct runtime
  (program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- SLOT OFFSET / NO-__dict__ INTEGRITY (worker, HARD, fail-fast).
    Each fiber owns one instance of a module-level __slots__ class Cell (many
    slots so the offset arithmetic spans several cells).  Per check the fiber:
      - writes a UNIQUE per-(wid,slot) value into every slot (val = base+slot_idx,
        base = wid*VALUE_SCALE + local_counter), inserting a yield partway through
        the writes so a sibling is mid-flight against the shared descriptors;
      - yields (yield_now / sleep) so siblings run on other hubs;
      - reads every slot back and asserts it equals EXACTLY the value written
        (a mismatch = the descriptor read the wrong cell, or the cell was torn);
      - asserts setting an UNDECLARED attribute raises AttributeError (proving no
        phantom __dict__ absorbed it);
      - asserts the instance exposes NO __dict__ (hasattr(inst,'__dict__') False,
        and type(inst).__slots__ is intact);
      - asserts each slot's member_descriptor is still the module-level object
        (identity), i.e. the class __dict__ entry was not swapped.
    Single-owner: the instance is created in a fiber-local variable, never shared.
    A failure is a runloom slot-offset / instance-layout isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a slot
    __get__/__set__ (a wedged descriptor access) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (slot_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): a small pool of SHARED Cell instances is
    hammered by all fibers -- every fiber writes then reads the SAME shared
    instance's slots.  Cross-fiber value observation is EXPECTED here (a shared
    __slots__ instance is a shared mutable object, exactly like a shared dict; the
    per-slot store is a plain non-atomic PyObject* write).  We MEASURE how often a
    fiber reads back a value different from the one it just wrote and REPORT it
    like p490's shared-enum arm -- we NEVER fail on it, because failing would
    mislabel documented shared-object semantics as a runtime bug.  This proves the
    hazard window is real (fibers DO interleave on the shared slots) so the
    single-owner arm is genuinely testing isolation, not missing the hazard.

FAIL ON: a single-owner __slots__ instance reading back a slot value different
from the exact value written across a yield, an undeclared-attribute set that does
NOT raise AttributeError, a __slots__ instance sprouting a __dict__, a slot
descriptor identity swap, or a SIGSEGV in a slot __get__/__set__.  The shared-pool
MEASURED arm is report-only and may show cross-fiber value differences (documented
M:N shared-object behavior) -- the load-bearing single-owner oracle must stay
clean.

Distinct from p451 (__set_name__ install race, which probes descriptor
INSTALLATION during class creation): this probes member_descriptor __get__/__set__
OFFSET integrity on an already-built, single-owner instance across hub migration.

Stresses: __slots__ member_descriptor __get__/__set__ over fixed C offsets, fixed
instance layout, __dict__ suppression, undeclared-attribute AttributeError, slot
descriptor identity, all under M:N hub migration + yields.

Good TSan / controlled-M:N-replay target: the member_descriptor offset is a shared
immutable C field read on every slot access while sibling fibers write their own
instances' cells; under the single-owner arm the cells are touched by one fiber
only, so a data-race report on a slot cell -- or a replay reading a slot mid-write
by another fiber's instance -- is the cleanest signal before the value oracle fires.
"""
import harness
import runloom

# Per-fiber slot values are drawn from this band.  base = wid*VALUE_SCALE + local
# so every fiber's writes are visibly distinct from siblings'.  VALUE_SCALE is far
# larger than NSLOTS so slot values never collide across (wid, local) pairs.
VALUE_SCALE = 1 << 20

# Number of declared slots.  Enough cells that the descriptor offset arithmetic
# spans several PyObject* slots (a torn/wrong offset lands on a neighbour cell).
NSLOTS = 12

SLOT_NAMES = tuple("s{0}".format(i) for i in range(NSLOTS))


class Cell(object):
    """Module-level __slots__ class.  Immutable at the type level: created once,
    its member_descriptors installed once.  Instances are fiber-local.

    NSLOTS declared slots => NSLOTS member_descriptors in Cell.__dict__, each with
    a fixed byte offset into the instance struct, and NO __dict__ on instances."""
    __slots__ = SLOT_NAMES


# Snapshot the module-level member_descriptor objects so the load-bearing arm can
# assert the class __dict__ entries were never swapped out under it.  These are the
# shared immutable descriptors every fiber's slot access routes through.
SLOT_DESCRIPTORS = {name: Cell.__dict__[name] for name in SLOT_NAMES}


# ---- LOAD-BEARING arm: single-owner __slots__ instance -------------------
def slot_check(H, wid, local, state):
    """Single-owner slot offset / no-__dict__ integrity check.

    One fiber-local Cell instance: write a unique value per slot, yield, read back
    and verify EXACT equality + no phantom __dict__ + undeclared-attr raises."""
    inst = Cell()
    base = (wid * VALUE_SCALE) + (local & (VALUE_SCALE - 1 - NSLOTS))

    # Write a unique per-slot value; yield partway so a sibling interleaves mid-write.
    expected = {}
    for i, name in enumerate(SLOT_NAMES):
        val = base + i
        setattr(inst, name, val)
        expected[name] = val
        if i == NSLOTS // 2:
            runloom.yield_now()            # sibling runs while our writes are half done

    # YIELD: allow siblings on other hubs to run their own writes/reads.
    runloom.yield_now()
    if local & 1:
        runloom.sleep(0.0002)

    # Read every slot back through its member_descriptor.__get__ and verify EXACT
    # equality with what THIS fiber wrote (a wrong offset reads a neighbour cell).
    for name in SLOT_NAMES:
        got = getattr(inst, name)
        want = expected[name]
        if got is not want and got != want:
            H.fail("slot VALUE WRONG: {0} read back {1!r}, wrote {2!r} (wid {3}) "
                   "-- the member_descriptor read the WRONG slot cell or the cell "
                   "was torn across a hub migration".format(name, got, want, wid))
            return
        # The value must be identity-stable too (the same int object we stored).
        if got is not want:
            H.fail("slot IDENTITY CHANGED: {0} is a different object than the one "
                   "written (wid {1}) -- the slot cell was overwritten by another "
                   "fiber's PyObject* store".format(name, wid))
            return

    # No phantom __dict__: a __slots__ instance must NOT have a __dict__.
    if hasattr(inst, "__dict__"):
        H.fail("__slots__ instance grew a __dict__ (wid {0}) -- the slot layout "
               "was corrupted so instances fell back to dict storage".format(wid))
        return

    # An UNDECLARED attribute must raise AttributeError (nowhere to hide without a
    # __dict__).  If it silently succeeds, a phantom __dict__ absorbed it.
    try:
        setattr(inst, "undeclared_x", 123)
    except AttributeError:
        pass
    else:
        H.fail("setting an UNDECLARED attribute on a __slots__ instance did NOT "
               "raise AttributeError (wid {0}) -- a phantom __dict__ absorbed the "
               "write, i.e. the __slots__ layout was defeated".format(wid))
        return

    # The class __dict__ member_descriptors must still be the exact module-level
    # objects (identity) -- not swapped under us across the yield.
    for name in SLOT_NAMES:
        if Cell.__dict__[name] is not SLOT_DESCRIPTORS[name]:
            H.fail("slot DESCRIPTOR SWAPPED: Cell.__dict__[{0!r}] is no longer the "
                   "module-level member_descriptor (wid {1}) -- the class __dict__ "
                   "entry was replaced under a live instance".format(name, wid))
            return

    state["slot_checks"][wid & 1023] += 1


# ---- MEASURED arm: shared __slots__ instance (report-only) ---------------
def shared_slot_check(H, wid, local, state):
    """Shared Cell instance slot access (MEASURED, report-only).

    All fibers write then read the SAME shared instance's slots.  Cross-fiber value
    differences are EXPECTED (shared mutable object; the per-slot store is a plain
    non-atomic PyObject* write, exactly like a shared dict).  We MEASURE the
    mismatch rate; we NEVER fail on it."""
    pool = state["shared_pool"]
    inst = pool[wid % len(pool)]
    name = SLOT_NAMES[local % NSLOTS]
    val = (wid * VALUE_SCALE) + (local & 0xFFFF)

    setattr(inst, name, val)
    runloom.yield_now()                    # sibling can overwrite the same slot here
    got = getattr(inst, name)

    state["shared_checks"][wid & 1023] += 1
    if got != val:
        state["shared_diffs"][wid & 1023] += 1


# Sustained checks per worker, bounded by H.running().  The offset-integrity
# hazard (if any) manifests only under sustained churn: many fibers building and
# tearing down single-owner instances while sleep-PARKED across their yield, so the
# scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner slot
    check (fail-fast) and the MEASURED shared-instance check (report only).  They
    share no data (fiber-local instance vs shared pool) so the shared writes never
    reach the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        local = 0
        while H.running() and local < INNER_CAP:
            slot_check(H, wid, local, state)           # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_slot_check(H, wid, local, state)    # MEASURED (report only)
            H.op(wid)
            local += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED Cell instances for the MEASURED arm.
    shared_pool = [Cell() for _ in range(8)]
    for inst in shared_pool:
        for name in SLOT_NAMES:
            setattr(inst, name, 0)

    H.state = {
        "slot_checks": [0] * 1024,        # LOAD-BEARING single-owner checks
        "shared_pool": shared_pool,       # small shared instance pool
        "shared_checks": [0] * 1024,      # MEASURED shared-instance checks
        "shared_diffs": [0] * 1024,       # cross-fiber value differences (shared)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["slot_checks"])
    schecks = sum(H.state["shared_checks"])
    sdiffs = sum(H.state["shared_diffs"])
    spct = (100.0 * sdiffs / schecks) if schecks else 0.0

    H.log("slots[single-owner LOAD-BEARING]: {0} offset/no-__dict__ integrity "
          "checks (all passed fail-fast) | slots[shared pool MEASURED]: {1} "
          "checks {2} cross-fiber value diffs ({3:.1f}%, documented shared-object "
          "behavior -- REPORT ONLY)".format(checks, schecks, sdiffs, spct))

    if sdiffs:
        H.log("note: the shared Cell pool observed {0} cross-fiber slot-value "
              "differences across {1} checks -- runloom hub fibers see each "
              "other's plain PyObject* slot stores on a SHARED instance (a shared "
              "mutable object, like a shared dict).  This is documented M:N "
              "shared-object behavior, NOT a runloom bug, and never reaches the "
              "load-bearing single-owner oracle".format(sdiffs, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(checks > 0,
            "no single-owner slot integrity checks ran -- the load-bearing "
            "member_descriptor offset hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a slot __get__/__set__.
    H.require_no_lost("slots member_descriptor offset integrity")


if __name__ == "__main__":
    harness.main(
        "p536_slots_member_descriptor_getset", body, setup=setup, post=post,
        default_funcs=8000,
        describe="__slots__ member_descriptors store a fixed byte OFFSET into the "
                 "instance; every attribute access on a __slots__ instance is a "
                 "raw load/store at that offset with no name lookup and no __dict__ "
                 "fallback.  LOAD-BEARING: each fiber owns one instance of a "
                 "module-level __slots__ class, writes a unique value into every "
                 "slot, yields (hub migration), then asserts every slot reads back "
                 "its EXACT written value, an undeclared attribute raises "
                 "AttributeError, and the instance has NO __dict__.  MEASURED "
                 "shared-instance pool (expected to show cross-fiber value diffs, "
                 "like p490) proves the hazard window is real.  A slot reading a "
                 "wrong/torn value, a defeated __slots__ (phantom __dict__ / no "
                 "AttributeError), or a descriptor identity swap is the runloom bug")
