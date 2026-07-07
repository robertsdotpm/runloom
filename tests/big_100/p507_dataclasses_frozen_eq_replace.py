"""big_100 / 507 -- dataclasses.make_dataclass frozen type assembly + eq/replace
isolation under M:N.

dataclasses.make_dataclass(name, fields, frozen=True) BUILDS a brand-new class at
call time: it collects the field specs, then GENERATES the source of __init__,
__eq__ and (for a frozen+eq class) __hash__ as strings and runs them through
exec() into a FRESH namespace dict, installs the resulting functions onto the
type, and installs per-field descriptors + the module-global _HAS_DEFAULT_FACTORY
sentinel.  That exec-into-a-namespace assembly, plus the descriptor install, is a
lot of transient per-call machinery.

WHERE M:N COULD BREAK IT (the gap this program probes).  Every fiber loops
building its OWN frozen dataclass.  Tens of thousands of fibers on >1 hub run
make_dataclass concurrently, GIL off, so many are inside the exec-driven class
assembly at the same instant.  If a hub-migration or a preemption during this
fiber's class construction let a SIBLING's generated __init__/__eq__/__hash__
function object, or a sibling's exec namespace, or a field descriptor, bind onto
THIS fiber's type, then this fiber's instances would carry the wrong constructor,
compare wrong, hash wrong, or replace() would splice a sibling's field slot.  The
class object, its methods, and every instance built from it are FIBER-LOCAL
(created in fiber-local variables, never shared), so on a CORRECT runtime every
one of the closed-form laws below MUST hold; a violation is a runloom type-
assembly / method-binding isolation bug, never documented Python semantics.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  A frozen dataclass is a pure value type.  For a fiber-local frozen instance
  built from a KNOWN per-wid field vector, the following are exact closed-form
  laws with a single owner (no sharing, so no documented-race escape hatch):

    * every field reads back its exact constructed value, unchanged across a yield
      (the descriptor + instance layout is intact);
    * hash(inst) is stable across the yield (the generated __hash__ over the field
      tuple did not get swapped for a sibling's);
    * asdict(inst) equals the known field->value map;
    * dataclasses.replace(inst) with NO changes produces a NEW, EQUAL instance
      (identity replace: __init__ + __eq__ round-trip);
    * dataclasses.replace(inst, one=new) changes EXACTLY that one field and leaves
      the original instance untouched (frozen), and the result is NOT equal to the
      original;
    * a fresh instance built from the SAME values compares == inst (eq is a pure
      function of the field tuple);
    * setattr on the frozen instance raises FrozenInstanceError (the frozen
      __setattr__ was installed, not a sibling's mutable one).

  All fiber-local -> a correct runtime passes every check and the program exits 0.
  A field value that changed across the yield, a hash that drifted, a wrong
  replace target, a broken eq, or a missing FrozenInstanceError is a runloom bug.

ORACLES:
  * LOAD-BEARING -- FROZEN DATACLASS ASSEMBLY + EQ/REPLACE (worker, HARD, fail-
    fast).  Each fiber make_dataclass()es its OWN frozen type (unique name, fields
    seeded by wid+idx), instantiates it, snapshots hash + fields BEFORE a yield,
    then re-verifies every law above AFTER the yield.  Single-owner throughout.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside exec-
    driven class assembly or a descriptor lookup on a desynced type never returns;
    the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (dc_checks > 0).

FAIL ON: a frozen-instance field that changed value/identity across a yield, a
hash that drifted, asdict mismatch, replace() touching the wrong field or mutating
the original, a fresh-equal instance that compares unequal, or a frozen setattr
that did NOT raise FrozenInstanceError.  There is no shared object anywhere in the
load-bearing arm, so any failure is a real runtime type-assembly desync.

Stresses: dataclasses.make_dataclass exec-into-namespace class assembly, generated
__init__/__eq__/__hash__ method binding, frozen __setattr__ install, field
descriptor offset integrity, dataclasses.replace / asdict, and eq/hash purity
across hub migration + yield under M:N.

Good TSan / controlled-M:N-replay target: make_dataclass runs exec() and installs
freshly-generated function objects onto a new type every iteration; a data-race
report on the type's __dict__ / method slot during a concurrent sibling assembly,
or a deterministic replay that binds a sibling's generated method, localizes the
desync before the eq/replace value oracle even fires.
"""
import dataclasses

import harness
import runloom

# Number of fields on each fiber-local frozen dataclass.  Enough that the
# generated __init__/__eq__/__hash__ over the field tuple is non-trivial and the
# descriptor install has several slots to get right, small enough to stay CPU
# cheap under tens of thousands of fibers.
FIELD_COUNT = 5

# Per-fiber value band.  base = wid * VALUE_SCALE + idx makes every fiber's field
# vector distinct from every other fiber's, so a cross-fiber field leak surfaces
# as a WRONG value (from a different wid's band), not just an identity blip.  Big
# enough that wid bands never overlap across the idx range a run reaches.
VALUE_SCALE = 100000000

# Distinct delta applied by the single-field replace() so the replaced value can
# never collide with any original field value in this fiber's vector.
REPLACE_DELTA = 999999999


def make_fiber_dc(wid, idx):
    """Build a DISTINCT frozen dataclass with a unique name and a per-wid+idx field
    vector.  The class, its methods, and (later) its instances are fiber-local --
    created in fiber-local variables, never shared.

    Returns (cls, field_names, values) where values maps field name -> the exact
    int it will be constructed with."""
    cls_name = "FiberDC_W{0}_I{1}".format(wid, idx)
    field_names = ["f{0}".format(i) for i in range(FIELD_COUNT)]
    # make_dataclass runs exec() to generate __init__/__eq__/__hash__ into a fresh
    # namespace, then installs them + the field descriptors onto this new type.
    cls = dataclasses.make_dataclass(
        cls_name, [(fn, int) for fn in field_names], frozen=True)
    base = wid * VALUE_SCALE + idx
    values = {fn: base + i for i, fn in enumerate(field_names)}
    return cls, field_names, values


def dc_check(H, wid, idx, state):
    """Single-owner frozen-dataclass assembly + eq/replace law check.

    Every object here is fiber-local; a violation is a runloom type-assembly /
    method-binding desync, never documented Python behavior."""
    cls, field_names, values = make_fiber_dc(wid, idx)
    inst = cls(**values)                     # frozen, fiber-local instance

    # Snapshot BEFORE the yield: hash + each field value + asdict.
    h0 = hash(inst)
    baseline = {fn: getattr(inst, fn) for fn in field_names}
    d0 = dataclasses.asdict(inst)

    # YIELD: let siblings run -- many are inside their own make_dataclass exec
    # assembly right now.  If method/namespace/descriptor binding is not fiber-
    # isolated, this fiber's type or instance could be corrupted while parked.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # --- 1. every field reads back its exact constructed value, unchanged -------
    for fn in field_names:
        got = getattr(inst, fn)
        if got != baseline[fn]:
            H.fail("frozen field VALUE CHANGED across yield: {0}.{1} was {2} now "
                   "{3} (wid {4}) -- a sibling's class assembly corrupted this "
                   "fiber's instance".format(
                       cls.__name__, fn, baseline[fn], got, wid))
            return
        if got != values[fn]:
            H.fail("frozen field VALUE WRONG: {0}.{1} == {2}, expected {3} (wid "
                   "{4}) -- a cross-fiber field/descriptor leak".format(
                       cls.__name__, fn, got, values[fn], wid))
            return

    # --- 2. hash stable across the yield (generated __hash__ intact) -----------
    h1 = hash(inst)
    if h1 != h0:
        H.fail("frozen instance HASH DRIFTED across yield: was {0} now {1} (wid "
               "{2}) -- the generated __hash__ over the field tuple was swapped or "
               "the field storage torn".format(h0, h1, wid))
        return

    # --- 3. asdict matches the known field vector ------------------------------
    d1 = dataclasses.asdict(inst)
    if d1 != values:
        H.fail("asdict MISMATCH: {0!r} != expected {1!r} (wid {2}) -- torn field "
               "storage or a cross-fiber descriptor".format(d1, values, wid))
        return
    if d1 != d0:
        H.fail("asdict CHANGED across yield: {0!r} -> {1!r} (wid {2})".format(
            d0, d1, wid))
        return

    # --- 4. identity replace() -> a NEW, EQUAL instance ------------------------
    same = dataclasses.replace(inst)
    if same != inst:
        H.fail("identity replace() NOT EQUAL: replace(inst) != inst (wid {0}) -- "
               "__init__/__eq__ round-trip broken by a sibling's generated "
               "method".format(wid))
        return
    if same is inst:
        H.fail("replace() returned the SAME object instead of a new instance "
               "(wid {0})".format(wid))
        return

    # --- 5. single-field replace() changes EXACTLY one field -------------------
    target = field_names[idx % FIELD_COUNT]
    newval = values[target] + REPLACE_DELTA
    replaced = dataclasses.replace(inst, **{target: newval})
    for fn in field_names:
        exp = newval if fn == target else values[fn]
        got = getattr(replaced, fn)
        if got != exp:
            H.fail("replace() touched the WRONG field: after replace({0}={1}), "
                   "{2}.{3} == {4}, expected {5} (wid {6}) -- replace bound a "
                   "sibling's field slot".format(
                       target, newval, cls.__name__, fn, got, exp, wid))
            return
    # original instance is untouched (frozen)
    for fn in field_names:
        if getattr(inst, fn) != values[fn]:
            H.fail("replace() MUTATED the original frozen instance: {0}.{1} == "
                   "{2}, expected {3} (wid {4})".format(
                       cls.__name__, fn, getattr(inst, fn), values[fn], wid))
            return
    if replaced == inst:
        H.fail("single-field replace() produced an EQUAL instance (wid {0}) -- "
               "eq ignored the changed field".format(wid))
        return

    # --- 6. a fresh instance with the SAME values compares == inst -------------
    fresh = cls(**values)
    if fresh != inst:
        H.fail("eq BROKEN: a fresh instance with identical field values != inst "
               "(wid {0}) -- generated __eq__ is not a pure function of the field "
               "tuple".format(wid))
        return
    if hash(fresh) != h0:
        H.fail("eq/hash INCONSISTENT: fresh-equal instance hashes {0}, inst hashes "
               "{1} (wid {2})".format(hash(fresh), h0, wid))
        return

    # --- 7. frozen __setattr__ was installed (raises FrozenInstanceError) ------
    try:
        setattr(inst, field_names[0], 12345)
        H.fail("frozen NOT ENFORCED: setattr on a frozen instance succeeded (wid "
               "{0}) -- a sibling's mutable __setattr__ was bound onto this frozen "
               "type".format(wid))
        return
    except dataclasses.FrozenInstanceError:
        pass

    state["dc_checks"][wid & 1023] += 1


# Sustained assembly churn per worker, bounded by H.running().  The exec-into-
# namespace / method-binding hazard only manifests under many fibers simultaneously
# inside make_dataclass while others park across a yield -- a single build per fiber
# barely overlaps a sibling's assembly and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            dc_check(H, wid, idx, state)         # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # dc_checks is a NON-VACUITY tally only (not a conservation law), so the
    # sharded wid&1023 slot table is race-free-enough for a count -- one fiber may
    # alias another's slot at wid>=1024, which can only UNDERcount the tally, never
    # fabricate a pass (it is compared > 0).
    H.state = {"dc_checks": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dchecks = sum(H.state["dc_checks"])
    H.log("dataclasses[single-owner LOAD-BEARING]: {0} frozen-type assembly + "
          "eq/replace law checks (all passed fail-fast); ops={1}".format(
              dchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing arm actually exercised the assembly hazard.
    H.check(dchecks > 0,
            "no frozen-dataclass assembly checks ran -- the make_dataclass exec-"
            "into-namespace hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside exec-driven
    # class assembly or a descriptor lookup on a desynced type).
    H.require_no_lost("dataclasses frozen eq/replace")


if __name__ == "__main__":
    harness.main(
        "p507_dataclasses_frozen_eq_replace", body, setup=setup, post=post,
        default_funcs=5000,
        describe="dataclasses.make_dataclass builds a frozen type by exec()ing "
                 "generated __init__/__eq__/__hash__ into a fresh namespace and "
                 "installing field descriptors.  Under M:N, if a hub-migration "
                 "during that class assembly bound a sibling's generated method / "
                 "namespace / descriptor onto this fiber's type, instances would "
                 "compare/hash/replace wrong.  LOAD-BEARING: each fiber "
                 "make_dataclass()es its OWN frozen type seeded by wid, then across "
                 "a yield asserts every field stable, hash unchanged, asdict "
                 "matches, replace(one=new) changes exactly one field, a fresh "
                 "instance compares == inst, and frozen setattr raises "
                 "FrozenInstanceError.  All fiber-local, so any failure is a "
                 "runloom type-assembly isolation bug")
