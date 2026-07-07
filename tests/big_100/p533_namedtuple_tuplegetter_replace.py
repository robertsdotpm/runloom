"""big_100 / 533 -- collections.namedtuple _tuplegetter descriptor + _replace isolation under M:N.

collections.namedtuple() dynamically builds a tuple subclass whose named fields
are exposed via C-level `_tuplegetter` member descriptors (operator.itemgetter-
style objects stored in the class dict, each holding a FIXED integer index).  When
you write `pt.x`, __getattribute__ finds the `_tuplegetter` descriptor for "x" on
the class, and its __get__ returns `tuple.__getitem__(pt, <index>)` -- i.e. the
field access is a C read of a fixed slot of an IMMUTABLE tuple.  `_asdict()` walks
the field names against the tuple's positions; `_replace(**kw)` builds a BRAND-NEW
tuple from `_make(map(kw.pop, self._fields, self))`, i.e. it reads every current
field and overrides the named ones.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its own
namedtuple CLASS and one INSTANCE holding unique per-wid values.  The instance is
an immutable tuple, so once built its slots can never legitimately change.  The
hazards a broken runtime would expose:

  * a torn `_tuplegetter` descriptor: across a hub migration the descriptor's
    stored index is read while a sibling's (identically-named) descriptor is being
    built, so `pt.x` returns the value of the WRONG position -- access-by-name no
    longer equals access-by-index;
  * a torn tuple read: `tuple.__getitem__` mid-migration returns a slot from a
    sibling's instance (a cross-fiber leak of single-owner state), so a field value
    changes across a yield even though the tuple is immutable;
  * `_replace(x=new)` inheriting a sibling's slot: the new tuple built by `_make`
    over `map(kw.pop, self._fields, self)` picks up a value from another fiber's
    instance in a non-replaced position.

Because the instance is a SINGLE-OWNER immutable tuple, EVERY one of these is a
real runtime corruption, never documented Python semantics: an immutable tuple's
slots cannot change, and a class's own `_tuplegetter` index is fixed at class-
build time.  A plain-threads control (each thread building its own namedtuple with
the same field names but distinct values, GIL on AND off) returns 100% correct
field/index/_asdict/_replace results with zero cross-thread leaks; under a correct
runloom it must also hold.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  * LOAD-BEARING -- SINGLE-OWNER NAMEDTUPLE ISOLATION (worker, HARD, fail-fast).
    Each fiber builds its OWN namedtuple class (unique typename + fixed field
    names) and one instance with UNIQUE per-wid field values (field i == wid*SCALE
    + i).  It snapshots the per-field ids and values, YIELDS (yield_now / tiny
    sleep) so siblings interleave, then re-verifies, for every field:
      - access-by-NAME (getattr) == access-by-INDEX (pt[i]) == expected value;
      - the field value object's id() is stable across the yield (no torn read);
      - `_asdict()` round-trips: keys == _fields, values == the tuple positions;
      - `_replace(field_i=sentinel)` returns a NEW tuple (distinct id) equal to
        the original in EVERY position except i, where it holds the sentinel;
      - the original instance is UNCHANGED by _replace (immutability preserved).
    Single-owner: the class and instance live in fiber-local variables, never
    shared.  Any mismatch is a runloom tuple/descriptor isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    __getattribute__ / _tuplegetter.__get__ / _make never returns; caught here.

  * NON-VACUITY (post, HARD): the load-bearing arm ran (nt_checks > 0).

FAIL ON: access-by-name != access-by-index, a field value/identity change across a
yield on a single-owner immutable tuple, an _asdict() that does not round-trip, an
_replace() that alters a non-replaced position or mutates the original, or a field
value that is not the expected unique-per-fiber value (a cross-fiber slot leak).

Stresses: collections.namedtuple class construction (exec-built __new__ + per-field
_tuplegetter descriptors), C member-descriptor __get__ over an immutable tuple's
fixed slot, _asdict()/_replace()/_make() building fresh tuples, tuple immutability
and per-fiber class isolation across hub migration + yield under M:N concurrency.

Good TSan / controlled-M:N-replay target: the _tuplegetter descriptor's index read
and tuple.__getitem__ slot read are C reads that, if racing a sibling's class build
or instance under a broken scheduler, surface as a data-race report on the tuple/
descriptor before the value/identity oracle even fires.
"""
import collections

import harness
import runloom

# Per-fiber field values are drawn from this band.  field i of wid's instance ==
# wid*VALUE_SCALE + i, so every field of every fiber has a distinct value and a
# cross-fiber slot leak shows up as a wrong (but recognizable) number.
VALUE_SCALE = 100000

# Fixed field names for every fiber's namedtuple.  Names are SHARED across fibers
# (so a broken name->index cache would alias), but each fiber's CLASS is distinct
# and its VALUES are unique -- the isolation the oracle asserts.
FIELD_NAMES = ["alpha", "bravo", "charlie", "delta", "echo",
               "foxtrot", "golf", "hotel"]
NFIELDS = len(FIELD_NAMES)

# Sentinel base for _replace: distinct from any field value band so a replaced
# slot is unmistakable.
REPLACE_SENTINEL = 0x7EFEFEFE

# Sustained checks per worker: the isolation hazard only manifests under sustained
# churn -- many fibers building/reading distinct namedtuples while sleep-PARKED
# across their yield, so a sibling's access reliably interleaves before this fiber
# resumes.  A single check per fiber barely overlaps and does not reproduce.
INNER_CAP = 100000


def make_fiber_nt(wid, idx):
    """Build a DISTINCT namedtuple class (unique typename) and one instance with
    UNIQUE per-wid field values.  Private to the fiber; never shared.

    Returns (nt_class, instance, expected_values_list)."""
    typename = "FiberNT_W{0}_I{1}".format(wid, idx)
    nt_cls = collections.namedtuple(typename, FIELD_NAMES)
    base = wid * VALUE_SCALE
    # Distinct value objects per field (int object identity matters for the id()
    # stability check; use ints beyond the small-int cache so id() is meaningful).
    values = [base + i for i in range(NFIELDS)]
    inst = nt_cls(*values)
    return nt_cls, inst, values


def nt_check(H, wid, idx, state):
    """Single-owner namedtuple isolation check (fail-fast).

    Builds a fiber-local namedtuple + instance with unique values, yields, then
    verifies field access-by-name==by-index==expected, id-stability, _asdict()
    round-trip, and _replace() semantics on the immutable single-owner tuple."""
    nt_cls, inst, values = make_fiber_nt(wid, idx)

    # Snapshot per-field value-object identities BEFORE the yield.
    baseline_ids = [id(inst[i]) for i in range(NFIELDS)]

    # YIELD: allow siblings to build/read their own conflicting namedtuples.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # ---- verify every field on the single-owner immutable instance ------------
    for i in range(NFIELDS):
        name = FIELD_NAMES[i]
        expected = values[i]

        by_name = getattr(inst, name)
        by_index = inst[i]

        # Check 1: access-by-name == access-by-index (the _tuplegetter descriptor's
        # stored index must match the tuple position).
        if by_name != by_index:
            H.fail("namedtuple ACCESS DESYNC: {0}.{1} (by-name)={2} != "
                   "instance[{3}] (by-index)={4} (wid {5}) -- the _tuplegetter "
                   "descriptor's index no longer matches the tuple slot; a torn "
                   "descriptor read across a yield".format(
                       nt_cls.__name__, name, by_name, i, by_index, wid))
            return

        # Check 2: value matches expected (not a cross-fiber slot leak).
        if by_name != expected:
            H.fail("namedtuple VALUE WRONG: {0}.{1}={2}, expected {3} (wid {4}) "
                   "-- a cross-fiber slot leak; this fiber's single-owner "
                   "immutable tuple returned a sibling's value".format(
                       nt_cls.__name__, name, by_name, expected, wid))
            return

        # Check 3: value-object identity stable across the yield (no torn read of
        # an immutable tuple's slot).
        if id(inst[i]) != baseline_ids[i]:
            H.fail("namedtuple FIELD IDENTITY CHANGED: {0}.{1} (index {2}) value "
                   "object id changed across a yield (wid {3}) -- the immutable "
                   "tuple's slot was replaced or read from a sibling's "
                   "instance".format(nt_cls.__name__, name, i, wid))
            return

    # ---- _asdict() round-trips against _fields and positions ------------------
    d = inst._asdict()
    if list(d.keys()) != list(FIELD_NAMES):
        H.fail("namedtuple _asdict() KEYS WRONG: {0!r} != {1!r} (wid {2}) -- "
               "field-name ordering corrupted".format(
                   list(d.keys()), list(FIELD_NAMES), wid))
        return
    for i in range(NFIELDS):
        if d[FIELD_NAMES[i]] != values[i]:
            H.fail("namedtuple _asdict() VALUE WRONG: [{0!r}]={1}, expected {2} "
                   "(wid {3}) -- _asdict() read a wrong/leaked slot".format(
                       FIELD_NAMES[i], d[FIELD_NAMES[i]], values[i], wid))
            return

    # ---- _replace(field_j=sentinel) returns a NEW tuple differing in ONE slot --
    j = idx % NFIELDS
    sentinel = REPLACE_SENTINEL + wid          # unique-per-fiber sentinel
    replaced = inst._replace(**{FIELD_NAMES[j]: sentinel})

    if replaced is inst or id(replaced) == id(inst):
        H.fail("namedtuple _replace() DID NOT COPY: returned the SAME object "
               "(wid {0}) -- _replace must build a new tuple".format(wid))
        return
    for i in range(NFIELDS):
        want = sentinel if i == j else values[i]
        if replaced[i] != want:
            H.fail("namedtuple _replace() SLOT WRONG: replaced[{0}]={1}, expected "
                   "{2} (replaced field {3}, wid {4}) -- _replace inherited a "
                   "wrong/sibling slot in a non-replaced position".format(
                       i, replaced[i], want, j, wid))
            return
    # Original must be UNCHANGED (immutability preserved by _replace).
    for i in range(NFIELDS):
        if inst[i] != values[i]:
            H.fail("namedtuple ORIGINAL MUTATED by _replace: instance[{0}]={1}, "
                   "expected {2} (wid {3}) -- the source immutable tuple was "
                   "altered".format(i, inst[i], values[i], wid))
            return

    state["nt_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber sustains the single-owner namedtuple isolation check (fail-fast)
    to keep every hub churning with concurrent class-build + descriptor/tuple reads
    while parked across the yield, so siblings reliably interleave."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            nt_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "nt_checks": [0] * 1024,           # LOAD-BEARING single-owner checks (sharded tally, non-vacuity only)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    nchecks = sum(H.state["nt_checks"])
    H.log("namedtuple[single-owner LOAD-BEARING]: {0} isolation checks "
          "(field-by-name==by-index==expected, id-stable, _asdict round-trip, "
          "_replace one-slot copy -- all passed fail-fast); ops={1}".format(
              nchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(nchecks > 0,
            "no single-owner namedtuple isolation checks ran -- the load-bearing "
            "_tuplegetter/tuple-immutability hazard was never exercised (oracle "
            "would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a descriptor/_make read.
    H.require_no_lost("namedtuple tuplegetter/_replace isolation")


if __name__ == "__main__":
    harness.main(
        "p533_namedtuple_tuplegetter_replace", body, setup=setup, post=post,
        default_funcs=8000,
        describe="collections.namedtuple exposes fields via C _tuplegetter member "
                 "descriptors (fixed index) over an immutable tuple.  LOAD-BEARING: "
                 "each fiber builds its OWN namedtuple class + instance with unique "
                 "per-wid values; across a yield, access-by-name==access-by-index=="
                 "expected for every field, id() stable, _asdict() round-trips, and "
                 "_replace(one=new) returns a NEW tuple equal in all-but-one slot "
                 "with the original unchanged.  A torn descriptor index, a torn "
                 "immutable-tuple slot read, a cross-fiber slot leak, or an "
                 "_replace inheriting a sibling's slot is the runloom bug")
