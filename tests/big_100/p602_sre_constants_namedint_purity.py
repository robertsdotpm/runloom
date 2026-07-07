"""big_100 / 602 -- sre_constants._NamedIntConstant single-owner purity under M:N.

sre_constants (an alias re-export of re._constants) is the table of named opcode /
category / at-code constants the regex compiler emits.  Its ONE piece of live
machinery is `_makecodes(*names)`, which the module runs at import time to mint
every OPCODE/ATCODE/CHCODE:

    def _makecodes(*names):
        items = [_NamedIntConstant(i, name) for i, name in enumerate(names)]
        globals().update({item.name: item for item in items})
        return items

The value-carrying object it produces is `_NamedIntConstant`, an `int` subclass
that pins a `.name` string onto each instance in __new__:

    class _NamedIntConstant(int):
        def __new__(cls, value, name):
            self = super().__new__(cls, value)
            self.name = name
            return self
        def __repr__(self):
            return self.name

So a `_NamedIntConstant` is a SINGLE object carrying TWO coupled facts: an integer
value (its `int` payload) and a `.name` (a per-instance attribute stored in the
instance __dict__).  The closed-form LAW of `_makecodes` is: for a list built from
names[0..N-1], items[i] is an int equal to i whose .name is names[i]; repr(items[i])
is names[i]; and the multiset of int values is exactly range(N).

WHERE M:N COULD BREAK IT (the gap this program probes).  `_NamedIntConstant.__new__`
does two writes that must both land and stay coupled: the immutable int payload
(via int.__new__) and the mutable `.name` slot (an instance-__dict__ store).  Under
free-threaded 3.14t with the GIL off and runloom migrating a fiber across hubs at a
yield, a runtime bug (a torn instance-dict write, a cross-fiber attribute leak, an
identity swap of a live object across a park/resume, or a corrupted int payload)
would show up as: a constant whose .name no longer matches its value, a repr that
drifted, an int payload that changed, or an object identity that was replaced mid-
flight.  Because each fiber builds its OWN table from fiber-local (value, name)
pairs and never shares it, a CORRECT runtime keeps every instance bit-identical
across the yield, and this oracle PASSES (program exits 0 when there is no bug).

WHY THIS IS SINGLE-OWNER, NOT A SHARED-OBJECT RACE.  Each fiber calls the REAL
`_NamedIntConstant` constructor (the code under test) on its OWN fiber-local names
and values, storing the resulting list in a fiber-local variable that no sibling
can see.  This is NOT `globals().update()` -- we deliberately do NOT touch the
module's shared namespace (that would be a documented shared-object mutation, not a
runloom bug).  We only exercise the constructor + attribute machinery on private
objects.  A plain-threads control (each OS thread minting its own private table of
named constants, GIL on and off) returns 100% coupled, correct constants -- so
under a correct runloom it must too.

ORACLES:
  * LOAD-BEARING -- NAMED-CONSTANT PURITY (worker, HARD, fail-fast).  Each fiber
    builds a fiber-local list of `_NamedIntConstant`s from a fiber-local (value,
    name) table that mirrors `_makecodes`'s enumerate law (value == index, unique
    per-fiber names).  It snapshots each instance's int value, .name, repr, and
    id(), YIELDS (so siblings interleave / the fiber may migrate hubs), then
    re-verifies every instance:
      - int(nic) still equals the snapshot int value AND equals its index i;
      - nic.name still equals the snapshot name AND equals the expected name;
      - repr(nic) still equals the name (the __repr__ contract);
      - id(nic) is unchanged (the live object was not replaced);
      - the int identity laws hold: nic == i, hash(nic) == hash(i), and the
        int arithmetic payload is intact (nic + 1 == i + 1);
      - isinstance(nic, int) and isinstance(nic, _NamedIntConstant).
    Any drift is a runloom single-owner-object corruption, not Python semantics.

  * CONSERVATION (worker, HARD, closed-form).  The `_makecodes` law: the set of
    int values across the fiber-local table is EXACTLY range(N), and the names are
    exactly the fiber-local unique names -- a closed-world identity that a lost /
    doubled / torn instance would break.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-construction
    or mid-attribute-read never returns; the watchdog + require_no_lost catch it.

FAIL ON: a fiber-local named constant whose value/.name/repr/id changed across a
yield, a value != its index, a name != the expected name, a broken int-identity
law, or the range(N) conservation set not matching -- each a real runtime bug
(torn instance dict, cross-fiber attribute leak, identity swap, corrupted int
payload).  No shared mutable state is touched, so there is no documented-Python
shared-object arm to misread.

Stresses: sre_constants._NamedIntConstant.__new__ (int subclass creation +
per-instance .name attribute store), __repr__, int-identity/hash/arithmetic on the
subclass, and the `_makecodes` enumerate closed-form -- all on single-owner objects
across hub migration + yield under M:N.
"""
import warnings

import harness
import runloom

# sre_constants emits a DeprecationWarning on import (it is an alias of
# re._constants); silence it once at import so the constructor + names we probe
# come from the real, faithful module under test.
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import sre_constants

# The object under test: the int-subclass constructor that mints every named
# regex constant.  We call it directly on fiber-local args (single-owner), the
# same way _makecodes does, but WITHOUT the globals().update() (no shared mutation).
NamedIntConstant = sre_constants._NamedIntConstant

# Number of named constants each fiber mints per table -- large enough to push the
# instance-dict allocation path repeatedly and give the yield a real window, small
# enough that many rounds finish under the timeout.
TABLE_SIZE = 24

# Sustained checks per worker, bounded by H.running().  A single table barely
# overlaps a sibling; sustained churn (many fibers minting/reading private tables
# while parked across a yield) is what makes a torn-write / identity-swap bug
# reproduce.
INNER_CAP = 100000


def build_table(wid, idx):
    """Build ONE fiber-local table of _NamedIntConstant mirroring _makecodes.

    Values are the enumerate index (0..TABLE_SIZE-1) exactly as _makecodes assigns
    them; names are unique per (wid, idx, i) so a cross-fiber attribute leak yields
    a recognizably-wrong name.  Returns (items, names) where items[i] is a
    _NamedIntConstant with int value i and .name names[i]."""
    names = ["W{0}_T{1}_C{2}".format(wid, idx, i) for i in range(TABLE_SIZE)]
    items = [NamedIntConstant(i, names[i]) for i in range(TABLE_SIZE)]
    return items, names


def constant_check(H, wid, idx, state):
    """Single-owner purity + closed-form conservation on a fiber-local constant table.

    Mint the table, snapshot each instance's (int, name, repr, id), yield so
    siblings interleave and this fiber may migrate hubs, then re-verify every fact
    is bit-identical and matches the _makecodes closed-form law."""
    items, names = build_table(wid, idx)

    # Snapshot BEFORE the yield -- the baseline every re-read must match.
    base_int = [int(nic) for nic in items]
    base_name = [nic.name for nic in items]
    base_repr = [repr(nic) for nic in items]
    base_id = [id(nic) for nic in items]

    # YIELD: let siblings mint/read their own tables; allow hub migration.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    for i in range(TABLE_SIZE):
        nic = items[i]

        # int payload stable AND equal to its enumerate index (the _makecodes law).
        if int(nic) != base_int[i]:
            H.fail("_NamedIntConstant INT payload changed across a yield: "
                   "items[{0}] was {1}, now {2} (wid {3}, table {4}) -- the int "
                   "value of a single-owner constant was corrupted".format(
                       i, base_int[i], int(nic), wid, idx))
            return
        if int(nic) != i:
            H.fail("_makecodes law broken: items[{0}] has int value {1}, expected "
                   "index {0} (wid {2}) -- enumerate() value assignment torn".format(
                       i, int(nic), wid))
            return

        # .name stable AND equal to the expected fiber-local name (no cross-fiber
        # attribute leak).
        if nic.name != base_name[i]:
            H.fail("_NamedIntConstant .name changed across a yield: items[{0}] "
                   ".name was {1!r}, now {2!r} (wid {3}) -- a torn instance-dict "
                   "write or a cross-fiber attribute leak".format(
                       i, base_name[i], nic.name, wid))
            return
        if nic.name != names[i]:
            H.fail("_NamedIntConstant .name is {0!r}, expected {1!r} (wid {2}, "
                   "index {3}) -- the .name slot holds another fiber's value".format(
                       nic.name, names[i], wid, i))
            return

        # __repr__ contract: repr(nic) is its name; must be stable too.
        r = repr(nic)
        if r != base_repr[i] or r != names[i]:
            H.fail("_NamedIntConstant repr drifted: items[{0}] repr was {1!r}, now "
                   "{2!r}, expected name {3!r} (wid {4}) -- __repr__ read a torn "
                   ".name".format(i, base_repr[i], r, names[i], wid))
            return

        # Identity stable: the live object was not replaced by a sibling's.
        if id(nic) != base_id[i]:
            H.fail("_NamedIntConstant IDENTITY changed across a yield: items[{0}] "
                   "id was {1}, now {2} (wid {3}) -- the single-owner object was "
                   "swapped out".format(i, base_id[i], id(nic), wid))
            return

        # int-identity laws on the subclass: equality, hash, arithmetic payload.
        if nic != i or hash(nic) != hash(i) or (nic + 1) != (i + 1):
            H.fail("_NamedIntConstant int-identity broken: items[{0}] nic=={1} "
                   "hash-eq={2} plus1=={3} (wid {4}) -- the int base of the "
                   "subclass is corrupt".format(
                       i, (nic == i), (hash(nic) == hash(i)), (nic + 1),
                       wid))
            return

        # Type contract intact.
        if not isinstance(nic, int) or not isinstance(nic, NamedIntConstant):
            H.fail("_NamedIntConstant lost its type across a yield: items[{0}] "
                   "isinstance(int)={1} isinstance(NIC)={2} (wid {3})".format(
                       i, isinstance(nic, int), isinstance(nic, NamedIntConstant),
                       wid))
            return

    # CLOSED-FORM conservation: the set of int values is EXACTLY range(TABLE_SIZE)
    # and the names are exactly the fiber-local unique names -- the _makecodes law.
    if set(int(n) for n in items) != set(range(TABLE_SIZE)):
        H.fail("_makecodes conservation broken: the int value SET across the table "
               "is not exactly range({0}) (wid {1}) -- a constant was lost, "
               "doubled, or torn".format(TABLE_SIZE, wid))
        return
    if [nic.name for nic in items] != names:
        H.fail("_makecodes conservation broken: the .name sequence is not the "
               "fiber-local name table (wid {0}) -- cross-fiber name leak".format(
                   wid))
        return

    state["checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            constant_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # checks[] is a NON-VACUITY tally only (sharded by wid & 1023); it feeds no
    # conservation sum, so sharding is safe here (never wid-exact-conservation).
    H.state = {
        "checks": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("sre_constants._NamedIntConstant single-owner purity: {0} fiber-local "
          "constant-table checks (each: value/.name/repr/id stable across a yield, "
          "value==index, range(N) conservation -- all passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(checks > 0,
            "no _NamedIntConstant purity checks ran -- the single-owner constant "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-construction / mid-read.
    H.require_no_lost("sre_constants namedint purity")


if __name__ == "__main__":
    harness.main(
        "p602_sre_constants_namedint_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="sre_constants._NamedIntConstant is the int-subclass the regex "
                 "constant table is built from: it pins a .name onto each int in "
                 "__new__ (the object _makecodes mints).  LOAD-BEARING single-owner: "
                 "each fiber builds its OWN fiber-local table of named constants "
                 "(mirroring _makecodes's enumerate law, value==index, unique names) "
                 "and, across a yield + possible hub migration, re-verifies every "
                 "instance's int payload, .name, repr, id, int-identity/hash/"
                 "arithmetic, and type are bit-identical + match the closed-form "
                 "range(N) conservation law.  No shared state is touched.  A value/"
                 ".name/repr/id drift, a value != index, or a broken conservation "
                 "set is a real runloom object-corruption / cross-fiber-leak bug")
