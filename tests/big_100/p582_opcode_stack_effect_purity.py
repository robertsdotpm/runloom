"""big_100 / 582 -- opcode.stack_effect PURITY + opcode-table involution under M:N.

The `opcode` module is a thin Python facade over the C `_opcode` extension plus a
handful of read-only metadata tables (opmap, opname, hasarg, hasjump, hascompare,
...).  Its load-bearing callable is `opcode.stack_effect(op, oparg=None, *,
jump=None)` -- a PURE C function (in _opcode) that reads the interpreter's shared,
read-only opcode-metadata tables to compute how many stack slots an instruction
pushes/pops.  For a fixed (op, oparg, jump) triple the answer is a mathematical
constant: it depends ONLY on immutable interpreter data, never on any mutable
Python state, so two calls with the same arguments MUST return the identical int.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython 3.14t with the GIL off, `_opcode.stack_effect` runs in C while many hub
fibers call it concurrently and while runloom migrates a fiber across hubs at the
cooperative yield in the middle of a check.  If the C routine kept ANY hidden
mutable/per-thread scratch that were not fiber/thread isolated (a static buffer, a
cached last-oparg, a shared table pointer transiently swapped), or if runloom
corrupted the C call's arguments/return across a hub migration, a fiber could read
back a stack effect that (a) differs from the SAME call it made microseconds
earlier across a yield, or (b) differs from the closed-form ground truth computed
single-threaded before any fiber ran.  Likewise the module tables opmap / opname
are read-only involutions (opname[opmap[name]] == name); a torn read of those under
concurrency would break the identity.

PURITY LAW (single-owner, closed-form, falsifiable).  Before any fiber runs we
compute -- single-threaded in the root -- the GROUND TRUTH stack effect for a fixed
grid of (opcode, oparg, jump) triples covering ALL real opcodes.  That table is
frozen and thereafter only READ.  Each fiber owns a fiber-local ordering of the
triples; for each triple it:
    * computes se_before = opcode.stack_effect(op, oparg, jump=...)
    * asserts se_before == the frozen ground truth (closed-form match)
    * YIELDS (runloom.yield_now / sleep) so a sibling interleaves on the shared
      C routine and the shared read-only tables, and the scheduler may migrate
      this fiber to another hub
    * recomputes se_after = opcode.stack_effect(op, oparg, jump=...)
    * asserts se_after == se_before  (bit-identical across the yield -- the C
      function is pure, so the value CANNOT drift)
    * asserts se_after is a plain int (never a torn / non-int object)
  and separately verifies the opmap/opname involution across the same yield:
    opname[op] == name  and  opmap[name] == op  before AND after.

Everything the oracle touches is either fiber-local (the triple ordering, the
before/after ints) or immutable shared read-only (the frozen ground-truth dict, the
opcode module tables).  There is NO shared mutable container in the fail-fast arm,
so a FAIL cannot be documented shared-object semantics -- it can only be a real
runtime fault: a torn C return, a cross-fiber leak of the C routine's scratch, an
argument/return corruption across hub migration, or a torn read of the module
tables.  We verified the law holds under a plain-threads control (the C stack_effect
is pure; 8 OS threads GIL on/off return identical values for identical args), so a
correct runloom must keep it clean and the program exits 0 when there is no bug.

ORACLES:
  * LOAD-BEARING -- STACK-EFFECT PURITY (worker, HARD, fail-fast).  Fiber-local
    triple order; closed-form match + bit-identical across a yield, as above.
  * LOAD-BEARING -- TABLE INVOLUTION (worker, HARD, fail-fast).  opname/opmap round
    trip stable across the same yield.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-check inside
    the C call never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the purity arm actually ran (checks > 0).

FAIL ON: a stack effect that differs from the frozen ground truth, changes across a
yield, or comes back non-int; an opname/opmap involution that breaks across a yield.
Each is a real runtime fault, never documented Python semantics.

Stresses: _opcode.stack_effect C entry under GIL-off M:N concurrency + hub
migration mid-call, read-only opcode metadata table access, opmap/opname involution
reads racing thousands of concurrent pure-function calls.

Good TSan / controlled-M:N-replay target: many fibers hammer one pure C routine and
two module-level read-only containers; a TSan report on _opcode's tables or a
replay where a migrated fiber reads a drifted value localizes the fault before the
closed-form comparison even fires.
"""
import opcode

import harness
import runloom

# A fixed oparg grid.  stack_effect depends on oparg for many opcodes (BUILD_LIST,
# CALL, UNPACK_SEQUENCE, ...), so a spread of opargs makes the ground-truth table
# exercise the C routine's real arithmetic rather than a single constant.
OPARG_GRID = (0, 1, 2, 3, 5, 8, 16, 255)


def build_ground_truth():
    """Compute the frozen closed-form stack-effect table, single-threaded, in the
    root before any fiber runs.  Returns a list of case tuples

        (name, op, oparg, jump, expected_se)

    where oparg is None for no-argument opcodes, jump is None (opcode has no jump
    variance), True, or False, and expected_se is the reference int.  Covers ALL
    real opcodes in opmap across the oparg grid and both jump senses where a jump
    opcode's effect varies.  This is READ-ONLY forever after; fibers only compare
    against it."""
    hasarg = frozenset(opcode.hasarg)
    hasjump = frozenset(opcode.hasjump)
    cases = []
    for name, op in opcode.opmap.items():
        if op in hasarg:
            if op in hasjump:
                for oparg in OPARG_GRID:
                    cases.append((name, op, oparg, True,
                                  opcode.stack_effect(op, oparg, jump=True)))
                    cases.append((name, op, oparg, False,
                                  opcode.stack_effect(op, oparg, jump=False)))
            else:
                for oparg in OPARG_GRID:
                    cases.append((name, op, oparg, None,
                                  opcode.stack_effect(op, oparg)))
        else:
            cases.append((name, op, None, None,
                          opcode.stack_effect(op, None)))
    return cases


def call_se(op, oparg, jump):
    """Invoke the C stack_effect the same way the ground truth was built."""
    if jump is None:
        return opcode.stack_effect(op, oparg)
    return opcode.stack_effect(op, oparg, jump=jump)


# Split point in the fiber-local case walk where we yield, so a sibling reliably
# interleaves on the shared C routine + tables before this fiber re-checks.
def purity_check(H, wid, order, state):
    """Single-owner stack-effect purity + table-involution check.

    `order` is this fiber's private ordering (a shuffled index list) into the
    frozen, read-only ground-truth cases.  We walk it, computing each stack effect
    twice around a yield and matching the closed form.  Nothing here is shared and
    mutable, so any mismatch is a real runtime fault."""
    cases = state["cases"]
    ncases = len(cases)
    # Snapshot every "before" value first, then yield ONCE, then re-verify every
    # "after" -- this maximises the window during which siblings run on the shared
    # C routine between our two reads of each triple.
    befores = [0] * ncases
    for pos in range(ncases):
        i = order[pos]
        name, op, oparg, jump, expected = cases[i]

        se_before = call_se(op, oparg, jump)
        # Closed-form: the C routine must return the frozen ground truth.
        if se_before != expected:
            H.fail("stack_effect CLOSED-FORM MISMATCH: {0}(op={1}, oparg={2}, "
                   "jump={3}) returned {4}, ground truth is {5} (wid {6}) -- a "
                   "torn C return or corrupted opcode metadata table under M:N".format(
                       name, op, oparg, jump, se_before, expected, wid))
            return
        if not isinstance(se_before, int) or isinstance(se_before, bool):
            H.fail("stack_effect returned NON-INT {0!r} for {1}(op={2}, oparg={3}, "
                   "jump={4}) (wid {5}) -- a torn / non-int object from the C "
                   "routine under M:N".format(
                       se_before, name, op, oparg, jump, wid))
            return
        # Involution BEFORE the yield: opname/opmap round trip.
        if opcode.opname[op] != name or opcode.opmap[name] != op:
            H.fail("opcode table involution BROKEN before yield: opname[{0}]={1!r} "
                   "opmap[{2!r}]={3} (expected name {2!r}, op {0}) (wid {4}) -- a "
                   "torn read of the read-only opcode tables under M:N".format(
                       op, opcode.opname[op], name, opcode.opmap[name], wid))
            return
        befores[pos] = se_before

    # YIELD: hand the hub to siblings hammering the same pure C routine + tables,
    # and give the scheduler a chance to migrate this fiber to another hub.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    for pos in range(ncases):
        i = order[pos]
        name, op, oparg, jump, expected = cases[i]

        se_after = call_se(op, oparg, jump)
        # Bit-identical across the yield: a pure function CANNOT drift.
        if se_after != befores[pos]:
            H.fail("stack_effect DRIFTED across a yield: {0}(op={1}, oparg={2}, "
                   "jump={3}) was {4} before, {5} after (ground truth {6}) (wid "
                   "{7}) -- the pure C routine returned a different value across a "
                   "hub migration; a cross-fiber leak of its scratch or an "
                   "argument/return corruption".format(
                       name, op, oparg, jump, befores[pos], se_after, expected, wid))
            return
        # And still the closed form (guards against BOTH reads drifting together).
        if se_after != expected:
            H.fail("stack_effect CLOSED-FORM MISMATCH after yield: {0}(op={1}, "
                   "oparg={2}, jump={3}) returned {4}, ground truth is {5} (wid "
                   "{6})".format(name, op, oparg, jump, se_after, expected, wid))
            return
        # Involution AFTER the yield.
        if opcode.opname[op] != name or opcode.opmap[name] != op:
            H.fail("opcode table involution BROKEN after yield: opname[{0}]={1!r} "
                   "opmap[{2!r}]={3} (wid {4}) -- a torn read of the read-only "
                   "opcode tables across a hub migration".format(
                       op, opcode.opname[op], name, opcode.opmap[name], wid))
            return

    # One purity+involution sweep completed clean.
    state["checks"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber owns a PRIVATE shuffled ordering of the frozen cases (fiber-local
    list; the cases themselves are immutable shared read-only) and repeatedly runs
    the single-owner purity+involution sweep, failing fast on any mismatch."""
    ncases = len(state["cases"])
    order = list(range(ncases))
    rng.shuffle(order)                       # fiber-local ordering (single-owner)
    for _ in H.round_range():
        if not H.running():
            break
        # Re-shuffle each round so the yield interleaves a different triple boundary.
        rng.shuffle(order)
        purity_check(H, wid, order, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    cases = build_ground_truth()
    H.state = {
        "cases": cases,                      # frozen, read-only ground truth
        "checks": [0] * H.funcs,             # ONE slot per worker (race-free, wid-indexed)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    ncases = len(H.state["cases"])
    H.log("opcode.stack_effect purity: {0} single-owner sweeps over {1} "
          "(opcode,oparg,jump) triples each (every closed-form + bit-identical-"
          "across-yield + opname/opmap involution check passed fail-fast); "
          "ops={2}".format(checks, ncases, H.total_ops()))
    # NON-VACUITY: the purity arm actually ran.
    H.check(checks > 0,
            "no stack_effect purity sweeps completed -- the pure-C-routine M:N "
            "hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the C call).
    H.require_no_lost("opcode stack_effect purity")


if __name__ == "__main__":
    harness.main(
        "p582_opcode_stack_effect_purity", body, setup=setup, post=post,
        default_funcs=6000,
        describe="opcode.stack_effect is a PURE C routine (_opcode) reading the "
                 "interpreter's read-only opcode-metadata tables; for a fixed "
                 "(op,oparg,jump) triple its result is a mathematical constant. "
                 "Ground truth for a grid over ALL opcodes is frozen single-"
                 "threaded before any fiber runs.  LOAD-BEARING: each fiber, on a "
                 "private triple ordering, computes each stack effect twice around "
                 "a yield and asserts it equals the frozen ground truth and is "
                 "bit-identical across the yield/hub-migration; it also checks the "
                 "opname/opmap involution across the same yield.  A drifted value, "
                 "a closed-form mismatch, a non-int return, or a broken table "
                 "involution is a torn C return / cross-fiber scratch leak / "
                 "argument-return corruption under M:N")
