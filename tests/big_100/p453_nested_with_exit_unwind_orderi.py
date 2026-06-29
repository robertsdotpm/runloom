"""big_100 / 453 -- nested-with __exit__ unwind exc_info isolation across hubs.

The subject is the CPython per-frame / per-tstate EXCEPTION-STATE chain that a
nested ``with A() as a, B() as b, C() as c:`` statement threads through its
unwind -- NOT contextlib.ExitStack's deque (that is p449's subject, a different
object).  Here the object under attack is the interpreter's in-flight exception
state itself:

  * ``tstate->exc_info`` -> a ``_PyErr_StackItem`` whose ``exc_value`` is the
    "currently-handled" exception, and
  * the per-frame exception cell the compiler reserves for a ``with`` block.

A ``with`` statement compiles to a SETUP-style block whose cleanup, when the body
raises, runs (in 3.12+) the ``WITH_EXCEPT_START`` opcode: it loads the live
(exc_type, exc_value, tb) that the raise pushed and calls the manager's
``__exit__(t, v, tb)`` with it, having SAVED the previous ``tstate->exc_info``
and pushed a fresh stack item so ``sys.exc_info()`` *inside* ``__exit__`` reports
THIS exception; on return it RESTORES the saved ``exc_info`` and, if ``__exit__``
returned falsy, re-raises and proceeds OUTWARD to the next manager's cleanup.
A nested ``with A, B, C:`` is a CHAIN of these: the body raise drives C.__exit__,
then B.__exit__, then A.__exit__, each receiving the SAME live exception in
strict REVERSE-of-entry order, the in-flight exception threaded the whole way
down that save/push/call/restore sequence over ``tstate->exc_info``.

THE M:N HAZARD (the exact racing op pair).  ``tstate`` is a PER-HUB thread-state
SHARED by every fiber multiplexed onto that hub.  When a fiber unwinding nested
``__exit__`` blocks PARKS mid-unwind -- e.g. it cooperatively yields between
C.__exit__ and B.__exit__, or *inside* an ``__exit__`` -- its partial unwind
state (the saved/pushed ``_PyErr_StackItem``, the live exc_value, the next
manager to clean up) sits on its grown-down C stack and on the frame's exc cell,
while ANOTHER fiber scheduled onto the SAME hub tstate RAISES and HANDLES its own
exception, pushing/popping ITS OWN ``exc_info`` onto that shared
``tstate->exc_info``.  The racing op pair is:

  (a) the parked fiber's WITH_EXCEPT_START save/restore of ``tstate->exc_info``
      around a later ``__exit__`` call, vs
  (b) the sibling fiber's raise/except push+pop of ``tstate->exc_info`` on the
      same hub, landing inside (a)'s park window.

If the runtime does not snapshot+restore the FULL exception state across a fiber
switch, a torn ``exc_info`` hands a later ``__exit__`` the WRONG exception value
(a SIBLING's, or a stale one) -- ``sys.exc_info()`` inside that ``__exit__``
returns a FOREIGN exception, or the ``v`` argument is not this block's own raise.

TARGET INVARIANT -- ORDER + IDENTITY conservation (closed-world, falsifiable).
Every block raises a UNIQUE sentinel ``BlockExc`` tagged with its own block id
drawn from a finite sentinel UNIVERSE.  Each of its managers A/B/C records, into
a per-slot table, the ORDINAL at which its ``__enter__`` and ``__exit__`` ran and
the TAG of the exception its ``__exit__`` received.  For EVERY block:

  * ORDER: ``__exit__`` order is the EXACT reverse of ``__enter__`` order
    (C, B, A) -- a scrambled order means the unwind chain walked the wrong frame
    exc cell;
  * IDENTITY: every ``__exit__`` that received an exception received THIS block's
    OWN sentinel tag (never a sibling's, never out-of-universe) AND the received
    ``v`` is identical (``is``) to the sentinel this fiber raised, AND
    ``sys.exc_info()[1]`` inside the ``__exit__`` IS that same value -- exc_info
    isolation across the park;
  * EXACTLY-ONCE: every entered manager's ``__exit__`` ran exactly once (no
    skipped / no doubled cleanup);
  * the re-raised exception caught at the ``try`` boundary is this block's own
    sentinel (the unwind did not swallow it or substitute a sibling's).

CONTROL ARM (case 0).  A nested-with block run ENTIRELY in one fiber with NO
cooperative yield anywhere in its unwind (no park, no sibling interleave).  It
MUST still show exact reverse ordering and each ``__exit__`` seeing ONLY its own
sentinel.  A foreign exception or wrong order in the CONTROL is interpreter
exc-info-threading corruption itself (the WITH_EXCEPT_START save/restore is
broken), NOT M:N contention -- this is the falsifier that disambiguates "the
interpreter's nested-with exc threading is broken" from "the fiber switch tore
exc_info".

CONTENDED ARMS (cases 1/2/3) drive the park window:
  * case 1 YIELD-BETWEEN: yield between each manager's ``__exit__`` (in the body
    epilogue is not possible, so we yield at the TOP of each ``__exit__``), so a
    sibling lands between two cleanup calls.
  * case 2 YIELD-INSIDE: yield INSIDE each ``__exit__`` AFTER reading exc_info,
    then re-read it -- both reads must see this block's own sentinel, proving the
    pushed ``_PyErr_StackItem`` survived the park even while a sibling pushed its
    own on the shared tstate.
  * case 3 SIBLING-RAISER co-runs: alongside the unwinding fiber, a gated sibling
    fiber on the same pool repeatedly RAISES+HANDLES its OWN BlockExc (pushing and
    popping its exc_info on the hub tstate) during the unwind's park window -- the
    active foreign exc_info pressure the isolation must withstand.

COVERAGE: round-robin the four cases by worker id in the first ops
(``sel = (wid + i) % 4``) then random, so each case is exercised even when each
worker manages only a few ops under the timeout (the p125/p126/p172 flaky-random
coverage fix).

Invariant (hot, fail-fast): per block, exit order == reverse(enter order); every
``__exit__`` saw ONLY this block's own sentinel (tag in UNIVERSE, ``is`` the
raised value, ``sys.exc_info()[1] is`` it); every entered ``__exit__`` ran once;
the caught exception is this block's own.  Invariant (post): blocks-run > 0,
exits == 3*blocks (every A/B/C cleanup ran exactly once, none dropped/doubled),
every case exercised, no out-of-universe tag, no lost worker.

Stresses: WITH_EXCEPT_START exc_info save/push/restore across a fiber park,
tstate->exc_info isolation between siblings on a shared hub tstate, nested-with
reverse-order unwind, torn/foreign exception value in a later __exit__, exc cell
threading under M:N.

Good TSan / controlled-M:N-replay target: the per-hub tstate->exc_info
push/pop around WITH_EXCEPT_START vs a sibling's raise/except on the same hub is
a textbook shared-state read/modify race; a TSan report on the exc_info store, or
a single foreign tag under replay, localizes the torn exception before the
identity assert even fires (RNG is per-worker for replay).
"""
import sys

import harness
import runloom

# Finite sentinel UNIVERSE of block ids.  Every BlockExc carries a tag drawn from
# this set; a tag a later __exit__ ever observes that is NOT in this set is a
# torn/freed exc_value from a foreign or rehashed slot -- a hard fault.  Sized so
# many blocks coexist on each hub (more distinct foreign tags to mistakenly see).
UNIVERSE_SIZE = 4096
TAG_BASE = 0x45300000
UNIVERSE = frozenset(TAG_BASE + i for i in range(UNIVERSE_SIZE))

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Manager names in entry order.  Exit MUST be the exact reverse: C, B, A.
ENTER_ORDER = ("A", "B", "C")
EXIT_ORDER = ("C", "B", "A")

# The contended cases (case 0 is the control).  post() asserts each was hit.
CASE_CONTROL = 0       # one fiber, no yield anywhere in the unwind
CASE_YIELD_BETWEEN = 1  # yield at the top of each __exit__ (between cleanups)
CASE_YIELD_INSIDE = 2  # yield inside each __exit__, re-read exc_info after
CASE_SIBLING_RAISER = 3  # a gated sibling raises/handles its own exc during unwind
NCASES = 4

# How many raise/handle cycles the case-3 sibling spins during the park window --
# enough that its exc_info is provably live on the shared hub tstate while the
# unwinding fiber is parked mid-__exit__.
SIBLING_CYCLES = 8


class BlockExc(Exception):
    """A per-block sentinel exception tagged with this block's UNIVERSE id.  Its
    identity (``is``) AND its ``tag`` both pin it to the fiber that raised it; a
    later __exit__ that sees a different tag / a different object has been handed
    a foreign or torn exc_value off the shared hub tstate."""

    def __init__(self, tag):
        self.tag = tag
        super().__init__(tag)


def make_manager(name, tag, observed, do_yield_top, do_yield_inside, H):
    """Build one context manager for a nested-with block.

    On __exit__ it records, into the block-local ``observed`` list, a tuple
    (name, enter_ordinal_was_before_exit, received_tag, is_same_object,
    exc_info_is_same) so the worker can reconstruct order + identity.  ``order``
    is a one-element list shared by the block's three managers giving the next
    ordinal to stamp (single fiber stamps it -> race-free within the block)."""

    class Manager(object):
        def __init__(self):
            self.name = name
            self.enter_ord = -1
            self.exit_ord = -1
            self.exit_runs = 0

        def __enter__(self):
            self.enter_ord = observed["next"][0]
            observed["next"][0] += 1
            observed["enters"].append(self.name)
            return self

        def __exit__(self, exc_type, exc_value, tb):
            # Park mid-unwind BETWEEN cleanup calls: a sibling on this hub lands
            # here, raising/handling its own exception on the shared tstate.
            if do_yield_top:
                runloom.yield_now()

            self.exit_runs += 1
            self.exit_ord = observed["next"][0]
            observed["next"][0] += 1

            # Read the exception this __exit__ was handed, BOTH from the v arg and
            # from sys.exc_info() (the live tstate->exc_info that WITH_EXCEPT_START
            # pushed).  They must agree and must be THIS block's own sentinel.
            recv_tag = getattr(exc_value, "tag", None)
            same_obj = exc_value is observed["raised"][0]
            ei = sys.exc_info()[1]
            ei_same = ei is exc_value

            if do_yield_inside:
                # Park INSIDE the __exit__ AFTER reading, then re-read: the pushed
                # _PyErr_StackItem must survive the park even while a sibling
                # pushes/pops its own exc_info on the shared hub tstate.
                runloom.yield_now()
                ei2 = sys.exc_info()[1]
                ei2_same = ei2 is exc_value
                recv_tag2 = getattr(sys.exc_info()[1], "tag", None)
            else:
                ei2_same = ei_same
                recv_tag2 = recv_tag

            observed["exits"].append((self.name, self.exit_ord, recv_tag,
                                      same_obj, ei_same, ei2_same, recv_tag2))
            # Return falsy -> propagate the exception OUTWARD to the next manager,
            # which is exactly what drives the WITH_EXCEPT_START chain we attack.
            return False

    return Manager()


def run_block(H, wid, tag, do_yield_top, do_yield_inside):
    """Run ONE nested ``with A, B, C:`` block that raises THIS block's unique
    sentinel inside the body, and verify ORDER + IDENTITY + EXACTLY-ONCE on the
    unwind.  Returns (ok, n_exits) -- ok False means an invariant fired."""
    observed = {
        "next": [0],            # next ordinal to stamp (single-fiber, race-free)
        "enters": [],           # manager names in __enter__ order
        "exits": [],            # (name, exit_ord, recv_tag, same_obj, ei_same,
                                #  ei2_same, recv_tag2) per __exit__
        "raised": [None],       # the sentinel object this fiber raised
    }
    a = make_manager("A", tag, observed, do_yield_top, do_yield_inside, H)
    b = make_manager("B", tag, observed, do_yield_top, do_yield_inside, H)
    c = make_manager("C", tag, observed, do_yield_top, do_yield_inside, H)

    sentinel = BlockExc(tag)
    observed["raised"][0] = sentinel
    caught_tag = None
    caught_is = False
    try:
        with a, b, c:
            # Body raise: drives C.__exit__, then B.__exit__, then A.__exit__ in
            # strict reverse-of-entry order, threading `sentinel` through each via
            # WITH_EXCEPT_START / tstate->exc_info.
            raise sentinel
    except BlockExc as got:
        caught_tag = got.tag
        caught_is = got is sentinel
    except Exception as other:                    # noqa: BLE001
        # ANY non-BlockExc escaping the unwind is a fault: the chain substituted
        # a foreign exception (a sibling's) for this block's own raise.
        H.fail("nested-with unwind raised a FOREIGN/non-sentinel exception "
               "{0}: {1!r} (block tag {2:#x}) -- the WITH_EXCEPT_START exc chain "
               "handed back another fiber's exception off the shared hub tstate"
               .format(type(other).__name__, other, tag))
        return False, 0

    # ---- ORDER: __enter__ A,B,C ; __exit__ must be exact reverse C,B,A --------
    enters = tuple(observed["enters"])
    exit_names = tuple(e[0] for e in observed["exits"])
    if enters != ENTER_ORDER:
        H.fail("block tag {0:#x}: __enter__ order {1} != expected {2} -- the "
               "with-chain entered managers out of order".format(
                   tag, enters, ENTER_ORDER))
        return False, 0
    if exit_names != EXIT_ORDER:
        H.fail("block tag {0:#x}: __exit__ order {1} != reverse-of-entry {2} -- "
               "the nested-with unwind walked the frame exc cells in the WRONG "
               "order (torn unwind chain under M:N park)".format(
                   tag, exit_names, EXIT_ORDER))
        return False, 0

    # The exit ordinals must be strictly increasing in the C,B,A sequence (each
    # __exit__ ran exactly once and after all enters); enters are ordinals 0,1,2.
    exit_ords = [e[1] for e in observed["exits"]]
    if exit_ords != sorted(exit_ords) or len(set(exit_ords)) != 3:
        H.fail("block tag {0:#x}: exit ordinals {1} not strictly increasing/"
               "unique -- an __exit__ ran twice or was skipped (doubled/dropped "
               "cleanup)".format(tag, exit_ords))
        return False, 0

    # ---- IDENTITY: every __exit__ saw THIS block's own sentinel ---------------
    for name, _ord, recv_tag, same_obj, ei_same, ei2_same, recv_tag2 in \
            observed["exits"]:
        if recv_tag not in UNIVERSE:
            H.fail("block tag {0:#x}: {1}.__exit__ received OUT-OF-UNIVERSE tag "
                   "{2!r} -- a torn/freed exc_value off the shared hub tstate "
                   "(exc_info corruption)".format(tag, name, recv_tag))
            return False, 0
        if recv_tag != tag:
            H.fail("block tag {0:#x}: {1}.__exit__ received FOREIGN sentinel tag "
                   "{2:#x} (a SIBLING's exception) -- tstate->exc_info was torn "
                   "across the park; WITH_EXCEPT_START handed the wrong exc"
                   .format(tag, name, recv_tag))
            return False, 0
        if not same_obj:
            H.fail("block tag {0:#x}: {1}.__exit__ v-arg is NOT (is) this block's "
                   "raised sentinel -- a substituted exception object from the "
                   "shared hub tstate".format(tag, name))
            return False, 0
        if not ei_same:
            H.fail("block tag {0:#x}: {1}.__exit__ sys.exc_info()[1] is NOT the "
                   "v-arg -- the pushed _PyErr_StackItem disagrees with the "
                   "handler exception (torn exc_info across the park)".format(
                       tag, name))
            return False, 0
        if not ei2_same or recv_tag2 != tag:
            H.fail("block tag {0:#x}: {1}.__exit__ exc_info CHANGED across the "
                   "in-__exit__ park: re-read tag {2!r} same={3} -- a sibling's "
                   "exc_info overwrote ours on the shared hub tstate".format(
                       tag, name, recv_tag2, ei2_same))
            return False, 0

    # ---- the caught (re-raised) exception is this block's own -----------------
    if caught_tag != tag or not caught_is:
        H.fail("block tag {0:#x}: the try/except caught tag {1!r} (is-own={2}) -- "
               "the unwind re-raised a FOREIGN/substituted exception, not this "
               "block's own sentinel".format(tag, caught_tag, caught_is))
        return False, 0

    return True, len(observed["exits"])


def sibling_raiser(H, tag_base, cycles):
    """Case-3 co-runner: repeatedly RAISE+HANDLE its OWN BlockExc, pushing and
    popping its exc_info on the shared hub tstate during the unwinding fiber's
    park window.  Verifies its OWN exc_info stays its own (a foreign tag here is
    the same isolation break seen from the other side), then returns.  Its
    pressure is the point; it does not stamp the shared tables."""
    for n in range(cycles):
        tag = tag_base + (n & 0x3F)        # stays in UNIVERSE (offsets < size)
        e = BlockExc(tag)
        try:
            raise e
        except BlockExc:
            runloom.yield_now()            # park while OUR exc_info is live
            seen = sys.exc_info()[1]
            if seen is not e or getattr(seen, "tag", None) != tag:
                H.fail("sibling-raiser exc_info torn: handling tag {0:#x} but "
                       "sys.exc_info() is {1!r} -- a foreign exception overwrote "
                       "our exc_info on the shared hub tstate".format(
                           tag, seen))
                return


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    blocks = state["blocks"]
    exits = state["exits"]
    casemat = state["casemat"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the four cases by worker id in the first ops so each is
        # exercised even under a short timeout (the p125/p126 flaky-coverage fix);
        # random after.  Each worker's block tag is unique within UNIVERSE.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        # Unique-ish tag for this block, kept inside the sentinel UNIVERSE.
        tag = TAG_BASE + ((wid * 1315423911 + i * 2654435761) % UNIVERSE_SIZE)
        i += 1

        if sel == CASE_CONTROL:
            ok, nex = run_block(H, wid, tag, do_yield_top=False,
                                do_yield_inside=False)
        elif sel == CASE_YIELD_BETWEEN:
            ok, nex = run_block(H, wid, tag, do_yield_top=True,
                                do_yield_inside=False)
        elif sel == CASE_YIELD_INSIDE:
            ok, nex = run_block(H, wid, tag, do_yield_top=False,
                                do_yield_inside=True)
        else:  # CASE_SIBLING_RAISER -- spawn a co-running raiser, unwind in parallel
            sib_base = TAG_BASE + ((tag + 1) % (UNIVERSE_SIZE - 0x40))
            wg = runloom.WaitGroup()
            wg.add(1)

            def run_sib(sib_base=sib_base, wg=wg):
                try:
                    sibling_raiser(H, sib_base, SIBLING_CYCLES)
                finally:
                    wg.done()

            H.fiber(run_sib)
            # Drive our own unwind WITH yields so it parks while the sibling's
            # exc_info is live on the same hub tstate.
            ok, nex = run_block(H, wid, tag, do_yield_top=True,
                                do_yield_inside=True)
            wg.wait()

        if not ok:
            return
        blocks[slot] += 1
        exits[slot] += nex
        # Per-(slot,case) matrix, single-writer-per-slot (this worker owns `slot`),
        # so the per-case coverage tally is race-free under GIL-off.  Summed by
        # case across all slots in post().
        casemat[slot * NCASES + sel] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom primitives
    # are the cooperative M:N-safe ones.  Per-slot tallies are single-writer.
    H.state = {
        "blocks": [0] * SLOTS,     # nested-with blocks fully verified (per slot)
        "exits": [0] * SLOTS,      # total __exit__ calls verified (per slot)
        # Per-case exercised counts as a per-(slot,case) matrix flattened to
        # slot*NCASES + case, so each worker writes ONLY its own slot's cells
        # (single-writer-per-slot, race-free under GIL-off); summed by case in
        # post().
        "casemat": [0] * (SLOTS * NCASES),
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    blocks = sum(H.state["blocks"])
    exits = sum(H.state["exits"])
    H.log("nested-with blocks verified={0} __exit__ calls verified={1} "
          "(expected exits == 3*blocks) ops={2}".format(
              blocks, exits, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(blocks > 0,
            "no nested-with unwind blocks completed -- the exc_info-isolation "
            "race window was never exercised")

    # EXACTLY-ONCE conservation: every verified block ran its three managers'
    # __exit__ exactly once, so total verified __exit__ calls == 3 * blocks.  A
    # dropped or doubled cleanup would already have fired the per-block ordinal
    # check, but assert the global sum too (a second, independent reconciliation).
    H.check(exits == 3 * blocks,
            "exit conservation broken: {0} __exit__ calls verified across {1} "
            "blocks, expected exactly {2} (3 per block) -- a manager's __exit__ "
            "was dropped or doubled in the nested-with unwind".format(
                exits, blocks, 3 * blocks))

    # Each case was exercised (the deterministic round-robin guarantees it once
    # enough workers/ops ran; assert it so a coverage regression is caught).
    casemat = H.state["casemat"]
    for case in range(NCASES):
        total = 0
        for slot in range(SLOTS):
            total += casemat[slot * NCASES + case]
        names = {CASE_CONTROL: "control(no-yield)",
                 CASE_YIELD_BETWEEN: "yield-between",
                 CASE_YIELD_INSIDE: "yield-inside",
                 CASE_SIBLING_RAISER: "sibling-raiser"}
        H.check(total > 0,
                "case {0} ({1}) was never exercised -- coverage gap".format(
                    case, names[case]))

    H.require_no_lost("nested-with-exc-isolation completeness")


if __name__ == "__main__":
    harness.main(
        "p453_nested_with_exit_unwind_orderi", body, setup=setup, post=post,
        default_funcs=3000,
        describe="nested with A,B,C unwind across a fiber park while a sibling on "
                 "the same hub raises/handles its own exception: per block, "
                 "__exit__ order == reverse(enter), every __exit__ sees ONLY this "
                 "block's own sentinel (tag in a finite universe, is the raised "
                 "value, sys.exc_info() is it), each __exit__ runs exactly once -- "
                 "a foreign exc, wrong order, or skipped/doubled cleanup fails")
