"""big_100 / 538 -- closure free-variable (PyCellObject) capture stability under M:N.

A Python closure reads and writes its free variables through PyCellObject cells
that are SHARED between the defining frame and every nested function that closes
over the name.  When a factory returns an inner function that captures a loop
variable, CPython allocates a fresh cell per iteration (late binding via a factory
argument) so each returned closure sees its OWN captured value.  When a closure
writes a free variable (via `nonlocal`), the write goes through `STORE_DEREF` into
the cell and later reads come back via `LOAD_DEREF`.  The cell object is the single
mutable box holding the free variable's contents.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each fiber
its own stackful C stack and its own Python frame chain, but a closure's cell is
heap-allocated and reachable from BOTH the (now-dead) factory frame and the live
closure object.  If hub-migration relocates a fiber's frame or its `f_localsplus`
freevars storage, or if a `co_freevars`/`co_cellvars` slot is confused across two
fibers' frames that momentarily share a hub, then:

  * a closure's `LOAD_DEREF` could read a SIBLING fiber's cell (cross-cell bleed:
    closure_i() returns j != i), or
  * a single-owner `nonlocal` RMW on a private counter cell could LOSE an
    increment because the STORE_DEREF landed in the wrong cell / was clobbered by
    a relocated freevars array.

Both are single-owner in this program: every closure and every cell in the fail-
fast arms is created and mutated by exactly ONE fiber and never shared.  So a
failure cannot be "documented shared-object races" -- it can only be the runtime
relocating or aliasing a fiber's private cell storage across a yield / hub move.

WHICH ORACLES ARE LOAD-BEARING, AND WHY:

  * ARM A -- LATE-BINDING CAPTURE ISOLATION (fail-fast, single-owner).  Each fiber
    builds SPAN closures via a factory `make(i) -> (lambda: i)`.  Each captures a
    DISTINCT value `i` in its OWN fresh cell.  The fiber records the list, YIELDS
    (so siblings interleave and may build their own factory closures on the same
    hub), then asserts `closures[i]() == base + i` for every i -- i.e. no closure's
    captured cell was overwritten by, or aliased to, a sibling's.  A classic
    late-binding bug (all closures share one cell) would already show `closure_i()
    == base + SPAN-1` for all i on a CORRECT interpreter, so we use the factory form
    that is GUARANTEED distinct on plain CPython; therefore ANY mismatch here is a
    runtime cell-storage desync, not a Python late-binding gotcha.

  * ARM B -- PRIVATE-CELL nonlocal RMW CONSERVATION (fail-fast, single-owner).
    Each fiber owns ONE closure `bump()` that does `nonlocal n; n += 1` over a
    PRIVATE cell (the factory's local `n`).  The fiber calls `bump()` exactly N
    times, yielding between calls, then asserts the read-back `get()` == N.  The
    cell has EXACTLY ONE writer (this fiber), so this is a race-free CONSERVATION
    law: on a correct runtime the private cell never loses an increment.  A lost
    increment means a STORE_DEREF landed in the wrong cell or the freevars array
    was relocated mid-RMW -- a real runtime bug.

  * NON-VACUITY (post, HARD): cell_checks > 0 -- ARM B actually ran and closed its
    conservation law; capture_checks > 0 -- ARM A actually ran.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-RMW (inside
    a LOAD_DEREF/STORE_DEREF on a relocated cell) never returns; the watchdog +
    require_no_lost catch it.

Single-owner throughout: no closure, cell, or list crosses fibers.  There is no
MEASURED/shared arm because a shared cell RMW would be documented lost-count
behavior (identical to a shared `x += 1`), which must never reach a fail-fast
oracle -- so this program deliberately has no shared-cell arm at all.

FAIL ON: a late-binding closure returning a value other than its captured `base+i`
(cross-cell bleed / aliasing), or a private-cell nonlocal counter reading back
!= N after N single-writer increments (lost increment / relocated freevars),
or a SIGSEGV inside LOAD_DEREF/STORE_DEREF.

Stresses: PyCellObject allocation per factory call, LOAD_DEREF/STORE_DEREF over
`nonlocal`, co_freevars/co_cellvars slot binding, f_localsplus freevars storage
across a yield + hub migration, single-owner cell conservation under sustained
M:N churn.
"""
import harness
import runloom

# Number of late-binding closures each fiber builds per ARM-A pass.  Enough that a
# freevars-array relocation would corrupt several slots, not just one, and enough
# to push the per-fiber closure list through a few reallocations.
SPAN = 16

# Number of nonlocal increments each fiber applies to its PRIVATE cell per ARM-B
# pass.  Big enough that a single lost STORE_DEREF moves the read-back by a
# detectable amount; small enough that many passes complete under the timeout.
BUMPS = 24

# Per-fiber value base so ARM-A captured values differ VISIBLY across fibers: a
# sibling's closure for index i would return (sibling_wid*BASE_SCALE + i), which is
# distinct from this fiber's (wid*BASE_SCALE + i).  A cross-cell bleed is therefore
# a value from a different band, not just a different index.
BASE_SCALE = 1000000

# Sustained inner churn per worker round, bounded by H.running().  The cell-storage
# hazard only manifests under MANY fibers simultaneously building/mutating private
# cells while PARKED across their yields, so a sibling reliably interleaves a cell
# op before this fiber resumes.  A single pass per fiber barely overlaps and does
# NOT reproduce a relocation.
INNER_CAP = 100000


def make_capture(value):
    """Factory: return a closure capturing `value` in its OWN fresh cell.

    This is the GUARANTEED-distinct late-binding form (the captured name is the
    factory's parameter, so each call gets a fresh cell).  On correct CPython every
    returned closure reads back its own `value`; any cross-closure bleed is a
    runtime cell-storage desync, not a Python late-binding gotcha."""
    return lambda: value


def make_counter():
    """Factory: return (bump, get) closing over ONE private cell `n`.

    `bump` does a `nonlocal n; n += 1` -- a STORE_DEREF read-modify-write over the
    cell.  `get` reads the cell via LOAD_DEREF.  The cell is private to the caller;
    exactly one fiber ever bumps it, so its final value is a race-free conservation
    law (== number of bump() calls)."""
    n = 0

    def bump():
        nonlocal n
        n += 1

    def get():
        return n

    return bump, get


# ---- ARM A: late-binding capture isolation (single-owner, fail-fast) ------
def capture_check(H, wid, idx, state):
    """Build SPAN factory closures each capturing a DISTINCT value, yield, then
    assert every closure still reads back exactly its captured value.  Single-
    owner: the closures live in a fiber-local list, never shared."""
    base = wid * BASE_SCALE
    closures = [make_capture(base + i) for i in range(SPAN)]

    # YIELD: let siblings build their own factory closures / cells on this hub.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    for i in range(SPAN):
        got = closures[i]()
        expected = base + i
        if got != expected:
            H.fail("late-binding capture BLEED: closure #{0} (wid {1}) returned "
                   "{2}, expected {3} -- its captured cell was aliased to or "
                   "overwritten by another closure/fiber across a yield "
                   "(band {4} vs got-band {5})".format(
                       i, wid, got, expected, base // BASE_SCALE,
                       got // BASE_SCALE if isinstance(got, int) else -1))
            return

    state["capture_checks"][wid & 1023] += 1


# ---- ARM B: private-cell nonlocal RMW conservation (single-owner, fail-fast)
def cell_conservation_check(H, wid, idx, state):
    """Own ONE closure over a PRIVATE cell, bump it BUMPS times with yields between,
    then assert the cell reads back EXACTLY BUMPS.  Exactly one writer -> race-free
    conservation.  A lost increment is a relocated/aliased freevars store."""
    bump, get = make_counter()
    for b in range(BUMPS):
        bump()                              # nonlocal n += 1 -> STORE_DEREF RMW
        # Yield mid-RMW-sequence so a sibling's cell op interleaves between this
        # fiber's STORE_DEREF and its next LOAD_DEREF.
        if b & 3 == 0:
            runloom.yield_now()

    got = get()                             # LOAD_DEREF of the private cell
    if got != BUMPS:
        H.fail("private-cell conservation broken: nonlocal counter read back {0} "
               "after {1} single-writer bump() calls (wid {2}) -- a STORE_DEREF "
               "landed in the wrong cell or the freevars array was relocated "
               "mid-RMW; a private single-owner cell must never lose an "
               "increment".format(got, BUMPS, wid))
        return

    state["cell_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH single-owner fail-fast arms per iteration: late-binding
    capture isolation (ARM A) and private-cell nonlocal conservation (ARM B).
    Neither arm shares data with any sibling, so the mixed churn keeps hubs busy
    without any shared mutation reaching a fail-fast oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            capture_check(H, wid, idx, state)          # ARM A (fail-fast)
            if H.failed:
                return
            cell_conservation_check(H, wid, idx, state)  # ARM B (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "capture_checks": [0] * 1024,       # ARM A single-owner checks (sharded tally)
        "cell_checks": [0] * 1024,          # ARM B single-owner conservation checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    cchecks = sum(H.state["capture_checks"])
    ncells = sum(H.state["cell_checks"])
    H.log("closure[ARM-A late-binding capture]: {0} isolation checks (all passed "
          "fail-fast) | closure[ARM-B private-cell nonlocal]: {1} conservation "
          "checks (each read back EXACTLY {2}); ops={3}".format(
              cchecks, ncells, BUMPS, H.total_ops()))

    # NON-VACUITY: both single-owner hazards were actually exercised.
    H.check(ncells > 0,
            "no private-cell nonlocal conservation checks ran -- the load-bearing "
            "cell-RMW hazard was never exercised (oracle would be vacuous)")
    H.check(cchecks > 0,
            "no late-binding capture-isolation checks ran -- ARM A was never "
            "exercised")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # LOAD_DEREF/STORE_DEREF on a relocated cell).
    H.require_no_lost("closure cellvar capture stability")


if __name__ == "__main__":
    harness.main(
        "p538_closure_cellvar_capture_stability", body, setup=setup, post=post,
        default_funcs=6000,
        describe="Python closures read/write free variables through shared "
                 "PyCellObject cells reachable from the defining frame and every "
                 "nested closure.  Under M:N, if hub-migration relocates a fiber's "
                 "frame or freevars storage, a cell's contents or the closure->cell "
                 "binding could cross a sibling's.  LOAD-BEARING (both single-"
                 "owner): ARM A builds SPAN factory closures each capturing a "
                 "DISTINCT value and asserts closure_i()==base+i across a yield (no "
                 "cross-cell bleed); ARM B owns ONE closure over a PRIVATE cell, "
                 "does N single-writer nonlocal increments interleaved with yields, "
                 "and asserts the cell reads back EXACTLY N (race-free conservation "
                 "-- a lost increment is a relocated/aliased freevars store).  No "
                 "shared arm exists: a shared cell RMW is documented lost-count "
                 "behavior and must never reach a fail-fast oracle")
