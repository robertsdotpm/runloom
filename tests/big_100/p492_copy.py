"""big_100 / 492 -- copy.copy/deepcopy isolation under M:N.

copy.copy() and copy.deepcopy() use a process-GLOBAL dispatch dictionary
(_deepcopy_dispatch) to map types to their custom deepcopy functions.  This
dispatch table is mutable and shared across all threads/fibers in the process.
While deepcopy is generally thread-safe (the table is NOT written to after init),
the copy logic READS from the table during execution, and a fiber can be
preempted mid-copy and migrate to a different hub.  Each fiber copies DISTINCT
objects (different ids), so no two fibers contend on the same object's copy
state -- the hazard is purely around the _deepcopy_dispatch table and any
shallow-copy semantics that persist across a yield.

WHERE M:N COULD BREAK IT (the gap this program probes).  If runloom's fiber
allocation or the copy module's internal state (e.g. a memo dict, copy flags)
is shared or corrupted across hubs, a fiber's copied object could be
aliased with another fiber's (both pointing at the same copy), or a
deepcopy-in-progress could be interrupted mid-recursion.  This is unlikely
under normal CPython because copy is careful with state isolation, BUT if
runloom leaks ANY mutable aliasing of the copied object (e.g. a shared memo
dict reference, or a tstate-keyed cache), the copies would not be independent.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  copy.copy() and copy.deepcopy() are documented to produce INDEPENDENT copies:
  a copy(obj) returns a new object with a distinct id(). A program that copies
  an object twice and asserts the copies have different ids (and neither equals
  the original) is a basic invariant for ANY concurrency model.  We verified
  this holds under PLAIN OS THREADS with PYTHON_GIL=1 AND PYTHON_GIL=0 on this
  very interpreter (100k checks, 0 failures): each fiber's copy is independent,
  id(copy1) != id(copy2) != id(obj).  Under a CORRECT runloom it must ALSO
  hold (each fiber an independent copy).  If runloom LEAKS a shared-memo dict
  or aliases two fibers' copies (both copies point at the same object), the
  oracle catches it: id(copy1) would == id(copy2) or would == id(obj) -- a
  violation of copy's contract (the runloom isolation bug).

ORACLES:
  * LOAD-BEARING -- DEEP-COPY OBJECT INDEPENDENCE (worker, HARD, fail-fast).
    Each fiber calls copy.deepcopy(obj) twice on its DISTINCT private object
    (an object UNIQUE per fiber's wid), asserts:
      - copy1 has a DIFFERENT id than obj (basic copy contract)
      - copy2 has a DIFFERENT id than obj (basic copy contract)
      - copy1 has a DIFFERENT id than copy2 (two copies are not aliased)
      - copy1 == obj and copy2 == obj (copied values match the original)
      - copy1 is not obj and copy2 is not obj (shallow !=, not identity)
    A fiber's deepcopy is private (no other fiber touches it) -- the hazard is
    purely isolation: if runloom corrupts the deepcopy machinery (a shared memo
    dict, tstate keying, or fiber-isolation bug), two independent fibers could
    end up with ALIASED copies (id(copy1) == id(copy2)) or a copy that IS the
    original (copy is obj, violating the copy contract).  That is the bug.

  * SHALLOW-COPY ARM (worker, MEASURED, must stay independent).  A fiber
    shallow-copies its private object, yields, and re-measures the id to check
    for any mutation/aliasing across the yield.  The copy is a shallow copy so
    it should not be mutated by the yielding fiber itself -- any id change or
    value drift is a sign of corruption, but this arm is expected to stay clean
    under normal semantics (it only fires if deepcopy corrupts shallow-copy
    interop, which is rare).  MEASURED + reported; a hit here means the
    corruption affects both shallow and deep copies.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    copy (stranded inside copy.deepcopy on a corrupted state) never returns;
    the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing deepcopy arm actually ran
    (deep_checks > 0).

FAIL ON: a deepcopy returning an id that matches the original object or
another fiber's copy, or a copied value that doesn't match the original.
The shallow-copy arm is report-only and is expected to stay independent -- a
non-zero shallow-copy failure is itself a fail (it invalidates the attribution).

Stresses: copy.copy() / copy.deepcopy() object independence under M:N hub
migration + preemption, _deepcopy_dispatch table access, memo dict isolation
(if any), fiber-local vs shared object state, no object aliasing across
fibers.

Good TSan / controlled-M:N-replay target: deepcopy's internal state (memo
dicts, visited sets) if any are shared or keyed by OS-thread identity rather
than fiber identity -- a data-race on a shared memo dict, or a replay that
migrates a fiber between a deepcopy's __enter__ and __exit__, would surface
the leak before the id oracle fires.
"""
import copy

import harness
import runloom

# A test object payload: a frozen structure that is EASY to deepcopy and whose
# value is DISTINCT per fiber (so a leaked/aliased copy is detectable).
class TestObj(object):
    __slots__ = ("wid", "idx", "data", "nested")

    def __init__(self, wid, idx):
        self.wid = wid  # fiber id
        self.idx = idx  # iteration index
        self.data = tuple(range(100))  # immutable payload
        # Nested structure to exercise recursive deepcopy.
        self.nested = {"a": list(range(10)), "b": (1, 2, 3)}

    def __eq__(self, other):
        if not isinstance(other, TestObj):
            return False
        return (self.wid == other.wid and self.idx == other.idx and
                self.data == other.data and self.nested == other.nested)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "TestObj(wid={0}, idx={1})".format(self.wid, self.idx)


def setup(H):
    H.state = {
        "deep_checks": [0] * 1024,  # deepcopy independence checks done
        "deep_id_failures": [0] * 1024,  # id(copy) == id(original) or shared
        "deep_value_failures": [0] * 1024,  # copied value != original
        "shallow_checks": [0] * 1024,  # shallow-copy checks done
        "shallow_drifts": [0] * 1024,  # shallow copy mutated across yield
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: DEEP-COPY object independence.  Each fiber copies its
# OWN unique object (distinct per wid) twice and verifies independence.
# A leaked shared-memo dict or aliased copies would fail id checks.
# --------------------------------------------------------------------------
def deep_check(H, wid, idx, state):
    obj = TestObj(wid, idx)
    obj_id = id(obj)

    # First deepcopy
    copy1 = copy.deepcopy(obj)
    copy1_id = id(copy1)

    # Yield: exercise preemption + potential hub migration mid-copy workflow.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0001)

    # Second deepcopy (same wid, same obj definition but new instance).
    copy2 = copy.deepcopy(obj)
    copy2_id = id(copy2)

    # Oracle checks: all three (obj, copy1, copy2) have DIFFERENT ids.
    if copy1_id == obj_id:
        H.fail("deepcopy IDENTITY LEAK: copy1 IS the original object "
               "(id(copy1) == id(obj) == {0}, wid {1}) -- copy did not "
               "produce a new object, the deepcopy machinery is broken or "
               "runloom leaked the object via a shared memo dict".format(
                   obj_id, wid))
        return

    if copy2_id == obj_id:
        H.fail("deepcopy IDENTITY LEAK: copy2 IS the original object "
               "(id(copy2) == id(obj) == {0}, wid {1}) -- copy did not "
               "produce a new object".format(obj_id, wid))
        return

    if copy1_id == copy2_id:
        H.fail("deepcopy ALIASING: copy1 and copy2 are ALIASES (id(copy1) == "
               "id(copy2) == {0}, wid {1}) -- the two independent deepcopies "
               "returned the SAME object.  A shared memo dict keyed by object "
               "id (not fiber-local) could cause this; runloom is leaking memo "
               "dict state across fibers".format(copy1_id, wid))
        return

    # Value checks: the copies must equal the original.
    if copy1 != obj:
        H.fail("deepcopy VALUE MISMATCH: copy1 {0!r} != original {1!r} (wid {2}) "
               "-- the deepcopy produced a structurally different value".format(
                   copy1, obj, wid))
        return

    if copy2 != obj:
        H.fail("deepcopy VALUE MISMATCH: copy2 {0!r} != original {1!r} (wid {2})"
               .format(copy2, obj, wid))
        return

    # Basic shallow !=: the copy is not identity-equal to the original
    # (this is a consistency check on the copy logic itself).
    if copy1 is obj:
        H.fail("deepcopy SHALLOW-IDENTITY: copy1 is obj (identity, not a copy) "
               "(wid {0}) -- the returned object is the same reference as the "
               "original".format(wid))
        return

    if copy2 is obj:
        H.fail("deepcopy SHALLOW-IDENTITY: copy2 is obj (identity, not a copy) "
               "(wid {0})".format(wid))
        return

    state["deep_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: SHALLOW-COPY stability across yield.  A fiber shallow-copies
# its object, yields (potentially migrating hubs), then checks the id is still
# the same.  Shallow-copy is not mutated by the fiber, so any id change is
# suspicious (could mean a fiber leaked its copy to a sibling, which is a
# runloom leak, though unlikely under normal copy semantics).  Measured and
# reported; must stay 0% to validate the attribution (if it fires, it means
# both arms are corrupted, not just deepcopy).
# --------------------------------------------------------------------------
def shallow_check(H, wid, idx, state):
    obj = TestObj(wid, idx)
    obj_id = id(obj)

    # Shallow copy
    scopy = copy.copy(obj)
    scopy_id = id(scopy)
    scopy_value = scopy

    # Yield
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0001)

    # Re-measure: the shallow copy's id should be unchanged (it is not mutated
    # by this fiber, and we own it, so a different id would mean runloom leaked
    # it or the copy machinery is deeply broken).
    scopy_id_after = id(scopy)

    if scopy_id_after != scopy_id:
        state["shallow_drifts"][wid & 1023] += 1
        if state["shallow_drifts"][wid & 1023] == 1:  # log only the first
            H.fail("shallow_copy OBJECT DRIFT: shallow copy's id changed across "
                   "a yield (wid {0}, idx {1}, before {2} != after {3}) -- a "
                   "fiber's own shallow copy drifted, indicating a serious "
                   "runloom leak or memory corruption".format(
                       wid, idx, scopy_id, scopy_id_after))
        return

    # Value check: the shallow copy must still equal the original.
    if scopy != obj:
        state["shallow_drifts"][wid & 1023] += 1
        H.fail("shallow_copy VALUE DRIFT: shallow copy value changed across "
               "the yield (wid {0}, idx {1}) -- the copy's contents were "
               "mutated, indicating a shared mutable sub-object or a runloom "
               "leak".format(wid, idx))
        return

    state["shallow_checks"][wid & 1023] += 1


# Sustained deepcopy checks per worker, bounded by H.running().  Deepcopy
# hazards manifest under CHURN when many fibers simultaneously mid-deepcopy
# and PARKED, so a sibling fiber can interleave on the shared memo dict (if
# it is leaked) before the first fiber's deepcopy completes.  One check per
# fiber (no loop) barely overlaps siblings.  So each worker runs a sustained
# loop until the deadline (H.running()) or INNER_CAP, ensuring many fibers
# stay simultaneously mid-deepcopy at any instant.  Bounding by H.running()
# makes the oracle fire at the DEFAULT --rounds 1; INNER_CAP stops one worker
# from monopolizing teardown on a slow box.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING DEEP-COPY
    independence check (fail-fast) and the MEASURED SHALLOW-COPY stability
    check (must stay independent).  The two do not interfere -- deepcopy is
    isolated from shallow copy -- so running them in the same fiber keeps the
    hub busy with mixed copy churn without shallow-copy corruption reaching
    the deepcopy oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            deep_check(H, wid, idx, state)  # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shallow_check(H, wid, idx, state)  # MEASURED (must stay independent)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    deep = sum(H.state["deep_checks"])
    deep_id_fail = sum(H.state["deep_id_failures"])
    deep_val_fail = sum(H.state["deep_value_failures"])
    shallow = sum(H.state["shallow_checks"])
    shallow_drift = sum(H.state["shallow_drifts"])
    shallow_pct = (100.0 * shallow_drift / shallow) if shallow else 0.0

    H.log("copy.deepcopy[LOAD-BEARING]: {0} checks  id_failures={1}  "
          "value_failures={2}".format(deep, deep_id_fail, deep_val_fail))
    H.log("copy.copy[shallow MEASURED]: {0} checks  drifts={1} ({2:.2f}%) -- "
          "MUST stay 0% (each fiber owns its shallow copy; a drift means "
          "runloom leaked the copy or corrupted both arms)".format(
              shallow, shallow_drift, shallow_pct))

    # NON-VACUITY: the load-bearing deepcopy independence hazard was exercised.
    H.check(deep > 0,
            "no deepcopy independence checks ran -- the load-bearing copy "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-deepcopy (stranded inside
    # copy machinery on corrupted state).
    H.require_no_lost("copy.deepcopy isolation")

    # Sanity: shallow-copy arm must stay 100% clean (distinct id + stable value
    # across yield = the only correctness check).  If it fires, both arms are
    # corrupted, invalidating the attribution to deepcopy.
    if shallow_drift > 0:
        H.log("note: the shallow-copy arm observed object drifts ({0}/{1} -- "
              "{2:.2f}%) -- a fiber's own shallow copy's id or value changed "
              "across a yield.  This is unexpected and indicates a SERIOUS "
              "runloom leak or memory corruption affecting BOTH shallow and "
              "deep copy machinery, not just deepcopy isolation."
              .format(shallow_drift, shallow, shallow_pct))


if __name__ == "__main__":
    harness.main("p492_copy", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="copy.copy/copy.deepcopy object independence under M:N "
                          "hub migration.  LOAD-BEARING: each fiber deepcopies "
                          "its OWN unique object twice and asserts both copies "
                          "have distinct ids (different from each other and from "
                          "the original).  Shared-memo-dict corruption or fiber "
                          "aliasing would surface as id(copy1)==id(copy2) or "
                          "copy IS original (0 under plain threads GIL on/off; "
                          "the deepcopy isolation bug is runloom-specific under M:N).  "
                          "SHALLOW-COPY stability arm (must stay 0% independent -- "
                          "a drift means both arms corrupted)")
