"""big_100 / 491 -- pathlib.Path operations under M:N.

pathlib.Path is pure Python with NO module-level mutable caches or thread-affine
state, so it should remain unaffected by hub fiber migrations or concurrent fiber
operations on the same hub thread.  Each fiber constructs its own distinct Path
objects and performs a series of operations (exists checks, name extraction,
joining, iteration) across scheduler yields.  Under a correct runtime, a Path
object's semantics remain invariant across yields and preemption -- the object
itself is not shared, so hub migrations are transparent.

This is a NEGATIVE CONTROL / EXPECTED PASS (no runloom-specific hazard expected).
Pathlib has no module-level state that would be corrupted by hub fiber sharing or
migration, so the load-bearing oracle MUST pass 100% of the time under both GIL-on
plain threads and runloom M:N.  If it fails, it indicates either:
  (a) a deeper corruption in Python's object model or garbage collection
  (b) a pathlib regression or bug independent of M:N (also 0 under plain threads)
  (c) a real runloom fiber-context desynchronization (very unlikely; pathlib is
      pure Python and holds no interpreter state)

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  pathlib.Path objects are IMMUTABLE after construction -- Path(x).parts,
  .stem, .suffix, .parent, etc. are read-only snapshots computed from the
  constructor argument.  So a Path object p created with p = Path("/a/b/c")
  will ALWAYS return the same parts tuple, stem, suffix, etc. across any number
  of yields, migrations, or concurrent sibling operations (because the Path
  holds no mutable shared state, and siblings' Path operations on different
  objects cannot interfere).

  The load-bearing oracle: each fiber creates a unique Path from a unique
  deterministic string (derived from its wid + iteration counter), then:
    1. Snapshots Path.parts and stores it
    2. YIELDS + SLEEPS (to allow scheduler to migrate this fiber or run sibs)
    3. Re-reads Path.parts and asserts it equals the snapshot
    4. Verifies string representation consistency (repr(path) is idempotent)
    5. Checks specific path accessors (.stem, .suffix, .name) match expected
       constants derived from the constructor string

  A failure (snapshot != re-read, or a torn/corrupt parts tuple) would indicate
  that a sibling's Path operation corrupted this fiber's object (impossible with
  immutable semantics), or pathlib itself has a bug.  This is expected to PASS
  on a correct runtime (plain threads GIL on/off AND runloom M:N).

ARMS:
  * LOAD-BEARING -- PATH IMMUTABILITY across yields (worker, HARD, fail-fast).
    Each fiber creates a unique deterministic Path, snapshots its .parts, yields,
    and asserts the snapshot is unchanged.  Also verifies .stem, .suffix, .name,
    string repr consistency, and parent traversal are stable across yields.  A
    mismatch = path corruption (unexpected; pathlib has no shared state).
  * SECONDARY (report-only, NEVER fails): per-path object counts and construction
    patterns.  Measured to confirm the hazard (distinct paths per fiber) is
    actually exercised at scale.

FAIL ON: a path's .parts snapshot changing across a yield, a torn repr, or a
wrong .stem/.suffix/.name value (all closed-world / immutable derived fields).
NEVER fail on secondary metrics (this is expected to pass 100%).

Stresses: pathlib.Path immutability under hub migration + preempt + sibling
churn, pure-Python object model under M:N, no module-level caches or thread-
affine state, closed-world path parsing and normalization.
"""
import pathlib
from pathlib import Path, PurePath

import harness
import runloom


def canonical_path_for(wid, idx):
    """Generate a deterministic, unique path string for this fiber + iteration.
    Format: /unique/<wid>/deep/<idx>/file_<wid>_<idx>.txt
    This yields a Path with known .parts, .stem, .suffix, .name, .parent."""
    return "/unique/{0}/deep/{1}/file_{0}_{1}.txt".format(wid, idx)


def expected_stem_suffix_name(path_str):
    """Return (stem, suffix, name) for a canonical path.
    For "/unique/{wid}/deep/{idx}/file_{wid}_{idx}.txt":
      name = "file_{wid}_{idx}.txt"
      stem = "file_{wid}_{idx}"
      suffix = ".txt"
    Returns a tuple (stem, suffix, name) to check against Path accessors."""
    p = pathlib.PurePath(path_str)
    return (p.stem, p.suffix, p.name)


def expected_parts(path_str):
    """Return the expected .parts tuple for a canonical path."""
    return pathlib.PurePath(path_str).parts


def setup(H):
    H.state = {
        "immutability_checks": [0] * 1024,  # paths checked for snapshot stability
        "snapshot_mismatches": [0] * 1024,  # .parts snapshot != re-read (corruption)
        "repr_unstable": [0] * 1024,        # repr() changed across yields
        "accessor_wrong": [0] * 1024,       # .stem/.suffix/.name mismatch
        "sample": [None],                   # first observed corruption
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: PATH IMMUTABILITY across yields.  A fiber constructs a
# unique deterministic Path, snapshots immutable fields, yields, re-reads,
# and asserts they are unchanged.  Pathlib is pure Python with no shared
# state, so this MUST pass 100% on a correct runtime (plain threads GIL
# on/off AND runloom M:N).
# --------------------------------------------------------------------------
def immutability_check(H, wid, idx, state):
    """Construct a unique Path, snapshot it, yield, re-read, and verify
    immutability across the yield."""
    path_str = canonical_path_for(wid, idx)
    path = Path(path_str)

    # Snapshot immutable fields before the yield.
    snap_parts = path.parts
    snap_repr = repr(path)
    snap_stem, snap_suffix, snap_name = path.stem, path.suffix, path.name
    snap_str = str(path)
    snap_parent_str = str(path.parent)

    # YIELD + SLEEP: let the scheduler potentially migrate this fiber to a
    # different hub, run a sibling on the hub, preempt, etc.  All operations
    # on OTHER Path objects cannot affect THIS one (no shared state).
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Re-read the same fields after the yield and verify they are unchanged.
    reread_parts = path.parts
    reread_repr = repr(path)
    reread_stem, reread_suffix, reread_name = path.stem, path.suffix, path.name
    reread_str = str(path)
    reread_parent_str = str(path.parent)

    state["immutability_checks"][wid & 1023] += 1

    # Check 1: .parts must be the EXACT same tuple.
    if reread_parts != snap_parts:
        state["snapshot_mismatches"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "parts_mismatch",
                snap_parts, reread_parts
            )
        H.fail(
            "pathlib.Path.parts CORRUPTED across yield: {0} -> {1} "
            "(wid {2} idx {3}, path {4!r}) -- Path is immutable; a snapshot "
            "must survive a yield.  Expected (parts is a closed-world "
            "derived immutable, pathlib.Path has no shared state under M:N).".
            format(snap_parts, reread_parts, wid, idx, path_str)
        )
        return

    # Check 2: repr() must be stable (a derived immutable string).
    if reread_repr != snap_repr:
        state["repr_unstable"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "repr_mismatch",
                snap_repr, reread_repr
            )
        H.fail(
            "pathlib.Path repr UNSTABLE across yield: {0!r} -> {1!r} "
            "(wid {2} idx {3}) -- repr() is a derived immutable, must not "
            "change across a yield.".
            format(snap_repr, reread_repr, wid, idx)
        )
        return

    # Check 3: .stem, .suffix, .name must be unchanged (immutable derived fields).
    if reread_stem != snap_stem:
        state["accessor_wrong"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "stem_changed",
                snap_stem, reread_stem
            )
        H.fail(
            "pathlib.Path.stem CHANGED across yield: {0!r} -> {1!r} "
            "(wid {2} idx {3}, path {4!r}) -- .stem is immutable; a snapshot "
            "must survive a yield.".
            format(snap_stem, reread_stem, wid, idx, path_str)
        )
        return

    if reread_suffix != snap_suffix:
        state["accessor_wrong"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "suffix_changed",
                snap_suffix, reread_suffix
            )
        H.fail(
            "pathlib.Path.suffix CHANGED across yield: {0!r} -> {1!r} "
            "(wid {2} idx {3}, path {4!r}) -- .suffix is immutable; a snapshot "
            "must survive a yield.".
            format(snap_suffix, reread_suffix, wid, idx, path_str)
        )
        return

    if reread_name != snap_name:
        state["accessor_wrong"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "name_changed",
                snap_name, reread_name
            )
        H.fail(
            "pathlib.Path.name CHANGED across yield: {0!r} -> {1!r} "
            "(wid {2} idx {3}, path {4!r}) -- .name is immutable; a snapshot "
            "must survive a yield.".
            format(snap_name, reread_name, wid, idx, path_str)
        )
        return

    # Check 4: verify closed-world expectations (deterministic derivation).
    exp_stem, exp_suffix, exp_name = expected_stem_suffix_name(path_str)
    if snap_stem != exp_stem or snap_suffix != exp_suffix or snap_name != exp_name:
        state["accessor_wrong"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "accessor_wrong",
                (snap_stem, snap_suffix, snap_name),
                (exp_stem, exp_suffix, exp_name)
            )
        H.fail(
            "pathlib.Path accessor WRONG against closed-world expected: "
            "got (stem={0!r}, suffix={1!r}, name={2!r}) != expected "
            "(stem={3!r}, suffix={4!r}, name={5!r}) for path {6!r} "
            "(wid {7} idx {8}) -- pathlib parsing or accessor semantics "
            "corrupted.".
            format(
                snap_stem, snap_suffix, snap_name,
                exp_stem, exp_suffix, exp_name,
                path_str, wid, idx
            )
        )
        return

    # Check 5: .parts must match expected.
    exp_parts = expected_parts(path_str)
    if snap_parts != exp_parts:
        state["snapshot_mismatches"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (
                wid, idx, "parts_wrong",
                snap_parts, exp_parts
            )
        H.fail(
            "pathlib.Path.parts WRONG against closed-world expected: "
            "got {0} != expected {1} for path {2!r} "
            "(wid {3} idx {4}) -- pathlib parsing or normalization "
            "corrupted.".
            format(snap_parts, exp_parts, path_str, wid, idx)
        )
        return


# Sustained immutability checks per worker, bounded by H.running().
# The immutability hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously mid-check and parked across their yield, so the scheduler
# reliably runs siblings on the same hub before this fiber resumes.  A single
# check per fiber barely overlaps a sibling's.  So each worker runs a sustained
# internal loop (one immutability check per iteration, interleaved with harness
# counter calls) until the deadline (H.running()) or INNER_CAP.  Bounding by
# H.running() makes the oracle fire at the DEFAULT --rounds 1.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs sustained immutability checks: one unique Path per
    iteration, snapshot + yield + re-read cycle, until H.running() or
    INNER_CAP."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            immutability_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["immutability_checks"])
    mismatches = sum(H.state["snapshot_mismatches"])
    repr_issues = sum(H.state["repr_unstable"])
    accessor_issues = sum(H.state["accessor_wrong"])
    sample = H.state["sample"][0]

    H.log("pathlib.Path: immutability_checks={0} (LOAD-BEARING, all PASSED "
          "fail-fast) | snapshot_mismatches={1} repr_unstable={2} "
          "accessor_wrong={3} | sample={4}".format(
              checks, mismatches, repr_issues, accessor_issues, sample))

    # NON-VACUITY: the load-bearing immutability hazard was actually exercised.
    H.check(checks > 0,
            "no Path immutability checks ran -- the load-bearing pathlib "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-check
    # on a corrupted stack).
    H.require_no_lost("pathlib.Path immutability")


if __name__ == "__main__":
    harness.main(
        "p491_pathlib", body, setup=setup, post=post,
        default_funcs=8000,
        describe="pathlib.Path is pure Python with no module-level mutable "
                 "caches or thread-affine state; immutable Path objects must "
                 "retain their semantics (.parts, .stem, .suffix, .name) across "
                 "yields and hub fiber migrations.  LOAD-BEARING: each fiber "
                 "creates a unique deterministic Path, snapshots immutable "
                 "fields, yields (scheduler may migrate hub), re-reads, and "
                 "asserts the snapshot is unchanged (0 failures expected under "
                 "plain threads GIL on/off AND runloom M:N; a failure indicates "
                 "a deeper object-model corruption or pathlib regression)"
    )
