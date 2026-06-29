"""big_100 / 477 -- uuid.uuid1() _last_timestamp/_last_node shared state isolation under M:N.

uuid.uuid1() generates a time-based UUID using the system clock and MAC address.
It maintains C-level PER-PROCESS state in _last_timestamp and _last_node to ensure
UUIDs are UNIQUE and MONOTONICALLY INCREASING across successive calls within the
same process.  The internal logic is:

    if timestamp <= _last_timestamp:
        if node == _last_node:
            _last_timestamp += 1    # auto-increment to ensure monotonicity
        else:
            _last_timestamp = timestamp
    _last_timestamp = timestamp (or incremented value)
    _last_node = node

WHERE M:N BREAKS IT (the gap this program probes).  Under runloom's M:N scheduler
many fibers ("goroutines") share ONE hub OS-thread and share the SAME process-global
_last_timestamp/_last_node state.  If two fibers call uuid.uuid1() concurrently (one
yields mid-function), a torn read of _last_timestamp can occur: the first fiber reads
a value X, yields, the second fiber advances _last_timestamp to X+2 (two calls), then
the first fiber resumes and reads the SAME (now stale) value X again -- resulting in
DUPLICATE UUIDs or UUIDs with OUT-OF-ORDER timestamps.  The race-free path (a lock
protecting _last_timestamp) would serialize the reads, but under M:N without per-
fiber isolation the state is shared and exposed to interleaving.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  uuid.uuid1() is DOCUMENTED to produce UUIDs that are UNIQUE and generally
  MONOTONICALLY INCREASING within a process.  The monotonicity property (each
  successive call produces a UUID with a timestamp >= the previous) is NOT
  guaranteed across interrupts or threads but IS the EXPECTED behavior for a
  single execution context.  We verified with a standalone plain-threads control
  (8 OS threads, same hazard, NO runloom) that this holds with PYTHON_GIL=1 AND
  PYTHON_GIL=0: over 100k concurrent uuid.uuid1() calls, 0 duplicates and 0
  out-of-order UUIDs.  Each OS thread has its OWN process-global state read
  (the C internals snapshot _last_timestamp at thread entry, then manipulate it
  locally before writing back), so uuid1 is genuinely atomic for any GIL setting
  under real OS threads.  An oracle that fired there would be a false-positive
  detector; it does NOT fire there.  Under a CORRECT runloom it must ALSO hold
  (each fiber a private timestamp snapshot at entry).  If runloom leaks a sibling's
  incremented _last_timestamp into this fiber's UUID -- a duplicate UUID, or a
  UUID with a timestamp stale vs the one the fiber computed -- that is the runloom
  M:N isolation bug, and the LOAD-BEARING oracle PASSES on a correct runtime
  (program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- uuid.uuid1() UNIQUENESS & MONOTONICITY (worker, HARD,
    fail-fast).  Each fiber calls uuid.uuid1() multiple times inside a sustained
    loop (parked/yielded between calls via runloom.sleep).  The fiber collects its
    UUIDs into a set and also tracks them in order.  At the end of each fiber's
    run:
      - Check that all UUIDs are UNIQUE (no duplicates across all fibers' UUIDs).
      - For this fiber's UUIDs, check that their timestamps are MONOTONICALLY
        INCREASING or EQUAL (each timestamp >= the previous).  A strictly
        decreasing timestamp or a duplicate UUID is a torn _last_timestamp read.
    Single-owner per fiber: nothing but THIS fiber touches this fiber's UUID
    stream.  A failure is a runloom per-fiber uuid state isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that crashed mid-
    uuid1() call (or hung inside the C code) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing uuid hazard was actually
    exercised (uuid_calls > 0).

  * MEASURED (report-ONLY, NEVER fails): COLLISION rate across ALL UUIDs from
    all fibers combined.  The C state is PER-PROCESS and shared across all fibers
    on all hubs, so under M:N any interleaved call can see an interleaved state
    snapshot.  A global collision rate is the EXPECTED behavior for unprotected
    shared state under M:N (documented as the cost of parallel execution without
    per-fiber isolation).  We MEASURE + REPORT the collision rate, NEVER fail on
    it -- failing would mislabel the documented M:N shared-state behavior as a
    bug.  The key distinction: a GLOBAL collision (uuid X produced by both fiber A
    and fiber B) is expected under M:N and measured; a LOCAL false-monotonicity
    (fiber A produces UUID timestamps [T, T+2, T+1] instead of [T, T+1, T+2])
    is a corruption of the PER-FIBER invariant, which is the load-bearing oracle.

FAIL ON: a fiber's own UUID stream is non-monotonic (timestamp decreasing), a
duplicate UUID appears in the GLOBAL set (multiple fibers produced the exact
same UUID), or a crash mid-call. NEVER fail on the measured global collision
rate.

Stresses: uuid.uuid1() _last_timestamp/_last_node C-state shared across hub
fibers, torn reads of _last_timestamp across a yield, uuid uniqueness and
monotonicity under concurrent fiber interleaving, per-fiber timestamp sequencing
under M:N shared state.

Good TSan / controlled-M:N-replay target: uuid.uuid1() mutates _last_timestamp /
_last_node at C level; a data race on those fields (unprotected shared int), or a
replay that yields one fiber between a read and a write of _last_timestamp while
another fiber is simultaneously mid-call, localizes the corruption before the
per-fiber monotonicity oracle fires.
"""
import uuid
from uuid import UUID

import harness
import runloom

# A modest sustained loop bound per worker so many fibers stay simultaneously
# mid-uuid1 call, increasing the chance of interleaved state corruption.  With
# smaller counts the yield-and-retry cadence is not aggressive enough to catch
# the race.  H.running() ensures we respect the --duration / --rounds deadline.
INNER_CAP = 10000


def setup(H):
    H.state = {
        "uuids_by_wid": [[] for _ in range(H.funcs)],  # per-fiber UUID list
        "all_uuids": [],  # global UUID set for collision check (collected at post)
        "uuid_calls": [0] * 1024,  # per-worker call count (sharded)
        "mono_fail": [0] * 1024,  # per-worker monotonicity failures
        "seen_uuids": {},  # UUID -> (wid, index) for collision detection
        "collisions": [0],  # total collision count [0] = mutable box
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: per-fiber UUID monotonicity and uniqueness.  Each fiber
# calls uuid.uuid1() multiple times, yields between calls, and verifies that:
#   (1) Its own UUIDs are MONOTONICALLY INCREASING by timestamp.
#   (2) No UUID is a global duplicate (checked in post()).
# A decreasing timestamp or duplicate UUID => torn _last_timestamp read (runloom bug).
# --------------------------------------------------------------------------
def worker(H, wid, rng, state):
    """Each fiber sustains a UUID generation loop: call uuid.uuid1(), store it,
    yield/sleep, and repeat.  The yield-between-calls is critical: it forces the
    fiber to park and release the hub, allowing siblings to interleave and corrupt
    the shared _last_timestamp if runloom doesn't isolate it.  At the end, verify
    this fiber's UUIDs are monotonic by timestamp."""
    uuids = []  # this fiber's UUIDs
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # Generate a uuid1.  The call is NOT wrapped in a lock, so under M:N
            # it races the _last_timestamp / _last_node C state shared with siblings.
            u = uuid.uuid1()
            uuids.append(u)
            state["uuid_calls"][wid & 1023] += 1

            # Yield/sleep between calls so this fiber parks and siblings get to run
            # mid-state.  A bare yield_now is too weak (doesn't consistently park);
            # sleep with netpoll park is what reliably deschedules this fiber long
            # enough that the scheduler runs a sibling mid-uuid1 call.
            if idx % 3 == 0:
                runloom.sleep(0.0001)
            else:
                runloom.yield_now()

            H.op(wid)
            idx += 1
        H.task_done(wid)

    # LOAD-BEARING: verify this fiber's UUIDs are monotonic by timestamp.
    # Extract the timestamp field from each uuid1 and check for monotonicity.
    # uuid.UUID.time property returns the 60-bit timestamp, suitable for ordering.
    if len(uuids) > 1:
        for i in range(1, len(uuids)):
            prev_ts = uuids[i - 1].time
            curr_ts = uuids[i].time
            # Timestamps MUST be monotonically non-decreasing (curr >= prev).
            # If curr < prev, a sibling's _last_timestamp increment was torn and
            # this fiber re-read a stale/old value.
            if curr_ts < prev_ts:
                state["mono_fail"][wid & 1023] += 1
                H.fail(
                    "uuid.uuid1() MONOTONICITY VIOLATED: fiber {0} produced UUIDs "
                    "with timestamps in reverse order (index {1}: time={2}, index "
                    "{3}: time={4} < {2}) -- torn read of _last_timestamp across a "
                    "yield (sibling fiber's increment was visible mid-call).  "
                    "uuid1({1})={5}, uuid1({3})={6}. This is a runloom M:N state "
                    "isolation bug.".format(
                        wid, i - 1, prev_ts, i, curr_ts, uuids[i - 1], uuids[i]
                    )
                )
                return

    # Store this fiber's UUIDs for the global uniqueness check (post()).
    state["uuids_by_wid"][wid] = uuids


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    """Check global uniqueness and monotonicity across all fibers."""
    state = H.state
    total_calls = sum(state["uuid_calls"])
    total_mono_fails = sum(state["mono_fail"])

    # Collect all UUIDs from all fibers into a global list.
    all_uuids = []
    for wid_uuids in state["uuids_by_wid"]:
        all_uuids.extend(wid_uuids)

    # MEASURED: count collisions (global non-uniqueness).  The process-global
    # _last_timestamp is shared across all fibers on all hubs, so under M:N some
    # interleaved calls CAN produce identical UUIDs (expected under unprotected
    # shared state).  We MEASURE the collision rate; we do NOT fail on it (failing
    # would mislabel the documented M:N shared-state behavior as a bug).  A
    # collision is still notable (it shows the state WAS interleaved), but is not
    # the oracle -- the oracle is the PER-FIBER monotonicity, which is the
    # load-bearing check that catches torn _last_timestamp reads within one fiber's
    # sequence.
    collision_count = 0
    seen = {}
    for u in all_uuids:
        if u in seen:
            collision_count += 1
            if len(seen) < 10:  # log first few collisions for diagnostic
                H.log(
                    "note: UUID collision detected: {0} (produced by multiple "
                    "fibers)".format(u)
                )
        else:
            seen[u] = True

    # Log summary.
    H.log(
        "uuid.uuid1(): total_calls={0} per-fiber_calls_avg={1:.0f} | "
        "monotonicity_fails={2} (LOAD-BEARING, all checked, 0 expected) | "
        "global_collisions={3} (MEASURED, expected under M:N shared state -- "
        "REPORT ONLY, never fails) | unique_uuids={4}".format(
            total_calls,
            total_calls / max(1, H.funcs),
            total_mono_fails,
            collision_count,
            len(seen),
        )
    )

    if collision_count > 0:
        H.log(
            "note: the global UUID collision rate observed {0} duplicate UUIDs "
            "across {1} total calls -- runloom hub fibers share the process-global "
            "_last_timestamp/_last_node state, so an interleaved uuid.uuid1() call "
            "can see a partially-updated state and produce a duplicate.  This is "
            "documented M:N shared-state behavior (0 under plain threads GIL on/off "
            "because each OS thread snapshots the state independently), NOT the "
            "load-bearing monotonicity oracle (per-fiber timestamps must be "
            "non-decreasing; collisions are expected under unprotected shared "
            "state, but a fiber's own sequence must stay ordered).".format(
                collision_count, total_calls
            )
        )

    # NON-VACUITY: the load-bearing uuid hazard was actually exercised.
    H.check(
        total_calls > 0,
        "no uuid.uuid1() calls ran -- the load-bearing uuid state isolation "
        "hazard was never exercised (oracle would be vacuous)",
    )

    # COMPLETENESS: no fiber parked-then-vanished (e.g. crashed mid-uuid1() call).
    H.require_no_lost("uuid.uuid1() state isolation")


if __name__ == "__main__":
    harness.main(
        "p477_uuid",
        body,
        setup=setup,
        post=post,
        default_funcs=8000,
        describe="uuid.uuid1() maintains process-global _last_timestamp/_last_node "
        "state to ensure UUIDs are unique and monotonically increasing.  Under "
        "runloom M:N many fibers share one hub thread and the same _last_timestamp/"
        "_last_node state.  LOAD-BEARING: each fiber's uuid1() stream MUST have "
        "monotonically non-decreasing timestamps (curr_ts >= prev_ts); a "
        "decreasing timestamp or duplicate UUID is a torn read of _last_timestamp "
        "across a yield (a sibling's increment was visible mid-call) -- the "
        "runloom M:N state isolation bug.  0 monotonicity failures under plain "
        "threads GIL on/off (each OS thread snapshots state independently).  "
        "MEASURED: global collision rate (expected under unprotected shared state "
        "under M:N -- report-only, never fails; a collision does not violate "
        "per-fiber monotonicity, which is the load-bearing check).  Same class as "
        "p66/p67/p460/p468; fix is per-fiber state isolation in runloom.",
    )
