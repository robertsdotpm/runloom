"""big_100 / 488 -- heapq heap isolation under M:N.

heapq maintains a MinHeap in a plain Python list where events are stored in heap
order (parent <= children).  Multiple fibers can each construct a SEPARATE heap
(a plain list) and independently push/pop DISTINCT events.  Under M:N, multiple
fibers share one hub OS-thread, so they compete for Python's GIL-free interpreter
atomicity.  If a fiber yields mid-heap-operation (during heappush or heappop),
a sibling fiber on the same hub can simultaneously mutate a DIFFERENT heap list,
and if runloom's fiber isolation is weak, the shared hub memory can leak one
fiber's heap mutations into another's.

WHERE M:N BREAKS IT (the gap this program probes).  heapq's operations on a list
are NOT atomic across fiber yields:

  1. heappush(heap, item): appends the item, then "bubbles up" by swapping with
     parents (mutating the list in place).
  2. heappop(heap): swaps the root with the last element, shrinks the list, then
     "bubbles down" the new root (many mutations to the list).

Each fiber maintains its OWN SEPARATE heap (a distinct list object).  If
runloom's fiber isolation is correct, a sibling fiber's heap operations run on a
DIFFERENT list object and cannot corrupt this fiber's heap.  But if runloom leaks
memory or shares hub state incorrectly, a sibling's concurrent push/pop can race
and corrupt this fiber's heap: reordered elements, lost elements, or a heap
invariant violation (parents > children).  The next pop then dequeues in wrong
order or crashes.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically):

  The load-bearing oracle: a fiber creates a single heap instance (a plain list,
  distinct from siblings' heaps), pushes 0..N unique events, yields to let
  siblings run their own heaps (no cross-fiber heap mutation possible -- each has
  its own list), then pops all N events and asserts they come out in the SAME
  order they were pushed (heap order preserved).  Corruption of heap order
  (out-of-sequence pop) or lost events (fewer than N popped) indicates a runloom
  heap-corruption or sibling-mutation leak, which should NOT happen with separate
  instances.  The test then runs a SECOND PHASE with a SHARED heap: all fibers
  push events into ONE shared heap, then drain.  The shared phase is MEASURED +
  reported for cross-fiber event loss / disorder, NEVER failed -- because a shared
  heap is NOT documented as M:N-safe (contention on the shared list is expected).

  On a CORRECT runtime (and plain threads, GIL on AND off -- verified via control
  program), each fiber's SEPARATE heap stays perfectly isolated.  The private-
  instance phase NEVER fires, so the program exits 0 when there is no bug.

ORACLES:
  * LOAD-BEARING -- PRIVATE HEAP ISOLATION (worker, HARD, fail-fast).
    Each fiber creates its OWN heap list (a distinct Python list object).  It
    pushes N events at a common priority key, yields (a scheduling point where
    siblings run their own heaps, not this one).  Then it pops all events and
    asserts:
      - all N events were popped (no loss);
      - events came out in push order (heap order preserved);
      - no event is duplicated;
      - the heap is exactly empty (no residue).
    A pop out of order or < N events suggests a runloom heap-corruption (sibling
    mutation on a supposedly private heap list, or fiber resuming with a torn
    heap state after a yield).  FAIL fast.  (On plain threads GIL on AND off,
    the private-instance oracle NEVER fires, so the program exits 0 when there is
    no bug.)

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    heap-operation (stranded inside heappush / heappop, or lost on a bad wake)
    never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing private-instance arm actually ran
    (priv_events_popped > 0).

  * MEASURED (report-ONLY, NEVER fails): the SHARED HEAP arm.  A minority of
    workers push events into ONE global shared heap (contention on the same list),
    yield, and drain.  Cross-fiber event loss / disorder is expected (many fibers'
    heap mutations on one list), documented-unsafe usage for M:N (not an M:N-safe
    primitive).  We measure the loss/disorder rate and REPORT it; we NEVER fail on
    it -- failing would mislabel the shared-instance contention as a bug when it
    is a caveat.  The shared arm runs in isolation (separate workers, separate
    pre-phase) so its measured drift cannot poison the load-bearing private-
    instance oracle.

FAIL ON: a private heap's pop out-of-order, missing events, or any heap
         corruption in the private phase.  NEVER fail on the shared phase
         (documented-unsafe).

Stresses: heapq operations on plain Python lists across hub fibers, heappush /
heappop atomicity across fiber yields, private heap-instance isolation under M:N,
heap invariant (parent <= children), event identity and uniqueness.

Good TSan / controlled-M:N-replay target: heappush / heappop mutate the list
(append, swaps, pop operations); a data race on list indices, or a replay that
yields a fiber mid-heappush / mid-heappop and runs a sibling's heap op on a
different list but the same hub, localizes the tear before the out-of-order
oracle fires.
"""
import heapq

import harness
import runloom

# Primes for unique event IDs across the entire run.
_EVENT_COUNTER = [0]
_EVENT_LOCK = runloom.sync.Lock()


def next_event_id():
    """Allocate a unique event ID across all fibers."""
    with _EVENT_LOCK:
        eid = _EVENT_COUNTER[0]
        _EVENT_COUNTER[0] += 1
    return eid


# Event payload: (priority, event_id, payload_marker).
# Each fiber's pushed events have unique event_ids so we can verify no loss/
# duplication.  On pop, the event_ids must come out in the order they were
# pushed (for the same priority), which is a closed-world invariant.
def make_event(priority, marker):
    """Return a (priority, unique_id, marker) tuple for the heap."""
    return (priority, next_event_id(), marker)


def setup(H):
    H.state = {
        # LOAD-BEARING: private heap instance arm.
        "priv_checks": [0] * 1024,                  # fibers that ran the private phase
        "priv_events_pushed": [0] * 1024,           # total events pushed (private)
        "priv_events_popped": [0] * 1024,           # total events popped (private)
        "priv_out_of_order": [0] * 1024,            # popped out-of-order (the bug)
        "priv_lost_events": [0] * 1024,             # pushed but never popped
        "priv_wrong_values": [0] * 1024,            # popped wrong event/duplicate
        # MEASURED: shared heap instance arm (report-only).
        "shared_checks": [0] * 1024,                # fibers that ran the shared phase
        "shared_events_pushed": [0] * 1024,         # total events pushed (shared)
        "shared_events_popped": [0] * 1024,         # total events popped (shared)
        "shared_out_of_order": [0] * 1024,          # popped out-of-order (expected)
        "shared_lost_events": [0] * 1024,           # lost in shared contention
        # Shared heap instance used in the MEASURED arm.
        "shared_heap": [],
        # Fraction of workers assigned to the MEASURED shared arm.
        "shared_fraction": 0.2,
        "sample": [None],                           # first observed corruption
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: PRIVATE heap instance.
# Each fiber creates its OWN heap (a plain list), pushes N unique events at a
# common priority P, yields (so siblings' separate heaps can run on the hub),
# then pops and asserts events come out in push order.
# --------------------------------------------------------------------------
def private_phase(H, wid, r, rng, state):
    """LOAD-BEARING: private heap instance integrity.

    No cross-fiber heap mutation possible -- each fiber owns its heap list.
    """
    # Distinct priority per fiber so we can verify order within priority.
    priority = 100 + (wid % 50)

    # Build a sequence of unique events to push.
    n_events = 10 + rng.randint(0, 20)
    pushed = []
    for i in range(n_events):
        evt = make_event(priority, "priv-{0}-{1}-{2}".format(wid, r, i))
        pushed.append(evt)

    # Create a PRIVATE heap instance (each fiber's own list).
    priv_heap = []

    # Push all events.
    for evt in pushed:
        heapq.heappush(priv_heap, evt)

    state["priv_events_pushed"][wid & 1023] += n_events

    # YIELD: let siblings run their OWN heaps (no contention on this one's list).
    # A sibling cannot mutate our heap because they have their own list object.
    # If runloom leaks a sibling's heap mutation into our list, that is the
    # runloom bug.
    runloom.sleep(0.0001)
    runloom.yield_now()
    if rng.random() < 0.5:
        runloom.sleep(0.0002)

    # Pop all events and verify order.
    popped = []
    while priv_heap:
        evt = heapq.heappop(priv_heap)
        popped.append(evt)

    state["priv_events_popped"][wid & 1023] += len(popped)

    # Verify: all events popped, in push order, no duplicates.
    if len(popped) != n_events:
        state["priv_lost_events"][wid & 1023] += (n_events - len(popped))
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "priv-loss", len(popped), n_events)
        H.fail("private heap LOST EVENTS: pushed {0} but popped {1} (wid {2}) -- "
               "heap corruption or event loss mid-yield, runloom leaked a sibling's "
               "list mutation into this private heap".format(n_events, len(popped), wid))
        return

    # Verify order: popped event_ids must match pushed (within the priority).
    for i, (evt_p, evt_e) in enumerate(zip(popped, pushed)):
        if evt_p[1] != evt_e[1]:  # event_id mismatch
            state["priv_out_of_order"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "priv-order", i, evt_p[1], evt_e[1])
            H.fail("private heap OUT-OF-ORDER: event {0} at pop index {1} has id {2}, "
                   "expected {3} (wid {4}) -- heap order corrupted, runloom leaked "
                   "a sibling's heap mutation".format(evt_p, i, evt_p[1], evt_e[1], wid))
            return
        if evt_p != evt_e:  # full tuple mismatch (payload or priority)
            state["priv_wrong_values"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "priv-value", i, evt_p, evt_e)
            H.fail("private heap WRONG VALUE: popped {0} != expected {1} at index {2} "
                   "(wid {3}) -- event corrupted".format(evt_p, evt_e, i, wid))
            return

    # Verify the heap is now empty.
    if priv_heap:
        state["priv_wrong_values"][wid & 1023] += len(priv_heap)
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "priv-residue", priv_heap)
        H.fail("private heap NOT EMPTY after drain: {0} events remain (wid {1}) -- "
               "events lost or not popped, heap corrupted".format(len(priv_heap), wid))
        return

    state["priv_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: SHARED heap instance (report-only, contention expected).
# A minority of workers push into ONE global heap, yield, and drain.
# Cross-fiber event loss / order drift is expected and measured, NEVER failed.
# This arm runs in isolation (separate workers, separate pre-phase) so it
# cannot poison the load-bearing private-instance oracle.
# --------------------------------------------------------------------------
def shared_phase(H, wid, r, rng, state):
    """MEASURED: shared heap instance contention (report-only).

    Many fibers push/pop ONE shared heap list -- cross-fiber mutation is expected
    (documented-unsafe).
    """
    heap = state["shared_heap"]

    # Shared priority so all events are comparable.
    priority = 200

    # Small N to keep the contention window tight and measured (not the bottleneck).
    n_events = 5 + rng.randint(0, 10)
    my_event_ids = []

    # Push events.
    for i in range(n_events):
        evt = make_event(priority, "shared-{0}-{1}-{2}".format(wid, r, i))
        my_event_ids.append(evt[1])
        heapq.heappush(heap, evt)

    state["shared_events_pushed"][wid & 1023] += n_events

    # YIELD: let siblings push/pop the SAME heap list.  Contention is expected.
    runloom.sleep(0.0001)
    runloom.yield_now()

    # Pop events: we expect to see SOME of our events, but not necessarily all
    # (siblings may have taken them first), and not necessarily in order (the
    # shared heap is being mutated concurrently).  MEASURE, never fail.
    my_events_popped = []
    # Drain the ENTIRE shared heap (so we count what happened to our events).
    while heap:
        evt = heapq.heappop(heap)
        # If this event is one of mine (by event_id), track it.
        if evt[1] in my_event_ids:
            my_events_popped.append(evt[1])

    state["shared_events_popped"][wid & 1023] += len(my_events_popped)

    # Measure: did we get all of our events back?
    if len(my_events_popped) < n_events:
        state["shared_lost_events"][wid & 1023] += (n_events - len(my_events_popped))

    # Measure: did our events come out in order?
    for i, (my_id, expected_id) in enumerate(zip(my_events_popped, my_event_ids)):
        if my_id != expected_id:
            state["shared_out_of_order"][wid & 1023] += 1
            break  # count once per worker

    state["shared_checks"][wid & 1023] += 1


# Sustained checks per worker.
INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING private-instance phase ONLY.

    The shared phase is run in a SEPARATE fully-drained pre-phase so its measured
    drift cannot contaminate the private-instance oracle.
    """
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            private_phase(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
    H.task_done(wid)


def run_shared_phase(H, state):
    """MEASURED pre-phase: spawn a modest shared-instance worker pool, let them
    contend on ONE shared heap, fully drain them (WaitGroup.wait) before
    returning.  This runs BEFORE the load-bearing private-instance pool so the
    two arms never touch the shared heap concurrently -- the measured contention
    drift is isolated to this drained pre-phase and cannot poison the private-
    instance oracle.
    """
    fraction = state.get("shared_fraction", 0.2)
    nshared = max(1, int(H.funcs * fraction))
    if nshared <= 0:
        return

    wg = runloom.WaitGroup()
    wg.add(nshared)

    def run_one(wid):
        rng = H.derive("shared", wid)
        try:
            for r in range(max(1, H.rounds)):
                if not H.running():
                    break
                shared_phase(H, wid, r, rng, state)
        finally:
            wg.done()

    for wid in range(nshared):
        H.fiber(run_one, wid)
    wg.wait()


def body(H):
    # Phase 1 (MEASURED, fully drained): the shared-instance contention arm,
    # in isolation so it cannot contaminate the private-instance oracle.
    run_shared_phase(H, H.state)

    # Phase 2 (LOAD-BEARING): the private-instance pool.
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    priv_checks = sum(H.state["priv_checks"])
    priv_enq = sum(H.state["priv_events_pushed"])
    priv_deq = sum(H.state["priv_events_popped"])
    priv_oo = sum(H.state["priv_out_of_order"])
    priv_lost = sum(H.state["priv_lost_events"])
    priv_wrong = sum(H.state["priv_wrong_values"])

    shared_checks = sum(H.state["shared_checks"])
    shared_enq = sum(H.state["shared_events_pushed"])
    shared_deq = sum(H.state["shared_events_popped"])
    shared_oo = sum(H.state["shared_out_of_order"])
    shared_lost = sum(H.state["shared_lost_events"])

    sample = H.state["sample"][0]

    H.log("heapq[private LOAD-BEARING]: checks={0} pushed={1} popped={2} "
          "out-of-order={3} lost={4} wrong={5} sample={6}".format(
              priv_checks, priv_enq, priv_deq, priv_oo, priv_lost, priv_wrong,
              sample))
    H.log("heapq[shared MEASURED]: checks={0} pushed={1} popped={2} "
          "out-of-order={3} ({4:.1f}%) lost={5} ({6:.1f}%, documented-unsafe "
          "contention -- REPORT ONLY)".format(
              shared_checks, shared_enq, shared_deq, shared_oo,
              (100.0 * shared_oo / shared_checks) if shared_checks else 0,
              shared_lost,
              (100.0 * shared_lost / shared_enq) if shared_enq else 0))

    # LOAD-BEARING: the private-instance arm must never corrupt heap order or
    # lose events.  Each fiber's heap is a private list; no cross-fiber mutation
    # is possible.  If the private arm shows out-of-order or lost events, that is
    # a runloom heap-corruption or sibling-list-leak (the runloom bug).
    if priv_oo or priv_lost or priv_wrong:
        H.fail("heapq PRIVATE-INSTANCE CORRUPTED: the LOAD-BEARING arm observed "
               "out-of-order={0} lost={1} wrong={2} events -- each fiber owns its "
               "private heap (a distinct list object), so cross-fiber mutation is "
               "NOT possible.  A corruption here is a runloom heap-tear (sibling's "
               "list mutation leaked into a private heap, or a fiber resumed with a "
               "torn heap after a yield) -- a runloom M:N bug, NOT a documented "
               "caveat.".format(priv_oo, priv_lost, priv_wrong))

    # NON-VACUITY: the load-bearing private-instance hazard was exercised.
    H.check(priv_checks > 0,
            "no private-instance heap checks ran -- the load-bearing heap-"
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-heap operation.
    H.require_no_lost("heapq private-instance heap isolation")

    # Informational: the shared phase observed expected contention.
    if shared_oo or shared_lost:
        H.log("note: the shared-instance arm observed out-of-order={0} and "
              "lost={1} events across {2} shared pushes -- many fibers contended "
              "on ONE shared heap list, so cross-fiber event loss / disorder is "
              "documented-unsafe (a caveat, NOT a runloom bug); this measured "
              "drift was isolated to the shared-phase pre-run and never reached "
              "the private-instance oracle.".format(
                  shared_oo, shared_lost, shared_enq))


if __name__ == "__main__":
    harness.main("p488_sched", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="heapq.heappush/heappop maintain heap order in a plain "
                          "Python list.  LOAD-BEARING: each fiber creates its OWN "
                          "private heap list and pushes unique events at a common "
                          "priority.  After yield (so siblings run their own heaps), "
                          "events pop in push order; out-of-order or lost events = "
                          "runloom heap-tear (sibling's list mutation leaked into "
                          "private heap, or torn heap resume after yield) -- the M:N "
                          "bug.  0 under plain threads GIL on AND off; a private-"
                          "instance oracle firing is a true runloom signal.  MEASURED: "
                          "a shared-instance arm (documented-unsafe, contention "
                          "expected) runs in pre-phase isolation and reports order "
                          "drift, never fails")
