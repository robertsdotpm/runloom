"""big_100 / 408 -- queue.PriorityQueue heap-ordering under M:N producers/consumers.

queue.PriorityQueue is queue.Queue with `_put = heappush(self.queue, item)` and
`_get = heappop(self.queue)`, both executed UNDER the queue's `not_empty` /
`not_full` Condition.  Once threading is monkey-patched that Condition is the
cooperative CoCondition wrapping a CoLock, so a `get()` that finds the queue
empty PARKS the fiber inside `not_empty.wait()` -- on a grown-down C stack, on
whatever hub it was running on -- while producers and other consumers on OTHER
hubs keep doing heappush/heappop on the SAME `self.queue` list and keep calling
`item.__lt__` to sift the heap.

Why this stresses free-threading:
  * every put/get touches the shared `self.queue` list -- a heappush past a
    capacity boundary REALLOCs the list's ob_item array, and a heappop sifts
    entries down; both run while a different hub may be reading/writing that same
    array (list refcount + ob_item churn under the GIL off);
  * heapq compares items with tuple `__lt__`, which INCREFs/decrefs each element
    of the tuple being compared -- a comparison touching an item that another
    fiber is simultaneously dropping (last reference released on its own get())
    is exactly the use-after-free / torn-refcount window;
  * a torn heap would surface as an out-of-ORDER pop (an item handed back whose
    priority is below one still buffered), a LOST or DUPLICATED item, a TORN
    payload (value not f(seq)), or a comparison raising.

The falsifiable invariants (closed-world oracle, one shared queue per round
hammered by many producer+consumer fibers across hubs):

  1. ORDERING / heap-floor.  We pop the heap min and, UNDER THE SAME mutex,
     read the new heap top (`self.queue[0]` after the heappop).  The popped
     priority must be <= that remaining floor: the queue must never hand back an
     item whose priority is lower than one still buffered at the instant of the
     get.  A torn heap that sifts wrong returns popped > floor -> FAIL.  (We
     compare only the integer priority, never the payload, so equal-priority
     ties -- deliberately frequent -- are legal.)

  2. CONSERVATION.  Producers put a closed universe of NITEMS items with unique
     seq in [0, NITEMS) and payload == f(seq).  Across all consumers every seq
     is pulled EXACTLY ONCE: union covers the universe, with no seq pulled twice
     (lost item or duplicated item is a torn-heap fault).

  3. IDENTITY.  Every pulled item's payload == f(its seq): a (prio, seq, payload)
     tuple whose payload != f(seq) is a torn entry (fields from different/freed
     slots).

  4. NO SPURIOUS RAISE.  The only exception a get() may raise is queue.Empty (on
     a timed get that legitimately found nothing yet) -- any other exception
     (e.g. a comparison TypeError from a corrupted item, an IndexError from a
     torn heap) is a fault.

Termination is heap-ordered and deterministic: after the universe is produced,
the round puts NCONS sentinels at a priority ABOVE every real item, so heap
order drains every real item before any consumer sees its sentinel; a consumer
that pulls a sentinel returns.  Thus every consumer returns and the universe is
fully drained every round (post() asserts the per-round conservation held).

Two consumer modes are round-robined by worker id in the first ops so BOTH are
covered under load even when each worker completes only a handful of rounds
(pure-random case-selection reliably misses one at low op-count -- the flaky-
coverage bug p125/p126/p172 had to fix):
  * mode 0  BLOCKING   -- get() with no timeout, parks in not_empty.wait().
  * mode 1  TIMED      -- get(timeout=...) with Empty-retry, exercising the
                          timed wait + Empty path concurrently with the heap.

Stresses: PriorityQueue heappush/heappop on a shared list across hubs, tuple
__lt__ refcounting an item another fiber is dropping, Condition park/notify
under M:N, heap-order preservation, closed-world conservation, timed-get Empty.
"""
import queue
import random

import harness
import runloom

# Closed universe per round: enough items to push the heap list through several
# growth/realloc boundaries (the realloc is what moves ob_item out from under a
# concurrent heappush/heappop), but small enough that many rounds complete under
# the timeout-bound window.
NITEMS = 600

# Producer / consumer fibers sharing ONE queue per round.  Several of each so
# the single shared queue + its Condition + its heap list are driven from many
# hubs at once.  (Items are split across producers; consumers race to drain.)
NPROD = 4
NCONS = 6

# Priorities are drawn from a SMALL range so equal-priority ties are frequent --
# ties are the interesting case for tuple __lt__ (it must fall through to the
# next tuple element, comparing seq, which refcounts that element too).
PRIO_RANGE = 16

# Sentinel priority sits strictly ABOVE every real priority, so heap order drains
# all real items before any consumer pulls a sentinel and returns.  Sentinel seq
# is the out-of-universe marker -1.
SENTINEL_PRIO = PRIO_RANGE + 100
SENTINEL_SEQ = -1

# Consumer modes (round-robined by worker id, see worker()).
MODE_BLOCKING = 0
MODE_TIMED = 1
NMODES = 2

# Per-worker tally slots.  64k slots (one writer per slot, masked off wid) so a
# tally `+=` is single-writer race-free up to 65536 workers -- matching the
# harness's own NSHARDS sharding rather than the 1024-slot pattern, which aliases
# (and races) once --funcs exceeds 1024.
NSLOTS = 1 << 16
SLOT_MASK = NSLOTS - 1


def f(seq):
    """Deterministic seq -> payload.  A pulled item whose payload != f(seq) is a
    TORN tuple (the payload came from a different/freed slot).  Reversible-ish so
    a torn pair is unlikely to coincidentally satisfy it."""
    return (seq * 2654435761 + 0x9E3779B9) & 0xFFFFFFFFFFFF


class FloorPQ(queue.PriorityQueue):
    """PriorityQueue that checks the heap-ORDER invariant ATOMICALLY inside
    _get(), under the same Condition mutex Queue.get() already holds when it
    calls self._get().

    Doing the check INSIDE _get is what makes the oracle race-free: the popped
    priority and the remaining heap floor (self.queue[0][0] AFTER the heappop)
    are both read under the serializing mutex, in the same critical section, so
    no other consumer on another hub can pop/push between the two reads.  (An
    earlier version stashed the floor on a shared attribute and read it back in
    the consumer AFTER the mutex was released -- that read-after-release race in
    the ORACLE produced false 'heap order' hits; the fix is to never leave the
    critical section between the pop and the floor read.)

    A real torn heap (heappop that sifted wrong under concurrent heappush/
    heappop) returns popped > floor here and is reported through H.fail (which
    is itself lock-guarded).  The check covers sentinel pops too (a sentinel
    handed back ahead of a buffered real item is itself an order violation); only
    an EMPTIED queue (nothing left to compare against) is exempt."""

    def bind(self, H):
        self.harness = H
        return self

    def _get(self):
        item = queue.PriorityQueue._get(self)        # heappop under the mutex
        # self.queue is the heap list; queue[0] is the new min AFTER the pop.
        # Both reads happen here, still under the held mutex -> race-free.  The
        # check covers SENTINEL pops too: a sentinel (top priority) handed back
        # while a lower-priority real item is still buffered is itself an order
        # violation, so we do NOT exempt sentinels from the floor check.
        if self.queue:
            popped = item[0]
            floor = self.queue[0][0]
            if popped > floor:
                self.harness.fail(
                    "HEAP ORDER VIOLATED: popped priority {0} (seq {1}) > "
                    "remaining heap floor {2} -- the queue handed back an item "
                    "lower-ranked than one still buffered AT THE INSTANT OF THE "
                    "POP (torn heap under concurrent heappush/heappop)".format(
                        popped, item[1], floor))
        return item


def producer(H, wid, q, seqs, rng):
    """Put a slice of the closed universe (its seq list) into the shared queue
    with random, frequently-tied priorities and payload == f(seq)."""
    for seq in seqs:
        prio = rng.randrange(PRIO_RANGE)
        q.put((prio, seq, f(seq)))
        # Occasionally yield so producers interleave with consumers on other
        # hubs (drives the park/notify + heap churn concurrently rather than
        # front-loading all puts before any get).
        if (seq & 7) == 0:
            runloom.yield_now()


def consumer(H, wid, q, mode, mine):
    """Drain the shared queue until this consumer pulls a sentinel, validating
    each real item's IDENTITY (payload == f(seq)) and recording its seq into
    `mine` -- a PRIVATE per-consumer list (single-writer -> race-free; the
    caller unions all consumers' lists after the join to check conservation).
    Heap ORDERING is checked atomically inside FloorPQ._get.  mode selects
    blocking vs timed get."""
    while True:
        if not H.running():
            return
        try:
            if mode == MODE_BLOCKING:
                item = q.get()
            else:
                # Timed get: loop on Empty so a transient empty queue (producers
                # still filling, or other consumers ahead) retries rather than
                # exiting early -- the sentinel is the only legal stop.
                item = None
                while item is None:
                    if not H.running():
                        return
                    try:
                        item = q.get(timeout=0.05)
                    except queue.Empty:
                        continue
        except queue.Empty:
            continue
        except Exception as exc:                       # noqa: BLE001
            # (4) Any non-Empty exception out of get()/heappop/__lt__ is a fault:
            # a comparison TypeError from a corrupted item, an IndexError from a
            # torn heap, etc.
            H.fail("get() raised {0}: {1} -- not the legal Empty outcome (torn "
                   "heap / corrupted item under M:N)".format(
                       type(exc).__name__, exc))
            return

        prio, seq, payload = item
        if seq == SENTINEL_SEQ:
            return                                     # our sentinel: done
        # (3) IDENTITY: payload must be f(seq).
        if payload != f(seq):
            H.fail("TORN ITEM: seq {0} payload {1} != f(seq) {2} (payload came "
                   "from a different/freed slot under concurrent heap mutation)"
                   .format(seq, payload, f(seq)))
            return
        mine.append(seq)                               # private -> race-free


def run_round(H, wid, rng, mode, counts, slot):
    """One round: build a shared FloorPQ, spawn NPROD producers + NCONS consumers
    that hammer it across hubs, then verify the round's conservation.

    Termination: producers trip `prod_done` as they finish; a sentinel-pusher
    waits on `prod_done` (so the WHOLE universe is enqueued first), then pushes
    NCONS top-priority sentinels.  Heap order drains every real item before any
    consumer pulls its sentinel and returns -- so every consumer returns and the
    universe is fully drained each round.  `wg` joins producers + consumers + the
    sentinel-pusher so the worker round is one accountable op."""
    q = FloorPQ().bind(H)

    # Partition the universe across producers (contiguous slices; random
    # priorities re-shuffle heap order regardless of the seq layout).
    all_seqs = list(range(NITEMS))
    per = (NITEMS + NPROD - 1) // NPROD
    prod_slices = [all_seqs[i:i + per] for i in range(0, NITEMS, per)]
    while len(prod_slices) < NPROD:                    # pad if NITEMS < NPROD
        prod_slices.append([])

    # One PRIVATE seq-list per consumer (single-writer each -> race-free).  The
    # worker unions them AFTER the join to check conservation; we never write a
    # shared container from multiple hubs.
    cons_lists = [[] for _ in range(NCONS)]

    wg = runloom.WaitGroup()                           # joins ALL fibers this round
    wg.add(NPROD + NCONS + 1)                          # +1 for the sentinel-pusher
    prod_done = runloom.WaitGroup()                    # producers -> sentinel-pusher
    prod_done.add(NPROD)

    def run_prod(seqs, pseed):
        # Each producer gets its OWN deterministic Random (a shared one corrupts
        # GIL-off); seed derived from the worker rng.
        prng = random.Random(pseed)
        try:
            producer(H, wid, q, seqs, prng)
        finally:
            prod_done.done()
            wg.done()

    def run_cons(mine):
        try:
            consumer(H, wid, q, mode, mine)
        finally:
            wg.done()

    def push_sentinels():
        # Wait until producers have enqueued the WHOLE universe before adding the
        # sentinels, so no consumer pulls a sentinel while real items are still
        # unproduced (which would strand them -> a conservation MISS that is a
        # TEST artifact, not a runtime bug).
        try:
            prod_done.wait()
            for _ in range(NCONS):
                q.put((SENTINEL_PRIO, SENTINEL_SEQ, 0))
        finally:
            wg.done()

    # Spawn consumers FIRST so some are already parked in not_empty.wait() when
    # producers start pushing -- maximizing the park/notify + concurrent-heap
    # overlap the bug lives in.
    for c in range(NCONS):
        H.fiber(run_cons, cons_lists[c])
    H.fiber(push_sentinels)
    for i in range(NPROD):
        H.fiber(run_prod, prod_slices[i], rng.getrandbits(48))

    wg.wait()

    if H.failed:
        return False

    # SHUTDOWN GUARD: if the run-deadline fell during this round, consumers
    # returned EARLY on `not H.running()` (the harness contract -- workers stop at
    # the deadline), legitimately leaving real items undrained.  That is a benign
    # shutdown, NOT a lost item, so do not run the conservation check on a round
    # the deadline cut short -- it would false-FAIL exactly at t=deadline.  The
    # heap-ORDER and IDENTITY invariants already ran live on every item that WAS
    # pulled, so a real torn-heap fault is still caught regardless.
    if not H.running():
        return True

    # (2) CONSERVATION, checked in THIS single fiber after the join (race-free):
    # union every consumer's private list -> every seq in [0, NITEMS) appears
    # EXACTLY ONCE.  A duplicate seq = the heap returned one slot to two
    # consumers (torn heap); a missing seq = a lost item.
    seen = [0] * NITEMS
    total = 0
    for lst in cons_lists:
        for seq in lst:
            total += 1
            if seq < 0 or seq >= NITEMS:
                H.fail("OUT-OF-UNIVERSE seq {0} pulled (corrupted/torn entry)"
                       .format(seq))
                return False
            seen[seq] += 1
            if seen[seq] > 1:
                H.fail("DUPLICATED item: seq {0} pulled {1} times (heappop "
                       "returned the same slot to two consumers -- torn heap)"
                       .format(seq, seen[seq]))
                return False
    missing = NITEMS - sum(1 for c in seen if c == 1)
    if not H.check(total == NITEMS and missing == 0,
                   "CONSERVATION: round pulled {0} real items, {1}/{2} distinct "
                   "seqs missing (a lost item -- heappop dropped a slot under "
                   "concurrent mutation)".format(total, missing, NITEMS)):
        return False
    counts[slot] += total                              # real items this round
    return True


def worker(H, wid, rng, state):
    counts = state["consumed"]
    slot = wid & SLOT_MASK
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the two consumer modes by worker id in the first ops so
        # BOTH blocking and timed get() are exercised under load even when each
        # worker manages only a few rounds (pure-random selection misses a mode
        # at low op-count -- the flaky-coverage bug p125/p126/p172 had to fix).
        if i < NMODES:
            mode = (wid + i) % NMODES
        else:
            mode = rng.randrange(NMODES)
        i += 1
        if state["modes"][mode][slot] == 0:
            state["modes"][mode][slot] = 1
        if not run_round(H, wid, rng, mode, counts, slot):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "consumed": [0] * NSLOTS,                  # real items consumed (per slot)
        "modes": [[0] * NSLOTS, [0] * NSLOTS],     # which modes were exercised
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    consumed = sum(H.state["consumed"])
    blocking = sum(H.state["modes"][MODE_BLOCKING])
    timed = sum(H.state["modes"][MODE_TIMED])
    H.log("items_consumed={0} rounds={1} blocking_mode={2} timed_mode={3}".format(
        consumed, H.total_ops(), blocking, timed))
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(consumed > 0, "no items consumed -- the heap was never exercised")
    H.check(blocking > 0, "blocking-get mode never exercised")
    H.check(timed > 0, "timed-get mode never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p408_priorityqueue_ordering", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="shared queue.PriorityQueue hammered by producer/"
                          "consumer fibers across hubs; heap order preserved "
                          "(popped<=remaining floor), closed-world conservation, "
                          "payload==f(seq), only Empty may raise -- else torn heap")
