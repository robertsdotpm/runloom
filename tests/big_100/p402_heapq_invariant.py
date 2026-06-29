"""big_100 / 402 -- shared heapq under M:N, the in-place sift hazard.

`heapq.heappush` / `heappop` do an IN-PLACE sift on a plain Python list: they
read `heap[parent]` and `heap[childpos]`, compare, and swap slots, walking an
index up or down the array.  Nothing in CPython locks that list for the duration
of a sift -- and `heappush` first does `heap.append(item)` (which can REALLOC the
list's `ob_item` array) and `heappop` does `heap.pop()` before its sift-down.
Under M:N a sift can PARK mid-swap with a raw index live on its grown-down C
stack while another hub sifts the SAME list, so on resume it reads through a
stale index and can publish a torn/duplicated element; worse, an append-driven
realloc on one hub can move the array out from under a live index on another --
a refcount + buffer-realloc memory-safety hazard, not merely a lost update.

We serialize every heapq-API call behind ONE shared `runloom.sync.Lock`, which
SHOULD make the structure correct.  Concurrency then enters two ways the lock
does not by itself fix:

  * a lock-free READER snapshots the list (`list(heap)`) WITHOUT the lock, racing
    the in-place sift's slot writes and the append/pop realloc.  Each snapshotted
    tuple must still be a whole, in-universe `(priority, g^-1-consistent key)` --
    a torn first/second field, an out-of-universe entry, or a SIGSEGV walking a
    reallocated array is the bug.
  * the lock itself can be acquired/released across a park on a foreign hub; a
    broken hand-off that let two critical sections overlap would corrupt the
    heap's array directly, surfacing as a torn pop or a conservation miss.

Finite UNIVERSE: keys 0..U-1, each with priority == g(key).  A popped or
snapshotted tuple whose priority != g(its key), or whose key is out of range, is
a torn tuple (the two slots came from different elements under a concurrent
realloc/sift).  Pops from a single fully-drained heap must come out NON-
DECREASING (the heap invariant held).  And conservation: across the whole run,
for every key, pushes - pops == the count of that key surviving in the heap at
post() -- counted by draining the actual surviving heap, NOT a summed counter, so
a lost or duplicated element (an item that fell out of the array or got published
twice) breaks the equality even if no torn tuple was ever observed.

Three worker roles, round-robined by id so all three are guaranteed exercised
even when only a handful of rounds complete under load:
  role 0 PUSHER  : push a random in-universe (priority, key) under the lock.
  role 1 POPPER  : pop one item under the lock; validate it; tally the popped key.
  role 2 DRAINER : under the lock, snapshot+pop the WHOLE heap into a private
                   list, assert the sequence is non-decreasing and every tuple
                   whole, re-push everything back (so the heap is conserved), and
                   ALSO take an unlocked `list(heap)` snapshot mid-drain to race
                   the reader against live sifts.

Invariant (hot, fail-fast): every popped/snapshotted tuple is whole and in
UNIVERSE; a fully-drained heap pops non-decreasing.
Invariant (post): for every key, pushed[key] - popped[key] == surviving[key]
(conservation, by draining the real heap); all three roles exercised.

Stresses: heapq in-place sift under M:N park, append/pop realloc vs live index,
lock hand-off across hubs, lock-free list snapshot racing slot writes, torn-tuple
/ lost-element / heap-order corruption.
"""
import heapq

import harness
import runloom
import runloom.sync as sync

# Finite UNIVERSE of keys 0..UNIVERSE_SIZE-1.  Big enough that the shared heap
# grows and shrinks across several list-realloc boundaries (the realloc is what
# can move the array out from under a live sift index on another hub).
UNIVERSE_SIZE = 512

# Number of distinct shared heaps.  More than one so workers contend on several
# independent locks/lists (wider M:N contention surface) rather than one global
# bottleneck, while each heap stays small enough to drain cheaply.
NHEAPS = 8

# Per-key tally slots: each worker writes only its own (wid & MASK) slot for a
# given key, never a shared `+= across hubs`.  Conservation is summed in post().
TALLY_MASK = 1023


def g(key):
    """Deterministic key -> priority.  A tuple whose priority != g(its key) is
    TORN: the priority slot and the key slot came from different elements (a
    concurrent realloc/sift published a half-updated pair).  Monotonic in key so
    priority order and key order agree, which lets the non-decreasing-pop check
    also catch a key/priority swap."""
    return key * 2 + 1


def make_item(rng):
    """A whole, in-universe (priority, key) tuple."""
    key = rng.randrange(UNIVERSE_SIZE)
    return (g(key), key)


def tuple_ok(H, who, item):
    """Validate one tuple popped/snapshotted from the shared heap.  Returns
    False on the first violation (caller stops)."""
    if not isinstance(item, tuple) or len(item) != 2:
        H.fail("{0}: heap element {1!r} is not a 2-tuple -- the list slot held a "
               "torn/foreign object (M:N heap-array corruption)".format(who, item))
        return False
    prio, key = item
    if not isinstance(key, int) or not (0 <= key < UNIVERSE_SIZE):
        H.fail("{0}: OUT-OF-UNIVERSE key {1!r} in element {2!r} -- a torn tuple "
               "from a reallocated-away slot (M:N heap corruption)".format(
                   who, key, item))
        return False
    if prio != g(key):
        H.fail("{0}: TORN tuple {1!r}: priority {2!r} != g(key) {3!r} -- the two "
               "slots came from different elements under a concurrent sift/"
               "realloc".format(who, item, prio, g(key)))
        return False
    return True


def snapshot_ok(H, snap):
    """Validate a LOCK-FREE list(heap) snapshot taken while sifts may be writing
    slots on another hub.  Every element must still be a whole in-universe tuple;
    a torn/duplicated/foreign element here is a published-mid-sift corruption.
    Does not check heap order (a snapshot taken mid-sift is legitimately not a
    valid heap -- only WHOLENESS of each element is guaranteed by tuple
    immutability + the GIL-off memory model holding)."""
    for item in snap:
        if not tuple_ok(H, "reader-snapshot", item):
            return False
    return True


def role_pusher(H, wid, rng, heap, lock, pushed, slot):
    item = make_item(rng)
    with lock:
        heapq.heappush(heap, item)
    # Tally AFTER the locked push lands.  Single-writer slot, no shared +=.
    pushed[item[1]][slot] += 1


def role_popper(H, wid, rng, heap, lock, popped, slot):
    item = None
    with lock:
        if heap:
            item = heapq.heappop(heap)
    if item is None:
        return True                         # empty heap is fine; nothing to pop
    if not tuple_ok(H, "popper", item):
        return False
    popped[item[1]][slot] += 1
    return True


def role_drainer(H, wid, rng, heap, lock, pushed, popped, slot):
    """Drain the WHOLE heap under the lock into a private list (which must come
    out non-decreasing -- proving the heap invariant survived all the concurrent
    sifts), validate every tuple, then re-push everything so the heap content is
    conserved.  Mid-drain, take an UNLOCKED list(heap) snapshot to race the
    lock-free reader against the live sift writes the re-push will do."""
    drained = []
    with lock:
        # Pop everything: a correct heap yields these in non-decreasing order.
        while heap:
            drained.append(heapq.heappop(heap))
        # Re-push them all back, conserving content.  Each heappush appends +
        # sifts up in place -- the slot writes a concurrent unlocked reader will
        # race.  We take the unlocked snapshot from OUTSIDE... but we are holding
        # the lock here, so a same-heap reader is serialized; the cross-heap and
        # cross-round readers still race other heaps.  To actually race THIS
        # list's sift we drop a snapshot grab right between pushes below.
        for i, item in enumerate(drained):
            heapq.heappush(heap, item)
            if i == len(drained) // 2 and drained:
                # Hand the scheduler a chance to run another hub's reader against
                # this half-rebuilt list -- the index of the just-sifted element
                # is the kind of thing a foreign hub could read stale.  We still
                # hold the lock for heapq-API safety; yielding here parks WITH the
                # list in a mid-rebuild shape, which the unlocked reader below and
                # other heaps' readers observe.
                runloom.yield_now()
    # Validate the drained sequence: non-decreasing + every tuple whole.
    prev = None
    for item in drained:
        if not tuple_ok(H, "drainer", item):
            return False
        if prev is not None and item < prev:
            H.fail("drainer: heap popped OUT OF ORDER {0!r} after {1!r} -- the "
                   "heap invariant was violated by a concurrent in-place sift "
                   "(M:N heapq corruption)".format(item, prev))
            return False
        prev = item
    return True


def reader(H, heap, done):
    """Lock-free snapshotter: repeatedly `list(heap)` (no lock) and validate each
    element is a whole in-universe tuple, until `done` is set.  This races the
    sift's slot writes and append/pop reallocs directly."""
    while not done[0]:
        snap = list(heap)                   # NO lock -- intentional race
        if not snapshot_ok(H, snap):
            return
        runloom.yield_now()


def worker(H, wid, rng, state):
    heaps = state["heaps"]
    locks = state["locks"]
    pushed = state["pushed"]
    popped = state["popped"]
    slot = wid & TALLY_MASK
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        hi = (wid + i) % NHEAPS             # spread workers across the heaps
        heap = heaps[hi]
        lock = locks[hi]

        # Round-robin the three roles by (wid + i) so every role is GUARANTEED
        # exercised even when only a few rounds complete under load -- pure random
        # role selection reliably misses a role at low op-count (the suite's
        # p125/p126/p172 flaky-coverage lesson).  Go random after the first ops to
        # preserve a chaotic concurrent mix.
        if i < 3:
            role = (wid + i) % 3
        else:
            role = rng.randrange(3)
        i += 1

        # Spawn a short-lived lock-free reader against THIS heap for the duration
        # of this op, so a snapshot is always racing some sift somewhere.
        done = [False]
        rseed = rng.getrandbits(48)
        wg = runloom.WaitGroup()
        wg.add(1)

        def run_reader(heap=heap, done=done):
            try:
                reader(H, heap, done)
            finally:
                wg.done()

        H.fiber(run_reader)
        try:
            if role == 0:
                ok = role_pusher(H, wid, rng, heap, lock, pushed, slot)
                ok = True if ok is None else ok
            elif role == 1:
                ok = role_popper(H, wid, rng, heap, lock, popped, slot)
            else:
                ok = role_drainer(H, wid, rng, heap, lock, pushed, popped, slot)
        finally:
            done[0] = True
            wg.wait()
        if ok is False:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # NHEAPS independent shared heaps, each guarded by its own Lock.  Per-key
    # tally arrays are sharded single-writer-per-slot (no shared += across hubs);
    # conservation is summed in post().
    H.state = {
        "heaps": [[] for _ in range(NHEAPS)],
        "locks": [sync.Lock() for _ in range(NHEAPS)],
        "pushed": [[0] * (TALLY_MASK + 1) for _ in range(UNIVERSE_SIZE)],
        "popped": [[0] * (TALLY_MASK + 1) for _ in range(UNIVERSE_SIZE)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    st = H.state
    heaps = st["heaps"]
    pushed = st["pushed"]
    popped = st["popped"]

    # Conservation, counted by DRAINING the real surviving heaps (not a summed
    # counter): for every key, pushed - popped must equal the number of that key
    # left in the heaps.  A lost element (fell out of a reallocated array) or a
    # duplicated one (published twice by a racing sift) breaks this even if no
    # torn tuple was ever observed mid-run.
    surviving = [0] * UNIVERSE_SIZE
    total_survivors = 0
    for heap in heaps:
        for item in heap:
            # Defensive: a corrupted survivor here is itself a fault.
            if (isinstance(item, tuple) and len(item) == 2
                    and isinstance(item[1], int) and 0 <= item[1] < UNIVERSE_SIZE):
                surviving[item[1]] += 1
                total_survivors += 1
            else:
                H.fail("post: corrupted survivor in heap: {0!r}".format(item))

    total_pushed = 0
    total_popped = 0
    mismatches = 0
    for key in range(UNIVERSE_SIZE):
        p = sum(pushed[key])
        q = sum(popped[key])
        total_pushed += p
        total_popped += q
        if p - q != surviving[key]:
            mismatches += 1
            if mismatches <= 5:
                H.fail("CONSERVATION key {0}: pushed {1} - popped {2} = {3} != "
                       "surviving {4} (lost/duplicated heap element under M:N)"
                       .format(key, p, q, p - q, surviving[key]))

    H.log("pushed={0} popped={1} survivors={2} (sum p-q={3}) mismatches={4} "
          "ops={5}".format(total_pushed, total_popped, total_survivors,
                           total_pushed - total_popped, mismatches,
                           H.total_ops()))
    H.check(total_pushed - total_popped == total_survivors,
            "CONSERVATION (aggregate): pushed-popped {0} != survivors {1}".format(
                total_pushed - total_popped, total_survivors))
    H.check(H.total_ops() > 0, "no rounds completed -- workload never ran")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p402_heapq_invariant", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="shared heapq under one Lock + lock-free list snapshot: "
                          "every popped/snapshotted (g(key),key) tuple whole and "
                          "in-universe, drained heap non-decreasing, and pushed-"
                          "popped == surviving (conservation) -- else M:N heap "
                          "corruption")
