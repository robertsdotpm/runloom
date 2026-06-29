"""big_100 / 428 -- queue.SimpleQueue unbounded FIFO direct-handoff conservation.

The subject is the cooperative ``queue.SimpleQueue`` -- after monkey.patch()
every ``queue.SimpleQueue()`` is runloom's ``CoSimpleQueue``
(src/runloom/monkey/queues.py:27).  It is the ONE queue in the cooperative
stdlib with NO Condition / no internal lock: it hand-rolls TWO bare
``collections.deque``s and mutates them WITHOUT serialization --

    __slots__ = ("_items", "_waiters")

    def put(self, item, block=True, timeout=None):
        self._items.append(item)                 # (A) append to _items
        while self._waiters:                     # (B) read _waiters
            self._waiters.popleft().unpark()      # (C) pop a waiter, hand off
            break

    def get(self, block=True, timeout=None):
        if self._items:                          # (D) read _items ...
            return self._items.popleft()         # (E) ... then popleft  <-- TOCTOU
        ...
        self._waiters.append(p)                  # (F) enqueue self as waiter
        p.park(timeout); p.release()
        if self._items:
            return self._items.popleft()         # (G) take handed item
        try: self._waiters.remove(p)             # (H) timed-out self-removal
        except ValueError: pass
        raise Empty

The exact C-level state under attack is the pair of ``deque`` objects
``_items`` and ``_waiters`` and their ``ob_item`` ring-block pointers, mutated
by the racing pair:

  * (put) ``_items.append`` + ``_waiters.popleft`` (direct hand-off)  vs
  * (get) ``_items.popleft`` (fast path D->E) / ``_waiters.append`` + park.

With the GIL off and runloom in M:N, producers on one hub and consumers on
another touch these UNGUARDED deques truly in parallel.  Three mutually-
exclusive corruption modes, each made falsifiable below:

  * LOST ITEM.  A put reads ``_waiters`` non-empty (B) and pops a head waiter
    (C) that is SIMULTANEOUSLY timing out and self-removing (H) on another hub;
    the item was appended to ``_items`` but the wake races the removal -- or two
    concurrent ``deque.popleft`` on a one-element ``_items`` underflow one side.
    A payload appended by a producer is never delivered to any consumer.
  * DUPLICATED ITEM.  Two consumers both pass the fast-path read (D) on a
    one-element ``_items`` and both reach ``popleft`` (E); a non-atomic deque
    ``popleft`` torn across hubs hands the SAME logical slot to both -- the same
    payload is delivered twice (or a direct hand-off to a waiter races a
    fast-path pop of the same item).
  * TORN / OUT-OF-UNIVERSE PAYLOAD.  A ``deque`` block realloc/rotate under a
    concurrent append+popleft publishes a half-written ``ob_item`` slot; the
    value read back is not a payload this round ever put (or SIGSEGV).

TARGET INVARIANT -- closed-world CONSERVATION of a finite sentinel UNIVERSE of
payloads.  Per round a worker owns ONE shared SimpleQueue and spawns PRODUCERS
producer fibers + CONSUMERS consumer fibers on different hubs:

  * each payload is ``encode(pid, seq)`` drawn from a fixed UNIVERSE, UNIQUE
    within the round (producers get disjoint id ranges).  Each producer puts a
    KNOWN multiset (here: a contiguous run of its own unique payloads), recorded
    in a single-writer-per-slot ``offered[]`` table, and ALSO into its OWN
    PRIVATE SimpleQueue put/get'd race-free -- the CONTROL arm.
  * consumers ``get()`` real payloads until they receive their poison ``None``
    sentinel, marking each payload exactly once in a per-round ``seen`` array
    (a SECOND mark on the same slot is a DUPLICATE -- hot fail-fast) and
    asserting every value decodes back into this round's universe.

After the producer WaitGroup joins, the main fiber puts CONSUMERS ``None``
sentinels (one drains each consumer; the hand-off of a ``None`` to a parked
waiter exercises path B/C against H), then joins the consumer WaitGroup.  Now
quiescent and single-owner:

  * multiset(received) == multiset(offered) EXACTLY: every payload delivered
    once, none lost, none duplicated (the per-round ``seen`` array == the set of
    offered payloads, |seen| == total offered);
  * every received value in UNIVERSE and decodes to a (pid, seq) of THIS round
    (a torn slot reads out-of-universe -> hard fault);
  * ``shared.empty()`` / ``qsize()==0`` and ``_waiters`` empty at quiesce (no
    item stranded, no waiter leaked);
  * FIFO-per-producer suffix: per-producer payloads are a contiguous seq run, so
    we assert per-producer COUNT received == count offered (cross-consumer
    arrival order is NOT asserted -- M:N is not asyncio-deterministic; per-
    producer total + universe membership is the conserved law).

CONTROL ARM.  Alongside the contended shared queue, each producer drives a
PRIVATE single-owner SimpleQueue with the SAME multiset and immediately drains
it itself; a private queue put/get'd by one fiber is race-free by construction,
so if the CONTROL loses or duplicates a payload the fault is the CoSimpleQueue
deque machinery itself, not cross-hub contention -- that disambiguates "the
primitive is buggy" from "M:N contention dropped it".

COVERAGE (the p125/p126/p172 flaky-random lesson): the consumer's get() mode
(blocking get vs timed get-with-retry vs get_nowait-spin) is round-robined by
(wid + consumer index) % NMODES in the first ops so every get path that touches
``_items``/``_waiters`` differently is exercised even under a short window.

Stresses: SimpleQueue _items/_waiters two-deque direct-handoff, deque
append/popleft non-atomic across hubs, put-handoff vs timed-waiter self-removal,
fast-path D->E double-pop, poison-pill drain, payload conservation (no lost / no
duplicate / no torn) under M:N, private-vs-shared SimpleQueue control.

Good TSan / controlled-M:N-replay target: the unguarded ``_items.append`` vs
``_items.popleft`` (and ``_waiters.popleft`` hand-off vs ``_waiters.remove``)
are textbook deque data races; a TSan report on the deque block pointer, or a
single duplicated/dropped payload under replay, localizes the fault before the
conservation reconciliation even closes.
"""
import queue

import harness
import runloom

# Finite sentinel UNIVERSE of payload values.  Every value a consumer ever takes
# must decode into this space; a value outside it is a torn/garbage deque slot.
# A payload = UNIVERSE_BASE + (pid_local * PER_PRODUCER + seq), so per round the
# PRODUCERS * PER_PRODUCER live payloads are a contiguous, recognizable, UNIQUE
# block.  Sized to push _items through many deque ring-blocks (a deque block is
# 64 slots) so append/popleft cross block boundaries where the realloc/rotate
# race lives.
UNIVERSE_BASE = 0x42800000
PER_PRODUCER = 256          # payloads each producer puts per round (>= 4 blocks)
PRODUCERS = 4               # producer fibers per shared queue per round
CONSUMERS = 4               # consumer fibers per shared queue per round
TOTAL = PRODUCERS * PER_PRODUCER          # real payloads per round
UNIVERSE_SIZE = TOTAL                      # one universe slot per live payload
GUARD_SHARDS = 64                          # shard the per-item dup-check guard so
                                           # only TRUE same-item marks contend (the
                                           # queue itself is unguarded -- the probe);
                                           # a single guard convoyed at high funcs.
# None is the poison sentinel that drains a consumer.  It is NOT a payload, so it
# is never in the universe and never marked in `seen`.

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Consumer get() MODES -- each touches the _items/_waiters paths differently.
MODE_BLOCK = 0      # get(block=True): fast-path D->E or park on _waiters
MODE_TIMED = 1      # get(timeout=t): park with a deadline, retry on Empty
MODE_SPIN = 2       # get_nowait() spin + yield: pure fast-path D->E hammering
NMODES = 3


def encode(pid_local, seq):
    """Unique payload for (producer-local-id, seq) within a round.  Bijective
    onto a contiguous block of the UNIVERSE so a torn value almost never decodes
    back to a legal (pid, seq)."""
    return UNIVERSE_BASE + pid_local * PER_PRODUCER + seq


def decode(value):
    """Inverse of encode.  Returns (pid_local, seq) or None if `value` is not a
    payload of this round's universe (a torn/out-of-universe slot)."""
    off = value - UNIVERSE_BASE
    if off < 0 or off >= TOTAL:
        return None
    return divmod(off, PER_PRODUCER)


def run_producer(shared, pid_local):
    """Put this producer's KNOWN multiset of unique payloads into the SHARED
    queue (the contended _items.append + _waiters.popleft hand-off path), and the
    SAME multiset through a PRIVATE single-owner SimpleQueue that we drain
    ourselves (the race-free CONTROL).  Returns the count the private control
    conserved (must be PER_PRODUCER).  The shared put count is fixed and exact
    (PER_PRODUCER) -- it is recorded once by the worker fiber after the join, so
    we keep NO contended per-fiber tally here (sibling producers share a worker
    slot; a `tbl[slot] += 1` here would be the classic GIL-off lost-count race in
    our OWN accounting, not the queue)."""
    # CONTROL: a private queue put then immediately drained by this one fiber.
    # Single owner -> race-free; a loss/dup here is the deque machinery itself.
    private = queue.SimpleQueue()
    control_ok = 0
    for seq in range(PER_PRODUCER):
        payload = encode(pid_local, seq)
        private.put(payload)
        got = private.get()
        if got == payload:
            control_ok += 1
    # SHARED contended path: put every payload (no lock -- the unguarded two-deque
    # surface under test).  Yield mid-run so a consumer's popleft/park on another
    # hub overlaps our append + hand-off.
    for seq in range(PER_PRODUCER):
        shared.put(encode(pid_local, seq))
        if (seq & 63) == 0:
            runloom.yield_now()            # let consumers race the hand-off
    return control_ok


def run_consumer(H, shared, mode, seen, guard, slot, dup_box):
    """Drain real payloads from the SHARED queue until the poison `None` sentinel
    arrives, marking each payload exactly once in the per-round `seen` array.

    `seen` is written under `guard` (a DISTINCT cooperative lock, NOT the queue
    under test) ONLY so the mark+check is itself exact -- the queue's own
    _items/_waiters mutation stays fully unguarded, which is the race we probe.
    A second mark of the same slot is a DUPLICATE delivery -> hot fail-fast.  An
    out-of-universe value is a torn deque slot -> hot fail-fast.  We keep NO
    per-fiber received tally (sibling consumers share a worker slot; the per-round
    `seen` bitmap IS the authoritative delivery count, reconciled after join)."""
    # NB: NO `if not H.running(): return` deadline-bail here.  Producers never
    # check H.running() (they always put their full multiset), and the main fiber
    # always puts the CONSUMERS poison Nones after prod_wg.wait(), so every
    # consumer is guaranteed to receive its own None and exit cleanly -- bailing
    # mid-round on a duration deadline would abandon the shared queue with items
    # still in _items (and this consumer's None unconsumed), tripping the
    # empty-at-quiesce check with a FALSE "stranded payload".  The round drains to
    # completion regardless of the wall-clock deadline; H.running() gates whole
    # rounds in worker(), not the drain of an in-flight round.
    while True:
        try:
            if mode == MODE_BLOCK:
                item = shared.get(block=True, timeout=10.0)
            elif mode == MODE_TIMED:
                # Timed get: parks with a deadline; on a (rare, legal) Empty
                # timeout retry -- this is the path whose self-removal (H) races
                # a producer's hand-off (C).
                try:
                    item = shared.get(block=True, timeout=0.05)
                except queue.Empty:
                    runloom.yield_now()
                    continue
            else:  # MODE_SPIN -- pure fast-path D->E hammering via get_nowait
                try:
                    item = shared.get_nowait()
                except queue.Empty:
                    runloom.yield_now()
                    continue
        except queue.Empty:
            # A blocking get timed out: only legal if the round is winding down.
            runloom.yield_now()
            continue
        except IndexError:
            # A bare IndexError out of deque.popleft is the UNDERFLOW signature of
            # two consumers double-popping a one-element _items (the D->E TOCTOU)
            # -- the deque was read non-empty then popped empty across hubs.  This
            # IS the bug this program hunts: record it and fail.
            dup_box[slot] += 1
            H.fail("SimpleQueue.get() raised IndexError from deque.popleft -- "
                   "two consumers passed the fast-path empty check (D) then both "
                   "popleft'd a one-element _items (E): a torn double-pop / "
                   "underflow on the unguarded _items deque across hubs")
            return
        if item is None:
            # Poison sentinel -> this consumer is done.  None is never a payload.
            return
        # A real payload.  Validate it decodes into THIS round's universe.
        dec = decode(item)
        if dec is None:
            H.fail("SimpleQueue.get() yielded OUT-OF-UNIVERSE value {0!r} -- a "
                   "torn/garbage slot from a deque block realloc/rotate raced by "
                   "a concurrent append+popleft on _items".format(item))
            return
        pid_local, seq = dec
        # Mark exactly once.  A second mark == a DUPLICATED delivery.
        with guard[(item - UNIVERSE_BASE) % GUARD_SHARDS]:
            if seen[item - UNIVERSE_BASE]:
                dup_box[slot] += 1
                dupd = True
            else:
                seen[item - UNIVERSE_BASE] = 1
                dupd = False
        if dupd:
            H.fail("SimpleQueue delivered DUPLICATE payload (pid={0}, seq={1}, "
                   "value={2:#x}) -- the same logical _items slot was handed to "
                   "two consumers (fast-path D->E double-pop, or a direct "
                   "hand-off racing a fast-path pop of the same item)".format(
                       pid_local, seq, item))
            return


def run_round_impl(H, wid, rng, slot, state):
    """One conservation round: PRODUCERS producers put TOTAL unique payloads into
    one shared SimpleQueue (contended two-deque path) while CONSUMERS consumers
    drain them across hubs; after the producers join, poison each consumer with a
    None, join the consumers, then check the closed-world conservation law on the
    now-quiescent queue."""
    offered = state["offered"]
    received = state["received"]
    control = state["control"]
    dup_box = state["dup"]
    guard = [runloom.sync.Lock() for _ in range(GUARD_SHARDS)]  # per-item shard

    shared = queue.SimpleQueue()           # CoSimpleQueue after monkey.patch()
    # Per-round delivery bitmap: one slot per live payload (single mark each).
    seen = [0] * UNIVERSE_SIZE

    prod_wg = runloom.WaitGroup()
    prod_wg.add(PRODUCERS)
    cons_wg = runloom.WaitGroup()
    cons_wg.add(CONSUMERS)

    control_box = [0] * PRODUCERS          # per-producer private-control conserved

    def producer(idx):
        try:
            control_box[idx] = run_producer(shared, idx)
        finally:
            prod_wg.done()

    def consumer(cidx):
        # Round-robin the get() mode by (wid + cidx) so every _items/_waiters get
        # path is exercised under a short window (the flaky-random fix).
        mode = (wid + cidx) % NMODES
        try:
            run_consumer(H, shared, mode, seen, guard, slot, dup_box)
        finally:
            cons_wg.done()

    # Spawn consumers FIRST so some are already parked on _waiters when producers
    # start putting -- that is when the direct hand-off path (B/C) fires, and when
    # a timed waiter's self-removal (H) can race a hand-off.
    for cidx in range(CONSUMERS):
        H.fiber(consumer, cidx)
    for idx in range(PRODUCERS):
        H.fiber(producer, idx)

    prod_wg.wait()                         # every real payload has been put
    # Poison each consumer: one None drains exactly one consumer.  A None handed
    # to a parked waiter exercises put's hand-off (C) against a live waiter.
    for _ in range(CONSUMERS):
        shared.put(None)
    cons_wg.wait()                         # consumers joined -> queue quiescent

    if H.failed:
        return

    # ---- closed-world conservation law (round now quiescent, single reader) ----
    # 1. Private-control: each single-owner SimpleQueue conserved its whole
    #    multiset (a loss HERE is the deque machinery, not contention).
    for idx in range(PRODUCERS):
        if not H.check(control_box[idx] == PER_PRODUCER,
                       "private CONTROL SimpleQueue lost/corrupted a payload: "
                       "producer {0} round-tripped {1}/{2} -- a single-owner "
                       "queue put/get'd by one fiber must conserve exactly "
                       "(CoSimpleQueue deque machinery bug, not contention)"
                       .format(idx, control_box[idx], PER_PRODUCER)):
            return

    # 2. The shared queue is EMPTY at quiesce (no item stranded in _items, no
    #    waiter leaked in _waiters).
    if not H.check(shared.empty() and shared.qsize() == 0,
                   "shared SimpleQueue not empty at quiesce: qsize={0} -- a "
                   "payload was stranded in _items (a hand-off / pop was lost "
                   "across hubs)".format(shared.qsize())):
        return
    if not H.check(len(shared._waiters) == 0,
                   "shared SimpleQueue left {0} waiter(s) in _waiters at quiesce "
                   "-- a consumer parked and was never handed an item nor removed "
                   "(lost-wakeup on the _waiters deque)".format(
                       len(shared._waiters))):
        return

    # 3. CONSERVATION: every offered payload was delivered exactly once.  `seen`
    #    is the per-round delivery bitmap; it must be all-ones over exactly the
    #    TOTAL live payloads (no payload lost == every slot marked; no duplicate
    #    already caught hot via the second-mark check).
    delivered = 0
    for off in range(UNIVERSE_SIZE):
        if seen[off]:
            delivered += 1
        else:
            pid_local, sq = divmod(off, PER_PRODUCER)
            H.fail("CONSERVATION broken: payload (pid={0}, seq={1}, value={2:#x}) "
                   "was put by a producer but NEVER delivered to any consumer -- "
                   "a LOST item (put's _items.append/_waiters hand-off raced a "
                   "consumer's popleft/timed self-removal across hubs)".format(
                       pid_local, sq, UNIVERSE_BASE + off))
            return
    if not H.check(delivered == TOTAL,
                   "CONSERVATION count mismatch: delivered {0} != offered {1} "
                   "this round -- a payload was lost or duplicated on the "
                   "unguarded _items/_waiters deques".format(delivered, TOTAL)):
        return

    # 4. FIFO-per-producer suffix: per-producer total delivered == PER_PRODUCER
    #    (cross-consumer arrival order is intentionally NOT asserted -- M:N is not
    #    asyncio-deterministic; per-producer count + universe membership is the
    #    conserved law).  `seen` is contiguous per producer, so a full block of
    #    PER_PRODUCER ones per producer == this holds; verify it explicitly.
    for pid_local in range(PRODUCERS):
        base = pid_local * PER_PRODUCER
        cnt = sum(seen[base:base + PER_PRODUCER])
        if not H.check(cnt == PER_PRODUCER,
                       "per-producer conservation broken: producer {0} had {1}/"
                       "{2} of its payloads delivered -- this producer's run was "
                       "torn on _items".format(pid_local, cnt, PER_PRODUCER)):
            return

    # Record this round's exact tallies ONCE, here in the WORKER fiber (single
    # writer for `slot` -- run_round_impl runs sequentially per worker), so the
    # global post() reconciliation is race-free.  TOTAL was put and `delivered`
    # (== TOTAL, just proven) was taken; control conserved PRODUCERS*PER_PRODUCER.
    offered[slot] += TOTAL
    received[slot] += delivered
    control[slot] += PRODUCERS * PER_PRODUCER


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    for _ in H.round_range():
        if not H.running():
            break
        run_round_impl(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so queue.SimpleQueue is
    # the cooperative CoSimpleQueue and runloom.sync.Lock is M:N-safe.  `guard`
    # makes the per-round `seen` mark+check exact WITHOUT guarding the queue's own
    # _items/_waiters mutation (that stays the unguarded race we probe).
    H.state = {
        "guard": runloom.sync.Lock(),
        "offered": [0] * SLOTS,            # payloads put on the shared queue
        "received": [0] * SLOTS,           # real payloads taken off the shared queue
        "control": [0] * SLOTS,            # payloads conserved by private controls
        "dup": [0] * SLOTS,                # duplicate/underflow hits (must stay 0)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    offered = sum(H.state["offered"])
    received = sum(H.state["received"])
    control = sum(H.state["control"])
    dup = sum(H.state["dup"])
    H.log("shared-queue payloads offered={0} received={1} private-control "
          "conserved={2} duplicates/underflows={3} ops={4}".format(
              offered, received, control, dup, H.total_ops()))

    H.check(H.total_ops() > 0,
            "no conservation rounds completed -- the SimpleQueue two-deque "
            "hand-off race window was never exercised")

    # Reaching post with no failure already proves every per-round conservation +
    # private-control check held (they are fail-fast).  Assert the run did work
    # and that the global tallies reconcile: every payload put on a shared queue
    # was taken off exactly once (offered == received across the whole run).
    H.check(offered > 0,
            "no payloads were offered to any shared SimpleQueue (vacuous run)")
    H.check(offered == received,
            "GLOBAL conservation broken: total offered={0} != total received={1} "
            "-- the unguarded _items/_waiters deques lost or duplicated a payload "
            "summed across the whole run".format(offered, received))
    H.check(control > 0,
            "private-control arm never ran -- the single-owner SimpleQueue "
            "falsifier was not exercised")
    H.check(dup == 0,
            "{0} duplicate/underflow deliveries observed -- _items double-pop "
            "or duplicated hand-off on the shared SimpleQueue".format(dup))

    H.require_no_lost("simplequeue-handoff conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p428_simplequeue_unbounded_handoff_", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many producers/consumers race one queue.SimpleQueue's two "
                 "unguarded deques (_items append/popleft + _waiters direct "
                 "hand-off) across hubs; closed-world conservation of a finite "
                 "payload universe -- every payload delivered exactly once, none "
                 "lost/duplicated/torn, queue empty at quiesce, vs a private "
                 "single-owner SimpleQueue control")
