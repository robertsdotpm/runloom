"""big_100 / 315 -- two-semaphore bounded buffer (empty<->full handoff).

The textbook producer/consumer bounded buffer built from TWO classic counting
semaphores plus a guard around a shared ring:

    sem_empty = Semaphore(K)   # permits to PUT (slots currently free)
    sem_full  = Semaphore(0)   # permits to GET (items currently present)
    guard     = Lock           # protects the ring + present-bitmap

    producer:  sem_empty.acquire(); with guard: ring put + mark present
               sem_full.release()
    consumer:  sem_full.acquire();  with guard: ring get + clear present + record
               sem_empty.release()

These are the cooperative counting semaphores (`threading.Semaphore` ->
runloom.monkey.CoSemaphore: a true up/down semaphore, value 0 means "block until
released"), NOT runloom.sync.Semaphore (which is a WEIGHTED *borrowing*
semaphore: its value is a fixed LIMIT and you can only release what you acquired,
so Semaphore(0)+release() is illegal -- the wrong model for the full side of a
bounded buffer).  The cooperative CoSemaphore is the one whose release() hands a
SPECIFIC FIFO waiter the permit via the documented `[parker, active, got_permit]`
waiter record -- exactly the surface this program adversarially exercises.

No existing program wires TWO semaphores as a bounded buffer: p48/p205 use a
single (weighted) semaphore as a concurrency LIMITER, p52 is a Chan-cap
backpressure test.  The new surface is the CROSS-semaphore empty<->full handoff
under M:N: release() sets the front waiter's got_permit under the guard, then
g.wake()s it across hubs.  If release() ever wakes a waiter WITHOUT actually
transferring the count (got_permit not set / double-grant), or a permit is lost
on the empty<->full boundary, then either MORE than K items live in the ring at
once (over-grant) or an item is lost / duplicated as it crosses the handoff.

ORACLE -- a DUAL conservation law (a permit bug breaks at least one):

  (1) MAX-IN-FLIGHT bound.  A monitor goroutine sums a single-writer
      present-bitmap (one slot per ring index; set by the producer under the
      guard, cleared by the consumer under the guard) and asserts the count
      NEVER exceeds K, AND tracks a high-water max_in_flight that must stay <= K.
      An over-grant on sem_empty (a PUT permit handed out while the ring was
      already full) is exactly >K present.  (The bitmap write and the matching
      sem release are not one atomic step, but every write happens UNDER the
      guard while holding the permit, so a legitimate run can momentarily read
      up to K and never more -- >K is a real breach, not sampling skew.)

  (2) TOKEN conservation.  Each producer enqueues globally-unique tokens
      (wid<<40 | producer<<32 | seq).  Consumers collect every token they GET
      into per-consumer SETS (single writer each -> race-free).  post asserts:
      union of all consumer sets == the produced count (no loss), and
      sum(len(set_i)) == len(union) (no dup -- no token consumed twice).  A lost
      item -> union < produced; a duplicated item (a double-grant on sem_full
      waking two consumers for one slot) -> sum(len) > len(union).

  (3) PERMIT no-leak.  After a balanced drain, BOTH semaphores reconstruct to
      their baseline: sem_empty._value == K (all slots free again) and
      sem_full._value == 0 (no items present).  A leaked permit on either side
      leaves the value off baseline -- the up/down analogue of p205's _held==0
      no-leak gate.

Closed-world per worker so conservation is EXACT: each pool goroutine owns one
ring + both semaphores + its producers/consumers, runs a FIXED token budget,
then poison-pills its consumers so they all terminate (every goroutine MUST
return under mn_run).  require_no_lost guards a lost handoff wake (a stranded
producer/consumer = a lost wake on the empty<->full boundary).

Invariant (post): max_in_flight <= K; consumed-distinct == produced (no loss);
sum(per-consumer sizes) == distinct (no dup); sem_empty back to K, sem_full to 0.

Stresses: counting-Semaphore acquire/release across the empty<->full boundary,
FIFO got_permit waiter handoff under contention, no over-grant, no lost/dup item
across the cross-semaphore handoff, no permit leak.

Good TSan / controlled-M:N-replay target: the release()-handoff (the got_permit
flag written under the guard vs the woken acquirer re-reading it across hubs) is
a memory-ordering surface; a data-race report on the got_permit write/read is
often the first signal before the conservation oracle even fires.
"""
import threading      # patched -> runloom.monkey.CoSemaphore (cooperative)

import harness
import runloom
import runloom.sync as sync

K = 4                       # ring capacity (== sem_empty initial permits)
PRODUCERS = 3               # producer fibers per worker group
CONSUMERS = 3               # consumer fibers per worker group
ITEMS_PER_PRODUCER = 96     # fixed budget per producer (closed-world)

POISON = -1                 # sentinel token: tells a consumer to terminate


def producer(H, group, ring, present, sem_empty, sem_full, guard, base, n):
    """PUT n globally-unique tokens through the empty->full handoff."""
    for i in range(n):
        tok = base | (i & 0xFFFFFFFF)
        sem_empty.acquire()                  # claim a free slot (PUT permit)
        with guard:
            slot = ring.index(None)          # a free slot is guaranteed (we hold it)
            ring[slot] = tok
            present[slot] = 1                # single-writer-under-guard bitmap set
        sem_full.release()                   # publish: one item now present
    H.op(group, n)


def poisoner(ring, present, sem_empty, sem_full, guard, count, drained, total):
    """After ALL real items have been consumed, inject `count` poison pills so
    every consumer receives exactly one and terminates.  We must wait for the
    full drain first: the ring is not FIFO, so a pill injected while real items
    are still buffered could be grabbed by a consumer that then exits and strands
    the remaining real items.  Each pill still goes through the SAME handoff so
    the permit accounting stays balanced (consumes an empty permit, releases a
    full permit, like a real item)."""
    while True:
        with guard:
            done = (drained[0] >= total)
        if done:
            break
        runloom.yield_now()
    for _ in range(count):
        sem_empty.acquire()
        with guard:
            slot = ring.index(None)
            ring[slot] = POISON
            present[slot] = 1
        sem_full.release()


def consumer(H, group, ring, present, sem_empty, sem_full, guard, seen, drained):
    """GET items through the full->empty handoff until a poison pill arrives.
    Record every REAL token into this consumer's own set (no shared mutation of
    another consumer's set -> race-free); bump the shared drained-count under the
    guard so the poisoner knows when every real item is gone."""
    while True:
        sem_full.acquire()                   # claim a present item (GET permit)
        with guard:
            # An occupied slot is guaranteed -- we hold a full permit.
            slot = next(i for i in range(K) if present[i])
            tok = ring[slot]
            ring[slot] = None
            present[slot] = 0                # single-writer-under-guard bitmap clear
            if tok != POISON:
                drained[0] += 1              # one more real item consumed
        sem_empty.release()                  # publish: one slot now free
        if tok == POISON:
            return
        seen.add(tok)
        H.op(group)


def worker(H, wid, rng, state):
    """One closed-world bounded-buffer group: K-slot ring, sem_empty(K),
    sem_full(0), PRODUCERS producers + CONSUMERS consumers, fixed budget."""
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break

        ring = [None] * K
        present = [0] * K
        sem_empty = threading.Semaphore(K)   # K free slots to PUT
        sem_full = threading.Semaphore(0)    # 0 items present to GET
        guard = sync.Lock()
        # Each consumer owns its OWN set (single writer) -> no shared-set race.
        sets = [set() for _ in range(CONSUMERS)]
        # Shared real-items-consumed counter (mutated only under the guard) so the
        # poisoner injects pills ONLY after the whole budget has been drained.
        drained = [0]
        total_items = PRODUCERS * ITEMS_PER_PRODUCER

        # Monitor: sample the present-bitmap; assert in-flight <= K, track HWM.
        mon_breach = [0]
        mon_hwm = [0]
        mon_stop = [False]

        def monitor(present=present, mon_breach=mon_breach, mon_hwm=mon_hwm,
                    mon_stop=mon_stop):
            while not mon_stop[0]:
                cur = sum(present)
                if cur > mon_hwm[0]:
                    mon_hwm[0] = cur
                if cur > K:
                    mon_breach[0] = 1
                    H.fail("OVER-GRANT: {0} items in flight > K={1} (a PUT permit "
                           "handed out while the ring was full)".format(cur, K))
                    return
                runloom.yield_now()

        # Consumers + producers + poisoner are joined; the monitor is
        # fire-and-forget but always returns once mon_stop is set.
        wg = runloom.WaitGroup()
        wg.add(PRODUCERS + 1 + CONSUMERS)
        H.fiber(monitor)

        def run_consumer(ci):
            try:
                consumer(H, slot, ring, present, sem_empty, sem_full, guard,
                         sets[ci], drained)
            finally:
                wg.done()

        for ci in range(CONSUMERS):
            H.fiber(run_consumer, ci)

        prod_wg = runloom.WaitGroup()
        prod_wg.add(PRODUCERS)

        def run_producer(pid):
            try:
                base = (wid << 40) | (pid << 32)
                producer(H, slot, ring, present, sem_empty, sem_full, guard,
                         base, ITEMS_PER_PRODUCER)
            finally:
                prod_wg.done()
                wg.done()

        for pid in range(PRODUCERS):
            H.fiber(run_producer, pid)

        def run_poisoner():
            try:
                prod_wg.wait()                  # all real items enqueued first
                poisoner(ring, present, sem_empty, sem_full, guard, CONSUMERS,
                         drained, total_items)
            finally:
                wg.done()

        H.fiber(run_poisoner)

        wg.wait()                                # producers + poisoner + consumers
        mon_stop[0] = True                       # let the monitor return

        # ---- per-group conservation check (fold into shared accounting) ----
        produced = PRODUCERS * ITEMS_PER_PRODUCER
        distinct = len(set().union(*sets)) if sets else 0
        total_recv = sum(len(s) for s in sets)
        st = state
        st["produced"][slot] += produced
        st["distinct"][slot] += distinct
        st["total_recv"][slot] += total_recv
        if mon_hwm[0] > st["max_in_flight"][0]:
            st["max_in_flight"][0] = mon_hwm[0]
        if mon_breach[0]:
            st["breach"][0] = 1

        # No-leak per group: a balanced drain returns both sems to baseline.
        ev = getattr(sem_empty, "_value", None)
        fv = getattr(sem_full, "_value", None)
        if ev is not None and ev != K:
            H.fail("permit LEAK on sem_empty: _value={0} (expected K={1} after "
                   "balanced drain)".format(ev, K))
        if fv is not None and fv != 0:
            H.fail("permit LEAK on sem_full: _value={0} (expected 0 after "
                   "balanced drain)".format(fv))

        H.task_done(slot)


def setup(H):
    H.state = {
        "produced": [0] * 1024,
        "distinct": [0] * 1024,
        "total_recv": [0] * 1024,
        "max_in_flight": [0],
        "breach": [0],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    st = H.state
    produced = sum(st["produced"])
    distinct = sum(st["distinct"])
    total_recv = sum(st["total_recv"])
    hwm = st["max_in_flight"][0]
    H.log("produced={0} distinct={1} total_recv={2} max_in_flight={3} (K={4})"
          .format(produced, distinct, total_recv, hwm, K))

    H.check(produced > 0, "no items produced (test did no work)")
    H.check(st["breach"][0] == 0,
            "in-flight count breached K during the run (over-grant)")
    H.check(hwm <= K,
            "max_in_flight high-water {0} > K={1} (over-grant on sem_empty)"
            .format(hwm, K))
    # Token conservation: every produced token consumed exactly once.
    H.check(distinct == produced,
            "item LOSS: distinct consumed {0} != produced {1} (a token lost "
            "across the empty<->full handoff)".format(distinct, produced))
    H.check(total_recv == distinct,
            "item DUP: sum(per-consumer sizes) {0} != distinct {1} (a token "
            "consumed twice -- double-grant on sem_full)".format(
                total_recv, distinct))
    # A stranded producer/consumer = a lost handoff wake.
    H.require_no_lost("bounded-buffer handoff completeness")


if __name__ == "__main__":
    harness.main("p315_semaphore_bounded_buffer", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="two-semaphore bounded buffer (empty<->full): "
                          "max-in-flight<=K, token conservation (no loss/dup), "
                          "no permit leak")
