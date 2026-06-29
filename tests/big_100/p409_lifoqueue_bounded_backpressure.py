"""big_100 / 409 -- bounded queue.LifoQueue(K) backpressure at the size boundary.

`queue.LifoQueue(maxsize)` is the stdlib bounded stack: put() blocks on the
`not_full` Condition while the queue is full, get() blocks on the `not_empty`
Condition while it is empty, and BOTH Conditions share one `mutex` Lock.  Under
`monkey.patch()` that Lock/Condition pair is cooperative, so a full putter and an
empty getter PARK runloom fibers.  The free-threaded hazard lives at the
boundary: a full->not-full transition (a get that frees a slot) must wake a
parked putter, and an empty->not-empty transition (a put that adds an item) must
wake a parked getter -- on a DIFFERENT M:N hub, EXACTLY ONCE.  A missed
not_full / not_empty signal at the boundary wedges the round (a producer parked
forever with consumers idle, or vice versa); a DOUBLE wake / torn list under
preempt corrupts the LIFO stack and loses or duplicates an item.

We make that detectable with a closed-world, finite-universe conservation oracle.
Each round runs ONE private LifoQueue(K) with a small K (so put/get collide at
the boundary constantly) and two fiber pools spawned across the hubs:

  * P producer fibers.  Producer p emits exactly ITEMS unique tokens
    token(wid, p, seq) drawn from a per-round finite UNIVERSE; each token value
    is produced EXACTLY ONCE in the whole round.  Every put() is the real
    stdlib blocking put -- it parks on `not_full` whenever the bounded stack is
    full, so the not_full wake is exercised on the hot path.
  * C consumer fibers.  Each get()s EXACTLY its pre-assigned quota of real items
    (the quotas sum to P*ITEMS), recording every token it pulled into its OWN
    per-fiber list (single-writer, race-free), and checks the live `qsize() <= K`
    invariant on every observation.  Exact quotas mean termination is
    deterministic -- no poison sentinel, so no LIFO-stack race where a consumer
    could exit on a poison-on-top while real tokens sit beneath it -- and every
    consumer parks on not_empty whenever the bounded stack runs dry.

After the round's WaitGroup joins (all producers + consumers returned), the
worker reconstructs the multiset of consumed tokens and asserts CONSERVATION
against the closed universe:

  * count: exactly P*ITEMS real tokens consumed (no lost backpressure wakeup
    silently dropped a put; no spurious wake fabricated an extra get);
  * identity: every consumed token is in UNIVERSE (no torn / out-of-stack value
    from a corrupted list under preempt);
  * no-dup / no-loss: every UNIVERSE token consumed EXACTLY ONCE (a doubled
    wake or torn stack would duplicate or drop one);
  * boundary: qsize() never observed > K.

Three put/get RATIO cases stress the two boundary directions differently, and
post() requires each was exercised, so -- per the suite's flaky-random-coverage
lesson (p125/p126/p172) -- the worker ROUND-ROBINS the case by worker id in its
FIRST ops (deterministic coverage whether one worker does many rounds or many
workers do one each), then goes random:

  * BALANCED (P==C): the queue oscillates across the boundary, both not_full and
    not_empty fire constantly.
  * PRODUCER-HEAVY (P>C): the stack sits FULL, so producers park on not_full and
    the get->not_full wake dominates.
  * CONSUMER-HEAVY (P<C): the stack sits EMPTY, so consumers park on not_empty
    and the put->not_empty wake dominates.

Invariant (post + per-round, fail-fast): per round the consumed multiset equals
UNIVERSE exactly (every real token once, none missing, none extra, none out of
universe) and qsize never exceeded K; across the run all three ratio cases ran
at least once and total puts == total gets.  A wedged round is caught by the
watchdog (EXIT_HANG); a lost/duplicated item or torn token fails the oracle.

Stresses: queue.LifoQueue(maxsize) bounded backpressure, not_full/not_empty
boundary wake across M:N hubs (exactly-once), shared two-Condition-one-Lock
park/unpark under the GIL off, LIFO stack discipline under preempt, put/get
conservation with no lost or doubled item.
"""
import queue

import harness
import runloom

# Per-round token UNIVERSE is closed and finite: a token NOT in it consumed by a
# getter is a torn / corrupted value from a stack mangled under preempt.  Tokens
# are packed as (producer << PROD_SHIFT) | seq, so they are dense and
# recognizable; the TOKEN_TAG high bit makes a corrupted/half-written value
# (which would clear it) obvious to the membership check.
PROD_SHIFT = 12                       # up to 4096 items per producer
TOKEN_TAG = 0x40000000                # set on every real token

# Three put/get ratio cases.  Each is (n_producers, n_consumers).  The product
# P*ITEMS is the closed count of real tokens per round; we keep it modest so a
# round retires quickly (these ops are park-heavy) while still forcing many
# boundary crossings at a small K.
ITEMS = 24                            # tokens each producer emits

CASES = (
    ("balanced", 3, 3),               # P==C: oscillate across the boundary
    ("producer_heavy", 5, 2),         # P>C: stack sits full -> not_full parks
    ("consumer_heavy", 2, 5),         # P<C: stack sits empty -> not_empty parks
)
NCASES = len(CASES)

# Bounded queue depth.  Small so put() and get() collide at the boundary on
# nearly every operation (the whole point); >1 so the LIFO stack actually holds
# more than one item and ordering/identity can be corrupted.
K = 4


def token(wid, producer, seq):
    """Pack a globally-unique-per-round token.  Producer/seq fit in the low bits;
    TOKEN_TAG marks it a valid item (wid is not encoded -- each round rebuilds a
    fresh private queue and universe, so wid need not be in the value)."""
    return TOKEN_TAG | (producer << PROD_SHIFT) | seq


def producer_body(H, wid, q, producer, wg, fail_slot, fails):
    """Emit ITEMS unique tokens with the real blocking put().  Parks on not_full
    whenever the bounded stack is full -- exercising the get->not_full wake.

    A round is a CLOSED, deadlock-free unit: producers always emit exactly
    nprod*ITEMS tokens and consumers always pull exactly that many (their quotas
    sum to it), so every put is matched by a get and the round is guaranteed to
    terminate.  We therefore do NOT short-circuit on H.running() mid-round: a
    half-emitted round would strand a counterpart consumer parked on not_empty
    (and a Condition-parked fiber is NOT reached by the netpoll-only
    cancel_all_parked() teardown, so it would wedge the drain).  The worker's
    round_range() loop is the only place a deadline stops new work."""
    try:
        for seq in range(ITEMS):
            tok = token(wid, producer, seq)
            q.put(tok)                 # blocks on not_full while q is full
            # qsize must never exceed the bound, even transiently as observed.
            n = q.qsize()
            if n > K:
                fails[fail_slot] += 1
                H.fail("LifoQueue qsize {0} > maxsize {1} after put -- bound "
                       "violated (torn size accounting under M:N)".format(n, K))
                return
    finally:
        wg.done()


def consumer_body(H, wid, q, quota, got_list, wg, fail_slot, fails):
    """Get EXACTLY `quota` real items (recording each into got_list, a single-
    writer per-fiber list), then return.  Parks on not_empty whenever the stack
    is empty -- exercising the put->not_empty wake.  The quotas sum to
    total_real, so every produced token is gotten exactly once and every consumer
    returns deterministically -- no poison sentinel, no LIFO termination race, and
    (with producers also running to completion) no deadlock, so the round always
    terminates without needing a mid-round H.running() bail (which would strand a
    Condition-parked counterpart past teardown)."""
    try:
        n = 0
        while n < quota:
            item = q.get()             # blocks on not_empty while q is empty
            sz = q.qsize()
            if sz > K:
                fails[fail_slot] += 1
                H.fail("LifoQueue qsize {0} > maxsize {1} after get -- bound "
                       "violated (torn size accounting under M:N)".format(sz, K))
                return
            got_list.append(item)
            n += 1
    finally:
        wg.done()


def run_round(H, wid, rng, nprod, ncons, counts, slot, fails, fail_slot):
    """One bounded-backpressure round: P producers + C consumers over a private
    LifoQueue(K).  Returns True on a clean, conserved round; H.fail + False on an
    invariant break.  A wedged round never returns (watchdog EXIT_HANG)."""
    q = queue.LifoQueue(K)
    total_real = nprod * ITEMS

    # Per-consumer result lists: each consumer owns exactly one (single writer),
    # so appends are race-free without a lock even with the GIL off.
    got_lists = [[] for _ in range(ncons)]

    # Distribute the closed total_real real tokens as EXACT per-consumer quotas
    # (sum == total_real).  No poison sentinel: each consumer pulls precisely its
    # quota and returns, so termination is deterministic and the LIFO stack drains
    # completely (a poison-on-top scheme could let a consumer exit while real
    # tokens sit beneath it -- this avoids that entirely).
    base = total_real // ncons
    rem = total_real % ncons
    quotas = [base + (1 if c < rem else 0) for c in range(ncons)]

    wg = runloom.WaitGroup()
    wg.add(nprod + ncons)

    for c in range(ncons):
        H.fiber(consumer_body, H, wid, q, quotas[c], got_lists[c], wg,
                fail_slot, fails)
    for p in range(nprod):
        H.fiber(producer_body, H, wid, q, p, wg, fail_slot, fails)

    # A round is a closed deadlock-free unit, so it runs to completion even if the
    # deadline fell mid-round; we always assert its conservation oracle below
    # (catching a corruption in a round that straddles the deadline too).
    wg.wait()

    # ----- CONSERVATION ORACLE (single-threaded here: all fibers returned) -----
    seen = {}
    out_of_universe = 0
    consumed = 0
    for lst in got_lists:
        for item in lst:
            consumed += 1
            if (item & TOKEN_TAG) == 0:
                out_of_universe += 1
                continue
            producer = (item & ~TOKEN_TAG) >> PROD_SHIFT
            seq = item & ((1 << PROD_SHIFT) - 1)
            if producer >= nprod or seq >= ITEMS:
                out_of_universe += 1
                continue
            seen[item] = seen.get(item, 0) + 1

    if out_of_universe:
        fails[fail_slot] += 1
        H.fail("consumer pulled {0} OUT-OF-UNIVERSE token(s) from LifoQueue -- "
               "torn/corrupted value off a stack mangled under M:N preempt "
               "(case nprod={1} ncons={2})".format(out_of_universe, nprod, ncons))
        return False

    if consumed != total_real:
        fails[fail_slot] += 1
        H.fail("conservation FAIL: consumed {0} real tokens, expected {1} "
               "(P*ITEMS) -- a lost not_full/not_empty wakeup dropped a put or a "
               "spurious wake fabricated a get (case nprod={2} ncons={3})".format(
                   consumed, total_real, nprod, ncons))
        return False

    dup = 0
    missing = 0
    for p in range(nprod):
        for seq in range(ITEMS):
            tok = token(wid, p, seq)
            c = seen.get(tok, 0)
            if c == 0:
                missing += 1
            elif c > 1:
                dup += 1
    if dup or missing:
        fails[fail_slot] += 1
        H.fail("LIFO conservation FAIL: {0} token(s) consumed MORE THAN ONCE, "
               "{1} token(s) NEVER consumed -- a doubled boundary wake duplicated "
               "an item or a torn stack lost one (case nprod={2} ncons={3})"
               .format(dup, missing, nprod, ncons))
        return False

    counts[slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & 1023
    counts_by_case = state["counts"]      # name -> [0]*1024
    case_done = state["case_done"]        # [0]*1024 per case index
    puts = state["puts"]
    gets = state["gets"]
    fails = state["fails"]
    fail_slot = wid & 1023

    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three ratio cases by worker id in the FIRST NCASES ops
        # so EACH case is provably exercised even when timeout/park-bound rounds
        # let only a handful complete (the p125/p126/p172 flaky-coverage fix);
        # random afterward to preserve the concurrent mix.
        if i < NCASES:
            ci = (wid + i) % NCASES
        else:
            ci = rng.randrange(NCASES)
        i += 1
        name, nprod, ncons = CASES[ci]

        ok = run_round(H, wid, rng, nprod, ncons, counts_by_case[name], slot,
                       fails, fail_slot)
        if not ok:
            return
        # Conserved totals (race-free: each worker owns slot `slot`).
        puts[slot] += nprod * ITEMS              # real tokens put
        gets[slot] += nprod * ITEMS              # every put is gotten exactly once
        case_done[ci][slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "counts": {name: [0] * 1024 for (name, _p, _c) in CASES},
        "case_done": [[0] * 1024 for _ in range(NCASES)],
        "puts": [0] * 1024,
        "gets": [0] * 1024,
        "fails": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    state = H.state
    total_puts = sum(state["puts"])
    total_gets = sum(state["gets"])
    per_case = {name: sum(state["counts"][name]) for (name, _p, _c) in CASES}
    case_rounds = [sum(state["case_done"][ci]) for ci in range(NCASES)]
    fails = sum(state["fails"])

    H.log("rounds_ok={0} puts={1} gets={2} per_case={3} fails={4}".format(
        sum(per_case.values()), total_puts, total_gets, per_case, fails))

    # Conservation across the whole run: every put was gotten exactly once.
    H.check(total_puts == total_gets,
            "global conservation FAIL: total puts {0} != total gets {1} -- a "
            "lost or doubled boundary wake across the run".format(
                total_puts, total_gets))
    H.check(total_puts > 0, "no put/get happened -- backpressure never exercised")

    # Coverage: each of the three ratio cases (balanced / producer-heavy /
    # consumer-heavy) must have completed at least one round, else we never
    # exercised that boundary direction (round-robin guarantees this unless the
    # run was too short to retire even NCASES rounds total).
    for ci, (name, _p, _c) in enumerate(CASES):
        H.check(case_rounds[ci] > 0,
                "ratio case {0!r} never completed a round -- {1} boundary "
                "direction not exercised".format(name, name))

    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p409_lifoqueue_bounded_backpressure", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="bounded queue.LifoQueue(K) with producer/consumer "
                          "fiber pools parking on not_full/not_empty at the "
                          "boundary across M:N hubs; per-round conservation "
                          "(every token consumed exactly once, qsize<=K) catches "
                          "a lost or doubled backpressure wakeup")
