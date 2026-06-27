"""big_100 / 316 -- Condition.wait_for predicate correctness under M:N.

p47_condition_storm checks only the coarse `produced == consumed + leftover`
bookkeeping -- it cannot catch a notify delivered to the WRONG waiter, nor a
wait_for that returns EARLY with its predicate still false.  This program uses
the strongest Condition oracle there is: a per-waiter PREDICATE keyed on a
monotonic shared `level`.

Topology (single Condition, closed-world):
  * One `Condition` guards a single monotonic integer `level[0]`.
  * N consumers, each with a UNIQUE threshold in 1..N, call
        cond.wait_for(lambda: level[0] >= my_threshold)
    Every distinct threshold is held by exactly one consumer.
  * A small fixed set of producers loop: under the lock they raise `level` by a
    random step and then randomly `notify(k)` or `notify_all()`.  They drive
    `level` monotonically up to >= N and then flush with repeated notify_all().

The two faults this hunts, neither visible to p47:
  (a) MIS-TARGETED WAKE -- a notify pops a waiter whose predicate is still false
      while a genuinely-satisfiable waiter stays parked.  CoCondition.wait_for
      RE-checks the predicate after each wake, so the woken false-predicate
      waiter must re-park; if it instead RETURNS, that is a spurious wake passed
      off as real.
  (b) LOST SIGNAL -- a notify consumed by an already-timed-out-but-not-yet-
      removed parker (the timed wait path removes itself from `_waiters` only
      after re-acquiring the lock), stranding a satisfiable waiter asleep with
      work (a higher level) already available.

Oracles:
  * PREDICATE (fail-fast): on EVERY wait_for return, level[0] >= my_threshold.
    A strictly-false return is only possible if wait_for itself is buggy (it
    re-checks), so this is a direct detector of an early/false-predicate return.
  * COVERAGE + CONSERVATION (post, the primary biter): the producers stop only
    after level reached >= N, so EVERY consumer's threshold (1..N) is <= the
    final level -> EVERY consumer is eligible and MUST have returned.  We assert
    returned == eligible == N, and require_no_lost catches a waiter parked-then-
    vanished by a lost/mis-targeted notify (the watchdog catches the hang if one
    is stranded outright).

Some consumers use a generous timeout on wait_for to exercise the timed-park
removal-from-_waiters path (the (b) lost-signal surface) without ever legitimately
timing out at the design tier: the timeout is far larger than the producers need
to reach N, so a timed return that did NOT satisfy the predicate is itself a bug
(checked the same way), and the timed and untimed waiters share one deque so a
timed parker that lingers can steal an untimed waiter's notify.

Stresses: Condition.wait_for per-waiter predicate, notify(k)/notify_all under
M:N, timed-vs-untimed parker coexistence in one _waiters deque, lost/mis-targeted
wake, no early false-predicate return.

Good TSan / controlled-M:N-replay target: the _waiters deque mutation (popleft /
notify_all swap / timed-out self-removal) races the cross-hub park/unpark, so a
data-race report on the deque is often the first signal before the coverage
oracle even fires.
"""
import random

import harness
import runloom

# A single Condition + notify_all() is intrinsically O(waiters): each notify_all
# wakes every parked consumer for one level bump (a thundering herd).  Cap the
# consumer count so the wait_for/notify storm is genuinely exercised AND the
# producers can drive level up to N and flush every waiter (the coverage check).
# The 1M survival tier does not apply to a single-condition test (p47 warns the
# same); forcing it just wedges on the herd.
MAX_CONSUMERS = 2000

NPRODUCERS = 6          # a few producers raising the shared level + notifying
TIMED_FRACTION = 0.5    # half the consumers use a (generous) timed wait_for


def consumer(H, wid, rng, state):
    """Wait until level >= my UNIQUE threshold, then assert the predicate held.

    threshold = wid + 1, so consumers cover 1..N exactly with no duplicates.
    Owns its own random.Random (sharing one across goroutines GIL-off corrupts
    the Mersenne state)."""
    cond = state["cond"]
    level = state["level"]
    threshold = wid + 1                 # unique per consumer; covers 1..N
    timed = (wid % 2 == 0)              # ~half timed, ~half untimed -> mixed deque
    # A timeout far larger than the producers need to reach N: a real timeout is
    # itself a fault here (the predicate could not be satisfied within an
    # enormous window), surfaced by the same predicate check below.
    to = state["timeout"] if timed else None

    def pred(level=level, threshold=threshold):
        return level[0] >= threshold

    with cond:
        ok = cond.wait_for(pred, to)
        # PREDICATE oracle: wait_for re-checks, so a return -- timed or untimed --
        # MUST observe level >= threshold.  A false return is an early/false-
        # predicate wake delivered as real, or a bogus timeout return.
        lv = level[0]
        if lv < threshold:
            H.fail("false-predicate wait_for return: level={0} < threshold={1} "
                   "(consumer {2}, timed={3}, wait_for returned {4!r})".format(
                       lv, threshold, wid, timed, ok))
            return
        # Record this eligible consumer as returned (single-writer slot).
        state["returned"][wid & 1023] += 1
        H.op(wid)
    H.task_done(wid)


def producer(H, pid, rng, state):
    """Raise the shared level under the lock and notify, until level >= N; then
    flush with repeated notify_all() so no satisfiable waiter is stranded.

    Owns its own random.Random."""
    cond = state["cond"]
    level = state["level"]
    n = state["nconsumers"]
    bumps = 0
    # Phase 1: drive level monotonically up to >= N, notifying as we go.
    while H.running():
        with cond:
            if level[0] >= n:
                break
            step = rng.randint(1, 3)
            level[0] = min(n, level[0] + step)
            bumps += 1
            # Mix targeted and broadcast wakes -- the (a) mis-target surface.
            if rng.random() < 0.5:
                cond.notify(rng.randint(1, 4))
            else:
                cond.notify_all()
        H.op(pid)
        if rng.random() < 0.1:
            runloom.yield_now()
    # Phase 2: flush.  level is already >= N, so EVERY consumer's predicate is
    # now true; keep waking until they have all drained (or the run ends).  This
    # is the teardown that turns a lurking lost-wake into a detectable shortfall
    # rather than a benign "didn't get a final notify".
    n_returned = lambda: sum(state["returned"])
    for _ in range(400):
        if n_returned() >= n:
            break
        with cond:
            level[0] = max(level[0], n)
            cond.notify_all()
        runloom.sleep(0.005)
    state["bumps"][pid & 1023] += bumps


def worker(H, wid, rng, state):
    # The first NPRODUCERS workers are producers; the rest are consumers.  The
    # producer/consumer split is fixed so consumer thresholds are dense 1..N.
    if wid < state["nproducers"]:
        producer(H, wid, rng, state)
    else:
        consumer(H, wid - state["nproducers"], rng, state)


def setup(H):
    nproducers = NPRODUCERS
    nconsumers = min(MAX_CONSUMERS, max(1, H.funcs - nproducers))
    H.state = {
        "cond": runloom.sync.Condition(),
        "level": [0],
        "nproducers": nproducers,
        "nconsumers": nconsumers,
        "returned": [0] * 1024,
        "bumps": [0] * 1024,
        # Generous: dwarfs the time the producers need to reach level N, so a
        # genuine timeout never legitimately fires at the design tier.
        "timeout": 30.0,
    }


def body(H):
    npool = H.state["nproducers"] + H.state["nconsumers"]
    H.run_pool(npool, worker, H.state, max_concurrent=npool)


def post(H):
    n = H.state["nconsumers"]
    returned = sum(H.state["returned"])
    bumps = sum(H.state["bumps"])
    final_level = H.state["level"][0]
    H.log("consumers(eligible)={0} returned={1} final_level={2} bumps={3}".format(
        n, returned, final_level, bumps))
    # Producers drove level to >= N before stopping, so every consumer (threshold
    # 1..N) was eligible.
    H.check(final_level >= n,
            "producers stopped below N: final_level={0} < nconsumers={1} "
            "(coverage premise broken)".format(final_level, n))
    # COVERAGE: every eligible consumer MUST have returned (predicate satisfiable
    # and satisfied).  A shortfall = a satisfiable waiter left asleep by a lost or
    # mis-targeted notify.
    H.check(returned == n,
            "coverage broken: {0}/{1} eligible consumers returned (a satisfiable "
            "waiter was stranded -- lost or mis-targeted notify)".format(
                returned, n))
    # CONSERVATION/completeness: no worker parked-then-vanished.
    H.require_no_lost("condition wait_for coverage")


if __name__ == "__main__":
    harness.main("p316_condition_predicate", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="Condition.wait_for per-waiter predicate (unique "
                          "threshold 1..N); every return sees level>=threshold "
                          "and every eligible waiter returns (no lost/mis-"
                          "targeted notify)")
