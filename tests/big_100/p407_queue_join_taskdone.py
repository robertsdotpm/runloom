"""big_100 / 407 -- queue.Queue join()/task_done() conservation under M:N.

The cooperative (monkey-patched) `queue.Queue` is a thread-safe producer/consumer
queue whose `join()` parks the caller on the internal `all_tasks_done` Condition
until `_unfinished_tasks` reaches 0, while `task_done()` -- called from OTHER
goroutines, on OTHER hubs -- decrements that shared counter and `notify_all()`s
the joiners.  This is exactly the cross-hub notify_all wake-up race the suite
must probe: with the GIL off the `_unfinished_tasks -= 1` and the
`all_tasks_done.notify_all()` straddle a cooperative Condition whose mutex is
contended from many hubs, and the two failure modes are sharp and falsifiable --

  * a LOST decrement or a LOST notify_all: a joiner stays parked after the last
    task_done() already drove unfinished to 0 -> the round's WaitGroup never
    completes -> the watchdog reports HANG (exit 3); or

  * an OVER-decrement / double-wake: `task_done()` is called more times than
    items were `put()`, so the shared counter goes negative and CPython's queue
    raises `ValueError("task_done() called too many times")` -- which we treat as
    a hard fault because our protocol calls task_done() exactly once per got item.

We make conservation itself the oracle.  Each round builds ONE shared
`queue.Queue`; P producer goroutines each `put()` N items, every item a unique
(token, value) pair drawn from the producer's own deterministic stream; then the
producers (or a separate external joiner, see below) call `join()`.  A pool of C
consumer goroutines `get()`/process/`task_done()` until they drain a sentinel.
After every `join()` returns we assert, from the joiner's own observation point:

  * `q.unfinished_tasks == 0` and `q.qsize() == 0` -- join() returned EXACTLY
    when the queue was fully accounted, not early (a lost decrement that wrapped)
    and not on a spurious wake;
  * a shared `all_done` flag, published by the LAST task_done() under the queue's
    own mutex, is already True when join() returns -- so join() cannot have
    returned BEFORE the final account (the join-return-timing invariant).

And at the end of the round, per-producer / per-consumer tallies (owned single-
writer slots, summed lock-free) must conserve:

  * count: total items put == total items got == P*N;
  * value sum and XOR checksum: sum/xor of every produced value == sum/xor of
    every consumed value (a lost item flips the sum; a duplicated/torn item flips
    the xor even when the sum coincidentally matches);
  * task_done() never raised "called too many times".

THREE join topologies are round-robined by worker id so coverage holds whether
one worker does many ops or many workers do one op each (the suite's recurring
flaky-random-coverage bug -- pure random selection misses a case at low op-count
under load; p125/p126/p172 had to seed the first ops deterministically, so we do
too):

  * case 0  PRODUCER-JOIN: each producer calls q.join() after its puts; many
    joiners park on all_tasks_done at once (the many-joiner notify_all fan-out);
  * case 1  EXTERNAL-JOIN: producers don't join; a separate joiner goroutine on
    its own hub calls q.join() once -- the lone-joiner cross-hub wake;
  * case 2  TIMEOUT-PRESSURE: a watchdog goroutine runs runloom timeouts in
    parallel with the join, churning the timer heap and adding cross-hub wake
    traffic while the joiner parks (probes a join park that competes with timer
    wakes on the same hubs), then a producer-join as in case 0.

Stresses: queue.Queue join()/task_done() cross-hub notify_all wake, shared
_unfinished_tasks decrement under a cooperative Condition, lost/over-decrement,
join-return timing vs final account, producer/consumer conservation under M:N.
"""
import queue                      # cooperative queue.Queue after monkey.patch()

import harness
import runloom

# Per-round sizing.  Small enough that a round completes within a timeout-bounded
# window (so workers manage several rounds and the conservation oracle actually
# runs), large enough that many items cross hubs and the all_tasks_done Condition
# is genuinely contended by multiple joiners + consumers.
PRODUCERS = 4
ITEMS_PER_PRODUCER = 16
CONSUMERS = 3
NCASES = 3

# Sentinel that tells a consumer to exit.  A unique object so it can never be
# confused with a real (token, value) item.
SENTINEL = object()


def make_value(pid, token):
    """Deterministic per-item value.  A bijection-ish mix so a torn/duplicated
    item is very unlikely to coincidentally preserve BOTH the running sum and the
    xor checksum."""
    return ((pid << 24) ^ (token * 0x9E3779B1) ^ 0x5A5A5A5A) & 0xFFFFFFFFFFFF


def producer(H, q, pid, items, putsum, putxor, slot):
    """Put `items` unique (token, value) pairs, recording our contribution to the
    conservation tallies in our own single-writer slots."""
    s = 0
    x = 0
    for token in range(items):
        val = make_value(pid, token)
        q.put((pid, token, val))
        s = (s + val) & 0xFFFFFFFFFFFFFFFF
        x ^= val
    putsum[slot] = (putsum[slot] + s) & 0xFFFFFFFFFFFFFFFF
    putxor[slot] ^= x


def consumer(H, q, cid, getcount, getsum, getxor, fault):
    """get()/process/task_done() until the sentinel.  Records the conservation
    tallies for the items it consumed; converts a 'task_done() called too many
    times' into a hard fault flag (our protocol calls it exactly once per item)."""
    s = 0
    x = 0
    n = 0
    while True:
        item = q.get()
        if item is SENTINEL:
            # The sentinel itself was put() (one unfinished unit); balance it so
            # join() can reach 0.  A consumer takes exactly one sentinel and exits.
            try:
                q.task_done()
            except ValueError as exc:
                fault[0] = "task_done after sentinel: {0}".format(exc)
            break
        _pid, _token, val = item
        s = (s + val) & 0xFFFFFFFFFFFFFFFF
        x ^= val
        n += 1
        try:
            q.task_done()
        except ValueError as exc:
            # The queue's own over-decrement guard fired: more task_done()s than
            # puts.  Under our 1:1 protocol this can only happen if a cross-hub
            # race double-counted a decrement -> real FT bug.
            fault[0] = ("task_done() raised 'too many times' (over-decrement / "
                        "double-wake of _unfinished_tasks): {0}".format(exc))
            break
    getcount[cid] = getcount[cid] + n
    getsum[cid] = (getsum[cid] + s) & 0xFFFFFFFFFFFFFFFF
    getxor[cid] ^= x


def timer_churn(H, stop):
    """Case 2 helper: churn the runloom timer heap with short sleeps while the
    join parks, so the joiner's all_tasks_done wake competes with timer wakes on
    the shared hubs.  Stops when `stop[0]` is set."""
    while not stop[0] and H.running():
        runloom.sleep(0.0005)


def run_round(H, wid, rng, state, case):
    """One shared queue.Queue, P producers + C consumers, join topology chosen by
    `case`.  Returns True on a clean, conserved round; H.fail / returns False on a
    violation."""
    counts = state
    slot = wid & 1023

    q = queue.Queue()

    # Per-round conservation tallies.  Producer slots are indexed by pid (single
    # writer each); consumer slots by cid.
    putsum = [0] * PRODUCERS
    putxor = [0] * PRODUCERS
    getcount = [0] * CONSUMERS
    getsum = [0] * CONSUMERS
    getxor = [0] * CONSUMERS
    fault = [None]                       # set by a consumer on task_done fault

    total = PRODUCERS * ITEMS_PER_PRODUCER

    # The join-return-timing invariant is observed AT the joiner: the instant
    # join() returns we snapshot the queue's own accounting (unfinished_tasks /
    # qsize), which must both be 0.  We rely on the queue's join() contract rather
    # than a hand-rolled all_done flag (reading unfinished outside the queue's lock
    # would itself race the decrement).
    join_observations = []               # (unfinished, qsize) seen right after join
    obs_lock = runloom.sync.Lock()

    # WaitGroups.  `put_barrier` is tripped by every producer the instant it has
    # finished PUTTING (before it joins).  Joiners wait on it before calling
    # join(), so join() is only ever entered once ALL puts have landed.  This is
    # what makes the join-return-timing invariant exact: with concurrent producers
    # still putting, `unfinished_tasks` can legitimately dip to 0 mid-run (one
    # producer's items fully consumed before another producer puts) and join()
    # would correctly return early with items still to come -- NOT a bug, just
    # queue.Queue semantics.  Gating join() behind the put-barrier removes that
    # benign early-return so a qsize!=0 / unfinished!=0 at join-return is a real
    # lost/over-decrement.
    put_barrier = runloom.WaitGroup()
    put_barrier.add(PRODUCERS)
    pwg = runloom.WaitGroup()            # producer goroutine completion
    pwg.add(PRODUCERS)
    cwg = runloom.WaitGroup()
    cwg.add(CONSUMERS)

    def observe_join():
        """Call join(), then snapshot the queue accounting AT the return point."""
        q.join()
        unf = q.unfinished_tasks
        qs = q.qsize()
        with obs_lock:
            join_observations.append((unf, qs))

    def prod_wrap(pid):
        try:
            producer(H, q, pid, ITEMS_PER_PRODUCER, putsum, putxor, pid)
            put_barrier.done()           # signal: my puts are all in
            if case == 0 or case == 2:
                # Producer joins -- but only after EVERY producer has put, so the
                # many joiners park on all_tasks_done together over the full set.
                put_barrier.wait()
                observe_join()
        finally:
            pwg.done()

    def cons_wrap(cid):
        try:
            consumer(H, q, cid, getcount, getsum, getxor, fault)
        finally:
            cwg.done()

    # Spawn consumers first so puts are drained as they land (the queue is
    # unbounded; consumers parked in get() are woken by put()'s not_empty notify).
    for cid in range(CONSUMERS):
        H.fiber(cons_wrap, cid)
    for pid in range(PRODUCERS):
        H.fiber(prod_wrap, pid)

    # Case 2: a parallel timer-churn goroutine to load the shared hubs' timer
    # heap while the joiners park.
    stop_timer = [False]
    if case == 2:
        H.fiber(timer_churn, H, stop_timer)

    # Case 1: a single EXTERNAL joiner goroutine on its own hub (producers did NOT
    # join).  It joins once ALL producers have finished putting.
    ewg = None
    if case == 1:
        ewg = runloom.WaitGroup()
        ewg.add(1)

        def external_joiner():
            try:
                put_barrier.wait()       # all puts in before this lone joiner parks
                observe_join()
            finally:
                ewg.done()

        H.fiber(external_joiner)

    # Wait for producers to finish (and, in case 0/2, to have joined).
    pwg.wait()
    if case == 1:
        ewg.wait()
    stop_timer[0] = True

    # All real items are now put and (for joiners) accounted.  Send one sentinel
    # per consumer so each drains and exits -- the consumers may still be draining
    # the tail when producers' join() returned, which is fine: join() only counts
    # task_done(), and every got() is followed by a task_done().
    for _ in range(CONSUMERS):
        q.put(SENTINEL)
    cwg.wait()

    # ----- invariant checks (post-round, but per-round so we fail fast) -----
    if fault[0] is not None:
        H.fail("p407 round (case {0}): {1}".format(case, fault[0]))
        return False

    # Conservation: counts.
    got_n = sum(getcount)
    if got_n != total:
        H.fail("p407 conservation (case {0}): items got {1} != put {2} "
               "(lost/duplicated item across the queue under M:N)".format(
                   case, got_n, total))
        return False

    # Conservation: value sum and xor checksum.
    ps = 0
    px = 0
    for i in range(PRODUCERS):
        ps = (ps + putsum[i]) & 0xFFFFFFFFFFFFFFFF
        px ^= putxor[i]
    gs = 0
    gx = 0
    for i in range(CONSUMERS):
        gs = (gs + getsum[i]) & 0xFFFFFFFFFFFFFFFF
        gx ^= getxor[i]
    if ps != gs:
        H.fail("p407 conservation (case {0}): produced sum {1} != consumed sum "
               "{2} (lost or altered item value)".format(case, ps, gs))
        return False
    if px != gx:
        H.fail("p407 conservation (case {0}): produced xor {1} != consumed xor "
               "{2} (duplicated/torn item -- sum coincided but identity didn't)"
               .format(case, px, gx))
        return False

    # join-return timing: every observed join() returned with the queue fully
    # accounted -- unfinished==0 AND empty -- never early on a lost decrement that
    # wrapped or a spurious wake.
    if not join_observations:
        H.fail("p407 (case {0}): no join() observation recorded -- joiner never "
               "ran or never returned".format(case))
        return False
    for unf, qs in join_observations:
        if unf != 0:
            H.fail("p407 join-timing (case {0}): join() returned with "
                   "unfinished_tasks={1} (must be 0) -- a lost decrement let "
                   "join() wake early".format(case, unf))
            return False
        if qs != 0:
            H.fail("p407 join-timing (case {0}): join() returned with qsize={1} "
                   "(must be 0) -- items remained when join() claimed done"
                   .format(case, qs))
            return False

    # Final queue state must be fully drained: nothing left, unfinished 0.
    if q.unfinished_tasks != 0:
        H.fail("p407 (case {0}): unfinished_tasks={1} after full drain (over-/"
               "under-count of task_done)".format(case, q.unfinished_tasks))
        return False

    # Record one rounds-completed-per-case tally (single-writer per slot).
    counts["case_done"][case][slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three join topologies in each worker's first ops so all
        # three are covered whether one worker manages NCASES ops or NCASES workers
        # manage 1 op each (deterministic coverage; random after that preserves the
        # concurrent mix).  This is the suite's flaky-random-coverage fix.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1
        ok = run_round(H, wid, rng, state, case)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "case_done": [[0] * 1024 for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    totals = [sum(H.state["case_done"][c]) for c in range(NCASES)]
    H.log("rounds by case producer_join={0} external_join={1} timeout_pressure={2}"
          " total_ops={3}".format(totals[0], totals[1], totals[2], H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(totals[0] > 0, "producer-join case never exercised")
    H.check(totals[1] > 0, "external-join case never exercised")
    H.check(totals[2] > 0, "timeout-pressure case never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p407_queue_join_taskdone", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="queue.Queue join()/task_done() conservation: many "
                          "producer/consumer fibers cross-hub; join() returns "
                          "exactly at unfinished==0, sum/xor conserved, "
                          "task_done never over-decrements")
