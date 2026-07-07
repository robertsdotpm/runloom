"""big_100 / 518 -- sched.scheduler fire-order + fire-once under M:N.

sched.scheduler keeps its pending events in an internal binary heap
(self._queue, sifted by heapq) guarded by a re-entrant self._lock (RLock).
scheduler.run() loops: under the lock it PEEKS the min event q[0] (the global
minimum by the tuple (time, priority, sequence)); if the event's time is still
in the future it calls delayfunc(time-now) and LEAVES the event on the heap;
otherwise it heappops the event and calls its action.  After every action it
calls delayfunc(0) "to let other threads run".

WHERE M:N COULD BREAK IT (the gap this program probes).  In a big_100 run the
scheduler's run() loop is being driven by a runloom GOROUTINE, and delayfunc
parks that goroutine (yield_now) -- once per pending event (the positive-delay
branch) and once after every fired action (the delay-of-0 hack).  So the fiber
is repeatedly PARKED and RESUMED while a partially-drained heap sits in q, the
RLock is released/re-acquired around each park, and tens of thousands of sibling
goroutines on hubs>1 are churning.  If runloom mis-resumes the fiber (a lost
wakeup that strands run() mid-drain, a duplicated wake that re-enters the loop,
or a torn resume that corrupts the fiber's own C stack / the live q[0] tuple it
just unpacked), the observable symptom is a scheduler that fires an event OUT OF
(time, priority) ORDER, fires one event TWICE, or DROPS an event entirely.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  Each goroutine OWNS its scheduler.  The scheduler, its heap, its RLock, its
  fake clock, and its fire-log list are all created inside the worker fiber and
  NEVER shared with a sibling -- this is a strict single-owner object, exactly
  like p490's per-fiber enum class.  The clock is a fiber-local fake: timefunc
  returns a counter the fiber controls, and delayfunc ADVANCES that counter and
  yields (no real sleep -- CPU-only).  Because the heap pops the global minimum,
  a single-owner scheduler MUST fire its events in EXACT nondecreasing
  (time, priority) order, and -- since heappop removes each event -- MUST fire
  each event EXACTLY ONCE.  This is a documented, deterministic guarantee of
  sched + heapq on a correctly single-threaded driver; we verified the same law
  holds under plain OS threads (each thread its own scheduler, GIL on AND off):
  0 mis-orders, 0 doubles, 0 drops.  Under a CORRECT runloom it must also hold,
  so the single-owner oracle PASSES (program exits 0) when there is no bug.

  The events are given DISTINCT (time, priority) pairs, so the sorted order is a
  UNIQUE total order and "nondecreasing" is really strictly increasing -- any
  adjacent-pair inversion in the fire-log is an unambiguous fault.  Each event
  carries a unique sentinel id; the fire-log is a fiber-local list appended by
  the action.  After run() drains the heap we assert, on the now-quiescent
  single-owner log:

    * COUNT / CONSERVATION: len(fire_log) == N events enqueued (no event fired
      twice, none dropped -- heappop guarantees once on a correct driver);
    * COVERAGE: the set of fired ids == the set of enqueued ids (nothing extra,
      nothing missing);
    * ORDER: the (time, priority) key of fire k is strictly greater than that of
      fire k-1 for every k (exact heap order preserved across every park/resume).

  A violation of ANY of the three, on a single-owner scheduler that no sibling
  touched, cannot be documented Python semantics (sched/heapq are correct on one
  thread) -- it can only be a runloom park/resume defect (lost/dup/torn wake).

ORACLES:
  * LOAD-BEARING (worker, HARD, fail-fast): single-owner sched.scheduler drains
    N distinct-(time,priority) events in exact heap order, each exactly once,
    across N+ park points inside delayfunc.  H.fail on any inversion, double,
    drop, or missing/extra id.
  * NON-VACUITY (post, HARD): the load-bearing arm actually fired events
    (fires > 0) -- else the ordering law was never exercised.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-drain
    (parked inside delayfunc and never resumed) never returns; the watchdog +
    require_no_lost catch it.

FAIL ON: a single-owner scheduler firing events out of (time, priority) order,
firing an event more than once, dropping an event, or firing an unknown id -- a
lost/duplicated/torn runloom wakeup around the delayfunc park points.  There is
NO shared-scheduler arm: a shared sched.scheduler mutated by several fibers
races exactly like it does across OS threads (documented -- sched's lock only
guards its own heap ops, not a cross-call ordering contract), so it would be a
false-positive generator and is deliberately excluded.

Stresses: heapq siftup/siftdown on a partially-drained heap held live across a
goroutine park; RLock release/re-acquire around each delayfunc yield; C-level
run() loop state (the unpacked q[0] tuple, the localized pop/lock/timefunc refs)
surviving repeated park/resume; fire-order + fire-once conservation under
sustained M:N churn on hubs>1.
"""
import sched

import harness
import runloom

# Events per single-owner scheduler per round.  Each event forces at least one
# delayfunc park (its positive-delay peek) plus one delay-of-0 park after it
# fires, so a round parks the driving fiber ~2*N times while a partially-drained
# heap is live.  Small enough that many rounds complete under the timeout; large
# enough that the heap goes through several siftup/siftdown levels between parks.
NEVENTS = 24

# Time band for events.  Times are drawn from a range SMALLER than NEVENTS so
# multiple events collide on the same `time` and the (time, priority) tiebreak
# through heapq's tuple compare is genuinely exercised (not just the time field).
TIME_SPAN = 12

# Per-worker inner iterations bound (each iteration = one full scheduler drain).
# The fire-order hazard only shows under SUSTAINED park/resume churn, so each
# worker drains many schedulers back-to-back while H.running().
INNER_CAP = 100000


class FakeClock(object):
    """Fiber-local fake clock: single-owner, never shared.

    timefunc() returns the current tick; delayfunc(d) advances the tick by d and
    PARKS the driving fiber (yield_now -- no real sleep, CPU-only).  Because the
    scheduler advances the clock only through delayfunc, the tick is monotone
    nondecreasing and every pending event eventually becomes due.  Each delayfunc
    call is a park point where a runloom mis-resume would corrupt the drain."""

    def __init__(self):
        self.tick = 0

    def time(self):
        return self.tick

    def delay(self, d):
        # Advance our own clock so the peeked event becomes due, then park so a
        # sibling on another hub reliably interleaves before we resume the drain.
        if d > 0:
            self.tick += d
        runloom.yield_now()


def build_events(rng):
    """Build N events with DISTINCT (time, priority) pairs and unique ids.

    Returns (events, expected_order) where events is a list of
    (time, priority, eid) in ENQUEUE order (shuffled), and expected_order is the
    list of eids in the UNIQUE sorted (time, priority) order the scheduler MUST
    fire them in.  Times are drawn from a narrow band so several events share a
    time and the priority tiebreak is exercised; (time, priority) is kept
    globally distinct so the sorted order is a total order with no ambiguity."""
    seen = set()
    events = []
    eid = 0
    while len(events) < NEVENTS:
        t = rng.randrange(1, TIME_SPAN + 1)         # >0 so the first peek delays
        p = rng.randrange(0, 1 << 30)
        if (t, p) in seen:
            continue                                 # keep (time, priority) distinct
        seen.add((t, p))
        events.append((t, p, eid))
        eid += 1
    # Unique total order by (time, priority): this is exactly the order heapq
    # will pop, hence the order the scheduler MUST fire.
    expected_order = [e[2] for e in sorted(events, key=lambda e: (e[0], e[1]))]
    # Enqueue order is shuffled so the heap actually has to sift into place; the
    # fire order must still come out sorted.
    rng.shuffle(events)
    return events, expected_order


def drain_one(H, wid, rng):
    """Build a single-owner scheduler, enqueue N distinct-(time,priority) events,
    drain it in a fiber that parks ~2*N times inside delayfunc, and verify the
    fire-log is in EXACT heap order, each event fired EXACTLY ONCE.

    Returns the number of events fired (for the non-vacuity tally), or -1 if a
    load-bearing invariant failed (caller returns immediately)."""
    events, expected_order = build_events(rng)
    clock = FakeClock()

    # fire_log is a FIBER-LOCAL list: the only writer is this fiber's own event
    # actions, running one at a time on this fiber.  Single-owner -> race-free.
    fire_log = []

    def make_action(time, priority, eid):
        def action():
            fire_log.append((time, priority, eid))
        return action

    scheduler = sched.scheduler(timefunc=clock.time, delayfunc=clock.delay)
    for (t, p, eid) in events:
        scheduler.enterabs(t, p, make_action(t, p, eid))

    # Drain: this parks the fiber once per positive-delay peek and once (delay 0)
    # after every fired action -- ~2*N park/resume cycles over a live heap.
    scheduler.run(blocking=True)

    # ---- LOAD-BEARING oracle on the now-quiescent single-owner fire-log -------
    n = len(fire_log)

    # CONSERVATION: heappop fires each event exactly once, so the count must be
    # exactly N.  Fewer -> an event was dropped (a strand that lost the event);
    # more -> an event fired twice (a duplicated wake re-entered the drain).
    if n != NEVENTS:
        H.fail("sched fire COUNT wrong: {0} events fired but {1} were enqueued "
               "(wid {2}) -- a single-owner scheduler must fire each event "
               "exactly once; {3} across the delayfunc park points".format(
                   n, NEVENTS, wid,
                   "an event was DROPPED (lost wakeup stranded the drain)"
                   if n < NEVENTS else
                   "an event fired MORE THAN ONCE (duplicated wakeup)"))
        return -1

    # COVERAGE: exactly the enqueued id set, nothing extra, nothing missing.
    fired_ids = [rec[2] for rec in fire_log]
    if set(fired_ids) != set(expected_order):
        missing = set(expected_order) - set(fired_ids)
        extra = set(fired_ids) - set(expected_order)
        H.fail("sched fire COVERAGE wrong (wid {0}): missing ids {1}, unknown "
               "ids {2} -- the single-owner scheduler fired an event set that "
               "differs from what was enqueued (dropped and/or torn event on a "
               "park/resume)".format(wid, sorted(missing), sorted(extra)))
        return -1

    # ORDER: fire k's (time, priority) key must be STRICTLY greater than fire
    # k-1's (pairs are distinct, so heap order is strictly increasing).  Any
    # inversion is an out-of-order fire across a park.
    for k in range(1, n):
        prev = (fire_log[k - 1][0], fire_log[k - 1][1])
        cur = (fire_log[k][0], fire_log[k][1])
        if not (cur > prev):
            H.fail("sched fire ORDER inverted (wid {0}) at position {1}: event "
                   "id {2} key {3} fired AFTER id {4} key {5} but should sort "
                   "BEFORE it -- a single-owner sched.scheduler fired out of "
                   "(time, priority) heap order across a delayfunc park "
                   "(lost/dup/torn runloom wakeup)".format(
                       wid, k, fire_log[k][2], cur, fire_log[k - 1][2], prev))
            return -1

    # Belt-and-braces: the exact id sequence must equal the unique sorted order.
    if fired_ids != expected_order:
        H.fail("sched fire SEQUENCE mismatch (wid {0}): fired {1} but the unique "
               "(time, priority) sorted order is {2} -- heap order not preserved "
               "across the park points".format(wid, fired_ids, expected_order))
        return -1

    return n


def worker(H, wid, rng, state):
    """Each fiber repeatedly builds and drains its OWN single-owner scheduler,
    parking ~2*N times per drain inside delayfunc so siblings on other hubs
    reliably interleave while a partially-drained heap sits live."""
    fired = state["fired"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            n = drain_one(H, wid, rng)
            if H.failed:
                return
            fired[wid] += n                 # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # fired[]: one slot per worker (wid-indexed, single-writer -> race-free), the
    # conservation tally feeding the non-vacuity check.  Allocated here where
    # H.funcs is known, per HARD RULE 1.
    H.state = {
        "fired": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    fired = sum(H.state["fired"])
    H.log("sched single-owner fire-order: {0} events drained in exact "
          "(time, priority) heap order, each fired exactly once (every "
          "per-drain order+conservation check passed fail-fast); ops={1}".format(
              fired, H.total_ops()))
    # NON-VACUITY: the load-bearing ordering law was actually exercised.
    H.check(fired > 0,
            "no sched events fired -- the single-owner fire-order hazard was "
            "never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (stranded inside delayfunc
    # mid-drain).
    H.require_no_lost("sched scheduler fire-order")


if __name__ == "__main__":
    harness.main(
        "p518_sched_scheduler_fire_order", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each goroutine owns a sched.scheduler with a fiber-local fake "
                 "clock (timefunc counter, delayfunc advances it + yields, no "
                 "real sleep); it enqueues N events with DISTINCT (time, "
                 "priority) and drains them, parking ~2*N times inside delayfunc "
                 "over a live partially-drained heap.  LOAD-BEARING single-owner "
                 "oracle: the fire-log must be in exact nondecreasing "
                 "(time, priority) order with each event fired exactly once -- an "
                 "out-of-order fire, a double, or a drop is a lost/dup/torn "
                 "runloom wakeup around the delayfunc park points.  No shared-"
                 "scheduler arm (that races like plain threads -- documented)")
