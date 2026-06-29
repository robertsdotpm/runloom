"""big_100 / 431 -- threading.Timer Event-gated cancel-vs-fire conservation.

The subject is the cooperative ``threading.Timer`` object (under monkey.patch()
every Timer is a fiber whose finished-Event is the cooperative CoEvent).  Timer
is a Thread subclass whose ENTIRE fire decision is one CHECK-then-ACT against a
single threading.Event flag (CPython Lib/threading.py)::

    def cancel(self):
        self.finished.set()                 # (A) Event._flag write + _unpark_all

    def run(self):
        self.finished.wait(self.interval)   # (B) returns on timeout OR on set()
        if not self.finished.is_set():      # (C) Event._flag READ
            self.function(*self.args, **self.kwargs)   # (D) the fire
        self.finished.set()

The hazard is the cross-hub race between cancel()'s flag write (A) and the timer
fiber's wait()-returns-then-is_set()-check (B->C->D).  This is NOT the scheduler
timer-HEAP (p119 After()/select boundary, p120 heap churn, p219 NewTimer
arm/Stop, p213/p213 heap conservation all test the runloom timer OBJECT and its
sift-down) -- it is a DIFFERENT mechanism: a threading.Event _flag race, the
canonical check-then-act, on the threading.Timer OBJECT, which nothing in the
suite drives.  Two mutually-exclusive corruption modes, BOTH made falsifiable:

  * OVER-FIRE -- cancel() (A) sets the flag, but the timer fiber's is_set() read
    (C) does not OBSERVE that set (a stale read on a weak-mem hub, or wait() (B)
    already returned on the interval timeout and the flag write lands in the
    torn window between B and C): the timer FIRES anyway -> a cancelled timer
    that ALSO ran its function.  fired==1 AND cancel_won for the SAME timer.
  * UNDER-FIRE -- the symmetric loss: a non-cancelled timer's wait() returns on
    timeout but a phantom/torn flag makes is_set() (C) read True, so (D) is
    skipped -> a timer that should have fired never does.  Caught by the CONTROL
    arm (a private timer with no canceller MUST reach fired==1).

CLOSED-WORLD CONSERVATION (exactly-once-XOR-cancelled).  Each round a worker
fiber arms a Timer whose function is a SINGLE-WRITER per-timer cell ``fired``:
the timer fiber alone increments it, so it is race-free by construction and
reads 0 (didn't fire), 1 (fired once), or >=2 (a TORN DOUBLE-FIRE from a re-armed
/ duplicated run() -- itself a hard fault).  A SEPARATE canceller fiber on
another hub races cancel() against that timer.  After BOTH the timer and the
canceller join (the Timer fiber is .join()'d so run() has fully returned and the
cell is provably quiescent), the outcome cell is read ONCE and classified:

    fired == 0  -> CANCEL WON (the function was correctly suppressed)
    fired == 1  -> the function FIRED
    fired >= 2  -> double-fire: torn re-arm -- HARD FAULT

The per-round, deterministic round-robin by wid drives three cases:

  * CANCEL-BEFORE-FIRE (sel 0): cancel() a tiny margin after start, against a
    DELIBERATELY LONG (multi-second) interval, so the cancel CAUSALLY precedes
    the deadline regardless of M:N scheduling latency (the cancel()'s wakeup
    being delayed under load cannot push it past a 3s deadline).  The function
    MUST be suppressed: fired==0 (cancel_won).  A fired==1 here is a straight
    OVER-FIRE -- the Event gate let a cancelled timer run.  cancel() sets the
    Event, which wakes the timer's wait() at once, so join() returns promptly
    (the long interval never makes the round slow).
  * LET-FIRE (sel 1): short interval, NO canceller.  The function MUST run:
    fired==1.  A fired==0 here is an UNDER-FIRE -- the gate suppressed an
    un-cancelled timer.
  * RACE (sel 2): cancel() fired from the sibling at ~the interval boundary, so
    (A) and (C) genuinely race across hubs.  The outcome is legally EITHER:
    fired==1 (the timer won the race -- is_set() read False before cancel's
    write) XOR fired==0 (cancel won -- is_set() read True).  Conservation: each
    RACE timer contributes EXACTLY ONE outcome -- never both (fired AND
    cancel_won -> Event gate broken), never neither (lost -> watchdog hang).

CONTROL ARM (the falsifier).  Every round ALSO arms a PRIVATE single-owner Timer
with NO canceller and a short interval, and joins it: it MUST reach fired==1.
A single-owner timer with no racing cancel() is race-free by construction, so a
miss there is the Event-gate / Timer-run() MACHINERY itself dropping a fire
(UNDER-FIRE), not contention -- this disambiguates "the Timer/Event gate is
buggy" from "M:N contention tilted the race".

Invariant (hot, fail-fast): no timer ever double-fires (cell never >= 2); a
CANCEL-BEFORE-FIRE timer's cell is 0; a LET-FIRE timer's cell is 1; a RACE timer
records exactly one of {fired, cancel_won}; every CONTROL timer fires exactly
once.
Invariant (post): sum(fired) + sum(cancel_won) == total timers armed (every
timer resolved exactly once -- none lost, none double-counted); per-timer
fired*cancel_won == 0 (no cancelled-and-fired); CONTROL fires == control timers
armed (no UNDER-FIRE in the race-free machinery); each case exercised >= 1.

Stresses: threading.Timer Event-gated cancel vs fire, Event._flag check-then-act
(wait-return -> is_set -> call) racing Event.set across hubs, exactly-once-XOR-
cancelled conservation, no over-fire / no under-fire, no torn double-fire,
private-vs-shared (control) Timer fire conservation.

Good TSan / controlled-M:N-replay target: cancel()'s Event._flag store racing
the timer fiber's is_set() load over a parked-then-resumed run() is a textbook
check-then-act; a TSan report on the CoEvent _flag, or a single over/under-fire
under replay, localizes the gate break before the conservation sum even closes.
"""
import threading

import harness
import runloom

# Slots for race-free per-worker tallies (single writer per slot, summed in
# post()).  Power of two so we can mask wid into a slot.
SLOTS = 1024

# The cancel-vs-fire CASES.  post() asserts each was exercised, so the worker
# round-robins them by id in its FIRST ops (NOT random -- pure random selection
# reliably MISSES a case at the handful-of-ops-per-worker counts a timeout-bound
# run produces, the flaky-coverage bug the suite already had to fix in
# p125/p126/p172).  Random after coverage is seeded.
CASE_CANCEL_BEFORE = 0   # cancel() with wide margin BEFORE interval -> must suppress
CASE_LET_FIRE = 1        # no canceller, short interval -> must fire
CASE_RACE = 2            # cancel() at ~interval boundary -> exactly one outcome
NCASES = 3

# Timer interval for the firing path (LET-FIRE and the RACE fire-branch).  Short
# enough that those cases resolve quickly so many rounds complete under the
# timeout; the RACE boundary is drawn off THIS interval so cancel() and is_set()
# genuinely race.
FIRE_INTERVAL = 0.004

# CANCEL-BEFORE-FIRE uses a DELIBERATELY LONG interval so the cancel CAUSALLY
# precedes the deadline regardless of M:N scheduling latency.  This case asserts
# an EXACT outcome (fired==0 required), so it must NOT depend on a wall-clock
# race: under load a canceller fiber's cooperative wakeup is delayed (measured:
# a sleep(0.0005) wakes in a median ~1.4ms and up to ~15ms when 2000 fibers
# share 8 hubs -- the same cooperative-sleep latency p119/p125 document), which
# would push a tight margin PAST a 4ms deadline and the timer would (correctly)
# fire before cancel() ran -- a TIMING ARTIFACT, not a torn Event flag.  A
# multi-second interval leaves a margin orders of magnitude larger than any
# scheduling delay, so cancel() always wins causally and fired==0 is a true test
# of the Event gate (an over-fire HERE is a genuine missed flag).  Mirrors p125's
# 2-4s parent timeout that makes the inner deadline fire clearly first.
CANCEL_BEFORE_INTERVAL = 3.0

# CANCEL-BEFORE-FIRE cancels this long after start -- a tiny delay so the cancel
# fiber runs on another hub, but ASTRONOMICALLY inside CANCEL_BEFORE_INTERVAL, so
# the cancel deterministically precedes the (multi-second) deadline.
CANCEL_MARGIN = 0.0005

# RACE: the canceller sleeps ~the (short) FIRE_INTERVAL before cancel()ing, so
# (A) the flag write and (C) the timer fiber's is_set() read genuinely race
# across hubs.  Either side may win; conservation (exactly ONE outcome -- never
# both, never neither) must hold regardless of which side wins, so unlike the
# exact-outcome CANCEL-BEFORE case this one is IMMUNE to scheduling latency: a
# late cancel just means the timer-won branch (fired==1), still exactly one
# outcome.  This is the case that actually probes the Event _flag race.
RACE_DELAY = FIRE_INTERVAL

# CONTROL timer interval -- short, single-owner, no canceller; MUST fire once.
CONTROL_INTERVAL = 0.002


def run_one_timer(H, wid, rng, state, slot, case):
    """Arm one threading.Timer + (for the cancel cases) a sibling canceller on
    another hub, join both, and classify the single-writer outcome cell against
    the case's required result.  Returns True on success, False on a violation
    (caller stops).

    The `fired` cell is owned exclusively by THIS timer's run() fiber -- it is the
    only writer -- so it is race-free: 0 (suppressed), 1 (fired once), >= 2 (a
    torn double-fire).  We read it only AFTER joining the Timer fiber, so run()
    has fully returned and the cell is quiescent before we classify it."""
    armed = state["armed"]
    fired_tally = state["fired"]
    cancel_won_tally = state["cancel_won"]
    case_tally = state["case_hits"]

    # Single-writer outcome cell for this one timer; the timer fiber alone bumps
    # it.  A list so the closure mutates in place across the hub boundary.
    cell = [0]

    def fire_fn(cell=cell):
        # The ONLY writer of this cell.  += so a torn re-arm / duplicated run()
        # shows up as cell[0] >= 2 (a double-fire), distinct from a single fire.
        cell[0] += 1

    if case == CASE_LET_FIRE:
        timer = threading.Timer(FIRE_INTERVAL, fire_fn)
        timer.start()
        timer.join()                     # run() fully returned; cell quiescent
        armed[slot] += 1
        case_tally[case][slot] += 1
        n = cell[0]
        if n >= 2:
            H.fail("LET-FIRE timer DOUBLE-FIRED (cell={0}>=2) -- torn re-arm / "
                   "duplicated run() ran the function more than once".format(n))
            return False
        if n != 1:
            # UNDER-FIRE: an un-cancelled timer whose interval elapsed did NOT
            # call its function -- is_set() (C) read True with no cancel().
            H.fail("UNDER-FIRE: LET-FIRE timer (no canceller, interval {0}s) did "
                   "NOT fire (cell={1}, expected 1) -- the Event gate suppressed "
                   "an un-cancelled timer (a phantom finished.is_set())".format(
                       FIRE_INTERVAL, n))
            return False
        fired_tally[slot] += 1
        return True

    # CANCEL-BEFORE-FIRE and RACE both arm a timer AND a sibling canceller on
    # another hub.  The canceller is a separate fiber so cancel()'s Event.set (A)
    # runs on a DIFFERENT hub from the timer fiber's wait/is_set (B/C) -- that is
    # the cross-hub flag race we attack.  CANCEL-BEFORE uses a LONG interval (the
    # cancel must causally precede the deadline; an exact fired==0 must not hinge
    # on a wall-clock race that scheduling latency can lose); RACE uses the short
    # interval so the cancel and the deadline genuinely collide.
    race = (case == CASE_RACE)
    interval = FIRE_INTERVAL if race else CANCEL_BEFORE_INTERVAL
    timer = threading.Timer(interval, fire_fn)
    cwg = runloom.WaitGroup()
    cwg.add(1)

    def canceller(timer=timer, race=race):
        try:
            if race:
                # Cancel at ~the interval boundary so (A) the _flag write races
                # (C) the timer fiber's is_set() read.
                runloom.sleep(RACE_DELAY)
            else:
                # CANCEL-BEFORE-FIRE: cancel WELL inside the interval so the gate
                # must deterministically suppress.
                runloom.sleep(CANCEL_MARGIN)
            timer.cancel()               # (A) Event._flag set + _unpark_all
        finally:
            cwg.done()

    timer.start()
    H.fiber(canceller)
    cwg.wait()                           # the cancel() has been issued
    timer.join()                         # run() fully returned; cell quiescent

    armed[slot] += 1
    case_tally[case][slot] += 1
    n = cell[0]

    if n >= 2:
        H.fail("{0} timer DOUBLE-FIRED (cell={1}>=2) -- torn re-arm / duplicated "
               "run() ran the function more than once".format(
                   "RACE" if race else "CANCEL-BEFORE", n))
        return False

    if case == CASE_CANCEL_BEFORE:
        if n == 1:
            # OVER-FIRE: cancel() (A) set the flag a wide margin before the
            # deadline, yet the timer fired -- is_set() (C) did not observe the
            # set, the Event gate let a cancelled timer run.
            H.fail("OVER-FIRE: CANCEL-BEFORE-FIRE timer (cancel() issued {0}s into "
                   "a {1}s interval) STILL FIRED (cell=1) -- the threading.Event "
                   "gate did not suppress a cancelled timer: cancel()'s flag set "
                   "was not observed by run()'s is_set() check".format(
                       CANCEL_MARGIN, CANCEL_BEFORE_INTERVAL))
            return False
        # n == 0: cancel correctly won.
        cancel_won_tally[slot] += 1
        return True

    # CASE_RACE: exactly one outcome is legal -- fired XOR cancel_won.  n is 0
    # (cancel won the boundary race) or 1 (timer won).  Either is fine; both
    # ({fired AND cancel_won}) is impossible for a single cell (one writer), and
    # 'neither' cannot occur because we joined the timer (run() always sets the
    # flag and returns).  Record whichever happened -- conservation in post()
    # asserts fired + cancel_won == armed, so a lost outcome would show there.
    if n == 1:
        fired_tally[slot] += 1
    else:
        cancel_won_tally[slot] += 1
    return True


def run_control(H, slot, state):
    """CONTROL ARM: a PRIVATE single-owner Timer with NO canceller and a short
    interval.  It MUST fire exactly once.  A single-owner timer with no racing
    cancel() is race-free by construction, so a miss here is the Event-gate /
    Timer.run() MACHINERY dropping a fire (an UNDER-FIRE), NOT contention -- the
    falsifier that disambiguates a buggy gate from a tilted race."""
    control_ok = state["control_ok"]
    ccell = [0]

    def control_fn(ccell=ccell):
        ccell[0] += 1

    ct = threading.Timer(CONTROL_INTERVAL, control_fn)
    ct.start()
    ct.join()
    n = ccell[0]
    if n >= 2:
        H.fail("CONTROL timer DOUBLE-FIRED (cell={0}>=2) -- the race-free single-"
               "owner Timer ran its function more than once (torn run())".format(n))
        return False
    if n != 1:
        H.fail("CONTROL timer did NOT fire (cell={0}, expected 1) -- a PRIVATE "
               "single-owner Timer with no canceller failed to call its function: "
               "the Timer/Event-gate machinery itself dropped a fire (UNDER-FIRE), "
               "not contention".format(n))
        return False
    control_ok[slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases by worker id in the first ops so every case
        # is exercised even when each worker manages only a few ops under the
        # timeout (the p125/p126/p172 flaky-random-coverage fix); random after.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        if not run_one_timer(H, wid, rng, state, slot, case):
            return
        # The race-free control arm runs every round alongside the contended one.
        if not run_control(H, slot, state):
            return

        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.Timer's
    # Event is the cooperative CoEvent and each Timer runs as an M:N fiber.  The
    # per-slot tally lists are single-writer-per-slot, summed in post().
    H.state = {
        "armed": [0] * SLOTS,            # timers armed on the contended path
        "fired": [0] * SLOTS,            # contended timers whose function fired
        "cancel_won": [0] * SLOTS,       # contended timers the cancel suppressed
        "control_ok": [0] * SLOTS,       # control timers that fired exactly once
        # Per-case tally so post() can assert each case was actually exercised.
        "case_hits": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    armed = sum(H.state["armed"])
    fired = sum(H.state["fired"])
    cancel_won = sum(H.state["cancel_won"])
    control_ok = sum(H.state["control_ok"])
    cancel_before = sum(H.state["case_hits"][CASE_CANCEL_BEFORE])
    let_fire = sum(H.state["case_hits"][CASE_LET_FIRE])
    race = sum(H.state["case_hits"][CASE_RACE])
    H.log("armed={0} fired={1} cancel_won={2} control_ok={3} "
          "(cancel_before={4} let_fire={5} race={6}) ops={7}".format(
              armed, fired, cancel_won, control_ok,
              cancel_before, let_fire, race, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(armed > 0,
            "no timers armed on the contended path -- the cancel-vs-fire race "
            "window was never exercised")

    # Conservation: every contended timer resolved EXACTLY ONCE -- it either fired
    # XOR its cancel won.  fired + cancel_won must equal the number armed; a
    # SHORTFALL is a lost outcome (over/under-counted), an EXCESS is a double-count
    # (the same timer recorded as both fired and cancel_won -> the Event gate let a
    # cancelled timer fire, an OVER-FIRE).  (Per-timer fired*cancel_won==0 is
    # enforced hot: one writer per cell, classified by a single read after join.)
    H.check(fired + cancel_won == armed,
            "FIRE CONSERVATION broken: fired({0}) + cancel_won({1}) = {2} != "
            "armed({3}) -- a timer was lost (neither fired nor cancelled) or "
            "double-counted (fired AND cancelled -> the Event gate let a "
            "cancelled timer over-fire)".format(
                fired, cancel_won, fired + cancel_won, armed))

    # CONTROL conservation: every race-free single-owner control timer fired
    # exactly once.  control_ok must equal the number of rounds (one control per
    # round); a shortfall is an UNDER-FIRE in the Timer/Event machinery itself.
    H.check(control_ok == armed,
            "CONTROL conservation broken: control_ok({0}) != armed({1}) -- a "
            "PRIVATE single-owner Timer (no canceller) failed to fire exactly "
            "once: the Timer/Event-gate machinery dropped a fire, not "
            "contention".format(control_ok, armed))

    # LET-FIRE conservation: every LET-FIRE timer must have fired (it has no
    # canceller), so the number of fires is AT LEAST the LET-FIRE count.  A
    # shortfall would already have failed fast inside run_one_timer; this guards
    # the case was reached.
    H.check(fired >= let_fire,
            "fired({0}) < let_fire({1}) -- a LET-FIRE timer was suppressed "
            "(UNDER-FIRE)".format(fired, let_fire))
    # CANCEL-BEFORE conservation: every cancel-before timer must have been
    # suppressed, so cancel_won is AT LEAST the cancel_before count.
    H.check(cancel_won >= cancel_before,
            "cancel_won({0}) < cancel_before({1}) -- a CANCEL-BEFORE timer "
            "fired despite a cancel with margin (OVER-FIRE)".format(
                cancel_won, cancel_before))

    # Each case actually exercised (deterministic round-robin guarantees it once
    # any worker ran NCASES ops or NCASES workers ran one each).
    H.check(cancel_before > 0, "CANCEL-BEFORE-FIRE case never exercised")
    H.check(let_fire > 0, "LET-FIRE case never exercised")
    H.check(race > 0, "RACE case never exercised")

    H.require_no_lost("timer cancel-vs-fire conservation")


if __name__ == "__main__":
    harness.main(
        "p431_timer_cancel_vs_fire_event_gat", body, setup=setup, post=post,
        default_funcs=3000,
        describe="threading.Timer Event-gated cancel-vs-fire across hubs: a "
                 "single-writer per-timer fired cell + a sibling canceller; "
                 "exactly-once-XOR-cancelled conservation (fired+cancel_won=="
                 "armed, never both/neither, no double-fire) with a private "
                 "single-owner control Timer that must always fire -- an "
                 "over-fire (cancelled timer ran) or under-fire (un-cancelled "
                 "timer suppressed) fails")
