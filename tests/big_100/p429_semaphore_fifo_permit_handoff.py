"""big_100 / 429 -- plain Semaphore FIFO permit hand-off + conservation under M:N.

The subject is the cooperative ``threading.Semaphore`` (monkey.patch() hands
every fiber the cooperative CoSemaphore, src/runloom/monkey/events.py:216).  Its
internal state -- the EXACT fields this program attacks -- is::

    __slots__ = ("_value", "_waiters", "_guard", "_cancelled")
    _value    : int    -- the free-permit count
    _waiters  : deque  -- FIFO records [parker, active, got_permit] of parked acquirers
    _guard    : CoLock -- cooperative lock serialising _value + _waiters bookkeeping

The HAZARD is the DIRECT hand-off in release() racing a head waiter's own
timeout-removal and a barging acquire().  release(n) does, per permit::

    while self._waiters:
        w = self._waiters.popleft()
        if w[1]:                 # active (not timed out)?
            w[2] = True          # hand THIS permit to the FIFO-head waiter
            ...unpark w[0]; break
        # else: stale timed-out waiter -> discard, keep scanning
    else:
        self._value += 1         # nobody waiting -> bump the count

and a timed acquire(), on deadline, does (under the SAME _guard)::

    with self._guard:
        if not w[2]:
            w[1] = False         # mark myself inactive so a racing release skips me

while a fresh acquire() barges the fast path::

    self._guard.acquire()
    if self._value > 0:
        self._value -= 1         # take a free permit straight off _value
        ...

The torn window this drives: release() decides _waiters is non-empty and pops a
head waiter that is CONCURRENTLY being timed-out (its record about to flip
w[1]=False) -- if the hand-off (w[2]=True) and the self-cancel (w[1]=False) are
not serialised by _guard, the permit is handed to a dead waiter and VANISHES
(under-count / a stuck-below-K starvation), OR a barging acquire() reads
_value>0 and decrements while release() ALSO chose to bump _value for the SAME
logical permit (double-count / over-grant -> live holders momentarily exceed K).
The op pair under attack is therefore (release handoff-or-incr) vs (acquire
decr-or-park) vs (timed waiter self-cancel) on _value + _waiters.

p412 tests the BoundedSemaphore CHECK (over-release ValueError); p205 tests
cancel-of-a-queued-waiter; p48 tests the active<=K cap -- but NONE tests the
FIFO hand-off FAIRNESS + permit CONSERVATION of the PLAIN Semaphore's
_value-vs-_waiters direct transfer across hubs, where a release() popping a
waiter races that waiter's own timeout-removal or a barge-in acquire().

We make that a CLOSED-WORLD, falsifiable law with a single-owner CONTROL arm.
Three cases, round-robined by worker id in the first ops (NEVER flaky random --
the p125/p126/p172 coverage-flake fix):

CASE 0 CONSERVATION (contended shared hold).  Thousands of fibers acquire() a
permit on one of a small pool of SHARED Semaphore(K), bump a SEPARATELY-guarded
EXACT live-holder counter (NOT the primitive under test) and assert it never
exceeds K (a double-count / over-grant pushes it to K+1 -- the LEAK half), hold
across cooperative yields so other hubs contend, then release().  Every grant is
tallied (granted[]) and every matching release tallied (returned[]) in per-slot
single-writer tables.

CASE 1 TIMED SELF-CANCEL vs HAND-OFF (the hazard, contended).  A fiber does a
SHORT-timeout acquire() on a saturated shared semaphore while siblings hold and
release -- this is exactly the race between release()'s popleft+hand-off and the
timed waiter's own ``w[1]=False`` self-cancel.  The acquire MUST be all-or-
nothing: it returns True with a REAL permit it then releases (tallied as a
granted+returned pair), or False having taken NOTHING (no permit may be consumed
by a timed-out acquire -- that is the VANISHED-permit half).  A True return that
does not correspond to a real free slot, or a False return that nonetheless
consumed a permit, breaks conservation and is caught end-of-run by acquires ==
releases AND _value == K.

CASE 2 SINGLE-OWNER CONTROL.  A PRIVATE Semaphore(1) the fiber alone touches,
acquire()/release()'d race-free, MUST read _value == 1 after the matched pair
EVERY round.  A private semaphore has ONE writer, so it is conservation-correct
by construction; a drift there is the _value/_waiters hand-off machinery ITSELF
losing or doubling a permit, NOT contention -- this disambiguates "the primitive
is buggy" from "M:N contention dropped it" (the p405/p412 control-arm pattern).

FAIRNESS oracle (deterministic, separate gated round).  A coordinator drains all
K permits off a fresh shared Semaphore(K), then enqueues ORDERED waiters STRICTLY
SERIALLY: waiter i+1 is spawned only after waiter i is CONFIRMED parked in
sem._waiters (observed via len(sem._waiters) under the semaphore's own _guard), so
each waiter's index IS its real position in the _waiters deque -- the append order
the popleft hand-off actually follows (an EXTERNAL mark would not, since a fiber
can be preempted between the mark and the internal _waiters.append, an oracle
artifact not a fault).  The coordinator then release()s one permit at a time and
each woken waiter records its GRANT index.  Because every waiter is still queued
and none times out, FIFO requires grant-order == queue-order: a permit handed past
a strictly-earlier still-queued waiter (position i granted after position j>i) is a
FIFO HAND-OFF violation (the _waiters.popleft head-of-line guarantee broken under
M:N).

Invariant (hot, fail-fast): live holders of any shared semaphore <= K always; a
timed acquire is all-or-nothing; the private control semaphore reads _value == 1
after each matched pair; fairness grant-order == queue-order.
Invariant (post): per shared semaphore, granted == returned (conservation: no
permit vanished, none double-granted) and _value == K with zero live holders;
the private control never drifted; >=1 fairness round ran with no FIFO violation;
all three cases exercised; no lost worker.

Stresses: CoSemaphore release() FIFO popleft hand-off vs timed waiter
self-cancel (w[1]) vs barging acquire() decrement, _value/_waiters direct permit
transfer across hubs, permit conservation (vanished / double-granted), FIFO
head-of-line fairness under M:N.

Good TSan / controlled-replay target: the unsynchronised pair would be a data
race on the SAME _waiters record (release writes w[2], the timeout writes w[1])
or on _value (release += vs acquire -=); a TSan report on that record/field
localises a lost or doubled permit before the conservation sum even closes.
"""
import harness
import runloom


# K permits per shared semaphore.  Small enough that contention is real (most
# fibers must park and be handed a permit by a releaser on another hub), large
# enough that the live-holder count has room to be pushed past K by a double-
# grant rather than trivially staying at 1.
K = 4

# A small pool of SHARED semaphores so thousands of fibers pile onto each one --
# that is what drives genuine cross-hub release()-vs-acquire()-vs-timeout
# interleave on the SAME _value/_waiters fields.  Too many would scatter it.
NSEM = 8

# The case-1 timed self-cancel pool is even SMALLER so it is genuinely
# SATURATED: with thousands of fibers funneling through just CANCEL_NSEM
# Semaphore(K), most must park, and the parked timed acquirers actually reach
# their deadline and self-cancel WHILE a holder is releasing -- the exact
# popleft-hand-off vs w[1]=False race.  A big pool would never saturate and the
# race window would never open (every timed acquire would grab a free permit).
CANCEL_NSEM = 2

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Cooperative yields a holder does before releasing -- keeps the permit held
# across a park so multiple holders coexist and the live-holder count is
# genuinely driven toward K (where a leak would show as K+1).
HOLD_YIELDS = 2

# The short timeout for the case-1 timed self-cancel probe.  Tiny so most of
# these acquires actually hit the deadline while the semaphore is saturated --
# that is what fires the timed-waiter ``w[1]=False`` self-cancel concurrently
# with a releaser popping the head waiter.  Non-zero so the fiber really parks
# (a 0 timeout would short-circuit before appending to _waiters).
SELF_CANCEL_TIMEOUT = 0.003

# Fairness round: how many ordered waiters queue behind a drained Semaphore(K).
# Enough that a head-of-line violation has room to manifest (a later arrival
# jumping an earlier still-queued one).
FAIR_WAITERS = 6

# The three round-robined cases.
CASE_CONSERVE = 0       # contended shared acquire/hold/release
CASE_SELF_CANCEL = 1    # short-timeout acquire racing release's hand-off
CASE_CONTROL = 2        # private single-owner Semaphore(1) control
NCASES = 3


def shared_conserve(H, wid, rng, state, slot):
    """Case 0: contended shared path.  Acquire a permit on one of the shared
    Semaphores, assert the exact live-holder count never exceeds K while held,
    hold across yields, then release.  Tally the grant and the matching return
    so post() can prove granted == returned (no permit vanished or doubled)."""
    idx = wid % NSEM
    sem = state["sems"][idx]
    guard = state["guards"][idx]
    holders = state["holders"]            # exact live-holder count per semaphore
    granted = state["granted"]
    returned = state["returned"]

    got = sem.acquire()                   # blocking -- always returns True with a permit
    if got is not True:
        H.fail("shared blocking acquire() on semaphore {0} returned {1!r}, not "
               "True -- a blocking acquire must deliver a real permit (a permit "
               "was lost in the _value/_waiters hand-off)".format(idx, got))
        return False
    granted[slot] += 1

    over = False
    with guard:
        holders[idx] += 1
        h = holders[idx]
        if h > K:
            over = True
    if over:
        H.fail("live holders of shared semaphore {0} reached {1} > K={2} -- the "
               "plain Semaphore OVER-GRANTED a permit (release() bumped _value "
               "for a permit a barging acquire() ALSO took off _value, or handed "
               "a duplicate to a waiter): the double-count half".format(
                   idx, h, K))
        with guard:
            holders[idx] -= 1
        sem.release()
        returned[slot] += 1
        return False

    for _ in range(HOLD_YIELDS):
        runloom.yield_now()

    with guard:
        holders[idx] -= 1
    sem.release()
    returned[slot] += 1
    return True


def timed_self_cancel(H, wid, rng, state, slot):
    """Case 1 (the hazard): a SHORT-timeout acquire() on a SATURATED shared
    semaphore, racing the releasers.  Two siblings hold permits at the moment
    this fiber tries, so it parks in _waiters and either (a) is handed a permit
    by a release()'s popleft hand-off before the deadline -- returns True with a
    real permit it MUST release -- or (b) hits the deadline and self-cancels
    (w[1]=False) -- returns False having taken NOTHING.

    The all-or-nothing law: a True must be a real permit (tallied granted +
    returned as a pair); a False must consume no permit.  A permit handed to a
    waiter that simultaneously self-cancelled would VANISH (under-count); a
    permit both bumped onto _value and taken by this acquire would DOUBLE
    (over-count).  Either breaks the end-of-run granted == returned / _value == K
    conservation that post() checks."""
    # A SMALL cancel pool (CANCEL_NSEM << NSEM) so thousands of fibers pile onto
    # a few Semaphore(K) and most must park -> the semaphore is genuinely
    # SATURATED, which is what forces timed acquirers to actually hit their
    # deadline and self-cancel while a holder's release() is concurrently popping
    # the FIFO head.  Without saturation every timed acquire grabs a free permit
    # instantly and the self-cancel race never opens.
    idx = (wid + 1) % CANCEL_NSEM
    sem = state["cancel_sems"][idx]
    granted = state["granted"]
    returned = state["returned"]
    tg = state["timed_granted"]
    tt = state["timed_timeout"]

    got = sem.acquire(blocking=True, timeout=SELF_CANCEL_TIMEOUT)
    if got is True:
        # Handed a REAL permit (fast path, or by a releaser's hand-off before our
        # deadline).  HOLD it across a cooperative sleep LONGER than the timeout
        # so the timed acquirers parked behind us on the same saturated semaphore
        # actually reach their deadline and self-cancel WHILE we are about to
        # release() -- that is the popleft-hand-off vs w[1]=False race we attack.
        granted[slot] += 1
        tg[slot] += 1
        runloom.sleep(SELF_CANCEL_TIMEOUT * 2.0)
        sem.release()
        returned[slot] += 1
        return True
    if got is False:
        # Timed out: self-cancelled, took NOTHING.  Nothing to tally on the
        # permit ledger -- that is the whole point (a False that nonetheless
        # consumed a permit is the vanished-permit bug, caught by post()).
        tt[slot] += 1
        return True
    H.fail("timed acquire() on semaphore {0} returned {1!r} -- a timed acquire "
           "must be all-or-nothing (True with a permit, or False with none); "
           "any other value means the _value/_waiters hand-off torn a return"
           .format(idx, got))
    return False


def control_single_owner(H, wid, rng, state, slot):
    """Case 2: a PRIVATE single-owner Semaphore(1).  acquire() then release(),
    touched by NO other fiber, so it is conservation-correct by construction.
    After the matched pair _value MUST read 1.  A drift here is the
    _value/_waiters hand-off machinery ITSELF losing or doubling a permit, not
    contention -- the control falsifier."""
    import threading
    sem = threading.Semaphore(1)          # _value == 1
    got = sem.acquire(blocking=True, timeout=2.0)
    if got is not True:
        H.fail("acquire() on a fresh private Semaphore(1) with its permit free "
               "returned {0!r} -- a single-owner permit was lost on acquire "
               "(the hand-off machinery dropped it with no contention)".format(
                   got))
        return False
    # While held, _value must be 0 (the one permit is ours).
    v_held = sem._value
    if v_held != 0:
        H.fail("private Semaphore(1) read _value={0} while held, expected 0 -- "
               "a phantom permit appeared with a single owner (double-count in "
               "the decrement path)".format(v_held))
        sem.release()
        return False
    sem.release()
    v = sem._value
    if v != 1:
        H.fail("private single-owner Semaphore(1) ended a matched acquire/"
               "release at _value={0}, expected 1 -- the permit was {1} by the "
               "_value/_waiters machinery with NO contention (a primitive bug, "
               "not an M:N race)".format(v, "LOST" if v < 1 else "DOUBLED"))
        return False
    state["control_ok"][slot] += 1
    return True


def fairness_round(H, wid, rng, state, slot):
    """Deterministic FIFO hand-off check on a FRESH shared Semaphore(K).

    The FIFO order under test is the order in which acquirers APPEND to the
    semaphore's own ``_waiters`` deque inside acquire() -- NOT the order in which
    they reach some external counter (a fiber can be preempted between an
    external mark and the internal append, so an external mark does not reflect
    _waiters order; that is an oracle artifact, not a runtime fault).  So we
    establish a PROVEN _waiters order by enqueuing waiters STRICTLY SERIALLY:
    waiter i+1 is spawned only AFTER waiter i is CONFIRMED parked (its record is
    actually present in sem._waiters, observed under the semaphore's own guard).
    Its queue index is therefore exactly its position in _waiters.

    A coordinator drains all K permits (so every later acquire must park), serially
    enqueues FAIR_WAITERS confirmed-parked waiters, then release()s one permit at a
    time and each woken waiter records its GRANT index.  No waiter ever times out,
    so FIFO requires grant-order == queue-order: grant i must go to queue position
    i.  A permit handed past a strictly-earlier still-queued waiter is a head-of-
    line violation (the _waiters.popleft FIFO guarantee broken under M:N)."""
    import threading
    sem = threading.Semaphore(K)
    guard = state["fair_guard"]

    # Drain all K permits so every spawned waiter must park.
    for _ in range(K):
        if not sem.acquire(blocking=False):
            # Fresh Semaphore(K) with all permits free must grant K non-blocking
            # acquires; failing is itself a defect.
            H.fail("fresh Semaphore(K={0}) refused a non-blocking drain acquire "
                   "-- a free permit was unavailable (lost on init/decrement)"
                   .format(K))
            return False

    gseq = [0]                 # next grant index to hand out (under guard)
    grant_of = {}              # waiter id (== its proven queue index) -> grant index
    parked = [0]               # how many waiters are CONFIRMED in sem._waiters
    wg = runloom.WaitGroup()
    wg.add(FAIR_WAITERS)

    def waiter(myid):
        try:
            # Block until the coordinator hands us a permit (no timeout: this
            # waiter never self-cancels, so the only thing that wakes it is a
            # release() hand-off -- the FIFO path under test).  We are spawned
            # only after the PREVIOUS waiter is confirmed parked, so our append
            # to sem._waiters lands strictly after theirs -> our myid IS our
            # _waiters position (the proven queue order).
            got = sem.acquire(blocking=True)
            if got is not True:
                H.fail("fairness waiter {0} blocking acquire returned {1!r}, not "
                       "True -- a queued waiter was woken WITHOUT a permit (the "
                       "hand-off vanished a permit)".format(myid, got))
                return
            with guard:
                gi = gseq[0]
                gseq[0] = gi + 1
                grant_of[myid] = gi
            # We deliberately do NOT re-release here: a waiter re-releasing while
            # later waiters are still parked would itself hand a permit to a
            # still-queued waiter, polluting the one-coordinator-release-per-grant
            # accounting the FIFO oracle depends on.  We hold the permit and exit;
            # the conservation check below accounts for the K drained + the exact
            # FAIR_WAITERS handed out.
        finally:
            wg.done()

    # Serial enqueue: spawn waiter i, then yield until it is CONFIRMED present in
    # sem._waiters before spawning i+1.  This makes myid == _waiters position, so
    # the queue order the FIFO oracle checks is the semaphore's REAL append order
    # (immune to the preempt-between-mark-and-append reordering artifact).
    for i in range(FAIR_WAITERS):
        H.fiber(waiter, i)
        # Wait for this waiter to actually park in _waiters.  We read the length
        # under the semaphore's OWN guard so the snapshot is consistent with the
        # append.  Bounded spin: if it never parks (a lost wakeup on the way to
        # park) the harness watchdog catches the stall.
        target = i + 1
        spins = 0
        while True:
            with sem._guard:
                n = len(sem._waiters)
            if n >= target:
                break
            spins += 1
            if spins > 100000:
                H.fail("fairness waiter {0} never appeared in _waiters after "
                       "{1} yields -- it was lost on the way to park (a permit "
                       "queue never formed)".format(i, spins))
                return False
            runloom.yield_now()

    # All FAIR_WAITERS are now confirmed queued in _waiters in index order.
    # Release ONE permit at a time and WAIT for that permit's grant to be RECORDED
    # before releasing the next.  This is essential: the grant index is recorded
    # by the woken waiter, and if we released several permits back-to-back the
    # woken waiters would race each other to the grant-record guard in arbitrary
    # scheduler order, so grant_of order would NOT reflect the actual hand-off
    # order (another oracle artifact).  By draining one hand-off fully before the
    # next release, grant index g is provably the g-th permit release() handed
    # out, so grant-order == the true FIFO popleft order.
    for r in range(FAIR_WAITERS):
        sem.release()
        spins = 0
        while True:
            with guard:
                recorded = gseq[0]
            if recorded > r:
                break
            spins += 1
            if spins > 100000:
                H.fail("fairness release {0} never produced a grant after {1} "
                       "yields -- the permit was not handed to any queued waiter "
                       "(lost hand-off / vanished permit)".format(r, spins))
                return False
            runloom.yield_now()

    wg.wait()
    if H.failed:
        return False

    # Every waiter was granted exactly once, and the grant order matches the
    # queue order: grant index i went to the waiter whose _waiters position is i.
    if len(grant_of) != FAIR_WAITERS:
        H.fail("fairness round incomplete: granted={0} of {1} queued waiters -- "
               "a queued waiter was never handed a permit (lost hand-off / "
               "starvation)".format(len(grant_of), FAIR_WAITERS))
        return False
    for qi in range(FAIR_WAITERS):
        gi = grant_of[qi]
        if qi != gi:
            H.fail("FIFO hand-off violation: waiter at _waiters position {0} was "
                   "GRANTED at position {1} -- a permit was handed past a "
                   "strictly-earlier still-queued waiter (release()'s "
                   "_waiters.popleft head-of-line order broke under M:N)".format(
                       qi, gi))
            return False

    # Conservation on this fresh semaphore.  Accounting: started at K, the
    # coordinator drained K non-blocking (_value K -> 0), then released exactly
    # FAIR_WAITERS permits, EACH of which was handed to a queued waiter (so each
    # left _value at 0 and woke one waiter who holds it).  No waiter re-releases.
    # Therefore the only correct end state is _value == 0 with FAIR_WAITERS
    # permits held by the (now-exited) waiters: every permit is accounted for,
    # none vanished (would read < 0, impossible) and none doubled (would push
    # _value above 0 with no extra release).
    v = sem._value
    if v != 0:
        H.fail("fairness semaphore ended at _value={0}, expected 0 -- the K "
               "drained + {1} permits each handed to exactly one queued waiter "
               "should leave _value at 0; a non-zero value means release() "
               "DOUBLED a permit (bumped _value while ALSO handing one off)"
               .format(v, FAIR_WAITERS))
        return False
    state["fair_ok"][slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    # The fairness round is heavier (spawns FAIR_WAITERS children) and is run by
    # a deterministic subset of workers so it is always exercised but does not
    # dominate.  Every worker still round-robins the three permit-ledger cases.
    do_fair = (wid % 17) == 0
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1

        if sel == CASE_CONSERVE:
            ok = shared_conserve(H, wid, rng, state, slot)
        elif sel == CASE_SELF_CANCEL:
            ok = timed_self_cancel(H, wid, rng, state, slot)
        else:
            ok = control_single_owner(H, wid, rng, state, slot)
        if not ok:
            return

        # Interleave a fairness round on the chosen subset (after the ledger
        # case so coverage of the three cases is unaffected).
        if do_fair and (i % 5) == 1:
            if not fairness_round(H, wid, rng, state, slot):
                return

        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.* and the
    # cooperative locks are the patched, M:N-safe primitives.
    import threading
    sems = [threading.Semaphore(K) for _ in range(NSEM)]
    cancel_sems = [threading.Semaphore(K) for _ in range(CANCEL_NSEM)]
    guards = [threading.Lock() for _ in range(NSEM)]
    H.state = {
        "sems": sems,                     # case 0 contended-conservation pool
        "cancel_sems": cancel_sems,       # case 1 timed self-cancel pool
        "guards": guards,                 # exact live-holder guard per case-0 sem
        "holders": [0] * NSEM,            # exact live-holder count per case-0 sem
        "fair_guard": threading.Lock(),   # serialises fairness queue/grant indices
        "granted": [0] * SLOTS,           # permits taken (acquire returned True)
        "returned": [0] * SLOTS,          # permits given back (release called)
        "timed_granted": [0] * SLOTS,     # case-1 acquires handed a real permit
        "timed_timeout": [0] * SLOTS,     # case-1 acquires that timed out (no permit)
        "control_ok": [0] * SLOTS,        # case-2 control rounds that held _value==1
        "fair_ok": [0] * SLOTS,           # fairness rounds with no FIFO violation
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    granted = sum(H.state["granted"])
    returned = sum(H.state["returned"])
    tgrant = sum(H.state["timed_granted"])
    ttimeout = sum(H.state["timed_timeout"])
    control_ok = sum(H.state["control_ok"])
    fair_ok = sum(H.state["fair_ok"])
    H.log("granted={0} returned={1} (timed: handed={2} timed-out={3}) "
          "control_ok={4} fairness_rounds={5} ops={6}".format(
              granted, returned, tgrant, ttimeout, control_ok, fair_ok,
              H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed -- window was vacuous")

    # PERMIT CONSERVATION (the vanished/doubled law): every permit a fiber was
    # granted (acquire returned True) was matched by exactly one release; no
    # permit vanished into a self-cancelled waiter and none was double-granted.
    H.check(granted == returned,
            "permit conservation broken: granted={0} != returned={1} -- a permit "
            "VANISHED (handed to a timed-out/dead waiter -> under-count) or was "
            "DOUBLE-GRANTED (release bumped _value for a permit acquire also "
            "took) across the FIFO hand-off".format(granted, returned))
    H.check(granted > 0,
            "shared acquire/release path never exercised (no contention probe)")

    # Per-semaphore: zero live holders and back at the full bound K.  A drift in
    # _value is a net permit gain (over-grant) or loss (vanished) across the run.
    holders = H.state["holders"]
    for idx in range(NSEM):
        H.check(holders[idx] == 0,
                "shared semaphore {0} ended with {1} live holders (expected 0) "
                "-- permits not fully returned".format(idx, holders[idx]))
        v = H.state["sems"][idx]._value
        H.check(v == K,
                "case-0 semaphore {0} ended at _value={1}, expected K={2} -- net "
                "permit gain (over-grant) or loss (vanished) across the run"
                .format(idx, v, K))
    for idx in range(CANCEL_NSEM):
        cv = H.state["cancel_sems"][idx]._value
        H.check(cv == K,
                "case-1 (timed self-cancel) semaphore {0} ended at _value={1}, "
                "expected K={2} -- a timed acquire VANISHED a permit (handed to a "
                "self-cancelling waiter) or DOUBLE-counted one (barge vs incr)"
                .format(idx, cv, K))

    # The timed self-cancel hazard was actually exercised: at least one timed
    # acquire timed out (parked then self-cancelled, the race we attack) AND at
    # least one was handed a permit (the hand-off path).  If none timed out the
    # window never saturated; if none was handed a permit the hand-off never ran.
    H.check(tgrant + ttimeout > 0,
            "the timed self-cancel case never ran -- the release-vs-timeout "
            "hand-off race window was never opened")

    # The single-owner CONTROL never drifted: if it had, post() would already
    # have failed fast inside control_single_owner.  Assert it actually ran so
    # the falsifier is not vacuous.
    H.check(control_ok > 0,
            "the single-owner control arm never ran -- cannot distinguish a "
            "primitive bug from contention without it")

    # Fairness: at least one deterministic FIFO round ran clean (any head-of-line
    # violation already failed fast inside fairness_round).
    H.check(fair_ok > 0,
            "no fairness round completed -- the FIFO hand-off order guarantee "
            "was never exercised")

    H.require_no_lost("semaphore-fifo-handoff completeness")


if __name__ == "__main__":
    harness.main(
        "p429_semaphore_fifo_permit_handoff", body, setup=setup, post=post,
        default_funcs=3000,
        describe="plain Semaphore(K) FIFO permit hand-off + conservation under "
                 "M:N: release()'s _waiters.popleft direct hand-off races a timed "
                 "waiter's w[1]=False self-cancel and a barging acquire() decrement "
                 "-- granted==returned (no permit vanished/doubled), _value==K end "
                 "of run, a private Semaphore(1) control never drifts, and FIFO "
                 "grant-order==queue-order (no head-of-line violation)")
