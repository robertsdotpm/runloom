"""big_100 / 427 -- Event.set() waiter-deque snapshot/rebind vs a racing wait().

The subject is the cooperative ``threading.Event`` (monkey.patch() hands every
fiber the cooperative CoEvent, src/runloom/monkey/events.py:17, __slots__
``_flag`` / ``_waiters`` / ``_guard``).  Event is NOT a Condition: it gates
fibers on a single boolean ``_flag`` plus a hand-rolled ``_waiters`` deque, and
its set() does a TWO-STEP snapshot-then-rebind of that deque:

    def set(self):                                  # events.py:50
        self._guard.acquire()
        if self._flag:
            self._guard.release(); return
        self._flag = True
        waiters, self._waiters = list(self._waiters), collections.deque()
        self._guard.release()
        _unpark_all(waiters)                         # wakes the SNAPSHOT only

while a concurrent wait() does the mirror append + flag re-read:

    def wait(self, timeout=None):                   # events.py:69
        if self._flag: return True
        p = _Parker(inmem=True)
        self._guard.acquire()
        if self._flag: ... return True              # set() fired mid-build
        self._waiters.append(p)                     # <-- the racing append
        self._guard.release()
        while not self._flag: p.park(None)          # <-- the racing flag-read

THE HAZARD (exact C-level state + racing op pair).  ``self._waiters`` is a
collections.deque object pointer.  set()'s ``waiters, self._waiters =
list(self._waiters), deque()`` reads the OLD deque into a snapshot list and
REBINDS ``self._waiters`` to a fresh empty deque -- two stores around the live
object.  If a wait()er on ANOTHER hub runs its ``self._waiters.append(p)``
AFTER set() took the ``list(...)`` snapshot but BEFORE (or interleaved with) the
rebind, that append lands in a deque that is about to be DISCARDED -- the parker
is dropped, never handed to ``_unpark_all``, and the waiter ``while not _flag:
p.park(None)`` parks FOREVER despite a set() it should have observed.  The
mirror hazard is ``_flag``: set() writes True, clear() (events.py:60) writes
False, and wait()'s ``while not self._flag`` re-reads it -- a torn/lost flip
either strands a waiter (lost True) or wakes one that should still block (lost
False).  Racing triple: (set: snapshot+rebind+flag=True) vs (wait: flag-read +
waiter-append + park) vs (clear: flag=False).

The fix the suite is regression-guarding: events.py:39's ``_guard`` lock makes
set()'s snapshot+rebind ATOMIC against wait()'s append + flag re-check (both run
under the same ``self._guard``; the comment at events.py:36-38 states "Without
it set()'s waiter snapshot races a concurrent wait()'s append -> a lost wakeup
(the appended waiter parks forever)").  This program drives that exact window
hard and asserts the conservation law that a dropped append would break; if the
guard were removed or its acquire/release order changed, a waiter would be
parked-then-vanished and require_no_lost() / the woken==armed sum would fail.

TARGET INVARIANT -- CONSERVATION of wakeups under a closed handshake.  Per round
one SHARED Event and N waiter fibers.  Each waiter, on a different hub:
  1. bumps a single-writer per-slot ``armed`` cell (it has registered);
  2. signals a registration WaitGroup, then calls wait() -- appends its parker
     into ``_waiters`` and parks on ``_flag``;
  3. on return writes a single-writer per-slot ``woke`` cell IFF wait() returned
     True.
A single setter, AFTER the registration WaitGroup confirms all N have reached
their pre-wait point, calls set() EXACTLY ONCE (it yields once first so the
appends provably race the snapshot+rebind).  Conservation law for the round:
  woke == armed == N   -- every waiter that registered before the set MUST
return True from wait().  ``woke < armed`` means a snapshot/rebind DROPPED a
waiter's append (the lost wakeup) or ``_flag`` was torn so a parked waiter never
saw True; ``woke > armed`` is impossible by construction (single-writer cells)
and would itself indicate a torn write.

CLEAR sub-round (the flag-torn-the-OTHER-way half).  After the N waiters return,
the round does set()->clear()->fresh wait(timeout): a waiter that registers
AFTER the clear() must NOT spuriously return True from a timed wait -- if it
does, ``_flag`` read stale True (or clear()'s ``_flag = False`` store was lost),
a different tear of the same field.  We assert the post-clear timed wait()
returns False (it must time out) and that ``is_set()`` is False after clear().

SINGLE-OWNER CONTROL ARM.  A PRIVATE Event per waiter fiber, set/cleared/waited
with NO other fiber touching it, must yield woke==armed EXACTLY (1 arm, 1 set,
1 woke) and a post-clear private timed wait must return False.  A private Event
has one writer for its ``_waiters``/``_flag``, so it is race-free by
construction: if the PRIVATE arm ever drops a wakeup or spuriously returns True,
the CoEvent flag/deque machinery is broken INDEPENDENT of contention -- the
fault is CPython's/runloom's Event itself, not the cross-hub race.  The shared
arm is the contention probe; the private arm is the falsifier.

Invariant (hot, fail-fast): in any round, no waiter returns False from an untimed
wait() whose Event was set before the wait completed; a post-clear timed wait
returns False; is_set() agrees with the last set/clear.  Invariant (post): summed
shared woke == summed shared armed (every registered waiter woken -- no lost
append, no torn flag); summed private woke == summed private armed (control);
both set and clear sub-cases exercised; no worker LOST (require_no_lost catches a
waiter parked-then-vanished by a dropped append).

Stresses: Event.set() ``_waiters`` deque snapshot+rebind vs a concurrent
wait()'s ``_waiters.append`` (lost-append / lost-wakeup), ``_flag`` set/clear/
re-read tear, the ``_guard``-atomicity regression, cross-hub waiter handshake
conservation, private-vs-shared Event wakeup conservation.

Good TSan / controlled-replay target: the snapshot read + rebind store of
``self._waiters`` against a sibling hub's ``self._waiters.append`` is a textbook
object-pointer / deque-mutation race; a TSan report on the ``_waiters`` slot or
``_flag`` store/load, or a single round where woke<armed under replay, localizes
the dropped wakeup before the conservation sum even closes.  RNG is per-worker
(rng) for replay; the setter's pre-set yield seeds the park window.
"""
import harness
import runloom

# Waiter fibers per shared round.  Small enough that every waiter genuinely
# parks (the setter fires exactly once after they all register, so each must be
# handed the wake by set()'s snapshot), large enough that several distinct hubs
# append into ``_waiters`` concurrently with the snapshot+rebind -- that is the
# cross-hub window the lost-append needs.
WAITERS = 6

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Bound for the post-clear timed wait that MUST time out (return False).  Kept
# short so a correct clear is confirmed quickly; long enough that a slow M:N
# hand-off does not falsely "time out" a wait that a (buggy) stale flag would
# have returned True for -- if the flag were torn True the wait returns True
# IMMEDIATELY, well inside this bound, so a False here is an honest timeout.
CLEAR_WAIT_TIMEOUT = 0.05

# The two sub-cases post() asserts were exercised; the worker round-robins them
# by id in its first ops (NOT random -- pure random reliably MISSES a case at the
# low op-count a timeout-bound run completes, the p125/p126/p172 flaky-coverage
# bug the suite already had to fix).
CASE_SHARED_HANDSHAKE = 0   # N waiters park on a SHARED Event, one set() wakes all
CASE_CLEAR_BLOCKS = 1       # set()->clear(); a post-clear timed wait must NOT return
NCASES = 2


def run_shared_handshake(H, wid, rng, state, slot):
    """CASE 0 -- the conservation handshake on a SHARED Event.

    Spawn WAITERS waiter fibers; each bumps a per-slot ``armed`` cell, signals a
    registration WaitGroup, then wait()s on the shared Event (appending its
    parker into ``_waiters`` and parking on ``_flag``).  The setter waits on the
    registration WaitGroup so every waiter has reached its pre-wait point, yields
    once so the appends race the snapshot, then set()s EXACTLY ONCE.  Each waiter
    writes a per-slot ``woke`` cell iff wait() returned True.  Conservation
    (woke == armed == WAITERS) is reconciled in post(); a dropped append shows up
    here as a waiter still parked at join time -> require_no_lost, and as
    woke < armed.

    A SHARED single-writer cell per waiter index avoids any cross-hub ``+=``: the
    setter's slot is keyed by the waiter's local index, and each waiter writes
    only its own cell."""
    ev = state["make_event"]()              # fresh SHARED Event for this round
    armed = state["shared_armed"]
    woke = state["shared_woke"]

    # Per-round single-writer result cells, one per waiter index (no shared +=).
    armed_cell = [0] * WAITERS
    woke_cell = [0] * WAITERS

    reg_wg = runloom.WaitGroup()            # every waiter signals once pre-wait
    reg_wg.add(WAITERS)
    done_wg = runloom.WaitGroup()           # every waiter signals once on return
    done_wg.add(WAITERS)

    def waiter(idx):
        try:
            # Register: we are about to append into _waiters and park on _flag.
            armed_cell[idx] = 1
            reg_wg.done()
            # wait() with NO timeout: by contract it returns True only when the
            # flag is observed set.  The set() below fires exactly once after all
            # of us registered, so a correct Event MUST wake every one of us.  A
            # dropped append (snapshot/rebind discarded our parker) strands us
            # here -> the watchdog/require_no_lost catches the parked-then-vanished
            # waiter; if the lost append instead let wait() return on a stale flag
            # path it would still set woke, so woke<armed is the in-band signal.
            got = ev.wait()
            if got:
                woke_cell[idx] = 1
            else:
                # An untimed wait() must NEVER return False -- a False here is the
                # contract violation (a torn _flag let the park loop exit without
                # the flag actually being set).
                H.fail("shared untimed Event.wait() returned False for waiter "
                       "{0} (wid {1}) -- an untimed wait must only ever return "
                       "True; _flag was read torn/false after set()".format(
                           idx, wid))
        finally:
            done_wg.done()

    def setter():
        # Wait until every waiter has registered (reached its pre-wait point), so
        # set() races their _waiters.append into the snapshot+rebind window.
        reg_wg.wait()
        # Yield once so the appends and our snapshot/rebind genuinely interleave
        # across hubs (the appends may still be landing as we take list(_waiters)).
        runloom.yield_now()
        ev.set()                            # EXACTLY ONCE: wakes the snapshot

    for idx in range(WAITERS):
        H.fiber(waiter, idx)
    H.fiber(setter)

    done_wg.wait()                          # all waiters returned -> round quiescent
    if H.failed:
        return False

    # Hot per-round conservation: every armed waiter must have woken.  (The post()
    # sum reconciles globally; this fail-fast localizes the round.)
    a = sum(armed_cell)
    w = sum(woke_cell)
    if a != WAITERS:
        H.fail("shared handshake: {0}/{1} waiters armed (wid {2}) -- a waiter "
               "fiber never reached its pre-wait registration".format(
                   a, WAITERS, wid))
        return False
    if w != a:
        H.fail("shared handshake LOST WAKEUP: woke={0} != armed={1} (wid {2}) -- "
               "a waiter that registered before set() was NOT woken; set()'s "
               "_waiters snapshot+rebind dropped a concurrent wait()'s append "
               "(the appended parker landed in the discarded deque)".format(
                   w, a, wid))
        return False

    # The Event is set and quiescent now; is_set() must agree.
    if not ev.is_set():
        H.fail("shared Event.is_set() False after a set() that woke all waiters "
               "(wid {0}) -- _flag store lost".format(wid))
        return False

    # Record into per-slot tallies (single-writer-per-slot, summed in post).
    armed[slot] += a
    woke[slot] += w
    return True


def run_clear_blocks(H, wid, rng, state, slot):
    """CASE 1 -- the flag-torn-the-OTHER-way half on a SHARED Event.

    set() then clear(); then a FRESH waiter registers (after the clear) and does
    a TIMED wait().  Because _flag was cleared, that wait() MUST time out and
    return False -- a True return means _flag was read stale True (or clear()'s
    ``_flag = False`` store was lost), the inverse tear of the lost-wakeup.  Also
    asserts is_set() is False after clear()."""
    ev = state["make_event"]()
    blocked = state["shared_clear_blocked"]

    ev.set()
    if not ev.is_set():
        H.fail("shared Event.is_set() False immediately after set() (wid {0}) -- "
               "_flag = True store lost".format(wid))
        return False
    ev.clear()
    if ev.is_set():
        H.fail("shared Event.is_set() True immediately after clear() (wid {0}) -- "
               "clear()'s _flag = False store was lost (flag torn the other "
               "way)".format(wid))
        return False

    # A timed wait registered AFTER the clear must NOT spuriously return True.
    got = ev.wait(timeout=CLEAR_WAIT_TIMEOUT)
    if got:
        H.fail("post-clear timed Event.wait() returned True (wid {0}) -- the flag "
               "was read stale True after clear(): clear()'s _flag store was lost "
               "or wait()'s `while not _flag` re-check saw a torn flag, so a "
               "waiter unblocked despite a cleared Event".format(wid))
        return False
    # Correct: the wait timed out on a cleared flag.
    blocked[slot] += 1
    return True


def run_private_control(H, wid, rng, state, slot):
    """CONTROL ARM -- a PRIVATE Event with a SINGLE owner (this fiber).

    No other fiber touches this Event, so its ``_waiters``/``_flag`` have one
    writer and it is race-free by construction.  set() then wait() (pre-set: the
    flag is already True so wait() returns immediately without ever appending,
    exercising the early ``if self._flag: return True`` fast path) must yield
    woke==armed==1; clear() then a TIMED wait() must return False.  If the PRIVATE
    arm ever loses a wakeup or spuriously returns True, the Event machinery is
    broken independent of contention -- the falsifier that distinguishes "the
    primitive is buggy" from "the cross-hub race dropped it".  Run every op so the
    control sum tracks the op count."""
    ev = state["make_event"]()
    parm = state["private_armed"]
    pwoke = state["private_woke"]

    # set() before wait(): the fast-path `if self._flag: return True`.
    ev.set()
    armed = 1
    got1 = ev.wait()
    if not got1:
        H.fail("PRIVATE Event.wait() returned False after our own set() (wid {0}) "
               "-- a single-owner Event lost a wakeup; the CoEvent flag/fast-path "
               "is broken INDEPENDENT of contention".format(wid))
        return False
    # Also park-then-wake on a private Event: clear, spawn a child that sets after
    # we are parked, and confirm the untimed wait wakes (the real append+park+
    # snapshot path, but single-owner so race-free).
    ev.clear()
    if ev.is_set():
        H.fail("PRIVATE Event.is_set() True after clear() (wid {0}) -- single-"
               "owner clear() store lost".format(wid))
        return False
    woke2_cell = [0]
    set_wg = runloom.WaitGroup()
    set_wg.add(1)

    def child_setter():
        try:
            # Yield a few times so the parent reaches its park before we set, so
            # this exercises the append+park+snapshot wake path (not the fast
            # path), yet only THIS fiber-pair touches the Event -> still race-free.
            for _ in range(3):
                runloom.yield_now()
            ev.set()
        finally:
            set_wg.done()

    H.fiber(child_setter)
    got2 = ev.wait()                        # untimed: parks, child set() wakes us
    set_wg.wait()
    if got2:
        woke2_cell[0] = 1
    else:
        H.fail("PRIVATE Event.wait() returned False after a single-owner child "
               "set() (wid {0}) -- the append+park+snapshot wake path lost a "
               "wakeup on an UNCONTENDED Event".format(wid))
        return False

    # Two single-owner wakeups proven (fast-path + park path); the post-clear
    # timed-wait control mirrors case 1's inverse tear on a private Event.
    ev.clear()
    got3 = ev.wait(timeout=CLEAR_WAIT_TIMEOUT)
    if got3:
        H.fail("PRIVATE post-clear timed Event.wait() returned True (wid {0}) -- "
               "a single-owner cleared Event spuriously unblocked; clear() store "
               "lost or _flag read stale True, independent of contention".format(
                   wid))
        return False

    # Conservation on the control arm: armed (1) == woke (1, the park-path wake).
    parm[slot] += armed
    pwoke[slot] += woke2_cell[0]
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the two shared cases by worker id in the first ops so each
        # is exercised even when a worker manages only a few ops under the timeout
        # (the flaky-random-coverage fix); random after.  The private control arm
        # runs EVERY round so its conservation sum always tracks the op count.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1

        if sel == CASE_SHARED_HANDSHAKE:
            if not run_shared_handshake(H, wid, rng, state, slot):
                return
        else:
            if not run_clear_blocks(H, wid, rng, state, slot):
                return

        # Control arm every round -- the single-owner falsifier.
        if not run_private_control(H, wid, rng, state, slot):
            return

        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.Event is
    # the cooperative CoEvent.  make_event() hands out a fresh CoEvent so each
    # round's shared/private/clear arms get an independent _waiters deque + _flag.
    import threading
    H.state = {
        "make_event": threading.Event,
        # Shared-handshake conservation (per-slot single-writer tallies).
        "shared_armed": [0] * SLOTS,
        "shared_woke": [0] * SLOTS,
        # Clear sub-case: post-clear timed waits that correctly stayed blocked.
        "shared_clear_blocked": [0] * SLOTS,
        # Private control-arm conservation.
        "private_armed": [0] * SLOTS,
        "private_woke": [0] * SLOTS,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    s_armed = sum(H.state["shared_armed"])
    s_woke = sum(H.state["shared_woke"])
    s_blocked = sum(H.state["shared_clear_blocked"])
    p_armed = sum(H.state["private_armed"])
    p_woke = sum(H.state["private_woke"])
    H.log("shared handshake armed={0} woke={1} | post-clear stayed-blocked={2} | "
          "private-control armed={3} woke={4} | ops={5}".format(
              s_armed, s_woke, s_blocked, p_armed, p_woke, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed -- the set/clear/wait race "
                               "window was never exercised")

    # CONSERVATION (shared arm): every waiter that armed before a set() was woken.
    # A lost append in set()'s snapshot+rebind, or a torn _flag, drops a wakeup
    # here -> s_woke < s_armed.  (The hot per-round check already fails fast; this
    # is the global reconciliation after every worker joined.)
    H.check(s_woke == s_armed,
            "shared-Event wakeup conservation broken: woke={0} != armed={1} -- a "
            "waiter that registered before set() was not woken; set()'s _waiters "
            "snapshot+rebind dropped a concurrent wait()'s append (lost wakeup)"
            .format(s_woke, s_armed))
    H.check(s_armed > 0,
            "shared handshake never exercised -- no waiter ever armed on a shared "
            "Event (the cross-hub snapshot-vs-append window was never driven)")

    # CONTROL ARM: a single-owner Event must conserve wakeups exactly.  A loss here
    # is the CoEvent machinery itself, not contention -- the falsifier.
    H.check(p_woke == p_armed,
            "PRIVATE-Event control conservation broken: woke={0} != armed={1} -- a "
            "single-owner Event lost a wakeup, so the CoEvent flag/deque machinery "
            "is broken INDEPENDENT of contention".format(p_woke, p_armed))
    H.check(p_armed > 0,
            "private control arm never exercised -- the single-owner falsifier did "
            "not run")

    # The clear sub-case (flag-torn-the-other-way) must have been exercised at
    # least once: a post-clear timed wait correctly stayed blocked.
    H.check(s_blocked > 0,
            "post-clear blocking sub-case never exercised -- the flag-cleared "
            "no-spurious-wake invariant (case 1) was never tested")

    # Lost-vs-slow oracle: a waiter dropped by a snapshot/rebind append-loss is
    # parked-then-vanished -> require_no_lost FAILS (the in-band woke<armed check
    # is the value-level signal; this is the structural one).
    H.require_no_lost("event-handshake conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p427_event_set_clear_waiters_snapsh", body, setup=setup, post=post,
        default_funcs=3000,
        describe="N waiter fibers park on a SHARED threading.Event across hubs; a "
                 "single setter set()s once after a WaitGroup confirms all "
                 "registered, racing each wait()'s _waiters.append against set()'s "
                 "deque snapshot+rebind.  Conservation: woke==armed (every "
                 "registered waiter woken -- a dropped append is a lost wakeup); a "
                 "post-clear timed wait must NOT spuriously return; a PRIVATE "
                 "single-owner Event control must conserve wakeups exactly")
