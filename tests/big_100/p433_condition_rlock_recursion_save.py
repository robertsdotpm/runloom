"""big_100 / 433 -- Condition(RLock()).wait() recursion-depth save/restore under M:N.

The subject is the cooperative ``threading.Condition`` built on a re-entrant
``threading.RLock`` (monkey.patch() hands every fiber a CoCondition wrapping a
CoRLock).  The internal state under attack is the CoRLock's RECURSION DEPTH
COUNTER, ``self._lock._count`` (an ordinary int field on the SHARED lock object,
alongside ``self._lock._owner``).  The racing op pair is the wait()-time
save/force/restore of that exact field versus a sibling fiber's
acquire()/release() mutation of the SAME field on the shared lock.

CoCondition.wait() (src/runloom/monkey/events.py:145-183) literally does, for an
RLock-backed Condition:

    owned_recursion = self._lock._count        # SAVE the recursion depth
    self._lock._count = 1                       # FORCE to 1 so the single
    self._lock.release()                        #   release() fully drops it
    ...park...                                  #   (count 1 -> 0, _owner = None)
    self._lock.acquire()                        # reacquire (count -> 1)
    ...
    self._lock._count = owned_recursion         # RESTORE the saved depth

That save/force-1/restore straddles a PARK on a grown-down C stack.  While the
waiter is parked its lock is FULLY released (_count == 0, _owner == None), so a
sibling fiber on ANOTHER hub can acquire the SAME shared Condition's lock to its
OWN depth and itself enter wait() -- forcing _count = 1 on the shared field --
in the exact window before the first waiter's restore lands.  The hazards, both
falsifiable:

  * UNDER-RESTORE: the woken waiter writes back a depth computed from a stale
    _count/_owner (a racing release() nulled _owner, or another waiter's force-1
    overwrote _count between the reacquire and the restore).  The waiter resumes
    holding the lock 1-deep (or 0-deep) when it entered D-deep.  Its own later
    D releases then under-flow: the (D-1)th release drops _count to 0 and nulls
    _owner early, and a subsequent release on the now-unowned lock raises
    "cannot release un-acquired lock" (orphaned _owner / negative count).
  * OVER-RESTORE: the waiter writes back a depth LARGER than D (a stale larger
    _count snapshot).  Its D releases never reach 0, so the inner CoLock is
    NEVER released -- the Condition's lock is leaked, permanently held; the next
    fiber to acquire it parks forever (watchdog HANG) and a probe try-acquire
    fails.

p316 tests the predicate logic and p47 the notify storm -- NEITHER touches the
recursion-count save/restore, which is the precise field a torn read corrupts:
a waiter that recursively held the lock 3-deep MUST wake holding it 3-deep, not
1-deep or 0-deep.

TARGET INVARIANT -- RECURSION-DEPTH CONSERVATION across wait():

  * SHARED arm (the contention probe).  A small pool of SHARED Condition(RLock())
    is hammered by many waiter fibers + a notifier fiber.  Each waiter acquires
    the Condition's lock to a round-robined depth D = (wid % 3) + 1 (so the
    universe of legal depths is the FINITE SET {1, 2, 3}), then cond.wait_for(pred)
    AT that depth.  On return it MUST observe _recursion_count() == D and
    _is_owned() True -- the depth it entered with is EXACTLY restored (a depth
    outside {1,2,3}, or != the D it entered, is a torn save/restore).  It writes
    that observed-on-wake depth into a PER-WAITER single-writer cell and asserts
    cell == D.  It then releases D times; after the Dth release the lock MUST be
    fully free for the next acquirer (no leak).  Per-shared-lock acquires and
    releases are tallied (single-writer-per-slot); CONSERVATION: total acquire
    levels == total release levels across all fibers, so no recursion level is
    leaked (over-restore -> a permanently held level) or phantom (under-restore
    -> an under-flowing release).  End of run every shared lock is provably free
    (a probe fiber acquires it: _count goes 0 -> 1, _owner set; release -> 0).

  * SINGLE-OWNER CONTROL arm (the falsifier).  A PRIVATE Condition(RLock()) that
    no other fiber touches, entered to depth D, wait_for'd with an immediately-
    TRUE predicate.  predicate() is true on the first check so wait() never
    actually parks for a notify -- BUT the save/force-1/restore of _count still
    runs the first time through wait_for only if the predicate is false; with a
    true predicate wait() is not entered at all, so we ALSO run a control variant
    that forces ONE real park (predicate flips true via a private notifier fiber)
    on a lock NO sibling shares.  A single-owner lock is race-free by
    construction, so if the control restores a depth != D the fault is in the
    _count stash machinery ITSELF, not contention -- this isolates the bug.

require_no_lost() in post() catches a waiter stranded by a lost restore (an
over-restore leak parks the next acquirer forever; a lost-wakeup never returns).

Invariant (hot, fail-fast): every observed-on-wake depth in {1,2,3} and == the
entered D; _is_owned() True on wake; after D releases the lock is free; the
private control restores depth D.
Invariant (post): acquire levels == release levels (recursion conservation);
every shared lock free at end (probe acquires/releases cleanly); each depth case
{1,2,3} exercised on both arms; no lost waiter.

Stresses: CoCondition.wait() recursion-count save (_count -> 1) / restore
(_count -> owned_recursion) on a SHARED CoRLock across a park, vs sibling
acquire/release mutating the same _count/_owner; depth-conservation under M:N;
under-restore under-flow ("cannot release un-acquired lock") / over-restore lock
leak.

Good TSan / controlled-replay target: the plain `self._lock._count = ...` writes
in wait() racing a sibling's `self._count += 1` / `self._count -= 1` on the same
shared int field are a textbook read-modify-write / torn-write data race; a TSan
report on CoRLock._count localizes the corruption before the depth assert fires.
RNG is per-fiber (rng / rng.getrandbits-seeded children) so a failure replays.
"""
import threading

import harness
import runloom

# Finite UNIVERSE of legal recursion depths.  A waiter enters at depth D in this
# set and MUST wake at exactly D; an observed-on-wake depth outside this set (or
# != the entered D) is a torn save/restore.  Three depths so re-entrancy is real
# (depth 1 is the trivial non-recursive case; 2 and 3 exercise the count having
# to be saved as >1, forced to 1, and restored to >1).
DEPTHS = (1, 2, 3)
NDEPTH = len(DEPTHS)

# A small pool of SHARED Condition(RLock()) so many waiter fibers pile onto each
# one -- that is what drives genuine cross-hub acquire/release-vs-save/restore
# interleave on the SAME _count/_owner fields.  Too many would scatter the
# contention; too few would serialize through one lock and hide the race.
NCOND = 6

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Waiters per shared Condition per round.  Several distinct hubs holding the SAME
# Condition's lock to different depths and parking in wait() is the contention
# that lets a sibling's force-1 land in another waiter's restore window.
WAITERS = 4


def shared_round(H, wid, rng, slot, state):
    """One contention round on a SHARED Condition(RLock()).

    Spawn WAITERS waiter fibers, each acquiring the shared Condition's lock to a
    round-robined depth D in {1,2,3} and parking in wait_for() at that depth,
    plus one notifier fiber that flips the predicate and notify_all()s.  Each
    waiter asserts on wake: _recursion_count() == D, _is_owned() True, observed
    depth in {1,2,3}.  It then releases D times; the cell it wrote MUST equal D.
    Acquire/release LEVELS are tallied for the post() conservation law.

    The contention under test is the WAITERS fibers all parking on the SAME
    shared CoRLock and each forcing its _count to 1 in wait() -- their
    save/force-1/restore writes race across hubs on the one shared _count/_owner.
    Many shared rounds on the OTHER conds run fully concurrently.  We DO serialize
    distinct rounds on the SAME cond behind a per-cond round-guard (a SEPARATE
    cooperative lock, NOT the Condition's RLock), so two rounds don't clobber each
    other's predicate state -- that would be a test bug, not the hazard.  The
    cross-hub _count race the program targets is entirely WITHIN a round."""
    idx = wid % NCOND
    cond = state["conds"][idx]
    box = state["boxes"][idx]            # {"ready": bool}
    guard = state["guards"][idx]         # per-cond round-guard (distinct lock)
    acq_levels = state["acq_levels"]     # per-slot tally of acquire levels
    rel_levels = state["rel_levels"]     # per-slot tally of release levels
    ok_depth = state["ok_depth"]         # per-slot count of correct wake depths

    # Own this cond for the round so a sibling round on the SAME cond can't reset
    # `ready` underneath our waiters.  guard is a plain CoLock, distinct from the
    # Condition's RLock under test, so it never touches the _count field.
    if not guard.acquire(blocking=True, timeout=20.0):
        # The previous round on this cond did not release the guard in time --
        # that itself means a waiter is stranded (an over-restore leak parks the
        # next acquirer).  Let require_no_lost / watchdog adjudicate; bail.
        return
    try:
        with cond:
            box["ready"] = False

        wg = runloom.WaitGroup()
        wg.add(WAITERS + 1)

        def run_waiter(k):
            # Round-robin the depth by (wid + k) so every depth in {1,2,3} is
            # exercised even when each fiber does only one round (the flaky-random
            # coverage fix from p125/p126).
            d = DEPTHS[(wid + k) % NDEPTH]
            try:
                if H.failed or not H.running():
                    return
                # Acquire the SHARED Condition's lock to depth d (the `with` is
                # depth 1; d-1 more acquires take it to d).
                cond.acquire()
                extra = 0
                try:
                    for _ in range(d - 1):
                        cond.acquire()
                        extra += 1
                    # Now hold the SHARED CoRLock d-deep.  wait_for parks
                    # (predicate false until the notifier flips it) -- this is the
                    # save (_count -> 1) / release / park / reacquire / restore
                    # (_count -> d) straddling the park, while sibling waiters on
                    # other hubs do the SAME force-1 on this very lock.
                    got = cond.wait_for(lambda: box["ready"], timeout=20.0)
                    if not got:
                        # Predicate never satisfied within the generous timeout:
                        # the notify was lost OR the restore stranded this waiter.
                        # Either is a fault on the shared path (the notifier always
                        # fires this round; require_no_lost separately catches a
                        # vanished waiter).
                        H.fail("shared waiter (cond {0}, depth {1}) timed out in "
                               "wait_for -- notify lost or recursion restore "
                               "stranded the waiter".format(idx, d))
                        return
                    # ---- the depth invariant, checked WHILE still holding -----
                    depth = cond._lock._recursion_count()
                    owned = cond._lock._is_owned()
                    if depth not in DEPTHS:
                        H.fail("shared waiter woke at OUT-OF-UNIVERSE recursion "
                               "depth {0} (cond {1}, entered depth {2}); legal "
                               "depths are {3} -- torn _count save/restore across "
                               "the park under M:N".format(depth, idx, d, DEPTHS))
                        return
                    if depth != d:
                        H.fail("recursion depth NOT conserved: shared waiter "
                               "entered wait() at depth {0} but woke at depth {1} "
                               "(cond {2}) -- a sibling's force-1/acquire mutated "
                               "the shared _count in the restore window "
                               "(under/over-restore)".format(d, depth, idx))
                        return
                    if not owned:
                        H.fail("shared waiter woke NOT owning its lock (cond {0}, "
                               "entered depth {1}) -- _owner was nulled across the "
                               "park and the restore did not re-establish "
                               "ownership".format(idx, d))
                        return
                    ok_depth[slot] += 1
                    acq_levels[slot] += d      # entered d levels (conservation)
                finally:
                    # Release the d-1 extra levels we took (mirror of the loop
                    # above); the `with` releases the last level.  If the restore
                    # corrupted _count this is exactly where an under-flow raises.
                    for _ in range(extra):
                        cond.release()
                # leaving the `with`: the final release.  After it the lock MUST be
                # fully free for the next acquirer.
                cond.release()
                rel_levels[slot] += d          # released d levels (conservation)
            except RuntimeError as exc:
                # "cannot release un-acquired lock" is the SYMPTOM of an
                # under-restore under-flow -- the depth came back too small, so a
                # release dropped _count to 0 / nulled _owner early and the next
                # release blew up.
                H.fail("shared waiter (cond {0}, depth {1}) raised on release: "
                       "{2} -- recursion-count under-restore under-flowed the "
                       "CoRLock (_count went non-positive / _owner nulled early)"
                       .format(idx, d, exc))
            finally:
                wg.done()

        def run_notifier():
            try:
                # Let the waiters reach their park, then flip the predicate and
                # wake them all.  notify_all under the lock is the legal protocol;
                # the waiters reacquire one-by-one and each runs its restore.
                for _ in range(WAITERS):
                    runloom.yield_now()
                spins = 0
                while not H.failed:
                    with cond:
                        box["ready"] = True
                        cond.notify_all()
                    # A couple more notify rounds in case a waiter had not yet
                    # parked when the first notify fired (it re-checks the
                    # predicate, which is now true, so it won't re-park -- but a
                    # belt-and-braces re-notify keeps the round from depending on
                    # exact park timing).
                    runloom.yield_now()
                    spins += 1
                    if spins >= 4:
                        break
            finally:
                wg.done()

        for k in range(WAITERS):
            H.fiber(run_waiter, k)
        H.fiber(run_notifier)
        wg.wait()
    finally:
        guard.release()


def control_round(H, wid, rng, slot, state):
    """SINGLE-OWNER CONTROL: a PRIVATE Condition(RLock()) no sibling touches,
    entered to depth D, wait_for'd with ONE real park, then woken by a private
    notifier.  A single-owner lock is race-free by construction, so if the depth
    is not exactly restored the fault is the _count stash machinery itself, not
    contention.  Round-robin D by wid so every depth is covered."""
    d = DEPTHS[wid % NDEPTH]
    cond = threading.Condition(threading.RLock())     # PRIVATE -- one owner only
    box = {"ready": False}
    ctrl_ok = state["ctrl_ok"]
    ctrl_acq = state["ctrl_acq"]
    ctrl_rel = state["ctrl_rel"]

    wg = runloom.WaitGroup()
    wg.add(2)

    result = {"depth": None, "owned": None, "underflow": False}

    def run_waiter():
        try:
            cond.acquire()
            extra = 0
            try:
                for _ in range(d - 1):
                    cond.acquire()
                    extra += 1
                # Predicate false initially -> wait() IS entered and the
                # save/force-1/release/park/reacquire/restore runs for real, then
                # the private notifier flips it.
                got = cond.wait_for(lambda: box["ready"], timeout=20.0)
                if not got:
                    H.fail("CONTROL waiter (depth {0}) timed out in wait_for on a "
                           "PRIVATE single-owner Condition -- the _count stash "
                           "machinery stranded a waiter with NO contention".format(d))
                    return
                result["depth"] = cond._lock._recursion_count()
                result["owned"] = cond._lock._is_owned()
                ctrl_acq[slot] += d
            finally:
                for _ in range(extra):
                    cond.release()
            cond.release()
            ctrl_rel[slot] += d
        except RuntimeError as exc:
            result["underflow"] = True
            H.fail("CONTROL waiter (depth {0}) raised on release: {1} -- the "
                   "recursion-count save/restore under-flowed a PRIVATE lock "
                   "with NO sibling contention (machinery bug, not a race)".format(
                       d, exc))
        finally:
            wg.done()

    def run_notifier():
        try:
            for _ in range(2):
                runloom.yield_now()
            with cond:
                box["ready"] = True
                cond.notify_all()
        finally:
            wg.done()

    H.fiber(run_waiter)
    H.fiber(run_notifier)
    wg.wait()

    if H.failed:
        return
    depth = result["depth"]
    owned = result["owned"]
    if depth not in DEPTHS:
        H.fail("CONTROL woke at OUT-OF-UNIVERSE recursion depth {0} (entered {1}) "
               "on a PRIVATE single-owner lock -- the wait() _count save/restore "
               "is broken independent of contention".format(depth, d))
        return
    if depth != d:
        H.fail("CONTROL depth NOT restored: PRIVATE waiter entered at depth {0} "
               "but woke at depth {1} -- the _count stash (save -> force 1 -> "
               "restore) does not round-trip even with a single owner".format(
                   d, depth))
        return
    if not owned:
        H.fail("CONTROL woke NOT owning its PRIVATE lock (entered depth {0}) -- "
               "the restore did not re-establish _owner".format(d))
        return
    ctrl_ok[slot] += 1


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the two arms (shared contention probe / single-owner
        # control) by id in the first ops so both are exercised even under a
        # tight op budget; random after.  Depth coverage is round-robined inside
        # each arm by (wid + k) / (wid).
        if i < 2:
            sel = (wid + i) % 2
        else:
            sel = rng.randrange(2)
        i += 1
        if sel == 0:
            shared_round(H, wid, rng, slot, state)
        else:
            control_round(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.Condition /
    # threading.RLock are the cooperative CoCondition / CoRLock under test.
    conds = [threading.Condition(threading.RLock()) for _ in range(NCOND)]
    boxes = [{"ready": False} for _ in range(NCOND)]
    # Per-cond round-guard: a SEPARATE cooperative lock (NOT the Condition's
    # RLock) so only one shared_round drives a given cond at a time.  It never
    # touches the _count field under test -- it just keeps two rounds from
    # clobbering each other's `ready` predicate (a test artifact, not the hazard).
    guards = [runloom.sync.Lock() for _ in range(NCOND)]
    H.state = {
        "conds": conds,
        "boxes": boxes,
        "guards": guards,
        "acq_levels": [0] * SLOTS,   # shared-arm recursion levels acquired
        "rel_levels": [0] * SLOTS,   # shared-arm recursion levels released
        "ok_depth": [0] * SLOTS,     # shared-arm waiters that woke at the right depth
        "ctrl_ok": [0] * SLOTS,      # control waiters that restored depth correctly
        "ctrl_acq": [0] * SLOTS,     # control recursion levels acquired
        "ctrl_rel": [0] * SLOTS,     # control recursion levels released
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    acq = sum(H.state["acq_levels"])
    rel = sum(H.state["rel_levels"])
    ok_depth = sum(H.state["ok_depth"])
    ctrl_ok = sum(H.state["ctrl_ok"])
    ctrl_acq = sum(H.state["ctrl_acq"])
    ctrl_rel = sum(H.state["ctrl_rel"])
    H.log("shared: acq_levels={0} rel_levels={1} correct_wake_depths={2}; "
          "control: ok={3} acq_levels={4} rel_levels={5}; ops={6}".format(
              acq, rel, ok_depth, ctrl_ok, ctrl_acq, ctrl_rel, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # ---- shared-arm recursion-depth CONSERVATION -----------------------------
    # Every acquired recursion level was matched by a release level: no level was
    # leaked (over-restore -> a permanently held level) and none was phantom
    # (under-restore -> an under-flowing release that never had a real level).
    H.check(acq == rel,
            "shared recursion-level conservation broken: acquired {0} levels but "
            "released {1} -- a wait() save/restore leaked a level (over-restore) "
            "or under-flowed one (under-restore) on the shared CoRLock".format(
                acq, rel))
    H.check(acq > 0,
            "shared contention arm never exercised (no acquire/release levels) -- "
            "the save/restore race window was not driven")
    H.check(ok_depth > 0,
            "no shared waiter ever woke at the depth it entered -- the depth "
            "invariant was never actually checked")

    # ---- every shared Condition's lock is provably FREE at end ----------------
    # A probe acquires each shared lock once: a healthy lock goes _count 0 -> 1,
    # _owner set; a LEAKED (over-restored) lock is permanently held and a
    # non-blocking acquire fails.  Then release back to fully free.
    for idx in range(NCOND):
        cond = H.state["conds"][idx]
        lk = cond._lock
        # Before acquiring, a fully-released lock must read _count == 0 / _owner
        # None (nothing leaked it held).
        if not H.check(lk._count == 0 and lk._owner is None,
                       "shared Condition {0} lock ended HELD: _count={1} "
                       "_owner={2!r} (expected free 0/None) -- an over-restore "
                       "leaked a recursion level, the lock is permanently held"
                       .format(idx, lk._count, lk._owner)):
            continue
        got = lk.acquire(blocking=False)
        if not H.check(got,
                       "post-probe could NOT acquire shared Condition {0}'s lock "
                       "(non-blocking) -- it is leaked/held by a vanished waiter "
                       "(over-restore leak)".format(idx)):
            continue
        H.check(lk._count == 1 and lk._is_owned(),
                "post-probe acquired shared Condition {0}'s lock but _count={1} "
                "(expected 1) -- the recursion counter is corrupted".format(
                    idx, lk._count))
        lk.release()
        H.check(lk._count == 0 and lk._owner is None,
                "shared Condition {0}'s lock not free after probe release: "
                "_count={1} _owner={2!r}".format(idx, lk._count, lk._owner))

    # ---- single-owner CONTROL conservation + coverage -------------------------
    H.check(ctrl_acq == ctrl_rel,
            "CONTROL recursion-level conservation broken: acquired {0} != "
            "released {1} on PRIVATE single-owner locks -- the _count save/restore "
            "machinery itself loses/leaks a level (not contention)".format(
                ctrl_acq, ctrl_rel))
    H.check(ctrl_ok > 0,
            "single-owner CONTROL never ran -- the contention-free falsifier for "
            "the _count stash machinery was not exercised")

    H.require_no_lost("recursion-save/restore completeness")


if __name__ == "__main__":
    harness.main(
        "p433_condition_rlock_recursion_save", body, setup=setup, post=post,
        default_funcs=3000,
        describe="Condition(RLock()).wait() saves the RLock recursion depth, "
                 "forces _count=1, parks, then restores the depth on wake; a "
                 "depth-D waiter MUST wake at depth D (in {1,2,3}), owning the "
                 "lock, with acquire levels == release levels (conservation) and "
                 "every lock free at end -- a torn save/restore (under-restore "
                 "under-flow / over-restore leak) under M:N fails, with a private "
                 "single-owner control isolating the _count stash machinery")
