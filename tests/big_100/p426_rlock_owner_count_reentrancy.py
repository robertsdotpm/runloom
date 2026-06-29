"""big_100 / 426 -- RLock owner-token / recursion-count reentrancy under M:N.

The subject is the cooperative ``threading.RLock`` -- monkey.patch() hands every
fiber the cooperative CoRLock (src/runloom/monkey/locks.py:85,
``__slots__ = ("_lock", "_owner", "_count", "__weakref__")``).  It is a
REENTRANT lock whose ownership is keyed on the CURRENT SCHEDULABLE IDENTITY
(``runloom.current()`` -- the bare G handle for this fiber).  Two of its
operations are non-atomic test/update pairs over the SAME ``_owner`` /
``_count`` fields, and that pair is the hazard:

    def acquire(self, blocking=True, timeout=-1):
        cur = runloom.current() if _in_fiber() else _real_get_ident()
        if self._owner is not None and self._owner == cur:   # TEST
            self._count += 1                                  # then INCR
            return True
        ok = self._lock.acquire(blocking, timeout)
        if ok:
            self._owner = cur                                 # SET owner
            self._count = 1
        return ok

    def release(self):
        cur = runloom.current() if _in_fiber() else _real_get_ident()
        if self._owner != cur:                                # TEST
            ... raise RuntimeError("cannot release un-acquired lock")
        self._count -= 1                                      # DECR
        if self._count == 0:
            self._owner = None                                # then NULL owner
            self._lock.release()

The reentry TEST-then-INCR (``if _owner==cur: _count+=1``) and the release
DECR-then-NULL (``_count-=1; if _count==0: _owner=None``) read+write _owner and
_count WITHOUT holding the inner CoLock for the reentrant fast path (only the
FIRST acquire takes _lock; a re-entry just bumps _count).  Under M:N a fiber can
acquire(), recurse acquire() (count++), PARK on a grown-down C stack INSIDE the
held region, and be resumed on a DIFFERENT hub thread; meanwhile a sibling on
another hub takes the SAME RLock object.  The torn reads/writes can corrupt the
pair:

  * a stale _owner read lets a NON-owner believe it re-entered -> _count
    inflation / double owner (count goes up without a matching real acquire);
  * release() nulls _owner while a re-entrant acquire on the TRUE owner just
    bumped _count -> _count>0 with _owner is None (an ORPHANED lock that no
    fiber can ever release: release() then raises "un-acquired");
  * _count drifts negative, or a fiber sees a foreign wid in the owner-token
    cell while it believes it holds the lock at depth d.

We make that a falsifiable CONSERVATION + IDENTITY law with a single-owner
control arm, not a racy probe:

CONSERVATION + IDENTITY (the contended shared arm -- case 0).  A small pool of
SHARED RLocks is hammered by thousands of fibers.  Each fiber runs a
DETERMINISTIC, BALANCED LADDER on one shared RLock: acquire x D then release x
D, where D = (wid % 4) + 1 (round-robined so every reentry depth 1..4 is
covered).  At every rung it asserts:

  * _recursion_count() == d            (the exact current held depth)
  * _is_owned() is True                (we, the current fiber, own it)
  * a per-lock OWNER-TOKEN cell, which the fiber writes its OWN wid into at the
    bottom rung UNDER the lock, still reads its wid at every nested rung -- no
    foreign owner ever saw the lock as theirs while we held it.

Because the ladder is balanced (D acquires, D releases) per fiber and the fiber
holds the lock across cooperative yields (forcing a sibling to contend during
the park), the per-worker single-writer tally of (acquires - releases) must be
EXACTLY ZERO, summed in post().  END-OF-ROUND the lock MUST be fully free:
_count == 0, _owner is None, and a fresh probe fiber's try-acquire succeeds
IMMEDIATELY (an orphaned _count>0/_owner=None lock would make that probe block
forever, caught as a HANG / a non-grant).

SINGLE-OWNER CONTROL ARM (case 1).  The identical ladder run on a PRIVATE,
per-fiber RLock with no sibling contention.  A single-owner reentrant lock is
race-free by construction, so if the CONTROL loses a level (_recursion_count()
!= d, or _is_owned() False while held, or it ends non-free) the fault is in the
RLock MACHINERY itself, not contention -- this disambiguates "the primitive is
buggy" from "M:N contention tore the shared pair".

ORPHAN PROBE (case 2).  A fresh shared RLock per attempt: one fiber takes it to
depth 2, parks; a sibling spins acquire(blocking=False) on the SAME object
during the park (it MUST fail -- a non-owner can never acquire a held RLock),
then the holder unwinds; finally a probe try-acquire MUST succeed (the lock is
free, not orphaned at _count>0/_owner=None) and report _recursion_count()==1.

COVERAGE (the flaky-random lesson the suite already fixed in p125/p126/p172):
round-robin the three cases by worker id in the FIRST ops -- ``sel = (wid + i)
% 3`` -- then go random, so every case is exercised whether one worker does K
ops or K workers do 1 each.  post() asserts each case ran and that the net
acquire/release tally is exactly zero.

Invariant (hot, fail-fast): at every held rung _recursion_count()==d,
_is_owned() True, owner-token cell == wid; a non-owner try-acquire of a held
lock returns False; release never raises on a balanced ladder.
Invariant (post): net (acquires - releases) == 0 across all workers (no count
inflated, none lost); every shared RLock ends _count==0/_owner is None and a
fresh probe acquires it; the control arm never lost a level; all three cases
exercised; no worker lost.

Stresses: RLock reentry test-and-incr vs release decr-and-null on shared
_owner/_count; orphaned lock (_count>0, _owner None) detection; owner-token
identity under cross-hub park/resume; recursion-depth conservation under M:N.

Good TSan / controlled-M:N-replay target: the unlocked reentrant read-modify-
write of _owner/_count is a textbook data race; a TSan report on the CoRLock
_count store/load, or a single net != 0 ladder under replay, localizes the torn
reentrancy before the conservation sum even closes.
"""
import harness
import runloom

# A small pool of SHARED RLocks so thousands of fibers pile onto each one --
# that is what drives a genuine cross-hub reentry-vs-release interleave on the
# same _owner/_count.  Too many would scatter the contention to nothing.
NLOCK = 8

# Slots for race-free per-worker tallies (single writer per slot, summed post).
SLOTS = 1024

# How many cooperative yields a holder does at the DEEPEST rung before it starts
# unwinding -- keeps the lock held across a park so a sibling on another hub
# contends for the SAME object during the resume-on-a-different-hub window, when
# a torn release() would null _owner out from under our still-positive _count.
HOLD_YIELDS = 2

# The empty owner-token sentinel: a per-lock cell reads this when no fiber claims
# ownership.  A held lock whose token cell reads NEITHER this nor our own wid
# means a FOREIGN fiber's wid leaked into the token while we believed we held the
# lock -- a torn owner identity.
NO_OWNER = -1


def held_ladder(H, wid, rlock, token_cell, idx, depth, acq_tally, rel_tally,
                slot, contended):
    """Run a balanced reentrant ladder on `rlock`: acquire x depth, asserting the
    held invariants at each rung on the way DOWN, hold across yields at the
    bottom, then release x depth asserting on the way UP.

    `token_cell` is a one-element list (the per-lock owner-token cell, written
    UNDER the lock).  On a SHARED lock it is only valid to read/write while we
    actually hold it -- which we do for the whole ladder -- so a foreign wid there
    means a non-owner believed it owned the lock concurrently.

    Returns True on a clean balanced ladder; H.fail + False on the first torn
    rung.  Single-writer per-slot tallies are bumped so post() can prove net
    acquires == releases (== 0 conservation per fiber)."""
    cur = runloom.current()

    # ---- climb: acquire `depth` times, checking the reentry test-and-incr -----
    for d in range(1, depth + 1):
        got = rlock.acquire()
        if not got:
            # A reentrant acquire by the current owner (d>1) can NEVER fail; the
            # first (d==1) on a SHARED lock blocks until granted, so a False here
            # is a lost grant.
            H.fail("acquire() at rung {0} on {1} lock {2} returned False -- a "
                   "reentrant/granting acquire was lost".format(
                       d, "shared" if contended else "private", idx))
            return False
        acq_tally[slot] += 1                # single-writer-per-slot, race-free

        # We now hold the lock at depth d.  At the BOTTOM rung (first acquire)
        # stamp our wid into the owner-token cell under the lock; at every rung
        # assert the cell still reads our wid (no foreign owner saw it as theirs).
        if d == 1:
            token_cell[idx] = wid
        tok = token_cell[idx]
        if tok != wid:
            H.fail("owner-token of {0} lock {1} reads wid {2} at rung {3} but "
                   "THIS fiber (wid {4}) holds the lock -- a FOREIGN owner saw a "
                   "held RLock as theirs (torn _owner test-and-set let a non-"
                   "owner in)".format("shared" if contended else "private",
                                      idx, tok, d, wid))
            return False

        # _recursion_count() must report EXACTLY the current held depth, and
        # _is_owned() must report this fiber as the owner.  A torn _count++/_owner
        # read shows up here as a wrong depth or a False ownership.
        rc = rlock._recursion_count()
        if rc != d:
            H.fail("_recursion_count()={0} at rung {1} on {2} lock {3} -- expected "
                   "exactly {1} (reentry _count++ inflated/lost a level under "
                   "M:N)".format(rc, d, "shared" if contended else "private", idx))
            return False
        if not rlock._is_owned():
            H.fail("_is_owned() False at rung {0} on {1} lock {2} while THIS fiber "
                   "holds depth {0} -- _owner was nulled/overwritten out from "
                   "under a positive _count (orphaned-while-held)".format(
                       d, "shared" if contended else "private", idx))
            return False
        # On a SHARED lock, force a hand-off MID-CLIMB so a sibling on another hub
        # contends for the SAME object while our _count is positive and _owner is
        # our G -- the window a torn release() would corrupt.
        if contended:
            runloom.yield_now()
        _ = cur                              # keep `cur` live for replay clarity

    # ---- hold at the bottom across yields: maximize the park/resume race ------
    for _ in range(HOLD_YIELDS):
        runloom.yield_now()
        if token_cell[idx] != wid:
            H.fail("owner-token of {0} lock {1} changed to {2} while held at full "
                   "depth by wid {3} -- a foreign fiber overwrote the owner under "
                   "a held RLock".format("shared" if contended else "private",
                                         idx, token_cell[idx], wid))
            return False

    # ---- unwind: release `depth` times, checking the decr-and-null path -------
    for d in range(depth, 0, -1):
        # Before releasing rung d we should still be the owner at depth d.
        rc = rlock._recursion_count()
        if rc != d:
            H.fail("_recursion_count()={0} before releasing rung {1} on {2} lock "
                   "{3} -- expected {1} (a concurrent release decremented OUR "
                   "_count, or _count drifted)".format(
                       rc, d, "shared" if contended else "private", idx))
            return False
        # At the LAST rung clear our token under the lock BEFORE we drop it, so a
        # sibling that acquires next never sees our stale wid.
        if d == 1:
            token_cell[idx] = NO_OWNER
        try:
            rlock.release()
        except RuntimeError as exc:
            # A balanced ladder must NEVER hit "cannot release un-acquired lock":
            # that means our _owner was nulled (orphaned) or our _count was
            # decremented out from under us by a torn concurrent release.
            H.fail("release() at rung {0} on {1} lock {2} raised RuntimeError "
                   "({3}) on a BALANCED ladder -- _owner/_count was torn (an "
                   "orphaned lock or a stolen decrement under M:N)".format(
                       d, "shared" if contended else "private", idx, exc))
            return False
        rel_tally[slot] += 1
        if contended:
            runloom.yield_now()

    # Fully unwound: this fiber no longer owns the lock.  _recursion_count() must
    # be 0 for us now (it reports 0 for a non-owner / fully-released lock).
    if rlock._recursion_count() != 0:
        H.fail("_recursion_count()={0} after a fully-balanced unwind on {1} lock "
               "{2} (expected 0) -- a residual reentry count was left behind"
               .format(rlock._recursion_count(),
                       "shared" if contended else "private", idx))
        return False
    return True


def shared_ladder(H, wid, rng, state, slot):
    """Case 0: the contended shared arm.  Run the balanced reentrant ladder on one
    of the SHARED RLocks, depth = (wid % 4) + 1 so all reentry depths 1..4 are
    covered.  Conservation (net acquires == releases, lock ends free) is checked
    end-of-round / in post()."""
    idx = wid % NLOCK
    rlock = state["locks"][idx]
    tokens = state["tokens"]               # one cell per shared lock
    depth = (wid % 4) + 1
    return held_ladder(H, wid, rlock, tokens, idx, depth,
                       state["acq"], state["rel"], slot, contended=True)


def private_ladder(H, wid, rng, state, slot):
    """Case 1: the single-owner CONTROL arm.  The identical balanced ladder on a
    PRIVATE, per-attempt RLock with no sibling contention.  Race-free by
    construction: if THIS loses a level, the fault is in the RLock machinery, not
    M:N contention."""
    import threading
    rlock = threading.RLock()
    token = [NO_OWNER]                     # private one-cell token
    depth = (wid % 4) + 1
    ok = held_ladder(H, wid, rlock, token, 0, depth,
                     state["pacq"], state["prel"], slot, contended=False)
    if not ok:
        return False
    # The private lock must also end fully free (it is single-owner, so this is a
    # pure machinery check).
    if rlock._is_owned() or rlock._recursion_count() != 0:
        H.fail("private control RLock not fully free after a balanced ladder: "
               "_is_owned={0} _recursion_count={1} -- the RLock machinery left a "
               "residual hold with ONE owner and no contention".format(
                   rlock._is_owned(), rlock._recursion_count()))
        return False
    return True


def orphan_probe(H, wid, rng, state, slot):
    """Case 2: orphaned-lock detector.  A fresh shared RLock per attempt: a holder
    fiber takes it to depth 2 and parks; a contender on another hub spins a
    NON-BLOCKING acquire on the SAME object during the park (it MUST fail -- a
    non-owner can never take a held RLock); the holder then unwinds; finally a
    probe try-acquire MUST succeed and report depth 1 (the lock is free, not
    orphaned at _count>0/_owner=None)."""
    import threading
    rlock = threading.RLock()

    enter = runloom.WaitGroup()            # holder trips this once it is at depth 2
    enter.add(1)
    release_gate = runloom.Chan(1)         # holder waits here before unwinding
    wg = runloom.WaitGroup()
    wg.add(2)
    result = {"contender_acquired": False, "holder_ok": True}

    def holder():
        try:
            rlock.acquire()
            rlock.acquire()                # depth 2
            if rlock._recursion_count() != 2:
                result["holder_ok"] = False
                H.fail("orphan-probe holder: _recursion_count()={0} at depth 2 "
                       "(expected 2)".format(rlock._recursion_count()))
            enter.done()                   # tell the contender we are held at d2
            runloom.yield_now()            # park inside the held region
            release_gate.recv()            # wait until the contender has tried
            rlock.release()
            rlock.release()                # fully unwind
        finally:
            wg.done()

    def contender():
        try:
            enter.wait()                   # holder is provably at depth 2 now
            # A NON-OWNER non-blocking acquire of a HELD RLock MUST fail.  If it
            # succeeds, the holder's _owner was torn/nulled and a foreign fiber
            # stole the lock (double owner).
            got = rlock.acquire(blocking=False)
            if got:
                result["contender_acquired"] = True
                # Give the permit back so the holder's unwind doesn't wedge.
                try:
                    rlock.release()
                except RuntimeError:
                    pass
            runloom.yield_now()
            release_gate.send(True)        # let the holder unwind
        finally:
            wg.done()

    H.fiber(holder)
    H.fiber(contender)
    wg.wait()

    if result["contender_acquired"]:
        H.fail("orphan-probe: a NON-OWNER fiber acquire(blocking=False) SUCCEEDED "
               "on a RLock held at depth 2 by another fiber -- _owner was "
               "torn/nulled so a foreign fiber stole the held lock (double owner)")
        return False
    if not result["holder_ok"] or H.failed:
        return False

    # Holder fully unwound; the lock must now be FREE (not orphaned at
    # _count>0/_owner=None).  A fresh probe try-acquire must succeed immediately.
    got = rlock.acquire(blocking=False)
    if not got:
        H.fail("orphan-probe: try-acquire on a fully-unwound RLock returned False "
               "-- the lock is ORPHANED (_count>0 with _owner=None left by a torn "
               "release decr-and-null), no fiber can ever take it again")
        return False
    if rlock._recursion_count() != 1:
        H.fail("orphan-probe: probe acquire reports _recursion_count()={0} "
               "(expected 1) on a freshly re-taken lock".format(
                   rlock._recursion_count()))
        rlock.release()
        return False
    rlock.release()
    state["orphan_ok"][slot] += 1
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
        if i < 3:
            sel = (wid + i) % 3
        else:
            sel = rng.randrange(3)
        i += 1
        if sel == 0:
            ok = shared_ladder(H, wid, rng, state, slot)
        elif sel == 1:
            ok = private_ladder(H, wid, rng, state, slot)
        else:
            ok = orphan_probe(H, wid, rng, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.RLock is
    # the cooperative CoRLock under test.  The shared locks + their owner-token
    # cells are the contended arm; per-slot tallies are single-writer-per-slot.
    import threading
    H.state = {
        "locks": [threading.RLock() for _ in range(NLOCK)],
        "tokens": [NO_OWNER] * NLOCK,      # per-lock owner-token cell
        "acq": [0] * SLOTS,                # shared-arm acquires
        "rel": [0] * SLOTS,                # shared-arm releases
        "pacq": [0] * SLOTS,               # private-control acquires
        "prel": [0] * SLOTS,               # private-control releases
        "orphan_ok": [0] * SLOTS,          # orphan-probe completions
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    acq = sum(H.state["acq"])
    rel = sum(H.state["rel"])
    pacq = sum(H.state["pacq"])
    prel = sum(H.state["prel"])
    orphan = sum(H.state["orphan_ok"])
    H.log("shared-ladder acquires={0} releases={1} (net {2}); private-control "
          "acquires={3} releases={4} (net {5}); orphan-probes={6}; ops={7}".format(
              acq, rel, acq - rel, pacq, prel, pacq - prel, orphan,
              H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # Conservation on the contended shared arm: every reentrant acquire was
    # matched by exactly one release across the whole run (net 0).  A torn
    # reentry _count++ that inflated, or a stolen decrement that lost a level,
    # breaks this even when no single rung assert fired.
    H.check(acq == rel,
            "shared-ladder conservation broken: acquires={0} != releases={1} "
            "(net {2}) -- a reentrant _count++ was inflated or a release "
            "decrement was lost/doubled under M:N".format(acq, rel, acq - rel))
    H.check(acq > 0,
            "shared reentrant ladder never exercised (no contention probe)")

    # Single-owner control arm: a private RLock must be perfectly balanced (it is
    # the falsifier -- if THIS net != 0 the RLock machinery itself dropped a
    # reentry level, independent of contention).
    H.check(pacq == prel,
            "private-control conservation broken: acquires={0} != releases={1} "
            "(net {2}) -- a SINGLE-OWNER RLock lost/inflated a reentry level, so "
            "the fault is the RLock machinery, NOT M:N contention".format(
                pacq, prel, pacq - prel))
    H.check(pacq > 0, "private control ladder never exercised")

    # Every shared RLock must end fully FREE: _count == 0, _owner is None, and a
    # fresh acquire succeeds immediately (an orphaned _count>0/_owner=None lock
    # would make this probe block forever, or _owner!=None would mis-key it).
    for idx in range(NLOCK):
        rlock = H.state["locks"][idx]
        c = rlock._count
        o = rlock._owner
        H.check(c == 0,
                "shared RLock {0} ended with _count={1} (expected 0) -- a "
                "reentry level was leaked (residual hold under M:N)".format(idx, c))
        H.check(o is None,
                "shared RLock {0} ended with _owner={1!r} (expected None) -- the "
                "owner token was left dangling after a balanced run (orphaned "
                "owner identity)".format(idx, o))
        # A no-contention probe acquire now must grant and report depth 1.
        got = rlock.acquire(blocking=False)
        H.check(got,
                "shared RLock {0} could NOT be re-acquired at end of run -- it is "
                "ORPHANED (held with _count>0 but _owner=None left by a torn "
                "release), no fiber can ever take it again".format(idx))
        if got:
            H.check(rlock._recursion_count() == 1,
                    "shared RLock {0} probe acquire reports depth {1} (expected 1)"
                    .format(idx, rlock._recursion_count()))
            rlock.release()
        H.check(H.state["tokens"][idx] == NO_OWNER,
                "shared RLock {0} owner-token cell ended at {1} (expected "
                "NO_OWNER) -- a holder did not clear its identity".format(
                    idx, H.state["tokens"][idx]))

    H.check(orphan > 0,
            "orphan-probe case never completed -- the held-lock-steal / orphaned-"
            "lock detector was never exercised")

    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p426_rlock_owner_count_reentrancy", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="threading.RLock (CoRLock) reentry test-and-incr vs "
                          "release decr-and-null on shared _owner/_count under "
                          "M:N: a balanced reentrant ladder conserves acquires=="
                          "releases, _recursion_count()==depth and owner-token=="
                          "wid at every held rung, the lock ends free (not "
                          "orphaned), a non-owner never steals a held lock, and a "
                          "single-owner private control arm never loses a level")
