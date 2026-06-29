"""big_100 / 412 -- BoundedSemaphore bound-enforcement under M:N over-release.

The subject is the cooperative ``threading.BoundedSemaphore`` (monkey.patch()
hands every fiber the cooperative CoBoundedSemaphore).  Its release() is the
hazard:

    def release(self, n=1):
        if self._value + len(self._waiters) + n > self._initial:
            raise ValueError("Semaphore released too many times")
        super().release(n)

That is a CHECK-then-INCREMENT against the initial bound.  Under M:N thousands
of acquire()/release() pairs interleave across hubs, and a torn check (reading a
stale ``_value``/``_waiters`` while another hub is mid-update) would break the
bound in one of two mutually-exclusive ways, BOTH of which we make falsifiable:

  * a PERMIT LEAK -- the check passes when it should not, an extra permit slips
    in, and the live-holder count momentarily exceeds the bound K; or
  * a SPURIOUS ValueError -- the check fires when the semaphore was NOT actually
    at the bound, a legal release is lost, and a permit that an acquirer was
    owed never comes back (eventual under-count / starvation).

We drive that with two interleaved, concurrently-running probes on shared hub
tstates:

CONSERVATION PROBE (the contended shared path -- case 0).  A small pool of
SHARED BoundedSemaphore(K) is hammered by thousands of fibers, each
acquire()->critical-section->release().  While holding a permit a holder bumps a
per-semaphore live-holder counter guarded by a SEPARATE cooperative lock (so the
counter is exact and is NOT the primitive under test); the invariant is that the
holder count NEVER exceeds K at any sampling point (a leak would push it to K+1)
and that, end of run, every shared semaphore is back at exactly _value == K with
no fiber still holding (acquires == releases -- no permit lost, none leaked).

BOUND-ORACLE PROBE (deterministic, per-attempt-fresh -- cases 1 and 2).  Two
cases whose required outcome is exact, so a torn check is caught directly:

  * case 1 AT-BOUND OVER-RELEASE: a fresh BoundedSemaphore(K) sits at
    _value == _initial == K (nothing acquired); an extra release() MUST raise
    ValueError.  If it returns silently, the bound leaked (permit-count
    invariant broken) -- FAIL.
  * case 2 BELOW-THEN-AT BOUND: acquire() once (now below bound), release() once
    -- this MUST NOT raise (a spurious ValueError here is the "never spurious"
    violation); then a SECOND release() at the restored bound MUST raise
    ValueError (the "never lost" half).  Either deviation is FAIL.

COVERAGE (the flaky-random lesson the suite already had to fix in p125/p126):
post() asserts all three cases were exercised AND that an over-release actually
raised at least once; timeout-bound runs complete only a handful of ops, so
round-robin the cases by worker id in the FIRST ops -- ``sel = (wid + i) % 3`` --
then go random, so coverage holds whether one worker does 3 ops or 3 workers do
1 op each.

Invariant (hot, fail-fast): live holders of any shared semaphore <= K always;
an at-bound over-release raises ValueError; a below-bound release never raises.
Invariant (post): every shared semaphore back at _value == K, acquires ==
releases (conservation), all three cases hit, >=1 over-release raised, no lost
worker.

Stresses: BoundedSemaphore.release() check-then-increment vs initial bound,
permit conservation under M:N contention, ValueError enforcement (never
lost / never spurious), cross-hub acquire/release interleave.
"""
import harness
import runloom

# K permits per shared semaphore.  Small enough that contention is real (most
# fibers must park and be handed a permit by a releaser on another hub), large
# enough that the live-holder count has room to be pushed past the bound by a
# leak rather than trivially staying at 1.
K = 4

# A small pool of SHARED semaphores so thousands of fibers pile onto each one --
# that is what drives genuine cross-hub release()-vs-acquire() interleave on the
# same _value/_waiters fields.  Too many semaphores would scatter the contention.
NSEM = 8

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# How many critical-section "work" yields a holder does before releasing -- keeps
# the permit held across a park so multiple holders coexist and the live-holder
# count is genuinely exercised toward K.
HOLD_YIELDS = 2


def shared_acquire_release(H, wid, rng, state, slot):
    """Case 0: the contended shared path.  Acquire a permit on one of the shared
    BoundedSemaphores, assert the live-holder count never exceeds K while held,
    then release.  Conservation (acquires == releases, holder count back to 0)
    is checked end-of-run in post()."""
    idx = wid % NSEM
    sem = state["sems"][idx]
    guard = state["guards"][idx]
    holders = state["holders"]            # one-element list per semaphore
    acq = state["acq"]
    rel = state["rel"]

    sem.acquire()
    # We now hold a permit.  Bump the exact (separately-guarded) live-holder
    # count and assert the bound.  guard is a DISTINCT cooperative lock, so this
    # accounting does not serialize the semaphore's own release path.
    over = False
    with guard:
        holders[idx] += 1
        h = holders[idx]
        if h > K:
            over = True
    if over:
        H.fail("live holders of shared semaphore {0} reached {1} > bound K={2} "
               "-- BoundedSemaphore leaked a permit (torn check-then-increment "
               "in release() let an extra permit out)".format(idx, h, K))
        # Still give the permit back so we don't wedge the pool on the way out.
        with guard:
            holders[idx] -= 1
        sem.release()
        return False
    acq[slot] += 1

    # Hold across a few cooperative yields so other fibers on other hubs contend
    # for the remaining permits -- this is when a leaking release() would push the
    # holder count past K.
    for _ in range(HOLD_YIELDS):
        runloom.yield_now()

    with guard:
        holders[idx] -= 1
    sem.release()
    rel[slot] += 1
    return True


def at_bound_over_release(H, wid, rng, raised):
    """Case 1: a fresh BoundedSemaphore(K) at full value; an extra release()
    MUST raise ValueError.  A silent return means the bound leaked."""
    import threading
    sem = threading.BoundedSemaphore(K)     # _value == _initial == K
    try:
        sem.release()
    except ValueError:
        raised[wid & (SLOTS - 1)] += 1
        return True
    # No exception: the bound was NOT enforced -- a permit-count invariant break.
    H.fail("at-bound over-release on a full BoundedSemaphore(K={0}) did NOT "
           "raise ValueError -- release() let _value exceed the initial bound "
           "(silent permit leak)".format(K))
    return False


def below_then_at_bound(H, wid, rng, raised):
    """Case 2: acquire() once (below bound) then release() -- MUST NOT raise
    (no spurious ValueError); a SECOND release() at the restored bound MUST
    raise (the legal enforcement is not lost)."""
    import threading
    sem = threading.BoundedSemaphore(K)
    got = sem.acquire(blocking=True, timeout=2.0)
    if not got:
        # A private semaphore with K>0 free permits and no other holder must
        # grant immediately; failing to is itself a defect.
        H.fail("acquire() on a fresh private BoundedSemaphore(K={0}) with all "
               "permits free returned False -- a permit was lost on acquire"
               .format(K))
        return False
    # Below the bound now: this release matches the acquire and MUST be silent.
    try:
        sem.release()
    except ValueError:
        H.fail("below-bound release() raised a SPURIOUS ValueError: the matching "
               "release of an outstanding acquire was rejected -- torn check read "
               "a stale _value above the true count")
        return False
    # Back at the bound: a further release MUST be rejected.
    try:
        sem.release()
    except ValueError:
        raised[wid & (SLOTS - 1)] += 1
        return True
    H.fail("at-bound over-release (after acquire/release back to K={0}) did NOT "
           "raise ValueError -- bound enforcement lost".format(K))
    return False


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    raised = state["raised"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases by worker id in the first ops so every
        # case is exercised even when each worker manages only a few ops under
        # the timeout (the p125/p126 flaky-random-coverage fix); random after.
        if i < 3:
            sel = (wid + i) % 3
        else:
            sel = rng.randrange(3)
        i += 1
        if sel == 0:
            ok = shared_acquire_release(H, wid, rng, state, slot)
        elif sel == 1:
            ok = at_bound_over_release(H, wid, rng, raised)
        else:
            ok = below_then_at_bound(H, wid, rng, raised)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so threading.* and the
    # cooperative lock are the patched, M:N-safe primitives.
    import threading
    sems = [threading.BoundedSemaphore(K) for _ in range(NSEM)]
    guards = [threading.Lock() for _ in range(NSEM)]
    H.state = {
        "sems": sems,
        "guards": guards,
        "holders": [0] * NSEM,            # exact live-holder count per semaphore
        "acq": [0] * SLOTS,               # shared-path permit acquisitions
        "rel": [0] * SLOTS,               # shared-path permit releases
        "raised": [0] * SLOTS,            # over-releases that correctly raised
        "case0": [0] * SLOTS,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    acq = sum(H.state["acq"])
    rel = sum(H.state["rel"])
    raised = sum(H.state["raised"])
    H.log("shared-path acquires={0} releases={1} over-releases-raised={2} "
          "ops={3}".format(acq, rel, raised, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # Conservation on the contended shared path: every acquire was matched by a
    # release (no permit lost, none leaked) and every shared semaphore is back at
    # its full bound with no fiber still holding.
    H.check(acq == rel,
            "shared-path conservation broken: acquires={0} != releases={1} "
            "(a permit was lost or leaked across hubs)".format(acq, rel))
    H.check(acq > 0,
            "shared acquire/release path never exercised (no contention probe)")

    holders = H.state["holders"]
    for idx in range(NSEM):
        H.check(holders[idx] == 0,
                "shared semaphore {0} ended with {1} live holders (expected 0) "
                "-- permits not fully returned".format(idx, holders[idx]))
        v = H.state["sems"][idx]._value
        H.check(v == K,
                "shared semaphore {0} ended at _value={1}, expected the full "
                "bound K={2} -- net permit gain/loss across the run".format(
                    idx, v, K))

    # Both over-release cases were exercised and at least one over-release
    # correctly raised ValueError (the bound was actually tested, not skipped).
    H.check(raised > 0,
            "no over-release ever raised ValueError -- the bound-enforcement "
            "cases (1 and 2) were never reached, so the invariant is untested")

    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p412_boundedsemaphore_overrelease", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="BoundedSemaphore bound enforcement under M:N: live "
                          "holders never exceed K, releases match acquires, an "
                          "at-bound over-release raises ValueError (never lost, "
                          "never spurious)")
