"""big_100 / 416 -- cross-hub cooperative flock(LOCK_EX) lock handoff.

N fibers spread across M:N hubs contend for ONE advisory file lock on a single
shared file.  Each fiber opens its OWN open-file-description on that file and
takes fcntl.flock(fd, LOCK_EX): flock locks are tied to the open file
description, so two independent open()s contend even INSIDE one process -- which
is exactly what makes this a real in-process mutual-exclusion primitive across
fibers (verified: a second open()+flock on the same file from the same process
blocks until the first releases).  Each fiber then loops:

    fcntl.flock(fd, LOCK_EX)      # cooperative-blocking acquire (cross-hub wake)
    <CRITICAL SECTION>           # guarded shared state mutated here
    fcntl.flock(fd, LOCK_UN)      # release -> wakes the next parked acquirer

(We use ONLY flock, not lockf.  POSIX/lockf locks are owned by the PROCESS, not
the open file description, so two fds in the SAME process do NOT contend -- lockf
cannot enforce mutual exclusion between fibers in one process, only across forked
processes.  Exercising lockf in-process would let two "holders" coexist legally
and is the wrong primitive for an in-process cross-fiber lock; flock is the
right one and is what the PRIMITIVE here names.)

runloom monkey-patches fcntl.flock: a blocking LOCK_EX can't be handed to
netpoll (you cannot epoll a file lock), so the cooperative form acquires with
LOCK_NB and, on EWOULDBLOCK/EAGAIN/EACCES contention, PARKS the fiber via a
backoff _co_sleep and retries.  An advisory file lock thus becomes a cross-hub
blocking primitive whose release "wake" is a retry that must win the
non-blocking re-acquire.  Two hazards this drives:

  * LOST RELEASE-WAKE -- the holder unlocks but the next acquirer's parked
    retry never wins / never resumes, stranding it forever.  Caught by
    require_no_lost() (a parked-then-vanished worker is LOST, not merely slow)
    plus a hard conservation count that can't be reached if an acquirer hangs.

  * BROKEN MUTUAL EXCLUSION -- the non-atomic NB-acquire/retry loop, run
    concurrently from many hubs with the GIL off, lets TWO fibers believe they
    each hold the EXCLUSIVE lock at once.  Caught two ways:

      (1) a SENTINEL: `holders` is bumped to 1 on entry and back to 0 on exit,
          all under the lock; any fiber that sees holders != 1 right after its
          own entry bump (i.e. a second concurrent holder) sets a `clobbered`
          flag -- a hard fault.

      (2) CONSERVATION: inside the critical section we do a plain
          `shared_count[0] += 1`.  That read-modify-write is ONLY race-free if
          the lock truly serialises holders; if mutual exclusion breaks, two
          concurrent `+=` lose an increment.  Each worker also tallies its own
          acquisitions into a private per-slot list (race-free).  In post(),
          shared_count[0] must equal the sum of those private tallies -- a lost
          increment (or a torn count from a double holder) makes them diverge.

Falsifiable invariants (hot + post, fail-fast):
  * clobbered flag never set        -> mutual exclusion held every entry
  * shared_count[0] == sum(private) -> no lost / torn increment
  * holder sentinel ends at 0       -> balanced enter/exit, no stranded holder
  * no worker LOST                  -> no dropped release-wake stranded a parker

Stresses: cross-hub cooperative flock handoff, the LOCK_NB-retry park/wake on
release, mutual exclusion of an advisory lock under M:N with the GIL off,
conservation of a lock-guarded counter, lost-release-wake stranding.
"""
import os

import harness
import runloom

try:
    import fcntl
except ImportError:
    fcntl = None

# How many acquire/release cycles a worker attempts per round before it gives
# the round back.  Keeps each round accountable while still HAMMERING the lock
# (the inner loop also stops the instant H.running() goes false).
ACQUIRES_PER_ROUND = 64

# Slot fan-out for race-free per-worker tallies (each worker owns one slot).
NSLOTS = 1024


def critical_section(H, wid, state, slot):
    """Run the guarded mutation under the held flock.  Returns False (and fails)
    the instant the sentinel shows a second concurrent holder."""
    holders = state["holders"]
    shared = state["shared_count"]

    # --- enter: bump the holder sentinel.  Under a correct lock this read-
    #     modify-write is serialised, so holders goes 0 -> 1 and we see 1. ---
    holders[0] += 1
    seen = holders[0]
    if seen != 1:
        # A SECOND fiber is inside the critical section at the same time --
        # mutual exclusion of the advisory lock broke under M:N.
        state["clobbered"][slot] = 1
        H.fail("MUTUAL EXCLUSION BROKEN: holders={0} (>1) inside the flock "
               "critical section -- two fibers hold LOCK_EX at once "
               "(worker {1})".format(seen, wid))
        holders[0] -= 1
        return False

    # --- the conservation increment: race-free ONLY if the lock serialises.
    #     A concurrent second holder turns this into a lost-update window. ---
    shared[0] += 1

    # Widen the window: yield mid-critical-section so any racing entrant that
    # wrongly believes it holds the lock gets a real chance to be scheduled on
    # another hub and trip the sentinel above.
    runloom.yield_now()

    # Re-check the sentinel after the yield: a second holder that slipped in
    # during the yield is just as much a violation.
    seen2 = holders[0]
    if seen2 != 1:
        state["clobbered"][slot] = 1
        H.fail("MUTUAL EXCLUSION BROKEN (post-yield): holders={0} (>1) -- a "
               "second holder entered during the critical-section yield "
               "(worker {1})".format(seen2, wid))
        holders[0] -= 1
        return False

    # --- exit: drop the sentinel back to 0. ---
    holders[0] -= 1
    return True


def worker(H, wid, rng, state):
    if fcntl is None:
        return
    slot = wid & (NSLOTS - 1)
    path = state["path"]
    acq = state["acquires"]            # per-slot race-free tally

    for _ in H.round_range():
        if not H.running():
            break

        # Each round uses a FRESH open file description: a distinct open() is a
        # distinct flock owner, so this worker genuinely contends with every
        # other live worker (and re-opening keeps live fd count ~= live
        # workers, not workers*rounds).
        try:
            fd = os.open(path, os.O_RDWR)
        except OSError:
            # fd exhaustion at extreme over-scale is a benign box limit, not a
            # lock bug; record it and give the round back.
            if not H.running():
                break
            H.note_scale_limit("worker {0}: os.open: fd pressure".format(wid))
            return

        try:
            for i in range(ACQUIRES_PER_ROUND):
                if not H.running():
                    break
                fcntl.flock(fd, fcntl.LOCK_EX)     # cooperative-blocking acquire
                try:
                    ok = critical_section(H, wid, state, slot)
                finally:
                    fcntl.flock(fd, fcntl.LOCK_UN)  # release -> wake next parker
                if not ok:
                    return
                acq[slot] += 1                      # race-free private tally
                H.op(wid)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

        H.task_done(wid)


def setup(H):
    if fcntl is None or not hasattr(fcntl, "flock"):
        # Non-POSIX / no flock (Windows): benign auto-skip, not a fault.
        H.note_scale_limit("no fcntl.flock on this platform -- skipping")
        H.state = {"path": None}
        return
    tmp = H.make_tmpdir(prefix="big100_p416_")
    path = os.path.join(tmp, "flock.lock")
    # Create the shared lock file once (workers open their own fds on it).
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    os.write(fd, b"runloom-flock-handoff")
    os.close(fd)

    H.state = {
        "path": path,
        # Lock-guarded shared state.  Lists are mutable single-element cells so
        # the in-place mutation is visible across hubs; correctness relies on
        # the flock serialising every touch.
        "holders": [0],            # sentinel: # fibers currently inside the CS
        "shared_count": [0],       # the conservation counter (guarded += 1)
        "clobbered": [0] * NSLOTS,  # per-slot: set if a double-holder seen
        "acquires": [0] * NSLOTS,  # per-slot race-free acquisition tally
    }


def body(H):
    if H.state.get("path") is None:
        return
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state.get("path") is None:
        H.log("fcntl.flock unavailable -- nothing to check (benign skip)")
        return
    st = H.state
    total_acq = sum(st["acquires"])
    shared = st["shared_count"][0]
    holders = st["holders"][0]
    clobbered = sum(st["clobbered"])
    H.log("acquisitions={0} shared_count={1} holders_residual={2} "
          "clobbered={3} ops={4}".format(
              total_acq, shared, holders, clobbered, H.total_ops()))

    H.check(total_acq > 0, "no lock acquisitions completed -- the flock handoff "
            "was never exercised")
    # Mutual exclusion: the sentinel must never have tripped.
    H.check(clobbered == 0, "{0} worker-slot(s) saw a SECOND concurrent holder "
            "inside the LOCK_EX critical section -- mutual exclusion of the "
            "advisory lock broke under M:N".format(clobbered))
    # Balance: every enter matched an exit (no stranded holder / lost release).
    H.check(holders == 0, "holder sentinel ended at {0}, not 0 -- unbalanced "
            "enter/exit (a lost release or a stranded holder)".format(holders))
    # Conservation: the guarded counter must equal the total acquisitions.  A
    # diverging value means a lost-update (broken exclusion) or torn count.
    H.check(shared == total_acq, "CONSERVATION VIOLATED: shared_count={0} != "
            "total acquisitions={1} -- a lock-guarded increment was lost/torn "
            "(broken mutual exclusion under M:N)".format(shared, total_acq))
    # Completeness: a dropped release-wake strands the next acquirer -- it parks
    # then vanishes, which is LOST (not merely slow).
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p416_fcntl_flock_handoff", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="N fibers across hubs contend for one flock(LOCK_EX) "
                          "on a shared file; mutual exclusion (sentinel) + "
                          "conservation (count == acquisitions) + no "
                          "lost-release-wake under cooperative cross-hub handoff")
