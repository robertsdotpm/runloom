"""big_100 / 329 -- Barrier phase-agreement + unanimous break under M:N.

`threading.Barrier` (patched -> cooperative, built on CoCondition) is a
*reusable* rendezvous of PARTY members that cycles through phases
("generations"): every phase, all PARTY members must call `wait()` before ANY
of them is released, and the release is unanimous -- the whole group advances
to phase P+1 together or no one does.  Barrier is exercised by ZERO other
programs except p49, and p49 checks ONLY the coarse cross-generation SPREAD
bound (max(cycles)-min(cycles)<=1).  It never asserts the two laws that
actually define a barrier:

  (A) PER-PHASE ARRIVAL == PARTY.  A phase is released ONLY when all PARTY
      members have arrived; so on a clean release every member arrived in that
      phase.  A phantom release (someone advanced past a phase a peer never
      reached) or a missing arrival breaks this -- p49's spread bound cannot
      see a single phase that released with a member absent.
  (B) UNANIMOUS BREAK IN ONE PHASE.  If a member is cancelled / times out
      mid-phase the barrier BREAKS (BrokenBarrierError) and EVERY OTHER member
      waiting on that SAME phase must observe the break -- a survivor must NOT
      silently advance past a phase a peer never completed.  If the wake on
      break is lost, or a survivor advances a phase the others didn't, the
      group DISAGREES and a member is stranded waiting forever on a phase no
      one else is in.  p49 never injects a break at all.

The M:N hazard: the barrier's release / break wake fans out across hubs.  Under
true parallelism a lost release wake leaves a member parked on a phase its
peers already left (disagreement -> hang), and a lost BREAK wake leaves a
survivor parked on the dead phase while its peers have moved on after reset().

Topology (closed-world per worker group, so conservation is EXACT):
  * Each pool worker owns ONE Barrier(PARTY) + a 2-D arrival bitmap
    arrival[phase][member] and a 1-D broke[member] vector.
  * It spawns PARTY member fibers that loop over NPHASES phases in lock-step.
  * Members are WaitGroup-fenced PER PHASE by the barrier itself (no member can
    leave phase P until the whole group has, by definition), and the worker
    joins all PARTY members at the end.

Per-phase member protocol (the ordering that makes the oracle race-free):
  arrival[phase][member] = 1            # set BEFORE wait(): see note below
  idx = barrier.wait(timeout)
  if idx == 0:  (this member is the LEADER of this phase's generation)
      H.check(popcount(arrival[phase]) == PARTY)   # every peer arrived
  ... advance to phase+1 ...

  WHY arrival is set BEFORE wait() and read by the leader AFTER its wait()
  returns: the barrier releases a generation ONLY after all PARTY members have
  entered wait().  So by the time ANY member's wait() returns, all PARTY
  members have already executed the `arrival[...] = 1` line that PRECEDES their
  wait() call.  The leader (idx==0) therefore reads a bitmap whose PARTY bits
  are all guaranteed set -- popcount<PARTY is a REAL missing-arrival / phantom-
  release breach, never a sampling race.  Each member writes only its OWN
  arrival/broke slot (single writer) so the bitmap is race-free.

The injected BREAK (law B), at a designated DEAD_PHASE:
  * the designated victim member (member 0) deliberately does NOT arrive at the
    DEAD_PHASE -- it skips wait() for that phase entirely.
  * the other PARTY-1 members call wait(timeout=BREAK_TIMEOUT); with the victim
    absent the generation can never fill, so EVERY one of them must raise
    BrokenBarrierError for THIS phase.  Each survivor sets broke[member]=1 and
    STOPS (does not advance past DEAD_PHASE).
  * the worker then audits: popcount(broke)==PARTY-1 (every survivor broke,
    unanimously, in the same phase) AND no survivor advanced past DEAD_PHASE
    (clean_phase_reached[member] for the dead phase is never set on a survivor).
    Only after that does the worker barrier.reset() -- a survivor stranded on
    the dead phase (lost break wake) is caught by the audit's shortfall and by
    the watchdog (it never reaches its member-join).

ORACLE (primary -> secondary):
  * PRIMARY  (per clean phase): leader asserts popcount(arrival[phase])==PARTY.
  * PRIMARY  (dead phase): survivors-broke == PARTY-1, unanimous, same phase;
    no survivor advanced past the dead phase.
  * SECONDARY (post): every clean phase released with all PARTY arrivals
    (sum over groups of clean releases == G * (NPHASES-1) * 1 leader-checks),
    every group's break was unanimous, and require_no_lost catches a member
    parked-then-vanished on a phase no peer is in.

Stresses: reusable Barrier generation cycling, unanimous release vs unanimous
break, BrokenBarrierError fan-out wake across hubs, reset()/resync choreography,
per-phase arrival conservation, no lost release/break wake.

Good TSan / controlled-M:N-replay target: the barrier's internal CoCondition
notify_all on release / break races the cross-hub member parks; a data-race on
the generation count or the broken flag is often the first signal before the
arrival/break conservation oracle fires.
"""
import threading      # patched -> cooperative Barrier (built on CoCondition)

import harness
import runloom

PARTY = 16                # members per barrier group (small, like p49)
NPHASES = 6               # phases each group cycles through before the break
DEAD_PHASE = 4            # the phase whose generation we deliberately break
VICTIM = 0                # the member that skips arriving at DEAD_PHASE
# Survivors wait this long at the dead phase; the victim never arrives, so the
# generation can never fill and every survivor must time out -> BrokenBarrier.
# Small so the injected break is fast; larger than a clean phase's rendezvous
# so a survivor that legitimately could have met never times out spuriously.
BREAK_TIMEOUT = 0.4
# Generous ceiling for a CLEAN phase rendezvous: dwarfs the time PARTY members
# need to all reach wait().  A clean-phase timeout here would itself be a fault
# (the group could not rendezvous within a huge window) -- surfaced as a break
# the leader-arrival oracle would then flag as a phantom on the clean phase.
CLEAN_TIMEOUT = 30.0


def member(H, gslot, mid, barrier, arrival, broke, advanced, state):
    """One barrier member.  Loops phases 0..NPHASES-1 in lock-step with PARTY-1
    peers.  Writes only its OWN arrival[phase][mid] / broke[mid] / advanced[mid]
    slots (single writer -> race-free).  The leader (wait()==0) of each clean
    phase audits per-phase arrival conservation.

    At DEAD_PHASE the VICTIM skips arriving (forces the break); every survivor
    must catch BrokenBarrierError for that phase and STOP without advancing."""
    for phase in range(NPHASES):
        if not H.running():
            return
        if phase == DEAD_PHASE:
            if mid == VICTIM:
                # The cancelled / timed-out member: do NOT arrive.  Just leave
                # -- the absent arrival is what breaks the generation for the
                # PARTY-1 survivors.  (We still recorded NO arrival for this
                # phase, which is correct: the victim never reached it.)
                return
            # Survivor: wait on a generation that can never fill -> must break.
            arrival[phase][mid] = 1          # we DID arrive at the dead phase
            try:
                barrier.wait(timeout=BREAK_TIMEOUT)
            except threading.BrokenBarrierError:
                broke[mid] = 1               # observed the break, this phase
                return                       # STOP -- do not advance past it
            except Exception as exc:         # noqa: BLE001
                H.fail("group {0} member {1}: unexpected {2} at dead phase "
                       "(expected BrokenBarrierError)".format(
                           gslot, mid, type(exc).__name__))
                return
            # Reaching here means wait() RETURNED at the dead phase -- a phantom
            # release of a generation the victim never joined.
            H.fail("group {0} member {1}: barrier RELEASED at DEAD_PHASE {2} "
                   "with the victim absent (phantom release -- a survivor "
                   "advanced past a phase a peer never reached)".format(
                       gslot, mid, DEAD_PHASE))
            return

        # ---- a clean phase ----
        arrival[phase][mid] = 1              # set BEFORE wait() (see module doc)
        try:
            idx = barrier.wait(timeout=CLEAN_TIMEOUT)
        except threading.BrokenBarrierError:
            # A clean phase must NOT break: every member arrives.  A break here
            # is a lost release wake / spurious break -> disagreement.
            H.fail("group {0} member {1}: BrokenBarrierError on CLEAN phase "
                   "{2} (lost release wake / spurious break -- the group "
                   "disagreed on a phase everyone reached)".format(
                       gslot, mid, phase))
            return
        except Exception as exc:             # noqa: BLE001
            H.fail("group {0} member {1}: unexpected {2} on clean phase {3}"
                   .format(gslot, mid, type(exc).__name__, phase))
            return
        advanced[mid] = phase + 1            # highest phase this member cleared
        if idx == 0:
            # LEADER of this generation.  By the barrier contract every PARTY
            # member entered wait() before any was released, so every member's
            # arrival[phase][.] = 1 (which PRECEDES its wait()) has run.  A
            # popcount < PARTY here is a REAL missing arrival / phantom release.
            arrived = sum(arrival[phase])
            if not H.check(
                    arrived == PARTY,
                    "group {0} clean phase {1} RELEASED with only {2}/{3} "
                    "arrivals (a member advanced past a phase a peer never "
                    "reached -- phantom release / lost arrival)".format(
                        gslot, phase, arrived, PARTY)):
                return
            state["clean_releases"][gslot & 1023] += 1
            H.op(gslot)
    # A member that completed all phases without hitting DEAD_PHASE's break path
    # only happens if DEAD_PHASE >= NPHASES (it isn't); included for safety.


def worker(H, wid, rng, state):
    """One closed-world barrier group: Barrier(PARTY) + PARTY members cycling
    NPHASES phases, with an injected break at DEAD_PHASE.  Joins all members,
    then audits unanimous break + (already-checked) per-phase arrival, resets
    the barrier and folds the group's results into the shared accounting."""
    gslot = wid
    for _ in H.round_range():
        if not H.running():
            break

        barrier = threading.Barrier(PARTY)
        # arrival[phase][member]: single-writer per (phase, member) slot.
        arrival = [[0] * PARTY for _ in range(NPHASES)]
        broke = [0] * PARTY                  # survivor observed the break
        advanced = [0] * PARTY               # highest phase each member cleared

        wg = runloom.WaitGroup()
        wg.add(PARTY)

        def run_member(mid):
            try:
                member(H, gslot, mid, barrier, arrival, broke, advanced, state)
            finally:
                wg.done()

        for mid in range(PARTY):
            H.fiber(run_member, mid)

        wg.wait()                            # every member returned (or watchdog)

        if not H.running():
            break

        # ---- LAW B audit: unanimous break in the dead phase ----
        survivors_broke = sum(broke)
        # Every survivor (all but the absent victim) must have caught the break
        # for DEAD_PHASE -- unanimous, same phase.  A shortfall = a survivor
        # whose break wake was lost (it would have hung the wg.wait above; if it
        # somehow returned without breaking, that is disagreement) or a survivor
        # that wrongly advanced past the dead phase.
        if not H.check(
                survivors_broke == PARTY - 1,
                "group {0}: only {1}/{2} survivors observed BrokenBarrier at "
                "DEAD_PHASE {3} (non-unanimous break -- a survivor advanced "
                "past or lost the break wake)".format(
                    gslot, survivors_broke, PARTY - 1, DEAD_PHASE)):
            return
        # No member may have advanced PAST the dead phase (advanced records the
        # highest clean phase cleared; nobody clears DEAD_PHASE, so the max must
        # be DEAD_PHASE, i.e. they cleared phases 0..DEAD_PHASE-1 only).
        if not H.check(
                max(advanced) <= DEAD_PHASE,
                "group {0}: a member advanced to phase {1} > DEAD_PHASE {2} "
                "(silently advanced past the broken phase)".format(
                    gslot, max(advanced), DEAD_PHASE)):
            return

        # The dead phase's arrival bitmap: exactly the PARTY-1 survivors arrived
        # (the victim never did).  A phantom arrival here would mean a survivor's
        # slot was written by the wrong fiber.
        dead_arrivals = sum(arrival[DEAD_PHASE])
        if not H.check(
                dead_arrivals == PARTY - 1,
                "group {0}: DEAD_PHASE {1} arrival bitmap has {2} bits, expected "
                "PARTY-1={3} (phantom / cross-member arrival)".format(
                    gslot, DEAD_PHASE, dead_arrivals, PARTY - 1)):
            return

        # Reset the (now broken) barrier -- the resync step a real consumer does
        # after a break.  reset() must clear the broken state.
        barrier.reset()
        if not H.check(
                not barrier.broken,
                "group {0}: barrier.reset() left broken={1} (resync would "
                "strand the survivors)".format(gslot, barrier.broken)):
            return

        # Fold per-group conservation into shared accounting.
        state["groups_done"][gslot & 1023] += 1
        state["survivors_broke"][gslot & 1023] += survivors_broke
        H.task_done(gslot)


def setup(H):
    H.state = {
        # Number of clean-phase leader checks that passed (one per clean phase
        # per group): the secondary coverage signal.
        "clean_releases": [0] * 1024,
        "groups_done": [0] * 1024,
        "survivors_broke": [0] * 1024,
    }


def body(H):
    # One worker == one barrier group; each spawns PARTY members from inside the
    # root.  Cap groups so PARTY*funcs live fibers stay in the design tier (the
    # break choreography is genuinely per-group L-effort).
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    groups = sum(H.state["groups_done"])
    clean = sum(H.state["clean_releases"])
    broke = sum(H.state["survivors_broke"])
    H.log("groups_done={0} clean_phase_releases={1} survivor_breaks={2} "
          "(PARTY={3} NPHASES={4} DEAD_PHASE={5})".format(
              groups, clean, broke, PARTY, NPHASES, DEAD_PHASE))

    H.check(groups > 0, "no barrier group completed (test did no work)")
    # Every completed group released exactly DEAD_PHASE clean phases (0..DEAD-1)
    # with full arrival, and broke unanimously at DEAD_PHASE.  Coverage check:
    # clean releases == groups * DEAD_PHASE (each group's leader passed the
    # per-phase arrival==PARTY check for phases 0..DEAD_PHASE-1).
    if groups > 0:
        H.check(clean == groups * DEAD_PHASE,
                "clean-phase coverage broken: {0} leader arrival-checks passed "
                "but expected groups*DEAD_PHASE = {1}*{2} = {3} (a phase "
                "released without a full-arrival leader check)".format(
                    clean, groups, DEAD_PHASE, groups * DEAD_PHASE))
        # Every completed group's break was unanimous (PARTY-1 survivors).
        H.check(broke == groups * (PARTY - 1),
                "unanimous-break coverage broken: total survivor breaks {0} != "
                "groups*(PARTY-1) = {1}*{2} = {3} (a group's break was not "
                "unanimous)".format(
                    broke, groups, PARTY - 1, groups * (PARTY - 1)))
    # A member parked-then-vanished on a phase no peer is in == a lost release /
    # break wake -> disagreement.  This is the completeness backstop.
    H.require_no_lost("barrier phase-agreement completeness")


if __name__ == "__main__":
    harness.main("p329_barrier_phase_agreement", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=20000,
                 describe="reusable Barrier phase agreement: every clean phase "
                          "releases with arrival==PARTY (per-phase bitmap), an "
                          "injected break is unanimous in ONE phase (PARTY-1 "
                          "survivors broke, none advanced past it), then reset")
