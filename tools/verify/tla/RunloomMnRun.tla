------------------------------- MODULE RunloomMnRun -------------------------------
(***************************************************************************)
(* TLA+ model of the runloom_mn_run() main-poll-loop DEADLOCK CENSUS plus the *)
(* STALL-KICK liveness backstop (mn_sched_init_fini.c.inc, runloom_mn_run        *)
(* 937-1072, has_wakeable_work 813-849, stalled_with_runnable 858-874,           *)
(* kick_all_hubs 888-900).  FV coverage gap #2.                                  *)
(*                                                                              *)
(* THE PROTOCOL.  The main thread detaches its tstate and polls every 1 ms.     *)
(*  (1) Termination: pending_global == 0 => all fibers done, return.            *)
(*  (2) STALL-KICK BACKSTOP: if pending hasn't moved AND stalled_with_runnable()*)
(*      -- a QUEUED runnable fiber sits un-run while NO hub is mid-resume -- bump*)
(*      a no-progress streak; at the budget, kick_all_hubs() and reset.  This is *)
(*      INDEPENDENT of the census: a stranded-runnable hang keeps                *)
(*      has_wakeable_work() TRUE (via gQueued / I/O-parked fibers), so the       *)
(*      census never fires; only this backstop recovers it.                     *)
(*  (3) DEADLOCK CENSUS: only when has_wakeable_work() is FALSE, accumulate a    *)
(*      quiet streak; at the threshold declare DEADLOCK.  ANY wakeable work      *)
(*      resets quiet to 0.  So has_wakeable_work() => no deadlock verdict.       *)
(*  (4) kick_all_hubs() fires ALL THREE wake paths for EVERY hub -- the io_uring *)
(*      ring eventfd, idle_cond under idle_lock, and the netpoll pump -- because *)
(*      a hub can be blocked in one of three distinct WAIT MODES and the idle    *)
(*      condvar reaches only ONE.                                               *)
(*                                                                              *)
(* We model ONE stranded-runnable fiber g (queued on gHub, runnable, not run),   *)
(* per-hub wait-mode + idle/resuming state, and the poll-loop bookkeeping.       *)
(*                                                                              *)
(* CONSTANT BuggyKick = TRUE is the documented regression: kick_all_hubs marks a *)
(* hub woken ONLY when its wait mode is "idle" (the old idle_cond-ONLY signal),  *)
(* so a hub stranded in its io_uring RING or in epoll/pump is never recovered -- *)
(* a runnable fiber pinned to it hangs forever (the cov_workload --hubs 4        *)
(* loop-backend hang).  The deadlock census stays blind to this by design        *)
(* (gQueued keeps has_wakeable_work() TRUE), so NoFalseDeadlock still HOLDS even *)
(* under the bug; only EventuallyRun (the stall-kick backstop's liveness) is     *)
(* violated -- a permanent lost wakeup.                                          *)
(***************************************************************************)

EXTENDS Integers

CONSTANTS Hubs,            \* finite set of hub ids, e.g. {h1, h2}
          BuggyKick,       \* TRUE -> kick reaches only WaitMode "idle" (old bug)
          StallKick,       \* consecutive no-progress polls before kick_all_hubs
          DeadlockThresh   \* consecutive quiescent polls before a DEADLOCK verdict

\* Wait modes: how a hub blocks -- the three distinct kick targets.
\*   idle = idle_cond ; ring = io_uring loop_wake_fd ; pump = netpoll pump/epoll.
WaitMode == {"idle", "ring", "pump"}

VARIABLES
    hubState,   \* [Hubs -> {"resuming","idle"}]  resuming = mid-resume (resume_start_ns!=0)
    hubWait,    \* [Hubs -> WaitMode]  which wait the hub is currently blocked in
    hubKicked,  \* [Hubs -> BOOLEAN]   a kick has reached THIS hub in ITS wait mode
    gHub,       \* hub the stranded g is queued/pinned on  (in Hubs)
    gQueued,    \* TRUE: g sits in gHub's ready-ring/deque/sub_head, runnable, not run
    gRun,       \* TRUE: g has been resumed and run to completion
    extWake,    \* BOOLEAN: an external wake source is live (netpoll/blockpool/iouring/foreign/sleeper/timer)
    pending,    \* {0,1}: runloom_mn_pending_global  (1 while g not done)
    lastTotal,  \* last pending value the loop saw  (stall watchdog)
    stall,      \* consecutive no-progress polls
    quiet,      \* consecutive quiescent (no-wakeable-work) polls
    verdict     \* "none" | "deadlock"  -- set only when the census fires

vars == <<hubState, hubWait, hubKicked, gHub, gQueued, gRun,
          extWake, pending, lastTotal, stall, quiet, verdict>>

TypeOK ==
    /\ hubState  \in [Hubs -> {"resuming", "idle"}]
    /\ hubWait   \in [Hubs -> WaitMode]
    /\ hubKicked \in [Hubs -> BOOLEAN]
    /\ gHub      \in Hubs
    /\ gQueued   \in BOOLEAN
    /\ gRun      \in BOOLEAN
    /\ extWake   \in BOOLEAN
    /\ pending   \in {0, 1}
    /\ lastTotal \in {-1, 0, 1}
    /\ stall     \in 0..StallKick
    /\ quiet     \in 0..DeadlockThresh
    /\ verdict   \in {"none", "deadlock"}

----------------------------------------------------------------------------
\* Derived predicates -- the C census functions, transcribed.

\* has_wakeable_work() (813-849): ANY external wake source OR any mid-resume hub
\* OR g still queued (runnable work on a hub).  The SUPERSET.
HasWakeableWork ==
    \/ extWake
    \/ \E h \in Hubs : hubState[h] = "resuming"
    \/ gQueued

\* stalled_with_runnable() (858-874): QUEUED runnable work AND no hub mid-resume.
\* A strict SUBSET of HasWakeableWork (documented by SubsetOK below).
StalledWithRunnable ==
    /\ gQueued
    /\ \A h \in Hubs : hubState[h] # "resuming"

----------------------------------------------------------------------------
Init ==
    /\ hubState  = [h \in Hubs |-> "idle"]
    /\ hubWait   \in [Hubs -> WaitMode]      \* hubs may each block in any wait mode
    /\ hubKicked = [h \in Hubs |-> FALSE]
    /\ gHub      \in Hubs
    /\ gQueued   = TRUE                       \* g is queued runnable from the start
    /\ gRun      = FALSE
    /\ extWake   \in BOOLEAN                  \* allow idle-server (TRUE) AND pure-stranded (FALSE)
    /\ pending   = 1
    /\ lastTotal = -1
    /\ stall     = 0
    /\ quiet     = 0
    /\ verdict   = "none"

\* LIVENESS-teeth Init: the lost wake is PURELY the missed kick -- no external
\* wake source, and gHub blocks in a mode the idle_cond-only kick cannot reach.
\* (Mirrors RunloomMnFini parametrising Buggy: here a dedicated Init pins the
\* stranded-in-ring/pump scenario the kick_all_hubs bug strands.)
InitLive ==
    /\ gHub      \in Hubs
    /\ hubState  = [h \in Hubs |-> "idle"]
    /\ hubWait   \in [Hubs -> WaitMode]
    /\ (hubWait[gHub] \in {"ring", "pump"})  \* gHub stranded in a non-idle wait
    /\ hubKicked = [h \in Hubs |-> FALSE]
    /\ gQueued   = TRUE
    /\ gRun      = FALSE
    /\ extWake   = FALSE                       \* purely the missed kick, no other wake
    /\ pending   = 1
    /\ lastTotal = -1
    /\ stall     = 0
    /\ quiet     = 0
    /\ verdict   = "none"

\* SAFETY-teeth Init: a GENUINE deadlock -- g is parked on a channel/lock/await
\* (NOT queued, NOT runnable), every hub idle, and NO external wake source.  This
\* is the only state shape where ~HasWakeableWork holds with pending=1, so it is
\* the only way CensusTick can reach a real verdict="deadlock".  It exercises
\* NoFalseDeadlock NON-VACUOUSLY: the verdict DOES fire here, and it fires ONLY
\* with ~HasWakeableWork -- exactly the C contract.  (g never runs here, by design:
\* a real deadlock; so EventuallyRun is NOT checked under this cfg.)
InitDeadlock ==
    /\ gHub      \in Hubs
    /\ hubState  = [h \in Hubs |-> "idle"]
    /\ hubWait   \in [Hubs -> WaitMode]
    /\ hubKicked = [h \in Hubs |-> FALSE]
    /\ gQueued   = FALSE                       \* parked on a channel, NOT runnable
    /\ gRun      = FALSE
    /\ extWake   = FALSE                       \* no I/O/timer/foreign wake left
    /\ pending   = 1                           \* the live-but-blocked fiber
    /\ lastTotal = -1
    /\ stall     = 0
    /\ quiet     = 0
    /\ verdict   = "none"

----------------------------------------------------------------------------
\* Next actions.  One loop iteration advances only via these.

\* (A) Loop sees a wake source / mid-resume / queued runnable -> resets the census
\*     counter (else-branch quiet=kicked=reported=0, 1028-1030) and drives the
\*     stall watchdog (980-989): if pending is unchanged AND stalled_with_runnable,
\*     accumulate stall; else record last_total and reset stall.
PollProgress ==
    /\ pending = 1
    /\ HasWakeableWork
    /\ quiet' = 0
    /\ verdict' = "none"
    /\ IF (pending = lastTotal /\ StalledWithRunnable)
         THEN /\ stall' = IF stall < StallKick THEN stall + 1 ELSE stall
              /\ lastTotal' = lastTotal
         ELSE /\ lastTotal' = pending
              /\ stall' = 0
    /\ UNCHANGED <<hubState, hubWait, hubKicked, gHub, gQueued, gRun, extWake, pending>>

\* (B) Census tick when NO wakeable work (991-1027): accumulate quiet; at the
\*     threshold fire the DEADLOCK verdict.  Reachable ONLY under ~HasWakeableWork
\*     -- this is where SAFETY (NoFalseDeadlock) bites.
CensusTick ==
    /\ pending = 1
    /\ ~HasWakeableWork
    /\ quiet' = IF quiet < DeadlockThresh THEN quiet + 1 ELSE quiet
    /\ IF quiet + 1 >= DeadlockThresh
         THEN verdict' = "deadlock"
         ELSE verdict' = "none"
    /\ UNCHANGED <<hubState, hubWait, hubKicked, gHub, gQueued, gRun,
                   extWake, pending, lastTotal, stall>>

\* kick_all_hubs() (888-900) -- the load-bearing piece.  CORRECT: fire all three
\* wake paths, so every hub is woken regardless of wait mode.  BUGGY: idle_cond
\* ONLY -- a hub is woken only if it is blocked in the "idle" wait mode; a hub
\* stranded in its ring or pump is missed.
KickAllHubs ==
    hubKicked' = [h \in Hubs |->
        IF BuggyKick THEN (hubWait[h] = "idle")
                     ELSE TRUE]

\* (C) Stall-kick backstop fires (982-985): the no-progress streak reached the
\*     budget -> kick_all_hubs(); reset the streak.
StallKickFire ==
    /\ pending = 1
    /\ stall >= StallKick
    /\ KickAllHubs                 \* updates hubKicked
    /\ stall' = 0
    /\ UNCHANGED <<hubState, hubWait, gHub, gQueued, gRun,
                   extWake, pending, lastTotal, quiet, verdict>>

\* A hub owning g, once kicked (and idle), wakes -> re-polls its submission list /
\* ring -> resumes g (transitions to mid-resume); g leaves the queue.
HubWakeAndResume ==
    /\ gQueued
    /\ hubKicked[gHub]
    /\ hubState[gHub] = "idle"
    /\ hubState'  = [hubState EXCEPT ![gHub] = "resuming"]
    /\ gQueued'   = FALSE
    /\ UNCHANGED <<hubWait, hubKicked, gHub, gRun, extWake,
                   pending, lastTotal, stall, quiet, verdict>>

\* The resuming hub finishes g -> g done, pending drops to 0, hub returns idle.
HubComplete ==
    /\ hubState[gHub] = "resuming"
    /\ gRun = FALSE
    /\ gRun'      = TRUE
    /\ pending'   = 0
    /\ hubState'  = [hubState EXCEPT ![gHub] = "idle"]
    /\ UNCHANGED <<hubWait, hubKicked, gHub, gQueued, extWake,
                   lastTotal, stall, quiet, verdict>>

\* Terminal: g done (pending=0) -- the loop would break and return.
Done == pending = 0

Next ==
    \/ PollProgress \/ CensusTick \/ StallKickFire
    \/ HubWakeAndResume \/ HubComplete
    \/ (Done /\ UNCHANGED vars)

----------------------------------------------------------------------------
\* Weak fairness on every progress action so a permanently-stranded g is a real
\* liveness lasso, not an unfair stutter.  WF on PollProgress drives the stall
\* counter up to StallKick (pending/lastTotal stabilise at 1 while g is stranded
\* and StalledWithRunnable holds), which enables StallKickFire.
Fairness ==
    /\ WF_vars(PollProgress) /\ WF_vars(CensusTick) /\ WF_vars(StallKickFire)
    /\ WF_vars(HubWakeAndResume) /\ WF_vars(HubComplete)

Spec         == Init         /\ [][Next]_vars /\ Fairness
SpecLive     == InitLive     /\ [][Next]_vars /\ Fairness
SpecDeadlock == InitDeadlock /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: the census NEVER declares deadlock while any wake source / runnable
\* work + a hub exists.  verdict="deadlock" is set only inside CensusTick (guarded
\* by ~HasWakeableWork); PollProgress resets it under HasWakeableWork.  This is
\* exactly has_wakeable_work() => no deadlock verdict (the C invariant: the census
\* never declares deadlock while a parked g can still be woken or runnable work +
\* a hub to run it exists).
NoFalseDeadlock == (verdict = "deadlock") => ~HasWakeableWork

\* AUXILIARY: documents the C subset relation stalled_with_runnable() =>
\* has_wakeable_work().
SubsetOK == StalledWithRunnable => HasWakeableWork

\* LIVENESS: a stranded-runnable g is eventually run -- no permanent lost wakeup.
\* The stall-kick backstop kicks all hubs; the correct (all-mode) kick reaches
\* gHub in whatever wait mode it is in, the hub re-drains, g runs.  Under BuggyKick
\* with gHub stranded in ring/pump (SpecLive), the kick never reaches gHub, so g
\* never runs -> this is VIOLATED (the lost-wakeup teeth).
EventuallyRun == <>(gRun = TRUE)

\* SAFETY-teeth witness: a GENUINE deadlock verdict IS eventually reached under
\* SpecDeadlock.  Checked as a violated property under -deadlock -- the violation
\* (a behaviour reaching verdict="deadlock") demonstrates CensusTick fires the
\* census non-vacuously, so NoFalseDeadlock holding above is meaningful, not vacuous.
DeadlockNeverReached == [](verdict # "deadlock")
=============================================================================
