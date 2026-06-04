--------------------------- MODULE RunloomMNControl ---------------------------
(***************************************************************************)
(* TLA+ model of runloom's CONTROLLED M:N scheduler (RUNLOOM_MN_SEED, the        *)
(* tools/mn_controlled experiment): an execution "baton" serializes        *)
(* goroutine execution across hubs, a seeded controller hands it off, and  *)
(* a barrier rendezvous pins the requester set so the choice is a function *)
(* of the schedule (toward deterministic replay).                          *)
(*                                                                         *)
(* This models the two properties that matter for the implementation:      *)
(*                                                                         *)
(*  - MutualExclusion (safety): at most one hub holds the baton (runs a    *)
(*    goroutine) at a time.  The whole point of the baton.                 *)
(*                                                                         *)
(*  - AllRun (liveness / no deadlock): every hub eventually gets to run.   *)
(*    This is the deadlock that bit the prototype: a CPU-bound goroutine    *)
(*    that does not yield holds the baton forever and starves the others.  *)
(*    Modelled by `spun` (a run that won't cooperatively yield).  The fix   *)
(*    is PREEMPTION -- a spun hub is forced to release.  With CONSTANT      *)
(*    Preempt = FALSE the model reproduces the deadlock (AllRun violated);  *)
(*    with Preempt = TRUE it holds.  This is the formal counterpart of the  *)
(*    "keep preemption ON" fix.                                            *)
(*                                                                         *)
(*  - DeterministicGrant (determinism): the baton is granted only when no  *)
(*    runnable hub is still in flight to the rendezvous, so the controller  *)
(*    always chooses over the COMPLETE requester set (a function of the     *)
(*    schedule, not OS timing).  CONSTANT Barrier = FALSE drops the         *)
(*    rendezvous and the model finds a grant made over a partial set        *)
(*    (DeterministicGrant violated) -- the residual nondeterminism the      *)
(*    barrier-rendezvous is meant to remove.                               *)
(*                                                                         *)
(*  - DeterministicTick (timers / lever 6): with CONSTANT Timers = TRUE a   *)
(*    hub may, instead of arriving directly, wait on a TIMER with a         *)
(*    deadline; the controller fires a timer only at a QUIESCENT point (no  *)
(*    hub arriving/waiting/running -- the analogue of the runtime's         *)
(*    quiescent census) and, under CONSTANT LogicalClock = TRUE, only the   *)
(*    EARLIEST pending deadline.  LogicalClock = FALSE lets it fire a later  *)
(*    timer while an earlier one is pending (DeterministicTick violated) -- *)
(*    the wall-clock nondeterminism the logical clock removes.  The clock   *)
(*    advance never touches the baton, so MutualExclusion is preserved, and *)
(*    AllRun still holds (timed hubs eventually fire and run).              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Hubs,         \* set of hub ids (OS threads)
          NoHub,        \* sentinel: baton unheld (model value)
          Preempt,      \* TRUE -> a CPU-bound (spun) hub is forced to yield
          Barrier,      \* TRUE -> grant only when the requester set is complete
          Timers,       \* TRUE -> hubs may wait on timers (logical-clock lever)
          LogicalClock, \* TRUE -> a timer fires only at its EARLIEST deadline
          MaxClock,     \* bound on logical time (finite state)
          MaxRuns       \* bound each hub's run count (finite state)

VARIABLES
    st,        \* st[h] in {"idle","arriving","waiting","running","timed"}
    current,   \* hub holding the baton, or NoHub
    runs,      \* runs[h] : times h has completed a run (<= MaxRuns)
    spun,      \* spun[h] : current run is CPU-bound (won't cooperatively yield)
    badGrant,  \* history: a grant was ever made while a hub was still arriving
    clock,     \* logical time; advances only when a timer fires
    deadline,  \* deadline[h] : a timed hub's fire time (0 when not timed)
    badTick    \* history: a timer ever fired while an earlier one was pending

vars == <<st, current, runs, spun, badGrant, clock, deadline, badTick>>

TypeOK ==
    /\ st \in [Hubs -> {"idle","arriving","waiting","running","timed"}]
    /\ current \in Hubs \cup {NoHub}
    /\ runs \in [Hubs -> 0..MaxRuns]
    /\ spun \in [Hubs -> BOOLEAN]
    /\ badGrant \in BOOLEAN
    /\ clock \in 0..MaxClock
    /\ deadline \in [Hubs -> 0..MaxClock]
    /\ badTick \in BOOLEAN

Init ==
    /\ st = [h \in Hubs |-> "idle"]
    /\ current = NoHub
    /\ runs = [h \in Hubs |-> 0]
    /\ spun = [h \in Hubs |-> FALSE]
    /\ badGrant = FALSE
    /\ clock = 0
    /\ deadline = [h \in Hubs |-> 0]
    /\ badTick = FALSE

NoneArriving == \A h \in Hubs : st[h] # "arriving"

\* Quiescent: no hub is requesting or holding the baton (some may be "timed"
\* or "idle").  The only point at which the controller advances the clock.
Quiescent == \A h \in Hubs : st[h] \in {"idle","timed"}

\* Earliest deadline among the currently-timed hubs.
TimedHubs == {h \in Hubs : st[h] = "timed"}
MinDeadline == IF TimedHubs = {} THEN 0
               ELSE CHOOSE d \in {deadline[h] : h \in TimedHubs} :
                        \A h \in TimedHubs : deadline[h] >= d

\* A hub with remaining work leaves idle and heads toward the rendezvous.
Arrive(h) ==
    /\ st[h] = "idle"
    /\ runs[h] < MaxRuns
    /\ st' = [st EXCEPT ![h] = "arriving"]
    /\ UNCHANGED <<current, runs, spun, badGrant, clock, deadline, badTick>>

\* It reaches the rendezvous and registers its request for the baton.
Rendezvous(h) ==
    /\ st[h] = "arriving"
    /\ st' = [st EXCEPT ![h] = "waiting"]
    /\ UNCHANGED <<current, runs, spun, badGrant, clock, deadline, badTick>>

\* The controller hands the baton to a waiting hub.  With the barrier it may
\* fire only when no hub is still arriving (requester set complete).
Grant(h) ==
    /\ current = NoHub
    /\ st[h] = "waiting"
    /\ (Barrier => NoneArriving)
    /\ current' = h
    /\ st' = [st EXCEPT ![h] = "running"]
    /\ spun' = [spun EXCEPT ![h] = FALSE]
    /\ badGrant' = (badGrant \/ ~NoneArriving)
    /\ UNCHANGED <<runs, clock, deadline, badTick>>

\* The running goroutine is CPU-bound this run: it keeps the baton (one-shot
\* marker to bound the state space).
Spin(h) ==
    /\ current = h
    /\ st[h] = "running"
    /\ ~spun[h]
    /\ spun' = [spun EXCEPT ![h] = TRUE]
    /\ UNCHANGED <<st, current, runs, badGrant, clock, deadline, badTick>>

\* The running hub yields/finishes and releases the baton.
Release(h) ==
    /\ current = h
    /\ st[h] = "running"
    /\ current' = NoHub
    /\ st' = [st EXCEPT ![h] = "idle"]
    /\ runs' = [runs EXCEPT ![h] = runs[h] + 1]
    /\ UNCHANGED <<spun, badGrant, clock, deadline, badTick>>

\* A hub elects to wait on a timer before its next run (sched_sleep): it leaves
\* idle for "timed" with some future deadline.  Optional -- the alternative to
\* Arrive -- so no fairness forces it.  A range of deadlines is explored so the
\* min-selection below is non-trivial.
StartTimer(h) ==
    /\ Timers
    /\ st[h] = "idle"
    /\ runs[h] < MaxRuns
    /\ clock < MaxClock
    /\ \E d \in (clock + 1)..MaxClock :
          /\ st' = [st EXCEPT ![h] = "timed"]
          /\ deadline' = [deadline EXCEPT ![h] = d]
    /\ UNCHANGED <<current, runs, spun, badGrant, clock, badTick>>

\* The controller advances the logical clock and fires a timer.  ONLY at a
\* quiescent point (no hub requesting/holding the baton) -- the analogue of the
\* runtime's quiescent census.  Under LogicalClock it may fire only a timer at
\* the EARLIEST pending deadline; without it a later timer may fire first, which
\* badTick records (DeterministicTick violated).  Never touches the baton.
TimerFire(h) ==
    /\ Timers
    /\ st[h] = "timed"
    /\ Quiescent
    /\ (LogicalClock => deadline[h] = MinDeadline)
    /\ clock' = deadline[h]
    /\ st' = [st EXCEPT ![h] = "arriving"]
    /\ deadline' = [deadline EXCEPT ![h] = 0]
    /\ badTick' = (badTick \/ (deadline[h] > MinDeadline))
    /\ UNCHANGED <<current, runs, spun, badGrant>>

\* All work finished and every hub idle: a legitimate terminal state.  Give it
\* a self-loop so TLC doesn't report normal completion as a deadlock (then the
\* only "deadlock" TLC can find is a real stuck state, and a genuinely starved
\* run shows up as an AllRun *liveness* violation, not a false deadlock).
Terminated == \A h \in Hubs : runs[h] = MaxRuns /\ st[h] = "idle"

Next == \/ \E h \in Hubs :
               \/ Arrive(h)     \/ Rendezvous(h) \/ Grant(h)
               \/ Spin(h)       \/ Release(h)
               \/ StartTimer(h) \/ TimerFire(h)
        \/ (Terminated /\ UNCHANGED vars)

\* Fairness: the protocol's own steps make progress.  A cooperative run
\* (spun=FALSE) is always fairly released; a CPU-bound run (spun=TRUE) is
\* fairly released ONLY under preemption -- the crux the model checks.  A timed
\* hub is fairly fired (so timers don't starve AllRun); StartTimer stays a free
\* choice (no fairness), since a goroutine need not sleep.
Fairness ==
    /\ \A h \in Hubs : WF_vars(Arrive(h))
    /\ \A h \in Hubs : WF_vars(Rendezvous(h))
    /\ \A h \in Hubs : SF_vars(Grant(h))         \* strong: a waiting hub that is
                                                 \* repeatedly grantable is not starved
    /\ \A h \in Hubs : WF_vars(Release(h) /\ ~spun[h])
    /\ (Preempt => \A h \in Hubs : WF_vars(Release(h)))
    /\ (Timers  => \A h \in Hubs : SF_vars(TimerFire(h)))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
MutualExclusion ==
    Cardinality({h \in Hubs : st[h] = "running"}) <= 1

\* The baton holder is exactly the (at most one) running hub.
BatonConsistent ==
    \/ current = NoHub
    \/ (current \in Hubs /\ st[current] = "running")

DeterministicGrant == ~badGrant

\* A timer never fired while an earlier deadline was still pending: the logical
\* clock always advances to the earliest deadline, so timer firing order is a
\* function of the schedule, not of wall-clock polls.  Holds under LogicalClock.
DeterministicTick == ~badTick

\* Liveness: every hub eventually reaches its full run count -- no goroutine
\* is starved by a baton holder that never lets go.
AllRun == <>(\A h \in Hubs : runs[h] = MaxRuns)
=============================================================================
