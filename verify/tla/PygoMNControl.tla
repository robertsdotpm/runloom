--------------------------- MODULE PygoMNControl ---------------------------
(***************************************************************************)
(* TLA+ model of pygo's CONTROLLED M:N scheduler (PYGO_MN_SEED, the        *)
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
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Hubs,      \* set of hub ids (OS threads)
          NoHub,     \* sentinel: baton unheld (model value)
          Preempt,   \* TRUE -> a CPU-bound (spun) hub is forced to yield
          Barrier,   \* TRUE -> grant only when the requester set is complete
          MaxRuns    \* bound each hub's run count (finite state)

VARIABLES
    st,        \* st[h] in {"idle","arriving","waiting","running"}
    current,   \* hub holding the baton, or NoHub
    runs,      \* runs[h] : times h has completed a run (<= MaxRuns)
    spun,      \* spun[h] : current run is CPU-bound (won't cooperatively yield)
    badGrant   \* history: a grant was ever made while a hub was still arriving

vars == <<st, current, runs, spun, badGrant>>

TypeOK ==
    /\ st \in [Hubs -> {"idle","arriving","waiting","running"}]
    /\ current \in Hubs \cup {NoHub}
    /\ runs \in [Hubs -> 0..MaxRuns]
    /\ spun \in [Hubs -> BOOLEAN]
    /\ badGrant \in BOOLEAN

Init ==
    /\ st = [h \in Hubs |-> "idle"]
    /\ current = NoHub
    /\ runs = [h \in Hubs |-> 0]
    /\ spun = [h \in Hubs |-> FALSE]
    /\ badGrant = FALSE

NoneArriving == \A h \in Hubs : st[h] # "arriving"

\* A hub with remaining work leaves idle and heads toward the rendezvous.
Arrive(h) ==
    /\ st[h] = "idle"
    /\ runs[h] < MaxRuns
    /\ st' = [st EXCEPT ![h] = "arriving"]
    /\ UNCHANGED <<current, runs, spun, badGrant>>

\* It reaches the rendezvous and registers its request for the baton.
Rendezvous(h) ==
    /\ st[h] = "arriving"
    /\ st' = [st EXCEPT ![h] = "waiting"]
    /\ UNCHANGED <<current, runs, spun, badGrant>>

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
    /\ UNCHANGED runs

\* The running goroutine is CPU-bound this run: it keeps the baton (one-shot
\* marker to bound the state space).
Spin(h) ==
    /\ current = h
    /\ st[h] = "running"
    /\ ~spun[h]
    /\ spun' = [spun EXCEPT ![h] = TRUE]
    /\ UNCHANGED <<st, current, runs, badGrant>>

\* The running hub yields/finishes and releases the baton.
Release(h) ==
    /\ current = h
    /\ st[h] = "running"
    /\ current' = NoHub
    /\ st' = [st EXCEPT ![h] = "idle"]
    /\ runs' = [runs EXCEPT ![h] = runs[h] + 1]
    /\ UNCHANGED <<spun, badGrant>>

\* All work finished and every hub idle: a legitimate terminal state.  Give it
\* a self-loop so TLC doesn't report normal completion as a deadlock (then the
\* only "deadlock" TLC can find is a real stuck state, and a genuinely starved
\* run shows up as an AllRun *liveness* violation, not a false deadlock).
Terminated == \A h \in Hubs : runs[h] = MaxRuns /\ st[h] = "idle"

Next == \/ \E h \in Hubs :
               \/ Arrive(h) \/ Rendezvous(h) \/ Grant(h)
               \/ Spin(h)   \/ Release(h)
        \/ (Terminated /\ UNCHANGED vars)

\* Fairness: the protocol's own steps make progress.  A cooperative run
\* (spun=FALSE) is always fairly released; a CPU-bound run (spun=TRUE) is
\* fairly released ONLY under preemption -- the crux the model checks.
Fairness ==
    /\ \A h \in Hubs : WF_vars(Arrive(h))
    /\ \A h \in Hubs : WF_vars(Rendezvous(h))
    /\ \A h \in Hubs : SF_vars(Grant(h))         \* strong: a waiting hub that is
                                                 \* repeatedly grantable is not starved
    /\ \A h \in Hubs : WF_vars(Release(h) /\ ~spun[h])
    /\ (Preempt => \A h \in Hubs : WF_vars(Release(h)))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
MutualExclusion ==
    Cardinality({h \in Hubs : st[h] = "running"}) <= 1

\* The baton holder is exactly the (at most one) running hub.
BatonConsistent ==
    \/ current = NoHub
    \/ (current \in Hubs /\ st[current] = "running")

DeterministicGrant == ~badGrant

\* Liveness: every hub eventually reaches its full run count -- no goroutine
\* is starved by a baton holder that never lets go.
AllRun == <>(\A h \in Hubs : runs[h] = MaxRuns)
=============================================================================
