---------------------------- MODULE RunloomSched ----------------------------
(***************************************************************************)
(* A system-level TLA+ model of runloom's M:N scheduler, composing the wake/  *)
(* park protocol with the multi-hub dispatcher.                            *)
(*                                                                         *)
(* The Spin models verify each primitive (wake_state, park_safe,           *)
(* netpoll_commit, hub_submit) SEPARATELY.  The verify/ README is honest   *)
(* that their COMPOSITION is only exercised by tests.  This spec checks the *)
(* emergent end-to-end properties of the composed system that no per-       *)
(* primitive model can: across N goroutines and M hubs, with the one-shot  *)
(* wake/park race in the middle,                                           *)
(*                                                                         *)
(*   - NoDoubleRun  (safety): a goroutine runs on at most one hub, and a   *)
(*                  hub runs at most one goroutine, at any instant.         *)
(*   - DoneIsTerminal (safety): a finished goroutine never runs again.     *)
(*   - AllComplete (liveness): EVERY goroutine eventually finishes -- no    *)
(*                  lost wake, no stranded goroutine, no work left undone.  *)
(*                                                                         *)
(* The one-shot wake models real I/O readiness (netpoll): it fires at most *)
(* once per goroutine.  The race is: a wake can arrive while the goroutine  *)
(* is still RUNNING and merely intending to park.  The correct protocol    *)
(* records that as a pending wake and consumes it at park time (the         *)
(* netpollblockcommit / wake_state guard).  With CONSTANT Buggy = TRUE the  *)
(* park ignores the pending wake -- the classic lost wakeup -- and TLC      *)
(* finds AllComplete violated: the goroutine parks forever.                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Gs,        \* set of goroutine ids
          Hubs,      \* set of hub ids (OS threads)
          NoHub,     \* sentinel "no owning hub" (a model value)
          Buggy      \* TRUE -> park drops the pending-wake check (lost wakeup)

VARIABLES
    pc,          \* pc[g]   in {"runnable","running","parked","done"}
    owner,       \* owner[g] in Hubs \cup {NoHub}  (hub currently running g)
    wakeFired,   \* wakeFired[g] : the one-shot wake event has fired
    pending,     \* pending[g]   : a wake arrived while g was still running
    parkedOnce   \* parkedOnce[g]: g has already used its single park

vars == <<pc, owner, wakeFired, pending, parkedOnce>>

HubFree(h) == \A g \in Gs : owner[g] # h

TypeOK ==
    /\ pc \in [Gs -> {"runnable","running","parked","done"}]
    /\ owner \in [Gs -> Hubs \cup {NoHub}]
    /\ wakeFired \in [Gs -> BOOLEAN]
    /\ pending \in [Gs -> BOOLEAN]
    /\ parkedOnce \in [Gs -> BOOLEAN]

Init ==
    /\ pc = [g \in Gs |-> "runnable"]
    /\ owner = [g \in Gs |-> NoHub]
    /\ wakeFired = [g \in Gs |-> FALSE]
    /\ pending = [g \in Gs |-> FALSE]
    /\ parkedOnce = [g \in Gs |-> FALSE]

\* A free hub dispatches a runnable goroutine.
Dispatch(h, g) ==
    /\ HubFree(h)
    /\ pc[g] = "runnable"
    /\ pc' = [pc EXCEPT ![g] = "running"]
    /\ owner' = [owner EXCEPT ![g] = h]
    /\ UNCHANGED <<wakeFired, pending, parkedOnce>>

\* A running goroutine decides to park (block on I/O).  It uses its single
\* park.  Correct: if a wake is already pending, consume it and stay
\* runnable (commit).  Buggy: ignore pending and park anyway (lost wakeup).
ParkReq(g) ==
    /\ pc[g] = "running"
    /\ ~parkedOnce[g]
    /\ parkedOnce' = [parkedOnce EXCEPT ![g] = TRUE]
    /\ owner' = [owner EXCEPT ![g] = NoHub]
    /\ IF (pending[g] /\ ~Buggy)
         THEN pc' = [pc EXCEPT ![g] = "runnable"]   \* consume the pending wake
         ELSE pc' = [pc EXCEPT ![g] = "parked"]
    /\ UNCHANGED <<wakeFired, pending>>

\* The one-shot I/O readiness for g.  If g is already parked, re-enqueue it;
\* if g is still running (about to park), record a pending wake.
Wake(g) ==
    /\ ~wakeFired[g]
    /\ pc[g] \in {"running","parked"}
    /\ wakeFired' = [wakeFired EXCEPT ![g] = TRUE]
    /\ IF pc[g] = "parked"
         THEN /\ pc' = [pc EXCEPT ![g] = "runnable"]
              /\ UNCHANGED pending
         ELSE /\ pending' = [pending EXCEPT ![g] = TRUE]
              /\ UNCHANGED pc
    /\ UNCHANGED <<owner, parkedOnce>>

\* A running goroutine finishes.
Complete(g) ==
    /\ pc[g] = "running"
    /\ pc' = [pc EXCEPT ![g] = "done"]
    /\ owner' = [owner EXCEPT ![g] = NoHub]
    /\ UNCHANGED <<wakeFired, pending, parkedOnce>>

\* When every goroutine is done the system stutters (a legitimate terminal
\* state, not a deadlock) so liveness can observe [](all done).
Terminating ==
    /\ \A g \in Gs : pc[g] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E h \in Hubs, g \in Gs : Dispatch(h, g)
    \/ \E g \in Gs : ParkReq(g)
    \/ \E g \in Gs : Wake(g)
    \/ \E g \in Gs : Complete(g)
    \/ Terminating

\* Fairness: idle hubs eventually dispatch, parked goroutines eventually get
\* their wake, running goroutines eventually complete.  ParkReq is NOT fair
\* (parking is allowed, never forced).
Fairness ==
    /\ \A g \in Gs : WF_vars(Complete(g))
    /\ \A g \in Gs : WF_vars(Wake(g))
    /\ \A g \in Gs : \A h \in Hubs : WF_vars(Dispatch(h, g))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* Safety invariants
NoDoubleRun ==
    \A h \in Hubs : Cardinality({g \in Gs : owner[g] = h}) <= 1

DoneIsTerminal ==
    \A g \in Gs : pc[g] = "done" => owner[g] = NoHub

\* Liveness: every goroutine eventually finishes (no lost wake / no stranding).
AllComplete == <>[](\A g \in Gs : pc[g] = "done")
============================================================================
