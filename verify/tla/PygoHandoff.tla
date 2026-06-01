---------------------------- MODULE PygoHandoff ----------------------------
(***************************************************************************)
(* TLA+ model of pygo's stall-recovery / P-handoff (PYGO_HANDOFF, the      *)
(* "Group B" arc): when a hub WEDGES (blocks in a syscall or a Python       *)
(* handler and stops draining its run queue), a rescue M must adopt that    *)
(* hub and drain ITS queue to empty, so the wedged hub's work is never      *)
(* stranded.                                                                *)
(*                                                                          *)
(* The Spin/Coq models cover the per-primitive wake protocol; this is the   *)
(* SYSTEM-level emergent property the recovery exists for:                  *)
(*                                                                          *)
(*   AllDrained (liveness): EVERY queued item is eventually drained, even   *)
(*     from a hub that wedges -- no stranded work.                          *)
(*                                                                          *)
(* The rescue is load-bearing: with CONSTANT EnableRescue = FALSE (no       *)
(* adoption) a wedged hub's queue is stuck forever and TLC reports          *)
(* AllDrained violated.  A running hub drains its own queue; a wedged hub   *)
(* is drained ONLY via adoption (status "adopted"), so the owner and the    *)
(* rescue never drain the same queue concurrently.                          *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Hubs,          \* set of hub ids
          InitWork,      \* items each hub starts with
          EnableRescue   \* TRUE -> a rescue M may adopt a wedged hub

Status == {"run", "wedged", "adopted"}

VARIABLES q,        \* q[h]      : items left on hub h's run queue
          status    \* status[h] : "run" | "wedged" | "adopted"

vars == <<q, status>>

TypeOK ==
    /\ q \in [Hubs -> 0..InitWork]
    /\ status \in [Hubs -> Status]

Init ==
    /\ q = [h \in Hubs |-> InitWork]
    /\ status = [h \in Hubs |-> "run"]

\* a running hub drains one of its own items
Drain(h) ==
    /\ status[h] = "run"
    /\ q[h] > 0
    /\ q' = [q EXCEPT ![h] = @ - 1]
    /\ UNCHANGED status

\* a hub wedges (blocks and stops draining)
Wedge(h) ==
    /\ status[h] = "run"
    /\ status' = [status EXCEPT ![h] = "wedged"]
    /\ UNCHANGED q

\* a rescue M adopts a wedged, non-empty hub (only if rescue is enabled)
Adopt(h) ==
    /\ EnableRescue
    /\ status[h] = "wedged"
    /\ q[h] > 0
    /\ status' = [status EXCEPT ![h] = "adopted"]
    /\ UNCHANGED q

\* the rescue M drains the adopted hub's queue (never steals to other hubs)
RescueDrain(h) ==
    /\ status[h] = "adopted"
    /\ q[h] > 0
    /\ q' = [q EXCEPT ![h] = @ - 1]
    /\ UNCHANGED status

\* drained to empty: hand the hub back
Handback(h) ==
    /\ status[h] = "adopted"
    /\ q[h] = 0
    /\ status' = [status EXCEPT ![h] = "run"]
    /\ UNCHANGED q

\* all work drained: legitimate terminal state (stutter, not deadlock)
Terminating ==
    /\ \A h \in Hubs : q[h] = 0
    /\ UNCHANGED vars

Next ==
    \/ \E h \in Hubs : Drain(h) \/ Wedge(h) \/ Adopt(h)
                       \/ RescueDrain(h) \/ Handback(h)
    \/ Terminating

\* Fairness: running hubs drain; wedged hubs get adopted, rescue-drained, and
\* handed back.  Wedge is NOT fair (wedging is allowed, never forced).
Fairness ==
    /\ \A h \in Hubs : WF_vars(Drain(h))
    /\ \A h \in Hubs : WF_vars(Adopt(h))
    /\ \A h \in Hubs : WF_vars(RescueDrain(h))
    /\ \A h \in Hubs : WF_vars(Handback(h))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* an item is drained by at most one actor: a hub is drained by its owner only
\* while "run", and by the rescue only while "adopted" -- mutually exclusive.
NoConcurrentDrain ==
    \A h \in Hubs : (status[h] = "adopted") => (status[h] # "run")

\* Liveness: every queued item is eventually drained -- even from a wedged hub.
AllDrained == <>[](\A h \in Hubs : q[h] = 0)
============================================================================
