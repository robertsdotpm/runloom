---------------------------- MODULE RunloomComposite ----------------------------
(***************************************************************************)
(* WHOLE-PROGRAM LIVENESS model of runloom: the scheduler + every wake     *)
(* SOURCE (channels, netpoll fds, timers, foreign/cross-thread wakes)      *)
(* composed, checked for the one property that matters for HANGS --        *)
(*                                                                         *)
(*   NoHang:  <> (every goroutine is done)                                 *)
(*                                                                         *)
(* Real hangs live in the SEAMS between subsystems, not inside any one, so *)
(* the model's value is the shared machinery they all funnel through: a g  *)
(* BLOCKS on a source; the source eventually FIRES a wake; the wake must    *)
(* ROUTE to the woken g's home hub's sub-queue; the hub must DRAIN it; and  *)
(* a hub must not commit to IDLE past a wake that has landed (the           *)
(* census-idle wake-guard).                                                *)
(*                                                                         *)
(* KEY ABSTRACTION (and why this is the right whole-program model for      *)
(* HANGS): the four wake sources differ only in what makes a blocked g's    *)
(* wake eligible to fire --                                                 *)
(*   - channel : the counterparty goroutine completes (an INTERNAL wake);   *)
(*   - netpoll : an fd becomes ready;                                       *)
(*   - timer   : a deadline arrives;                                        *)
(*   - foreign : a non-hub thread (executor / io_uring CQE) posts a wake;   *)
(* the last three are all EXTERNAL events, and ALL FOUR funnel through the  *)
(* identical route-to-home + drain + don't-idle-past machinery.  Their      *)
(* differences (timer ORDERING, channel FIFO) are about determinism, which  *)
(* the dedicated models cover (RunloomMNControl, RunloomSched); for LIVENESS *)
(* they are one seam.  So the composite models two source kinds -- internal *)
(* (channel) and external (fd/timer/foreign) -- and the two ways the shared *)
(* seam breaks, each a real hang class:                                     *)
(*   BuggyQuiesce : the wake interrupt misses the "idling" window (the      *)
(*                  census-idle wake-guard bug) -> a hub idles past a wake;  *)
(*   BuggyRoute   : an external wake is routed to the WRONG hub (a netpoll   *)
(*                  / foreign wake-routing bug) -> the home hub never gets   *)
(*                  it.                                                      *)
(* The all-correct config MUST satisfy NoHang; each Buggy* control MUST      *)
(* violate it.  Small bounded instance (TLC liveness is costly).            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS BuggyQuiesce,   \* TRUE -> wake interrupt misses the "idling" window
          BuggyRoute      \* TRUE -> an external wake routes to the wrong hub

\* ---- the modelled workload (small + bounded) ----
\* g1 (home h1) blocks on a CHANNEL: it waits for g2 (home h2) to complete -- a
\*   cross-hub INTERNAL wake.
\* g2 (home h2) waits for nothing; it just runs and completes (and wakes g1).
\* g3 (home h1) blocks on an EXTERNAL source (an fd / timer / foreign wake): it is
\*   woken by ExternalFire, exercising the route-to-home + interrupt seam.
NoG   == "noG"
Gs    == {"g1", "g2", "g3"}
Hubs  == {"h1", "h2"}
Home  == [g \in Gs |-> IF g = "g2" THEN "h2" ELSE "h1"]
Src   == [g \in Gs |-> IF g = "g3" THEN "ext" ELSE "internal"]
Needs == [g \in Gs |-> IF g = "g1" THEN "g2" ELSE NoG]   \* internal dependency
Other(h) == CHOOSE k \in Hubs : k # h

VARIABLES
    gstate,     \* gstate[g] in {"ready","running","blocked","done"}
    running,    \* running[h] : the g running on hub h, or NoG
    sub,        \* sub[h] : woken gs routed to hub h, pending drain
    hub,        \* hub[h] in {"active","idling","idle"}  (idling = decided, not committed)
    woken,      \* woken[g] : g's wake has been delivered (sub-routed)
    extFired    \* extFired[g] : g's external event has fired (fd ready / deadline / CQE)

vars == <<gstate, running, sub, hub, woken, extFired>>

NoneReadyAt(h) == \A g \in Gs : ~(gstate[g] = "ready" /\ Home[g] = h)

\* A goroutine may run to completion when its source is satisfied.
CanComplete(g) ==
    IF Src[g] = "ext"
      THEN extFired[g]
      ELSE IF Needs[g] = NoG THEN TRUE ELSE gstate[Needs[g]] = "done"

TypeOK ==
    /\ gstate   \in [Gs -> {"ready","running","blocked","done"}]
    /\ running  \in [Hubs -> Gs \cup {NoG}]
    /\ sub      \in [Hubs -> SUBSET Gs]
    /\ hub      \in [Hubs -> {"active","idling","idle"}]
    /\ woken    \in [Gs -> BOOLEAN]
    /\ extFired \in [Gs -> BOOLEAN]

Init ==
    /\ gstate   = [g \in Gs |-> "ready"]
    /\ running  = [h \in Hubs |-> NoG]
    /\ sub      = [h \in Hubs |-> {}]
    /\ hub      = [h \in Hubs |-> "active"]
    /\ woken    = [g \in Gs |-> FALSE]
    /\ extFired = [g \in Gs |-> FALSE]

\* The wake INTERRUPT decision (shared by every wake): knock the target hub off
\* idle so it drains.  CORRECT: reliable across the whole idle window ("idling"
\* OR "idle" -> active).  BuggyQuiesce: only a fully-committed "idle" hub is
\* interrupted, MISSING the "idling" window, so a hub that decided to idle just
\* before the wake landed never wakes -> the woken g is stranded (a hang).
InterruptHub(h, hubfn, gotwk) ==
    IF gotwk /\ (hubfn[h] = "idle" \/ (~BuggyQuiesce /\ hubfn[h] = "idling"))
      THEN "active" ELSE hubfn[h]

\* A hub picks a ready, home goroutine and runs it.  Only an active hub runs.
Resume(h, g) ==
    /\ hub[h] = "active"
    /\ running[h] = NoG
    /\ gstate[g] = "ready"
    /\ Home[g] = h
    /\ running' = [running EXCEPT ![h] = g]
    /\ gstate'  = [gstate  EXCEPT ![g] = "running"]
    /\ UNCHANGED <<sub, hub, woken, extFired>>

\* The running g completes if its source is satisfied; on completion it WAKES
\* every (internal-source) g waiting on it, routing each to its home hub.
Complete(g) ==
    /\ gstate[g] = "running"
    /\ (CanComplete(g) \/ woken[g])
    /\ gstate'  = [gstate  EXCEPT ![g] = "done"]
    /\ running' = [running EXCEPT ![Home[g]] = NoG]
    /\ LET wk == {x \in Gs : Src[x] = "internal" /\ Needs[x] = g /\ gstate[x] = "blocked"}
           gotwk(h) == \E x \in wk : Home[x] = h
       IN
         /\ sub'   = [h \in Hubs |-> sub[h] \cup {x \in wk : Home[x] = h}]
         /\ woken' = [x \in Gs |-> IF x \in wk THEN TRUE ELSE woken[x]]
         /\ hub'   = [h \in Hubs |-> InterruptHub(h, hub, gotwk(h))]
    /\ UNCHANGED extFired

\* An EXTERNAL event fires for a blocked ext goroutine (fd ready / timer deadline /
\* foreign CQE).  It wakes g, routing it to a hub.  CORRECT: routes to g's home
\* hub.  BuggyRoute: routes to the WRONG (other) hub, so g's home hub never gets
\* the wake and never resumes it -> a hang (the netpoll / foreign wake-routing bug).
ExternalFire(g) ==
    /\ Src[g] = "ext"
    /\ gstate[g] = "blocked"
    /\ ~woken[g]
    /\ extFired' = [extFired EXCEPT ![g] = TRUE]
    /\ woken'    = [woken    EXCEPT ![g] = TRUE]
    /\ LET tgt == IF BuggyRoute THEN Other(Home[g]) ELSE Home[g] IN
         /\ sub' = [sub EXCEPT ![tgt] = sub[tgt] \cup {g}]
         /\ hub' = [h \in Hubs |-> InterruptHub(h, hub, h = tgt)]
    /\ UNCHANGED <<gstate, running>>

\* A running g blocks because its source is not yet satisfied.
Block(g) ==
    /\ gstate[g] = "running"
    /\ ~CanComplete(g)
    /\ ~woken[g]
    /\ gstate'  = [gstate  EXCEPT ![g] = "blocked"]
    /\ running' = [running EXCEPT ![Home[g]] = NoG]
    /\ UNCHANGED <<sub, hub, woken, extFired>>

\* Hub drains its sub-queue: woken gs become ready.  An "idle" hub is blocked in
\* the poll and cannot drain on its own -- a wake interrupt must reactivate it.
Drain(h) ==
    /\ hub[h] # "idle"
    /\ sub[h] # {}
    /\ gstate' = [g \in Gs |-> IF g \in sub[h] THEN "ready" ELSE gstate[g]]
    /\ sub'    = [sub EXCEPT ![h] = {}]
    /\ hub'    = [hub EXCEPT ![h] = "active"]
    /\ UNCHANGED <<running, woken, extFired>>

\* The census-idle wake-guard in two steps so a wake can race the idle.
IdleDecide(h) ==
    /\ hub[h] = "active"
    /\ running[h] = NoG
    /\ NoneReadyAt(h)
    /\ sub[h] = {}
    /\ hub' = [hub EXCEPT ![h] = "idling"]
    /\ UNCHANGED <<gstate, running, sub, woken, extFired>>

IdleCommit(h) ==
    /\ hub[h] = "idling"
    /\ hub' = [hub EXCEPT ![h] = "idle"]
    /\ UNCHANGED <<gstate, running, sub, woken, extFired>>

Done == \A g \in Gs : gstate[g] = "done"

Next ==
    \/ \E h \in Hubs, g \in Gs : Resume(h, g)
    \/ \E g \in Gs : Complete(g) \/ Block(g) \/ ExternalFire(g)
    \/ \E h \in Hubs : Drain(h) \/ IdleDecide(h) \/ IdleCommit(h)
    \/ (Done /\ UNCHANGED vars)          \* terminal self-loop (no false deadlock)

\* Fairness: every continuously-enabled progress step is eventually taken.  Note
\* ExternalFire is fair (the fd/timer/foreign event DOES eventually arrive), so a
\* correct system must still drain it; a hang must come from the scheduler losing
\* the wake, not from the event never arriving.
Fairness ==
    /\ \A h \in Hubs, g \in Gs : SF_vars(Resume(h, g))
    /\ \A g \in Gs : SF_vars(Complete(g)) /\ WF_vars(Block(g)) /\ SF_vars(ExternalFire(g))
    /\ \A h \in Hubs : SF_vars(Drain(h))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: at most one g runs per hub; a running g is the hub's running slot.
RunConsistent == \A h \in Hubs :
    (running[h] # NoG) => (gstate[running[h]] = "running" /\ Home[running[h]] = h)

\* Safety shadow of the liveness property: a g whose source is satisfied is never
\* left blocked with no pending wake (a dropped wake), as long as it's home-routed.
NoLostWake == \A g \in Gs :
    (gstate[g] = "blocked" /\ CanComplete(g) /\ woken[g])
        => (\E h \in Hubs : g \in sub[h] \/ gstate[g] = "ready")

\* THE LIVENESS PROPERTY: every goroutine eventually completes -- no hang.
NoHang == <> Done
=============================================================================
