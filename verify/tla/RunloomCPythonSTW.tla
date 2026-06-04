-------------------------- MODULE RunloomCPythonSTW --------------------------
(***************************************************************************)
(* TLA+ model of the BOUNDARY between runloom's M:N hubs and free-threaded *)
(* CPython 3.13t's stop-the-world (STW) machinery -- the contract whose    *)
(* violation cost us the gc-churn use-after-frees.  It composes two CPython *)
(* internal state machines, read from Python/pystate.c (see                *)
(* docs/dev/cpython_boundary.md):                                          *)
(*                                                                         *)
(*  M1  the per-tstate attach/detach machine  tstate->state in             *)
(*      {ATTACHED, DETACHED, SUSPENDED}: a hub ATTACHES (PyEval_RestoreThread*)
(*      -> tstate_try_attach CAS detached->attached) to run a goroutine, and *)
(*      DETACHES (PyEval_SaveThread) before it blocks, so a blocked hub sits *)
(*      detached at a GC-safe point (contract C4).                         *)
(*                                                                         *)
(*  M2  the stop_the_world handshake (pystate.c stop_the_world /           *)
(*      park_detached_threads / _PyThreadState_Suspend): a hub that calls   *)
(*      gc.collect becomes the REQUESTER; it drives every OTHER hub to      *)
(*      SUSPENDED -- CAS-parking the ones already DETACHED, and setting the *)
(*      eval-breaker stop bit on the ATTACHED ones so they SUSPEND          *)
(*      themselves at the next safe point -- and only then is the world     *)
(*      "stopped" and the requester reclaims alone.  start_the_world flips  *)
(*      every suspended hub back to DETACHED.                              *)
(*                                                                         *)
(* THE SAFETY INVARIANT (STWExclusive): while the world is stopped, every   *)
(* hub but the requester is SUSPENDED -- nobody is ATTACHED, so nobody      *)
(* mutates an object or a refcount while the requester reclaims.  This is   *)
(* exactly what the gc-churn UAFs broke.                                   *)
(*                                                                         *)
(* THE BUG CONTROL (Bypass): the handoff rescue path (bug 2 / contract C3)  *)
(* re-ATTACHES a tstate WITHOUT going through tstate_wait_attach, which     *)
(* would otherwise block a SUSPENDED tstate until start_the_world.          *)
(* CONSTANT Bypass = TRUE enables that transition; TLC then finds a state   *)
(* with the world stopped and a non-requester hub ATTACHED -- STWExclusive  *)
(* violated.  Bypass = FALSE (the fix: only attach via the proper gate)     *)
(* holds.  The negative control is the formal counterpart of "never         *)
(* re-attach a tstate another thread may have suspended mid-STW".           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Hubs,       \* set of hub OS-thread ids
          NoHub,      \* sentinel: no requester (model value)
          Bypass,     \* TRUE -> enable the handoff re-attach bug (C3 violation)
          BuggyBlock, \* TRUE -> a hub may BLOCK while ATTACHED (a C4 violation): it
                      \*         never reaches a safe point, so stop-the-world can
                      \*         never complete -> the STW-monopoly HANG
          MaxStops    \* bound on completed STW cycles (finite state)

Others(r) == Hubs \ {r}

VARIABLES
    state,      \* state[h] in {"attached","detached","suspended"} (M1)
    world,      \* {"running","stopping","stopped"} (M2)
    requester,  \* the hub holding the world stopped, or NoHub
    stops,      \* completed STW cycles
    wedged      \* wedged[h] : h is blocked WHILE attached (a C4 violation; it never
                \* reaches a safe point, so it can't be suspended for a STW)

vars == <<state, world, requester, stops, wedged>>

TypeOK ==
    /\ state \in [Hubs -> {"attached","detached","suspended"}]
    /\ world \in {"running","stopping","stopped"}
    /\ requester \in Hubs \cup {NoHub}
    /\ stops \in 0..MaxStops
    /\ wedged \in [Hubs -> BOOLEAN]

Init ==
    /\ state = [h \in Hubs |-> "detached"]
    /\ world = "running"
    /\ requester = NoHub
    /\ stops = 0
    /\ wedged = [h \in Hubs |-> FALSE]

\* M1: attach to run a goroutine.  tstate_try_attach is a CAS detached->attached.
\* Impossible once the world is "stopped" (the requester holds the eval lock); the
\* real race with a "stopping" requester is allowed -- it just defers the stop until
\* this hub self-suspends again.  A hub never attaches while it is the requester.
Attach(h) ==
    /\ state[h] = "detached"
    /\ world # "stopped"
    /\ state' = [state EXCEPT ![h] = "attached"]
    /\ UNCHANGED <<world, requester, stops, wedged>>

\* M1 / C4: detach before blocking (PyEval_SaveThread).  The requester does not
\* detach while it holds the world.
Detach(h) ==
    /\ state[h] = "attached"
    /\ h # requester
    /\ state' = [state EXCEPT ![h] = "detached"]
    /\ UNCHANGED <<world, requester, stops, wedged>>

\* M2: a hub calls gc.collect -> stop_the_world.  It must be attached (running
\* Python) and there must be no STW in flight.
GCRequest(r) ==
    /\ world = "running"
    /\ state[r] = "attached"
    /\ stops < MaxStops
    /\ world' = "stopping"
    /\ requester' = r
    /\ UNCHANGED <<state, stops, wedged>>

\* M2: park_detached_threads -- the requester CAS-flips a DETACHED other hub to
\* SUSPENDED ("gc stopped").
GCPark(h) ==
    /\ world = "stopping"
    /\ h \in Others(requester)
    /\ state[h] = "detached"
    /\ state' = [state EXCEPT ![h] = "suspended"]
    /\ UNCHANGED <<world, requester, stops, wedged>>

\* M1+M2: an ATTACHED other hub hits the eval-breaker stop bit the requester set and
\* suspends ITSELF (_PyThreadState_Suspend) -- how an attached hub reaches a safe
\* point.  This is the load-bearing step: the world cannot stop until it happens.
SelfSuspend(h) ==
    /\ world = "stopping"
    /\ h \in Others(requester)
    /\ state[h] = "attached"
    /\ ~wedged[h]               \* a hub blocked-while-attached can't reach a safe point
    /\ state' = [state EXCEPT ![h] = "suspended"]
    /\ UNCHANGED <<world, requester, stops, wedged>>

\* M2: stop_the_world completes once every other hub is suspended.
GCStopComplete ==
    /\ world = "stopping"
    /\ \A h \in Others(requester) : state[h] = "suspended"
    /\ world' = "stopped"
    /\ UNCHANGED <<state, requester, stops, wedged>>

\* M2: start_the_world -- flip every suspended hub back to detached, release.
GCStart ==
    /\ world = "stopped"
    /\ world' = "running"
    /\ state' = [h \in Hubs |-> IF state[h] = "suspended" THEN "detached" ELSE state[h]]
    /\ requester' = NoHub
    /\ stops' = stops + 1
    /\ UNCHANGED wedged

\* THE BUG (Bypass=TRUE): re-attach a SUSPENDED tstate directly, bypassing the
\* tstate_wait_attach gate that would block until start_the_world.  Models the
\* handoff rescue adopting a tstate mid-STW (bug 2 / contract C3).
AdoptSuspended(h) ==
    /\ Bypass
    /\ state[h] = "suspended"
    /\ state' = [state EXCEPT ![h] = "attached"]
    /\ UNCHANGED <<world, requester, stops, wedged>>

\* THE STW-MONOPOLY BUG (BuggyBlock=TRUE): a non-requester hub BLOCKS while still
\* ATTACHED -- a C4 contract violation (it should detach before blocking).  It then
\* never reaches a safe point (SelfSuspend is disabled), so the requester can never
\* drive it to "suspended" and stop_the_world can never complete: the whole world
\* hangs.  The real bug class is the gc-churn STW-monopoly deadlock.
BlockAttached(h) ==
    /\ BuggyBlock
    /\ state[h] = "attached"
    /\ h # requester
    /\ ~wedged[h]
    /\ wedged' = [wedged EXCEPT ![h] = TRUE]
    /\ UNCHANGED <<state, world, requester, stops>>

Next ==
    \/ \E h \in Hubs : \/ Attach(h)     \/ Detach(h)   \/ GCRequest(h)
                       \/ GCPark(h)      \/ SelfSuspend(h) \/ AdoptSuspended(h)
                       \/ BlockAttached(h)
    \/ GCStopComplete
    \/ GCStart
    \/ (stops = MaxStops /\ world = "running" /\ UNCHANGED vars)  \* terminal self-loop

Spec == Init /\ [][Next]_vars

\* Fairness for the liveness check: the STW protocol's own steps make progress.  A
\* requested stop is driven to completion -- detached hubs get parked, attached
\* (non-wedged) hubs reach their safe point and self-suspend, and the stop then
\* completes -- so a stuck "stopping" world can only come from a hub blocked while
\* attached (no fairness can force a wedged hub to suspend).  GCStart is fair so a
\* completed stop is released and the run makes progress.
LiveFairness ==
    /\ \A h \in Hubs : SF_vars(GCPark(h))
    /\ \A h \in Hubs : SF_vars(SelfSuspend(h))
    /\ SF_vars(GCStopComplete)
    /\ WF_vars(GCStart)

FairSpec == Init /\ [][Next]_vars /\ LiveFairness

----------------------------------------------------------------------------
\* SAFETY: while the world is stopped, no non-requester hub is attached.  The
\* invariant the gc-churn use-after-frees violated.
STWExclusive ==
    (world = "stopped") => \A h \in Others(requester) : state[h] = "suspended"

\* The requester is attached exactly while it holds the world stopped.
RequesterAttached ==
    (world = "stopped") => (requester \in Hubs /\ state[requester] = "attached")

\* LIVENESS (the STW-monopoly hang): every requested stop-the-world eventually
\* completes.  Holds under BuggyBlock=FALSE (with LiveFairness); a hub that blocks
\* while attached (BuggyBlock=TRUE) wedges the world in "stopping" forever ->
\* violated.  Checked against FairSpec.
STWCompletes == [](world = "stopping" => <>(world = "stopped"))
=============================================================================
