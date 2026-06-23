----------------------------- MODULE RunloomGRefcount -----------------------------
EXTENDS Integers
(***************************************************************************)
(* TLA+ model of Tier-2 #6 -- the runloom_g_t REFCOUNT LEDGER composed with the *)
(* wake_state machine (the per-g state machine on `struct runloom_g`, also       *)
(* modelled lock-free in verify/spin/wake_state.pml).  wake_state.pml proves     *)
(* the ENTRY/OWNER discipline (at-most-one runq entry, exactly-one resumer);     *)
(* this adds the orthogonal property that machine maintains: the integer         *)
(* refcount stays consistent with it, so a g is freed exactly once and never     *)
(* while a reference is still outstanding.                                       *)
(*                                                                            *)
(* Who holds a ref to a g (global-runq / per-g-tstate mode):                   *)
(*   - the SCHEDULER ref, taken at spawn (rc=1), dropped by the g's final        *)
(*     completion (runloom_g_decref -> rc 0 -> slab_free).                       *)
(*   - the QUEUE ref: a global-runq entry holds exactly one ref, taken when a    *)
(*     wake enqueues the g (PARKED->QUEUED, runloom_g_incref) and dropped when a  *)
(*     hub consumes the entry (QUEUED->RUNNING, runloom_g_decref).               *)
(* A wake that lands while the g is RUNNING is REMEMBERED (RUNNING->RUNNING_WOKEN)*)
(* and takes NO ref until the owner re-enqueues it at release (RUNNING_WOKEN->    *)
(* QUEUED).  So the invariant the whole machine maintains is:                    *)
(*                                                                            *)
(*     rc  =  (scheduler ref ? 1 : 0)  +  (wake_state = QUEUED ? 1 : 0)          *)
(*                                                                            *)
(* CONSTANT Buggy = TRUE drops the QUEUED->RUNNING decref (a hub consumes the     *)
(* entry but forgets runloom_g_decref) -- the lost-queue-ref leak: the g is never *)
(* freed, and the LEDGER no longer matches the wake_state.                       *)
(***************************************************************************)

CONSTANT Buggy

WS == {"PARKED", "QUEUED", "RUNNING", "RUNNING_WOKEN", "DONE"}

VARIABLES
    ws,        \* the g's wake_state
    rc,        \* runloom_g_t.refcount
    schedRef,  \* the scheduler still holds its spawn ref (dropped at completion)
    freed      \* slab_free has run (rc reached 0)

vars == <<ws, rc, schedRef, freed>>

TypeOK ==
    /\ ws \in WS
    /\ rc \in 0..3
    /\ schedRef \in BOOLEAN
    /\ freed \in BOOLEAN

\* A fresh g spawns RUNNING with the single scheduler ref (mn_go_core sets
\* wake_state = RUNNING under a global-runq mode; rc = 1).
Init ==
    /\ ws = "RUNNING"
    /\ rc = 1
    /\ schedRef = TRUE
    /\ freed = FALSE

QueueRef == IF ws = "QUEUED" THEN 1 ELSE 0
SchedCnt == IF schedRef THEN 1 ELSE 0

\* The g parks off-queue (netpoll/chan/sleep): RUNNING -> PARKED, no ref change.
Park ==
    /\ ws = "RUNNING"
    /\ ws' = "PARKED"
    /\ UNCHANGED <<rc, schedRef, freed>>

\* A wake while PARKED enqueues the g and takes the queue ref (PARKED -> QUEUED).
Wake ==
    /\ ws = "PARKED"
    /\ ws' = "QUEUED"
    /\ rc' = rc + 1
    /\ UNCHANGED <<schedRef, freed>>

\* A wake while RUNNING is remembered, NOT enqueued (RUNNING -> RUNNING_WOKEN).
WakeWhileRunning ==
    /\ ws = "RUNNING"
    /\ ws' = "RUNNING_WOKEN"
    /\ UNCHANGED <<rc, schedRef, freed>>

\* A hub pulls the entry and claims the g (QUEUED -> RUNNING); it drops the queue
\* ref.  Buggy=TRUE forgets that decref -> the queue ref leaks.
Consume ==
    /\ ws = "QUEUED"
    /\ ws' = "RUNNING"
    /\ IF Buggy THEN rc' = rc ELSE rc' = rc - 1
    /\ UNCHANGED <<schedRef, freed>>

\* The owner ends a resume that saw a wake mid-flight: RUNNING_WOKEN -> QUEUED,
\* re-arming exactly one entry (takes the queue ref).
ReleaseWoken ==
    /\ ws = "RUNNING_WOKEN"
    /\ ws' = "QUEUED"
    /\ rc' = rc + 1
    /\ UNCHANGED <<schedRef, freed>>

\* The owner ends a resume that parked: RUNNING_WOKEN can also settle to PARKED
\* if no wake is pending (modelled by RUNNING -> PARKED via Park); and a plain
\* RUNNING g can just keep running (stutter) -- covered by Park/Complete.

\* The g's coroutine finishes WHILE RUNNING (a g completes on-CPU, never while
\* parked/queued): drop the scheduler ref; if that was the last ref, free it.
Complete ==
    /\ ws = "RUNNING"
    /\ schedRef
    /\ ws' = "DONE"
    /\ rc' = rc - 1
    /\ schedRef' = FALSE
    /\ freed' = (rc - 1 = 0)

Done == ws = "DONE" /\ freed

Next ==
    \/ Park \/ Wake \/ WakeWhileRunning \/ Consume \/ ReleaseWoken \/ Complete
    \/ (Done /\ UNCHANGED vars)            \* terminal self-loop (no false deadlock)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* LEDGER: the integer refcount always equals the scheduler ref plus the (0 or 1)
\* outstanding queue ref.  Buggy=TRUE's lost decref breaks it: after a buggy
\* Consume, ws = RUNNING (QueueRef = 0) but rc still carries the leaked queue ref.
Ledger == rc = SchedCnt + QueueRef

\* No negative refcount (no over-decref / double-free).
RcNonNeg == rc >= 0

\* Freed is exactly rc == 0, and a freed g holds NO outstanding reference -- in
\* particular it is never freed while a queue entry could still resume it (UAF).
FreedConsistent ==
    /\ (freed <=> rc = 0)
    /\ (freed => (~schedRef /\ ws # "QUEUED"))
=============================================================================
