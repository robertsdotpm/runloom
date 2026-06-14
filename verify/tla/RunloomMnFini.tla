------------------------------- MODULE RunloomMnFini -------------------------------
(***************************************************************************)
(* TLA+ model of Tier-2 #9 -- the runloom_mn_fini TEARDOWN STOP-SIGNAL handshake  *)
(* (mn_sched_init_fini.c.inc): the lost-wakeup that flaky-hangs mn_fini's          *)
(* pthread_join (memory: "a hub parked in its idle condvar at first idle-park      *)
(* misses the stop -> pthread_join blocks").                                       *)
(*                                                                                *)
(* THE PROTOCOL.  A hub's idle loop, holding idle_lock, checks `stopping`; if not  *)
(* set it cond_waits (which ATOMICALLY releases idle_lock and blocks).  mn_fini    *)
(* sets stopping = TRUE, then signals idle_cond UNDER idle_lock, then joins.        *)
(* Because the signal is taken under the SAME lock the hub holds across its        *)
(* stopping-check-and-commit-to-wait, the signal can never slip into the window    *)
(* between the hub's check and its wait: the hub either sees stopping in its check *)
(* (and exits without waiting) or is already waiting when the signal fires (and is *)
(* woken, re-checks, exits).  So every hub eventually exits and the join completes.*)
(*                                                                                *)
(* CONSTANT Buggy = TRUE signals WITHOUT taking idle_lock (the BUG #10 regression):*)
(* the signal can fire while the hub has checked stopping == FALSE but is not yet  *)
(* waiting -> the signal is lost -> the hub waits forever -> pthread_join hangs.    *)
(***************************************************************************)

CONSTANT Buggy

VARIABLES
    stopping,    \* mn_fini has requested the hubs stop
    lock,        \* idle_lock owner: "free" / "hub" / "fini"
    hubpc,       \* "idle" / "check" / "waiting" / "exited"
    finipc       \* "init" / "stopped" / "signaled" / "joined"

vars == <<stopping, lock, hubpc, finipc>>

TypeOK ==
    /\ stopping \in BOOLEAN
    /\ lock  \in {"free", "hub", "fini"}
    /\ hubpc \in {"idle", "check", "waiting", "exited"}
    /\ finipc \in {"init", "stopped", "signaled", "joined"}

Init ==
    /\ stopping = FALSE
    /\ lock = "free"
    /\ hubpc = "idle"
    /\ finipc = "init"

\* Hub: take idle_lock and read `stopping`.  If set, release + exit; else hold the
\* lock and commit to waiting (-> "check": lock held, about to cond_wait).
HubLockAndCheck ==
    /\ hubpc = "idle"
    /\ lock = "free"
    /\ IF stopping
         THEN /\ hubpc' = "exited"
              /\ lock' = "free"
         ELSE /\ hubpc' = "check"
              /\ lock' = "hub"
    /\ UNCHANGED <<stopping, finipc>>

\* Hub: cond_wait -- ATOMICALLY release idle_lock and block ("waiting").
HubWait ==
    /\ hubpc = "check"
    /\ hubpc' = "waiting"
    /\ lock' = "free"
    /\ UNCHANGED <<stopping, finipc>>

\* mn_fini: request stop (stopping = TRUE, RELEASE).
FiniStop ==
    /\ finipc = "init"
    /\ stopping' = TRUE
    /\ finipc' = "stopped"
    /\ UNCHANGED <<lock, hubpc>>

\* mn_fini: signal idle_cond.  A condvar signal is EPHEMERAL: it wakes the hub only
\* if it is waiting AT THIS MOMENT (-> re-loops to the idle check); to a non-waiting
\* hub it is LOST (no memory).  CORRECT: taken under idle_lock, so it can only run
\* when the lock is free -- i.e. NOT while the hub holds it across its check-then-
\* wait window, which is exactly what makes the lost case harmless (a non-waiting hub
\* is then either pre-check, so it will see stopping, or already exited).  BUGGY:
\* taken without the lock, so it can fire while the hub sits at "check" (checked
\* stopping == FALSE, lock held, not yet waiting) -> lost -> the hub then waits forever.
FiniSignal ==
    /\ finipc = "stopped"
    /\ (~Buggy => lock = "free")        \* correct: must hold idle_lock to signal
    /\ hubpc' = IF hubpc = "waiting" THEN "idle" ELSE hubpc   \* ephemeral: wake iff waiting
    /\ finipc' = "signaled"
    /\ UNCHANGED <<stopping, lock>>

\* mn_fini: pthread_join -- completes only once the hub has exited.
FiniJoin ==
    /\ finipc = "signaled"
    /\ hubpc = "exited"
    /\ finipc' = "joined"
    /\ UNCHANGED <<stopping, lock, hubpc>>

Done == finipc = "joined"

Next ==
    \/ HubLockAndCheck \/ HubWait
    \/ FiniStop \/ FiniSignal \/ FiniJoin
    \/ (Done /\ UNCHANGED vars)

\* Weak fairness on every action so a permanently-stuck hub (the lost-wakeup lasso)
\* is a real liveness violation, not just an unfair stutter.
Fairness ==
    /\ WF_vars(HubLockAndCheck) /\ WF_vars(HubWait)
    /\ WF_vars(FiniStop) /\ WF_vars(FiniSignal) /\ WF_vars(FiniJoin)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: a hub holds idle_lock while at "check" -- so a correct (under-lock)
\* signal cannot run concurrently with the check-then-wait window.
MutexOK == (hubpc = "check") => (lock = "hub")

\* LIVENESS (the teardown completes): mn_fini's join always eventually returns,
\* i.e. the hub always eventually exits.  Holds under the under-lock signal; the
\* Buggy=TRUE control loses the wakeup -> the hub waits forever -> violated.
JoinCompletes == <>(finipc = "joined")
=============================================================================
