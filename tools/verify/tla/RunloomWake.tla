---------------------------- MODULE RunloomWake ----------------------------
(***************************************************************************)
(* TLA+ model of runloom's NETPOLL-DRAIN wake protocol -- the one handshake *)
(* the existing specs do NOT cover.  RunloomSched.tla proves the per-        *)
(* goroutine park/wake commit (pending-wake consumed at park); this models   *)
(* the LAYER ABOVE: the single-thread drain's decide-to-block window vs a    *)
(* foreign thread's wake, and the 2 ms "FOREIGN-THREAD WAKE BACKSTOP"        *)
(* (runloom_sched_drain.c.inc:192-195, commit f214341).                      *)
(*                                                                          *)
(* THE PROTOCOL.  A fiber parks (off every runnable structure).  A foreign   *)
(* thread (blockpool worker / executor / CQE) wakes it by (1) appending the  *)
(* g to the owner's wake_list with a RELEASE store (parkwake.c:505) -- a     *)
(* DURABLE queue -- then (2) poking a level-triggered pump eventfd to wake    *)
(* the drain if it is asleep in epoll_wait.  The drain loop: re-peek         *)
(* wake_list at loop-top (drain.c:80), drain any g to the ready ring, and    *)
(* only if there is nothing to do block in the pump.                         *)
(*                                                                          *)
(* THE HAZARD.  Because wake_list is durable and the drain ALWAYS re-peeks   *)
(* at loop-top, a lost poke is harmless UNLESS the drain has already peeked  *)
(* empty and committed to block UNBOUNDED (epoll_wait(-1)): then it never    *)
(* re-peeks and the appended g is stranded forever.  A poke can be lost for  *)
(* low-level reasons the drain cannot prevent at this layer -- the           *)
(* pool->wake_pending SEQ_CST dedup swallowing a second poke                 *)
(* (netpoll_wake_iouring.c.inc:483), or a memory-ordering re-order of the    *)
(* eventfd write vs the block.  We ABSTRACT that as a nondeterministic       *)
(* "the poke may not deliver"; the fidelity claim is only that a poke CAN be *)
(* lost, not why -- which is all the backstop's liveness argument needs.     *)
(*                                                                          *)
(* THE FIX (what we are proving).  While a foreign job is in flight          *)
(* (bp_inflight > 0, decremented LAST -- blockpool.c:234) the backstop caps  *)
(* the block to 2 ms, so the drain re-polls and re-drains wake_list within a *)
(* bounded time even if the poke was lost.  CONSTANT Backstop = TRUE         *)
(* (RunloomWake.cfg): liveness HOLDS -- every appended fiber is eventually   *)
(* resumed.  CONSTANT Backstop = FALSE (RunloomWake_bug.cfg): a lost poke    *)
(* with an unbounded block strands the fiber -> AllWoken violated, the       *)
(* lost-wakeup lasso.  This is the formal statement of "the backstop closes  *)
(* the lost-poke window".                                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Gs,        \* set of parked fiber ids (each woken by its own foreign waker)
          Backstop   \* TRUE -> 2ms cap re-polls; FALSE -> unbounded block (the bug)

VARIABLES
    wake_list,      \* SUBSET Gs : g's appended by foreign wakers, not yet drained
    poke_pending,   \* BOOLEAN  : the level-triggered pump eventfd is signalled
    bp_inflight,    \* Nat      : foreign jobs still in flight (decremented LAST)
    drain_pc,       \* the drain loop's program counter
    drain_timeout,  \* "infinite" (epoll_wait(-1)) or "backstop_2ms" (capped)
    fiber_pc,       \* [Gs -> {"parked","on_wake_list","ready","resumed"}]
    waker_pc        \* [Gs -> {"running","appended","poked","done"}]  (waker for g)

vars == <<wake_list, poke_pending, bp_inflight, drain_pc, drain_timeout,
          fiber_pc, waker_pc>>

DrainPCs == {"loop_top","peeked","decided","blocked"}

TypeOK ==
    /\ wake_list \subseteq Gs
    /\ poke_pending \in BOOLEAN
    /\ bp_inflight \in 0..Cardinality(Gs)
    /\ drain_pc \in DrainPCs
    /\ drain_timeout \in {"infinite","backstop_2ms"}
    /\ fiber_pc \in [Gs -> {"parked","on_wake_list","ready","resumed"}]
    /\ waker_pc \in [Gs -> {"running","appended","poked","done"}]

Init ==
    /\ wake_list = {}
    /\ poke_pending = FALSE
    /\ bp_inflight = Cardinality(Gs)      \* one in-flight foreign job per fiber
    /\ drain_pc = "loop_top"
    /\ drain_timeout = "infinite"
    /\ fiber_pc = [g \in Gs |-> "parked"]
    /\ waker_pc = [g \in Gs |-> "running"]

HasReady == \E g \in Gs : fiber_pc[g] = "ready"

----------------------------------------------------------------------------
\* ---- Foreign waker (one per fiber g) ----

\* Append g to wake_list with a RELEASE store -- the DURABLE publish, BEFORE the
\* poke (parkwake.c:505).
WakerAppend(g) ==
    /\ waker_pc[g] = "running"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "appended"]
    /\ wake_list' = wake_list \cup {g}
    /\ fiber_pc' = [fiber_pc EXCEPT ![g] = "on_wake_list"]
    /\ UNCHANGED <<poke_pending, bp_inflight, drain_pc, drain_timeout>>

\* Poke the pump eventfd.  NONDETERMINISTIC delivery: poke_pending' may become
\* TRUE (delivered) or stay as-is (LOST -- dedup-suppressed / re-ordered).  This
\* is the abstracted hazard: a poke CAN be lost.  The g is already durably on
\* wake_list, so a delivered poke and a lost-but-re-polled poke both heal; a
\* lost poke with an unbounded block does not.
WakerPoke(g) ==
    /\ waker_pc[g] = "appended"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "poked"]
    /\ poke_pending' \in {poke_pending, TRUE}
    /\ UNCHANGED <<wake_list, bp_inflight, drain_pc, drain_timeout, fiber_pc>>

\* Decrement bp_inflight LAST -- strictly after append+poke (blockpool.c:234).
\* Load-bearing: while bp_inflight>0 the backstop stays armed across the window.
WakerDec(g) ==
    /\ waker_pc[g] = "poked"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "done"]
    /\ bp_inflight' = bp_inflight - 1
    /\ UNCHANGED <<wake_list, poke_pending, drain_pc, drain_timeout, fiber_pc>>

----------------------------------------------------------------------------
\* ---- Drain (the single owner thread) ----

\* loop-top: a non-empty wake_list is drained to the ready ring (core.c:177-194).
\* This is the unconditional re-peek that makes wake_list durable + self-healing.
DrainConsume ==
    /\ drain_pc = "loop_top"
    /\ wake_list # {}
    /\ fiber_pc' = [g \in Gs |-> IF g \in wake_list THEN "ready" ELSE fiber_pc[g]]
    /\ wake_list' = {}
    /\ UNCHANGED <<poke_pending, bp_inflight, drain_pc, drain_timeout, waker_pc>>

\* loop-top: run a ready fiber (ready_pop + coro_resume, drain.c:317/406).
DrainResume ==
    /\ drain_pc = "loop_top"
    /\ wake_list = {}
    /\ HasReady
    /\ fiber_pc' = [g \in Gs |-> IF fiber_pc[g] = "ready" THEN "resumed"
                                                          ELSE fiber_pc[g]]
    /\ UNCHANGED <<wake_list, poke_pending, bp_inflight, drain_pc, drain_timeout,
                   waker_pc>>

\* loop-top with nothing to do: take the ACQUIRE-peek empty branch (drain.c:80).
\* This SNAPSHOT latches "nothing to drain" -- a WakerAppend an instant later is
\* not seen before the block.
DrainPeekEmpty ==
    /\ drain_pc = "loop_top"
    /\ wake_list = {}
    /\ ~HasReady
    /\ drain_pc' = "peeked"
    /\ UNCHANGED <<wake_list, poke_pending, bp_inflight, drain_timeout, fiber_pc,
                   waker_pc>>

\* foreign_park_inflight: a fiber that parked foreign-wakeable is "in flight"
\* from park until it is RESUMED (runloom_foreign_park_acquire/release) -- i.e.
\* while it is anything but "resumed".  This is the SECOND backstop term, and it
\* is load-bearing: bp_inflight (the blockpool job) can hit 0 the instant the
\* last waker decrements, leaving a freshly-appended g with a lost poke and an
\* unarmed backstop -> stranded.  TLC found exactly that lasso when the backstop
\* armed on bp_inflight ALONE.  foreign_park_inflight stays > 0 until the g is
\* actually resumed, so the real predicate -- bp_inflight>0 OR
\* foreign_park_inflight>0 (drain.c:192) -- keeps the cap armed across the whole
\* window.  Both terms are necessary; this models that.
ForeignParkInflight == \E g \in Gs : fiber_pc[g] # "resumed"

\* Decide the block timeout: the f214341 backstop caps it to 2 ms iff a foreign
\* job OR a foreign-wakeable parked fiber is in flight.  Backstop=FALSE forces
\* "infinite" (the regression).
DrainDecide ==
    /\ drain_pc = "peeked"
    /\ drain_pc' = "decided"
    /\ drain_timeout' = IF (Backstop /\ (bp_inflight > 0 \/ ForeignParkInflight))
                          THEN "backstop_2ms" ELSE "infinite"
    /\ UNCHANGED <<wake_list, poke_pending, bp_inflight, fiber_pc, waker_pc>>

\* Enter the pump (epoll_wait).  The drain is now asleep; the lost-poke window
\* has closed behind it.
DrainBlock ==
    /\ drain_pc = "decided"
    /\ drain_pc' = "blocked"
    /\ UNCHANGED <<wake_list, poke_pending, bp_inflight, drain_timeout, fiber_pc,
                   waker_pc>>

\* A DELIVERED poke wakes the drain: consume the eventfd level and re-loop to the
\* top, where DrainConsume re-drains wake_list.  This is how a poke heals.
DrainPumpWake ==
    /\ drain_pc = "blocked"
    /\ poke_pending = TRUE
    /\ drain_pc' = "loop_top"
    /\ poke_pending' = FALSE
    /\ UNCHANGED <<wake_list, bp_inflight, drain_timeout, fiber_pc, waker_pc>>

\* The 2 ms cap elapses (Backstop armed): epoll_wait returns 0 events, the drain
\* re-loops to the top and re-drains wake_list -- the self-heal for a LOST poke.
DrainBackstopTimeout ==
    /\ drain_pc = "blocked"
    /\ drain_timeout = "backstop_2ms"
    /\ drain_pc' = "loop_top"
    /\ UNCHANGED <<wake_list, poke_pending, bp_inflight, drain_timeout, fiber_pc,
                   waker_pc>>

\* The lost-wakeup lasso: asleep in epoll_wait(-1) with no pending poke and no
\* backstop -> nothing re-peeks wake_list -> stranded forever.
DrainStuck ==
    /\ drain_pc = "blocked"
    /\ poke_pending = FALSE
    /\ drain_timeout = "infinite"
    /\ UNCHANGED vars

----------------------------------------------------------------------------
Next ==
    \/ \E g \in Gs : WakerAppend(g)
    \/ \E g \in Gs : WakerPoke(g)
    \/ \E g \in Gs : WakerDec(g)
    \/ DrainConsume \/ DrainResume \/ DrainPeekEmpty
    \/ DrainDecide \/ DrainBlock \/ DrainPumpWake \/ DrainBackstopTimeout
    \/ DrainStuck

\* Weak fairness on every progress action (NOT on DrainStuck -- the lasso) so a
\* permanently-blocked drain is a real liveness violation, not an unfair stutter.
Fairness ==
    /\ \A g \in Gs : WF_vars(WakerAppend(g))
    /\ \A g \in Gs : WF_vars(WakerPoke(g))
    /\ \A g \in Gs : WF_vars(WakerDec(g))
    /\ WF_vars(DrainConsume) /\ WF_vars(DrainResume) /\ WF_vars(DrainPeekEmpty)
    /\ WF_vars(DrainDecide) /\ WF_vars(DrainBlock)
    /\ WF_vars(DrainPumpWake) /\ WF_vars(DrainBackstopTimeout)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: the ready/resumed lifecycle is monotone -- a resumed fiber is never
\* re-queued, and only an appended fiber is ever readied.
ResumeIsTerminal ==
    \A g \in Gs : (fiber_pc[g] = "resumed") => (g \notin wake_list)

\* LIVENESS (the property the backstop guarantees): every fiber a foreign waker
\* appended to wake_list is EVENTUALLY resumed -- no lost wake, no stranding.
AllWoken == <>[](\A g \in Gs : fiber_pc[g] = "resumed")
============================================================================
