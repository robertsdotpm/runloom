--------------------------- MODULE RunloomMNWake ---------------------------
(***************************************************************************)
(* TLA+ model of runloom's M:N HUB-SUBMIT wake protocol -- ROUTE A, the     *)
(* default mode (runloom_use_global_runq()==false).  This is the SIBLING of  *)
(* RunloomWake.tla: same six-layer skeleton, same nine must-mirror soundness *)
(* tricks, but a DIFFERENT state space because the M:N route differs from the *)
(* single-thread drain on the four axes the single-thread model cannot reach. *)
(*                                                                          *)
(* WHY A SEPARATE MODEL.  RunloomWake models ONE owner thread that re-peeks a *)
(* single shared wake_list and -- absent the f214341 fix -- blocks UNBOUNDED  *)
(* in epoll_wait(-1); its hazard is the unbounded block and its heal is a 2ms *)
(* decide-to-block cap.  An M:N hub is structurally different:                *)
(*   (1) DESTINATION is a PER-HUB sub-list h->sub_head keyed by g->park_hub,  *)
(*       OWNER-DRAINED: only the hub that owns park_hub ever drains that g, so *)
(*       a lost signal cannot be healed by any other thread re-peeking        *)
(*       (mn_sched_mn_api.c.inc:131-138, mn_sched_hub_main.c.inc:484-495).    *)
(*   (2) The block is NEVER epoll_wait(-1): every idle wait is TIMED          *)
(*       (pump_ns ~1ms base, hub_main:925; idle_cond cond_timedwait_ns,       *)
(*       hub_main:1014).  On each timeout the hub re-loops and UNCONDITIONALLY *)
(*       re-drains sub_head (hub_main:484) -- the "M:N backstop" named at      *)
(*       runloom_sched_drain.c.inc:186-188.  So the heal is the BOUNDED POLL,  *)
(*       intrinsic to the timed wait, not a special blockpool-gated cap.       *)
(*   (3) The SIGNAL is TWO independent free-delivery kicks: the per-hub        *)
(*       idle_cond signal (mn_api:165-169) AND the per-hub wake-pump eventfd / *)
(*       self-pipe (mn_api:184 -> netpoll_wake_iouring.c.inc:483-485).  EITHER *)
(*       delivered heals immediately; the lasso needs BOTH lost.              *)
(*   (4) The g lifecycle adds a try_incref queue-ref + in_sub_queue 0->1 CAS   *)
(*       dedup (mn_api:90,94; CBMC sched_qref_cbmc.c) -- abstracted here as    *)
(*       "append at most once per episode", which the per-episode waker_pc     *)
(*       already enforces.                                                    *)
(*                                                                          *)
(* THE HAZARD (mirrors RunloomWake's, re-pointed at the M:N sites).  The hub  *)
(* drains sub_head, finds it empty, and commits to a TIMED block.  In the      *)
(* instant between its empty re-check and the block, a foreign waker appends g *)
(* to sub_head (RELEASE, mn_api:138), bumps sub_gen, and fires BOTH kicks.    *)
(* Each kick can be LOST for a low-level reason the hub cannot prevent at this *)
(* layer: the wake-pump eventfd dedup (wake_pending 0->1 exchange swallows a   *)
(* second poke, netpoll_wake_iouring.c.inc:483), an idle_cond signal arriving  *)
(* before idle_waiting=1 is published, or a memory re-order of the kick vs the *)
(* block.  We ABSTRACT each as nondeterministic free delivery; the fidelity    *)
(* claim is only that a kick CAN be lost, not why -- all the backstop needs.   *)
(*                                                                          *)
(* THE FIX (what we are proving).  BoundedPoll=TRUE (RunloomMNWake.cfg): the   *)
(* idle wait is TIMED, so even with BOTH kicks lost the hub re-loops within    *)
(* one bounded poll tick and re-drains sub_head -> every appended fiber is     *)
(* eventually resumed; AllWoken HOLDS.  BoundedPoll=FALSE                      *)
(* (RunloomMNWake_bug.cfg): the regression where an idle hub blocks UNBOUNDED  *)
(* (e.g. a backend with no usable per-hub eventfd that fell back to an         *)
(* uninterruptible infinite wait, or a future change that dropped the timed    *)
(* cap) -- with both kicks lost the appended g is stranded on sub_head forever *)
(* while its owner hub sleeps -> AllWoken violated, the M:N lost-wakeup lasso. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Gs,         \* set of parked fiber ids; each woken by its own foreign waker
          BoundedPoll \* TRUE -> the idle hub's wait is TIMED (re-drains sub_head);
                      \* FALSE -> the regression: an idle hub blocks unbounded (bug)

VARIABLES
    sub_list,      \* SUBSET Gs : g's appended to their owner hub's sub_head, not yet drained
    idle_cond,     \* BOOLEAN : the per-hub idle_cond signal is pending (free delivery)
    pump_efd,      \* BOOLEAN : the per-hub wake-pump eventfd/self-pipe is pending (free)
    fp_inflight,   \* Nat : foreign-wakeable parked fibers in flight (foreign_park_count)
    hub_pc,        \* the owner hub's main-loop program counter
    hub_timeout,   \* "pump_1ms" (TIMED -- re-drains) or "infinite" (the BoundedPoll=FALSE bug)
    fiber_pc,      \* [Gs -> {"parked","on_sub_list","ready","resumed"}]
    waker_pc       \* [Gs -> {"running","appended","signalled","done"}]  (waker for g)

vars == <<sub_list, idle_cond, pump_efd, fp_inflight, hub_pc, hub_timeout,
          fiber_pc, waker_pc>>

HubPCs == {"loop_top","drained_empty","decided","blocked"}

TypeOK ==
    /\ sub_list \subseteq Gs
    /\ idle_cond \in BOOLEAN
    /\ pump_efd \in BOOLEAN
    /\ fp_inflight \in 0..Cardinality(Gs)
    /\ hub_pc \in HubPCs
    /\ hub_timeout \in {"pump_1ms","infinite"}
    /\ fiber_pc \in [Gs -> {"parked","on_sub_list","ready","resumed"}]
    /\ waker_pc \in [Gs -> {"running","appended","signalled","done"}]

Init ==
    /\ sub_list = {}
    /\ idle_cond = FALSE
    /\ pump_efd = FALSE
    /\ fp_inflight = Cardinality(Gs)       \* one foreign-wakeable park per fiber
    /\ hub_pc = "loop_top"
    /\ hub_timeout = "pump_1ms"
    /\ fiber_pc = [g \in Gs |-> "parked"]
    /\ waker_pc = [g \in Gs |-> "running"]

HasReady == \E g \in Gs : fiber_pc[g] = "ready"
AnyKick  == idle_cond \/ pump_efd          \* either free-delivery signal pending

----------------------------------------------------------------------------
\* ---- Foreign waker (one per fiber g): wake_safe -> runloom_mn_wake_g -> hub_submit ----

\* L2 step 1: append g to its owner hub's sub_head with a RELEASE store -- the
\* DURABLE publish, BEFORE the signal (mn_sched_mn_api.c.inc:131-138).  This is
\* the ONLY step the negative control suppresses.  The try_incref queue-ref +
\* in_sub_queue 0->1 CAS dedup (mn_api:90,94) are abstracted as "at most once per
\* episode", which waker_pc="running" already enforces.
WakerAppend(g) ==
    /\ waker_pc[g] = "running"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "appended"]
    /\ sub_list' = sub_list \cup {g}
    /\ fiber_pc' = [fiber_pc EXCEPT ![g] = "on_sub_list"]
    /\ UNCHANGED <<idle_cond, pump_efd, fp_inflight, hub_pc, hub_timeout>>

\* L2 step 2: fire the TWO independent kicks.  sub_gen is bumped RELEASE first
\* (mn_api:152), then BOTH the idle_cond signal (mn_api:165-169) and the
\* unconditional wake-pump eventfd (mn_api:184) are sent.  Each has FREE delivery:
\* idle_cond' may stay or become TRUE, pump_efd' may stay or become TRUE -- the
\* abstracted hazard that a kick CAN be lost (dedup-suppressed / re-ordered /
\* signalled before idle_waiting=1).  The g is already durably on sub_list, so a
\* delivered kick and a lost-but-re-polled kick both heal; both lost + unbounded
\* block does not.
WakerSignal(g) ==
    /\ waker_pc[g] = "appended"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "signalled"]
    /\ idle_cond' \in {idle_cond, TRUE}
    /\ pump_efd'  \in {pump_efd,  TRUE}
    /\ UNCHANGED <<sub_list, fp_inflight, hub_pc, hub_timeout, fiber_pc>>

\* L2 step 3: the waker is done.  Unlike the single-thread blockpool path there
\* is NO bp_inflight decrement gating the M:N backstop -- the heal is the timed
\* poll, armed intrinsically.  But foreign_park_inflight (fp_inflight) is the
\* M:N second-inflight term that keeps the deadlock-census ALIVE across the whole
\* submit->drain window: it stays >0 from park (foreign_park_acquire,
\* parkwake.c:182) until the g is RESUMED, and the hub run-alive predicate
\* (mn_sched_init_fini.c.inc:1066) holds the scheduler open while it is >0.  We
\* decrement it at RESUME (see HubResume), not here, mirroring the C lifetime.
WakerDone(g) ==
    /\ waker_pc[g] = "signalled"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "done"]
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, fp_inflight, hub_pc, hub_timeout,
                   fiber_pc>>

----------------------------------------------------------------------------
\* ---- Owner hub main loop (runloom_hub_main) ----

\* L3 loop-top: drain a non-empty sub_head under sub_lock; snap.valid woken gs go
\* to the local ready FIFO (mn_sched_hub_main.c.inc:484-550).  The UNCONDITIONAL
\* re-drain every loop is what makes sub_head self-healing -- a lost kick costs at
\* most one bounded poll tick.
HubDrain ==
    /\ hub_pc = "loop_top"
    /\ sub_list # {}
    /\ fiber_pc' = [g \in Gs |-> IF g \in sub_list THEN "ready" ELSE fiber_pc[g]]
    /\ sub_list' = {}
    /\ UNCHANGED <<idle_cond, pump_efd, fp_inflight, hub_pc, hub_timeout, waker_pc>>

\* L3 loop-top: pick the ready FIFO first (ready_pop, hub_main:646) and resume
\* (coro_resume, hub_main:1394).  At resume in_sub_queue is exchanged 1->0
\* (hub_main:1284) and the queue ref dropped (hub_main:1412).  The resumed g's
\* foreign_park_release runs as it returns from park_generic, so fp_inflight
\* decrements HERE -- the C lifetime: acquired at park, released at resume.
HubResume ==
    /\ hub_pc = "loop_top"
    /\ sub_list = {}
    /\ HasReady
    /\ LET nready == Cardinality({g \in Gs : fiber_pc[g] = "ready"}) IN
         /\ fiber_pc' = [g \in Gs |-> IF fiber_pc[g] = "ready" THEN "resumed"
                                                               ELSE fiber_pc[g]]
         /\ fp_inflight' = fp_inflight - nready
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, hub_pc, hub_timeout, waker_pc>>

\* L3 loop-top with nothing to do: the SNAPSHOT.  The hub drained sub_head empty
\* and latched "nothing to run" (hub_main:484 drain found empty + no ready) -- a
\* WakerAppend an instant later is NOT seen before the block.  This is the open
\* edge of the lost-signal window.
HubDrainEmpty ==
    /\ hub_pc = "loop_top"
    /\ sub_list = {}
    /\ ~HasReady
    /\ hub_pc' = "drained_empty"
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, fp_inflight, hub_timeout,
                   fiber_pc, waker_pc>>

\* fp_inflight is the M:N SECOND-INFLIGHT arming term, exactly analogous to
\* RunloomWake's ForeignParkInflight.  It is >0 while ANY foreign-wakeable fiber
\* is parked-but-not-resumed (foreign_park_count, parkwake.c:155).  An idle M:N
\* hub announces idle / arms its wake-pump only while it still owns undrained
\* foreign-park work; the run-alive predicate (init_fini:1066) keeps the hub
\* spinning its bounded poll while fp_inflight>0, so the timed re-drain stays
\* armed across the WHOLE submit->drain window even if the last waker has already
\* finished signalling.  (In the C the hub's pump is gated on parked>0 ||
\* iouring_inflight>0, hub_main:924; fp_inflight is the model's faithful witness
\* that there is still owner work that MUST be re-polled.)
ForeignParkInflight == fp_inflight > 0

\* L3 decide the block timeout.  BoundedPoll=TRUE: a TIMED pump_1ms wait whenever
\* there is still foreign-park work, so the hub re-loops and re-drains within the
\* bound (the heal).  BoundedPoll=FALSE: the regression -- an idle hub that blocks
\* "infinite" (no usable timed wake), the M:N analogue of epoll_wait(-1).
HubDecide ==
    /\ hub_pc = "drained_empty"
    /\ hub_pc' = "decided"
    /\ hub_timeout' = IF (BoundedPoll /\ ForeignParkInflight)
                        THEN "pump_1ms" ELSE "infinite"
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, fp_inflight, fiber_pc, waker_pc>>

\* L3 enter the block (netpoll_pump(pump_ns) / cond_timedwait_ns, hub_main:978/1014).
\* The hub is now asleep; the lost-signal window has closed behind it.
HubBlock ==
    /\ hub_pc = "decided"
    /\ hub_pc' = "blocked"
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, fp_inflight, hub_timeout,
                   fiber_pc, waker_pc>>

\* L4 route 1: a DELIVERED kick (either the wake-pump eventfd OR the idle_cond
\* signal) breaks the wait; consume it and re-loop to the top where HubDrain
\* re-drains sub_head.  This is the fast-path heal (microseconds).
HubKickWake ==
    /\ hub_pc = "blocked"
    /\ AnyKick
    /\ hub_pc' = "loop_top"
    /\ idle_cond' = FALSE                  \* consuming the wait clears both edges
    /\ pump_efd'  = FALSE
    /\ UNCHANGED <<sub_list, fp_inflight, hub_timeout, fiber_pc, waker_pc>>

\* L4 route 2: the pump_1ms timeout elapses (BoundedPoll armed): the timed wait
\* returns 0 events, the hub re-loops and re-drains sub_head -- the M:N self-heal
\* for a LOST kick (runloom_sched_drain.c.inc:186 "they busy-poll ~1ms").
HubPollTimeout ==
    /\ hub_pc = "blocked"
    /\ hub_timeout = "pump_1ms"
    /\ hub_pc' = "loop_top"
    /\ UNCHANGED <<sub_list, idle_cond, pump_efd, fp_inflight, hub_timeout,
                   fiber_pc, waker_pc>>

\* L4 the lasso: an idle hub blocked with NEITHER kick pending AND an unbounded
\* timeout -> nothing re-drains sub_head -> the appended g is stranded forever.
\* Reachable ONLY when BoundedPoll=FALSE (the regression that dropped the timed
\* wait): with BoundedPoll=TRUE the timeout is always pump_1ms while fp_inflight>0,
\* so HubPollTimeout is always enabled and this is unreachable.
HubStuck ==
    /\ hub_pc = "blocked"
    /\ ~AnyKick
    /\ hub_timeout = "infinite"
    /\ UNCHANGED vars

----------------------------------------------------------------------------
Next ==
    \/ \E g \in Gs : WakerAppend(g)
    \/ \E g \in Gs : WakerSignal(g)
    \/ \E g \in Gs : WakerDone(g)
    \/ HubDrain \/ HubResume \/ HubDrainEmpty
    \/ HubDecide \/ HubBlock \/ HubKickWake \/ HubPollTimeout
    \/ HubStuck

\* Weak fairness on every progress action (NOT on HubStuck -- the lasso) so a
\* permanently-blocked hub is a real liveness violation, not an unfair stutter.
Fairness ==
    /\ \A g \in Gs : WF_vars(WakerAppend(g))
    /\ \A g \in Gs : WF_vars(WakerSignal(g))
    /\ \A g \in Gs : WF_vars(WakerDone(g))
    /\ WF_vars(HubDrain) /\ WF_vars(HubResume) /\ WF_vars(HubDrainEmpty)
    /\ WF_vars(HubDecide) /\ WF_vars(HubBlock)
    /\ WF_vars(HubKickWake) /\ WF_vars(HubPollTimeout)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY: the ready/resumed lifecycle is monotone -- a resumed fiber is never
\* still on a hub sub-list, and only an appended fiber is ever readied.  This is
\* the M:N ResumeIsTerminal, identical in spirit to RunloomWake's (a resumed g is
\* never re-queued; the in_sub_queue dedup + queue-ref make a double-resume
\* impossible in the C, this asserts the model never violates it).
ResumeIsTerminal ==
    \A g \in Gs : (fiber_pc[g] = "resumed") => (g \notin sub_list)

\* LIVENESS (the property the bounded poll guarantees): every fiber a foreign
\* waker appended to its owner hub's sub-list is EVENTUALLY resumed -- no lost
\* wake, no stranding.  Holds under BoundedPoll=TRUE; violated under
\* BoundedPoll=FALSE (the unbounded-block regression).
AllWoken == <>[](\A g \in Gs : fiber_pc[g] = "resumed")
============================================================================
