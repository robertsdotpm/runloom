------------------------- MODULE RunloomIouringWake -------------------------
(***************************************************************************)
(* TLA+ model of runloom's IO_URING CQE wake protocol -- the route a fiber   *)
(* takes when it submits an SQE and parks, woken by a kernel completion.      *)
(* RunloomWake.tla models the single-thread FOREIGN-POKE route (a runloom     *)
(* thread os.write()s a pump eventfd; the hazard is a dedup-swallowed poke;    *)
(* the heal is a 2ms time cap).  The io_uring route differs on every axis and  *)
(* needs its own model (no TLA model existed; only the op->wait commit is      *)
(* GenMC-proven in tools/verify/genmc/iouring_waitcommit.c).                   *)
(*                                                                          *)
(* THE PROTOCOL.  A fiber submits an SQE (RELEASE sq_tail + io_uring_enter +   *)
(* inflight++, io_uring_l_buf.c.inc:128/:136) and parks on the op->wait        *)
(* INFLIGHT/PARKED/DONE commit.  The kernel posts a CQE into the CQ ring AND,  *)
(* because the eventfd is IORING_REGISTER_EVENTFD'd, signals that registered   *)
(* eventfd -> an EPOLLET edge the shared epoll holds.  The drainer's pump      *)
(* observes the edge, single-walks the CQ ring (the `draining` flag), RELEASE- *)
(* stores op->result, commits op->wait->DONE, and wakes the parked fiber.      *)
(* Unlike the foreign-poke eventfd, the registered eventfd has NO wake_pending  *)
(* dedup -- a CQE that lands in the VISIBLE ring reliably signals it.           *)
(*                                                                          *)
(* THE HAZARD: CQ-RING OVERFLOW.  When more completions arrive than the CQ     *)
(* ring holds (multishot under backpressure, or many concurrent completions),  *)
(* the kernel parks the excess in a kernel-side overflow backlog (NODROP) and  *)
(* CRUCIALLY does NOT re-signal the registered eventfd for a backlogged         *)
(* completion (io_uring_l_sys.c.inc:48-62).  A drainer that only walks the      *)
(* VISIBLE CQ ring never sees those completions; the fibers parked on them are  *)
(* never woken and inflight never balances.  The deadlock: the drain empties    *)
(* the visible CQ, then blocks waiting for an eventfd edge that will never come *)
(* -- and the only thing that could free CQ space is the very fiber stranded    *)
(* in the backlog (a real backpressure deadlock; observed: CQ empty +           *)
(* IORING_SQ_CQ_OVERFLOW set + asleep in epoll_wait(-1)).                       *)
(*                                                                          *)
(* THE FIX (what we prove): DRAIN-FIRST OVERFLOW FLUSH (not a 2ms time cap).   *)
(* Before ANY block, while runloom_iouring_inflight()>0, the drain calls        *)
(* runloom_iouring_drain FIRST, whose CQ-walk issues io_uring_enter(GETEVENTS)  *)
(* when IORING_SQ_CQ_OVERFLOW is set (io_uring_l_sys.c.inc:66-73), flushing the *)
(* backlog back into the VISIBLE ring; it then processes those CQEs and wakes   *)
(* the stranded waiters (runloom_sched_drain.c.inc:155-158).  CONSTANT Heal =   *)
(* TRUE (RunloomIouringWake.cfg): the flush makes the backlog visible before    *)
(* any unbounded block -> every completion is eventually consumed; AllWoken     *)
(* HOLDS.  Heal = FALSE (RunloomIouringWake_bug.cfg): no drain-first flush, so  *)
(* a completion stranded in overflow with the eventfd un-signalled and the      *)
(* drain blocked is lost forever -> AllWoken violated, the CQ-overflow lasso.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Gs,      \* parked fiber/op episodes, each woken by its own kernel CQE
          CQCAP,   \* visible CQ ring capacity (=1 in the cfg: 2 ops force overflow)
          Heal     \* TRUE -> drain-first overflow flush armed; FALSE -> the bug

VARIABLES
    cq_inflight,   \* SUBSET Gs : ops the kernel still owns (inflight_count), dec'd LAST
    cq_ring,       \* SUBSET Gs : completions VISIBLE in the CQ ring (|.| <= CQCAP)
    overflow,      \* SUBSET Gs : kernel-side NODROP backlog -- invisible + NO eventfd
    evfd_pending,  \* BOOLEAN  : the registered eventfd edge the shared epoll holds
    fiber_pc,      \* [Gs -> {"parked_inflight","completed_unseen","ready","resumed"}]
    waker_pc,      \* [Gs -> {"running","submitted","completed","drained"}] (op lifecycle)
    drain_pc,      \* the drainer's loop pc
    drain_mode     \* "block" or "flush_first" (the analogue of RunloomWake's drain_timeout)

vars == <<cq_inflight, cq_ring, overflow, evfd_pending, fiber_pc, waker_pc,
          drain_pc, drain_mode>>

DrainPCs == {"loop_top","peeked","decided","blocked"}

TypeOK ==
    /\ cq_inflight \subseteq Gs
    /\ cq_ring \subseteq Gs
    /\ Cardinality(cq_ring) <= CQCAP
    /\ overflow \subseteq Gs
    /\ evfd_pending \in BOOLEAN
    /\ fiber_pc \in [Gs -> {"parked_inflight","completed_unseen","ready","resumed"}]
    /\ waker_pc \in [Gs -> {"running","submitted","completed","drained"}]
    /\ drain_pc \in DrainPCs
    /\ drain_mode \in {"block","flush_first"}

Init ==
    /\ cq_inflight = Gs                 \* one in-flight op per fiber
    /\ cq_ring = {}
    /\ overflow = {}
    /\ evfd_pending = FALSE
    /\ fiber_pc = [g \in Gs |-> "parked_inflight"]
    /\ waker_pc = [g \in Gs |-> "running"]
    /\ drain_pc = "loop_top"
    /\ drain_mode = "block"

HasReady == \E g \in Gs : fiber_pc[g] = "ready"
IouringInflight == Cardinality(cq_inflight) > 0   \* runloom_iouring_inflight()>0 (drain.c:155)

----------------------------------------------------------------------------
\* ---- Submitter + kernel (one op per fiber g) ----

\* Submit the SQE: RELEASE sq_tail + io_uring_enter + inflight++ (l_buf:128/:136).
\* The DURABLE publish -- the ONLY step the negative control suppresses.
Submit(g) ==
    /\ waker_pc[g] = "running"
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "submitted"]
    /\ fiber_pc' = [fiber_pc EXCEPT ![g] = "parked_inflight"]
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, evfd_pending, drain_pc, drain_mode>>

\* The kernel posts the CQE.  If the VISIBLE ring has room it lands there AND the
\* kernel signals the registered eventfd (RELIABLE -- no wake_pending dedup on this
\* fd, unlike the foreign-poke route).  If the ring is FULL the completion goes to
\* the kernel overflow backlog and the eventfd is NOT signalled -- THE HAZARD
\* (io_uring_l_sys.c.inc:48-62).  cq_inflight is unchanged: the kernel still owns
\* the op until the drain consumes its CQE.
KernelComplete(g) ==
    /\ waker_pc[g] = "submitted"
    /\ g \in cq_inflight
    /\ waker_pc' = [waker_pc EXCEPT ![g] = "completed"]
    /\ fiber_pc' = [fiber_pc EXCEPT ![g] = "completed_unseen"]
    /\ IF Cardinality(cq_ring) < CQCAP
         THEN /\ cq_ring' = cq_ring \cup {g}
              /\ evfd_pending' = TRUE                 \* visible CQE -> eventfd signalled
              /\ overflow' = overflow
         ELSE /\ overflow' = overflow \cup {g}        \* backlog -> NO eventfd (hazard)
              /\ evfd_pending' = evfd_pending
              /\ cq_ring' = cq_ring
    /\ UNCHANGED <<cq_inflight, drain_pc, drain_mode>>

----------------------------------------------------------------------------
\* ---- Drainer (single-walker CQ pass; single-thread drain or an idle hub) ----

\* loop-top: single-walk the VISIBLE CQ ring -- RELEASE op->result, commit
\* op->wait->DONE, ready each completion (l_buf:182-241); the level drain re-arms
\* EPOLLET so evfd clears (l_buf:149-154); inflight decremented as the final CQE is
\* consumed (l_buf:238-241).  Whole visible ring at once (one walker).
DrainConsume ==
    /\ drain_pc = "loop_top"
    /\ cq_ring # {}
    /\ fiber_pc' = [g \in Gs |-> IF g \in cq_ring THEN "ready" ELSE fiber_pc[g]]
    /\ waker_pc' = [g \in Gs |-> IF g \in cq_ring THEN "drained" ELSE waker_pc[g]]
    /\ cq_inflight' = cq_inflight \ cq_ring
    /\ cq_ring' = {}
    /\ evfd_pending' = FALSE
    /\ UNCHANGED <<overflow, drain_pc, drain_mode>>

\* loop-top: the woken submitter resumes (l_buf:220-221 wake -> op.result read).
DrainResume ==
    /\ drain_pc = "loop_top"
    /\ cq_ring = {}
    /\ HasReady
    /\ fiber_pc' = [g \in Gs |-> IF fiber_pc[g] = "ready" THEN "resumed"
                                                          ELSE fiber_pc[g]]
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, evfd_pending, waker_pc,
                   drain_pc, drain_mode>>

\* loop-top with nothing VISIBLE to do: the SNAPSHOT.  A KernelComplete that lands
\* in overflow an instant later is unseen (no eventfd, not in the visible ring) --
\* the open edge of the overflow lost-wakeup window.
DrainPeekEmpty ==
    /\ drain_pc = "loop_top"
    /\ cq_ring = {}
    /\ ~HasReady
    /\ drain_pc' = "peeked"
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, evfd_pending, fiber_pc, waker_pc,
                   drain_mode>>

\* Decide the block mode: while the kernel still owns an op (inflight>0), Heal=TRUE
\* selects flush_first -- the drain-first GETEVENTS flush runs BEFORE any block
\* (runloom_sched_drain.c.inc:155).  Heal=FALSE forces a plain block (the
\* regression that dropped the drain-first flush).
DrainDecide ==
    /\ drain_pc = "peeked"
    /\ drain_pc' = "decided"
    /\ drain_mode' = IF (Heal /\ IouringInflight) THEN "flush_first" ELSE "block"
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, evfd_pending, fiber_pc, waker_pc>>

\* Enter the block (epoll_wait in the pump).  The drain is now asleep.
DrainBlock ==
    /\ drain_pc = "decided"
    /\ drain_pc' = "blocked"
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, evfd_pending, fiber_pc, waker_pc,
                   drain_mode>>

\* THE STRUCTURAL HEAL: io_uring_enter(GETEVENTS) flushes the kernel overflow
\* backlog back into the VISIBLE ring (io_uring_l_sys.c.inc:66-73), then the drain
\* re-loops to the top where DrainConsume processes them.  Moves a non-empty subset
\* that fits the ring (CQCAP).  Heals the lost-wakeup without any eventfd edge.
DrainFlushFirst ==
    /\ drain_pc = "blocked"
    /\ drain_mode = "flush_first"
    /\ overflow # {}
    /\ \E moved \in (SUBSET overflow) \ {{}} :
         /\ Cardinality(cq_ring) + Cardinality(moved) <= CQCAP
         /\ cq_ring' = cq_ring \cup moved
         /\ overflow' = overflow \ moved
    /\ drain_pc' = "loop_top"
    /\ UNCHANGED <<cq_inflight, evfd_pending, fiber_pc, waker_pc, drain_mode>>

\* A DELIVERED eventfd edge (a VISIBLE CQE was signalled) breaks the block; consume
\* the level and re-loop to the top where DrainConsume processes it.
DrainEvfdWake ==
    /\ drain_pc = "blocked"
    /\ evfd_pending = TRUE
    /\ drain_pc' = "loop_top"
    /\ evfd_pending' = FALSE
    /\ UNCHANGED <<cq_inflight, cq_ring, overflow, fiber_pc, waker_pc, drain_mode>>

\* Asleep in epoll_wait(-1) with no eventfd edge coming and no drain-first flush
\* (drain_mode="block") -- a self-loop (UNCHANGED vars), exactly like RunloomWake's
\* DrainStuck.  Two faces, distinguished by AllWoken, NOT by an extra guard:
\*   - BENIGN idle terminal: all fibers already resumed (under Heal=TRUE, reaching
\*     drain_mode="block" implies cq_inflight={} hence overflow={}, so everything is
\*     drained) -> the self-loop is harmless and AllWoken still holds.
\*   - THE LASSO (reachable only when Heal=FALSE): a completion stranded in the
\*     overflow backlog with the eventfd never firing and no flush -> that fiber is
\*     never resumed -> AllWoken violated.
DrainStuck ==
    /\ drain_pc = "blocked"
    /\ evfd_pending = FALSE
    /\ drain_mode = "block"
    /\ UNCHANGED vars

----------------------------------------------------------------------------
Next ==
    \/ \E g \in Gs : Submit(g)
    \/ \E g \in Gs : KernelComplete(g)
    \/ DrainConsume \/ DrainResume \/ DrainPeekEmpty
    \/ DrainDecide \/ DrainBlock
    \/ DrainFlushFirst \/ DrainEvfdWake \/ DrainStuck

\* Weak fairness on every progress action (NOT on DrainStuck -- the lasso).
Fairness ==
    /\ \A g \in Gs : WF_vars(Submit(g))
    /\ \A g \in Gs : WF_vars(KernelComplete(g))
    /\ WF_vars(DrainConsume) /\ WF_vars(DrainResume) /\ WF_vars(DrainPeekEmpty)
    /\ WF_vars(DrainDecide) /\ WF_vars(DrainBlock)
    /\ WF_vars(DrainFlushFirst) /\ WF_vars(DrainEvfdWake)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
\* SAFETY 1: a resumed fiber is never re-queued, re-completed, or still owned
\* (the op->wait PARKED-gate makes a double-resume impossible; l_buf:216-221).
ResumeIsTerminal ==
    \A g \in Gs : (fiber_pc[g] = "resumed") =>
                    /\ g \notin cq_ring
                    /\ g \notin overflow
                    /\ g \notin cq_inflight

\* SAFETY 2 (iouring-specific): a completion parked in the overflow backlog is
\* always still kernel-owned (in cq_inflight), so the inflight gate keeps the drain
\* loop alive while anything sits in the backlog (drain.c:68,:121,:165) -- there is
\* no stranded-and-forgotten completion.
NoStrandedCompletion ==
    \A g \in Gs : (g \in overflow) => (g \in cq_inflight)

\* LIVENESS (the property the drain-first flush guarantees): every kernel
\* completion -- including one forced into overflow -- is eventually consumed and
\* its fiber resumed.  Holds under Heal=TRUE; violated under Heal=FALSE.
AllWoken == <>[](\A g \in Gs : fiber_pc[g] = "resumed")
=============================================================================
