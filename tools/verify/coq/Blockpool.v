(* Blockpool.v -- machine-checked, UNBOUNDED proof of the blocking-offload
   wake-order invariant.

   verify/spin/blockpool.pml proves (bounded) that the DNS/blocking-offload
   path is safe ONLY because each worker re-queues its goroutine BEFORE
   decrementing the `inflight` counter: the single-thread drain blocks in
   epoll_wait and stays alive while inflight > 0, so the instant inflight hits
   0 every offloaded goroutine must already be back on the ready queue -- else
   the drain exits and strands it (a lost wake).

   This Coq development proves the same over EVERY reachable state, for ANY
   number of offloaded jobs.  Counters: `offloaded` (jobs sent to workers),
   `requeued` (goroutines put back on the ready queue), `inflight` (jobs a
   worker still holds).  The inductive invariant

       requeued <= offloaded  /\  offloaded <= requeued + inflight

   says every offloaded job is either already re-queued or still in flight.
   Its corollary at inflight = 0 is the safety property: the drain may observe
   inflight = 0 only when offloaded = requeued, i.e. nothing is stranded.

   The order discipline lives in the `dec_inflight` guard: a worker may
   decrement only when there is slack (offloaded < requeued + inflight), which
   holds exactly when the job it is finishing was already re-queued.  The
   buggy order (decrement before re-queue) drops that guard and breaks the
   invariant -- the teeth.

   Build: coqc Blockpool.v  (verify/coq/run_coq.sh runs it). *)

From Stdlib Require Import Lia.

Record Pool : Set := mkPool {
  offloaded : nat;
  requeued  : nat;
  inflight  : nat
}.

Definition init : Pool := mkPool 0 0 0.

Inductive step : Pool -> Pool -> Prop :=
  (* a goroutine offloads a blocking job to a worker *)
  | S_offload : forall o r f,
      step (mkPool o r f) (mkPool (S o) r (S f))
  (* a worker re-queues its goroutine onto the ready queue *)
  | S_requeue : forall o r f,
      r < o ->
      step (mkPool o r f) (mkPool o (S r) f)
  (* a worker decrements inflight -- ONLY with slack, i.e. AFTER re-queue *)
  | S_dec : forall o r f,
      o < r + f ->
      step (mkPool o r f) (mkPool o r (f - 1)).

Definition Inv (p : Pool) : Prop :=
  requeued p <= offloaded p /\ offloaded p <= requeued p + inflight p.

Lemma init_inv : Inv init.
Proof. unfold Inv, init; simpl; lia. Qed.

Lemma step_preserves : forall p p', Inv p -> step p p' -> Inv p'.
Proof.
  intros p p' HInv Hstep.
  destruct p as [o r f].
  unfold Inv in HInv; cbn [offloaded requeued inflight] in HInv.
  destruct HInv as (H1 & H2).
  inversion Hstep; subst; unfold Inv; cbn [offloaded requeued inflight]; lia.
Qed.

Inductive reachable : Pool -> Prop :=
  | reach_init : reachable init
  | reach_step : forall p p', reachable p -> step p p' -> reachable p'.

Theorem inv_reachable : forall p, reachable p -> Inv p.
Proof.
  intros p H; induction H.
  - apply init_inv.
  - eapply step_preserves; eauto.
Qed.

(* NO LOST WAKE: when the drain observes inflight = 0, every offloaded
   goroutine has already been re-queued -- none is stranded. *)
Theorem drain_safe :
  forall p, reachable p -> inflight p = 0 -> offloaded p = requeued p.
Proof.
  intros p H Hf; apply inv_reachable in H; unfold Inv in H; lia.
Qed.

(* Teeth: the buggy order (decrement inflight BEFORE re-queue, i.e. without the
   slack guard) reaches a state the invariant rejects -- the drain could exit
   with a goroutine still un-re-queued. *)
Theorem invariant_has_teeth :
  Inv (mkPool 1 0 1) /\ ~ Inv (mkPool 1 0 0).
Proof.
  split.
  - unfold Inv; simpl; lia.
  - unfold Inv; simpl. intros (H1 & H2). lia.
Qed.
