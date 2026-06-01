(* WakeState.v -- a machine-checked, UNBOUNDED proof of the per-g wake_state
   machine's safety invariants.

   verify/spin/wake_state.pml proves the same invariants by EXHAUSTIVE model
   checking, but only over a BOUNDED instance (2 wakers / 2 hubs / 1 sweeper);
   the verify/ README is honest that "they are bounds".  This Coq development
   proves the two structural invariants hold over EVERY reachable state of the
   transition system -- no bound on the number of transitions or actors -- by
   exhibiting an inductive invariant.  That is the "machine-checked, unbounded"
   assurance the roadmap (verify/extra/README.md) lists, delivered for this
   primitive.

   (Scope: this is a proof over the protocol's transition system, like the Spin
   model but unbounded -- it does NOT model the C11 weak-memory orderings.
   A full Iris/FSL++ weak-memory separation-logic proof of the lock-free C
   remains the deeper, multi-week item.)

   The two invariants mirror the pml's check_inv():
     INV1  qentries = (state == QUEUED ? 1 : 0)   -- no dup / orphan runq entry
     INV2  owners   = (state in {RUNNING,RUNNING_WOKEN} ? 1 : 0)  -- <= 1 owner,
           i.e. no double resume.

   Build:  coqc WakeState.v     (verify/coq/run_coq.sh wraps it) *)

From Stdlib Require Import Lia.

(* The six wake states, exactly as in pygo_sched.h / wake_state.pml. *)
Inductive WState : Set :=
  | Parked
  | Queued
  | Running
  | RunningWoken
  | Sweeping
  | SweepingWoken.

Record Config : Set := mkConfig {
  st       : WState;
  qentries : nat;    (* run-queue entries referencing this g *)
  owners   : nat;    (* hubs currently owning (running) this g *)
  unserved : bool    (* a wake was issued and not yet served *)
}.

Definition init : Config := mkConfig Parked 0 0 false.

(* The transition relation: one constructor per atomic action in the pml,
   covering every actor (waker / hub pull+release / sweeper claim+release).
   Quantified over arbitrary q,o,u: the *invariant*, not the relation, is what
   constrains them -- preservation is proved using the invariant hypothesis. *)
Inductive step : Config -> Config -> Prop :=
  (* waker (wake_g): mark unserved, then move the state *)
  | S_wake_parked    : forall q o u, step (mkConfig Parked q o u)        (mkConfig Queued (S q) o true)
  | S_wake_sweeping  : forall q o u, step (mkConfig Sweeping q o u)      (mkConfig SweepingWoken q o true)
  | S_wake_running   : forall q o u, step (mkConfig Running q o u)       (mkConfig RunningWoken q o true)
  | S_wake_drop_q    : forall q o u, step (mkConfig Queued q o u)        (mkConfig Queued q o true)
  | S_wake_drop_rw   : forall q o u, step (mkConfig RunningWoken q o u)  (mkConfig RunningWoken q o true)
  | S_wake_drop_sw   : forall q o u, step (mkConfig SweepingWoken q o u) (mkConfig SweepingWoken q o true)
  (* hub: pull QUEUED -> RUNNING (serves the wake), then release *)
  | S_pull           : forall q o u, step (mkConfig Queued q o u)        (mkConfig Running (q - 1) (S o) false)
  | S_rel_running    : forall q o u, step (mkConfig Running q o u)       (mkConfig Parked q (o - 1) u)
  | S_rel_runwoken   : forall q o u, step (mkConfig RunningWoken q o u)  (mkConfig Queued (S q) (o - 1) u)
  (* idle-stack sweeper: claim PARKED -> SWEEPING, then release *)
  | S_sweep_claim    : forall q o u, step (mkConfig Parked q o u)        (mkConfig Sweeping q o u)
  | S_sweep_rel      : forall q o u, step (mkConfig Sweeping q o u)      (mkConfig Parked q o u)
  | S_sweep_rel_wkn  : forall q o u, step (mkConfig SweepingWoken q o u) (mkConfig Queued (S q) o u).

(* The inductive invariant. *)
Definition isQ (s : WState) : nat := match s with Queued => 1 | _ => 0 end.
Definition isR (s : WState) : nat := match s with Running | RunningWoken => 1 | _ => 0 end.

Definition Inv (c : Config) : Prop :=
  qentries c = isQ (st c) /\ owners c = isR (st c).

Lemma init_inv : Inv init.
Proof. unfold Inv, init; simpl; split; reflexivity. Qed.

(* The heart: every transition preserves the invariant.  This is what makes the
   bound disappear -- it holds for any q,o reachable, not a fixed cardinality. *)
Lemma step_preserves : forall c c', Inv c -> step c c' -> Inv c'.
Proof.
  intros c c' [Hq Ho] Hstep.
  inversion Hstep; subst; unfold Inv in *; simpl in *; split; lia.
Qed.

(* Reachability and the unbounded safety theorem. *)
Inductive reachable : Config -> Prop :=
  | reach_init : reachable init
  | reach_step : forall c c', reachable c -> step c c' -> reachable c'.

Theorem inv_reachable : forall c, reachable c -> Inv c.
Proof.
  intros c H; induction H.
  - apply init_inv.
  - eapply step_preserves; eauto.
Qed.

(* INV1 / INV2 as standalone corollaries over ALL reachable states. *)
Theorem no_dup_or_orphan_runq_entry :
  forall c, reachable c -> qentries c = isQ (st c).
Proof. intros c H; apply inv_reachable in H; apply H. Qed.

Theorem no_double_resume :
  forall c, reachable c -> owners c <= 1.
Proof.
  intros c H; apply inv_reachable in H; destruct H as [_ Ho].
  rewrite Ho; destruct (st c); simpl; lia.
Qed.

(* Teeth: the invariant is non-trivial.  A buggy hub-pull that fails to
   DECREMENT qentries (the exact INV1 violation the Spin assert guards) lands
   in a state the invariant rejects -- machine-checked. *)
Theorem invariant_has_teeth :
  Inv (mkConfig Queued 1 0 false) /\
  ~ Inv (mkConfig Running 1 1 false).   (* buggy pull: qentries kept at 1 *)
Proof.
  split.
  - unfold Inv; simpl; split; reflexivity.
  - unfold Inv; simpl; intros [Hbad _]; discriminate Hbad.
Qed.
