(* Deque.v -- machine-checked, UNBOUNDED conservation for the Chase-Lev
   work-stealing deque.

   verify/spin/cldeque.pml proves no-loss / no-duplication for the deque by
   EXHAUSTIVE model checking, but only at a bounded instance (CAP=4, NITEMS=3,
   2 thieves), and verify/cbmc checks the real C at a bounded unwinding.  This
   Coq development proves the deque's conservation invariant over EVERY
   reachable state of the transition system -- any number of pushes, pops,
   steals, and contended last-element races -- via an inductive invariant.

   The state is the counting abstraction of cldeque.c: `top` (advanced by
   steals and by a winning last-element pop), `bottom` (push/pop end),
   `pushed` and `consumed` totals.  The single invariant

       consumed + (bottom - top) = pushed          (with top <= bottom <= pushed)

   simultaneously captures BOTH headline properties:
     - NO LOSS:           every pushed item is either still live in [top,bottom)
                          or has been consumed exactly once;
     - NO DOUBLE-CONSUME: `consumed` can never exceed what has been produced and
                          removed -- a second consume of the same item would
                          make consumed + (bottom-top) > pushed.

   The famous Chase-Lev subtlety -- pop-last racing steal for the single
   remaining element -- is captured by `consume_top` being ONE atomic guarded
   transition (`top < bottom` -> top++, consumed++): once it fires on the last
   item, top = bottom (empty), so no second consume of that item is enabled.

   Scope: this is the counting/index abstraction (like the Spin model but
   unbounded), not the per-slot buffer identity nor the C11 memory orders --
   those are covered by CBMC (real C with the __atomic ops) and herd7/GenMC (RC11).

   Build: coqc Deque.v   (verify/coq/run_coq.sh runs it). *)

From Stdlib Require Import Lia.

Record Deq : Set := mkDeq {
  top      : nat;
  bottom   : nat;
  pushed   : nat;
  consumed : nat
}.

Definition init : Deq := mkDeq 0 0 0 0.

Inductive step : Deq -> Deq -> Prop :=
  (* owner pushes one item at the bottom *)
  | S_push : forall t b p c,
      step (mkDeq t b p c) (mkDeq t (S b) (S p) c)
  (* owner pops with >= 2 elements: no contention, take from the bottom *)
  | S_pop_many : forall t b p c,
      t + 2 <= b ->
      step (mkDeq t b p c) (mkDeq t (b - 1) p (S c))
  (* steal, or a winning last-element pop: the single atomic CAS that advances
     top.  Enabled iff an element exists (top < bottom); afterwards top moves
     up, so the just-consumed item cannot be taken again. *)
  | S_consume_top : forall t b p c,
      t < b ->
      step (mkDeq t b p c) (mkDeq (S t) b p (S c)).

Definition Inv (d : Deq) : Prop :=
  top d <= bottom d
  /\ bottom d <= pushed d
  /\ consumed d + (bottom d - top d) = pushed d.

Lemma init_inv : Inv init.
Proof. unfold Inv, init; simpl; lia. Qed.

Lemma step_preserves : forall d d', Inv d -> step d d' -> Inv d'.
Proof.
  intros d d' HInv Hstep.
  destruct d as [t b p c].
  unfold Inv in HInv; cbn [top bottom pushed consumed] in HInv.
  destruct HInv as (H1 & H2 & H3).
  (* cbn on the projections only -- NOT simpl, which would rewrite `S b - t`
     into a `match` form lia cannot see as subtraction. *)
  inversion Hstep; subst; unfold Inv; cbn [top bottom pushed consumed]; lia.
Qed.

Inductive reachable : Deq -> Prop :=
  | reach_init : reachable init
  | reach_step : forall d d', reachable d -> step d d' -> reachable d'.

Theorem inv_reachable : forall d, reachable d -> Inv d.
Proof.
  intros d H; induction H.
  - apply init_inv.
  - eapply step_preserves; eauto.
Qed.

(* NO LOSS, stated directly: at every reachable state the consumed items plus
   the items still logically in the deque equal exactly what was pushed. *)
Theorem no_loss :
  forall d, reachable d -> consumed d + (bottom d - top d) = pushed d.
Proof. intros d H; apply inv_reachable in H; apply H. Qed.

(* NO DOUBLE-CONSUME / NO PHANTOM: never consume more than was produced. *)
Theorem no_overconsume :
  forall d, reachable d -> consumed d <= pushed d.
Proof. intros d H; apply inv_reachable in H; unfold Inv in H; lia. Qed.

(* Teeth: a buggy "consume" that increments `consumed` WITHOUT advancing `top`
   (the double-consume of the last element the CAS prevents) lands in a state
   the conservation invariant rejects. *)
Theorem invariant_has_teeth :
  Inv (mkDeq 0 1 1 0) /\ ~ Inv (mkDeq 0 1 1 1).
Proof.
  split.
  - unfold Inv; simpl; lia.
  - unfold Inv; simpl. intros (H1 & H2 & H3). lia.
Qed.
