(* Select.v -- machine-checked, UNBOUNDED proof of the select() claim CAS.

   verify/spin/select_claim.pml proves, by bounded model checking, that the
   lock-free core of `select` fires at most one case: a goroutine parked on N
   channels shares one `fired_case`, channels race to CAS it from "none" to
   their own index, and only the winner does the handoff (chan.c waiter_claim).
   This Coq development proves the same safety property over EVERY reachable
   state of the transition system, for ANY number of racing channels, via an
   inductive invariant.

   State: `fired` (0 = no case has fired yet; S i = case i won) and `wins` (how
   many channels believe they won the claim).  The single invariant ties them:

       (fired = 0  /\ wins = 0)  \/  (fired <> 0  /\ wins = 1)

   which gives FIRES-AT-MOST-ONE-CASE (wins <= 1) and EXACTLY-ONCE-WAKE (a
   fired select has exactly one winner) at every reachable state, unbounded.

   The CAS is the single atomic guarded transition `claim_win` (enabled only
   while `fired = 0`); once it fires, `fired <> 0` so no second channel can win.

   Build: coqc Select.v  (verify/coq/run_coq.sh runs it). *)

From Stdlib Require Import Lia.

Record Sel : Set := mkSel {
  fired : nat;   (* 0 = none fired yet; S i = case i is the winner *)
  wins  : nat    (* number of channels that claimed the win *)
}.

Definition init : Sel := mkSel 0 0.

Inductive step : Sel -> Sel -> Prop :=
  (* a channel wins the claim CAS: enabled only while no case has fired *)
  | S_claim_win : forall i w,
      step (mkSel 0 w) (mkSel (S i) (S w))
  (* a channel loses (a case already fired): leaves a tombstone, no wake *)
  | S_claim_lose : forall f w,
      f <> 0 ->
      step (mkSel f w) (mkSel f w).

Definition Inv (s : Sel) : Prop :=
  (fired s = 0 /\ wins s = 0) \/ (fired s <> 0 /\ wins s = 1).

Lemma init_inv : Inv init.
Proof. unfold Inv, init; simpl; left; lia. Qed.

Lemma step_preserves : forall s s', Inv s -> step s s' -> Inv s'.
Proof.
  intros s s' HInv Hstep.
  destruct s as [f w].
  inversion Hstep; subst; unfold Inv in *; simpl in *.
  - (* claim_win: source was unfired (fired = 0) -> now fired, exactly one win *)
    destruct HInv as [[_ Hw] | [Hf _]].
    + right; split; [discriminate | lia].   (* Hw : w = 0, so S w = 1 *)
    + congruence.                            (* Hf : 0 <> 0 is absurd *)
  - (* claim_lose: state unchanged *)
    assumption.
Qed.

Inductive reachable : Sel -> Prop :=
  | reach_init : reachable init
  | reach_step : forall s s', reachable s -> step s s' -> reachable s'.

Theorem inv_reachable : forall s, reachable s -> Inv s.
Proof.
  intros s H; induction H.
  - apply init_inv.
  - eapply step_preserves; eauto.
Qed.

(* FIRES AT MOST ONE CASE: never more than one winner, at every reachable state. *)
Theorem fires_at_most_once :
  forall s, reachable s -> wins s <= 1.
Proof.
  intros s H; apply inv_reachable in H.
  destruct H as [[_ Hw] | [_ Hw]]; lia.
Qed.

(* EXACTLY-ONCE WAKE: a fired select has exactly one winner. *)
Theorem fired_has_one_winner :
  forall s, reachable s -> fired s <> 0 -> wins s = 1.
Proof.
  intros s H Hf; apply inv_reachable in H.
  destruct H as [[Hf0 _] | [_ Hw]]; [contradiction | exact Hw].
Qed.

(* Teeth: a buggy claim that increments `wins` WITHOUT the fired=0 guard (a
   second channel "winning" after a case already fired) reaches wins=2, which
   the invariant rejects. *)
Theorem invariant_has_teeth :
  Inv (mkSel 1 1) /\ ~ Inv (mkSel 1 2).
Proof.
  split.
  - right; simpl; split; [discriminate | reflexivity].
  - unfold Inv; simpl. intros [[Hf _] | [_ Hw]]; [discriminate | lia].
Qed.
