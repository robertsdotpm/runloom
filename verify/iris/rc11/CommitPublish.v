(* CommitPublish.v -- Stage 3 (weak memory): a machine-checked iRC11 / RC11
   separation-logic proof of runloom's netpoll commit-publish, the message-passing
   release/acquire pattern, under the RELAXED (RC11) memory model.

   Stages 1-2 (../OneShotWake.v, ../WakeQueue.v) are sequentially-consistent
   Iris.  This one is the real weak-memory tier: it uses iRC11 (gpfsl), whose
   HeapLang has the RC11 operational semantics with explicit release/acquire
   accesses, and proves the no-stale-read property the C relies on.

   The runloom shape (src/runloom_c/netpoll.c, and verify/litmus §15):
       claimer:  *ready_out = mask;          // publish the readiness (data)
                 <release the commit>        // commit-CAS / unlock pool->lock
       parker:   <acquire the commit>        // re-read under acquire
                 r = *ready_out;             // MUST observe mask, never stale 0

   Modelled as the canonical MP program: the claimer writes `ready` then does a
   RELEASE write of `commit`; the parker spins on an ACQUIRE read of `commit`
   and then reads `ready`.  The spec proves the parker reads 42 (the published
   value) and never the stale initial 0 -- i.e. the release/acquire pair is
   sufficient for the publish to be visible, under RC11.

   The proof is a faithful adaptation of gpfsl's own message-passing example
   (gpfsl-examples/mp/proof_gps.v): a GPS single-writer boolean protocol whose
   `true` state ships an escrow that transfers ownership of `ready ↦ 42`, so
   the acquiring reader obtains it.  A `coqc` PASS under gpfsl IS the proof.

   Run: verify/iris/rc11/run_rc11.sh  (needs the runloom-weakmem opam switch). *)

From stdpp Require Import namespaces.
From iris.algebra Require Import excl.
From iris.base_logic Require Import lib.own.
From iris.proofmode Require Import proofmode monpred.

From gpfsl.base_logic Require Import vprop.
From gpfsl.lang Require Export notation.
From gpfsl.logic Require Import proofmode lifting repeat_loop new_delete.
From gpfsl.gps Require Import surface_iSP protocols escrows.

Require Import iris.prelude.options.

(* ---- a unique (exclusive) token, inlined verbatim from gpfsl's
   gpfsl-examples/uniq_token.v so this file is self-contained ---- *)
Class uniqTokG Σ := UniqTokG { uniq_tokG : inG Σ (exclR unitO); }.
Local Existing Instances uniq_tokG.
Definition uniqTokΣ : gFunctors := #[GFunctor (constRF (exclR unitO))].
Global Instance subG_uniqTokΣ {Σ} : subG uniqTokΣ Σ → uniqTokG Σ.
Proof. solve_inG. Qed.

Section Tok.
  Context `{!uniqTokG Σ}.
  Notation vProp := (vProp Σ).
  Implicit Type (γ : gname).

  Definition UTok γ : vProp := ⎡ own γ (Excl ()) ⎤%I.

  #[global] Instance UTok_timeless γ : Timeless (UTok γ).
  Proof. apply _. Qed.
  #[global] Instance UTok_objective γ : Objective (UTok γ).
  Proof. apply _. Qed.

  Lemma UTok_alloc : ⊢ (|==> ∃ γ, UTok γ : vProp)%I.
  Proof.
    iStartProof. iMod (own_alloc (Excl ())) as (γ) "U"; [done|].
    iIntros "!>". by iExists γ.
  Qed.

  Lemma UTok_unique γ : UTok γ -∗ UTok γ -∗ False.
  Proof. iIntros "U1 U2". by iCombine "U1 U2" gives %?. Qed.
End Tok.

(* ---- the commit-publish program (message passing) ---- *)
Notation commit := 0 (only parsing).   (* the published flag: commit-CAS / unlock *)
Notation ready  := 1 (only parsing).   (* ready_out: the published readiness *)

Definition commit_publish : expr :=
  let: "p" := new [ #2] in
  "p" +ₗ #commit <- #0 ;;
  "p" +ₗ #ready  <- #0 ;;
  Fork ("p" +ₗ #ready <- #42 ;;          (* claimer: publish the readiness *)
        "p" +ₗ #commit <-ʳᵉˡ #1)          (* RELEASE: commit / unlock pool->lock *)
  ;;
  (repeat: !ᵃᶜ("p" +ₗ #commit)) ;;         (* parker: ACQUIRE the commit *)
  !("p" +ₗ #ready).                        (* read ready_out -> 42, never stale 0 *)

Definition cpN (n : loc) := nroot .@ "commitpublish" .@ n.

Definition cp_spec Σ `{!noprolG Σ} : Prop :=
  ∀ tid, {{{ True }}} commit_publish @ tid; ⊤ {{{ v, RET #v; ⌜v = 42⌝ }}}.

(* ---- protocol interpretation (adapted from mp/proof_gps.v) ---- *)
Section defs.
  Context `{!noprolG Σ, !uniqTokG Σ, gpG: !gpsG Σ boolProtocol}.
  Local Notation vProp := (vProp Σ).
  Implicit Types (x : loc) (γ : gname) (s : pr_stateT boolProtocol).

  (* the escrow: holding the unique token redeems ownership of ready ↦ 42 *)
  Definition XE x γ : vProp := [es UTok γ ⇝ x ↦ #42 ]%I.

  Definition YP x γ s (v : val) : vProp :=
    (match s with
     | false => ⌜v = #0⌝
     | true  => ⌜v = #1⌝ ∗ XE x γ
     end)%I.

  Definition cpInt x γ : interpO Σ boolProtocol := (λ _ _ _ _, YP x γ)%I.

  Global Instance YP_persistent x γ s v : Persistent (YP x γ s v).
  Proof. rewrite /Persistent. destruct s; by iIntros "#YP". Qed.
  Global Instance interp_persistent x γ b l γl t s v V :
    Persistent (cpInt x γ b l γl t s v V).
  Proof. destruct b; apply _. Qed.
End defs.

Section proof.
  Local Set Default Proof Using "All".
  Context `{!noprolG Σ, !uniqTokG Σ, gpG: !gpsG Σ boolProtocol, !atomicG Σ}.

  Lemma commit_publish_instance : cp_spec Σ.
  Proof.
    iIntros (tid Φ) "_ Post". rewrite /commit_publish.
    (* allocation *)
    wp_apply wp_new; [done..|].
    iIntros (p) "(DEL & p & Hp)".
    rewrite own_loc_na_vec_cons own_loc_na_vec_singleton.
    iDestruct "p" as "[p0 p1]".
    (* initialize commit:=0, ready:=0 *)
    wp_pures. rewrite shift_0. wp_write. wp_op. wp_write.
    (* construct the single-writer GPS protocol on the commit location *)
    iMod UTok_alloc as (γ) "Tok".
    iMod (GPS_iSP_Init (cpN p) (cpInt (p >> 1) γ) (cpInt (p >> 1) γ false) p false
            with "p0 []") as (γm tm) "W"; [done|].
    iDestruct (GPS_iSP_SWWriter_Reader with "W") as "#R".
    (* fork the claimer *)
    wp_apply (wp_fork with "[W p1]"); [done|..].
    - iIntros "!>" (tid').
      (* publish the readiness: ready := 42 *)
      wp_op. wp_write.
      (* wrap ready ↦ 42 in an escrow guarded by the unique token *)
      iMod (escrow_alloc (UTok γ) ((p >> 1) ↦ #42)%I with "[$p1]") as "#XE"; [done|].
      (* RELEASE write of commit := 1, shipping the escrow *)
      wp_op. rewrite shift_0.
      iApply (GPS_iSP_SWWrite (cpN p) (cpInt (p >> 1) γ) (cpInt (p >> 1) γ false)
                True%I _ AcqRel _ _ true _ #1 with "[$W]");
            [solve_ndisj|done|done|done|..].
      { iSplitL "". - iIntros "!>"; by iFrame "XE". - by iIntros "!> !> _". }
      by iIntros "!>" (?) "W".
    - iIntros "_". wp_seq. wp_bind (repeat: _)%E.
      (* parker: ACQUIRE-spin on commit until it reads 1 *)
      iLöb as "IH". iApply wp_repeat; [done|].
      wp_op. rewrite shift_0.
      iApply (GPS_iSP_Read (cpN p) (cpInt (p >> 1) γ) (cpInt (p >> 1) γ false)
                           (cpInt (p >> 1) γ false p γ) with "[$R]");
          [solve_ndisj|done|done|..].
      { iIntros "!>" (????) "!>". iSplit; last iSplit; by iIntros "#$". }
      iIntros "!>" (? s' v'). simpl.
      iDestruct 1 as "[% [R' Int]]". destruct s'; simpl; last first.
      { (* commit still 0: keep spinning *)
        iDestruct "Int" as %?. subst v'.
        iExists 0. iSplit; [done|]. simpl.
        by iApply ("IH" with "Post DEL Hp Tok"). }
      (* commit observed 1: redeem the escrow and read ready *)
      iDestruct "Int" as "[% XE]". subst v'.
      iExists 1. iSplit; [done|]. simpl. iIntros "!> !>". wp_seq.
      iMod (escrow_elim with "[] XE Tok") as "p1"; [done|..].
      { iIntros "[e1 e2]". by iApply (UTok_unique with "e1 e2"). }
      wp_op. wp_read. by iApply ("Post" $! 42).
  Qed.
End proof.
