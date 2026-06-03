(* WakeListHandoff.v -- Stage 3 (weak memory): an iRC11 / RC11 proof of runloom's
   CROSS-THREAD wake_list handoff, the second load-bearing release/acquire fence
   (the one the herd7 `wakelist_mpsc` litmus and the Phase-C owner-routed
   wake_safe cover).

   CommitPublish.v proves the netpoll commit publish; this proves the OTHER
   weak-memory site, where a goroutine is woken from a FOREIGN OS thread:

       waker (foreign thread):  publish the woken g's state (gstate := ready);
                                <release the wake_list lock>
       owner (drain thread):    <acquire the wake_list lock>;
                                read gstate  -- MUST see the published value

   It is the same message-passing release/acquire CORE as CommitPublish (so the
   proof shares its shape), but at a distinct runloom fence: cross-thread wake
   routing rather than the pump's commit.  Together the two give iRC11
   coverage of BOTH load-bearing fences the litmus tests isolate.

   The spec proves the owner reads the published readiness (42), never the stale
   0, under RC11.  Adapted from gpfsl-examples/mp; unique token inlined.

   Run: verify/iris/rc11/run_rc11.sh  (needs the pygo-weakmem gpfsl switch). *)

From stdpp Require Import namespaces.
From iris.algebra Require Import excl.
From iris.base_logic Require Import lib.own.
From iris.proofmode Require Import proofmode monpred.

From gpfsl.base_logic Require Import vprop.
From gpfsl.lang Require Export notation.
From gpfsl.logic Require Import proofmode lifting repeat_loop new_delete.
From gpfsl.gps Require Import surface_iSP protocols escrows.

Require Import iris.prelude.options.

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
  Proof. iStartProof. iMod (own_alloc (Excl ())) as (γ) "U"; [done|]. iIntros "!>". by iExists γ. Qed.
  Lemma UTok_unique γ : UTok γ -∗ UTok γ -∗ False.
  Proof. iIntros "U1 U2". by iCombine "U1 U2" gives %?. Qed.
End Tok.

Notation wl_lock := 0 (only parsing).   (* the wake_list lock / link flag *)
Notation gstate  := 1 (only parsing).   (* the woken goroutine's published state *)

Definition wakelist_handoff : expr :=
  let: "p" := new [ #2] in
  "p" +ₗ #wl_lock <- #0 ;;
  "p" +ₗ #gstate  <- #0 ;;
  Fork ("p" +ₗ #gstate  <- #42 ;;          (* foreign waker: publish g's state *)
        "p" +ₗ #wl_lock <-ʳᵉˡ #1)           (* RELEASE the wake_list lock *)
  ;;
  (repeat: !ᵃᶜ("p" +ₗ #wl_lock)) ;;          (* owner drain: ACQUIRE the lock *)
  !("p" +ₗ #gstate).                         (* read g state -> 42, never stale 0 *)

Definition wlN (n : loc) := nroot .@ "wakelist" .@ n.

Definition wl_spec Σ `{!noprolG Σ} : Prop :=
  ∀ tid, {{{ True }}} wakelist_handoff @ tid; ⊤ {{{ v, RET #v; ⌜v = 42⌝ }}}.

Section defs.
  Context `{!noprolG Σ, !uniqTokG Σ, gpG: !gpsG Σ boolProtocol}.
  Local Notation vProp := (vProp Σ).
  Implicit Types (x : loc) (γ : gname) (s : pr_stateT boolProtocol).

  Definition XE x γ : vProp := [es UTok γ ⇝ x ↦ #42 ]%I.
  Definition YP x γ s (v : val) : vProp :=
    (match s with
     | false => ⌜v = #0⌝
     | true  => ⌜v = #1⌝ ∗ XE x γ
     end)%I.
  Definition wlInt x γ : interpO Σ boolProtocol := (λ _ _ _ _, YP x γ)%I.

  Global Instance YP_persistent x γ s v : Persistent (YP x γ s v).
  Proof. rewrite /Persistent. destruct s; by iIntros "#YP". Qed.
  Global Instance interp_persistent x γ b l γl t s v V :
    Persistent (wlInt x γ b l γl t s v V).
  Proof. destruct b; apply _. Qed.
End defs.

Section proof.
  Local Set Default Proof Using "All".
  Context `{!noprolG Σ, !uniqTokG Σ, gpG: !gpsG Σ boolProtocol, !atomicG Σ}.

  Lemma wakelist_handoff_instance : wl_spec Σ.
  Proof.
    iIntros (tid Φ) "_ Post". rewrite /wakelist_handoff.
    wp_apply wp_new; [done..|].
    iIntros (p) "(DEL & p & Hp)".
    rewrite own_loc_na_vec_cons own_loc_na_vec_singleton.
    iDestruct "p" as "[p0 p1]".
    wp_pures. rewrite shift_0. wp_write. wp_op. wp_write.
    iMod UTok_alloc as (γ) "Tok".
    iMod (GPS_iSP_Init (wlN p) (wlInt (p >> 1) γ) (wlInt (p >> 1) γ false) p false
            with "p0 []") as (γm tm) "W"; [done|].
    iDestruct (GPS_iSP_SWWriter_Reader with "W") as "#R".
    wp_apply (wp_fork with "[W p1]"); [done|..].
    - iIntros "!>" (tid').
      wp_op. wp_write.
      iMod (escrow_alloc (UTok γ) ((p >> 1) ↦ #42)%I with "[$p1]") as "#XE"; [done|].
      wp_op. rewrite shift_0.
      iApply (GPS_iSP_SWWrite (wlN p) (wlInt (p >> 1) γ) (wlInt (p >> 1) γ false)
                True%I _ AcqRel _ _ true _ #1 with "[$W]");
            [solve_ndisj|done|done|done|..].
      { iSplitL "". - iIntros "!>"; by iFrame "XE". - by iIntros "!> !> _". }
      by iIntros "!>" (?) "W".
    - iIntros "_". wp_seq. wp_bind (repeat: _)%E.
      iLöb as "IH". iApply wp_repeat; [done|].
      wp_op. rewrite shift_0.
      iApply (GPS_iSP_Read (wlN p) (wlInt (p >> 1) γ) (wlInt (p >> 1) γ false)
                           (wlInt (p >> 1) γ false p γ) with "[$R]");
          [solve_ndisj|done|done|..].
      { iIntros "!>" (????) "!>". iSplit; last iSplit; by iIntros "#$". }
      iIntros "!>" (? s' v'). simpl.
      iDestruct 1 as "[% [R' Int]]". destruct s'; simpl; last first.
      { iDestruct "Int" as %?. subst v'.
        iExists 0. iSplit; [done|]. simpl.
        by iApply ("IH" with "Post DEL Hp Tok"). }
      iDestruct "Int" as "[% XE]". subst v'.
      iExists 1. iSplit; [done|]. simpl. iIntros "!> !>". wp_seq.
      iMod (escrow_elim with "[] XE Tok") as "p1"; [done|..].
      { iIntros "[e1 e2]". by iApply (UTok_unique with "e1 e2"). }
      wp_op. wp_read. by iApply ("Post" $! 42).
  Qed.
End proof.
