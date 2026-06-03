(* TreiberStack.v -- Iris (HeapLang) proof of a lock-free Treiber stack, the
   structure behind runloom's lock-free LIFO stacks (the idle-parker stack and the
   per-hub submission stack: push a node by CAS-ing the head, pop by CAS-ing
   the head to its tail).

   Unlike the cell-CAS proofs (OneShotWake.v / WakeQueue.v), this is a lock-free
   LINKED-LIST structure.  It proves the per-element-invariant ("bag") spec:
   push requires P v, and every value a concurrent pop returns satisfies P --
   i.e. under arbitrary interleaved push/pop the stack never corrupts its
   contents, never returns a phantom, and preserves the resource invariant P
   carried by each element.  This is the lock-free-list analogue of the
   no-loss / no-phantom properties cldeque.pml checks for the deque.

   Adapted verbatim from Iris's own canonical proof
   (iris-examples/concurrent_stacks/concurrent_stack1.v), with the
   examples-library `specs` dependency dropped so the file is self-contained.

   Run: verify/iris/run_iris.sh   (SC Iris; needs coq-iris-heap-lang). *)

From iris.base_logic.lib Require Import invariants.
From iris.program_logic Require Export weakestpre.
From iris.heap_lang Require Import notation proofmode.
From iris.prelude Require Import options.

Definition new_stack : val := λ: "_", ref NONEV.
Definition push : val :=
  rec: "push" "s" "v" :=
    let: "tail" := ! "s" in
    let: "new" := SOME (ref ("v", "tail")) in
    if: CAS "s" "tail" "new" then #() else "push" "s" "v".
Definition pop : val :=
  rec: "pop" "s" :=
    match: !"s" with
      NONE => NONEV
    | SOME "l" =>
      let: "pair" := !"l" in
      if: CAS "s" (SOME "l") (Snd "pair")
      then SOME (Fst "pair")
      else "pop" "s"
    end.

Section stacks.
  Context `{!heapGS Σ} (N : namespace).
  Implicit Types l : loc.

  Definition oloc_to_val (ol : option loc) : val :=
    match ol with
    | None => NONEV
    | Some loc => SOMEV (#loc)
    end.
  Local Instance oloc_to_val_inj : Inj (=) (=) oloc_to_val.
  Proof. intros [|] [|]; simpl; congruence. Qed.

  Definition is_list_pre (P : val → iProp Σ) (F : option loc -d> iPropO Σ) :
     option loc -d> iPropO Σ := λ v, match v with
     | None => True
     | Some l => ∃ (h : val) (t : option loc), l ↦□ (h, oloc_to_val t)%V ∗ P h ∗ ▷ F t
     end%I.

  Local Instance is_list_contr (P : val → iProp Σ) : Contractive (is_list_pre P).
  Proof. solve_contractive. Qed.

  Definition is_list_def (P : val -> iProp Σ) := fixpoint (is_list_pre P).
  Definition is_list_aux P : seal (@is_list_def P). Proof. by eexists. Qed.
  Definition is_list P := unseal (is_list_aux P).
  Definition is_list_eq P : @is_list P = @is_list_def P := seal_eq (is_list_aux P).

  Lemma is_list_unfold (P : val → iProp Σ) v :
    is_list P v ⊣⊢ is_list_pre P (is_list P) v.
  Proof. rewrite is_list_eq. apply (fixpoint_unfold (is_list_pre P)). Qed.

  Lemma is_list_dup (P : val → iProp Σ) v :
    is_list P v -∗ is_list P v ∗ match v with
      | None => True
      | Some l => ∃ h t, l ↦□ (h, oloc_to_val t)%V
      end.
  Proof.
    iIntros "Hstack". iDestruct (is_list_unfold with "Hstack") as "Hstack".
    destruct v as [l|].
    - iDestruct "Hstack" as (h t) "(#Hl & ? & ?)".
      rewrite (is_list_unfold _ (Some _)). iSplitL; iExists _, _; by iFrame.
    - rewrite is_list_unfold; iSplitR; eauto.
  Qed.

  Definition stack_inv P v :=
    (∃ l ol', ⌜v = #l⌝ ∗ l ↦ oloc_to_val ol' ∗ is_list P ol')%I.

  Definition is_stack (P : val → iProp Σ) v := inv N (stack_inv P v).

  Theorem new_stack_spec P :
    {{{ True }}} new_stack #() {{{ s, RET s; is_stack P s }}}.
  Proof.
    iIntros (ϕ) "_ Hpost".
    wp_lam. wp_alloc ℓ as "Hl".
    iMod (inv_alloc N ⊤ (stack_inv P #ℓ) with "[Hl]") as "Hinv".
    { iNext; iExists ℓ, None; iFrame;
      by iSplit; last (iApply is_list_unfold). }
    by iApply "Hpost".
  Qed.

  Theorem push_spec P s v :
    {{{ is_stack P s ∗ P v }}} push s v {{{ RET #(); True }}}.
  Proof.
    iIntros (Φ) "[#Hstack HP] HΦ".
    iLöb as "IH".
    wp_lam. wp_let. wp_bind (Load _).
    iInv N as (ℓ v') "(>% & Hl & Hlist)" "Hclose"; subst.
    wp_load.
    iMod ("Hclose" with "[Hl Hlist]") as "_".
    { iNext; iExists _, _; by iFrame. }
    iModIntro. wp_let. wp_alloc ℓ' as "Hl'". wp_pures. wp_bind (CmpXchg _ _ _).
    iInv N as (ℓ'' v'') "(>% & >Hl & Hlist)" "Hclose"; simplify_eq.
    destruct (decide (v' = v'')) as [->|Hne].
    - wp_cmpxchg_suc. { destruct v''; left; done. }
      iMod (pointsto_persist with "Hl'") as "Hl'".
      iMod ("Hclose" with "[HP Hl Hl' Hlist]") as "_".
      { iNext; iExists _, (Some ℓ'); iFrame; iSplit; first done;
        rewrite (is_list_unfold _ (Some _)) /=. eauto with iFrame. }
      iModIntro. wp_pures. by iApply "HΦ".
    - wp_cmpxchg_fail.
      { destruct v', v''; simpl; congruence. }
      { destruct v''; left; done. }
      iMod ("Hclose" with "[Hl Hlist]") as "_".
      { iNext; iExists _, _; by iFrame. }
      iModIntro. wp_pures. iApply ("IH" with "HP HΦ").
  Qed.

  Theorem pop_spec P s :
    {{{ is_stack P s }}} pop s {{{ ov, RET ov; ⌜ov = NONEV⌝ ∨ ∃ v, ⌜ov = SOMEV v⌝ ∗ P v }}}.
  Proof.
    iIntros (Φ) "#Hstack HΦ".
    iLöb as "IH".
    wp_lam. wp_bind (Load _).
    iInv N as (ℓ v') "(>% & Hl & Hlist)" "Hclose"; subst.
    iDestruct (is_list_dup with "Hlist") as "[Hlist Hlist2]".
    wp_load.
    iMod ("Hclose" with "[Hl Hlist]") as "_".
    { iNext; iExists _, _; by iFrame. }
    iModIntro.
    destruct v' as [l|]; last first.
    - wp_match. iApply "HΦ"; by iLeft.
    - wp_match. wp_bind (Load _).
      iInv N as (ℓ' v') "(>% & Hl' & Hlist)" "Hclose". simplify_eq.
      iDestruct "Hlist2" as (??) "Hl".
      wp_load.
      iMod ("Hclose" with "[Hl' Hlist]") as "_".
      { iNext; iExists _, _; by iFrame. }
      iModIntro.
      wp_pures. wp_bind (CmpXchg _ _ _).
      iInv N as (ℓ'' v'') "(>% & Hl' & Hlist)" "Hclose". simplify_eq.
      destruct (decide (v'' = (Some l))) as [-> |].
      * rewrite is_list_unfold.
        iDestruct "Hlist" as (h' t') "(Hl'' & HP & Hlist) /=".
        wp_cmpxchg_suc.
        iDestruct (pointsto_agree with "Hl'' Hl") as %[= <- <-%oloc_to_val_inj].
        iMod ("Hclose" with "[Hl' Hlist]") as "_".
        { iNext; iExists ℓ'', _; by iFrame. }
        iModIntro. wp_pures.
        iApply ("HΦ" with "[HP]"); iRight; iExists _; by iFrame.
      * wp_cmpxchg_fail. { destruct v''; simpl; congruence. }
        iMod ("Hclose" with "[Hl' Hlist]") as "_".
        { iNext; iExists ℓ'', _; by iFrame. }
        iModIntro. wp_pures. iApply ("IH" with "HΦ").
  Qed.
End stacks.
