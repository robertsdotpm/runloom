(* OneShotWake.v -- an Iris (HeapLang) proof that a CAS-based one-shot wake is
   claimed AT MOST ONCE, under genuine concurrency.

   This is the first step of the weak-memory / separation-logic arc.  Unlike
   the Spin and transition-system Coq proofs (which reason about an abstract
   state machine), this proves a *running concurrent program* in HeapLang: two
   threads race to consume a single wake via CmpXchg, and Iris's concurrent
   separation logic + an exclusive ghost token prove that at most one of them
   ever observes the wake -- the "no double resume" / "claimed exactly once"
   property at the heart of wake_state.pml and netpoll_commit.pml, now as a
   thread-modular program proof rather than a finite-state check.

   Scope: this is plain (sequentially-consistent) Iris.  Lifting the SAME
   property to the RC11 weak-memory model is Stage 3 (iRC11/gpfsl); see
   verify/iris/README.md. *)

From iris.algebra Require Import excl.
From iris.base_logic.lib Require Import invariants.
From iris.heap_lang Require Import lang proofmode notation.
From iris.heap_lang.lib Require Import par.
From iris.prelude Require Import options.

(* A fresh, undelivered wake is a cell holding true; consuming it is a CAS
   true -> false, returning whether this caller won. *)
Definition mk_wake : val := λ: <>, ref #true.
Definition consume : val := λ: "w", Snd (CmpXchg "w" #true #false).

(* Ghost theory: one exclusive token = "this caller claimed the wake". *)
Class wakeG Σ := WakeG { wake_inG : inG Σ (exclR unitO) }.
Local Existing Instance wake_inG.
Definition wakeΣ : gFunctors := #[GFunctor (exclR unitO)].
Global Instance subG_wakeΣ Σ : subG wakeΣ Σ → wakeG Σ.
Proof. solve_inG. Qed.

Section proof.
  Context `{!heapGS Σ, !wakeG Σ, !spawnG Σ}.
  Let N := nroot .@ "wake".

  Definition claimed (γ : gname) : iProp Σ := own γ (Excl ()).

  (* Invariant: while undelivered (b=true) the token lives in the invariant;
     the winning CmpXchg flips the cell to false and carries the token out. *)
  Definition wake_inv (γ : gname) (w : loc) : iProp Σ :=
    ∃ b : bool, w ↦ #b ∗ (⌜b = true⌝ ∗ claimed γ ∨ ⌜b = false⌝).

  Definition is_wake (γ : gname) (w : loc) : iProp Σ := inv N (wake_inv γ w).

  (* The token is exclusive: two claims are contradictory. *)
  Lemma claimed_exclusive γ : claimed γ -∗ claimed γ -∗ False.
  Proof.
    iIntros "H1 H2".
    by iDestruct (own_valid_2 with "H1 H2") as %[].
  Qed.

  Lemma mk_wake_spec :
    {{{ True }}} mk_wake #() {{{ γ w, RET #w; is_wake γ w }}}.
  Proof.
    iIntros (Φ) "_ HΦ". wp_lam.
    iMod (own_alloc (Excl ())) as (γ) "Htok"; first done.
    wp_alloc w as "Hw".
    iMod (inv_alloc N _ (wake_inv γ w) with "[Hw Htok]") as "#Hinv".
    { iNext. iExists true. iFrame. iLeft. by iFrame. }
    iApply "HΦ". by iApply "Hinv".
  Qed.

  (* A successful consume hands out the (unique) claimed token. *)
  Lemma consume_spec γ w :
    {{{ is_wake γ w }}}
      consume #w
    {{{ (v : bool), RET #v; if v then claimed γ else True }}}.
  Proof.
    iIntros (Φ) "#Hinv HΦ". wp_lam. wp_bind (CmpXchg _ _ _).
    iInv N as (b) "[>Hw >Hrest]" "Hclose".
    destruct b.
    - iDestruct "Hrest" as "[[_ Htok] | %Hb]"; last by inversion Hb.
      wp_cmpxchg_suc.
      iMod ("Hclose" with "[Hw]") as "_".
      { iNext. iExists false. iFrame. by iRight. }
      iModIntro. wp_pures. iApply "HΦ". iExact "Htok".
    - wp_cmpxchg_fail.
      iMod ("Hclose" with "[Hw Hrest]") as "_".
      { iNext. iExists false. iFrame. }
      iModIntro. wp_pures. by iApply "HΦ".
  Qed.

  (* The headline: two threads race; they cannot BOTH win the wake. *)
  Lemma consume_at_most_once γ w :
    {{{ is_wake γ w }}}
      (consume #w) ||| (consume #w)
    {{{ (v1 v2 : bool), RET (#v1, #v2); ⌜¬ (v1 = true ∧ v2 = true)⌝ }}}.
  Proof using All.
    iIntros (Φ) "#Hinv HΦ".
    wp_smart_apply (wp_par (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then claimed γ else True)%I
                           (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then claimed γ else True)%I
                    with "[] []").
    - iApply (consume_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iApply (consume_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iIntros (r1 r2) "[H1 H2]".
      iDestruct "H1" as (v1 ->) "Hc1". iDestruct "H2" as (v2 ->) "Hc2".
      iModIntro.
      (* both winning is impossible: two exclusive tokens cannot coexist *)
      destruct v1, v2; simpl.
      + iDestruct (claimed_exclusive with "Hc1 Hc2") as %[].
      + iApply "HΦ". iPureIntro. intros [_ H]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
  Qed.

End proof.
