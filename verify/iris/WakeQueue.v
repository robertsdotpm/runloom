(* WakeQueue.v -- Iris (HeapLang) proof of the wake_state protocol's two
   safety invariants under genuine concurrency (Stage 2).

   Stage 1 (OneShotWake.v) proved a single CAS race.  This models the actual
   wake_state lifecycle as a 3-state cell -- PARKED(0) -> QUEUED(1) ->
   RUNNING(2) -- with two transitions, each racing N threads:

     wake = CAS g 0 1   (a waker enqueues a parked g)
     pull = CAS g 1 2   (a hub pulls the queued g to run it)

   and proves, as a running concurrent program, the two invariants
   wake_state.pml checks as assertions (now thread-modular, any N):

     INV1  no duplicate / orphan run-queue entry: among racing wakers AT MOST
           ONE enqueues the g  (wake_at_most_once).
     INV2  no double resume: among racing hubs AT MOST ONE runs it
           (pull_at_most_once).

   Each is proved with an exclusive ghost token (enq / run) extracted by the
   single CAS winner; two winners would own two copies of an exclusive token,
   a contradiction. *)

From iris.algebra Require Import excl.
From iris.base_logic.lib Require Import invariants.
From iris.heap_lang Require Import lang proofmode notation.
From iris.heap_lang.lib Require Import par.
From iris.prelude Require Import options.

Definition mk_g   : val := λ: <>, ref #(0%nat).
Definition wake   : val := λ: "g", Snd (CmpXchg "g" #(0%nat) #(1%nat)).
Definition pull   : val := λ: "g", Snd (CmpXchg "g" #(1%nat) #(2%nat)).

(* Two exclusive tokens: enq = "I enqueued it", run = "I resumed it". *)
Class wqG Σ := WqG { wq_inG : inG Σ (exclR unitO) }.
Local Existing Instance wq_inG.
Definition wqΣ : gFunctors := #[GFunctor (exclR unitO)].
Global Instance subG_wqΣ Σ : subG wqΣ Σ → wqG Σ.
Proof. solve_inG. Qed.

Section proof.
  Context `{!heapGS Σ, !wqG Σ, !spawnG Σ}.
  Let N := nroot .@ "wq".

  Definition enq (γe : gname) : iProp Σ := own γe (Excl ()).
  Definition run (γr : gname) : iProp Σ := own γr (Excl ()).

  (* The token currently held by the invariant depends on the state:
     parked: both available; queued: enq handed out, run available;
     running: both handed out. *)
  Definition wq_inv (γe γr : gname) (g : loc) : iProp Σ :=
    ∃ s : nat, g ↦ #s ∗
      ( ⌜s = 0⌝ ∗ enq γe ∗ run γr
      ∨ ⌜s = 1⌝ ∗ run γr
      ∨ ⌜s = 2⌝ ).

  Definition is_g (γe γr : gname) (g : loc) : iProp Σ := inv N (wq_inv γe γr g).

  Lemma enq_exclusive γe : enq γe -∗ enq γe -∗ False.
  Proof. iIntros "H1 H2". by iDestruct (own_valid_2 with "H1 H2") as %[]. Qed.
  Lemma run_exclusive γr : run γr -∗ run γr -∗ False.
  Proof. iIntros "H1 H2". by iDestruct (own_valid_2 with "H1 H2") as %[]. Qed.

  Lemma mk_g_spec :
    {{{ True }}} mk_g #() {{{ γe γr g, RET #g; is_g γe γr g }}}.
  Proof.
    iIntros (Φ) "_ HΦ". wp_lam.
    iMod (own_alloc (Excl ())) as (γe) "He"; first done.
    iMod (own_alloc (Excl ())) as (γr) "Hr"; first done.
    wp_alloc g as "Hg".
    iMod (inv_alloc N _ (wq_inv γe γr g) with "[Hg He Hr]") as "#Hinv".
    { iNext. iExists 0. iFrame "Hg". iLeft. by iFrame. }
    iApply "HΦ". by iApply "Hinv".
  Qed.

  (* A winning wake (CAS 0->1) hands out the unique enq token. *)
  Lemma wake_spec γe γr g :
    {{{ is_g γe γr g }}} wake #g {{{ (v : bool), RET #v; if v then enq γe else True }}}.
  Proof.
    iIntros (Φ) "#Hinv HΦ". wp_lam. wp_bind (CmpXchg _ _ _).
    iInv N as (s) "[>Hg >Hrest]" "Hclose".
    iDestruct "Hrest" as "[(%Hs & He & Hr) | [(%Hs & Hr) | %Hs]]"; subst s.
    - wp_cmpxchg_suc.
      iMod ("Hclose" with "[Hg Hr]") as "_".
      { iNext. iExists 1. iFrame "Hg". iRight; iLeft. by iFrame "Hr". }
      iModIntro. wp_pures. iApply "HΦ". iExact "He".
    - wp_cmpxchg_fail; [by (intros ?; simplify_eq)|].
      iMod ("Hclose" with "[Hg Hr]") as "_".
      { iNext. iExists 1. iFrame "Hg". iRight; iLeft. by iFrame "Hr". }
      iModIntro. wp_pures. by iApply "HΦ".
    - wp_cmpxchg_fail; [by (intros ?; simplify_eq)|].
      iMod ("Hclose" with "[Hg]") as "_".
      { iNext. iExists 2. iFrame "Hg". iRight; iRight. done. }
      iModIntro. wp_pures. by iApply "HΦ".
  Qed.

  (* A winning pull (CAS 1->2) hands out the unique run token. *)
  Lemma pull_spec γe γr g :
    {{{ is_g γe γr g }}} pull #g {{{ (v : bool), RET #v; if v then run γr else True }}}.
  Proof.
    iIntros (Φ) "#Hinv HΦ". wp_lam. wp_bind (CmpXchg _ _ _).
    iInv N as (s) "[>Hg >Hrest]" "Hclose".
    iDestruct "Hrest" as "[(%Hs & He & Hr) | [(%Hs & Hr) | %Hs]]"; subst s.
    - wp_cmpxchg_fail; [by (intros ?; simplify_eq)|].
      iMod ("Hclose" with "[Hg He Hr]") as "_".
      { iNext. iExists 0. iFrame "Hg". iLeft. by iFrame. }
      iModIntro. wp_pures. by iApply "HΦ".
    - wp_cmpxchg_suc.
      iMod ("Hclose" with "[Hg]") as "_".
      { iNext. iExists 2. iFrame "Hg". iRight; iRight. done. }
      iModIntro. wp_pures. iApply "HΦ". iExact "Hr".
    - wp_cmpxchg_fail; [by (intros ?; simplify_eq)|].
      iMod ("Hclose" with "[Hg]") as "_".
      { iNext. iExists 2. iFrame "Hg". iRight; iRight. done. }
      iModIntro. wp_pures. by iApply "HΦ".
  Qed.

  (* INV1: two racing wakers cannot both enqueue the g. *)
  Lemma wake_at_most_once γe γr g :
    {{{ is_g γe γr g }}}
      (wake #g) ||| (wake #g)
    {{{ (v1 v2 : bool), RET (#v1, #v2); ⌜¬ (v1 = true ∧ v2 = true)⌝ }}}.
  Proof using All.
    iIntros (Φ) "#Hinv HΦ".
    wp_smart_apply (wp_par (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then enq γe else True)%I
                           (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then enq γe else True)%I
                    with "[] []").
    - iApply (wake_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iApply (wake_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iIntros (r1 r2) "[H1 H2]".
      iDestruct "H1" as (v1 ->) "Hc1". iDestruct "H2" as (v2 ->) "Hc2". iModIntro.
      destruct v1, v2; simpl.
      + iDestruct (enq_exclusive with "Hc1 Hc2") as %[].
      + iApply "HΦ". iPureIntro. intros [_ H]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
  Qed.

  (* INV2: two racing hubs cannot both resume the g. *)
  Lemma pull_at_most_once γe γr g :
    {{{ is_g γe γr g }}}
      (pull #g) ||| (pull #g)
    {{{ (v1 v2 : bool), RET (#v1, #v2); ⌜¬ (v1 = true ∧ v2 = true)⌝ }}}.
  Proof using All.
    iIntros (Φ) "#Hinv HΦ".
    wp_smart_apply (wp_par (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then run γr else True)%I
                           (λ r, ∃ v:bool, ⌜r = #v⌝ ∗ if v then run γr else True)%I
                    with "[] []").
    - iApply (pull_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iApply (pull_spec with "Hinv"). iIntros "!>" (v) "Hv". iExists v. by iFrame.
    - iIntros (r1 r2) "[H1 H2]".
      iDestruct "H1" as (v1 ->) "Hc1". iDestruct "H2" as (v2 ->) "Hc2". iModIntro.
      destruct v1, v2; simpl.
      + iDestruct (run_exclusive with "Hc1 Hc2") as %[].
      + iApply "HΦ". iPureIntro. intros [_ H]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
      + iApply "HΦ". iPureIntro. intros [H _]; discriminate.
  Qed.

End proof.
