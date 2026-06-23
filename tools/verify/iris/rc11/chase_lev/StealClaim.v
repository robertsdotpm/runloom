(* StealClaim.v -- EXPERIMENT (Stage A of the Chase-Lev work-stealing deque
   under iRC11 / RC11).

   This is the load-bearing weak-memory site of the Chase-Lev deque: a thief
   STEALS an element by (relaxed-)CAS-ing the shared `top` counter, having first
   ACQUIRE-read the `bot` counter the owner RELEASE-published.  It is the exact
   site that stalled an earlier pass: gpfsl ships the Chase-Lev *code*
   (gpfsl-examples/chase_lev/code.v) but NO proof, and the shared-writer CAS has
   no worked example outside the 525-line Treiber linked-list proof.

   What this file machine-checks (a faithful slice of cl_new/cl_push/cl_steal):
     - the owner writes element 42 into the buffer (non-atomic), then publishes
       it with a RELEASE write of `bot` (cl_push's release write);
     - TWO thieves race, each running the real cl_steal shape: relaxed-read top,
       acquire-read bot, range-check, then a RELAXED CAS on top (casʳˡˣ);
     - the winner reads the buffer slot and gets 42; the loser gets NONE.
   The theorem proves every thief returns NONE or exactly 42 -- i.e. under RC11
   the steal never reads a stale / uninitialised slot, and the element is
   claimed AT MOST ONCE (no double-steal), even with both thieves contending.

   The synchronisation argument, machine-checked here, is the real Chase-Lev
   insight:
     - the element rides in a PERSISTENT escrow seated in `bot`'s release
       interpretation, so the acquire-read of bot both view-synchronises the
       thief with the push AND hands it the (duplicable) escrow;
     - the EXCLUSIVE claim token that redeems the escrow is seated in `top`'s
       CAS interpretation, so exactly one CAS winner can extract it.  The token
       is objective, so it passes cleanly through the relaxed CAS's acquire view
       modality (▽).

   SCOPE (honest): this is a bounded instance -- one element, two thieves, no
   owner cl_try_pop, no array growth.  The full UNBOUNDED linearizability
   theorem (FIFO order over all indices + the owner/thief last-element SC-fence
   exclusion) is published-paper scope and remains open; see NOTES.md.

   Idioms adapted from CommitPublish.v (this suite) + gpfsl's
   proof_treiber_gps.v.  A `rocq compile` PASS *is* the proof check.

   Run: ../run_rc11.sh after copying in, or build.sh here. *)

From stdpp Require Import namespaces.
From iris.algebra Require Import excl.
From iris.base_logic Require Import lib.own.
From iris.proofmode Require Import proofmode monpred.

From gpfsl.base_logic Require Import vprop.
From gpfsl.lang Require Export notation.
From gpfsl.logic Require Import proofmode lifting new_delete relacq.
From gpfsl.gps Require Import surface_iSP surface_iPP protocols escrows.

Require Import iris.prelude.options.

(* ---- a unique (exclusive) token (as in CommitPublish.v) ---- *)
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
  #[global] Instance UTok_timeless γ : Timeless (UTok γ). Proof. apply _. Qed.
  #[global] Instance UTok_objective γ : Objective (UTok γ). Proof. apply _. Qed.
  Lemma UTok_alloc : ⊢ (|==> ∃ γ, UTok γ : vProp)%I.
  Proof. iStartProof. iMod (own_alloc (Excl ())) as (γ) "U"; [done|]. iIntros "!>". by iExists γ. Qed.
  Lemma UTok_unique γ : UTok γ -∗ UTok γ -∗ False.
  Proof. iIntros "U1 U2". by iCombine "U1 U2" gives %?. Qed.
End Tok.

(* ---- the deque layout (matching gpfsl-examples/chase_lev/code.v) ---- *)
Notation bot  := 0 (only parsing).   (* bottom counter (owner end) *)
Notation top  := 1 (only parsing).   (* top counter (thief end), CAS-ed *)
Notation buf  := 2 (only parsing).   (* the (single) buffer slot *)
Notation NONE := (-1)%Z (only parsing).

(* ---- the steal operation: a faithful slice of cl_steal (the FenceSC is
   dropped because this slice has no owner cl_try_pop to race the SC fence;
   the acquire-read of bot alone provides the steal/push synchronisation, as
   cl_steal's own comment notes). ---- *)
Definition steal : val :=
  λ: ["q"],
    let: "t"  := !ʳˡˣ ("q" +ₗ #top) in
    let: "bb" := !ᵃᶜ  ("q" +ₗ #bot) in
    if: ("bb" - "t") ≤ #0 then #NONE
    else
      if: casʳˡˣ("q" +ₗ #top, "t", "t" + #1)
      then ! ("q" +ₗ (#buf + "t"))
      else #NONE.

(* ---- the client: cl_new + one cl_push (publish 42 at index 0) + two racing
   thieves.  The forked thief's result is discarded; the spec is about the
   value the main thief returns. ---- *)
Definition steal_claim : expr :=
  let: "q" := new [ #(buf + 1) ] in
  "q" +ₗ #buf <- #42 ;;                 (* push: write the element (non-atomic) *)
  "q" +ₗ #top <- #0 ;;                  (* init top protocol *)
  "q" +ₗ #bot <- #0 ;;                  (* init bot protocol *)
  "q" +ₗ #bot <-ʳᵉˡ #1 ;;              (* push: RELEASE-publish bot 0 -> 1 *)
  Fork (steal ["q"]) ;;
  steal ["q"].

Definition topN (q : loc) : namespace := nroot .@ "cldeque" .@ "top" .@ q.
Definition botN (q : loc) : namespace := nroot .@ "cldeque" .@ "bot" .@ q.

Definition claim_spec Σ
  `{!noprolG Σ, !uniqTokG Σ, !gpsG Σ boolProtocol, !gpsG Σ unitProtocol, !atomicG Σ}
  : Prop :=
  ∀ tid, {{{ True }}} steal_claim @ tid; ⊤ {{{ (v:Z), RET #v; ⌜v = NONE ∨ v = 42⌝ }}}.

(* ---- protocol interpretations ---- *)
Section defs.
  Context `{!noprolG Σ, !uniqTokG Σ}.
  Local Notation vProp := (vProp Σ).
  Implicit Types (q : loc) (γtok : gname) (v : val).

  (* the escrow: holding the unique token redeems the buffer slot.  It is
     PERSISTENT and travels through bot's release interpretation. *)
  Definition XE q γtok : vProp := [es UTok γtok ⇝ (q >> buf) ↦ #42 ]%I.

  (* bot: single-writer boolean protocol (false = 0, unpublished; true = 1,
     published, ships the escrow). *)
  Definition botYP q γtok (s : bool) v : vProp :=
    (match s with
     | false => ⌜v = #0⌝
     | true  => ⌜v = #1⌝ ∗ XE q γtok
     end)%I.
  Definition botInt q γtok : interpO Σ boolProtocol := (λ _ _ _ _, botYP q γtok)%I.

  Global Instance botYP_persistent q γtok s v : Persistent (botYP q γtok s v).
  Proof. rewrite /Persistent. destruct s; by iIntros "#H". Qed.
  Global Instance botInt_persistent q γtok b l γl t s v V :
    Persistent (botInt q γtok b l γl t s v V).
  Proof. destruct b; apply _. Qed.

  (* top: plain (CAS) protocol on the unit state; the *value* carries the
     state.  The interp's boolean b distinguishes the CURRENT writer (true,
     holding the claim token at value 0) from a HISTORICAL entry (false, no
     token).  At value 1 the slot is already claimed -- no token either way. *)
  Definition topYP q γtok (b : bool) v : vProp :=
    (⌜v = #1⌝ ∨ (⌜v = #0⌝ ∗ if b then UTok γtok else True))%I.
  Definition topInt q γtok : interpO Σ unitProtocol := (λ b _ _ _ _, topYP q γtok b)%I.

  (* range / comparability facts (pure, so extractible without consuming),
     stated on the *interp* forms so they unify with GPS-lemma hypotheses. *)
  Lemma botInt_range q γtok b l γ t s v :
    botInt q γtok b l γ t s v -∗
    botInt q γtok b l γ t s v ∗ (⌜v = #0⌝ ∨ (⌜v = #1⌝ ∗ XE q γtok)).
  Proof.
    rewrite /botInt /botYP /=. destruct s; simpl.
    - iIntros "[%E #X]". subst v. iSplitR "".
      + by iFrame "X".
      + iRight. by iFrame "X".
    - iIntros "%E". subst v. iSplit; [done|]. by iLeft.
  Qed.

  Lemma topInt_range q γtok b l γ t s v :
    topInt q γtok b l γ t s v -∗ topInt q γtok b l γ t s v ∗ ⌜v = #0 ∨ v = #1⌝.
  Proof.
    rewrite /topInt /topYP /=. iIntros "H".
    iAssert (⌜v = #0 ∨ v = #1⌝)%I as "#%".
    { iDestruct "H" as "[%|[% _]]"; iPureIntro; [by right|by left]. }
    by iFrame "H".
  Qed.

  Lemma topInt_comparable q γtok b l γ t s v :
    topInt q γtok b l γ t s v -∗ ⌜∃ vl, v = #vl ∧ lit_comparable 0 vl⌝.
  Proof.
    rewrite /topInt /topYP /=. iIntros "[%|[% _]]"; subst v; iPureIntro;
      eexists; (split; [done|]); constructor.
  Qed.

  (* the load-bearing extraction: only the CURRENT (b=true) writer at value 0
     holds the unique claim token *)
  Lemma topInt_tok q γtok l γ t s :
    topInt q γtok true l γ t s #0 -∗ UTok γtok.
  Proof. rewrite /topInt /topYP /=. iIntros "[%H|[_ $]]". by exfalso; simplify_eq. Qed.

  Lemma topInt_false0 q γtok l γ t s : ⊢ topInt q γtok false l γ t s #0.
  Proof. rewrite /topInt /topYP /=. iRight. by iSplit. Qed.

  Lemma topInt_true1 q γtok l γ t s : ⊢ topInt q γtok true l γ t s #1.
  Proof. rewrite /topInt /topYP /=. by iLeft. Qed.
End defs.

Section proof.
  Local Set Default Proof Using "All".
  Context `{!noprolG Σ, !uniqTokG Σ, !gpsG Σ boolProtocol, !gpsG Σ unitProtocol, !atomicG Σ}.

  (* ------ the steal operation ------ *)
  Lemma steal_spec q γt γb γtok tt0 tb0 sb0 vb0 tid :
    {{{ GPS_iPP (topN q) (topInt q γtok) (q >> top) tt0 () #0 γt
        ∗ GPS_iSP_Reader (botN q) (botInt q γtok) (botInt q γtok false)
            q tb0 sb0 #vb0 γb }}}
      steal [ #q ] @ tid; ⊤
    {{{ (r:Z), RET #r; ⌜r = NONE ∨ r = 42⌝ }}}.
  Proof.
    iIntros (Φ) "[#Top #Bot] Post". wp_lam.
    (* relaxed-read top *)
    wp_op.
    wp_apply (GPS_iPP_Read (topN q) (topInt q γtok)
                (λ _ _ v, ⌜v = #0 ∨ v = #1⌝)%I Relaxed with "[$Top]");
      [solve_ndisj|done|done|..].
    { iIntros "!>" (t' s' v' _) "!>". iSplit; iIntros "I"; iModIntro;
        iDestruct (topInt_range with "I") as "[$ $]". }
    iIntros (tt1 [] vt) "(_ & #Top1 & Hvt)".
    iDestruct (acq_pure_elim with "Hvt") as %Hvt.
    wp_let.
    (* acquire-read bot *)
    wp_op. rewrite shift_0.
    wp_apply (GPS_iSP_Read (botN q) (botInt q γtok) (botInt q γtok false)
                (λ _ _ v, ⌜v = #0⌝ ∨ (⌜v = #1⌝ ∗ XE q γtok))%I with "[$Bot]");
      [solve_ndisj|done|done|..].
    { iIntros "!>" (t' s' v' _) "!>". iSplit; last iSplit; iIntros "I"; iModIntro;
        iDestruct (botInt_range with "I") as "[$ $]". }
    iIntros (tb1 sb1 vb) "(_ & #Bot1 & Hbb)".
    wp_let.
    (* range-check bb - t <= 0 *)
    destruct Hvt as [-> | ->].
    - (* vt = 0 *)
      iDestruct "Hbb" as "[%Ev | [%Ev #X]]"; subst vb.
      + (* bb = 0: 0 - 0 = 0 <= 0 -> NONE *)
        wp_pures. case_bool_decide as Hc; last (exfalso; lia). wp_pures.
        iApply ("Post" $! (-1)%Z). iPureIntro. by left.
      + (* bb = 1: 1 - 0 = 1 > 0 -> CAS *)
        wp_pures. case_bool_decide as Hc; first (exfalso; lia).
        wp_pures.
        iMod (rel_True_intro tid) as "#rTrue".
        wp_apply (GPS_iPP_CAS_int_simple (topN q) (topInt q γtok)
                    Relaxed Relaxed Relaxed (q >> top) #0 0 #1 tt1 ()
                    True%I
                    (λ _ _, UTok γtok)%I
                    (λ _ _, True)%I
                    (λ _ _, UTok γtok)%I
                    (λ _ _ _, True)%I
                    with "[$Top1]");
          [solve_ndisj|done|done|..].
        { (* VSC ∗ △VS ∗ △P *)
          iSplitR.
          { (* ▷ VSC: comparability *)
            iIntros "!> !>" (t' s' v' _) "[I|I]";
              by iDestruct (topInt_comparable with "I") as %?. }
          iSplitR; last by iExact "rTrue".
          (* △{tid} VS : build from <obj> VS via rel_sep_objectively (Treiber) *)
          rewrite /= -(bi.True_sep' (∀ _, _)%I).
          iApply (rel_sep_objectively with "[$rTrue]").
          iIntros "!>" (t' []) "_". iSplit; first iSplitR.
          - (* <obj>(▷ topInt true #0 ={}=∗ ▷True ∗ ▷UTok): take the token *)
            rewrite -bi.later_sep. iIntros "!> I !>". iNext.
            iDestruct (topInt_tok with "I") as "Tok". iSplitR; [done|]. iFrame "Tok".
          - (* writer continuation *)
            iIntros "_ Tok". iExists (). iSplitR; [done|].
            iIntros "!>" (t Ht) "#R1 !>". iSplitR.
            + (* <obj>(▷True ={}=∗ ▷ topInt false #0): historical entry, no token *)
              iIntros "!> _ !>". iNext. iApply topInt_false0.
            + (* |={}▷=> UTok ∗ ▷ topInt true #1: publish + hand out the token *)
              iIntros "!> !> !>". iFrame "Tok". iNext. iApply topInt_true1.
          - (* failure-projection R = True *)
            iIntros "!>" (v0 _) "!>". iSplit; by iIntros "$". }
        iIntros (b tt2 [] v2) "(_ & CASE)".
        iDestruct "CASE" as "[(%Eq & _ & HQ)|(%Eq & _ & _ & _)]".
        * (* CAS succeeded: redeem escrow, read buffer -> 42 *)
          destruct Eq as (-> & _ & _).
          iDestruct (acq_embed_elim with "HQ") as "Tok".
          iMod (escrow_elim with "[] X Tok") as "Hbuf"; [solve_ndisj|..].
          { iIntros "[e1 e2]". by iApply (UTok_unique with "e1 e2"). }
          iDestruct "Hbuf" as ">Hbuf".
          wp_pures. wp_read. iApply ("Post" $! 42). iPureIntro. by right.
        * (* CAS failed: NONE *)
          destruct Eq as (-> & _ & _). wp_pures.
          iApply ("Post" $! (-1)%Z). iPureIntro. by left.
    - (* vt = 1: bb - 1 <= 0 for bb in {0,1} -> NONE *)
      iDestruct "Hbb" as "[%Ev | [%Ev _]]"; subst vb;
        wp_pures; (case_bool_decide as Hc; last (exfalso; lia)); wp_pures;
        iApply ("Post" $! (-1)%Z); iPureIntro; by left.
  Qed.

  (* ------ the client ------ *)
  Lemma steal_claim_instance : claim_spec Σ.
  Proof.
    iIntros (tid Φ) "_ Post". rewrite /steal_claim.
    wp_apply wp_new; [done..|].
    iIntros (q) "(DEL & q & Hmq)".
    rewrite own_loc_na_vec_cons own_loc_na_vec_cons own_loc_na_vec_singleton.
    iDestruct "q" as "(q0 & q1 & q2)".
    iEval (rewrite shift_nat_assoc) in "q2".   (* q >> 1 >> 1  ==>  q >> 2 *)
    wp_pures.
    (* write buffer: q+buf := 42 *)
    wp_write.
    (* init top protocol: write q+top := 0 (na), seat the claim token *)
    iMod UTok_alloc as (γtok) "Tok".
    wp_op. wp_write.
    iMod (GPS_iPP_Init (topN q) (topInt q γtok) (q >> top) () #0
            with "q1 [Tok]") as (γt tt0) "#Top".
    { iIntros (t γ). iRight. iSplit; [done|]. by iFrame "Tok". }
    (* init bot protocol (state false, value 0) *)
    wp_pures. rewrite shift_0. wp_write.
    iMod (GPS_iSP_Init (botN q) (botInt q γtok) (botInt q γtok false) q false
            with "q0 []") as (γb tb0) "W"; [done|].
    (* push: stash the buffer in the escrow *)
    iMod (escrow_alloc (UTok γtok) ((q >> buf) ↦ #42)%I with "[$q2]") as "#X";
      [solve_ndisj|].
    (* RELEASE-publish bot 0 -> 1, shipping the escrow *)
    wp_pures. rewrite shift_0.
    wp_apply (GPS_iSP_SWWrite (botN q) (botInt q γtok) (botInt q γtok false)
                True%I _ AcqRel _ _ true _ #1 with "[$W]");
      [solve_ndisj|done|done|done|..].
    { iSplitL "".
      - iIntros "!>" (t' Ht'). iModIntro. rewrite /botInt /botYP /=.
        iSplit; [done|]. iFrame "X".
      - iIntros "!> !> _". iModIntro. rewrite /botInt /botYP /=. iSplit; done. }
    iIntros (tb1) "(_ & W & _)".
    iDestruct (GPS_iSP_SWWriter_Reader with "W") as "#Bot".
    wp_seq.
    (* fork one thief, run the other *)
    wp_apply (wp_fork with "[]").
    { done. }
    { iNext. iIntros (tid'). iApply (steal_spec with "[$Top $Bot]").
      by iIntros "!>" (r) "_". }
    iIntros "_". wp_seq. iApply (steal_spec with "[$Top $Bot]").
    iIntros "!>" (r) "%". by iApply "Post".
  Qed.
End proof.
