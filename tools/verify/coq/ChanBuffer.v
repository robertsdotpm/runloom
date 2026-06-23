(* ChanBuffer.v -- machine-checked, UNBOUNDED conservation for runloom's
   buffered channel + waiter FIFO.

   verify/spin/chan_buffer.pml proves CONSERVATION / FIFO / bounds / non-block
   for the buffered-channel ring + sender/receiver waiter queues
   (src/runloom_c/chan_ops.c.inc chan_send_locked / chan_recv_locked /
   runloom_chan_close on top of buf_push / buf_pop / waiter_pop_claimable /
   park_waiter / wake_waiter) by EXHAUSTIVE model checking, but only at a
   bounded instance (CAP=2, a few senders/receivers).  This Coq development
   proves the conservation invariant over EVERY reachable state of the
   transition system -- ANY number of sends, receives, buffered values, parked
   senders, and a close -- via an inductive invariant, with NO bound on the
   counts.

   The state is the counting abstraction of chan.c's struct runloom_chan +
   the waiter queues:
     buffered : values currently in the ring buffer            (ch->len)
     waiting  : values held by PARKED SENDERS (one each)        (|ch->senders|)
     received : values delivered to a receiver, exactly once
     produced : values a sender successfully put into the system
                (a send on a closed channel raises and never enters, so is
                 not counted; a parked sender woken by close un-produces).
   The single invariant ties them:

       received + buffered + waiting = produced      (the conservation law)

   which is EXACTLY property (1) of the Spin model: every value a sender
   produced is, at every reachable state, in exactly one of {ring buffer,
   a parked sender's hand, received-exactly-once} -- none lost, none
   duplicated.  NO-DOUBLE-RECEIVE follows: `received` only ever grows when a
   value LEAVES the buffer or a waiter (the same atomic step), so a value
   cannot be received twice (that would need received + buffered + waiting to
   exceed produced).

   Each transition mirrors one locked decision branch of the C:
     T_send_handoff  chan_send_locked: receivers waiting -> direct handoff
                     (produced++, received++).
     T_send_buffer   chan_send_locked: cap>0 && len<cap -> buf_push
                     (produced++, buffered++).
     T_send_park     chan_send_locked: full/unbuffered, no rx -> park sender
                     (produced++, waiting++; the sender HOLDS the value).
     T_recv_buffer   chan_recv_locked: len>0, no parked sender -> buf_pop
                     (buffered--, received++).
     T_recv_pull     chan_recv_locked: len>0 AND a sender parked -> buf_pop
                     then pull the sender's value into the freed slot
                     (buffered stays, waiting--, received++).
     T_recv_rendez   chan_recv_locked: len==0, sender parked -> unbuffered
                     handoff (waiting--, received++).
     T_close_sender  runloom_chan_close: wake a parked sender with "closed";
                     it DROPS its held value (Py_DECREF) -> waiting--,
                     produced-- (the value leaves WITHOUT delivery).
   Buffered values are deliberately NOT touched by close (recv's len>0 branch
   drains them after close); modelling close as dropping a BUFFERED value is
   exactly the BUG_DROP_ON_CLOSE negative control, shown below to violate the
   invariant (invariant_has_teeth).

   Scope: the counting/conservation abstraction (like the Spin model but
   unbounded), not the ring index arithmetic, the FIFO ORDER (Spin proves that
   with per-waiter tickets), nor the C11 memory orders / lock discipline (CBMC
   + the lock-rank checks cover those).

   Build: coqc ChanBuffer.v   (verify/coq/run_coq.sh runs it). *)

From Stdlib Require Import Lia.

Record Chan : Set := mkChan {
  buffered : nat;   (* values in the ring buffer            (ch->len)        *)
  waiting  : nat;   (* values held by parked senders        (|ch->senders|)  *)
  received : nat;   (* values delivered to receivers, once each              *)
  produced : nat    (* values a sender put into the system                   *)
}.

Definition init : Chan := mkChan 0 0 0 0.

Inductive step : Chan -> Chan -> Prop :=
  (* send, receiver already waiting: direct handoff *)
  | T_send_handoff : forall b w r p,
      step (mkChan b w r p) (mkChan b w (S r) (S p))
  (* send, buffer has room: push to the ring *)
  | T_send_buffer : forall b w r p,
      step (mkChan b w r p) (mkChan (S b) w r (S p))
  (* send, full / unbuffered, no receiver: park as sender (holds the value) *)
  | T_send_park : forall b w r p,
      step (mkChan b w r p) (mkChan b (S w) r (S p))
  (* recv, buffer non-empty, no parked sender: pop one *)
  | T_recv_buffer : forall b w r p,
      step (mkChan (S b) w r p) (mkChan b w (S r) p)
  (* recv, buffer non-empty AND a sender parked: pop one, pull the sender's
     value into the freed slot -> buffered unchanged, one fewer waiter,
     one more received.  Enabled iff a buffered value AND a parked sender
     exist (S b, S w). *)
  | T_recv_pull : forall b w r p,
      step (mkChan (S b) (S w) r p) (mkChan (S b) w (S r) p)
  (* recv, buffer empty, sender parked: unbuffered rendezvous handoff *)
  | T_recv_rendez : forall w r p,
      step (mkChan 0 (S w) r p) (mkChan 0 w (S r) p)
  (* close wakes a parked sender: it raises + DROPS its held value, which
     leaves the system without being received.  The dropped value was counted
     in `produced` (the sender had put it in), so close un-produces it:
     waiting--, produced-- (here produced is (S p) so it stays well-formed). *)
  | T_close_sender : forall b w r p,
      step (mkChan b (S w) r (S p)) (mkChan b w r p).

(* THE CONSERVATION LAW: received + buffered + waiting = produced. *)
Definition Inv (c : Chan) : Prop :=
  received c + buffered c + waiting c = produced c.

Lemma init_inv : Inv init.
Proof. unfold Inv, init; simpl; lia. Qed.

Lemma step_preserves : forall c c', Inv c -> step c c' -> Inv c'.
Proof.
  intros c c' HInv Hstep.
  inversion Hstep; subst; unfold Inv in *; cbn [buffered waiting received produced] in *; lia.
Qed.

Inductive reachable : Chan -> Prop :=
  | reach_init : reachable init
  | reach_step : forall c c', reachable c -> step c c' -> reachable c'.

Theorem inv_reachable : forall c, reachable c -> Inv c.
Proof.
  intros c H; induction H.
  - apply init_inv.
  - eapply step_preserves; eauto.
Qed.

(* CONSERVATION, stated directly: at every reachable state the received items
   plus what is still buffered plus what parked senders still hold equals
   exactly what was produced.  Nothing is lost; nothing is duplicated. *)
Theorem conservation :
  forall c, reachable c -> received c + buffered c + waiting c = produced c.
Proof. intros c H; apply inv_reachable in H; exact H. Qed.

(* NO OVER-RECEIVE / NO PHANTOM DELIVERY: a channel never delivers more values
   than were produced (a duplicate receive would need received > produced). *)
Theorem no_over_receive :
  forall c, reachable c -> received c <= produced c.
Proof. intros c H; apply inv_reachable in H; unfold Inv in H; lia. Qed.

(* NO LOSS: everything produced is still accounted for -- buffered, in a
   waiter's hand, or received.  (Same identity, read the other way.) *)
Theorem no_loss :
  forall c, reachable c ->
    produced c - received c = buffered c + waiting c.
Proof. intros c H; apply inv_reachable in H; unfold Inv in H; lia. Qed.

(* Teeth: BUG_DROP_ON_CLOSE -- close drops a BUFFERED value (buffered--) instead
   of leaving receivers to drain it, WITHOUT delivering it (received unchanged).
   That value vanishes: the state it lands in violates the conservation law,
   exactly as the Spin negative control's `produced == in_buf+in_waiter+received`
   assertion catches it. *)
Theorem invariant_has_teeth :
  Inv (mkChan 1 0 0 1) /\ ~ Inv (mkChan 0 0 0 1).
Proof.
  split.
  - unfold Inv; simpl; lia.        (* one buffered value, one produced: holds  *)
  - unfold Inv; simpl; lia.        (* dropped (buffered 1->0, undelivered): NO *)
Qed.
