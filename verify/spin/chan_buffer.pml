/*
 * chan_buffer.pml -- Promela model of runloom's buffered-channel ring +
 * waiter FIFO, the subsystem in src/runloom_c/chan_ops.c.inc (chan_send_locked
 * / chan_recv_locked / runloom_chan_close) on top of the ring helpers
 * buf_push / buf_pop and the waiter helpers waiter_push / waiter_pop /
 * waiter_pop_claimable / park_waiter / wake_waiter (chan_waiters.c.inc),
 * with the channel struct (buf/head/tail/blen/cap, senders/senders_tail,
 * receivers/receivers_tail, closed) from chan.c.
 *
 * Everything below the lock is modelled in `atomic` blocks: the C does every
 * decision (closed-check, claimable-waiter pop, buf room check, push/pop, the
 * recv-side "pull a parked sender's value into the freed slot") WHILE HOLDING
 * ch->lock, then drops the lock before yielding.  park_waiter is split exactly
 * as in the source: the locked enqueue (waiter_push, set queued=1) happens
 * under the lock, the fiber then yields with the lock dropped, and on wake it
 * re-acquires + checks `queued` (re-park if a spurious wake left it linked).
 * wake_waiter / waiter_pop run on the producer/close side under the lock.
 *
 * SUBSYSTEM SCOPE.  This models the buffered + waiter-FIFO data path, NOT the
 * select() multi-channel claim CAS (that is select_claim.pml / select_close.pml
 * / Select.v).  All waiters here are plain (select == NULL), so waiter_claim
 * always wins and waiter_pop_claimable == waiter_pop (faithful: the model's
 * pop never skips a tombstone because plain waiters never tombstone).
 *
 * PROPERTIES (cap-N buffer, sender FIFO + receiver FIFO, all under one lock):
 *
 *   (1) CONSERVATION -- every value a sender produced is, at every reachable
 *       state, in EXACTLY ONE of: still held by a parked sender, sitting in the
 *       ring buffer, or received exactly once.  No value is lost; none is
 *       duplicated.  Checked by a per-value owner tag (vstate[]) that every
 *       transition moves hand-to-hand, plus an end-state census
 *       (produced == in_buf + in_waiter + received).
 *
 *   (2) FIFO -- the receiver that parked first is woken (claimed) first, and
 *       likewise for senders.  Each parked waiter takes a monotonically
 *       increasing ticket on push; waiter_pop asserts it pops the smallest
 *       outstanding ticket (head of the queue), i.e. strict FIFO.
 *
 *   (3) BOUNDS -- ch->blen stays in [0, cap] at every step; the model asserts
 *       buf_push is only ever reached with blen < cap (no send-into-full) and
 *       buf_pop only with blen > 0 (no recv-from-empty).
 *
 *   (4) NON-BLOCK -- a blocking recv that finds the buffer non-empty OR a
 *       parked sender NEVER parks (it returns a value without enqueuing a
 *       receiver waiter).  Checked by a flag set iff a recv parks while the
 *       buffer was non-empty or a sender was queued at decision time.
 *
 * NEGATIVE CONTROLS (each reintroduces one real bug; the model then FAILS):
 *   -DBUG_LIFO_WAITERS   wake waiters LIFO (pop the tail, not the head) ->
 *                        FIFO violation (property 2).
 *   -DBUG_DROP_ON_CLOSE  close drops a buffered value (zeroes blen) instead of
 *                        leaving receivers to drain it -> conservation loss
 *                        (property 1): a produced value ends up nowhere.
 *
 * Template/style: verify/spin/select_close.pml.
 */

/* ---- sizing (kept tiny: exhaustive search) ----
 * Default is a cap-2 BUFFERED channel.  -DUNBUF flips to the cap-0 UNBUFFERED
 * channel (cap == 0): chan_send_locked's `ch->cap > 0` room-check is false, so
 * a sender with no waiting receiver parks IMMEDIATELY (empty buffer), and a
 * recv with an empty buffer + a parked sender takes the chan_recv_locked
 * unbuffered-handoff branch (blen==0 && sq_cnt>0).  Both configs are verified;
 * UNBUF is the one that exercises the rendezvous handoff + the SENDER FIFO. */
#ifdef UNBUF
#  define CAP      0     /* ch->cap : unbuffered (rendezvous)              */
#  define RING     1     /* a 0-length Promela array is illegal; size 1,   */
                          /* never written (buf_push/pop are unreachable    */
                          /* when CAP==0).                                  */
#else
#  define CAP      2     /* ch->cap : ring-buffer capacity                 */
#  define RING     CAP
#endif
#define NSEND      4     /* sender goroutines (each produces ONE value)    */
#define NRECV      2     /* receiver goroutines                            */
#define NVAL       NSEND /* one value id per sender (ids 1..NSEND)         */
/* NSEND=4 > CAP+? so that with CAP=2 the buffer fills and TWO senders park
 * simultaneously -- the SENDER FIFO needs >=2 parked senders to have teeth
 * (one parked sender makes FIFO==LIFO vacuous).  NRECV=2 keeps two receivers
 * able to park together for the RECEIVER FIFO.  Both BUG_LIFO_WAITERS queues
 * are thus exercised. */

/* per-value ownership (CONSERVATION).  Index 1..NVAL; 0 unused. */
#define V_NONE     0     /* not yet produced                               */
#define V_WAITER   1     /* held by a parked sender's waiter               */
#define V_BUF      2     /* sitting in the ring buffer                     */
#define V_RECVD    3     /* received (delivered to a receiver) exactly once */
byte vstate[NVAL + 1];

/* ---- channel state (chan.c struct runloom_chan) ---- */
byte rbuf[RING];         /* ch->buf : ring of VALUE IDS (0 = empty slot)   */
byte head = 0;           /* ch->head                                       */
byte tail = 0;           /* ch->tail                                       */
byte blen  = 0;           /* ch->blen  : MUST stay in [0,cap]                */
bit  closed = 0;         /* ch->closed                                     */

/* ---- waiter FIFO queues (senders / receivers) ----
 * A waiter is one slot in an array; head/tail/count model the singly-linked
 * FIFO (waiter_push appends at tail, waiter_pop removes at head).  Each slot
 * carries: gid (which goroutine), val (sender's value id; 0 for receivers),
 * a per-queue monotonic ticket (for the FIFO assertion), and the wake fields
 * the producer fills (delivered value + ok/send_result) that park_waiter reads
 * back after the lock re-acquire. */
#define QMAX (NSEND + NRECV)

/* sender queue */
byte sq_gid[QMAX]; byte sq_val[QMAX]; short sq_tic[QMAX];
byte sq_head = 0; byte sq_tail = 0; byte sq_cnt = 0;
short sq_next_ticket = 0;     /* next ticket to hand out (monotonic)        */
short sq_last_pop = -1;       /* last ticket popped (must strictly increase) */

/* receiver queue */
byte rq_gid[QMAX]; short rq_tic[QMAX];
byte rq_head = 0; byte rq_tail = 0; byte rq_cnt = 0;
short rq_next_ticket = 0;
short rq_last_pop = -1;

/* per-goroutine wake mailbox (filled by producer/close, read on wake).
 * gid in 1..(NSEND+NRECV).  For a sender: send_result (0 delivered / -1 closed).
 * For a receiver: rx_ok (1 got value / 0 closed) + rx_val (value id). */
bit  woken[QMAX + 1];
short send_result[QMAX + 1];
short rx_ok[QMAX + 1];
short rx_val[QMAX + 1];
bit  queued[QMAX + 1];        /* waiter->queued (lock-protected lifecycle)  */

/* ---- bookkeeping for the end-state census + properties ---- */
byte produced = 0;            /* count of values a sender produced          */
byte received = 0;            /* count of values a receiver returned (ok=1) */
byte nfin = 0;                /* finished goroutines                        */
bit  block_violation = 0;     /* set if a recv parked despite a ready source */

/* count how many ids are currently in each owner state (CONSERVATION census) */
inline census(c_buf, c_wait, c_recv) {
    c_buf = 0; c_wait = 0; c_recv = 0;
    byte vi;
    vi = 1;
    do
    :: (vi <= NVAL) ->
        if
        :: (vstate[vi] == V_BUF)    -> c_buf  = c_buf  + 1
        :: (vstate[vi] == V_WAITER) -> c_wait = c_wait + 1
        :: (vstate[vi] == V_RECVD)  -> c_recv = c_recv + 1
        :: else -> skip
        fi;
        vi = vi + 1
    :: else -> break
    od
}

/* ---- ring buffer (buf_push / buf_pop) ---- */
/* buf_push: caller verified blen < cap.  Steals (transfers) the value's
 * ownership -> V_BUF.  Asserts the no-send-into-full bound. */
inline buf_push(v) {
    assert(blen < CAP);                 /* (3) never push into a full buffer */
    rbuf[tail] = v;
    tail = (tail + 1) % RING;           /* RING==CAP buffered; ==1 (dead) unbuf */
    blen = blen + 1;
    vstate[v] = V_BUF
}
/* buf_pop: caller verified blen > 0.  Returns the id into `out`. */
inline buf_pop(out) {
    assert(blen > 0);                   /* (3) never pop from an empty buffer */
    out = rbuf[head];
    rbuf[head] = 0;
    head = (head + 1) % RING;
    blen = blen - 1
}

/* ---- waiter_push / waiter_pop : the FIFO ---- */
/* sender push: append at tail, hand out next ticket, set queued=1. */
inline sq_push(gid, v) {
    sq_gid[sq_tail] = gid; sq_val[sq_tail] = v; sq_tic[sq_tail] = sq_next_ticket;
    sq_next_ticket = sq_next_ticket + 1;
    sq_tail = (sq_tail + 1) % QMAX;
    sq_cnt = sq_cnt + 1;
    queued[gid] = 1
}
/* sender pop (waiter_pop_claimable: plain waiters always claim).
 * FIXED pops the head (FIFO).  BUG_LIFO_WAITERS pops the tail (LIFO).
 * Asserts the popped ticket strictly exceeds the last -> strict FIFO order. */
inline sq_pop(o_gid, o_val) {
    byte idx;
#ifdef BUG_LIFO_WAITERS
    idx = (sq_tail + QMAX - 1) % QMAX;          /* LIFO: most-recent first   */
#else
    idx = sq_head;                              /* FIFO: oldest first        */
#endif
    o_gid = sq_gid[idx]; o_val = sq_val[idx];
    assert(sq_tic[idx] > sq_last_pop);          /* (2) strict sender FIFO    */
    sq_last_pop = sq_tic[idx];
#ifdef BUG_LIFO_WAITERS
    sq_tail = idx;                              /* drop the tail element     */
#else
    sq_head = (sq_head + 1) % QMAX;             /* drop the head element     */
#endif
    sq_cnt = sq_cnt - 1;
    queued[o_gid] = 0                           /* waiter_pop clears queued  */
}

inline rq_push(gid) {
    rq_gid[rq_tail] = gid; rq_tic[rq_tail] = rq_next_ticket;
    rq_next_ticket = rq_next_ticket + 1;
    rq_tail = (rq_tail + 1) % QMAX;
    rq_cnt = rq_cnt + 1;
    queued[gid] = 1
}
inline rq_pop(o_gid) {
    byte idx;
#ifdef BUG_LIFO_WAITERS
    idx = (rq_tail + QMAX - 1) % QMAX;
#else
    idx = rq_head;
#endif
    o_gid = rq_gid[idx];
    assert(rq_tic[idx] > rq_last_pop);          /* (2) strict receiver FIFO  */
    rq_last_pop = rq_tic[idx];
#ifdef BUG_LIFO_WAITERS
    rq_tail = idx;
#else
    rq_head = (rq_head + 1) % QMAX;
#endif
    rq_cnt = rq_cnt - 1;
    queued[o_gid] = 0
}

/* ===================================================================
 * SENDER goroutine -- one per value.  Models chan_send_locked(blocking=1).
 * gid in 1..NSEND ; produces value id == gid.
 * =================================================================== */
proctype sender(byte gid)
{
    byte v = gid;                 /* this sender's unique value id          */
    byte rgid;                    /* receiver gid for a direct handoff      */
    bit parked = 0;

    atomic {
        /* produce: we now hold a ref on v (V_WAITER == "in our hand") */
        vstate[v] = V_WAITER;
        produced = produced + 1;

        /* chan_send_locked decision order (under the lock): */
        if
        :: (closed) ->
            /* send on closed channel -> raises.  Our value is dropped on the
             * error path; account for it so the census stays exact (it was
             * never delivered: not produced into the system).  Model this by
             * un-producing -- equivalently it never entered the channel. */
            vstate[v] = V_NONE;
            produced = produced - 1;
            goto sdone
        :: (!closed && rq_cnt > 0) ->
            /* receivers waiting -> direct handoff to FIRST receiver, wake it. */
            rq_pop(rgid);
            rx_val[rgid] = v; rx_ok[rgid] = 1;
            vstate[v] = V_RECVD; received = received + 1;
            woken[rgid] = 1;
            goto sdone
        :: (!closed && rq_cnt == 0 && CAP > 0 && blen < CAP) ->
            /* ch->cap > 0 && ch->len < ch->cap : buffer has room -> push. */
            buf_push(v);
            goto sdone
        :: (!closed && rq_cnt == 0 && !(CAP > 0 && blen < CAP)) ->
            /* full / unbuffered + no receivers -> PARK as sender. */
            send_result[gid] = -1;
            woken[gid] = 0;
            sq_push(gid, v);      /* waiter_push: queued=1, holds ref on v   */
            parked = 1
        fi
    }

    /* ---- parked: park_waiter's re-park loop.  Lock dropped, yield, on wake
     * re-acquire and check `queued`; re-park if still linked (spurious). ---- */
    if
    :: (parked) ->
        do
        :: atomic {
               (woken[gid] == 1);           /* a wake is pending             */
               if
               :: (queued[gid] == 0) ->     /* producer/close popped us -> real */
                   if
                   :: (send_result[gid] == 0) ->
                       /* delivered: receiver/buffer took our ref (v moved by
                        * the popper; we own nothing now). */
                       skip
                   :: (send_result[gid] == -1) ->
                       /* closed-while-parked: we still hold v, DECREF it.
                        * The value never entered the system -> un-produce. */
                       assert(vstate[v] == V_WAITER);
                       vstate[v] = V_NONE; produced = produced - 1
                   fi;
                   break
               :: (queued[gid] == 1) ->     /* spurious wake: re-park        */
                   woken[gid] = 0
               fi
           }
        od
    :: (!parked) -> skip
    fi;

sdone:
    atomic { nfin = nfin + 1 }
}

/* ===================================================================
 * RECEIVER goroutine.  Models chan_recv_locked(blocking=1).
 * gid in (NSEND+1)..(NSEND+NRECV).
 * =================================================================== */
proctype receiver(byte gid)
{
    byte r;                       /* received value id                      */
    byte tgid; byte tval;         /* a parked sender we pull a value from   */
    bit parked = 0;
    bit ready_at_decision;        /* (4): was a source ready when we parked? */

    atomic {
        /* chan_recv_locked decision order (under the lock):
         * Buffered values take PRIORITY over closed. */
        if
        :: (blen > 0) ->
            /* pop from ring; if a sender is parked (buffer was full), pull its
             * value into the freed slot and wake it. */
            buf_pop(r);
            vstate[r] = V_RECVD; received = received + 1;
            if
            :: (sq_cnt > 0) ->
                sq_pop(tgid, tval);
                buf_push(tval);             /* transfers tx's ref into slot  */
                send_result[tgid] = 0;
                woken[tgid] = 1
            :: (sq_cnt == 0) -> skip
            fi;
            goto rdone
        :: (blen == 0 && sq_cnt > 0) ->
            /* buffer empty, sender waiting -> unbuffered handoff (steal ref). */
            sq_pop(tgid, tval);
            r = tval;
            vstate[r] = V_RECVD; received = received + 1;
            send_result[tgid] = 0;
            woken[tgid] = 1;
            goto rdone
        :: (blen == 0 && sq_cnt == 0 && closed) ->
            /* empty + no senders + closed -> (None, ok=0). */
            goto rdone
        :: (blen == 0 && sq_cnt == 0 && !closed) ->
            /* empty + no senders + open -> PARK as receiver. */
            ready_at_decision = 0;          /* nothing ready -> blocking is OK */
            rx_ok[gid] = -1;
            woken[gid] = 0;
            rq_push(gid);
            parked = 1
        fi;
        /* (4) NON-BLOCK: a recv that parked must NOT have had a ready source.
         * The guards above guarantee parked only when blen==0 && sq_cnt==0, so
         * a park with a ready source is impossible by construction; this flag
         * records the property explicitly for the assertion below. */
        if
        :: (parked && (blen > 0 || sq_cnt > 0)) -> block_violation = 1
        :: else -> skip
        fi
    }

    if
    :: (parked) ->
        do
        :: atomic {
               (woken[gid] == 1);
               if
               :: (queued[gid] == 0) ->     /* real wake                     */
                   if
                   :: (rx_ok[gid] == 1) ->
                       r = rx_val[gid];      /* sender handed us a value      */
                       /* already counted received + V_RECVD by the sender    */
                       skip
                   :: (rx_ok[gid] == 0) ->
                       skip                  /* closed-and-empty: (None,0)    */
                   fi;
                   break
               :: (queued[gid] == 1) ->     /* spurious: re-park             */
                   woken[gid] = 0
               fi
           }
        od
    :: (!parked) -> skip
    fi;

rdone:
    atomic { nfin = nfin + 1 }
}

/* ===================================================================
 * CLOSER goroutine.  Models runloom_chan_close.
 * Buffered values are LEFT for receivers to drain (recv's blen>0 branch has
 * priority over closed); only parked senders/receivers are woken.
 * =================================================================== */
proctype closer()
{
    byte g; byte v;
    atomic {
        if
        :: (!closed) ->
            closed = 1;
#ifdef BUG_DROP_ON_CLOSE
            /* BUG: close discards whatever is buffered (drops the ring) instead
             * of leaving receivers to drain it via recv's blen>0 priority
             * branch.  Each dropped value is freed WITHOUT being delivered, so
             * it leaves the system entirely (vstate -> V_NONE): it is neither
             * buffered, nor held by a waiter, nor received.  A genuine
             * conservation LOSS -- the running/terminal census
             * `produced == in_buf + in_waiter + received` no longer holds. */
            do
            :: (blen > 0) -> buf_pop(v); vstate[v] = V_NONE  /* LOST, not delivered */
            :: (blen == 0) -> break
            od;
#endif
            /* wake every parked sender with send_result=-1 (they raise + drop
             * their held value on the error path -> un-produce on wake). */
            do
            :: (sq_cnt > 0) ->
                sq_pop(g, v);
                send_result[g] = -1;
                woken[g] = 1
            :: (sq_cnt == 0) -> break
            od;
            /* wake every parked receiver with ok=0. */
            do
            :: (rq_cnt > 0) ->
                rq_pop(g);
                rx_ok[g] = 0; rx_val[g] = 0;
                woken[g] = 1
            :: (rq_cnt == 0) -> break
            od
        :: (closed) -> skip       /* double close: error path, no state change */
        fi
    }
    atomic { nfin = nfin + 1 }
}

/* ===================================================================
 * Per-step safety monitor: ch->blen in [0,cap] at every reachable state,
 * and the running census never shows a value duplicated or vanished
 * (produced == in_buf + in_waiter + received at every step).
 * =================================================================== */
active proctype invariant_monitor()
{
    byte cb; byte cw; byte cr;
    do
    :: atomic {
           assert(blen >= 0 && blen <= CAP);          /* (3) bounds            */
           census(cb, cw, cr);
           /* (1) CONSERVATION running form: every produced value is accounted
            * for in exactly one owner state, never lost, never duplicated. */
           assert(produced == cb + cw + cr);
           assert(cr == received);                  /* received tally matches */
           assert(block_violation == 0);            /* (4) no bad blocking   */
       }
    od
}

init {
    atomic {
        byte i;
        i = 1;
        do
        :: (i <= NSEND) -> run sender(i); i = i + 1
        :: else -> break
        od;
        i = NSEND + 1;
        do
        :: (i <= NSEND + NRECV) -> run receiver(i); i = i + 1
        :: else -> break
        od;
        run closer()
    }

    /* ---- terminal census (CONSERVATION end-state) ----
     * Once every goroutine has finished, every value a sender produced is in
     * exactly one of {buffer, waiter, received}, none lost or duplicated. */
    (nfin == NSEND + NRECV + 1);
    atomic {
        byte cb; byte cw; byte cr;
        census(cb, cw, cr);
        assert(produced == cb + cw + cr);     /* no loss, no duplication      */
        assert(cr == received);
        assert(blen == cb);                    /* blen matches buffered census  */
        assert(blen >= 0 && blen <= CAP);
    }
}
