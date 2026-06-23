/*
 * pbuf_bid.pml -- Promela model of the io_uring PROVIDED-BUFFER-RING bid ownership
 * (runloom_iouring_pbuf_return + the multishot-recv handle in io_uring_l_buf.c.inc /
 * io_uring_l_do.c.inc).  (LIFECYCLE_INVARIANTS.md Tier-2 #11.)
 *
 * THE LIFECYCLE.  A process-global ring holds pool_n fixed buffers, bids
 * 0..pool_n-1.  The kernel LENDS a buffer to userspace by delivering it on a CQE
 * (IORING_CQE_F_BUFFER; bid = flags >> IORING_CQE_BUFFER_SHIFT) -- it leaves the
 * ring.  The multishot handle queues it (or carries a partially-consumed one in
 * inflight_bid), ms_recv copies the data out, and runloom_iouring_pbuf_return(bid)
 * puts it BACK in the ring for the kernel to reuse.  On OOM the buffer is returned
 * immediately; on handle close every still-inflight buffer is returned.
 *
 * Each bid is tracked as two counts -- ring[bid] and inflight[bid] -- so the model
 * can SEE a double-return (the same buffer placed in the ring twice), which a single
 * "where is it" enum cannot represent.
 *
 * LIFE-CYCLE invariants proved (every bid moves ring -> inflight -> ring):
 *   PARTITION    -- ring[bid] + inflight[bid] == 1 for every bid, ALWAYS: each buffer
 *                   is in exactly one place, never duplicated, never lost.
 *   NO-DUP       -- ring[bid] <= 1: a buffer is never in the kernel's ring twice
 *                   (a double-return would let the kernel hand it out twice).
 *   NO-LOSS      -- at handle close every bid is back in the ring (ring[bid] == 1);
 *                   a stranded inflight buffer shrinks the ring -> recv stall.
 *
 * Negative controls (must FAIL = pan finds the bug):
 *   -DBUG_DOUBLE_RETURN  : pbuf_return doesn't check the buffer was inflight -> it
 *                          increments ring[bid] for a buffer already in the ring
 *                          (ring[bid] == 2, partition broken).
 *   -DBUG_LOSE_ON_CLOSE  : handle close drops inflight buffers without returning
 *                          them -> bids leak out of the ring permanently.
 */

#define NB    3            /* pool_n: buffers in the ring */
#define NOPS  5

int ring[NB];              /* count of bid's buffers currently in the kernel ring */
int inflight[NB];          /* count lent to userspace (queued / partially consumed) */

/* PARTITION + NO-DUP, checked after every op. */
inline check_partition() {
    ck = 0;
    do
    :: (ck < NB) ->
        assert(ring[ck] + inflight[ck] == 1);     /* exactly one place */
        assert(ring[ck] <= 1);                    /* never duplicated in the ring */
        ck++
    :: (ck >= NB) -> break
    od;
}

/* The kernel lends bid b on a CQE: ring -> inflight.  Only a buffer in the ring
 * can be lent (the kernel picks from the ring). */
inline lend(b) {
    if
    :: (ring[b] >= 1) -> ring[b] = ring[b] - 1; inflight[b] = inflight[b] + 1
    :: else -> skip                  /* not in the ring: kernel picks another */
    fi
}

/* runloom_iouring_pbuf_return(b): inflight -> ring. */
inline pbuf_return(b) {
#ifdef BUG_DOUBLE_RETURN
    ring[b] = ring[b] + 1            /* BUG: return without consuming an inflight ref */
#else
    assert(inflight[b] >= 1);        /* RETURN-ONCE: only an inflight buffer is returnable */
    inflight[b] = inflight[b] - 1;
    ring[b] = ring[b] + 1
#endif
}

active proctype hub()
{
    int i = 0;
    int b;
    int op;
    int ck;                          /* check_partition scratch (hoisted inline local) */

    i = 0;
    do
    :: (i < NB) -> ring[i] = 1; inflight[i] = 0; i++
    :: (i >= NB) -> break
    od;

    i = 0;
    do
    :: (i < NOPS) ->
        i++;
        if :: b = 0 :: b = 1 :: b = 2 fi;
        if
        :: op = 0 -> lend(b)                  /* kernel lends bid b */
        :: op = 1 ->                          /* userspace consumes + returns bid b */
            if
            :: (inflight[b] >= 1) -> pbuf_return(b)
#ifdef BUG_DOUBLE_RETURN
            :: (inflight[b] == 0 && ring[b] >= 1) -> pbuf_return(b)  /* exercise double-return */
#endif
            :: else -> skip
            fi
        fi;
        check_partition()
    :: (i >= NOPS) -> break
    od;

    /* handle close: return every still-inflight buffer (no loss). */
    i = 0;
    do
    :: (i < NB) ->
#ifndef BUG_LOSE_ON_CLOSE
        do
        :: (inflight[i] >= 1) -> pbuf_return(i)
        :: else -> break
        od;
#endif
        i++
    :: (i >= NB) -> break
    od;

    /* NO-LOSS: after close every bid is back in the ring exactly once. */
    i = 0;
    do
    :: (i < NB) -> assert(ring[i] == 1 && inflight[i] == 0); i++
    :: (i >= NB) -> break
    od;
    check_partition()
}
