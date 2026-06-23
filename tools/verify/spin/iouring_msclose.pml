/*
 * iouring_msclose.pml -- Promela model of the io_uring multishot handle
 * lifetime: the free in runloom_iouring_ms_close / ms_on_cqe versus a parked
 * runloom_iouring_ms_recv re-locking the handle (io_uring_l_do.c.inc,
 * io_uring_l_msclose.c.inc).
 *
 * THE HAZARD.  runloom_iouring_ms_recv parks with the handle's waiter_g set, and
 * on wake RE-LOCKS the handle.  ms_on_cqe, on the closing CQE (was_closing &&
 * !more), wakes that waiter and then would free the handle OUTSIDE h->lock
 * (destroy h->lock + free(h)); ms_close's !armed branch frees too.  RunloomTCPConn
 * holds no lock around self->ms / self->closed, so on a SHARED conn recv and
 * close are unsynchronised -- a reader parked in ms_recv while another fiber
 * close()s the same conn.
 *
 * THE FIX (modelled here): a refcount on the handle.  ms_open takes a "live" ref;
 * an in-progress ms_recv takes a ref for its whole call -- ACROSS its park -- and
 * the terminal closing CQE / ms_close drop the live ref.  Whoever drops the LAST
 * ref frees.  So the wake-then-free can no longer free under a resuming recv: the
 * recv-ref keeps the handle alive until the recv has re-locked and returned.
 *
 *   NO USE-AFTER-FREE -- the consumer never re-locks the handle after it is freed
 *                        (assert(freed == 0) at the re-lock), even under a
 *                        concurrent close.
 *
 * Controls:
 *   -DBUG_CONCURRENT_CLOSE  lift the single-owner convention (close races a parked
 *                           recv).  WITH the refcount this is now SAFE (the new
 *                           guarantee) -- it must PASS.
 *   -DBUG_NO_REFCOUNT       drop the refcount (the OLD code) AND lift the
 *                           convention -> the closing CQE frees while the recv is
 *                           parked -> the woken recv re-locks freed memory.  Spin
 *                           finds the use-after-free.  Must FAIL.
 */

bit  h_lock     = 0;     /* h->lock                                        */
bit  freed      = 0;     /* handle freed (lock destroyed + free(h))        */
bit  waiter     = 0;     /* a consumer is parked (h->waiter_g set)         */
bit  woken      = 0;     /* the parked consumer has been woken             */
bit  closing    = 0;     /* ms_close set h->closing                        */
bit  recv_done  = 0;     /* ms_recv returned to its caller                 */
bit  recv_parked= 0;     /* ms_recv took its ref + parked (never cleared)  */
bit  cancel_cqe = 0;     /* the closing/cancel CQE has been delivered      */
byte refcount   = 1;     /* lifetime refcount; the "live" ref from ms_open */

#define LOCK   d_step { (h_lock == 0) -> h_lock = 1 }
#define UNLOCK h_lock = 0

/* runloom_iouring_ms_recv: no data yet -> take a recv-ref, register waiter, park,
 * and on wake RE-LOCK the handle, then drop the recv-ref (free iff last). */
active proctype recv()
{
    LOCK;
#ifndef BUG_NO_REFCOUNT
    refcount = refcount + 1;     /* recv-ref: pins h across the park */
#endif
    waiter = 1;                  /* h->waiter_g = current */
    recv_parked = 1;             /* ref taken + parked (caller held h alive at entry) */
    UNLOCK;
    (woken);                     /* park; wait for a drainer to wake us */
    assert(freed == 0);          /* RE-LOCK the handle -- UAF if freed */
    LOCK;
    recv_done = 1;
#ifndef BUG_NO_REFCOUNT
    refcount = refcount - 1;     /* drop recv-ref under the lock (ms_put) */
    if
    :: refcount == 0 -> UNLOCK; freed = 1;   /* we were last -> free */
    :: else          -> UNLOCK;
    fi;
#else
    UNLOCK;
#endif
}

/* runloom_iouring_ms_close: set closing; the armed path fires a cancel SQE whose
 * terminal CQE frees via ms_on_cqe. */
active proctype close_conn()
{
#if !defined(BUG_CONCURRENT_CLOSE) && !defined(BUG_NO_REFCOUNT)
    (recv_done);                 /* single-owner: close only after recv returns */
#else
    (recv_parked);               /* concurrent: close races a recv that has ALREADY
                                  * parked (holds its ref) -- the real shared-conn
                                  * scenario.  A recv that hasn't entered can't race
                                  * a freed handle: the caller's self->ms=NULL after
                                  * close stops a NEW recv from entering. */
#endif
    LOCK;
    closing = 1;                 /* h->closing = 1 */
    UNLOCK;
    cancel_cqe = 1;              /* fire-and-forget cancel; drain delivers its CQE */
}

/* A normal data CQE the drain may deliver, waking a parked consumer with closing
 * clear (no free) -- lets recv make progress in the single-owner flow. */
active proctype drain_data()
{
    /* A data CQE is OPTIONAL -- it need never arrive (the cancel CQE may wake the
     * consumer instead).  `end_dd` marks this wait as a valid end state so a run
     * where no data CQE comes is not a (spurious) invalid-end-state. */
end_dd:
    (waiter);
    LOCK;
    if
    :: waiter -> waiter = 0; woken = 1;   /* ms_on_cqe captures+NULLs waiter, wakes */
    :: else   -> skip;                    /* cancel CQE already took it */
    fi;
    UNLOCK;
}

/* ms_on_cqe for the closing/cancel CQE: wake the captured waiter (outside lock),
 * then -- because closing && !more -- drop the live ref (ms_put), freeing iff
 * no recv-ref is still held. */
active proctype drain_cancel()
{
    bit w;
    bit was_closing;
    (cancel_cqe);
    LOCK;
    w = waiter; waiter = 0;       /* capture + NULL waiter_g */
    was_closing = closing;
    UNLOCK;
    if :: w -> woken = 1;         /* wake the consumer */
       :: else -> skip;
    fi;
    if :: was_closing ->
#ifndef BUG_NO_REFCOUNT
            LOCK;
            refcount = refcount - 1;          /* drop the live ref (ms_put) */
            if
            :: refcount == 0 -> UNLOCK; freed = 1;
            :: else          -> UNLOCK;
            fi;
#else
            freed = 1;     /* OLD: free unconditionally outside the lock -> UAF
                            * if a recv is parked and about to re-lock */
#endif
       :: else -> skip;
    fi;
}
