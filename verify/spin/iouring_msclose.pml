/*
 * iouring_msclose.pml -- Promela model of the io_uring multishot handle
 * lifetime: the free in runloom_iouring_ms_close / on_cqe (io_uring.c) versus a
 * parked runloom_iouring_ms_recv re-locking the handle.
 *
 * AUDIT RESULT THIS FORMALISES.  runloom_iouring_ms_recv parks with the handle's
 * waiter_g set, and on wake RE-LOCKS the handle (io_uring.c:999).  on_cqe, on
 * the closing CQE (was_closing && !more), wakes that waiter and then frees the
 * handle OUTSIDE h->lock -- destroying h->lock + free(h) (io_uring.c:878-891);
 * ms_close's !armed branch frees immediately too (:1018-1032).  RunloomTCPConn
 * holds NO lock around self->ms / self->closed (runloom_tcp.c:44-51), so recv and
 * close are unsynchronised.
 *
 * This is memory-safe ONLY under the single-owner convention: a TCPConn is
 * used by one goroutine, so close() runs after recv() has returned -- there is
 * no consumer parked in ms_recv when the closing CQE frees the handle.
 * (RunloomTCPConn is a standalone primitive; it is NOT used by runloom.aio, and its
 * benches/tests are one-goroutine-per-conn.)  This model proves memory-safety
 * under that convention.
 *
 * Negative control -DBUG_CONCURRENT_CLOSE lifts the convention (a second task
 * closes the conn while the first is parked in recv -- e.g. a shared TCPConn
 * under RUNLOOM_TCPCONN_IOURING=1 on M:N free-threaded).  Spin then finds the
 * use-after-free: the closing CQE wakes the parked consumer and frees the
 * handle, and the woken consumer re-locks freed memory.
 *
 *   NO USE-AFTER-FREE -- the consumer never re-locks/touches the handle after
 *                        it has been freed (assert(freed == 0) at the re-lock).
 */

bit h_lock     = 0;     /* h->lock                                        */
bit freed      = 0;     /* handle freed (lock destroyed + free(h))        */
bit waiter     = 0;     /* a consumer is parked (h->waiter_g set)         */
bit woken      = 0;     /* the parked consumer has been woken             */
bit closing    = 0;     /* ms_close set h->closing                        */
bit recv_done  = 0;     /* ms_recv returned to its caller                 */
bit cancel_cqe = 0;     /* the closing/cancel CQE has been delivered      */

#define LOCK   d_step { (h_lock == 0) -> h_lock = 1 }
#define UNLOCK h_lock = 0

/* runloom_iouring_ms_recv: no data yet -> register waiter, park, and on wake
 * RE-LOCK the handle (io_uring.c:970-1001). */
active proctype recv()
{
    LOCK;
    waiter = 1;                 /* h->waiter_g = current (974) */
    UNLOCK;                     /* (976) */
    (woken);                    /* park_safe; wait for the drain to wake us */
    assert(freed == 0);         /* RE-LOCK the handle (999) -- UAF if freed */
    LOCK;
    recv_done = 1;
    UNLOCK;
}

/* runloom_iouring_ms_close: set closing; armed -> cancel SQE whose CQE frees via
 * on_cqe; this models the armed (cancel) path. */
active proctype close_conn()
{
#ifndef BUG_CONCURRENT_CLOSE
    (recv_done);                /* single-owner: close only after recv returns */
#endif
    LOCK;
    closing = 1;                /* h->closing = 1 (1014) */
    UNLOCK;
    /* fire-and-forget cancel SQE; the drain delivers its CQE (cancel_cqe) */
    cancel_cqe = 1;
}

/* A normal data CQE the drain may deliver, waking a parked consumer with
 * closing clear (no free).  Lets recv make progress in the single-owner flow. */
active proctype drain_data()
{
    (waiter);                     /* a data CQE arrives for a parked consumer */
    LOCK;
    if
    :: waiter -> waiter = 0; woken = 1;   /* on_cqe captures+NULLs waiter, wakes */
    :: else   -> skip;                    /* cancel CQE already took it */
    fi;
    UNLOCK;
    /* closing not set on a data CQE -> no free */
}

/* on_cqe for the closing/cancel CQE: wake the captured waiter (outside lock),
 * then -- because closing && !more -- free the handle OUTSIDE h->lock. */
active proctype drain_cancel()
{
    bit w;
    bit was_closing;
    (cancel_cqe);                /* the cancel CQE arrived */
    LOCK;
    w = waiter; waiter = 0;       /* capture + NULL waiter_g (864-866) */
    was_closing = closing;        /* (868) */
    UNLOCK;                       /* (869) */
    if :: w -> woken = 1;         /* wake the consumer (871-873) */
       :: else -> skip;
    fi;
    if :: was_closing -> freed = 1;   /* free OUTSIDE the lock (878-891) */
       :: else -> skip;
    fi;
}
