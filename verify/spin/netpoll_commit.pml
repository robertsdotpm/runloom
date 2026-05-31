/*
 * netpoll_commit.pml -- Promela model of the netpoll park/wake commit
 * protocol in src/pygo_core/netpoll.c (Go's netpollblockcommit, adapted to
 * pygo's re-queue model).  This is the lost-wake guard for I/O parking, the
 * piece where real lost-wake bugs have lived (EPOLLET edge-drop; the residual
 * "missing atomic park-commit" closed by the `commit` field).
 *
 * THE PROTOCOL (pygo_netpoll_wait_fd vs pygo_pump_dispatch_event):
 *
 *   commit: ARMED -> {PARKED | WOKEN}, set once, by a single CAS each.
 *
 *   Parking g (wait_fd): after linking the parker, CAS commit ARMED->PARKED.
 *     - success: g committed; it yields and waits for a pump to re-queue it.
 *     - fail (==WOKEN): a pump claimed the still-ARMED parker first; it has
 *       recorded readiness but did NOT re-queue us, so abort the park and
 *       return the readiness directly.
 *
 *   Pump (pygo_pump_claim): CAS commit ->WOKEN, returning the prior state.
 *     - prior WOKEN: someone already claimed it -- skip entirely (don't touch
 *       ready_out, don't unlink, don't re-queue).
 *     - prior ARMED: g hasn't parked; record readiness + unlink, do NOT
 *       re-queue (the g's own commit CAS will fail and it aborts the park).
 *     - prior PARKED: g parked; record readiness + unlink + re-queue (wake_g).
 *
 *   pool->lock is held across {claim, write ready_out, unlink, wake_g} in the
 *   pump; the parker's abort path re-takes pool->lock before reading
 *   ready_mask, so the readiness write happens-before the parker reads it.
 *
 * PROVEN (1 parking g racing 2 pumps that both see the fd ready):
 *   NO LOST WAKE  -- the parking g always makes progress (returns from
 *                    wait_fd): if it committed PARKED a pump re-queues it; if
 *                    a pump claimed first it aborts the park.  A lost wake
 *                    would leave the g blocked forever at its park = a Spin
 *                    invalid end state.
 *   AT MOST ONCE  -- the g is resumed at most once: at most one pump can claim
 *                    from PARKED (the rest see WOKEN and skip), and a g that
 *                    aborts is never re-queued.  `resumes <= 1`, `requeues<=1`.
 *   READINESS DELIVERED -- whenever the g returns, ready_out was written by the
 *                    claiming pump first (the pool->lock ordering).
 *   MUTUALLY EXCLUSIVE PATHS -- the g never both parks and aborts.
 *
 * Negative control -DBUG_NO_COMMIT drops the commit protocol (g always parks;
 * pump re-queues only if it happens to observe a plain `parked` flag already
 * set) -> Spin finds the classic lost wake: pump checks the flag before the g
 * sets it, declines to wake, g parks forever (invalid end state).
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

byte commit     = ARMED;   /* park->commit                                  */
bit  ready_set  = 0;       /* pump wrote *ready_out (readiness delivered)    */
bit  g_parked   = 0;       /* g committed to PARKED and yielded (suspended)  */
bit  g_returned = 0;       /* g returned from wait_fd                        */
int  resumes    = 0;       /* times g made runnable again (pump wake_g)      */
int  requeues   = 0;       /* pump wake_g calls (must equal resumes)         */

bit  lock = 0;             /* pool->lock                                     */

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

#ifndef BUG_NO_COMMIT
/* ---- correct protocol --------------------------------------------------- */

proctype parker()
{
    byte prev;
    /* CAS commit ARMED->PARKED -- lock-free, races the pumps' claim. */
    atomic {
        prev = commit;
        if
        :: commit == ARMED -> commit = PARKED;   /* CAS success */
        :: else            -> skip;              /* CAS fails (commit==WOKEN) */
        fi;
    }
    if
    :: prev == ARMED ->
        /* committed to parking: yield and wait for a pump to re-queue us. */
        g_parked = 1;
        (resumes > 0);                /* park; lost wake => stuck here */
        assert(ready_set == 1);       /* the waker delivered readiness first */
        g_returned = 1;
    :: else ->
        /* prev == WOKEN: a pump claimed our ARMED parker.  Abort the park and
         * return ready_mask.  We re-take pool->lock to unlink, which orders us
         * after the pump's ready_out write. */
        LOCK; UNLOCK;
        assert(ready_set == 1);
        assert(g_parked == 0);        /* MUTUALLY EXCLUSIVE: never parked */
        g_returned = 1;
    fi;
}

proctype pump()
{
    byte prior;
    LOCK;                              /* dispatch_event holds pool->lock ... */
    atomic {                           /* pygo_pump_claim: CAS commit->WOKEN  */
        prior = commit;
        if
        :: commit != WOKEN -> commit = WOKEN;
        :: else            -> skip;
        fi;
    }
    if
    :: prior == WOKEN -> skip;         /* already claimed: touch nothing */
    :: else ->
        ready_set = 1;                 /* *ready_out = mask, before any wake */
        if
        :: prior == PARKED ->          /* wake_g re-queues the parked g */
            requeues++; resumes++;
            assert(resumes  <= 1);     /* AT MOST ONCE: no double resume */
            assert(requeues <= 1);
        :: else -> skip;               /* ARMED: do NOT wake */
        fi;
    fi;
    UNLOCK;
}

#else
/* ---- BUG_NO_COMMIT: no commit CAS, plain "is it parked?" check ---------- */

proctype parker()
{
    g_parked = 1;                      /* set the bare parked flag ... */
    (resumes > 0);                     /* ... then yield; wake may be lost */
    g_returned = 1;
}

proctype pump()
{
    LOCK;
    if
    :: g_parked == 1 ->                 /* wake */
        ready_set = 1; requeues++; resumes++;
        assert(resumes <= 1);
    :: else -> ready_set = 1;           /* g not parked yet -> drop the wake
                                           (no pending bitmap in this model) */
    fi;
    UNLOCK;
}
#endif

init {
    atomic {
        run parker();
        run pump();
        run pump();                    /* two pumps race -> exercises the
                                          "second claimer sees WOKEN, skips" */
    }
}
