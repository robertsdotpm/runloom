/*
 * netpoll_multipool.pml -- Promela model of runloom_pump_dispatch_event's
 * MULTI-POOL dispatch, the per-fd POOL BITMASK fast-path, and the two-level
 * lock hierarchy (netpoll_pump_helpers.c.inc + netpoll_parker_link.c.inc).
 *
 * Per-hub parker pools mean a goroutine parked on hub H links into pool[H].
 * A delivery is processed by a pump that does not know which hub owns the
 * parker.  HISTORICALLY dispatch_event walked EVERY pool; now it consults a
 * per-fd BITMASK -- runloom_fd_poolmask[fd], bit i set => pool i holds a parker
 * for fd -- and VISITS ONLY the set-bit pools:
 *
 *     fm = poolmask[fd];
 *     for each pool p:  if (p's bit clear in fm) skip;
 *                       lock p; if a matching parker is here -> claim it
 *       (commit CAS), record readiness, unlink, wake_g(parker->hub); unlock p.
 *
 * The mask bit is SET in runloom_parker_link (under pool->lock, together with
 * the by_fd insert) BEFORE the caller arms the backend (runloom_netpoll_
 * register).  So the arm happens-after the bit, and any pump whose existence is
 * caused by the fd's event (=> after the arm) is guaranteed to see the bit.
 * The mask adds NO lock (atomic bit ops only), so the lock hierarchy and its
 * rank order are unchanged:
 *
 *     pool->lock  <  hub->sub_lock          (always; never the reverse)
 *     at most ONE pool lock held at a time  (dropped before the next pool)
 *
 * wake_g routes to the parker's HOME hub and takes that hub's sub_lock WHILE
 * holding the pool lock (the nested pool->sub region).
 *
 * PROVEN (two pumps racing one delivery whose parker lives in pool 1, plus a
 * sub_lock contender = a hub draining its submission list):
 *   NO DEADLOCK   -- pool-before-sub, one pool at a time; unchanged by the mask
 *                    (a Spin invalid end state would flag a circular wait).
 *   NO LOST WAKE  -- the mask bit is set BEFORE the arm, so a pump gated on the
 *                    arm always sees the bit and visits the parker's pool; the
 *                    committed-PARKED g is always made runnable (checker).
 *   CLAIMED ONCE  -- the commit CAS makes exactly one pump wake the g (wakes<=1).
 *
 * Negative controls:
 *   -DBUG_LOCK_ORDER     -- contender takes sub then pool (the ABBA a refactor
 *                           could introduce) -> Spin finds the deadlock.
 *   -DBUG_MASK_AFTER_ARM -- link arms the backend BEFORE setting the mask bit
 *                           -> a pump fires on the event, reads the bit as 0,
 *                           SKIPS the parker's pool, the wake is LOST -> the
 *                           checker's assert(g_runnable) fails.
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

/* pool locks for hub 0 / hub 1; sub lock for hub 1 (the parker's home) */
bit poolL0 = 0; bit poolL1 = 0;
bit subL1  = 0;

bit  linked1    = 0;      /* parker linked in pool 1 (its home hub) -- set by waiter */
bit  maskbit1   = 0;      /* dispatch bitmask: pool 1 holds a parker for this fd */
bit  armed      = 0;      /* fd armed in the backend; a pump (the event) only
                             fires after this */
byte commit     = PARKED; /* the g has committed to parking */
int  wakes      = 0;      /* wake_g calls -- must stay <= 1 */
bit  g_runnable = 0;
byte pumps_done = 0;

#define ACQ(L) d_step { (L == 0) -> L = 1 }
#define REL(L) L = 0

/* wait_fd: link the parker into pool 1 + set its mask bit, THEN arm the fd. */
proctype waiter()
{
#ifndef BUG_MASK_AFTER_ARM
    atomic { linked1 = 1; maskbit1 = 1; }   /* link + mask under pool->lock */
    armed = 1;                               /* arm AFTER -> mask is visible */
#else
    linked1 = 1;                             /* BUG: arm BEFORE the mask bit */
    armed = 1;
    maskbit1 = 1;                            /* too late: a pump may have skipped */
#endif
}

/* dispatch_event with the bitmask: runs because the fd's event fired (gated on
 * `armed`), then visits ONLY pools whose mask bit is set.  Pool 0's bit is
 * never set (no parker there) so it is always skipped; pool 1 iff maskbit1. */
proctype pump()
{
    byte prior;

    armed -> skip;            /* the pump exists because the fd's event fired */

    if
    :: maskbit1 ->            /* mask says pool 1 has a parker -> visit it */
        ACQ(poolL1);
        if
        :: linked1 ->
            atomic {          /* runloom_pump_claim */
                prior = commit;
                if :: commit != WOKEN -> commit = WOKEN;
                   :: else            -> skip;
                fi;
            }
            if
            :: prior == WOKEN -> skip;                 /* already claimed: skip */
            :: else ->
                linked1 = 0;                           /* unlink (under pool 1) */
                if
                :: prior == PARKED ->                  /* wake_g -> sub_lock[home=1] */
                    ACQ(subL1);                        /* NESTED: pool 1 held, take sub 1 */
                    wakes++;
                    assert(wakes <= 1);                /* CLAIMED ONCE */
                    g_runnable = 1;
                    REL(subL1);
                :: else -> skip;                       /* ARMED claim: g aborts itself */
                fi;
            fi;
        :: else -> skip;                               /* parker already gone */
        fi;
        REL(poolL1);
    :: else -> skip;          /* mask bit clear -> SKIP pool 1 (the fast-path) */
    fi;

    pumps_done++;
}

/* A hub draining its submission list: takes sub_lock alone (correct), or the
 * reverse order under -DBUG_LOCK_ORDER (the ABBA). */
proctype contender()
{
#ifndef BUG_LOCK_ORDER
    ACQ(subL1);               /* correct: sub lock only, no pool lock */
    REL(subL1);
#else
    ACQ(subL1);               /* BUG: sub lock THEN pool lock (reverse order) */
    ACQ(poolL1);              /* -> ABBA deadlock with a pump holding pool 1 */
    REL(poolL1);
    REL(subL1);
#endif
}

/* NO LOST WAKE: once both pumps have run the delivery, the committed-PARKED g
 * must have been made runnable.  -DBUG_MASK_AFTER_ARM makes a pump skip the
 * parker's pool, leaving g_runnable == 0 -> this assert fails. */
proctype checker()
{
    (pumps_done == 2) -> assert(g_runnable);
}

init {
    atomic {
        run waiter();
        run pump();
        run pump();           /* two pumps race the same delivery */
        run contender();
        run checker();
    }
}
