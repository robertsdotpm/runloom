/*
 * netpoll_multipool.pml -- Promela model of runloom_pump_dispatch_event's
 * MULTI-POOL walk and its two-level lock hierarchy (netpoll.c:1977-2023).
 *
 * Per-hub parker pools mean a goroutine parked on hub H links into pool[H].
 * One epoll delivery is processed by ONE pump (EPOLLONESHOT), which does not
 * know which hub owns the parker, so dispatch_event walks EVERY pool:
 *
 *     for each pool p:  lock p; if a matching parker is here -> claim it
 *       (commit CAS), record readiness, unlink, and wake_g(parker->hub);
 *       unlock p.           // each pool lock is dropped before the next
 *
 * wake_g routes to the parker's HOME hub and takes that hub's sub_lock
 * (runloom_mn_hub_submit, mn_sched.c:1273) -- WHILE still holding the pool lock.
 * So there are two nested levels, and a strict order the code documents
 * (netpoll.c:1972-1976):
 *
 *     pool->lock  <  hub->sub_lock          (always; never the reverse)
 *     at most ONE pool lock held at a time  (dropped before the next pool)
 *
 * Confirmed against the source: the only takers of BOTH locks are
 * dispatch_event and runloom_pump_drain_expired, both pool->sub; every sub_lock
 * region (hub_submit / the hub-drain at mn_sched.c:651) takes the sub lock
 * alone and never a pool lock.
 *
 * PROVEN (two pumps racing one delivery whose parker lives in pool 1, plus a
 * sub_lock contender = a hub draining its submission list):
 *   NO DEADLOCK    -- with everyone respecting pool-before-sub and one pool at
 *                     a time, no circular wait; every actor terminates (a
 *                     deadlock is a Spin invalid end state).
 *   FOUND ANYWHERE -- the parker is found in whichever pool holds it (here the
 *                     second pool walked), regardless of which pump gets there.
 *   CLAIMED ONCE   -- the commit CAS makes exactly one pump wake the g even
 *                     though both find it (`wakes <= 1`); the loser sees the
 *                     parker already unlinked / WOKEN.
 *
 * Negative control -DBUG_LOCK_ORDER makes the contender take its locks in the
 * REVERSE order (sub_lock then pool_lock) -- the ABBA a future refactor could
 * introduce.  Spin finds the deadlock: a pump holds pool 1 and waits for
 * sub 1; the contender holds sub 1 and waits for pool 1.
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

/* pool locks for hub 0 / hub 1; sub locks for hub 0 / hub 1 */
bit poolL0 = 0; bit poolL1 = 0;
bit subL1  = 0;

bit  linked1    = 1;      /* the parker is linked in pool 1 (its home hub) */
byte commit     = PARKED; /* the g has committed to parking (so the pump that
                             claims it must wake_g -> take the sub lock) */
int  wakes      = 0;      /* wake_g calls -- must stay <= 1 */
bit  g_runnable = 0;

#define ACQ(L) d_step { (L == 0) -> L = 1 }
#define REL(L) L = 0

/* dispatch_event: walk pool 0 (no parker), DROP its lock, then pool 1 (find).
 * On find, claim under pool 1's lock and wake_g under sub 1's lock (nested). */
proctype pump()
{
    byte prior;

    ACQ(poolL0);              /* walk pool 0 */
    /* no parker in pool 0 */
    REL(poolL0);              /* drop before taking the next pool lock */

    ACQ(poolL1);              /* walk pool 1 */
    if
    :: linked1 ->
        atomic {              /* runloom_pump_claim */
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
}

/* A hub draining its submission list (mn_sched.c:651): takes sub_lock alone. */
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

init {
    atomic {
        run pump();
        run pump();           /* two pumps race the same delivery */
        run contender();
    }
}
