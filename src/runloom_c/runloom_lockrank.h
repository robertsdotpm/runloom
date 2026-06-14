/* runloom_lockrank.h -- debug-only lock-ordering checker.
 *
 * runloom has a multi-class lock graph (registry, blockpool, hub-tstate, the
 * global runq, per-channel, per-parker-pool, per-hub sub/wake/idle locks, ...)
 * kept acyclic only by prose comments.  A wrong nesting is a latent deadlock.
 *
 * When built with -DRUNLOOM_LOCKRANK, each ranked lock acquisition pushes its
 * rank onto a per-thread held-stack and checks that it is STRICTLY GREATER than
 * every rank already held -- i.e. locks are always taken in increasing rank
 * order (a fixed total order => no cycle => no lock-order deadlock).  A
 * violation prints once per offending (held, acquired) pair; define
 * RUNLOOM_LOCKRANK_ABORT to turn it into an abort() instead.  The ranks below
 * encode the intended outer->inner order.
 *
 * Zero cost when not defined: RUNLOOM_RLOCK/RUNLOOM_RUNLOCK expand to the plain
 * mutex lock/unlock, and this header adds nothing to a release build. */
#ifndef RUNLOOM_LOCKRANK_H
#define RUNLOOM_LOCKRANK_H

#include "plat.h"
#include "plat_compat.h"

/* Lock ranks, OUTER (small) -> INNER (large).  A lock may be acquired only
 * while every currently-held lock has a strictly smaller rank.  Append/adjust
 * as the checker reveals the real order; keep the values spaced so insertions
 * don't renumber everything. */
typedef enum runloom_lock_rank {
    RUNLOOM_RANK_NONE         = 0,
    RUNLOOM_RANK_MN_CTRL      = 5,    /* runloom_mn_ctrl.lock (controlled-replay baton) */
    RUNLOOM_RANK_HUB          = 10,   /* per-hub h->lock (hub main state) */
    RUNLOOM_RANK_GREG         = 20,   /* runloom_greg_lock (global fiber registry) */
    RUNLOOM_RANK_HUB_TSTATE   = 30,   /* runloom_hub_tstate_lock */
    RUNLOOM_RANK_ARENA_INIT   = 35,   /* runloom_arena_init_lock */
    RUNLOOM_RANK_GLOBAL_RUNQ  = 40,   /* runloom_global_runq_lock */
    RUNLOOM_RANK_CHAN         = 50,   /* per-channel ch->lock */
    RUNLOOM_RANK_HUB_SUB      = 60,   /* per-hub h->sub_lock / s->sub_lock / tgt->sub_lock */
    RUNLOOM_RANK_HUB_IDLE     = 65,   /* per-hub h->idle_lock */
    RUNLOOM_RANK_WAKE_LIST    = 70,   /* sched s->wake_list_lock / owner->wake_list_lock */
    RUNLOOM_RANK_PARKER_POOL  = 80,   /* pool->lock / runloom_pool.lock (netpoll parkers) */
    RUNLOOM_RANK_IOURING_SUB  = 85,   /* runloom_iouring_state.sub_lock (io_uring submit) */
    RUNLOOM_RANK_IOURING_BRING= 88,   /* s->bring_lock (io_uring provided-buffer ring) */
    RUNLOOM_RANK_BLOCKPOOL    = 90,   /* bp_lock (offload blockpool) */
    RUNLOOM_RANK_RING_LIST    = 100,  /* runloom_ring_list_lock (io_uring rings) */
    RUNLOOM_RANK_GLOBAL_STACK = 110,  /* runloom_global_stack_lock (stack depot) */
    RUNLOOM_RANK_ADVICE       = 120,  /* runloom_advice_lock (stack advice) */
    RUNLOOM_RANK_CAL          = 130,  /* runloom_cal_lock (stack calibration) */
    RUNLOOM_RANK_TRACE        = 140,  /* mn_trace / gil_trace (leaf) */
    RUNLOOM_RANK__MAX         = 150
} runloom_lock_rank_t;

#ifdef RUNLOOM_LOCKRANK

#define RUNLOOM_LOCKRANK_DEPTH 32

/* Per-thread held-rank stack.  RUNLOOM_TLS is the portable TLS keyword
 * (see plat_compat.h). */
extern RUNLOOM_TLS int runloom_lockrank_held[RUNLOOM_LOCKRANK_DEPTH];
extern RUNLOOM_TLS int runloom_lockrank_depth;

void runloom_lockrank_violation(int held, int acquired);

RUNLOOM_INLINE void runloom_lockrank_push(int rank)
{
    int i;
    for (i = 0; i < runloom_lockrank_depth; i++) {
        if (runloom_lockrank_held[i] >= rank) {
            runloom_lockrank_violation(runloom_lockrank_held[i], rank);
            break;   /* report the first conflict; still push so unlock balances */
        }
    }
    if (runloom_lockrank_depth < RUNLOOM_LOCKRANK_DEPTH)
        runloom_lockrank_held[runloom_lockrank_depth++] = rank;
}

RUNLOOM_INLINE void runloom_lockrank_pop(int rank)
{
    int i;
    /* Pop the topmost matching rank (locks are released LIFO in practice, but
     * tolerate non-LIFO by searching from the top). */
    for (i = runloom_lockrank_depth - 1; i >= 0; i--) {
        if (runloom_lockrank_held[i] == rank) {
            int j;
            for (j = i; j < runloom_lockrank_depth - 1; j++)
                runloom_lockrank_held[j] = runloom_lockrank_held[j + 1];
            runloom_lockrank_depth--;
            return;
        }
    }
}

#define RUNLOOM_RLOCK(mu, rank) \
    do { runloom_lockrank_push((int)(rank)); runloom_mutex_lock(mu); } while (0)
#define RUNLOOM_RUNLOCK(mu, rank) \
    do { runloom_mutex_unlock(mu); runloom_lockrank_pop((int)(rank)); } while (0)

#else  /* !RUNLOOM_LOCKRANK -- zero cost */

#define RUNLOOM_RLOCK(mu, rank)   runloom_mutex_lock(mu)
#define RUNLOOM_RUNLOCK(mu, rank) runloom_mutex_unlock(mu)

#endif

#endif /* RUNLOOM_LOCKRANK_H */
