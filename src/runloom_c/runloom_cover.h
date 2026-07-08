/* runloom_cover.h -- named reachability ("Sometimes()") counters.
 *
 * QA-steal rank 6 (Antithesis Sometimes()/Reachable(), Kani kani::cover).
 * pygo has extensive recovery code (steal, handoff, slab balance, cold refill)
 * but no cheap runtime proof that a fuzz/soak session actually DROVE execution
 * into those states -- a green run may be vacuous.  These counters make each
 * "interesting concurrent state" a named atom; a session FAILS if any named
 * atom has zero hits, i.e. the chaos never exercised the rescue path.  The
 * runtime analog of the model-mutation "teeth", extended to dynamic testing.
 *
 * Gated behind -DRUNLOOM_COVER (setup.py: RUNLOOM_COVER=1) so release pays
 * nothing.  The accessors are ALWAYS present (return 0 / enabled()==0 when not
 * built) so Python code never needs to guard on the build flavor.
 */
#ifndef RUNLOOM_COVER_H
#define RUNLOOM_COVER_H

/* The named interesting states.  Keep in sync with runloom_cover_name(). */
typedef enum {
    RUNLOOM_COV_STEAL_HIT = 0,        /* a hub successfully stole a g from a victim */
    RUNLOOM_COV_DEQUE_FULL_FALLBACK,  /* deque full -> spilled to the ready FIFO */
    RUNLOOM_COV_GLOBAL_RUNQ_PULL,     /* pulled a migratable woken g from the global runq */
    RUNLOOM_COV_G_SLAB_SPILL,         /* per-thread g-slab over cap -> global balance pool */
    RUNLOOM_COV_G_SLAB_REFILL,        /* empty slab refilled from the global pool */
    RUNLOOM_COV_CORO_POOL_MISS,       /* coro pool miss -> cold new stack */
    RUNLOOM_COV__COUNT
} runloom_cov_point_t;

#if defined(RUNLOOM_COVER)
void runloom_cover_bump(runloom_cov_point_t pt);
#  define RUNLOOM_COVER_HIT(pt) runloom_cover_bump(pt)
#else
#  define RUNLOOM_COVER_HIT(pt) ((void)0)
#endif

/* Always present (0 / empty when not built with -DRUNLOOM_COVER). */
unsigned long runloom_cover_get(int pt);   /* hit count for point pt */
void          runloom_cover_reset(void);    /* zero all counts (per session) */
const char   *runloom_cover_name(int pt);   /* stable name, or NULL if oob */
int           runloom_cover_num(void);      /* RUNLOOM_COV__COUNT */
int           runloom_cover_enabled(void);  /* 1 iff built with -DRUNLOOM_COVER */

#endif /* RUNLOOM_COVER_H */
