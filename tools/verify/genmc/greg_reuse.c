/*
 * greg_reuse.c -- GenMC (RC11) model of the SLAB-REUSE continuation of the
 * g-registry publish race (TSan-GOLD, 2026-07-08).  Companion to greg_publish.c,
 * which models a single incarnation (state starts INIT) and proves the
 * st>=RUNNABLE gate closes the FRESH-spawn read-before-publish case.
 *
 * WHY greg_publish.c is not enough.  A completed g's STRUCT is never unlinked
 * from the fiber registry -- it is retained in the per-thread slab and reused
 * (runloom_sched_core.c.inc: slab_free :743 stores FREED, slab_alloc reuse :708
 * stores INIT, spawn_common re-publishes RUNNABLE :989).  So across lives the
 * g's `state` byte has a modification order that ALREADY contains a RUNNABLE
 * (and RUNNING/PARKED/DONE) from the PRIOR incarnation.  A registry-walking
 * sampler (runloom_fiber_snapshot) whose acquire-load has not been forced past
 * the later FREED/INIT/new-RUNNABLE stores can legally observe that STALE
 * prior-life RUNNABLE -- an earlier value in the same location's modification
 * order -- pass the st>=RUNNABLE gate, and read owner/noyield/refcount while
 * spawn_common concurrently rewrites them for the NEW incarnation.
 *
 * KEY NEGATIVE RESULT (modeled here): resetting state to a pre-publish value at
 * free/reuse does NOT close this.  The spawner path below performs BOTH resets
 * (FREED then INIT) exactly as the shipping code does, and the plain-field
 * control (-DBUG_PLAIN_FIELDS) STILL races -- because coherence never forces the
 * reader past the resets; with no happens-before it can read the initial
 * (prior-life) RUNNABLE regardless.
 *
 * THE FIX (default build): publish owner/noyield/refcount with RELAXED atomic
 * stores, matched by the reader's RELAXED atomic loads.  Two atomics to the same
 * location are never a data race under RC11 (the fields are display-only, so a
 * cross-incarnation value is benign) -- GenMC finds no race.
 *
 * PROVES (fixed, atomic fields):  NO DATA RACE on owner/noyield/refcount even
 *   when the reader observes a stale prior-life RUNNABLE.
 * Negative control -DBUG_PLAIN_FIELDS (plain fields, both sides, WITH the two
 *   resets): GenMC finds the non-atomic race -- exactly the TSan-GOLD report.
 */
#include <pthread.h>
#include <stdatomic.h>

#define GST_INIT     0
#define GST_RUNNABLE 2
#define GST_FREED    11

static atomic_int g_state;

#ifdef BUG_PLAIN_FIELDS
static int        g_owner;      /* plain -- the pre-fix code */
static int        g_noyield;
static int        g_refcount;
#else
static atomic_int g_owner;      /* RELAXED atomic -- the fix */
static atomic_int g_noyield;
static atomic_int g_refcount;
#endif

/* The recycling hub: free (reset FREED), reuse (reset INIT), write the new
 * incarnation's fields, then publish via release-store RUNNABLE.  All on one
 * thread, matching runloom_g_slab_free -> runloom_g_slab_alloc -> spawn_common. */
static void *spawner(void *arg)
{
    (void)arg;
    atomic_store_explicit(&g_state, GST_FREED, memory_order_release);  /* :743 */
    atomic_store_explicit(&g_state, GST_INIT,  memory_order_release);  /* :708 */
#ifdef BUG_PLAIN_FIELDS
    g_refcount = 1;                                        /* :966 plain field write */
    g_owner    = 7;                                        /* :967 plain field write */
    g_noyield  = 1;                                        /* :968 plain field write */
#else
    atomic_store_explicit(&g_refcount, 1, memory_order_relaxed);   /* :966 fix */
    atomic_store_explicit(&g_owner,    7, memory_order_relaxed);   /* :967 fix */
    atomic_store_explicit(&g_noyield,  1, memory_order_relaxed);   /* :968 fix */
#endif
    atomic_store_explicit(&g_state, GST_RUNNABLE, memory_order_release); /* :989 publish */
    return 0;
}

/* runloom_fiber_snapshot: acquire-load state, gate, read the fields. */
static void *reader(void *arg)
{
    (void)arg;
    unsigned int st = atomic_load_explicit(&g_state, memory_order_acquire);
    if (st < GST_RUNNABLE || st == GST_FREED) return 0;   /* the 9b21ec18 gate */
#ifdef BUG_PLAIN_FIELDS
    int o = g_owner;                                       /* plain read -- RACE */
    int n = g_noyield;                                     /* plain read -- RACE */
    int r = g_refcount;                                    /* plain read -- RACE */
#else
    int o = atomic_load_explicit(&g_owner,    memory_order_relaxed);
    int n = atomic_load_explicit(&g_noyield,  memory_order_relaxed);
    int r = atomic_load_explicit(&g_refcount, memory_order_relaxed);
#endif
    (void)o; (void)n; (void)r;
    return 0;
}

int main(void)
{
    pthread_t tw, tr;
    /* Retained g: state carries a STALE RUNNABLE from the prior incarnation. */
    atomic_init(&g_state, GST_RUNNABLE);
#ifdef BUG_PLAIN_FIELDS
    g_owner = 0; g_noyield = 0; g_refcount = 0;
#else
    atomic_init(&g_owner, 0); atomic_init(&g_noyield, 0); atomic_init(&g_refcount, 0);
#endif
    pthread_create(&tw, 0, spawner, 0);
    pthread_create(&tr, 0, reader, 0);
    pthread_join(tw, 0);
    pthread_join(tr, 0);
    return 0;
}
