/*
 * greg_publish.c -- GenMC (RC11) model of the g-registry PUBLISH race between
 * spawn_common (writer) and the introspect readers (runloom_fiber_snapshot /
 * the frame lookup) in src/runloom_c/runloom_sched_core.c.inc +
 * runloom_introspect.c.  This is the race TSan-GOLD caught 2026-07-07 and the
 * introspect-gate fix closed; the model guards that fix against regression.
 *
 * THE PROTOCOL.  spawn_common initialises a fiber's fields -- g->refcount,
 * g->owner, g->noyield (sched_core.c.inc:966-968) -- and THEN publishes the g by
 * release-storing g->state = RUNNABLE (:989).  The g is registry-linked from
 * first alloc, so a foreign sampler thread walking the registry
 * (runloom_fiber_snapshot) can reach it MID-SPAWN.  The reader acquire-loads
 * g->state and, iff the g is "live enough", reads owner/noyield/refcount.  The
 * greg_lock the reader holds orders reader-vs-reader + the list walk, NOT the
 * field writes (spawn_common writes them OUTSIDE any greg lock) -- so the ONLY
 * edge that can order "fields written" before "fields read" is the state
 * release/acquire pair.  greg_lock is therefore irrelevant to this race and is
 * omitted from the model.
 *
 * PROVES (fixed gate, st >= RUNNABLE):
 *   - NO DATA RACE on owner/noyield: a reader that reads the fields must have
 *     acquire-observed state == RUNNABLE, which pairs with the writer's release
 *     store, so the field writes happen-before the reads (and are their values).
 *
 * Negative control -DBUG_SKIP_FREED_ONLY (the pre-fix code): the reader gates
 * ONLY on st != FREED, so it reads owner/noyield while the g is still in INIT/
 * SPAWNING -- before the release store of RUNNABLE.  GenMC finds the data race:
 * a plain read concurrent with spawn_common's plain write, with no ordering
 * edge.  That is exactly the bug.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define GST_INIT     0
#define GST_SPAWNING 1
#define GST_RUNNABLE 2
#define GST_FREED    11

static atomic_int g_state;      /* g->state: published with RELEASE  */
static int        g_owner;      /* g->owner   (plain field)          */
static int        g_noyield;    /* g->noyield (plain field)          */
static atomic_int g_refcount;   /* g->refcount (relaxed atomic)      */

/* spawn_common: write the fields, THEN publish via release-store RUNNABLE. */
static void *writer(void *arg)
{
    (void)arg;
    atomic_store_explicit(&g_refcount, 1, memory_order_relaxed);
    g_owner = 7;                                          /* plain field write */
    g_noyield = 1;                                        /* plain field write */
    atomic_store_explicit(&g_state, GST_RUNNABLE, memory_order_release);
    return 0;
}

/* runloom_fiber_snapshot: acquire-load state, gate, then read the fields. */
static void *reader(void *arg)
{
    (void)arg;
    unsigned int st = atomic_load_explicit(&g_state, memory_order_acquire);
#ifdef BUG_SKIP_FREED_ONLY
    if (st == GST_FREED) return 0;              /* PRE-FIX: reads INIT/SPAWNING too */
#else
    if (st < GST_RUNNABLE || st == GST_FREED) return 0;   /* FIX: skip pre-publish */
#endif
    int o = g_owner;                                     /* plain field read */
    int n = g_noyield;                                   /* plain field read */
    (void)atomic_load_explicit(&g_refcount, memory_order_relaxed);
    /* Reached only when state was observed >= RUNNABLE (published): the
     * release/acquire edge makes the field writes visible with their values. */
    assert(o == 7 && n == 1);
    return 0;
}

int main(void)
{
    pthread_t tw, tr;
    atomic_init(&g_state, GST_INIT);            /* freshly alloc'd, pre-publish */
    atomic_init(&g_refcount, 0);
    g_owner = 0;
    g_noyield = 0;
    pthread_create(&tw, 0, writer, 0);
    pthread_create(&tr, 0, reader, 0);
    pthread_join(tw, 0);
    pthread_join(tr, 0);
    return 0;
}
