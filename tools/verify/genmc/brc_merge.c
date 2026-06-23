/*
 * brc_merge.c -- GenMC model of free-threaded CPython's BIASED REFERENCE COUNTING
 * cross-thread merge under a runloom per-g-tstate hub->hub MIGRATION, in REAL C
 * (pthreads + C11 atomics) under RC11.  (LIFECYCLE_INVARIANTS.md, deep surface
 * "biased refcount (brc)": "unlocks #8 + #2's drain half".)  Third of the
 * migration-drain trio with mimalloc_page_free.c (who may touch a page) and
 * qsbr_drain.c (when a deferred free may run); this is who may merge a refcount.
 *
 * GROUND TRUTH (Objects/object.c, pycore_object.h, brc).  Each object splits its
 * refcount into ob_ref_local (NON-ATOMIC, mutated only by the OWNER thread ob_tid)
 * and ob_ref_shared (atomic, used by every other thread).  A non-owner DECREF hits
 * ob_ref_shared atomically and, when that signals the object may be dead, QUEUES the
 * object onto the OWNER thread's brc.local_objects_to_merge so the owner later runs
 * _Py_MergeZeroLocalRefcount -- folding ob_ref_local into ob_ref_shared and freeing
 * iff the total is zero.  That merge reads ob_ref_local NON-ATOMICALLY, so it MUST
 * run on the owning thread.  A per-g tstate carries its own brc.{tid,
 * local_objects_to_merge}; runloom migrates it hub A -> hub B.  CPython's real
 * detach DRAINS brc on the leaving (owner) thread first (_Py_brc_merge); skipping
 * that, the migrated thread B runs the owner's pending merge -- reading ob_ref_local
 * while hub A may still touch it -> a non-atomic race -> phantom-zero free / UAF.
 *
 * MODEL.  One object X owned by hub A (ob_ref_local, owner-only non-atomic) with one
 * outstanding shared ref held on hub B (ob_ref_shared).  Hub A finishes its work and
 * drops its biased local ref; hub B does the cross-thread DECREF of the shared ref
 * and queues a merge.
 *   CORRECT: hub A DRAINS brc before migrating -- it runs the queued merge ITSELF
 *   (same thread that owns ob_ref_local), so ob_ref_local is never touched cross-
 *   thread; the merge sees both refs dropped and frees X exactly once.
 *
 * PROVES (RC11, all executions): NO DATA RACE on ob_ref_local, and X freed exactly
 * once (no phantom-zero double free, no lost merge).
 *
 * Negative control (must FAIL = GenMC reports the race):
 *   -DBUG_MERGE_AFTER_MIGRATE : the tstate migrated to hub B undrained, so B runs the
 *                               owner's merge -- reading ob_ref_local NON-ATOMICALLY
 *                               while hub A concurrently drops its biased local ref
 *                               -> the cross-thread non-atomic read/write race that
 *                               corrupts the merged refcount.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

static int        ob_ref_local;    /* biased local refcount: OWNER-only, NON-ATOMIC */
static atomic_int ob_ref_shared;   /* shared refcount: atomic (non-owners + the merge) */
static atomic_int merge_queued;    /* X is on the owner's local_objects_to_merge list */
static atomic_int ob_freed;        /* exactly-once witness */

/* _Py_MergeZeroLocalRefcount: fold the biased local count into the shared count and
 * free iff the object is truly dead.  READS ob_ref_local NON-ATOMICALLY -> only legal
 * on the owning thread. */
static void brc_merge(void)
{
    int local  = ob_ref_local;     /* NON-ATOMIC read of the biased local count */
    int shared = atomic_load_explicit(&ob_ref_shared, memory_order_acquire);
    if (local + shared == 0) {
        int n = atomic_fetch_add_explicit(&ob_freed, 1, memory_order_relaxed);
        assert(n == 0);            /* FREE-ONCE: no phantom-zero double free */
    }
}

/* Hub A: the owner.  Finishes its work (drops its biased local ref) and, at detach,
 * DRAINS its brc queue -- running any pending merge on the owning thread. */
static void *owner_drain(void *arg)
{
    (void)arg;
    ob_ref_local -= 1;             /* owner drops its biased local ref (non-atomic) */
#ifndef BUG_MERGE_AFTER_MIGRATE
    /* drain-on-detach: wait for the cross-thread decref's queue entry, then merge HERE
     * (on the owner) -- ob_ref_local is only ever touched by this thread. */
    while (!atomic_load_explicit(&merge_queued, memory_order_acquire)) { }
    brc_merge();
#endif
    return 0;
}

/* Hub B: a non-owner doing the last cross-thread DECREF, which queues the owner merge. */
static void *cross_decref(void *arg)
{
    (void)arg;
    atomic_fetch_sub_explicit(&ob_ref_shared, 1, memory_order_acq_rel);   /* drop shared ref */
    atomic_store_explicit(&merge_queued, 1, memory_order_release);        /* queue the merge */
#ifdef BUG_MERGE_AFTER_MIGRATE
    /* BUG: the tstate migrated to hub B undrained, so B runs the owner's merge --
     * reading ob_ref_local while hub A may still be dropping its biased ref -> race. */
    brc_merge();
#endif
    return 0;
}

int main(void)
{
    pthread_t a, b;
    ob_ref_local = 1;             /* owner hub A holds one biased local ref */
    atomic_init(&ob_ref_shared, 1);   /* hub B holds one shared ref */
    atomic_init(&merge_queued, 0);
    atomic_init(&ob_freed, 0);

    pthread_create(&a, 0, owner_drain, 0);
    pthread_create(&b, 0, cross_decref, 0);
    pthread_join(a, 0);
    pthread_join(b, 0);

    assert(atomic_load_explicit(&ob_freed, memory_order_relaxed) == 1);   /* X freed once */
    return 0;
}
