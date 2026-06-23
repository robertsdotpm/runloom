/*
 * mimalloc_page_free.c -- GenMC model of mimalloc's per-PAGE xthread_id ownership
 * gate under a runloom per-g-tstate hub->hub MIGRATION, in REAL C (pthreads + C11
 * atomics) under RC11.  This is the WEAK-MEMORY fidelity layer that
 * RunloomTstateMigration.tla explicitly defers to (LIFECYCLE_INVARIANTS.md,
 * deep surface "mimalloc page xthread_id / abandoned_pool"): the TLA spec proves
 * the abandon/adopt handshake PLACEMENT is necessary over an abstract owner; this
 * proves the actual DATA RACE the placement prevents -- the exact _mi_page_retire
 * corruption gated off in 70e6ddb (RUNLOOM_ALLOW_UNSAFE_MIGRATION).
 *
 * GROUND TRUTH (Objects/mimalloc free.c + page.c).  mi_free(p) reads the page's
 * xthread_id and branches: OWNER (page->xthread_id == _mi_thread_id()) takes
 * mi_free_block_local, which mutates the page AND its owning heap's page queues
 * NON-ATOMICALLY (and may call _mi_page_retire); a NON-OWNER takes
 * mi_free_block_mt, which only CAS-pushes onto the atomic page->xthread_free MPSC
 * list and touches NO heap queue.  So the per-page xthread_id is the sole gate
 * deciding whether a thread may touch the heap-queue bookkeeping non-atomically.
 * mimalloc transfers that ownership only via an explicit handshake: _mi_page_abandon
 * publishes the page to the abandoned pool (owner relinquishes, stops touching it),
 * and _mi_heap_reclaim/adopt re-stamps xthread_id to the new thread BEFORE it
 * touches the queues.  runloom migrates a per-g tstate (its whole mimalloc heap)
 * hub A -> hub B WITHOUT that handshake, so the heap's pages stay stamped+queued
 * under A while B operates them -> a local-path touch on B races A -> corruption.
 *
 * MODEL.  One page.  page_xtid (ATOMIC owner id, 0 == abandoned) gates ownership;
 * page_heapq (PLAIN heap-queue bookkeeping) may be touched only by the owner.
 *   Thread A owns the page, does an owner-side heap-queue touch, then ABANDONS it
 *     (release-store xtid = 0) and touches it no more.
 *   Thread B is the migrated operator: it must ADOPT (acquire the page once it is
 *     abandoned, then re-stamp xtid = B) BEFORE touching the heap queue.
 * The abandon (release) / adopt (acquire) pair orders A's last heap-queue touch
 * before B's first, so the two PLAIN accesses are never concurrent.
 *
 * PROVES (RC11, all executions): NO DATA RACE on page_heapq -- the owner-only
 * non-atomic heap-queue path is serialized across the ownership transfer.
 *
 * Negative controls (must FAIL = GenMC reports the data race):
 *   -DBUG_LOCAL_ON_STALE : B skips the adopt handshake and touches page_heapq while
 *                          xtid still names A (the missing-adopt migration bug) ->
 *                          B's local-path touch races A's -> _mi_page_retire
 *                          corruption precondition.
 *   -DBUG_ADOPT_RELAXED  : B performs the adopt but observes the abandon with a
 *                          RELAXED load, so it does NOT synchronize-with A's release
 *                          -> A's and B's heap-queue touches stay unordered -> race.
 *                          (Proves the ORDERING of the handshake is load-bearing,
 *                          not just its presence.)
 */
#include <pthread.h>
#include <stdatomic.h>

#define A         1
#define B         2
#define ABANDONED 0

#ifdef BUG_ADOPT_RELAXED
#  define ADOPT_LOAD_ORDER memory_order_relaxed
#else
#  define ADOPT_LOAD_ORDER memory_order_acquire
#endif

static atomic_int page_xtid;    /* page->xthread_id: owning thread id (0 = abandoned) */
static int        page_heapq;   /* owning heap's page-queue bookkeeping: PLAIN, owner-only */

/* Thread A: the original owner.  Does an owner-side heap-queue touch (a local-free /
 * _mi_page_retire), then ABANDONS the page so another thread may adopt it. */
static void *owner_detach(void *arg)
{
    (void)arg;
    page_heapq = A;             /* legit owner-side non-atomic heap-queue mutation */
    /* _mi_page_abandon: publish to the abandoned pool; touch the page no more. */
    atomic_store_explicit(&page_xtid, ABANDONED, memory_order_release);
    return 0;
}

/* Thread B: the migrated operator.  Adopts the abandoned page (acquiring A's release),
 * re-stamps ownership, THEN touches the heap queue as the legitimate sole owner. */
static void *adopt_operate(void *arg)
{
    (void)arg;
#ifndef BUG_LOCAL_ON_STALE
    /* _mi_heap_reclaim/adopt: wait until the page is abandoned, then claim it.  The
     * acquire load that observes ABANDONED synchronizes-with A's release, ordering
     * A's last heap-queue touch before ours. */
    while (atomic_load_explicit(&page_xtid, ADOPT_LOAD_ORDER) != ABANDONED) { }
    atomic_store_explicit(&page_xtid, B, memory_order_release);
#endif
    page_heapq = B;             /* owner local-free path: non-atomic heap-queue mutation */
    return 0;
}

int main(void)
{
    pthread_t a, b;
    atomic_init(&page_xtid, A);   /* page starts owned by hub A */
    page_heapq = 0;

    pthread_create(&a, 0, owner_detach, 0);
    pthread_create(&b, 0, adopt_operate, 0);
    pthread_join(a, 0);
    pthread_join(b, 0);
    return 0;
}
