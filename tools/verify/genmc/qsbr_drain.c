/*
 * qsbr_drain.c -- GenMC model of free-threaded CPython's QSBR grace-period reclaim
 * under a runloom per-g-tstate hub->hub MIGRATION, in REAL C (pthreads + C11
 * atomics) under RC11.  (LIFECYCLE_INVARIANTS.md, deep surface "QSBR seq
 * (_Py_qsbr)": "separates premature-reclaim from page mis-ownership ... GenMC
 * (real seq_cst / CAS orderings)".)  Companion to mimalloc_page_free.c: that
 * proves WHO may touch a page; this proves WHEN a deferred free may run.
 *
 * GROUND TRUTH (Python/qsbr.c, pycore_qsbr.h).  Free-threaded CPython reclaims
 * QSBR-deferred memory only after a GRACE PERIOD: a free deferred at write-sequence
 * `goal` may run only once EVERY participating thread has passed a quiescent state
 * at or beyond `goal` (_Py_qsbr_poll reads the minimum of all threads' seq).  A
 * thread publishes progress by storing its observed sequence into its qsbr slot at
 * a quiescent point (_Py_qsbr_quiescent_state).  A per-g tstate carries its OWN
 * qsbr thread-state and deferred-free queue (mem_free_queue); runloom migrates that
 * tstate hub A -> hub B.  CPython's real thread detach advances the leaving thread
 * OFFLINE (so it no longer holds back the grace period) AND flushes its deferred
 * queue to the interpreter (_PyMem_AbandonDelayed).  Skipping that, the migrated
 * reclaimer can poll a grace period that ignores a reader still live on hub A and
 * free an object out from under it -> premature reclaim / use-after-free.
 *
 * MODEL.  One QSBR-deferred object X (plain payload x_payload).  A READER on hub A
 * is inside a read-side critical section referencing X and has NOT yet reached a
 * quiescent state past the free's goal; a RECLAIMER (the migrated tstate processing
 * its deferred-free queue) wants to free X.
 *   CORRECT: the reclaimer POLLS the grace period -- it waits until hub A's reader
 *   has published seq >= GOAL (left its critical section).  The acquire load that
 *   observes that synchronizes-with the reader's release, ordering the reader's use
 *   of X strictly before the reclaim.
 *
 * PROVES (RC11, all executions): NO DATA RACE / no use-after-free of x_payload --
 * the deferred free is ordered after every reader's quiescent state; freed once.
 *
 * Negative controls (must FAIL = GenMC reports the race / UAF):
 *   -DBUG_NO_GRACE      : the migrated reclaimer skips the poll entirely (the
 *                         undrained-migration bug) -> frees X while hub A's reader
 *                         still uses it -> premature-reclaim use-after-free.
 *   -DBUG_POLL_RELAXED  : the reclaimer polls but reads the reader's seq RELAXED, so
 *                         observing seq >= GOAL does NOT synchronize-with the reader's
 *                         quiescent-state publish -> the use / free stay unordered ->
 *                         race.  (Proves the ACQUIRE on the poll is load-bearing.)
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define GOAL 1   /* the free was deferred at write-seq 1; safe once all readers' seq >= 1 */

#ifdef BUG_POLL_RELAXED
#  define POLL_ORDER memory_order_relaxed
#else
#  define POLL_ORDER memory_order_acquire
#endif

static int        x_payload;   /* X's bytes: USED by the reader, clobbered by the free */
static atomic_int a_seq;       /* hub A reader's qsbr seq (quiescent-state counter) */
static atomic_int x_freed;     /* exactly-once witness */

/* Hub A: a reader holding a read-side reference to the deferred object X.  It uses X
 * inside its critical section, then reaches a quiescent state (publishes seq=GOAL). */
static void *reader(void *arg)
{
    (void)arg;
    int v = x_payload;          /* read-side USE of X (races a premature free) */
    (void)v;
    /* leave the critical section -> quiescent state: publish progress past the goal */
    atomic_store_explicit(&a_seq, GOAL, memory_order_release);
    return 0;
}

/* The migrated tstate processing its QSBR deferred-free queue on hub B. */
static void *reclaimer(void *arg)
{
    (void)arg;
#ifndef BUG_NO_GRACE
    /* _Py_qsbr_poll: wait until every participating thread (here, hub A's reader) has
     * passed a quiescent state at or beyond the free's goal. */
    while (atomic_load_explicit(&a_seq, POLL_ORDER) < GOAL) { }
#endif
    int n = atomic_fetch_add_explicit(&x_freed, 1, memory_order_relaxed);
    assert(n == 0);             /* FREE-ONCE */
    x_payload = -1;             /* reclaim: the free clobbers X's bytes */
    return 0;
}

int main(void)
{
    pthread_t a, b;
    x_payload = 42;
    atomic_init(&a_seq, 0);     /* reader starts inside its critical section (seq < GOAL) */
    atomic_init(&x_freed, 0);

    pthread_create(&a, 0, reader, 0);
    pthread_create(&b, 0, reclaimer, 0);
    pthread_join(a, 0);
    pthread_join(b, 0);
    return 0;
}
