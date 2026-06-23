/*
 * chan_refcount.c -- GenMC model of the runloom_chan_t refcount free protocol
 * (runloom_chan_incref / runloom_chan_decref in chan_waiters.c.inc), in REAL C
 * (pthreads + C11 atomics) under RC11.  (LIFECYCLE_INVARIANTS.md Tier-2 #10.)
 *
 * THE PROTOCOL.  A channel is shared across hubs (wrapper object + per-waiter park
 * pins + select N-pins).  incref is RELAXED -- always done while the caller ALREADY
 * holds a ref, so the object cannot be freed under it.  decref is ACQ_REL: the
 * release half publishes this holder's accesses; the acquire half, on the decrement
 * that reaches 0, makes the freeing thread observe EVERY other holder's accesses
 * before it runs runloom_mutex_destroy + PyMem_Free.  So the free is ordered after
 * the last use, on every RC11 execution -- no use-after-free, freed exactly once.
 *
 * (incref is RELAXED-safe by a separate, simpler argument: the caller holds a ref
 * across it, so the refcount cannot reach 0 during the increment -- not what GenMC
 * needs to explore.  The load-bearing weak-memory question is the decref-to-0 / free
 * ordering, which two concurrent holders exercise.)
 *
 * PROVES (two concurrent holders, each uses the channel then drops its ref):
 *   NO UAF       -- no thread reads a channel field after the channel is freed (the
 *                   plain-field access vs the free is ordered by the acq_rel decref;
 *                   GenMC reports a data race if it is not).
 *   FREE-ONCE    -- exactly one decref drives the refcount to 0 and frees.
 *
 * Negative control -DBUG_DECREF_RELAXED: decref uses memory_order_relaxed, so the
 * decrement-to-0 does NOT acquire the other holders' releases -> the free races a
 * concurrent field access (GenMC finds the data race / UAF on the channel field).
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#ifdef BUG_DECREF_RELAXED
#  define DECREF_ORDER memory_order_relaxed
#else
#  define DECREF_ORDER memory_order_acq_rel
#endif

static atomic_int refcount;       /* ch->refcount */
static int        ch_field;       /* a plain channel field (cap/buf): live until freed */
static atomic_int freed_marker;   /* exactly-once witness (not used for ordering) */

/* runloom_chan_decref: ACQ_REL; the thread that reaches 0 frees (touches ch_field). */
static void chan_decref(void)
{
    int now = atomic_fetch_sub_explicit(&refcount, 1, DECREF_ORDER) - 1;  /* sub_fetch */
    if (now > 0) return;
    /* last ref: free.  The acquire half must order this after every holder's use. */
    int n = atomic_fetch_add_explicit(&freed_marker, 1, memory_order_relaxed);
    assert(n == 0);               /* FREE-ONCE */
    ch_field = -1;                /* runloom_mutex_destroy + PyMem_Free: the channel is gone */
}

/* A holder uses the channel (reads a field while holding its ref) then drops it. */
static void *holder(void *_)
{
    (void)_;
    assert(ch_field >= 0);        /* NO UAF: usable while we hold a ref */
    chan_decref();                /* drop our ref */
    return 0;
}

int main(void)
{
    pthread_t ta, tb;
    atomic_init(&refcount, 2);    /* two holders start with a ref each */
    atomic_init(&freed_marker, 0);
    ch_field = 8;

    pthread_create(&ta, 0, holder, 0);
    pthread_create(&tb, 0, holder, 0);
    pthread_join(ta, 0);
    pthread_join(tb, 0);

    assert(atomic_load_explicit(&refcount, memory_order_relaxed) == 0);  /* all dropped */
    return 0;
}
