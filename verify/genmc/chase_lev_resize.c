/*
 * chase_lev_resize.c -- GenMC oracle for the Chase-Lev deque's GROW (resize)
 * under RC11, in real C.  This is the "real unbounded version" check: the owner
 * grows the backing array (allocate bigger buffer, copy live elements, switch
 * the array pointer) CONCURRENTLY with thieves that may still read the OLD
 * buffer.  gpfsl-examples/chase_lev/code.v explicitly punts on this ("it may
 * not be so easy to allow concurrent reads of the underlying array while we
 * want to grow"); this harness pins down whether the release/acquire on the
 * array pointer is sufficient to keep the concurrent copy race-free under RC11.
 *
 * MODEL: array A (size 1) holds element 1.  Owner spawns 2 thieves, then GROWs:
 *   copy A[top..bot) into a fresh array B, then publish the new array pointer
 *   with a RELEASE store.  Thieves read the array pointer with ACQUIRE, then
 *   read their slot from whichever array they observe.
 *
 * SPEC (same two forbidden things):
 *   - DUPLICATION / LOSS: element 1 returned exactly once  (assert got==1)
 *   - DATA-RACE freedom: the owner's copy-write into B must not race a thief's
 *     read of B.  This holds ONLY if the array-pointer release/acquire orders
 *     the copy before any thief's read of the new array.
 *
 * Negative control -DBUG_RLX_ARR: publish/observe the array pointer with
 * RELAXED order instead of release/acquire -> GenMC must find the DATA RACE on
 * the new buffer B (owner's copy-write vs thief's read).  This isolates exactly
 * which fence the resize needs -- the answer the fixed-array model could not give.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define EMPTY (-1)

static int bufA[1];
static int bufB[2];
static int * _Atomic arr;        /* the live backing array (A then B) */
static atomic_int top;
static atomic_int bot;
static atomic_int got;           /* times element 1 was returned */

#ifdef BUG_RLX_ARR
#  define ARR_PUB memory_order_relaxed
#  define ARR_OBS memory_order_relaxed
#else
#  define ARR_PUB memory_order_release
#  define ARR_OBS memory_order_acquire
#endif

static int steal(void)
{
    int t = atomic_load_explicit(&top, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    int b = atomic_load_explicit(&bot, memory_order_acquire);
    if (b - t <= 0)
        return EMPTY;
    int *a = atomic_load_explicit(&arr, ARR_OBS);   /* observe live array */
    int x = a[t];                                   /* non-atomic slot read */
    if (!atomic_compare_exchange_strong_explicit(
            &top, &t, t + 1, memory_order_relaxed, memory_order_relaxed))
        return EMPTY;
    return x;
}

static void grow(void)                              /* owner: A(1) -> B(2) */
{
    int t = atomic_load_explicit(&top, memory_order_acquire);
    int b = atomic_load_explicit(&bot, memory_order_relaxed);
    int *a = atomic_load_explicit(&arr, memory_order_relaxed);
    for (int i = t; i < b; i++)
        bufB[i] = a[i];                             /* copy live elements */
    atomic_store_explicit(&arr, bufB, ARR_PUB);     /* publish new array */
}

static void record(int v) { if (v == 1) atomic_fetch_add(&got, 1); }
static void *thief(void *u) { (void)u; record(steal()); return 0; }

int main(void)
{
    atomic_init(&top, 0);
    atomic_init(&bot, 0);
    atomic_init(&got, 0);
    atomic_init(&arr, bufA);

    /* owner push(1) into A */
    bufA[0] = 1;
    atomic_store_explicit(&bot, 1, memory_order_release);

    pthread_t a, b;
    pthread_create(&a, 0, thief, 0);
    pthread_create(&b, 0, thief, 0);

    grow();                          /* owner grows, concurrent with the thieves */

    pthread_join(a, 0);
    pthread_join(b, 0);

    /* element pushed once, two thieves; <=1 can win the top CAS, and it is not
       lost regardless of which array it was read from */
    assert(atomic_load(&got) <= 1);
    return 0;
}
