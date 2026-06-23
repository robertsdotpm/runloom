/*
 * chase_lev2.c -- GenMC oracle, TWO-element configuration, to locate where the
 * Chase-Lev SC FENCE is actually necessary.
 *
 * chase_lev.c (single element) showed the SC fence is REDUNDANT for exactly-once
 * when only one element is contended: the top-CAS alone arbitrates take vs steal.
 * The fence's real job shows up here, with two elements: it stops `take` from
 * reading a STALE `top` that sends it down the NO-CAS branch (t < b) to return
 * buf[b] -- a slot a thief has already stolen -- which DUPLICATES that element.
 *
 * Two elements (1 at index 0, 2 at index 1) are pushed.  Then the owner runs
 * take() while TWO thieves run steal().  No element may be returned twice.
 *
 * SPEC: for every value v, got[v] <= 1   (NO DUPLICATION).
 *
 * Correct (with SC fences): No errors.
 * -DBUG_NO_FENCE: take's bot-store and top-load are no longer SC-ordered with
 *   the thieves; take reads a stale top=0 < b=1, returns buf[1]=2 in the no-CAS
 *   branch while a thief also steals index 1 -> got[2]==2 -> VIOLATION.  This
 *   pins the SC fence's necessity to the >=2-element no-CAS branch.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define CAP   4
#define EMPTY (-1)

static atomic_int top;
static atomic_int bot;
static int        buf[CAP];
static atomic_int got[CAP + 2];

#ifdef BUG_NO_FENCE
#  define SC_FENCE() ((void)0)
#else
#  define SC_FENCE() atomic_thread_fence(memory_order_seq_cst)
#endif

static void push(int x)
{
    int b = atomic_load_explicit(&bot, memory_order_relaxed);
    buf[b] = x;
    atomic_store_explicit(&bot, b + 1, memory_order_release);
}

static int steal(void)
{
    int t = atomic_load_explicit(&top, memory_order_relaxed);
    SC_FENCE();
    int b = atomic_load_explicit(&bot, memory_order_acquire);
    if (b - t <= 0) return EMPTY;
    int x = buf[t];
    if (!atomic_compare_exchange_strong_explicit(
            &top, &t, t + 1, memory_order_relaxed, memory_order_relaxed))
        return EMPTY;
    return x;
}

static int take(void)
{
    int b = atomic_load_explicit(&bot, memory_order_relaxed) - 1;
    atomic_store_explicit(&bot, b, memory_order_relaxed);
    SC_FENCE();
    int t = atomic_load_explicit(&top, memory_order_acquire);
    int x = EMPTY;
    if (t <= b) {
        x = buf[b];
        if (t == b) {
            if (!atomic_compare_exchange_strong_explicit(
                    &top, &t, t + 1, memory_order_relaxed, memory_order_relaxed))
                x = EMPTY;
            atomic_store_explicit(&bot, b + 1, memory_order_relaxed);
        }
    } else {
        atomic_store_explicit(&bot, b + 1, memory_order_relaxed);
    }
    return x;
}

static void record(int v) { if (v >= 1) atomic_fetch_add(&got[v], 1); }
static void *thief(void *u) { (void)u; record(steal()); return 0; }

int main(void)
{
    atomic_init(&top, 0);
    atomic_init(&bot, 0);
    for (int i = 0; i < CAP + 2; i++) atomic_init(&got[i], 0);

    push(1);
    push(2);

    pthread_t a, b;
    pthread_create(&a, 0, thief, 0);
    pthread_create(&b, 0, thief, 0);

    record(take());

    pthread_join(a, 0);
    pthread_join(b, 0);

    /* NO DUPLICATION: no element returned by two consumers */
    assert(atomic_load(&got[1]) <= 1);
    assert(atomic_load(&got[2]) <= 1);
    return 0;
}
