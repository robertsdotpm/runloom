/*
 * chase_lev.c -- GenMC oracle for the Chase-Lev work-stealing deque, in REAL C
 * (pthreads + C11 atomics) under GenMC's RC11 weak-memory model.
 *
 * This is the de-risking oracle for the iRC11 proof effort: before proving the
 * deque correct, MODEL-CHECK that the modeled algorithm (exact memory orders,
 * 2 thieves, the owner's take) actually IS correct under RC11.  If GenMC finds a
 * counterexample, the model is wrong and any proof would be wasted.
 *
 * THE HARD CASE -- the take/steal single-element race.  One element (value 1) is
 * pushed.  Then the owner runs take() concurrently with TWO thieves running
 * steal().  In a correct deque EXACTLY ONE of the three returns the element; the
 * others return EMPTY/ABORT.  This is precisely the race whose linearization
 * point is future-dependent (take's LP can fall inside a thief's CAS).
 *
 * SPEC -- forbids the two things that matter:
 *   (a) DUPLICATION: got[1] <= 1  (no element returned by two consumers)
 *   (b) LOSS:        got[1] >= 1  (the pushed element is returned by someone)
 *   => assert got[1] == 1 in EVERY RC11 execution.
 * Plus GenMC's built-in DATA-RACE freedom on the non-atomic buffer slot.
 *
 * MEMORY ORDERS pinned to the real Chase-Lev / gpfsl-examples/chase_lev/code.v:
 *   push:  bot relaxed-load, buf non-atomic write, bot RELEASE-store
 *   steal: top relaxed-load, SC-FENCE, bot ACQUIRE-load, buf non-atomic read,
 *          top RELAXED CAS
 *   take:  bot relaxed-load, bot relaxed-store(b-1), SC-FENCE, top ACQUIRE-load,
 *          buf non-atomic read, top RELAXED CAS (last-elt), bot relaxed-store
 * NO order is strengthened to SC except the two explicit SC FENCES the
 * algorithm itself uses.
 *
 * Negative controls (must FAIL = find the bug):
 *   -DBUG_NO_FENCE : drop both SC fences -> take and a thief can both win
 *                    (DUPLICATION) -- this is the fence whose necessity is the
 *                    primary output of the proof.
 *   -DBUG_NO_CAS   : take grabs the last element WITHOUT the CAS -> duplication.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define CAP   4
#define EMPTY (-1)

static atomic_int top;
static atomic_int bot;
static int        buf[CAP];        /* non-atomic: owner writes, consumers read */
static atomic_int got[CAP + 2];    /* got[v] = #consumers that returned value v */

#ifdef BUG_NO_FENCE
#  define SC_FENCE() ((void)0)
#else
#  define SC_FENCE() atomic_thread_fence(memory_order_seq_cst)
#endif

/* ---- owner: push (cl_push) ---- */
static void push(int x)
{
    int b = atomic_load_explicit(&bot, memory_order_relaxed);
    buf[b] = x;                                                   /* non-atomic */
    atomic_store_explicit(&bot, b + 1, memory_order_release);     /* publish */
}

/* ---- thief: steal (cl_steal) ---- */
static int steal(void)
{
    int t = atomic_load_explicit(&top, memory_order_relaxed);
    SC_FENCE();
    int b = atomic_load_explicit(&bot, memory_order_acquire);
    if (b - t <= 0)
        return EMPTY;                                /* empty / lost the bottom */
    int x = buf[t];                                  /* non-atomic speculative read */
    if (!atomic_compare_exchange_strong_explicit(
            &top, &t, t + 1, memory_order_relaxed, memory_order_relaxed))
        return EMPTY;                                /* lost the CAS race */
    return x;
}

/* ---- owner: take / try_pop (cl_try_pop, last-element branch) ---- */
static int take(void)
{
    int b = atomic_load_explicit(&bot, memory_order_relaxed) - 1;
    atomic_store_explicit(&bot, b, memory_order_relaxed);
    SC_FENCE();
    int t = atomic_load_explicit(&top, memory_order_acquire);
    int x = EMPTY;
    if (t <= b) {
        x = buf[b];                                  /* non-atomic */
        if (t == b) {
            /* last element: race the thieves for it */
#ifndef BUG_NO_CAS
            if (!atomic_compare_exchange_strong_explicit(
                    &top, &t, t + 1, memory_order_relaxed, memory_order_relaxed))
                x = EMPTY;                            /* a thief won */
#endif
            atomic_store_explicit(&bot, b + 1, memory_order_relaxed);
        }
    } else {
        atomic_store_explicit(&bot, b + 1, memory_order_relaxed); /* empty */
    }
    return x;
}

static void record(int v) { if (v >= 1) atomic_fetch_add(&got[v], 1); }

static void *thief(void *arg) { (void)arg; record(steal()); return 0; }

int main(void)
{
    atomic_init(&top, 0);
    atomic_init(&bot, 0);
    for (int i = 0; i < CAP + 2; i++) atomic_init(&got[i], 0);

    push(1);                       /* owner pushes one element, value 1 */

    pthread_t a, b;
    pthread_create(&a, 0, thief, 0);
    pthread_create(&b, 0, thief, 0);

    record(take());                /* owner takes, racing the two thieves */

    pthread_join(a, 0);
    pthread_join(b, 0);

    /* SPEC: the one pushed element is returned exactly once -- no loss, no dup */
    assert(atomic_load(&got[1]) == 1);
    return 0;
}
