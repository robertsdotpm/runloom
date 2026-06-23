/* cldeque.c -- Chase-Lev work-stealing deque.
 *
 * Atomic ops via GCC/Clang __atomic_* builtins.  On MSVC the same names
 * are macro-shimmed onto _Interlocked* via plat_atomic.h (included
 * through plat_compat.h), so the body below is portable as-is.  On
 * MinGW-w64 / Clang-on-Windows the real GCC builtins are used.
 */
#include "plat_compat.h"
#include "cldeque.h"

/* Ghost instrumentation for the verification monitor (tools/verify/cbmc,
 * tools/verify/genmc).  ZERO cost in production: without RUNLOOM_CLDEQUE_VERIFY the
 * hooks expand to nothing and the emitted code is byte-identical.  Under
 * RUNLOOM_CLDEQUE_VERIFY the harness supplies runloom_cl_* to check INV_race:
 * segment-disjointness at pop's fenced top-read + TAKEN-once per index. */
#ifndef RUNLOOM_CLDEQUE_VERIFY
#  define RUNLOOM_CL_PUSH(i)          ((void)0)
#  define RUNLOOM_CL_POP_FENCED(t, b) ((void)0)
#  define RUNLOOM_CL_CLAIM(i, who)    ((void)0)
#else
void runloom_cl_push(long i);
void runloom_cl_pop_fenced(long t, long b);
void runloom_cl_claim(long i, int who);   /* who: 0 = owner pop, 1 = thief steal */
#  define RUNLOOM_CL_PUSH(i)          runloom_cl_push(i)
#  define RUNLOOM_CL_POP_FENCED(t, b) runloom_cl_pop_fenced((t), (b))
#  define RUNLOOM_CL_CLAIM(i, who)    runloom_cl_claim((i), (who))
#endif

void runloom_cldeque_init(runloom_cldeque_t *d)
{
    __atomic_store_n(&d->top, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&d->bottom, 0, __ATOMIC_RELAXED);
}

int runloom_cldeque_push(runloom_cldeque_t *d, void *item)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED);
    long t = __atomic_load_n(&d->top, __ATOMIC_ACQUIRE);
    if (b - t >= RUNLOOM_CLDEQUE_CAP) return -1;
    d->buf[b & RUNLOOM_CLDEQUE_MASK] = item;
    __atomic_store_n(&d->bottom, b + 1, __ATOMIC_RELEASE);
    RUNLOOM_CL_PUSH(b);                       /* ghost: owner owns index b */
    return 0;
}

void *runloom_cldeque_pop(runloom_cldeque_t *d)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED) - 1;
    long t;
    void *item;

    __atomic_store_n(&d->bottom, b, __ATOMIC_SEQ_CST);
    t = __atomic_load_n(&d->top, __ATOMIC_SEQ_CST);
    RUNLOOM_CL_POP_FENCED(t, b);             /* ghost: INV_race -- owner owns [t,b] */
    if (t > b) {
        /* Deque empty.  Reset bottom = top to keep indices well-formed. */
        __atomic_store_n(&d->bottom, t, __ATOMIC_RELAXED);
        return NULL;
    }
    item = d->buf[b & RUNLOOM_CLDEQUE_MASK];
    if (t < b) {
        /* No contention possible; pop succeeded. */
        RUNLOOM_CL_CLAIM(b, 0);             /* ghost: owner takes index b */
        return item;
    }
    /* Last element: race with thieves.  CAS top from t to t+1. */
    {
        long expected = t;
        if (__atomic_compare_exchange_n(&d->top, &expected, t + 1,
                                        0, __ATOMIC_SEQ_CST,
                                        __ATOMIC_RELAXED)) {
            RUNLOOM_CL_CLAIM(t, 0);         /* ghost: owner takes last index t */
            __atomic_store_n(&d->bottom, t + 1, __ATOMIC_RELAXED);
            return item;
        }
        /* Lost the race; thief got it.  Restore bottom and return empty. */
        __atomic_store_n(&d->bottom, t + 1, __ATOMIC_RELAXED);
        return NULL;
    }
}

void *runloom_cldeque_steal(runloom_cldeque_t *d)
{
    long t = __atomic_load_n(&d->top, __ATOMIC_ACQUIRE);
    long b;
    void *item;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    b = __atomic_load_n(&d->bottom, __ATOMIC_ACQUIRE);
    if (t >= b) return NULL;
    item = d->buf[t & RUNLOOM_CLDEQUE_MASK];
    {
        long expected = t;
        if (__atomic_compare_exchange_n(&d->top, &expected, t + 1,
                                        0, __ATOMIC_SEQ_CST,
                                        __ATOMIC_RELAXED)) {
            RUNLOOM_CL_CLAIM(t, 1);         /* ghost: thief takes index t */
            return item;
        }
    }
    return NULL;
}

long runloom_cldeque_size(const runloom_cldeque_t *d)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED);
    long t = __atomic_load_n(&d->top, __ATOMIC_RELAXED);
    return b - t;
}
