/* cldeque.c -- Chase-Lev work-stealing deque.
 *
 * Atomic ops via GCC/Clang __atomic_* builtins.  On MSVC the same names
 * are macro-shimmed onto _Interlocked* via plat_atomic.h (included
 * through plat_compat.h), so the body below is portable as-is.  On
 * MinGW-w64 / Clang-on-Windows the real GCC builtins are used.
 */
#include "plat_compat.h"
#include "cldeque.h"

/* Ghost instrumentation for the verification monitor (verify/cbmc,
 * verify/genmc).  ZERO cost in production: without PYGO_CLDEQUE_VERIFY the
 * hooks expand to nothing and the emitted code is byte-identical.  Under
 * PYGO_CLDEQUE_VERIFY the harness supplies pygo_cl_* to check INV_race:
 * segment-disjointness at pop's fenced top-read + TAKEN-once per index. */
#ifndef PYGO_CLDEQUE_VERIFY
#  define PYGO_CL_PUSH(i)          ((void)0)
#  define PYGO_CL_POP_FENCED(t, b) ((void)0)
#  define PYGO_CL_CLAIM(i, who)    ((void)0)
#else
void pygo_cl_push(long i);
void pygo_cl_pop_fenced(long t, long b);
void pygo_cl_claim(long i, int who);   /* who: 0 = owner pop, 1 = thief steal */
#  define PYGO_CL_PUSH(i)          pygo_cl_push(i)
#  define PYGO_CL_POP_FENCED(t, b) pygo_cl_pop_fenced((t), (b))
#  define PYGO_CL_CLAIM(i, who)    pygo_cl_claim((i), (who))
#endif

void pygo_cldeque_init(pygo_cldeque_t *d)
{
    __atomic_store_n(&d->top, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&d->bottom, 0, __ATOMIC_RELAXED);
}

int pygo_cldeque_push(pygo_cldeque_t *d, void *item)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED);
    long t = __atomic_load_n(&d->top, __ATOMIC_ACQUIRE);
    if (b - t >= PYGO_CLDEQUE_CAP) return -1;
    d->buf[b & PYGO_CLDEQUE_MASK] = item;
    __atomic_store_n(&d->bottom, b + 1, __ATOMIC_RELEASE);
    PYGO_CL_PUSH(b);                       /* ghost: owner owns index b */
    return 0;
}

void *pygo_cldeque_pop(pygo_cldeque_t *d)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED) - 1;
    long t;
    void *item;

    __atomic_store_n(&d->bottom, b, __ATOMIC_SEQ_CST);
    t = __atomic_load_n(&d->top, __ATOMIC_SEQ_CST);
    PYGO_CL_POP_FENCED(t, b);             /* ghost: INV_race -- owner owns [t,b] */
    if (t > b) {
        /* Deque empty.  Reset bottom = top to keep indices well-formed. */
        __atomic_store_n(&d->bottom, t, __ATOMIC_RELAXED);
        return NULL;
    }
    item = d->buf[b & PYGO_CLDEQUE_MASK];
    if (t < b) {
        /* No contention possible; pop succeeded. */
        PYGO_CL_CLAIM(b, 0);             /* ghost: owner takes index b */
        return item;
    }
    /* Last element: race with thieves.  CAS top from t to t+1. */
    {
        long expected = t;
        if (__atomic_compare_exchange_n(&d->top, &expected, t + 1,
                                        0, __ATOMIC_SEQ_CST,
                                        __ATOMIC_RELAXED)) {
            PYGO_CL_CLAIM(t, 0);         /* ghost: owner takes last index t */
            __atomic_store_n(&d->bottom, t + 1, __ATOMIC_RELAXED);
            return item;
        }
        /* Lost the race; thief got it.  Restore bottom and return empty. */
        __atomic_store_n(&d->bottom, t + 1, __ATOMIC_RELAXED);
        return NULL;
    }
}

void *pygo_cldeque_steal(pygo_cldeque_t *d)
{
    long t = __atomic_load_n(&d->top, __ATOMIC_ACQUIRE);
    long b;
    void *item;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    b = __atomic_load_n(&d->bottom, __ATOMIC_ACQUIRE);
    if (t >= b) return NULL;
    item = d->buf[t & PYGO_CLDEQUE_MASK];
    {
        long expected = t;
        if (__atomic_compare_exchange_n(&d->top, &expected, t + 1,
                                        0, __ATOMIC_SEQ_CST,
                                        __ATOMIC_RELAXED)) {
            PYGO_CL_CLAIM(t, 1);         /* ghost: thief takes index t */
            return item;
        }
    }
    return NULL;
}

long pygo_cldeque_size(const pygo_cldeque_t *d)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED);
    long t = __atomic_load_n(&d->top, __ATOMIC_RELAXED);
    return b - t;
}
