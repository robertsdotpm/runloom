/* cldeque.c -- Chase-Lev work-stealing deque.
 *
 * Atomic ops via GCC/Clang __atomic_* builtins; MSVC has _Interlocked*
 * which is sufficient but the syntax differs.  We stay in the GCC
 * intrinsic family for now (works on MinGW too).
 */
#include "cldeque.h"

/* On Linux with -std=gnu99 + -D_GNU_SOURCE these atomics are fine. */

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
    return 0;
}

void *pygo_cldeque_pop(pygo_cldeque_t *d)
{
    long b = __atomic_load_n(&d->bottom, __ATOMIC_RELAXED) - 1;
    long t;
    void *item;

    __atomic_store_n(&d->bottom, b, __ATOMIC_SEQ_CST);
    t = __atomic_load_n(&d->top, __ATOMIC_SEQ_CST);
    if (t > b) {
        /* Deque empty.  Reset bottom = top to keep indices well-formed. */
        __atomic_store_n(&d->bottom, t, __ATOMIC_RELAXED);
        return NULL;
    }
    item = d->buf[b & PYGO_CLDEQUE_MASK];
    if (t < b) {
        /* No contention possible; pop succeeded. */
        return item;
    }
    /* Last element: race with thieves.  CAS top from t to t+1. */
    {
        long expected = t;
        if (__atomic_compare_exchange_n(&d->top, &expected, t + 1,
                                        0, __ATOMIC_SEQ_CST,
                                        __ATOMIC_RELAXED)) {
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
