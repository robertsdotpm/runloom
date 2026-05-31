/*
 * cldeque_cbmc.c -- CBMC bounded-model-checking harness for the REAL
 * Chase-Lev deque.  This compiles the unmodified src/pygo_core/cldeque.c
 * alongside this file, so we verify the production C -- the actual index
 * arithmetic and the actual __atomic_* memory orderings -- not a model.
 *
 * Chase-Lev under a relaxed memory model is famously subtle: the
 * original SPAA'05 pseudocode is buggy under C11 (see Lê, Pop, Cohen,
 * Nardelli, "Correct and Efficient Work-Stealing for Weak Memory
 * Models", PPoPP'13).  cldeque.c uses the seq_cst pop/steal + the
 * acquire/release push that the corrected formulation prescribes; this
 * harness machine-checks that on the production source.
 *
 * One owner thread (push/pop at the bottom) races two thief threads
 * (steal from the top).  CBMC explores every thread interleaving.
 *
 * Properties:
 *   - NO DUPLICATION: no item returned by two consumers (claimed[] oracle)
 *   - NO PHANTOM:     a returned item is always a real pushed tag
 *   - NO LOSS:        consumed + deque_size == pushes  at quiescence
 *
 * Build/run:  see verify/cbmc/run_cbmc.sh  (or run_verify.sh).
 */
#include <pthread.h>
#include "cldeque.h"

#define NITEMS 3

static pygo_cldeque_t D;
static _Bool claimed[NITEMS + 1];   /* 1-indexed item tags                 */
static int   consumed;              /* items popped or stolen              */

/* Oracle: record a consumed item; flag duplicates / phantom tags.
 * The bookkeeping (not the deque) is made indivisible so the oracle
 * itself can't race; the concurrency under test lives in cldeque.c. */
static void claim(void *item)
{
    long k = (long)item;
    __CPROVER_assert(k >= 1 && k <= NITEMS, "consumed a real pushed tag");
    __CPROVER_atomic_begin();
    __CPROVER_assert(claimed[k] == 0, "no item consumed twice");
    claimed[k] = 1;
    consumed++;
    __CPROVER_atomic_end();
}

static void *owner(void *arg)
{
    void *x;
    (void)arg;
    /* A schedule that repeatedly hits the 1-element boundary, where the
     * owner's pop CAS races the thieves' steal CAS on `top`. */
    (void)pygo_cldeque_push(&D, (void *)1L);
    (void)pygo_cldeque_push(&D, (void *)2L);
    x = pygo_cldeque_pop(&D);   if (x) claim(x);
    (void)pygo_cldeque_push(&D, (void *)3L);
    x = pygo_cldeque_pop(&D);   if (x) claim(x);
    x = pygo_cldeque_pop(&D);   if (x) claim(x);
    return (void *)0;
}

static void *thief(void *arg)
{
    void *x;
    (void)arg;
    x = pygo_cldeque_steal(&D);  if (x) claim(x);
    x = pygo_cldeque_steal(&D);  if (x) claim(x);
    return (void *)0;
}

int main(void)
{
    pthread_t o, t1, t2;
    long size;

    pygo_cldeque_init(&D);

    pthread_create(&o,  (void *)0, owner, (void *)0);
    pthread_create(&t1, (void *)0, thief, (void *)0);
    pthread_create(&t2, (void *)0, thief, (void *)0);
    pthread_join(o,  (void *)0);
    pthread_join(t1, (void *)0);
    pthread_join(t2, (void *)0);

    /* Quiescent: every pushed item is either consumed exactly once or
     * still logically in the deque -- nothing vanished, nothing doubled. */
    size = pygo_cldeque_size(&D);
    __CPROVER_assert(consumed + size == 3, "no work-item lost");
    __CPROVER_assert(size >= 0, "deque size non-negative");
    return 0;
}
