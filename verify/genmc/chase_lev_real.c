/*
 * chase_lev_real.c -- GenMC oracle driving the ACTUAL production deque,
 * src/runloom_c/cldeque.c, verbatim (#included below), under RC11.  This is the
 * Gate-2 bug-check: not a transcription of the algorithm, the shipped code.
 *
 * CAP is overridden to 4 (the header explicitly supports this for a bounded
 * checker).  Two elements are pushed; the owner pop()s while TWO thieves
 * steal().  SPEC: no element returned twice (no duplication) -- the property the
 * SC ordering in pop()/steal() must guarantee.
 *
 * Build:  genmc -- -I<runloom_c> -DRUNLOOM_CLDEQUE_CAP=4 chase_lev_real.c
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

/* GenMC's pthread model lacks condition variables.  cldeque.c never uses them,
 * but plat_compat.h declares (static inline, unused-here) runloom_cond_* helpers
 * over pthread_cond_t.  Provide just enough for those inlines to type-check;
 * being unused they are never instantiated or executed. */
typedef struct { int unused; } pthread_cond_t;
static int pthread_cond_init(pthread_cond_t *c, void *a);
static int pthread_cond_destroy(pthread_cond_t *c);
static int pthread_cond_wait(pthread_cond_t *c, void *m);
static int pthread_cond_timedwait(pthread_cond_t *c, void *m, void *ts);
static int pthread_cond_signal(pthread_cond_t *c);
static int pthread_cond_broadcast(pthread_cond_t *c);

#define RUNLOOM_CLDEQUE_CAP 4
#include "cldeque.h"
#include "cldeque.c"            /* the real production source, verbatim */

static runloom_cldeque_t d;
static int vals[3] = { 0, 1, 2 };   /* real addresses, no fake pointers */
static atomic_int got1, got2;

static void rec(void *r)
{
    if (r == &vals[1]) atomic_fetch_add(&got1, 1);
    else if (r == &vals[2]) atomic_fetch_add(&got2, 1);
}

static void *thief(void *u) { (void)u; rec(runloom_cldeque_steal(&d)); return 0; }

int main(void)
{
    runloom_cldeque_init(&d);
    atomic_init(&got1, 0);
    atomic_init(&got2, 0);

    runloom_cldeque_push(&d, &vals[1]);
    runloom_cldeque_push(&d, &vals[2]);

    pthread_t a, b;
    pthread_create(&a, 0, thief, 0);
    pthread_create(&b, 0, thief, 0);

    rec(runloom_cldeque_pop(&d));      /* owner pop races the two thieves */

    pthread_join(a, 0);
    pthread_join(b, 0);

    /* no element returned by two consumers (no duplication) */
    assert(atomic_load(&got1) <= 1);
    assert(atomic_load(&got2) <= 1);
    return 0;
}
