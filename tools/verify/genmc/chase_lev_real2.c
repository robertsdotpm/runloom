/*
 * chase_lev_real2.c -- STRONGER GenMC oracle for the production deque
 * src/runloom_c/cldeque.c (#included verbatim), under RC11.  Where
 * chase_lev_real.c pushes both elements BEFORE starting the thieves (so the
 * buf[] writes happen-before thread creation and the push-publish orders are
 * never exercised concurrently), this runs a CONCURRENT PRODUCER: it pushes
 * WHILE two thieves steal and the owner pops.
 *
 * That exercises the push-publish handshake -- push's bottom RELEASE store
 * (cldeque.c:41) paired with steal's bottom ACQUIRE load (:88), and push's top
 * ACQUIRE load (:38).  Relaxing any of those makes the producer's plain item
 * store (buf[b] = item, :40) race the thief's plain item read (buf[t], :90)
 * with no ordering edge -- a data race GenMC reports directly.  So this harness
 * is expected to KILL the L38/L41/L88 moflip mutants that chase_lev_real.c
 * leaves as survivors, telling us which of the 5 survivors were "harness too
 * weak" vs genuinely RC11-redundant.
 *
 * SPEC: no element returned twice (no duplication) -- and GenMC's own race check
 * on buf[] catches any relaxed-publish stale/garbage read.
 *
 * Build:  genmc -- -I<runloom_c> -DRUNLOOM_CLDEQUE_CAP=4 chase_lev_real2.c
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

/* GenMC's pthread model lacks condition variables; plat_compat.h declares
 * (unused-here) runloom_cond_* over pthread_cond_t.  Stub just enough to
 * type-check -- never instantiated or executed. */
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

    /* Chase-Lev's ONE owner does BOTH push and pop; only steal is concurrent.
     * Start the thieves FIRST, then have the owner (this thread) push AND pop --
     * so the owner's push runs CONCURRENTLY with the running thieves' steals,
     * exercising the push-publish (bottom RELEASE :41) vs steal bottom ACQUIRE
     * (:88) / top ACQUIRE (:38) handshake that chase_lev_real.c (all-push-first)
     * never does.  Relaxing those orders makes the owner's plain buf[b] store
     * race a thief's plain buf[t] read -> GenMC reports it. */
    pthread_t a, b;
    pthread_create(&a, 0, thief, 0);
    pthread_create(&b, 0, thief, 0);

    runloom_cldeque_push(&d, &vals[1]);
    runloom_cldeque_push(&d, &vals[2]);
    rec(runloom_cldeque_pop(&d));
    rec(runloom_cldeque_pop(&d));

    pthread_join(a, 0);
    pthread_join(b, 0);

    /* no element returned by two consumers (no duplication) */
    assert(atomic_load(&got1) <= 1);
    assert(atomic_load(&got2) <= 1);
    return 0;
}
