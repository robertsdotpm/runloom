/*
 * cldeque_disjoint.c -- CBMC monitor for INV_race on the REAL Chase-Lev deque.
 *
 * cldeque_cbmc.c checks the OUTCOME form of correctness (no dup / no loss / no
 * phantom).  This harness checks the deeper INTERNAL invariant that EXPLAINS it
 * -- INV_race from verify/iris/rc11/chase_lev/NOTES.md §4 -- ported as a ghost
 * monitor on the production source (compiled with -DRUNLOOM_CLDEQUE_VERIFY so the
 * runloom_cl_* hooks in cldeque.c fire; the hooks are zero-cost without it).
 *
 * THE PREDICATE (segment-disjointness, the one-fact that closes the race):
 *   at pop()'s SC-fenced top-read t (with b = bottom-1), the owner still owns
 *   the whole segment it is about to operate on:   ∀ i ∈ [t, b]: owner owns i.
 * Plus TAKEN-once per index (no index claimed by two consumers).
 *
 * The ghost state (owner_of[], taken[]) is updated inside CBMC atomic sections
 * so the monitor itself is race-free; the concurrency under test is entirely in
 * cldeque.c, untouched.
 *
 * Build: cbmc cldeque_disjoint.c ../../src/runloom_c/cldeque.c \
 *        -I stubs -I ../../src/runloom_c -DRUNLOOM_CLDEQUE_CAP=4 -DRUNLOOM_CLDEQUE_VERIFY
 */
#include <pthread.h>
#include "cldeque.h"

#define NITEMS 3
#define MAXIDX 16          /* absolute indices stay small in the bounded harness */

/* OWNER must be 0 so the static zero-init of owner_of[] means "owner holds it"
 * with NO init loop (a 16-iteration init loop would, without
 * --unwinding-assertions, become assume(false) and make the whole harness
 * vacuously pass -- the trap this monitor must not fall into). */
#define OWNER 0
#define TAKEN 1

static runloom_cldeque_t D;
static int owner_of[MAXIDX];   /* 0 = OWNER (owner holds claim); TAKEN = claimed */
static int taken[MAXIDX];      /* times index claimed -- must end <= 1          */

/* ---- ghost hooks called from inside cldeque.c (zero-cost in production) ---- */

void runloom_cl_push(long i)
{
    __CPROVER_atomic_begin();
    owner_of[i] = OWNER;       /* owner now holds the claim for index i */
    __CPROVER_atomic_end();
}

void runloom_cl_pop_fenced(long t, long b)
{
    /* INV_race, runtime-robust boundary form.  The full "owner owns ALL of
     * [t,B)" is a proof-INSTANT invariant (true at the linearization point) but
     * NOT a valid runtime monitor: between this fenced read and any observation,
     * thieves legitimately claim indices in [t, b-1] (the owner only takes b).
     * What IS robust -- and what depends on the SC ordering -- is the
     * NO-CONTENTION branch (t < b): there top can never reach b while the owner
     * holds bottom = b, so index b stays owner-owned through the pop.  If the
     * SC store(bottom)+SC load(top) ordering were broken, a thief could see a
     * stale bottom, advance top to b, and steal b -- and THIS assert would
     * catch it. */
    if (t < b) {
        __CPROVER_atomic_begin();
        __CPROVER_assert(owner_of[b] == OWNER,
            "INV_race: in the no-contention branch the owner still owns index b");
        __CPROVER_atomic_end();
    }
}

void runloom_cl_claim(long i, int who)
{
    (void)who;
    __CPROVER_atomic_begin();
    __CPROVER_assert(owner_of[i] == OWNER, "no index claimed twice (disjointness)");
    owner_of[i] = TAKEN;
    taken[i]++;
    __CPROVER_assert(taken[i] == 1, "TAKEN-once per index");
    __CPROVER_atomic_end();
}

/* ---- workload: owner pushes/pops at the bottom, two thieves steal ---- */

/* push-then-DRAIN: indices are monotonic (no re-push after a pop), so the
 * index-based disjointness predicate is well-defined.  The pops still race the
 * thieves' steals, including the last-element CAS race. */
static void *owner(void *arg)
{
    void *x; (void)arg;
    (void)runloom_cldeque_push(&D, (void *)1L);
    (void)runloom_cldeque_push(&D, (void *)2L);
    (void)runloom_cldeque_push(&D, (void *)3L);
    x = runloom_cldeque_pop(&D);   (void)x;
    x = runloom_cldeque_pop(&D);   (void)x;
    x = runloom_cldeque_pop(&D);   (void)x;
    return (void *)0;
}

static void *thief(void *arg)
{
    void *x; (void)arg;
    x = runloom_cldeque_steal(&D);  (void)x;
    x = runloom_cldeque_steal(&D);  (void)x;
    return (void *)0;
}

int main(void)
{
    pthread_t o, t1, t2;
    runloom_cldeque_init(&D);     /* owner_of[] is static-zero = OWNER; no init loop */
    pthread_create(&o,  (void *)0, owner, (void *)0);
    pthread_create(&t1, (void *)0, thief, (void *)0);
    pthread_create(&t2, (void *)0, thief, (void *)0);
    pthread_join(o,  (void *)0);
    pthread_join(t1, (void *)0);
    pthread_join(t2, (void *)0);

#ifdef BUG_SELFTEST
    /* teeth: a deliberate double-claim of one index must trip the monitor,
       proving the disjointness / TAKEN-once assertions are not vacuous. */
    runloom_cl_push(7);
    runloom_cl_claim(7, 1);
    runloom_cl_claim(7, 1);
#endif
    return 0;
}
