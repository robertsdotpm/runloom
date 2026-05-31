/*
 * test_cldeque.c -- high-volume concurrency stress for the Chase-Lev
 * work-stealing deque (src/pygo_core/cldeque.c).
 *
 * Complements verify/cbmc/cldeque_cbmc.c: CBMC proves correctness
 * exhaustively but on a tiny bounded schedule; this hammers the SAME
 * source with millions of real ops across real OS threads, and is meant
 * to be run under ThreadSanitizer (data races) and AddressSanitizer
 * (use-after-free / OOB).  Compiles cldeque.c directly (no Python).
 *
 * Invariants checked at runtime:
 *   - NO DUPLICATION: every pushed tag is consumed at most once
 *     (atomic test-and-set on claimed[]); a double-consume aborts.
 *   - NO PHANTOM:     a consumed tag is always in the pushed range.
 *   - NO LOSS:        after a full drain, consumed == total pushed and
 *     the deque is empty.
 *
 * Usage:  test_cldeque [TOTAL_PUSH] [N_THIEVES] [N_ROUNDS]
 * Default 50000 pushes x 4 thieves x 4 rounds.
 *
 * Build (see tests_c/Makefile):
 *   make -C tests_c test_cldeque            # plain -O2
 *   make -C tests_c test_cldeque-tsan       # data races
 *   make -C tests_c test_cldeque-asan       # memory errors
 *   make -C tests_c test_cldeque-ubsan
 */
#include "cldeque.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static pygo_cldeque_t   D;
static unsigned char   *claimed;     /* claimed[tag] = 1 once consumed     */
static long             total_push;  /* tags are 1..total_push             */
static long             consumed;    /* atomic count of consumed items     */
static int              producing;   /* atomic: owner still pushing         */
static int              n_thieves;

static void claim(void *item)
{
    long tag = (long)item;
    unsigned char old;
    if (tag < 1 || tag > total_push) {
        fprintf(stderr, "PHANTOM: consumed out-of-range tag %ld\n", tag);
        abort();
    }
    /* Atomic test-and-set: two consumers claiming the same slot is the
     * bug we are hunting -- under TSan this is also a flagged race if the
     * deque ever hands the same pointer to two threads. */
    old = __atomic_exchange_n(&claimed[tag], 1, __ATOMIC_SEQ_CST);
    if (old != 0) {
        fprintf(stderr, "DUPLICATION: tag %ld consumed twice\n", tag);
        abort();
    }
    __atomic_add_fetch(&consumed, 1, __ATOMIC_SEQ_CST);
}

static void *owner_fn(void *arg)
{
    long i;
    void *x;
    (void)arg;
    for (i = 1; i <= total_push; i++) {
        /* Push; if full, pop some down to make room (also races thieves). */
        while (pygo_cldeque_push(&D, (void *)i) != 0) {
            x = pygo_cldeque_pop(&D);
            if (x) claim(x);
        }
        /* Periodically pop from the bottom so the owner's pop CAS races
         * the thieves' steal CAS on the last element. */
        if ((i & 3) == 0) {
            x = pygo_cldeque_pop(&D);
            if (x) claim(x);
        }
    }
    /* Owner helps drain whatever the thieves left behind. */
    while ((x = pygo_cldeque_pop(&D)) != NULL) claim(x);
    __atomic_store_n(&producing, 0, __ATOMIC_RELEASE);
    return NULL;
}

static void *thief_fn(void *arg)
{
    void *x;
    (void)arg;
    for (;;) {
        x = pygo_cldeque_steal(&D);
        if (x) {
            claim(x);
            continue;
        }
        /* Empty/lost.  Stop only once production is done AND the deque
         * has been observed empty (owner's final drain has run). */
        if (!__atomic_load_n(&producing, __ATOMIC_ACQUIRE) &&
            pygo_cldeque_size(&D) <= 0) {
            break;
        }
    }
    return NULL;
}

static int run_round(void)
{
    pthread_t owner;
    pthread_t thieves[64];
    int i;
    long size;

    pygo_cldeque_init(&D);
    for (i = 0; i <= total_push; i++) claimed[i] = 0;
    consumed = 0;
    __atomic_store_n(&producing, 1, __ATOMIC_RELEASE);

    pthread_create(&owner, NULL, owner_fn, NULL);
    for (i = 0; i < n_thieves; i++)
        pthread_create(&thieves[i], NULL, thief_fn, NULL);

    pthread_join(owner, NULL);
    for (i = 0; i < n_thieves; i++) pthread_join(thieves[i], NULL);

    size = pygo_cldeque_size(&D);
    if (size != 0) {
        fprintf(stderr, "LOSS: deque not empty after drain (size=%ld)\n", size);
        return 1;
    }
    if (consumed != total_push) {
        fprintf(stderr, "LOSS: consumed %ld of %ld pushed\n", consumed, total_push);
        return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    int rounds, r;
    total_push = (argc > 1) ? atol(argv[1]) : 50000;
    n_thieves  = (argc > 2) ? atoi(argv[2]) : 4;
    rounds     = (argc > 3) ? atoi(argv[3]) : 4;
    if (n_thieves < 1)  n_thieves = 1;
    if (n_thieves > 64) n_thieves = 64;
    if (total_push < 1) total_push = 1;

    claimed = (unsigned char *)malloc((size_t)total_push + 1);
    if (!claimed) { fprintf(stderr, "OOM\n"); return 2; }

    printf("test_cldeque: %ld pushes, %d thieves, %d rounds (CAP=%d)\n",
           total_push, n_thieves, rounds, PYGO_CLDEQUE_CAP);
    for (r = 0; r < rounds; r++) {
        if (run_round() != 0) {
            fprintf(stderr, "FAIL at round %d\n", r);
            free(claimed);
            return 1;
        }
        printf("  round %d ok: consumed=%ld\n", r, consumed);
    }
    free(claimed);
    printf("PASS: no loss, no duplication, no phantom across %d rounds\n", rounds);
    return 0;
}
