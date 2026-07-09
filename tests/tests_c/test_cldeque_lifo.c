/*
 * test_cldeque_lifo.c -- DETERMINISTIC single-threaded semantics of the
 * Chase-Lev work-stealing deque (src/runloom_c/cldeque.c).
 *
 * Companion to test_cldeque.c (which hammers the SAME source with racing
 * thieves and checks conservation) and verify/cbmc/cldeque_cbmc.c (which proves
 * the concurrent ordering).  Neither pins the ISOLATED, no-thieves good-case
 * that a reader expects of a deque -- so this asserts it directly:
 *   - owner push/pop is LIFO (push A,B,C -> pop C,B,A -> NULL);
 *   - size tracks 1:1 with push/pop;
 *   - steal takes from the OTHER end, FIFO/oldest-first (push A,B,C -> steal
 *     A,B,C), with no thief racing;
 *   - push returns -1 (full) at RUNLOOM_CLDEQUE_CAP and does not corrupt size.
 *
 * Built with -DRUNLOOM_SHRINK so RUNLOOM_CLDEQUE_CAP == 8 (cheap full test).
 * Build + run via tests_c/Makefile (`make test_cldeque_lifo`); the pytest
 * wrapper is tests/test_cldeque_lifo_c.py.  Prints PASS and returns 0 on
 * success; on any failure prints the offending check and returns 1.
 */
#include "cldeque.h"

#include <stdio.h>

static int failures;

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            fprintf(stderr, "FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            failures++;                                                     \
        }                                                                   \
    } while (0)

/* Tags are non-NULL pointers 1..N (NULL is the deque's empty sentinel). */
#define TAG(i) ((void *)(long)(i))

static void test_empty(void)
{
    runloom_cldeque_t d;
    runloom_cldeque_init(&d);
    CHECK(runloom_cldeque_size(&d) == 0, "fresh deque size 0");
    CHECK(runloom_cldeque_pop(&d) == NULL, "pop of empty deque is NULL");
    CHECK(runloom_cldeque_steal(&d) == NULL, "steal of empty deque is NULL");
}

static void test_owner_lifo(void)
{
    runloom_cldeque_t d;
    runloom_cldeque_init(&d);
    CHECK(runloom_cldeque_push(&d, TAG(1)) == 0, "push A ok");
    CHECK(runloom_cldeque_push(&d, TAG(2)) == 0, "push B ok");
    CHECK(runloom_cldeque_push(&d, TAG(3)) == 0, "push C ok");
    CHECK(runloom_cldeque_size(&d) == 3, "size 3 after 3 pushes");

    /* Owner pops from the bottom: LIFO -> C, B, A. */
    CHECK(runloom_cldeque_pop(&d) == TAG(3), "pop returns C (LIFO)");
    CHECK(runloom_cldeque_size(&d) == 2, "size 2 after one pop");
    CHECK(runloom_cldeque_pop(&d) == TAG(2), "pop returns B (LIFO)");
    CHECK(runloom_cldeque_pop(&d) == TAG(1), "pop returns A (LIFO)");
    CHECK(runloom_cldeque_size(&d) == 0, "size 0 after draining");
    CHECK(runloom_cldeque_pop(&d) == NULL, "pop of drained deque is NULL");
}

static void test_steal_fifo_from_top(void)
{
    runloom_cldeque_t d;
    runloom_cldeque_init(&d);
    runloom_cldeque_push(&d, TAG(1));
    runloom_cldeque_push(&d, TAG(2));
    runloom_cldeque_push(&d, TAG(3));

    /* A thief takes from the TOP: oldest-first FIFO -> A, B, C.  No concurrent
     * owner, so every steal commits. */
    CHECK(runloom_cldeque_steal(&d) == TAG(1), "steal returns A (FIFO top)");
    CHECK(runloom_cldeque_steal(&d) == TAG(2), "steal returns B (FIFO top)");
    CHECK(runloom_cldeque_steal(&d) == TAG(3), "steal returns C (FIFO top)");
    CHECK(runloom_cldeque_size(&d) == 0, "size 0 after stealing all");
    CHECK(runloom_cldeque_steal(&d) == NULL, "steal of drained deque is NULL");
}

static void test_full_at_cap(void)
{
    runloom_cldeque_t d;
    long i;
    runloom_cldeque_init(&d);

    for (i = 1; i <= RUNLOOM_CLDEQUE_CAP; i++) {
        CHECK(runloom_cldeque_push(&d, TAG(i)) == 0, "push within cap ok");
    }
    CHECK(runloom_cldeque_size(&d) == RUNLOOM_CLDEQUE_CAP, "size == CAP when full");

    /* One past cap: push must report full (-1) and leave size unchanged. */
    CHECK(runloom_cldeque_push(&d, TAG(999)) == -1, "push past CAP returns -1 (full)");
    CHECK(runloom_cldeque_size(&d) == RUNLOOM_CLDEQUE_CAP, "size unchanged after full push");

    /* Drain LIFO: the last-in (CAP) comes out first, the never-admitted 999
     * is absent. */
    for (i = RUNLOOM_CLDEQUE_CAP; i >= 1; i--) {
        CHECK(runloom_cldeque_pop(&d) == TAG(i), "drain in LIFO order");
    }
    CHECK(runloom_cldeque_size(&d) == 0, "empty after full drain");
}

int main(void)
{
    test_empty();
    test_owner_lifo();
    test_steal_fifo_from_top();
    test_full_at_cap();
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("PASS: cldeque owner-LIFO / steal-FIFO / size / full-at-CAP(%d)\n",
           RUNLOOM_CLDEQUE_CAP);
    return 0;
}
