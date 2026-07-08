/* test_barrier_race.c -- barrier-synchronized race amplification (QA-steal:
 * CPython-FT / jcstress @Actor idiom).
 *
 * Ambient contention (swarm / contention_hammer) applies broad load but rarely
 * lands two threads on the EXACT racy instruction at the same instant.  A
 * barrier immediately before the suspected-racy op forces all N threads to
 * collide there every round, maximizing clash probability for one primitive --
 * and paired with the now fiber-correct TSan (docs/dev/soak/
 * fiber_sanitizer_annotations.md) it is a reliable per-primitive race finder.
 *
 * barrier_race_run() is the reusable helper; add a barrier_fn per primitive.
 * Shipped targets:
 *   deque   -- Chase-Lev push/pop (owner) vs steal (thieves), conservation oracle
 *   handle  -- rl_handle pin vs reclaim (gen bump), value-integrity oracle
 *
 * Build/run: tests/tests_c/Makefile test_barrier_race{,-asan,-tsan};
 * tools/run_sanitizers.sh drives all three.  Usage: test_barrier_race
 * <deque|handle> [threads] [rounds].
 */
#include <stdatomic.h>
#include "cldeque.h"
#include "rl_handle.c"        /* compile the handle unit under test directly */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- reusable barrier amplifier ---------------------------------------- */
typedef void (*barrier_fn)(int tid, int round, void *arg);

typedef struct {
    pthread_barrier_t *bar;
    int tid, rounds;
    barrier_fn fn;
    void *arg;
} br_ctx;

static void *br_worker(void *p)
{
    br_ctx *c = (br_ctx *)p;
    int r;
    for (r = 0; r < c->rounds; r++) {
        pthread_barrier_wait(c->bar);   /* all N collide on the next line */
        c->fn(c->tid, r, c->arg);
    }
    return NULL;
}

static void barrier_race_run(int nthreads, int rounds, barrier_fn fn, void *arg)
{
    pthread_barrier_t bar;
    pthread_t th[64];
    br_ctx ctx[64];
    int i;
    if (nthreads > 64) nthreads = 64;
    pthread_barrier_init(&bar, NULL, (unsigned)nthreads);
    for (i = 0; i < nthreads; i++) {
        ctx[i].bar = &bar; ctx[i].tid = i; ctx[i].rounds = rounds;
        ctx[i].fn = fn; ctx[i].arg = arg;
        pthread_create(&th[i], NULL, br_worker, &ctx[i]);
    }
    for (i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
    pthread_barrier_destroy(&bar);
}

/* ---- target 1: Chase-Lev deque push/pop vs steal ----------------------- */
/* Owner (tid 0) pushes a fixed batch then pops the rest; thieves steal.  Each
 * void* is a unique token; a shared claimed[] bitmap catches a lost OR duplicated
 * item (the conservation property the buf-race broke). */
#define DQ_BATCH  6              /* < RUNLOOM_CLDEQUE_CAP so push never overflows */
static runloom_cldeque_t g_dq;
static atomic_int *g_claimed;    /* one flag per (round*batch + i) token */
static int g_dq_rounds;

static void dq_claim(long tok)
{
    if (tok <= 0) return;        /* NULL / empty steal */
    int prev = atomic_fetch_add(&g_claimed[tok], 1);
    if (prev != 0) { fprintf(stderr, "DUP claim tok=%ld (x%d)\n", tok, prev + 1); abort(); }
}

static void dq_round(int tid, int round, void *arg)
{
    (void)arg;
    long base = (long)round * DQ_BATCH + 1;   /* tokens 1..DQ_BATCH for this round */
    if (tid == 0) {
        int i;
        for (i = 0; i < DQ_BATCH; i++)
            while (runloom_cldeque_push(&g_dq, (void *)(base + i)) != 0)
                dq_claim((long)runloom_cldeque_pop(&g_dq));   /* full: drain one */
        /* owner also pops, racing the thieves' steals */
        for (i = 0; i < DQ_BATCH; i++)
            dq_claim((long)runloom_cldeque_pop(&g_dq));
    } else {
        int i;
        for (i = 0; i < DQ_BATCH; i++)
            dq_claim((long)runloom_cldeque_steal(&g_dq));
    }
}

static int run_deque(int nthreads, int rounds)
{
    long total = (long)rounds * DQ_BATCH, tok;
    long lost = 0;
    memset(&g_dq, 0, sizeof(g_dq));
    g_dq_rounds = rounds;
    g_claimed = (atomic_int *)calloc((size_t)total + 1, sizeof(atomic_int));
    barrier_race_run(nthreads, rounds, dq_round, NULL);
    /* Drain any stragglers the barrier rounds left in the deque. */
    { void *x; while ((x = runloom_cldeque_pop(&g_dq)) != NULL) dq_claim((long)x); }
    for (tok = 1; tok <= total; tok++)
        if (atomic_load(&g_claimed[tok]) != 1) lost++;
    free(g_claimed);
    if (lost) { fprintf(stderr, "deque: %ld/%ld tokens lost\n", lost, total); return 1; }
    printf("deque: OK (%ld tokens, %d threads x %d rounds, no loss/dup)\n",
           total, nthreads, rounds);
    return 0;
}

/* ---- target 2: rl_handle pin vs reclaim -------------------------------- */
/* A small pool of live handles, each pointing at a struct carrying a self-check
 * magic.  Threads pin/read/unpin and periodically release+re-register, so a
 * pin races a reclaim (generation bump + deferred free).  The generation stamp
 * must guarantee pin returns either the CURRENT object (magic valid) or NULL --
 * never a dangling/torn pointer.  ASan catches a UAF; the magic check catches a
 * stale registration slipping through the gen guard. */
#define H_SLOTS 4
typedef struct { unsigned magic; unsigned payload; } h_obj;
#define H_MAGIC 0xC0FFEEu
static _Atomic(rl_handle_t) g_h[H_SLOTS];

static void h_free(void *p) { h_obj *o = (h_obj *)p; o->magic = 0xDEAD; free(o); }

static rl_handle_t h_new(unsigned payload)
{
    h_obj *o = (h_obj *)malloc(sizeof(*o));
    o->magic = H_MAGIC; o->payload = payload;
    return rl_handle_register(o, h_free);
}

static void h_round(int tid, int round, void *arg)
{
    (void)arg;
    int slot = (tid + round) % H_SLOTS;
    rl_handle_t h = atomic_load(&g_h[slot]);
    h_obj *o = (h_obj *)rl_handle_pin(h);
    if (o != NULL) {
        if (o->magic != H_MAGIC) { fprintf(stderr, "stale magic 0x%x\n", o->magic); abort(); }
        (void)o->payload;
        rl_handle_unpin(h);
    }
    /* One writer per slot per round churns the generation, racing others' pins. */
    if (tid == slot) {
        rl_handle_t fresh = h_new((unsigned)round);
        rl_handle_t old = atomic_exchange(&g_h[slot], fresh);
        rl_handle_release(old);     /* reclaim: gen bump + deferred h_free */
    }
}

static int run_handle(int nthreads, int rounds)
{
    int i;
    for (i = 0; i < H_SLOTS; i++) atomic_store(&g_h[i], h_new((unsigned)i));
    barrier_race_run(nthreads, rounds, h_round, NULL);
    for (i = 0; i < H_SLOTS; i++) rl_handle_release(atomic_load(&g_h[i]));
    printf("handle: OK (%d slots, %d threads x %d rounds, no UAF/stale)\n",
           H_SLOTS, nthreads, rounds);
    return 0;
}

int main(int argc, char **argv)
{
    const char *mode = (argc > 1) ? argv[1] : "deque";
    int nthreads = (argc > 2) ? atoi(argv[2]) : 4;
    int rounds   = (argc > 3) ? atoi(argv[3]) : 20000;
    if (nthreads < 2) nthreads = 2;
    if (strcmp(mode, "deque") == 0)  return run_deque(nthreads, rounds);
    if (strcmp(mode, "handle") == 0) return run_handle(nthreads, rounds);
    fprintf(stderr, "usage: %s <deque|handle> [threads] [rounds]\n", argv[0]);
    return 2;
}
