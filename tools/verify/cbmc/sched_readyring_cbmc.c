/*
 * sched_readyring_cbmc.c -- CBMC harness for runloom's per-sched ready FIFO ring
 * (runloom_sched.c: runloom_sched_ready_push / runloom_sched_ready_pop / runloom_ready_grow).
 *
 * FAITHFUL SLICE (not byte-shared).  runloom_sched.c pulls in Python.h, so the
 * three ring ops are reproduced here verbatim: monotonic head/tail counters,
 * power-of-2 mask indexing (`idx = counter & mask`), and grow-by-doubling that
 * copies the live window [head,tail) and re-bases head to 0.  (The production
 * grow malloc/free is modelled with a static backing array + scratch so CBMC
 * needn't reason about the heap; the index arithmetic is identical.)  Keep in
 * sync with the source -- the run script's drift-guard checks the ops.
 *
 * The ready ring is SINGLE-THREADED (per-sched, owner-only -- cross-thread wakes
 * go through the mutex'd wake_list, NOT this ring), so this is a bounded
 * SEQUENTIAL proof: a NONDETERMINISTIC push/pop sequence -- which CBMC forces
 * through wraparound AND a grow -- preserves FIFO order with no loss, no
 * duplication, no phantom, and grow preserves the live window.
 *
 * Capacities are shrunk (INIT_CAP=2, MAXCAP=8) for a tractable encoding; the
 * index arithmetic and grow logic are identical to production (init 64).
 *
 * Negative controls (must FAIL = CBMC finds the bug):
 *   -DBUG_GROW_NOOFFSET : grow copies old[i&mask] instead of old[(head+i)&mask]
 *                         -> a wrapped ring is reordered / loses entries on grow.
 *   -DBUG_NO_CAPCHECK   : push skips the full check -> overwrites a live entry
 *                         when tail catches head (lost goroutine).
 */

extern _Bool nondet_bool(void);

#define INIT_CAP 2
#define MAXCAP   8          /* doubling 2->4->8 stays within the op bound */
#define NOPS     7

typedef struct { int id; } g_t;     /* stand-in for runloom_g_t* (track by id) */

static g_t *ring[MAXCAP];           /* the backing array (static, no heap) */
static g_t *scratch[MAXCAP];        /* grow copy buffer */

static unsigned long ready_cap, ready_mask, ready_head, ready_tail;

/* ---- grow: faithful copy of runloom_ready_grow (doubling, head re-based to 0) -- */
static int ready_grow(void)
{
    unsigned long new_cap = ready_cap ? ready_cap * 2 : INIT_CAP;
    unsigned long i, head = ready_head, tail = ready_tail, count = tail - head;
    if (new_cap > MAXCAP) return -1;            /* harness bound (not in src) */
    for (i = 0; i < count; i++) {
#ifdef BUG_GROW_NOOFFSET
        scratch[i] = ring[i & ready_mask];
#else
        scratch[i] = ring[(head + i) & ready_mask];
#endif
    }
    for (i = 0; i < count; i++) ring[i] = scratch[i];
    ready_cap  = new_cap;
    ready_mask = new_cap - 1;
    ready_head = 0;
    ready_tail = count;
    return 0;
}

/* ---- push: faithful copy of runloom_sched_ready_push ---- */
static void ready_push(g_t *g)
{
#ifndef BUG_NO_CAPCHECK
    if (ready_tail - ready_head >= ready_cap) {
        if (ready_grow() < 0) return;           /* OOM/bound: drop (as in src) */
    }
#endif
    ring[ready_tail & ready_mask] = g;
    ready_tail++;
}

/* ---- pop: faithful copy of runloom_sched_ready_pop ---- */
static g_t *ready_pop(void)
{
    g_t *g;
    if (ready_head == ready_tail) return 0;
    g = ring[ready_head & ready_mask];
    ready_head++;
    return g;
}

int main(void)
{
    ready_cap  = INIT_CAP;
    ready_mask = INIT_CAP - 1;
    ready_head = 0;
    ready_tail = 0;

    g_t items[NOPS];
    int ref[NOPS];                  /* reference FIFO of ids */
    int rh = 0, rt = 0;
    int next_id = 1;

    for (int step = 0; step < NOPS; step++) {
        if (nondet_bool() && next_id <= NOPS) {
            items[next_id - 1].id = next_id;
            ready_push(&items[next_id - 1]);
            ref[rt++] = next_id;
            next_id++;
        } else {
            g_t *g = ready_pop();
            if (rh == rt) {
                __CPROVER_assert(g == 0, "pop on empty returns NULL");
            } else {
                __CPROVER_assert(g != 0, "pop on non-empty returns an entry (no loss)");
                __CPROVER_assert(g->id == ref[rh], "FIFO order preserved (no reorder/dup)");
                rh++;
            }
        }
        __CPROVER_assert((ready_tail - ready_head) == (unsigned long)(rt - rh),
                         "ring count == reference count (no loss/dup across wrap/grow)");
    }

    /* Drain: every still-queued entry comes out in FIFO order. */
    while (rh < rt) {
        g_t *g = ready_pop();
        __CPROVER_assert(g != 0 && g->id == ref[rh], "drain in FIFO order");
        rh++;
    }
    __CPROVER_assert(ready_pop() == 0, "fully drained -> NULL");
    return 0;
}
