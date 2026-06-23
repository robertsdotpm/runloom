/*
 * chunk_pool_alias_cbmc.c -- CBMC proof that runloom's datastack-chunk pool never
 * aliases the live CPython data-stack chain (runloom_chunk_pool_get / _install in
 * runloom_sched_pystate.c.inc, vs CPython's _PyThreadState_PopFrame in
 * Python/pystate.c:2985).
 *
 * THE HAZARD (the two-owner alias the critic flagged; see
 * docs/dev/LIFECYCLE_INVARIANTS.md Tier-1 #4).  runloom reuses `_PyStackChunk.previous`
 * as its pool free-list link -- the SAME field CPython threads the data-stack chain
 * through and walks + frees in _PyThreadState_PopFrame.  CPython frees the current
 * chunk and walks to `chunk->previous` exactly when a popped frame sits at the chunk
 * root (`base == &chunk->data[0]`); push_chunk avoids popping a ROOT chunk by starting
 * its first frame at `&data[new->previous == NULL]` (data[1] for a root).  So if a
 * pooled chunk is ever reachable from the live `datastack_chunk` via `->previous`,
 * CPython will arena-free / re-own a chunk that is STILL in runloom's pool -> a
 * double-owned chunk handed to two fibers = the "random UAF in exception state once
 * ~25 fibers are alive" bug.
 *
 * runloom maintains the safety with TWO guards, both modelled here:
 *   SEVER     -- pool_get clears `c->previous = NULL` before handing the chunk out,
 *                so the installed chunk's chain never includes a pool entry.
 *   ROOT-SKIP -- pool_install sets datastack_top = &c->data[1] (not data[0]), so the
 *                installed chunk is treated as a CPython root and its root frame is
 *                never popped (base is never &data[0]) -> the walk-to-previous path
 *                never fires for it.
 *
 * INVARIANT proved: at every point, the live data-stack chain (datastack_chunk and
 * every chunk reachable via ->previous) is DISJOINT from the pool free-list, and a
 * CPython PopFrame never frees/re-owns a pooled chunk.
 *
 * Negative controls (must FAIL = CBMC finds the alias):
 *   -DBUG_NO_SEVER     : pool_get keeps the free-list `previous` -> the installed
 *                        chunk's chain walks straight into the pool.
 *   -DBUG_NO_ROOT_SKIP : pool_install starts at data[0] -> the installed chunk's
 *                        root frame IS popped, firing the walk-to-previous free.
 */

extern _Bool nondet_bool(void);

#define N 4              /* chunks in play (pool + live), bounded for the encoding */
#define NIL (-1)

struct chunk {
    int previous;        /* index of the next chunk via ->previous, or NIL */
    int top;             /* datastack_top offset: 0 == &data[0], 1 == &data[1] */
    int in_pool;         /* on runloom's pool free-list */
    int freed;           /* arena-freed by CPython */
};

static struct chunk ch[N];
static int pool_head;          /* runloom_chunk_pool */
static int live_chunk;         /* tstate->datastack_chunk */
static int live_top;           /* tstate->datastack_top offset within live_chunk */

/* runloom_chunk_pool_get: pop the head, (sever its free-list link), reset top. */
static int pool_get(void)
{
    int c = pool_head;
    if (c == NIL) return NIL;
    pool_head = ch[c].previous;       /* unlink via the free-list `previous` */
    ch[c].in_pool = 0;
#ifndef BUG_NO_SEVER
    ch[c].previous = NIL;             /* SEVER: standalone chunk, no link into the pool */
#endif
    ch[c].top = 0;
    return c;
}

/* runloom_chunk_pool_install: wire the chunk onto the tstate as the root chunk. */
static void pool_install(int c)
{
    live_chunk = c;
#ifdef BUG_NO_ROOT_SKIP
    live_top = 0;                     /* data[0]: root frame WILL be popped (bug) */
#else
    live_top = 1;                     /* data[1]: CPython root-skip -> never popped */
#endif
}

/* The live data-stack chain (datastack_chunk + everything via ->previous) is
 * DISJOINT from the pool free-list: no chunk is both live-reachable and pooled. */
static void assert_chain_pool_disjoint(void)
{
    int c = live_chunk, steps = 0;
    while (c != NIL && steps < N + 1) {
        __CPROVER_assert(ch[c].in_pool == 0,
            "a pooled chunk is reachable from the live data-stack chain (alias)");
        __CPROVER_assert(ch[c].freed == 0,
            "a freed chunk is reachable from the live data-stack chain (UAF)");
        c = ch[c].previous;
        steps++;
    }
}

/* CPython _PyThreadState_PopFrame, root case: fires iff the popped frame is at
 * &data[0] (live_top == 0).  It walks to `previous`, severs, and arena-frees the
 * just-emptied chunk's predecessor cache slot.  Modelled: if it fires, the chunk it
 * is about to walk into / free must NOT be a pooled chunk. */
static void cpython_popframe_root(void)
{
    if (live_top == 0) {                       /* base == &data[0]: root-pop path */
        int prev = ch[live_chunk].previous;
        __CPROVER_assert(prev != NIL,
            "PopFrame root-pop requires a previous chunk (push_chunk invariant)");
        if (prev != NIL) {
            __CPROVER_assert(ch[prev].in_pool == 0,
                "PopFrame walks into a still-pooled chunk via ->previous");
            ch[live_chunk].freed = 0;          /* current chunk becomes the cache slot */
            ch[live_chunk].previous = NIL;     /* CPython severs here too */
            live_chunk = prev;                 /* walk to previous */
            live_top = ch[prev].top;
        }
    }
}

int main(void)
{
    /* Build a pool free-list of all N chunks, linked via `previous`. */
    for (int i = 0; i < N; i++) {
        ch[i].previous = (i + 1 < N) ? (i + 1) : NIL;
        ch[i].top = 0;
        ch[i].in_pool = 1;
        ch[i].freed = 0;
    }
    pool_head = 0;
    live_chunk = NIL;
    live_top = 0;

    /* Acquire a chunk for a fiber and install it. */
    int c = pool_get();
    if (c == NIL) return 0;

#ifndef BUG_NO_SEVER
    __CPROVER_assert(ch[c].previous == NIL,
                     "pool_get severs the free-list link (previous == NULL)");
#endif

    pool_install(c);
    assert_chain_pool_disjoint();

    /* A fiber's root frame pops to chunk-empty: CPython's PopFrame may fire. */
    cpython_popframe_root();
    assert_chain_pool_disjoint();

    /* And once more (a second pooled chunk could be reached on the walk). */
    cpython_popframe_root();
    assert_chain_pool_disjoint();

    return 0;
}
