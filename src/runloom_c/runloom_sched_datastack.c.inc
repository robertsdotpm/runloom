/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


void runloom_first_run_install_datastack(void)
{
#if PY_VERSION_HEX >= 0x030B0000
    PyThreadState *ts = PyThreadState_GET();
    if (!runloom_chunk_pool_install(ts)) {
        ts->datastack_chunk = NULL;
        ts->datastack_top = NULL;
        ts->datastack_limit = NULL;
    }
#else
    (void)0;
#endif
}

/* ---- Datastack-tail idle reclaim (companion to the C-stack sweep) ----
 *
 * A Python goroutine parked deep in CPython (e.g. in recv inside the eval
 * loop) holds a _PyStackChunk with its live frames in [chunk, datastack_top)
 * and unpushed free space in [datastack_top, datastack_limit).  The dwell
 * stack-sweep madvises the C stack below SP but NEVER this chunk, so the
 * chunk's free tail stays resident on every parked Python g.  This drops it.
 *
 * Only the CURRENT (top) chunk has a reclaimable tail: chunk->previous
 * chunks are full of live outer frames, so we touch only datastack_chunk.
 * The chunk is an arena allocation (mmap, page-aligned base, size a multiple
 * of the page), so datastack_limit is page-aligned and the geometry is clean.
 * We keep the partial page holding the live frontier and drop whole pages
 * strictly above it.  Refault on the next frame push is the (cheap, zero-
 * filled) cost, identical to the C-stack sweep's contract. */
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
/* Decompose counters (RUNLOOM_DATASTACK_DEBUG only). */
static unsigned long long runloom_ds_sweep_tail_bytes = 0;
static unsigned long long runloom_ds_sweep_resident_bytes = 0;
static unsigned long long runloom_ds_sweep_chunks = 0;

/* mincore the [lo,hi) range and return resident bytes.  Best-effort: on any
 * error (e.g. mincore unsupported) returns 0 so accounting just under-counts
 * rather than misleads. */
static unsigned long long runloom_ds_resident_bytes(uintptr_t lo, uintptr_t hi,
                                                 size_t page)
{
    unsigned char vec[64];           /* covers up to 64 pages == 256 KiB tail */
    size_t npages = (size_t)((hi - lo) / page);
    unsigned long long resident = 0;
    size_t i;
    if (npages == 0 || npages > sizeof(vec)) return 0;
    if (mincore((void *)lo, (size_t)(hi - lo), vec) != 0) return 0;
    for (i = 0; i < npages; i++) {
        if (vec[i] & 1) resident += (unsigned long long)page;
    }
    return resident;
}
#endif

void runloom_sched_madvise_datastack_idle(runloom_g_t *g)
{
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
    static int on = -1;              /* read RUNLOOM_DATASTACK_SWEEP once */
    static int dbg = -1;             /* read RUNLOOM_DATASTACK_DEBUG once */
    runloom_pystate_snap_t *snap;
    uintptr_t top, limit, lo, hi;
    long ps;
    size_t page;

    {
        int v = __atomic_load_n(&on, __ATOMIC_RELAXED);
        if (v < 0) {
            /* Default-ON, mirroring the master RUNLOOM_STACK_PARK_SWEEP flip:
             * unset -> on, "0" -> off.  The master sweep already gates
             * whether this runs at all (it's called only from the sweep's
             * batch loop) and supplies the churn throttle, so on a flat
             * handler -- where the tail is unfaulted and we reclaim nothing
             * -- this is just a cheap no-op madvise (measured: no throughput
             * cost); on the common deep-then-park shape it reclaims real
             * resident RAM.  Opt out with RUNLOOM_DATASTACK_SWEEP=0. */
            const char *e = getenv("RUNLOOM_DATASTACK_SWEEP");
            v = (e != NULL && e[0] == '0') ? 0 : 1;
            __atomic_store_n(&on, v, __ATOMIC_RELAXED);
        }
        if (!v) return;
    }

    if (g == NULL) return;
    snap = &g->snap;
    if (!snap->valid || snap->datastack_chunk == NULL ||
        snap->datastack_top == NULL || snap->datastack_limit == NULL) {
        return;                      /* C-only g, or no chunk installed */
    }

    ps = sysconf(_SC_PAGESIZE);
    page = (ps > 0) ? (size_t)ps : (size_t)4096;
    top   = (uintptr_t)snap->datastack_top;
    limit = (uintptr_t)snap->datastack_limit;
    if (limit <= top) return;
    lo = (top + page - 1) & ~(uintptr_t)(page - 1);   /* align UP past frontier */
    hi = limit & ~(uintptr_t)(page - 1);              /* align DOWN to chunk end */
    if (hi <= lo) return;            /* no whole free page to drop */

    {
        int d = __atomic_load_n(&dbg, __ATOMIC_RELAXED);
        if (d < 0) {
            const char *e = getenv("RUNLOOM_DATASTACK_DEBUG");
            d = (e != NULL && e[0] != '0') ? 1 : 0;
            __atomic_store_n(&dbg, d, __ATOMIC_RELAXED);
        }
        if (d) {
            unsigned long long tail = (unsigned long long)(hi - lo);
            unsigned long long res = runloom_ds_resident_bytes(lo, hi, page);
            __atomic_add_fetch(&runloom_ds_sweep_tail_bytes, tail,
                               __ATOMIC_RELAXED);
            __atomic_add_fetch(&runloom_ds_sweep_resident_bytes, res,
                               __ATOMIC_RELAXED);
            __atomic_add_fetch(&runloom_ds_sweep_chunks, 1ULL, __ATOMIC_RELAXED);
        }
    }

    (void)madvise((void *)lo, (size_t)(hi - lo), MADV_DONTNEED);
#else
    (void)g;
#endif
}

void runloom_sched_datastack_sweep_stats(unsigned long long *tail_bytes,
                                      unsigned long long *resident_bytes,
                                      unsigned long long *chunks)
{
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
    if (tail_bytes)
        *tail_bytes = __atomic_load_n(&runloom_ds_sweep_tail_bytes, __ATOMIC_RELAXED);
    if (resident_bytes)
        *resident_bytes = __atomic_load_n(&runloom_ds_sweep_resident_bytes,
                                          __ATOMIC_RELAXED);
    if (chunks)
        *chunks = __atomic_load_n(&runloom_ds_sweep_chunks, __ATOMIC_RELAXED);
#else
    if (tail_bytes) *tail_bytes = 0;
    if (resident_bytes) *resident_bytes = 0;
    if (chunks) *chunks = 0;
#endif
}

/* Drain the datastack-chunk chain currently attached to tstate,
 * returning tstate's datastack pointers to NULL.
 *
 * Called after a goroutine completes, BEFORE we restore the scheduler
 * or hub snapshot (which would overwrite tstate->datastack_chunk with
 * the scheduler's saved value and leak g's chain).
 *
 * Reused chunks go to the per-thread pool (up to RUNLOOM_CHUNK_POOL_CAP).
 * Overflow goes back to the arena allocator that CPython's frame
 * allocator pulls from.
 *
 * Algorithm matches greenlet's PythonState::did_finish, plus pool reuse. */
void runloom_drain_g_datastack(void)
{
#if PY_VERSION_HEX >= 0x030B0000
    PyThreadState *ts = PyThreadState_GET();
    _PyStackChunk *chunk = ts->datastack_chunk;
    PyObjectArenaAllocator alloc;

    if (chunk == NULL) return;

    ts->datastack_chunk = NULL;
    ts->datastack_top = NULL;
    ts->datastack_limit = NULL;

    PyObject_GetArenaAllocator(&alloc);
    while (chunk != NULL) {
        _PyStackChunk *prev = chunk->previous;
        runloom_chunk_pool_put(chunk, &alloc);
        chunk = prev;
    }
#endif
}

/* Install an initial root for the goroutine's Python frame chain.  Run
 * inside runloom_g_entry, on the goroutine's own stack, BEFORE we call any
 * user Python code.
 *
 * The point: when user code calls PyEval_EvalFrameDefault, the new
 * interpreter frame's `previous` field is linked to whatever was at
 * tstate's "top of chain" pointer.  If we don't sever the chain, that
 * "top" is whoever ran most recently on this OS thread -- the
 * scheduler, or worse, another goroutine.  Then traceback walks and
 * recursion checks pull in every frame across every goroutine.
 *
 * On 3.12, we put a fresh _PyCFrame at the bottom of g's stack, point its
 * previous to tstate->root_cframe (the per-thread sentinel), AND null its
 * current_frame so g's first interpreter frame roots its own chain.
 * On 3.13, the cframe is gone; we just NULL out tstate->current_frame.
 * In both cases the chain starts here and walks back to a terminator,
 * not to the previous coroutine. */
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
static void runloom_install_initial_root_frame(_PyCFrame *frame_storage)
{
    PyThreadState *ts = PyThreadState_GET();
    *frame_storage = *ts->cframe;
    /* Sever BOTH chains, not just the cframe `previous`: current_frame is what
     * interpreter-frame `previous` links to and what traceback/recursion walks
     * follow.  Inheriting the scheduler's current_frame links g's frames onto
     * whoever ran last on this thread -- usually harmless (that frame is live),
     * but after a frame is torn down (e.g. a task aborted by a KeyboardInterrupt
     * whose run is then re-entered) the inherited current_frame dangles, and g's
     * first frame push roots onto freed memory.  Silent on glibc (still mapped),
     * a hard access violation on Windows.  3.13 already nulls current_frame; do
     * the same here. */
    frame_storage->current_frame = NULL;
    frame_storage->previous = &ts->root_cframe;
    ts->cframe = frame_storage;
}
#endif

#if PY_VERSION_HEX >= 0x030D0000
static void runloom_install_initial_root_frame(void)
{
    PyThreadState *ts = PyThreadState_GET();
    ts->current_frame = NULL;
}
#endif

/* ---- Ready FIFO ops (ring buffer of g pointers) ----
 *
 * head/tail are MONOTONIC counters, masked into the ring on access.
 * As long as we don't wrap a size_t (which would take >5000 years at
 * 100M push/s on 64-bit), the (tail - head) arithmetic gives the
 * exact count.  Wraparound-safe for 32-bit too because we mask
 * differences, not absolute values.
 *
 * Grows on overflow by doubling; the ring is per-thread so no lock.
 */
static int runloom_ready_grow(runloom_sched_t *s)
{
    size_t old_cap = s->ready_cap;
    size_t new_cap = old_cap ? old_cap * 2 : 64;
    runloom_g_t **new_ring = (runloom_g_t **)PyMem_Calloc(new_cap, sizeof(runloom_g_t *));
    size_t i, head = s->ready_head, tail = s->ready_tail;
    if (new_ring == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    /* Copy from the old ring, preserving order.  Old has tail-head
     * entries starting at (head & old_mask). */
    for (i = 0; i < (tail - head); i++) {
        new_ring[i] = s->ready_ring[(head + i) & s->ready_mask];
    }
    PyMem_Free(s->ready_ring);
    s->ready_ring = new_ring;
    s->ready_cap = new_cap;
    s->ready_mask = new_cap - 1;
    s->ready_head = 0;
    s->ready_tail = tail - head;
    return 0;
}

void runloom_sched_ready_push(runloom_sched_t *s, runloom_g_t *g)
{
    if (__builtin_expect(s->ready_tail - s->ready_head >= s->ready_cap, 0)) {
        if (runloom_ready_grow(s) < 0) return;     /* OOM: drop g on the floor;
                                                 * caller checks PyErr */
    }
    s->ready_ring[s->ready_tail & s->ready_mask] = g;
    s->ready_tail++;
}

/* ====================================================================
 * PCT -- Probabilistic Concurrency Testing (controlled scheduler)
 *
 * Burckhardt, Kothari, Musuvathi, Nagarakatte, "A Randomized Scheduler with
 * Probabilistic Guarantees of Finding Bugs", ASPLOS 2010.  When RUNLOOM_PCT_SEED
 * is set, the single-hub ready-pop stops being FIFO and instead always runs
 * the highest-priority ready goroutine; goroutines get random priorities and
 * d-1 random "priority change points" demote a goroutine mid-run.  This gives
 * a probabilistic lower bound of finding any depth-d bug.
 *
 * SCOPE: meaningful ONLY for the single-hub cooperative run() path, where the
 * ready-pop order IS the entire schedule.  It does NOT control the M:N hubs
 * (those are real parallel OS threads -- see the RUNLOOM_MN_SEED baton in
 * mn_sched_hub_resume_preempt.c.inc).  Zero cost when the env var is unset.
 *
 * Env: RUNLOOM_PCT_SEED (enable + seed), RUNLOOM_PCT_DEPTH (d, default 3),
 *      RUNLOOM_PCT_STEPS (estimated total scheduling steps k, default 2000),
 *      RUNLOOM_PCT_DEBUG (print the chosen change points once).
 * ==================================================================== */
#define PCT_MAX_DEPTH 16
typedef struct {
    int      inited;
    int      enabled;
    uint64_t rng;
    int      depth;
    long     steps;
    long     change_at[PCT_MAX_DEPTH];   /* d-1 ascending step indices */
    int      next_change;
    int      lo_ctr;                     /* assigns distinct low (demoted) prios */
} runloom_pct_t;
static runloom_pct_t runloom_pct;

static uint64_t runloom_pct_rand(void)      /* xorshift64 */
{
    uint64_t x = runloom_pct.rng;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    runloom_pct.rng = x;
    return x;
}

static void runloom_pct_init(void)
{
    const char *seed = getenv("RUNLOOM_PCT_SEED");
    int i;
    long k;
    runloom_pct.inited = 1;
    if (seed == NULL || seed[0] == '\0') { runloom_pct.enabled = 0; return; }
    runloom_pct.enabled = 1;
    runloom_pct.rng = strtoull(seed, NULL, 10);
    if (runloom_pct.rng == 0) runloom_pct.rng = 0x9E3779B97F4A7C15ULL;  /* xorshift fixpoint */
    runloom_pct.depth = getenv("RUNLOOM_PCT_DEPTH") ? atoi(getenv("RUNLOOM_PCT_DEPTH")) : 3;
    if (runloom_pct.depth < 1) runloom_pct.depth = 1;
    if (runloom_pct.depth > PCT_MAX_DEPTH) runloom_pct.depth = PCT_MAX_DEPTH;
    k = getenv("RUNLOOM_PCT_STEPS") ? atol(getenv("RUNLOOM_PCT_STEPS")) : 2000;
    if (k < 1) k = 1;
    for (i = 0; i < runloom_pct.depth - 1; i++)
        runloom_pct.change_at[i] = 1 + (long)(runloom_pct_rand() % (uint64_t)k);
    for (i = 1; i < runloom_pct.depth - 1; i++) {   /* insertion sort ascending */
        long v = runloom_pct.change_at[i]; int j = i - 1;
        while (j >= 0 && runloom_pct.change_at[j] > v) {
            runloom_pct.change_at[j + 1] = runloom_pct.change_at[j]; j--;
        }
        runloom_pct.change_at[j + 1] = v;
    }
    runloom_pct.steps = 0;
    runloom_pct.next_change = 0;
    runloom_pct.lo_ctr = 0;
    if (getenv("RUNLOOM_PCT_DEBUG")) {
        fprintf(stderr, "[pct] seed=%s depth=%d steps<=%ld change_at=",
                seed, runloom_pct.depth, k);
        for (i = 0; i < runloom_pct.depth - 1; i++)
            fprintf(stderr, "%ld%s", runloom_pct.change_at[i],
                    i + 1 < runloom_pct.depth - 1 ? "," : "\n");
        if (runloom_pct.depth <= 1) fprintf(stderr, "(none)\n");
    }
}

/* Find the logical ring index of the highest-priority ready g, assigning a
 * fresh random high priority to any g that doesn't have one yet.
 *
 * FIFO goroutines (pct_fifo, the aio bridge's call_soon/task gs) must keep their
 * relative spawn order: only the OLDEST ready FIFO g is a candidate; younger
 * ones are skipped until it has run.  So PCT may still interleave reorderable
 * (raw) gs freely and may even defer the oldest FIFO g behind one, but it can
 * never run a later call_soon callback before an earlier one -- honouring
 * asyncio's call_soon-FIFO contract while still exploring raw concurrency. */
static size_t runloom_pct_argmax(runloom_sched_t *s)
{
    size_t p, best = s->ready_head;
    int best_prio = -2147483647 - 1;
    int seen_fifo = 0;
    for (p = s->ready_head; p != s->ready_tail; p++) {
        runloom_g_t *cg = s->ready_ring[p & s->ready_mask];
        if (cg->pct_fifo) {
            if (seen_fifo) continue;     /* keep FIFO gs in spawn order */
            seen_fifo = 1;
        }
        if (cg->pct_prio == 0)   /* unassigned -> random high priority */
            cg->pct_prio = 0x40000000 + (int)(runloom_pct_rand() & 0x3FFFFFFF);
        if (cg->pct_prio > best_prio) { best_prio = cg->pct_prio; best = p; }
    }
    return best;
}

static runloom_g_t *runloom_pct_pick(runloom_sched_t *s)
{
    size_t best, p;
    runloom_g_t *g;
    runloom_pct.steps++;
    best = runloom_pct_argmax(s);
    /* change point: demote the would-be winner below everyone, then re-pick,
     * so the schedule diverges at this step (one priority inversion). */
    if (runloom_pct.next_change < runloom_pct.depth - 1 &&
        runloom_pct.steps == runloom_pct.change_at[runloom_pct.next_change]) {
        s->ready_ring[best & s->ready_mask]->pct_prio = -(++runloom_pct.lo_ctr);
        runloom_pct.next_change++;
        best = runloom_pct_argmax(s);
    }
    g = s->ready_ring[best & s->ready_mask];
    /* remove logical slot `best` by shifting the rest of the queue down one */
    for (p = best; p + 1 != s->ready_tail; p++)
        s->ready_ring[p & s->ready_mask] = s->ready_ring[(p + 1) & s->ready_mask];
    s->ready_tail--;
    return g;
}

runloom_g_t *runloom_sched_ready_pop(runloom_sched_t *s)
{
    runloom_g_t *g;
    if (s->ready_head == s->ready_tail) return NULL;
    if (__builtin_expect(runloom_pct.enabled, 0))
        return runloom_pct_pick(s);
    g = s->ready_ring[s->ready_head & s->ready_mask];
    s->ready_head++;
    /* Note: we deliberately do NOT clear g->next here -- the linked-
     * list `next` field is now repurposed by the g_slab as the free-
     * list link and by the M:N submission list.  Whichever consumer
     * uses it next overwrites the slot. */
    return g;
}

#define runloom_ready_push runloom_sched_ready_push
#define runloom_ready_pop  runloom_sched_ready_pop

/* ---- Sleep heap (min-heap by (wake_at, sleep_seq)) ----
 * The sleep_seq tiebreak makes equal-deadline sleepers wake in FIFO insertion
 * order, matching asyncio's (when, seq) TimerHandle ordering.  Without it,
 * two coros sleeping the same duration but started a tick apart could wake in
 * the wrong relative order (observed breaking equal-timeout races, e.g.
 * hypercorn's lifespan startup-timeout test). */
static inline int runloom_sleep_before(const runloom_g_t *a, const runloom_g_t *b)
{
    if (a->wake_at < b->wake_at) return 1;
    if (a->wake_at > b->wake_at) return 0;
    return a->sleep_seq < b->sleep_seq;
}

static int runloom_sleep_grow(runloom_sched_t *s)
{
    Py_ssize_t new_cap = s->sleep_cap ? s->sleep_cap * 2 : 16;
    runloom_g_t **new_heap = (runloom_g_t **)PyMem_Realloc(
        s->sleep_heap, sizeof(runloom_g_t *) * (size_t)(new_cap + 1));
    if (new_heap == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    s->sleep_heap = new_heap;
    s->sleep_cap = new_cap;
    return 0;
}

static int runloom_sleep_push(runloom_sched_t *s, runloom_g_t *g)
{
    Py_ssize_t i;
    if (s->sleep_size + 1 > s->sleep_cap) {
        if (runloom_sleep_grow(s) < 0) return -1;
    }
    s->sleep_size++;
    i = s->sleep_size;
    /* Assign the FIFO tiebreak sequence at push time. */
    g->sleep_seq = s->sleep_seq_ctr++;
    s->sleep_heap[i] = g;
    /* sift up: move g toward the root while it is earlier than its parent */
    while (i > 1 && runloom_sleep_before(g, s->sleep_heap[i / 2])) {
        s->sleep_heap[i] = s->sleep_heap[i / 2];
        i /= 2;
    }
    s->sleep_heap[i] = g;
    return 0;
}

runloom_g_t *runloom_sched_sleep_peek(runloom_sched_t *s)
{
    if (s->sleep_size == 0) return NULL;
    return s->sleep_heap[1];
}

runloom_g_t *runloom_sched_sleep_pop(runloom_sched_t *s)
{
    runloom_g_t *top;
    runloom_g_t *last;
    Py_ssize_t i, child;
    if (s->sleep_size == 0) return NULL;
    top = s->sleep_heap[1];
    last = s->sleep_heap[s->sleep_size];
    s->sleep_size--;
    if (s->sleep_size == 0) return top;
    i = 1;
    while (1) {
        child = i * 2;
        if (child > s->sleep_size) break;
        if (child + 1 <= s->sleep_size &&
            runloom_sleep_before(s->sleep_heap[child + 1], s->sleep_heap[child])) {
            child++;
        }
        /* stop once `last` is no later than the smaller child */
        if (!runloom_sleep_before(s->sleep_heap[child], last)) break;
        s->sleep_heap[i] = s->sleep_heap[child];
        i = child;
    }
    s->sleep_heap[i] = last;
    return top;
}

#define runloom_sleep_peek runloom_sched_sleep_peek
#define runloom_sleep_pop  runloom_sched_sleep_pop

/* ---- Timer heap (min-heap by deadline) -- in-memory TIMED parks ----
 * By-VALUE entries { deadline, g } (no g->wake_at reuse, so a g may hold several
 * stale entries across re-parks).  Drained alongside the sleep heap; the drain
 * fires a due entry by CASing the g's parked_safe (the same exactly-once arbiter
 * as wake_safe).  See runloom_park_generic_timed. */
static int runloom_timer_grow(runloom_sched_t *s)
{
    Py_ssize_t new_cap = s->timer_cap ? s->timer_cap * 2 : 16;
    runloom_timer_entry_t *nh = (runloom_timer_entry_t *)PyMem_Realloc(
        s->timer_heap, sizeof(runloom_timer_entry_t) * (size_t)(new_cap + 1));
    if (nh == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    s->timer_heap = nh;
    s->timer_cap = new_cap;
    return 0;
}

static int runloom_timer_push(runloom_sched_t *s, double deadline, runloom_g_t *g)
{
    Py_ssize_t i;
    runloom_timer_entry_t e;
    if (s->timer_size + 1 > s->timer_cap) {
        if (runloom_timer_grow(s) < 0) return -1;
    }
    e.deadline = deadline;
    e.g = g;
    s->timer_size++;
    i = s->timer_size;
    while (i > 1 && s->timer_heap[i / 2].deadline > deadline) {  /* sift up */
        s->timer_heap[i] = s->timer_heap[i / 2];
        i /= 2;
    }
    s->timer_heap[i] = e;
    return 0;
}

/* Peek the earliest deadline (valid until the next push/pop), or NULL if empty. */
static runloom_timer_entry_t *runloom_timer_peek(runloom_sched_t *s)
{
    if (s->timer_size == 0) return NULL;
    return &s->timer_heap[1];
}

/* Pop the earliest entry into *out; returns 1 if popped, 0 if the heap is empty. */
static int runloom_timer_pop(runloom_sched_t *s, runloom_timer_entry_t *out)
{
    runloom_timer_entry_t last;
    Py_ssize_t i, child;
    if (s->timer_size == 0) return 0;
    *out = s->timer_heap[1];
    last = s->timer_heap[s->timer_size];
    s->timer_size--;
    if (s->timer_size == 0) return 1;
    i = 1;
    while (1) {                                                 /* sift down */
        child = i * 2;
        if (child > s->timer_size) break;
        if (child + 1 <= s->timer_size &&
            s->timer_heap[child + 1].deadline < s->timer_heap[child].deadline) {
            child++;
        }
        if (s->timer_heap[child].deadline >= last.deadline) break;
        s->timer_heap[i] = s->timer_heap[child];
        i = child;
    }
    s->timer_heap[i] = last;
    return 1;
}

/* Earliest timer deadline (monotonic seconds) on s, or -1.0 if no timers.
 * Used by the drain / hub idle-wait to bound the block so a due timer fires. */
double runloom_sched_timer_next_deadline(runloom_sched_t *s)
{
    if (s->timer_size == 0) return -1.0;
    return s->timer_heap[1].deadline;
}

/* ---- Scheduler lifecycle ---- */
/* Phase C: per-thread schedulers (see runloom_sched_get below).  runloom_cal_default
 * holds the calibrated default stack size that each new thread's sched is
 * initialised with. */

/* ---- Stack calibration ----
 *
 * Default: 512 KB.  This covers everything a goroutine realistically does --
 * the deepest stdlib single frame (`_decimal` squaretrans_pow2, 256 KB), full
 * TLS/SSH crypto in a callback (the aio bridge runs asyncssh's kex + OpenSSL on
 * 512 KB), nested parsers -- so ordinary code never hits the guard page.  It's
 * cheap: stacks are demand-paged (the unused tail consumes no physical RAM until
 * touched -- VM, not RSS, verified on Linux/macOS mmap and Windows CreateFiberEx
 * reserve), so the cost is virtual address space, not memory.  Calibration adapts
 * UP for stack-hungry programs (freeze to next_pow2(max_hwm * RUNLOOM_CAL_SAFETY)
 * after RUNLOOM_CAL_TARGET completions) but is FLOORED at this default -- it never
 * auto-shrinks below the safe size and re-exposes the deep cases.  Reclaiming the
 * tail for huge goroutine counts is the per-kind auto-sizer's job (opt-in
 * "grow down"; runloom.inspect.enable_stack_autosize), not calibration's.  A true
 * outlier deeper than 512 KB is a clean guard-page crash + a one-line pin
 * (runloom.go(fn, stack_size=...)), never corruption.
 *
 * Reads/writes are plain (single-threaded scheduler in v0); when
 * Phase C arrives these will move under the scheduler lock. */
#define RUNLOOM_DEFAULT_STACK_SIZE   (512 * 1024)
#define RUNLOOM_MIN_STACK_SIZE       (16  * 1024)   /* 3.13t hard floor (explicit override) */
#define RUNLOOM_MAX_STACK_SIZE       (8   * 1024 * 1024)
#define RUNLOOM_CAL_TARGET           1000     /* gs before freeze */
#define RUNLOOM_CAL_SAFETY           4        /* multiply HWM by this */

static size_t    runloom_cal_default = RUNLOOM_DEFAULT_STACK_SIZE;
static size_t    runloom_cal_max_hwm = 0;
static long long runloom_cal_completed = 0;
