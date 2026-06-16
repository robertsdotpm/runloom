/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

int runloom_netpoll_drain_parked(void);

/* ---- parker heap pool ----
 *
 * Replaces stack-allocated `runloom_parked_t park;` locals.  Each calling
 * thread keeps a small LIFO freelist of released parkers; acquire pops
 * from there or mallocs, release pushes back (capped, then free).
 *
 * Why heap, not stack: stack-allocated parkers shared the address
 * space of the fiber's coroutine stack.  Stacks are returned to
 * a TLS pool on g completion and reissued to the next g, so a missed
 * unlink path leaves the global / per-fd structures pointing at a
 * byte-identical address the new occupant just claimed.  Heap-pool
 * parkers cannot alias: when a parker is in the freelist, no global
 * pointer references it; when it's in flight, it sits at a unique
 * heap address.
 *
 * Generation: each acquire bumps p->gen.  Pure observability hook;
 * lookup paths still walk by pointer.  Recorded in the diag ring so
 * a future missed-unlink can be triangulated by gen mismatch. */
#define RUNLOOM_PARKER_POOL_CAP 64
static RUNLOOM_TLS runloom_parked_t *runloom_parker_pool_head = NULL;
static RUNLOOM_TLS int             runloom_parker_pool_size = 0;
/* Monotonic generation source.  Atomic so cross-thread acquires
 * (released-by-one-thread, acquired-by-another via pool transfer)
 * still get a unique bump, though typical usage is hub-local. */
static unsigned int runloom_parker_gen_next = 0;

static runloom_parked_t *runloom_parker_pool_acquire(void)
{
    runloom_parked_t *p = runloom_parker_pool_head;
    if (p != NULL) {
        runloom_parker_pool_head = p->pool_next;
        runloom_parker_pool_size--;
        p->pool_next = NULL;
    } else {
        p = (runloom_parked_t *)calloc(1, sizeof(*p));
        if (p == NULL) return NULL;
    }
    p->heap_index = -1;
    p->park_ts = 0;
    p->reclaimed = 0;
    /* Fresh park: not yet committed, not yet woken.  Plain store is
     * safe -- the parker is not linked, so no pump can see it. */
    p->commit = RUNLOOM_PARK_ARMED;
    /* Mint a fresh generation.  Wraparound after 2^32 is fine: the
     * gen is a diag aid, not a security token. */
    p->gen = __atomic_add_fetch(&runloom_parker_gen_next, 1, __ATOMIC_RELAXED);
    return p;
}

static void runloom_parker_pool_release(runloom_parked_t *p)
{
    if (p == NULL) return;
    /* Defence-in-depth: scrub list-link fields so a stray ref into
     * a freelist entry can't pivot through to other state. */
    p->next        = NULL;
    p->slot        = NULL;
    p->next_by_fd  = NULL;
    p->prev_by_fd  = NULL;
    p->fd          = -1;
    p->events      = 0;
    p->deadline_ns = -1;
    p->ready_out   = NULL;
    p->g           = NULL;
    p->hub         = NULL;
    p->heap_index  = -1;
    if (runloom_parker_pool_size >= RUNLOOM_PARKER_POOL_CAP) {
        free(p);
        return;
    }
    p->pool_next = runloom_parker_pool_head;
    runloom_parker_pool_head = p;
    runloom_parker_pool_size++;
}


/* ---- parker pool ----
 *
 * Groups every piece of state that protects parker lifetime under one
 * lock + one cache-line-locality region.  Phase A (this commit) carves
 * the state out of file-static globals into a single struct instance;
 * call sites take a `runloom_parker_pool_t *pool` parameter that they
 * thread through to the helpers.  Phase B will allocate N+1 instances
 * (one per M:N hub plus one for the single-thread sched) and route by
 * the parker's owning hub, dropping the contended global lock from
 * the hot path.  All call sites that pass &runloom_pool today are the
 * routing seams to update then.
 *
 * Members:
 *   lock / lock_inited        - exclusive mutex over the rest; lazy init
 *   head                      - global parker linked list (slot-pointer
 *                               trick for O(1) unlink); used by self-
 *                               check, drain_parked
 *   total                     - atomic count of linked parkers; read by
 *                               the sched/hub idle loops to gate pump
 *   by_fd / by_fd_cap         - sparse array fd -> head-of-bucket; the
 *                               hot path for epoll-event dispatch
 *   dh_arr / dh_size / dh_cap - min-heap of parkers keyed by deadline;
 *                               peek/remove in O(log N)
 *
 * State that stays GLOBAL (not in the pool, so all pools share one
 * copy under Phase B):
 *   runloom_fd_registered_bm        - the kernel epoll/kqueue ADD bit per
 *                                  fd; process-wide truth, not parker-
 *                                  owned
 *   runloom_fd_pending_wake         - cross-thread "edge fired before any
 *                                  parker was visible" bitmap; needs
 *                                  to be readable by any pump
 *   runloom_epoll_fd / runloom_kqueue  - single shared backend handle
 *   runloom_iouring_ring_*          - per-ring eventfd routing in the
 *                                  shared epoll
 *
 * The registration bitmap's read-then-set sequence runs under the
 * default pool's lock (runloom_pool.lock); the pending-wake bitmap is
 * lock-free.  Both arrays are preallocated once to the fd hard limit
 * (runloom_fd_arrays_init) and never realloc'd, so their base pointers
 * are stable for the process lifetime. */
typedef struct runloom_parker_pool {
    runloom_mutex_t lock;
    volatile long lock_inited;          /* 0 = not yet, 1 = in flight, 2 = ready */

    /* Per-hub kqueue backend (BSD/macOS).  Each hub polls its OWN kqueue +
     * self-pipe so register/collect/wake never serialise across hubs on the
     * kernel's single per-kqueue lock -- the shared-kqueue kq_lock contention
     * that collapsed connect throughput super-linearly with hub count.  Created
     * lazily in runloom_parker_pool_lock_ensure_inited (kqueue builds only);
     * -1 until then.  Unused on epoll/Windows/select (one shared backend handle
     * + the global self-pipe).  Load-balancing (a busy hub draining its own
     * events) is restored by hub_main's PERIODIC non-blocking self-pump. */
    int kqueue_fd;                      /* this pool's kqueue; -1 = none */
    int selfpipe_r;                     /* pump-wake self-pipe read end (in kqueue) */
    int selfpipe_w;                     /* pump-wake self-pipe write end */

    /* Per-hub epoll backend (Linux), gated on RUNLOOM_PERHUB_EPOLL.  Mirrors the
     * per-hub kqueue above: each hub polls its OWN epoll + wake eventfd so the
     * kernel ep->lock and the wake-eventfd thundering herd never serialise across
     * hubs (Axis B / R1+R2; see docs/dev/PERHUB_EPOLL.md).  Created lazily in
     * runloom_pool_backend_create; -1 until then.  Unused (the shared
     * runloom_epoll_fd path stays live) when the flag is off. */
    int epoll_fd;                       /* this pool's epoll; -1 = none */
    int wake_efd;                       /* pump-wake eventfd, registered in epoll_fd */

    runloom_parked_t *head;
    int total;                          /* read with __atomic_load_n */

    runloom_parked_t **by_fd;
    size_t          by_fd_cap;

    runloom_parked_t **dh_arr;
    int             dh_size;
    int             dh_cap;

    /* Stack-reclaim sweep cursor (RUNLOOM_STACK_PARK_SWEEP).  Persists
     * across sweeps so each sweep examines a bounded window of the
     * (head-insert, newest-first) list instead of walking it whole
     * under the lock; over successive sweeps the cursor covers the
     * tail (oldest, most-reclaimable) too.  Validated by the parker's
     * gen on resume so a unlinked/reused cursor falls back to head. */
    runloom_parked_t  *sweep_cursor;
    unsigned int    sweep_cursor_gen;

    /* Churn throttle (RUNLOOM_SWEEP_MAX_CHURN).  See runloom_netpoll_sweep_idle:
     * the sweep skips its walk when this pool's recent arrival (link)
     * rate is high, because perf showed the active-churn tail-latency
     * cost is lock contention between the sweep walk and the wake path
     * on pool->lock -- not refault -- and a fast-churning pool has little
     * genuinely-long-idle to reclaim anyway.  link_count is bumped in
     * runloom_parker_link; all three are touched only by the owning hub
     * (link + that hub's sweep), always under pool->lock, so no atomics. */
    unsigned long long link_count;        /* cumulative parker arrivals */
    unsigned long long churn_last_links;  /* link_count at last churn eval */
    long long          churn_last_ns;     /* monotonic ns at last churn eval */
} runloom_parker_pool_t;

/* One parker pool per M:N hub plus one for the single-thread sched.
 * Capped at 64 hubs (matches RUNLOOM_IOURING_RINGS_MAX) -- machines with
 * more cores than that fall back to the default pool for the overflow
 * hubs, which is correct but loses the per-hub locality benefit for
 * those hubs.  RUNLOOM_PARKER_POOL_DEFAULT is the index for parkers
 * outside any hub (single-thread sched on main thread, M:N-disabled
 * paths). */
#define RUNLOOM_PARKER_POOL_HUBS    64
#define RUNLOOM_PARKER_POOL_DEFAULT RUNLOOM_PARKER_POOL_HUBS
#define RUNLOOM_PARKER_POOL_MAX     (RUNLOOM_PARKER_POOL_HUBS + 1)
static runloom_parker_pool_t runloom_pools[RUNLOOM_PARKER_POOL_MAX];

/* Convenience pointer to the default pool.  Used by paths that don't
 * have a fiber context handy (e.g., backend init, drain_parked
 * iteration uses the array directly). */
#define runloom_pool (runloom_pools[RUNLOOM_PARKER_POOL_DEFAULT])

/* Map a parker's hub field (or the current thread's hub) to a pool.
 * Same dispatch lives in both forms because the choice happens once
 * at park time (current thread's hub -> pool stored implicitly via
 * by_fd/head ownership), and again at unlink/wake time (parker's
 * recorded hub -> the same pool).
 *
 * Per-hub routing is gated on the kernel-backed by-fd backends
 * (epoll, kqueue, Windows IOCP -- all driven through
 * runloom_pump_dispatch_event which iterates every pool's by_fd).  The
 * WSAPoll / select fallbacks still walk pool->head + pool->by_fd
 * directly and would miss parkers in non-default pools; on those
 * platforms we force every parker into the default pool so the old
 * single-list semantics are preserved.  IOCP-availability on Windows
 * is detected at runtime, so the gate runs at call time as well. */
static runloom_parker_pool_t *runloom_parker_pool_for_hub(void *hub_opaque)
{
#if defined(RUNLOOM_HAVE_EPOLL) || defined(RUNLOOM_HAVE_KQUEUE)
    if (hub_opaque != NULL) {
        int id = runloom_mn_hub_id_of(hub_opaque);
        if (id >= 0 && id < RUNLOOM_PARKER_POOL_HUBS) {
            return &runloom_pools[id];
        }
    }
#else
    /* Windows WSAPoll / select POSIX fallback walk pool->head and
     * pool->by_fd[fd] directly to build the fdset, so a parker in a
     * non-default pool would be invisible to them.  Route everything
     * to the default pool on those backends.  IOCP on Windows uses
     * runloom_pump_dispatch_event (which iterates pools) and would
     * benefit from per-hub routing, but the gate is runtime-only
     * (runloom_win_use_iocp), kept conservative here for now. */
    (void)hub_opaque;
#endif
    return &runloom_pools[RUNLOOM_PARKER_POOL_DEFAULT];
}

/* RUNLOOM_PERHUB_EPOLL: select the per-hub epoll backend (each hub polls its own
 * epoll + wake eventfd) over the single shared runloom_epoll_fd.  Axis B / R1+R2;
 * see docs/dev/PERHUB_EPOLL.md.  Resolved once; DEFAULT ON -- a measured +17-28%
 * saturation win (external loadgen -> isolated server_runloom).
 * RUNLOOM_PERHUB_EPOLL=0 forces the legacy shared epoll.  Always 0 off the epoll
 * backend. */
static int runloom_perhub_epoll(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    /* Resolved-once cache, read on the hot path by every hub -> the lazy init
     * must be atomic (RELAXED is enough: the value is idempotent, so a racing
     * recompute is harmless; we only need a non-torn read/write).  Matches
     * runloom_netpoll_dbg_stale_check's pattern. */
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_PERHUB_EPOLL");
        /* default ON; only an explicit "0" disables. */
        cur = (e != NULL && e[0] == '0' && e[1] == '\0') ? 0 : 1;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
#else
    return 0;
#endif
}

static int runloom_netpoll_inited = 0;

/* ---- deadline min-heap ----
 *
 * Indexing: 0-based array; parent = (i-1)/2, children = 2i+1/2i+2.
 * Each parker stores its own index in p->heap_index for O(log N)
 * arbitrary remove via sift-up + sift-down.  Parkers with
 * deadline_ns < 0 are never in the heap; heap_index stays -1.
 * Caller holds pool->lock for all ops below. */
static int runloom_dh_grow(runloom_parker_pool_t *pool)
{
    int newcap = pool->dh_cap ? pool->dh_cap * 2 : 64;
    runloom_parked_t **na = (runloom_parked_t **)realloc(
        pool->dh_arr, (size_t)newcap * sizeof(*na));
    if (na == NULL) return -1;
    pool->dh_arr = na;
    pool->dh_cap = newcap;
    return 0;
}

static void runloom_dh_swap(runloom_parker_pool_t *pool, int i, int j)
{
    runloom_parked_t *t = pool->dh_arr[i];
    pool->dh_arr[i] = pool->dh_arr[j];
    pool->dh_arr[j] = t;
    pool->dh_arr[i]->heap_index = i;
    pool->dh_arr[j]->heap_index = j;
}

static void runloom_dh_sift_up(runloom_parker_pool_t *pool, int i)
{
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (pool->dh_arr[i]->deadline_ns >= pool->dh_arr[parent]->deadline_ns)
            break;
        runloom_dh_swap(pool, i, parent);
        i = parent;
    }
}

static void runloom_dh_sift_down(runloom_parker_pool_t *pool, int i)
{
    int n = pool->dh_size;
    while (1) {
        int l = 2 * i + 1, r = 2 * i + 2, best = i;
        if (l < n && pool->dh_arr[l]->deadline_ns < pool->dh_arr[best]->deadline_ns)
            best = l;
        if (r < n && pool->dh_arr[r]->deadline_ns < pool->dh_arr[best]->deadline_ns)
            best = r;
        if (best == i) break;
        runloom_dh_swap(pool, i, best);
        i = best;
    }
}

/* Insert p into pool's heap.  No-op if p has no deadline
 * (deadline_ns < 0) or is already in the heap. */
static void runloom_dh_insert(runloom_parker_pool_t *pool, runloom_parked_t *p)
{
    if (p->deadline_ns < 0 || p->heap_index >= 0) return;
    if (pool->dh_size >= pool->dh_cap) {
        if (runloom_dh_grow(pool) != 0) return;   /* heap stays consistent; insert dropped */
    }
    pool->dh_arr[pool->dh_size] = p;
    p->heap_index = pool->dh_size;
    pool->dh_size++;
    runloom_dh_sift_up(pool, p->heap_index);
}

/* Remove p from pool's heap if present. */
static void runloom_dh_remove(runloom_parker_pool_t *pool, runloom_parked_t *p)
{
    int i = p->heap_index;
    if (i < 0 || i >= pool->dh_size) return;
    p->heap_index = -1;
    pool->dh_size--;
    if (i == pool->dh_size) return;          /* removed the tail */
    pool->dh_arr[i] = pool->dh_arr[pool->dh_size];
    pool->dh_arr[i]->heap_index = i;
    /* Could be either direction; try both. */
    runloom_dh_sift_up(pool, i);
    runloom_dh_sift_down(pool, i);
}

/* Peek earliest deadline; returns -1 if heap empty. */
static long long runloom_dh_peek_deadline(runloom_parker_pool_t *pool)
{
    if (pool->dh_size == 0) return -1;
    return pool->dh_arr[0]->deadline_ns;
}

/* ---- per-fd parker index ----
 * Sparse array indexed by fd; each slot holds the head of a doubly-
 * linked list of parkers interested in events on that fd (usually 1,
 * occasionally 2 for read+write).  Replaces the prior O(N) walk of
 * pool->head on every epoll event with an O(1) bucket lookup
 * + O(parkers-on-this-fd) walk.  At N=1024 concurrent conns this
 * changes the pump from O(N*events) to O(events).
 *
 * Lives in the parker pool; protected by pool->lock. */

static int runloom_parker_fd_index_ensure(runloom_parker_pool_t *pool, int fd)
{
    if (fd < 0) return -1;
    if ((size_t)fd < pool->by_fd_cap) return 0;
    {
        size_t newcap = pool->by_fd_cap ? pool->by_fd_cap * 2 : 256;
        runloom_parked_t **nb;
        while (newcap <= (size_t)fd) newcap *= 2;
        nb = (runloom_parked_t **)realloc(pool->by_fd,
                                       newcap * sizeof(*nb));
        if (nb == NULL) return -1;
        memset(nb + pool->by_fd_cap, 0,
               (newcap - pool->by_fd_cap) * sizeof(*nb));
        pool->by_fd     = nb;
        pool->by_fd_cap = newcap;
    }
    return 0;
}
