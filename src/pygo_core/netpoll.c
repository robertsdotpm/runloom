/* netpoll.c -- portable I/O multiplexing.
 *
 * Backends, picked by plat.h:
 *   Linux           -> epoll
 *   macOS / *BSD    -> kqueue
 *   Windows         -> WSAPoll (sockets) -- POSIX poll()-shaped Winsock API,
 *                      Vista+, no FD_SETSIZE cap.  IOCP would be more
 *                      efficient for the file-handle case but only sockets
 *                      flow through wait_fd today (regular files go to the
 *                      thread-pool backend in monkey.py).
 *   else            -> select() POSIX fallback
 *
 * Park mechanics:
 *   - the current goroutine snapshot tstate, gets pushed onto an internal
 *     "parked" list with (fd, events, deadline) metadata.  yields via
 *     pygo_coro_yield.
 *   - the scheduler's drain loop, when ready queue is empty, calls
 *     pygo_netpoll_pump(timeout) instead of sleeping the OS thread.
 *   - pump waits for I/O / timeout, wakes parked goroutines, returns.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "netpoll.h"
#include "coro.h"
#include "pygo_sched.h"
#include "mn_sched.h"
#include "io_uring.h"
#include "pygo_diag.h"
#include "pygo_gstate.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#if !defined(PYGO_OS_WINDOWS)
#  include <sys/resource.h>   /* getrlimit(RLIMIT_NOFILE) for fd-array sizing */
#endif

#if defined(PYGO_HAVE_EPOLL)
#  include <sys/epoll.h>
#  include <unistd.h>
#elif defined(PYGO_HAVE_KQUEUE)
#  include <sys/event.h>
#  include <sys/time.h>
#  include <unistd.h>
#elif defined(PYGO_OS_WINDOWS)
   /* winsock2.h, ws2tcpip.h and windows.h are already pulled in via
    * plat_compat.h.  WSAPoll + WSAPOLLFD live in winsock2.h, FD_SET /
    * FD_ISSET likewise -- no extra header needed here. */
#else
#  include <sys/select.h>
#  include <unistd.h>
#endif

/* ---- internal park record ----
 * Allocated on the parked goroutine's C stack inside pygo_netpoll_wait_fd
 * (the stack stays alive across yield because the goroutine isn't
 * destroyed until it completes).
 *
 * Two intrusive links:
 *   * `next` / `slot` into the global parked list (used by drain_parked
 *     and the timeout sweep).  Uses the "slot pointer" trick for O(1)
 *     unlink; the head pointer lives in a static, so its address is
 *     stable.
 *   * `next_by_fd` / `prev_by_fd` into the per-fd bucket (the hot path:
 *     epoll-event → matching parker → wake).  Doubly-linked because
 *     the bucket-head pointer lives inside a realloc-able heap array,
 *     and the slot-pointer trick can't survive a realloc that moves
 *     the array. */
typedef struct pygo_parked {
    int fd;
    int events;
    long long deadline_ns;     /* -1 = forever */
    int *ready_out;            /* where to store the wakeup mask */
    pygo_g_t *g;
    void *hub;                  /* M:N hub opaque; NULL = global sched */
    struct pygo_parked  *next;
    struct pygo_parked **slot;
    struct pygo_parked  *next_by_fd;
    struct pygo_parked  *prev_by_fd;
    /* Monotonic acquire generation (bumped each pool acquire).  When
     * the parker comes from the pool's freelist, the new gen
     * disambiguates this lifetime from prior ones with the same
     * heap address.  Pure defence-in-depth: pool acquire never
     * returns a parker that is currently linked into any list, so
     * the global / bucket pointers cannot alias a live entry.  The
     * gen field is exposed via the diag ring so a missed-unlink can
     * be traced to the lifetime that left the dangling reference. */
    unsigned int gen;
    /* Index into the deadline min-heap; -1 if not present (deadline
     * < 0 or never linked).  Maintained by the heap ops so unlink
     * can find and remove in O(log N). */
    int heap_index;
    /* TLS pool freelist linkage.  When released, the parker is on
     * its owning thread's freelist; pool_next chains them.  Cleared
     * on acquire. */
    struct pygo_parked *pool_next;
} pygo_parked_t;

/* Forcibly wake all parked goroutines with a cancelled marker.
 * Returns count of waiters woken.  Used by sched_reset() so paio.run
 * cleanup doesn't leave the next pygo_core.run() blocking on parked
 * accept loops / tickers / etc. */
int pygo_netpoll_drain_parked(void);

/* ---- parker heap pool ----
 *
 * Replaces stack-allocated `pygo_parked_t park;` locals.  Each calling
 * thread keeps a small LIFO freelist of released parkers; acquire pops
 * from there or mallocs, release pushes back (capped, then free).
 *
 * Why heap, not stack: stack-allocated parkers shared the address
 * space of the goroutine's coroutine stack.  Stacks are returned to
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
#define PYGO_PARKER_POOL_CAP 64
static PYGO_TLS pygo_parked_t *pygo_parker_pool_head = NULL;
static PYGO_TLS int             pygo_parker_pool_size = 0;
/* Monotonic generation source.  Atomic so cross-thread acquires
 * (released-by-one-thread, acquired-by-another via pool transfer)
 * still get a unique bump, though typical usage is hub-local. */
static unsigned int pygo_parker_gen_next = 0;

static pygo_parked_t *pygo_parker_pool_acquire(void)
{
    pygo_parked_t *p = pygo_parker_pool_head;
    if (p != NULL) {
        pygo_parker_pool_head = p->pool_next;
        pygo_parker_pool_size--;
        p->pool_next = NULL;
    } else {
        p = (pygo_parked_t *)calloc(1, sizeof(*p));
        if (p == NULL) return NULL;
    }
    p->heap_index = -1;
    /* Mint a fresh generation.  Wraparound after 2^32 is fine: the
     * gen is a diag aid, not a security token. */
    p->gen = __atomic_add_fetch(&pygo_parker_gen_next, 1, __ATOMIC_RELAXED);
    return p;
}

static void pygo_parker_pool_release(pygo_parked_t *p)
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
    if (pygo_parker_pool_size >= PYGO_PARKER_POOL_CAP) {
        free(p);
        return;
    }
    p->pool_next = pygo_parker_pool_head;
    pygo_parker_pool_head = p;
    pygo_parker_pool_size++;
}


/* ---- parker pool ----
 *
 * Groups every piece of state that protects parker lifetime under one
 * lock + one cache-line-locality region.  Phase A (this commit) carves
 * the state out of file-static globals into a single struct instance;
 * call sites take a `pygo_parker_pool_t *pool` parameter that they
 * thread through to the helpers.  Phase B will allocate N+1 instances
 * (one per M:N hub plus one for the single-thread sched) and route by
 * the parker's owning hub, dropping the contended global lock from
 * the hot path.  All call sites that pass &pygo_pool today are the
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
 *   pygo_fd_registered_bm        - the kernel epoll/kqueue ADD bit per
 *                                  fd; process-wide truth, not parker-
 *                                  owned
 *   pygo_fd_pending_wake         - cross-thread "edge fired before any
 *                                  parker was visible" bitmap; needs
 *                                  to be readable by any pump
 *   pygo_epoll_fd / pygo_kqueue  - single shared backend handle
 *   pygo_iouring_ring_*          - per-ring eventfd routing in the
 *                                  shared epoll
 *
 * The registration bitmap's read-then-set sequence runs under the
 * default pool's lock (pygo_pool.lock); the pending-wake bitmap is
 * lock-free.  Both arrays are preallocated once to the fd hard limit
 * (pygo_fd_arrays_init) and never realloc'd, so their base pointers
 * are stable for the process lifetime. */
typedef struct pygo_parker_pool {
    pygo_mutex_t lock;
    volatile long lock_inited;          /* 0 = not yet, 1 = in flight, 2 = ready */

    pygo_parked_t *head;
    int total;                          /* read with __atomic_load_n */

    pygo_parked_t **by_fd;
    size_t          by_fd_cap;

    pygo_parked_t **dh_arr;
    int             dh_size;
    int             dh_cap;
} pygo_parker_pool_t;

/* One parker pool per M:N hub plus one for the single-thread sched.
 * Capped at 64 hubs (matches PYGO_IOURING_RINGS_MAX) -- machines with
 * more cores than that fall back to the default pool for the overflow
 * hubs, which is correct but loses the per-hub locality benefit for
 * those hubs.  PYGO_PARKER_POOL_DEFAULT is the index for parkers
 * outside any hub (single-thread sched on main thread, M:N-disabled
 * paths). */
#define PYGO_PARKER_POOL_HUBS    64
#define PYGO_PARKER_POOL_DEFAULT PYGO_PARKER_POOL_HUBS
#define PYGO_PARKER_POOL_MAX     (PYGO_PARKER_POOL_HUBS + 1)
static pygo_parker_pool_t pygo_pools[PYGO_PARKER_POOL_MAX];

/* Convenience pointer to the default pool.  Used by paths that don't
 * have a goroutine context handy (e.g., backend init, drain_parked
 * iteration uses the array directly). */
#define pygo_pool (pygo_pools[PYGO_PARKER_POOL_DEFAULT])

/* Map a parker's hub field (or the current thread's hub) to a pool.
 * Same dispatch lives in both forms because the choice happens once
 * at park time (current thread's hub -> pool stored implicitly via
 * by_fd/head ownership), and again at unlink/wake time (parker's
 * recorded hub -> the same pool).
 *
 * Per-hub routing is gated on the kernel-backed by-fd backends
 * (epoll, kqueue, Windows IOCP -- all driven through
 * pygo_pump_dispatch_event which iterates every pool's by_fd).  The
 * WSAPoll / select fallbacks still walk pool->head + pool->by_fd
 * directly and would miss parkers in non-default pools; on those
 * platforms we force every parker into the default pool so the old
 * single-list semantics are preserved.  IOCP-availability on Windows
 * is detected at runtime, so the gate runs at call time as well. */
static pygo_parker_pool_t *pygo_parker_pool_for_hub(void *hub_opaque)
{
#if defined(PYGO_HAVE_EPOLL) || defined(PYGO_HAVE_KQUEUE)
    if (hub_opaque != NULL) {
        int id = pygo_mn_hub_id_of(hub_opaque);
        if (id >= 0 && id < PYGO_PARKER_POOL_HUBS) {
            return &pygo_pools[id];
        }
    }
#else
    /* Windows WSAPoll / select POSIX fallback walk pool->head and
     * pool->by_fd[fd] directly to build the fdset, so a parker in a
     * non-default pool would be invisible to them.  Route everything
     * to the default pool on those backends.  IOCP on Windows uses
     * pygo_pump_dispatch_event (which iterates pools) and would
     * benefit from per-hub routing, but the gate is runtime-only
     * (pygo_win_use_iocp), kept conservative here for now. */
    (void)hub_opaque;
#endif
    return &pygo_pools[PYGO_PARKER_POOL_DEFAULT];
}

static int pygo_netpoll_inited = 0;

/* ---- deadline min-heap ----
 *
 * Indexing: 0-based array; parent = (i-1)/2, children = 2i+1/2i+2.
 * Each parker stores its own index in p->heap_index for O(log N)
 * arbitrary remove via sift-up + sift-down.  Parkers with
 * deadline_ns < 0 are never in the heap; heap_index stays -1.
 * Caller holds pool->lock for all ops below. */
static int pygo_dh_grow(pygo_parker_pool_t *pool)
{
    int newcap = pool->dh_cap ? pool->dh_cap * 2 : 64;
    pygo_parked_t **na = (pygo_parked_t **)realloc(
        pool->dh_arr, (size_t)newcap * sizeof(*na));
    if (na == NULL) return -1;
    pool->dh_arr = na;
    pool->dh_cap = newcap;
    return 0;
}

static void pygo_dh_swap(pygo_parker_pool_t *pool, int i, int j)
{
    pygo_parked_t *t = pool->dh_arr[i];
    pool->dh_arr[i] = pool->dh_arr[j];
    pool->dh_arr[j] = t;
    pool->dh_arr[i]->heap_index = i;
    pool->dh_arr[j]->heap_index = j;
}

static void pygo_dh_sift_up(pygo_parker_pool_t *pool, int i)
{
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (pool->dh_arr[i]->deadline_ns >= pool->dh_arr[parent]->deadline_ns)
            break;
        pygo_dh_swap(pool, i, parent);
        i = parent;
    }
}

static void pygo_dh_sift_down(pygo_parker_pool_t *pool, int i)
{
    int n = pool->dh_size;
    while (1) {
        int l = 2 * i + 1, r = 2 * i + 2, best = i;
        if (l < n && pool->dh_arr[l]->deadline_ns < pool->dh_arr[best]->deadline_ns)
            best = l;
        if (r < n && pool->dh_arr[r]->deadline_ns < pool->dh_arr[best]->deadline_ns)
            best = r;
        if (best == i) break;
        pygo_dh_swap(pool, i, best);
        i = best;
    }
}

/* Insert p into pool's heap.  No-op if p has no deadline
 * (deadline_ns < 0) or is already in the heap. */
static void pygo_dh_insert(pygo_parker_pool_t *pool, pygo_parked_t *p)
{
    if (p->deadline_ns < 0 || p->heap_index >= 0) return;
    if (pool->dh_size >= pool->dh_cap) {
        if (pygo_dh_grow(pool) != 0) return;   /* heap stays consistent; insert dropped */
    }
    pool->dh_arr[pool->dh_size] = p;
    p->heap_index = pool->dh_size;
    pool->dh_size++;
    pygo_dh_sift_up(pool, p->heap_index);
}

/* Remove p from pool's heap if present. */
static void pygo_dh_remove(pygo_parker_pool_t *pool, pygo_parked_t *p)
{
    int i = p->heap_index;
    if (i < 0 || i >= pool->dh_size) return;
    p->heap_index = -1;
    pool->dh_size--;
    if (i == pool->dh_size) return;          /* removed the tail */
    pool->dh_arr[i] = pool->dh_arr[pool->dh_size];
    pool->dh_arr[i]->heap_index = i;
    /* Could be either direction; try both. */
    pygo_dh_sift_up(pool, i);
    pygo_dh_sift_down(pool, i);
}

/* Peek earliest deadline; returns -1 if heap empty. */
static long long pygo_dh_peek_deadline(pygo_parker_pool_t *pool)
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

static int pygo_parker_fd_index_ensure(pygo_parker_pool_t *pool, int fd)
{
    if (fd < 0) return -1;
    if ((size_t)fd < pool->by_fd_cap) return 0;
    {
        size_t newcap = pool->by_fd_cap ? pool->by_fd_cap * 2 : 256;
        pygo_parked_t **nb;
        while (newcap <= (size_t)fd) newcap *= 2;
        nb = (pygo_parked_t **)realloc(pool->by_fd,
                                       newcap * sizeof(*nb));
        if (nb == NULL) return -1;
        memset(nb + pool->by_fd_cap, 0,
               (newcap - pool->by_fd_cap) * sizeof(*nb));
        pool->by_fd     = nb;
        pool->by_fd_cap = newcap;
    }
    return 0;
}

/* Link p into both pool->head and pool's per-fd bucket.  Caller
 * holds pool->lock.
 *
 * Stack-pooling note: pygo_parked_t lives on the calling goroutine's
 * coroutine stack (see pygo_netpoll_wait_fd).  Stacks are returned to
 * a per-hub TLS pool when a g completes (pygo_stack_release) and
 * re-issued to the next g spawned on that hub.  The new g's wait_fd
 * places its parker at the SAME stack offset, so the parker address
 * is byte-identical to a previous occupant's.  All four list-link
 * fields are freshly zeroed before this call (in pygo_netpoll_wait_fd),
 * but pool->head and pool->by_fd[fd] can still reference this address
 * from a prior life if any unlink path for that previous occupant
 * missed (a residual M:N + free-threaded race that is not yet fully
 * isolated upstream).
 *
 * Detach any stale self-reference here before pushing.  Otherwise
 * p->next = pool->head sets p->next = p (1-cycle in the global
 * list), and head = pool->by_fd[p->fd] = p sets p->next_by_fd = p /
 * p->prev_by_fd = p (self-cycle in the bucket).  Either form wedges
 * the pump's list walks indefinitely. */
static void pygo_parker_link(pygo_parker_pool_t *pool, pygo_parked_t *p)
{
    /* Stale-reference clears.  See header comment.  Cheap (two
     * compare-and-conditional-store); only fires when stack reuse hits
     * a parker address that an unlink missed. */
    if (pool->head == p) {
        pool->head = NULL;
        PYGO_EVT(PYGO_EVT_PARKER_GHOST, p, NULL, (long long)p->fd);
    }
    if (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap &&
        pool->by_fd[p->fd] == p) {
        pool->by_fd[p->fd] = NULL;
        PYGO_EVT(PYGO_EVT_PARKER_GHOST, p, (void *)(uintptr_t)1, (long long)p->fd);
    }
#ifdef PYGO_PARKER_DEBUG
    /* Diagnostic: announce ghost references so we can pinpoint the
     * upstream missed-unlink path. */
    if (p->slot != NULL || p->next != NULL) {
        fprintf(stderr,
                "[pygo] parker has nonnull slot/next at link entry: "
                "parker=%p fd=%d g=%p slot=%p next=%p\n",
                (void *)p, p->fd, (void *)p->g,
                (void *)p->slot, (void *)p->next);
    }
#endif
    /* Global list: push at head, slot-pointer trick. */
    p->next = pool->head;
    if (p->next != NULL) p->next->slot = &p->next;
    pool->head = p;
    p->slot = &pool->head;

    /* Per-fd bucket: push at head, doubly-linked.  If the realloc
     * failed we still keep the parker on the global list (slow path
     * walks the global list to find it); subsequent epoll events on
     * this fd just won't find it through the fast path. */
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
    if (p->fd >= 0 && pygo_parker_fd_index_ensure(pool, p->fd) == 0) {
        pygo_parked_t *head = pool->by_fd[p->fd];
        p->next_by_fd = head;
        if (head != NULL) head->prev_by_fd = p;
        pool->by_fd[p->fd] = p;
    }
    /* Deadline heap: insert if this parker has a finite deadline.
     * Pump now reads min-deadline in O(1) instead of walking the
     * global list. */
    pygo_dh_insert(pool, p);
    __atomic_add_fetch(&pool->total, 1, __ATOMIC_RELEASE);
    PYGO_EVT(PYGO_EVT_PARKER_LINK, p, p->g, (long long)p->fd);
}

/* Unlink p from both lists.  Caller holds pool->lock.  Returns 1
 * if p was actually removed from either list (caller "owns" the wake);
 * 0 if p was already fully unlinked (no-op, e.g. pump and pending-bits
 * path both raced to clean us up).  The counter is decremented exactly
 * once per actual removal.
 *
 * The counter previously lived at the call sites and had to be paired
 * by hand with each unlink invocation.  That broke under M:N: pump
 * could fire wake_g + set pending bits for the same fd if multiple
 * events arrived between wait_fd's link and pump's drain, and the
 * pending-bits path then called unlink+decrement on an already-cleaned
 * parker -- double-decrementing the counter and leaving the idle path
 * (which gates pump on parked_total > 0) sleeping while a real parker
 * sat in the list. */
static int pygo_parker_unlink(pygo_parker_pool_t *pool, pygo_parked_t *p)
{
    int touched = 0;
#ifdef PYGO_PARKER_DEBUG
    if (p->prev_by_fd == p || p->next_by_fd == p) {
        fprintf(stderr,
                "[pygo] UNLINK on self-looped bucket entry parker=%p fd=%d "
                "g=%p hub=%p prev_by_fd=%p next_by_fd=%p bucket=%p\n",
                (void *)p, p->fd, (void *)p->g, p->hub,
                (void *)p->prev_by_fd, (void *)p->next_by_fd,
                (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap)
                    ? (void *)pool->by_fd[p->fd] : NULL);
        p->prev_by_fd = NULL;
        p->next_by_fd = NULL;
    }
#endif
    if (p->slot != NULL) {
        *p->slot = p->next;
        if (p->next != NULL) p->next->slot = p->slot;
        p->slot = NULL;
        p->next = NULL;
        touched = 1;
    }
    /* Bucket cleanup.  Normally either prev_by_fd is set (we're in the
     * middle/tail of the chain) OR bucket[fd] == p (we're the head) --
     * never both.  We check both unconditionally so that if a prior
     * link or stale-reference cleanup left the structure in an
     * inconsistent state (e.g., bucket points to us but we also have a
     * predecessor from a different chain), we still leave nothing
     * behind that the pump's bucket walk could trip on. */
    if (p->prev_by_fd != NULL) {
        p->prev_by_fd->next_by_fd = p->next_by_fd;
        touched = 1;
    }
    if (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap &&
        pool->by_fd[p->fd] == p) {
        pool->by_fd[p->fd] = p->next_by_fd;
        touched = 1;
    }
    if (p->next_by_fd != NULL) p->next_by_fd->prev_by_fd = p->prev_by_fd;
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
    /* Heap remove is independent of list/bucket touch -- a parker
     * can be in the heap even if its other linkages were already
     * cleaned by a partial unlink. */
    if (p->heap_index >= 0) {
        pygo_dh_remove(pool, p);
        touched = 1;
    }
    if (touched) {
        __atomic_sub_fetch(&pool->total, 1, __ATOMIC_RELEASE);
        /* Clear the per-g back-pointer so g completion's force-unlink
         * (in mn_sched.c hub_main) doesn't see a stale reference. */
        if (p->g != NULL && p->g->netpoll_parker == p) {
            p->g->netpoll_parker = NULL;
        }
        PYGO_EVT(PYGO_EVT_PARKER_UNLINK, p, p->g, (long long)p->fd);
    }
    return touched;
}

static void pygo_parker_pool_lock_ensure_inited(pygo_parker_pool_t *pool);

/* ---- self-check inspection hook ----
 *
 * Called by pygo_self_check() in pygo_diag.c.  Walks pool->head
 * (Floyd cycle detection) and every per-fd bucket, fills in the stats
 * struct.  Takes pool->lock.
 *
 * The stats struct layout is declared in pygo_diag.c as a friend
 * (extern struct, no shared header) -- this keeps pygo_diag.h free of
 * netpoll-internal types. */
struct pygo_self_check_stats;
extern void pygo_self_check_stats_set(struct pygo_self_check_stats *out,
                                      int global_list_count,
                                      int global_list_cycle,
                                      int parked_total_atomic,
                                      int bucket_count_total,
                                      int bucket_self_loops,
                                      int bucket_unreachable);

int pygo_netpoll_inspect_for_self_check(struct pygo_self_check_stats *out)
{
    pygo_parker_pool_t *pool = &pygo_pool;
    int global_count = 0;
    int global_cycle = 0;
    int bucket_total = 0;
    int bucket_self  = 0;
    int bucket_unreach = 0;
    int parked_atomic;
    pygo_parked_t *slow, *fast;
    size_t i;

    if (!__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) {
        pygo_self_check_stats_set(out, 0, 0, 0, 0, 0, 0);
        return 0;
    }
    pygo_parker_pool_lock_ensure_inited(pool);
    pygo_mutex_lock(&pool->lock);

    /* Floyd cycle detection on the global list, with a safety cap. */
    slow = pool->head;
    fast = pool->head;
    {
        int iters = 0;
        const int CAP = 200000;
        while (fast != NULL && iters < CAP) {
            if (iters > 0) slow = slow ? slow->next : NULL;
            fast = fast->next;
            if (fast != NULL) fast = fast->next;
            if (iters > 0 && slow != NULL && slow == fast) {
                global_cycle = 1;
                break;
            }
            iters++;
        }
    }
    /* Linear walk for a count (after cycle check; if cycle present we
     * cap at CAP to avoid spinning here). */
    if (!global_cycle) {
        pygo_parked_t *p = pool->head;
        while (p != NULL && global_count < 200000) {
            global_count++;
            p = p->next;
        }
    } else {
        /* On cycle, just report the count we walked to before detecting. */
        pygo_parked_t *p = pool->head;
        int iters = 0;
        while (p != NULL && iters < 200000) {
            global_count++;
            p = p->next;
            iters++;
            if (iters > 100000) break;
        }
    }
    parked_atomic = __atomic_load_n(&pool->total, __ATOMIC_ACQUIRE);

    /* Walk every per-fd bucket. */
    if (pool->by_fd != NULL) {
        for (i = 0; i < pool->by_fd_cap; i++) {
            pygo_parked_t *p = pool->by_fd[i];
            int chain_iters = 0;
            while (p != NULL && chain_iters < 10000) {
                bucket_total++;
                if (p->next_by_fd == p || p->prev_by_fd == p) {
                    bucket_self++;
                    break;          /* avoid infinite-loop */
                }
                /* Reachable-from-global check: walk global list,
                 * O(N*M) but only if we actually want it.  Skip the
                 * full check here; cheaper to assert via the link
                 * invariants. */
                (void)bucket_unreach;
                p = p->next_by_fd;
                chain_iters++;
            }
        }
    }

    pygo_mutex_unlock(&pool->lock);
    pygo_self_check_stats_set(out, global_count, global_cycle,
                              parked_atomic, bucket_total, bucket_self,
                              bucket_unreach);
    return 0;
}

/* ---- per-fd registration cache ----
 * One bit per fd; set when we've already issued EPOLL_CTL_ADD (or
 * the kqueue equivalent) for this fd as edge-triggered for both
 * READ and WRITE.  Subsequent wait_fd calls then skip the
 * epoll_ctl syscall entirely -- the kernel keeps reporting edges
 * until the fd is closed (which auto-clears the registration).
 *
 * Every access (get/set/clear) is under pygo_pool.lock, keeping the
 * check + ADD atomic against concurrent registers for the same fd.
 * The backing array is preallocated once (pygo_fd_arrays_init) and
 * never realloc'd, so the pointer is stable for the process lifetime. */
static unsigned char *pygo_fd_registered_bm = NULL;
static size_t         pygo_fd_registered_cap_bytes = 0;

/* ---- per-fd pending-wakeup bitmap ----
 * Closes the M:N + free-threaded race where an epoll edge fires for
 * an fd whose parker hasn't been linked yet (or whose previous
 * parker was just unlinked by a wake but the goroutine hasn't
 * called wait_fd again).  Without this, the pump finds an empty
 * pygo_pool.by_fd[fd] bucket and silently drops the event; with
 * EPOLLET the kernel won't refire and the goroutine waits forever.
 *
 * Each fd gets one byte holding a mask of PYGO_NETPOLL_READ /
 * PYGO_NETPOLL_WRITE bits.  Pump sets bits when it can't find a
 * matching parker (pygo_fd_pending_wake_set, called with NO lock and
 * from any hub's pump concurrently); wait_fd consumes bits before
 * parking (pygo_fd_pending_wake_consume, lock-free) and returns
 * immediately if a pending bit covers the requested event.
 *
 * The array is preallocated once to the fd hard limit and never
 * realloc'd (see pygo_fd_arrays_init).  That is what makes concurrent
 * lock-free set/consume safe: with no realloc the base pointer never
 * moves, so there is no writer/writer or writer/reader UAF -- only
 * per-byte atomic fetch_or / fetch_and on a stable buffer.
 *
 * Memory ordering: the pump's fetch_or pairs with wait_fd's fetch_and
 * via ACQ_REL on both, providing a clean happens-before for "kernel
 * event happened" -> "next wait observes it".  The init publish stores
 * the base (RELEASE) before the cap (RELEASE); readers load the cap
 * (ACQUIRE) before the base, so a non-zero cap implies a valid base. */
static unsigned char *pygo_fd_pending_wake = NULL;
static size_t         pygo_fd_pending_wake_cap = 0;

/* Upper/lower bounds on the preallocated fd capacity.  4M fds = 4MB
 * pending-wake array + 512KB registration bitmap, worst case. */
#define PYGO_FD_CAP_MIN   1024u
#define PYGO_FD_CAP_MAX   (8u * 1024u * 1024u)

/* Capacity to preallocate the per-fd arrays to.  An open fd is always
 * < the RLIMIT_NOFILE soft limit, which can never exceed the hard
 * limit without privilege; sizing to the hard limit guarantees no
 * legal fd ever indexes past the array, so it is never realloc'd.
 * PYGO_NETPOLL_MAXFD overrides for hosts whose hard limit is
 * unlimited (then both rlimits read as RLIM_INFINITY). */
static size_t pygo_fd_cap_target(void)
{
    size_t target = 65536;
    const char *env;
#if !defined(PYGO_OS_WINDOWS)
    struct rlimit r;
    if (getrlimit(RLIMIT_NOFILE, &r) == 0) {
        if (r.rlim_max != RLIM_INFINITY && r.rlim_max > 0)
            target = (size_t)r.rlim_max;
        else if (r.rlim_cur != RLIM_INFINITY && r.rlim_cur > 0)
            target = (size_t)r.rlim_cur;
    }
#endif
    env = getenv("PYGO_NETPOLL_MAXFD");
    if (env != NULL && *env != '\0') {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end != env && v > 0) target = (size_t)v;
    }
    if (target < PYGO_FD_CAP_MIN) target = PYGO_FD_CAP_MIN;
    if (target > PYGO_FD_CAP_MAX) target = PYGO_FD_CAP_MAX;
    return target;
}

/* One-time preallocation of both per-fd arrays.  Called from
 * pygo_netpoll_init while holding pygo_pool.lock, which serialises it
 * against bit get/set/clear (same lock) and against a racing init.
 * Idempotent: a second call that finds the arrays already present
 * (e.g. after fini/init) is a no-op. */
static void pygo_fd_arrays_init(void)
{
    size_t cap_fds, bm_bytes;
    unsigned char *pw, *bm;
    if (pygo_fd_pending_wake != NULL) return;
    cap_fds  = pygo_fd_cap_target();
    bm_bytes = (cap_fds + 7u) >> 3;
    pw = (unsigned char *)calloc(cap_fds, 1);
    bm = (unsigned char *)calloc(bm_bytes, 1);
    if (pw == NULL || bm == NULL) {
        free(pw);
        free(bm);
        return;   /* caps stay 0; set/consume/bit_* bounds-check to no-ops */
    }
    pygo_fd_registered_bm        = bm;
    pygo_fd_registered_cap_bytes = bm_bytes;
    /* Publish base before cap so the lock-free consumer that loads cap
     * (ACQUIRE) and sees it non-zero is guaranteed to see this base. */
    __atomic_store_n(&pygo_fd_pending_wake, pw, __ATOMIC_RELEASE);
    __atomic_store_n(&pygo_fd_pending_wake_cap, cap_fds, __ATOMIC_RELEASE);
}

/* Warn once when an fd exceeds the preallocated ceiling.  This cannot
 * happen for an fd this process can legally hold (it would be >= the
 * hard limit); the guard exists only so a privileged runtime
 * limit-raise degrades to a dropped event + diagnostic, never a
 * reintroduced realloc race. */
static void pygo_fd_cap_warn_once(int fd)
{
    static int warned = 0;
    int expected = 0;
    if (__atomic_compare_exchange_n(&warned, &expected, 1, 0,
                                    __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
        fprintf(stderr,
                "[pygo] fd %d exceeds preallocated netpoll capacity %zu; "
                "raise PYGO_NETPOLL_MAXFD (event dropped)\n",
                fd, pygo_fd_pending_wake_cap);
    }
}

/* Pump-side: mark an event as observed-but-unrouted.  Lock-free and
 * callable concurrently from any hub's pump -- safe because the array
 * is preallocated and never moves. */
static void pygo_fd_pending_wake_set(int fd, int mask)
{
    unsigned char *base;
    size_t cap;
    if (fd < 0 || mask == 0) return;
    cap = __atomic_load_n(&pygo_fd_pending_wake_cap, __ATOMIC_ACQUIRE);
    if ((size_t)fd >= cap) { pygo_fd_cap_warn_once(fd); return; }
    base = __atomic_load_n(&pygo_fd_pending_wake, __ATOMIC_ACQUIRE);
    if (base == NULL) return;
    __atomic_fetch_or(&base[fd], (unsigned char)mask, __ATOMIC_ACQ_REL);
}

/* wait_fd-side: claim any pending bits matching `events`.  Returns
 * the bits that were pending AND in events (0 = nothing pending).
 * Lock-free. */
static int pygo_fd_pending_wake_consume(int fd, int events)
{
    unsigned char *base;
    size_t cap = __atomic_load_n(&pygo_fd_pending_wake_cap, __ATOMIC_ACQUIRE);
    if (fd < 0 || (size_t)fd >= cap) return 0;
    base = __atomic_load_n(&pygo_fd_pending_wake, __ATOMIC_ACQUIRE);
    if (base == NULL) return 0;
    {
        unsigned char take = (unsigned char)events;
        unsigned char prev =
            __atomic_fetch_and(&base[fd],
                               (unsigned char)~take, __ATOMIC_ACQ_REL);
        return prev & events;
    }
}

static int pygo_fd_bit_get(int fd)
{
    if (fd < 0) return 0;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) return 0;
    return (pygo_fd_registered_bm[byte] >> (fd & 7)) & 1;
}

static int pygo_fd_bit_set(int fd)
{
    if (fd < 0) return -1;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) {
        /* Beyond the preallocated hard-limit ceiling (pygo_fd_arrays_init);
         * cannot happen for a legal fd.  Fail so the caller treats it as
         * ENOMEM rather than silently registering nothing. */
        return -1;
    }
    pygo_fd_registered_bm[byte] |= (unsigned char)(1u << (fd & 7));
    return 0;
}

static void pygo_fd_bit_clear(int fd)
{
    if (fd < 0) return;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) return;
    pygo_fd_registered_bm[byte] &= (unsigned char)~(1u << (fd & 7));
}

#if defined(PYGO_HAVE_EPOLL)
static int pygo_epoll_fd = -1;
/* Eventfd registered by io_uring.c for the GLOBAL ring; events on this
 * fd are dispatched to pygo_iouring_drain().  -1 = none registered. */
static int pygo_iouring_eventfd_in_epoll = -1;

/* Per-hub iouring rings registered via pygo_netpoll_add_iouring_ring.
 * The dispatch path matches epoll evs[i].data.fd against the eventfd
 * column and calls pygo_iouring_ring_drain on the corresponding ring.
 * Sized for typical CPU counts (one hub == one ring); 64 is comfortable
 * for any host we run on.  Protected by pygo_pool.lock.
 *
 * Parallel arrays instead of array-of-struct: small (one cache line each
 * at 64 entries) + lets us scan the fd column tightly.
 */
#define PYGO_IOURING_RINGS_MAX 64
static int                       pygo_iouring_ring_efds[PYGO_IOURING_RINGS_MAX];
static struct pygo_iouring_ring *pygo_iouring_ring_ptrs[PYGO_IOURING_RINGS_MAX];
static int                       pygo_iouring_ring_count = 0;
#elif defined(PYGO_HAVE_KQUEUE)
static int pygo_kqueue_fd = -1;
#elif defined(PYGO_OS_WINDOWS)
#  include "netpoll_iocp.h"
/* Runtime-selected Windows backend.  Tier order:
 *   1. IOCP+AFD via \Device\Afd -- fastest at scale, O(1) per
 *      ready socket.  Requires NtDeviceIoControlFile (NT 4.0+).
 *   2. WSAPoll -- Vista+, no FD_SETSIZE cap, linear over fds.
 *   3. select() -- XP / Server 2003 fallback.
 *
 * Selection happens once in pygo_netpoll_init; backend_name is
 * exported via pygo_core.netpoll_backend() for introspection. */
typedef int (WSAAPI *pygo_wsapoll_fn)(LPWSAPOLLFD, ULONG, INT);
static pygo_wsapoll_fn pygo_win_wsapoll = NULL;
static int             pygo_win_use_iocp = 0;
static const char     *pygo_win_backend_name = "select";   /* updated by init */
#endif

/* Initialise pool->lock once, regardless of platform.  POSIX could use
 * PTHREAD_MUTEX_INITIALIZER and skip this, but Windows CRITICAL_SECTION
 * has no static-init form so the lazy-init pattern is uniform. */
static void pygo_parker_pool_lock_ensure_inited(pygo_parker_pool_t *pool)
{
#if defined(PYGO_OS_WINDOWS)
    /* InterlockedCompareExchange returns the prior value; only the
     * first caller transitions 0 -> 1 and runs the init. */
    if (InterlockedCompareExchange(&pool->lock_inited, 1, 0) == 0) {
        pygo_mutex_init(&pool->lock);
    } else {
        /* Spin briefly while another thread finishes init.  In practice
         * the init is one InitializeCriticalSection call (~100 ns), so
         * any starvation here is bounded. */
        while (pool->lock_inited != 2) { /* spin */ }
        return;
    }
    pool->lock_inited = 2;
#else
    if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) == 2) {
        return;
    }
    long expected = 0;
    if (__atomic_compare_exchange_n(&pool->lock_inited, &expected, 1,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        pygo_mutex_init(&pool->lock);
        __atomic_store_n(&pool->lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2)
            { /* spin */ }
    }
#endif
}

const char *pygo_netpoll_backend(void)
{
#if defined(PYGO_HAVE_EPOLL)
    return "epoll";
#elif defined(PYGO_HAVE_KQUEUE)
    return "kqueue";
#elif defined(PYGO_OS_WINDOWS)
    /* Force init so the IOCP/WSAPoll probe runs.  Without this the
     * default string ("select") is returned even when IOCP would
     * succeed -- the actual init only runs on first wait. */
    if (!__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) pygo_netpoll_init();
    return pygo_win_backend_name;
#else
    return "select";
#endif
}

int pygo_netpoll_init(void)
{
    /* ACQUIRE pairs with the RELEASE store at the end: any thread that
     * sees inited==1 here also sees the array publication below. */
    if (__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) return 0;
    pygo_parker_pool_lock_ensure_inited(&pygo_pool);
    /* Preallocate the per-fd arrays under pygo_pool.lock (serialises
     * against bit get/set/clear and a racing init). */
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_arrays_init();
    pygo_mutex_unlock(&pygo_pool.lock);
#if defined(PYGO_HAVE_EPOLL)
    pygo_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (pygo_epoll_fd < 0) return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    pygo_kqueue_fd = kqueue();
    if (pygo_kqueue_fd < 0) return -1;
#elif defined(PYGO_OS_WINDOWS)
    /* Bring up Winsock once.  Idempotent via plat_compat's
     * InterlockedCompareExchange guard. */
    pygo_winsock_init();
    /* Backend selection on Windows.  Default tier order:
     *   1. IOCP+AFD     - NT 5.1+; O(1) per ready socket, scales to 10k+
     *   2. WSAPoll      - Vista+ fallback (linear walk per call)
     *   3. select()     - XP / restricted-sandbox fallback (FD_SETSIZE cap)
     *
     *   PYGO_NETPOLL=wsapoll  -> force WSAPoll
     *   PYGO_NETPOLL=select   -> force select() */
    {
        const char *env = getenv("PYGO_NETPOLL");
        int want_wsapoll = (env != NULL && strcmp(env, "wsapoll") == 0);
        int want_select  = (env != NULL && strcmp(env, "select")  == 0);

        if (!want_wsapoll && !want_select && pygo_iocp_init() == 0) {
            pygo_win_use_iocp = 1;
            pygo_win_backend_name = "iocp-afd";
        } else if (want_select) {
            pygo_win_backend_name = "select";
        } else {
            HMODULE ws2 = GetModuleHandleA("ws2_32.dll");
            if (ws2 == NULL) ws2 = LoadLibraryA("ws2_32.dll");
            if (ws2 != NULL) {
                pygo_win_wsapoll = (pygo_wsapoll_fn)
                    (void *)GetProcAddress(ws2, "WSAPoll");
            }
            pygo_win_backend_name = (pygo_win_wsapoll != NULL) ? "wsapoll" : "select";
        }
    }
#endif
    /* RELEASE: publish the array writes above to any thread that later
     * observes inited==1 via the ACQUIRE load at the top. */
    __atomic_store_n(&pygo_netpoll_inited, 1, __ATOMIC_RELEASE);
    return 0;
}

void pygo_netpoll_fini(void)
{
#if defined(PYGO_HAVE_EPOLL)
    if (pygo_epoll_fd >= 0) { close(pygo_epoll_fd); pygo_epoll_fd = -1; }
#elif defined(PYGO_HAVE_KQUEUE)
    if (pygo_kqueue_fd >= 0) { close(pygo_kqueue_fd); pygo_kqueue_fd = -1; }
#elif defined(PYGO_OS_WINDOWS)
    if (pygo_win_use_iocp) {
        pygo_iocp_fini();
        pygo_win_use_iocp = 0;
    }
    /* WSAPoll / select are stateless; nothing else to close.
     * Winsock itself is left up by design (see pygo_winsock_init). */
#endif
    __atomic_store_n(&pygo_netpoll_inited, 0, __ATOMIC_RELEASE);
}

static long long monotonic_ns(void)
{
    return pygo_monotonic_ns();
}

/* ---- registration ----
 * Edge-triggered, register-once.  On epoll/kqueue the fd is ADDed
 * exactly once with both READ and WRITE arms in ET mode.  All
 * subsequent wait_fd calls just consult the bitmap and skip the
 * epoll_ctl/kevent syscall -- the kernel keeps reporting edges
 * until the fd closes.
 *
 * Safety: the caller MUST try the operation first and only call
 * wait_fd after EAGAIN.  That serialises the "kernel observed not-
 * ready" state with our parking, so the next not-ready->ready
 * transition is guaranteed to deliver an edge.  This is the same
 * pattern Go's netpoll uses.
 *
 * Stale-fd recovery: socket close auto-clears the kernel
 * registration when the last fd reference goes away.  monkey.py's
 * close hook calls pygo_netpoll_unregister so the bitmap stays in
 * sync with the kernel for fd reuse. */
static int pygo_netpoll_register(int fd, int events)
{
#if defined(PYGO_HAVE_EPOLL)
    int need_register;
    struct epoll_event ev;

    /* T1.5 lost-wake fix: arm ONLY the requested direction,
     * LEVEL-triggered + EPOLLONESHOT, re-arming on every park.
     *
     * The old scheme (register once, both arms, EPOLLET|EPOLLEXCLUSIVE,
     * never re-armed) lost readiness wakeups under concurrency -- a g
     * could sit parked on READ forever with its socket Recv-Q > 0
     * (data waiting), all hubs idle.  Two mechanisms:
     *   - EPOLLEXCLUSIVE + EPOLLET drops an edge whose single exclusive
     *     wakeup lands on a hub not currently inside epoll_wait;
     *   - EPOLLET never refires once an edge is missed, and register-once
     *     never re-armed, so the parker was never woken.
     * The cure, which is also the standard multi-threaded epoll pattern:
     *   - LEVEL-triggered: EPOLL_CTL_MOD re-evaluates readiness and
     *     reports an fd ready *now*, so data that arrived before the
     *     parker linked is delivered (EPOLLET would NOT re-report it --
     *     verified: EPOLLET+ONESHOT+re-arm hung 96/96);
     *   - EPOLLONESHOT: exactly one delivery per arm, so no thundering
     *     herd (EPOLLEXCLUSIVE unneeded) and the always-ready OUT side
     *     can't busy-loop the pump;
     *   - per-DIRECTION arming: with both IN+OUT one-shot, the always-
     *     writable OUT would consume the single delivery before a READ
     *     waiter's data arrived -- so arm exactly what this wait needs.
     * Cost: one epoll_ctl per park (the bitmap now only distinguishes
     * ADD from MOD).  Correctness over the saved syscall. */
    ev.events = EPOLLONESHOT;
    if (events & PYGO_NETPOLL_READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (events & PYGO_NETPOLL_WRITE) ev.events |= EPOLLOUT;
    ev.data.fd = fd;

    pygo_mutex_lock(&pygo_pool.lock);
    need_register = !pygo_fd_bit_get(fd);
    if (need_register && pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    pygo_mutex_unlock(&pygo_pool.lock);

    if (need_register) {
        if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
        /* Stale registration (dup'd fd / missed close-hook): re-arm. */
        if (errno == EEXIST &&
            epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
    } else {
        /* Re-arm the one-shot for this park; MOD re-checks readiness. */
        if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
        /* ENOENT = fd dropped from epoll (close/reuse race); ADD. */
        if (errno == ENOENT &&
            epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
    }
    /* Failed: drop the bit so a future caller can retry. */
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    (void)events;
    pygo_mutex_lock(&pygo_pool.lock);
    if (pygo_fd_bit_get(fd)) {
        pygo_mutex_unlock(&pygo_pool.lock);
        return 0;
    }
    if (pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_pool.lock);
        errno = ENOMEM;
        return -1;
    }
    pygo_mutex_unlock(&pygo_pool.lock);
    {
        struct kevent kev[2];
        EV_SET(&kev[0], fd, EVFILT_READ,  EV_ADD | EV_CLEAR, 0, 0, NULL);
        EV_SET(&kev[1], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, NULL);
        if (kevent(pygo_kqueue_fd, kev, 2, NULL, 0, NULL) == 0) return 0;
    }
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    return -1;
#else
    (void)fd;
    return 0;  /* select doesn't need pre-registration */
#endif
}

void pygo_netpoll_unregister(int fd)
{
#if defined(PYGO_HAVE_EPOLL) || defined(PYGO_HAVE_KQUEUE)
    pygo_mutex_lock(&pygo_pool.lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_pool.lock);
    /* No kernel syscall: epoll/kqueue auto-remove the fd when the
     * last reference closes.  Calling EPOLL_CTL_DEL after close
     * would race with fd reuse anyway. */
#else
    (void)fd;
#endif
}

int pygo_netpoll_parked_count(void)
{
    int total = 0;
    int i;
    for (i = 0; i < PYGO_PARKER_POOL_MAX; i++) {
        total += __atomic_load_n(&pygo_pools[i].total, __ATOMIC_ACQUIRE);
    }
    return total;
}

void pygo_netpoll_force_unlink_g_parker(pygo_g_t *g)
{
    pygo_parked_t *p;
    pygo_parker_pool_t *pool;
    if (g == NULL) return;
    /* Cheap path: no parker tracked, nothing to do. */
    if (g->netpoll_parker == NULL) return;
    p = (pygo_parked_t *)g->netpoll_parker;
    /* Route via the parker's recorded hub -- pool ownership is set
     * once at link time (wait_fd's pygo_parker_pool_for_hub) and the
     * parker stays in that pool for its whole lifetime, so reading
     * p->hub is sufficient to find the right lock here even though
     * we're not in the parker's owning hub. */
    pool = pygo_parker_pool_for_hub(p->hub);
    pygo_mutex_lock(&pool->lock);
    /* Re-read p inside the lock in case g->netpoll_parker was
     * cleared by a concurrent unlink between the check above and
     * the lock acquire. */
    p = (pygo_parked_t *)g->netpoll_parker;
    if (p != NULL) {
        /* pygo_parker_unlink is no-op if p is already clean (e.g.,
         * pump unlinked it between the cheap-path check and the lock
         * acquire) -- safe to call unconditionally. */
        (void)pygo_parker_unlink(pool, p);
        g->netpoll_parker = NULL;
        PYGO_EVT(PYGO_EVT_PARKER_FORCE, p, g, (long long)p->gen);
    }
    pygo_mutex_unlock(&pool->lock);
    /* The g is completing and its wait_fd will never resume to call
     * pool_release, so we release here.  Safe: parker is unlinked
     * above so no other thread holds a reference. */
    if (p != NULL) pygo_parker_pool_release(p);
}

int pygo_netpoll_add_iouring_eventfd(int fd)
{
#if defined(PYGO_HAVE_EPOLL)
    struct epoll_event ev;
    if (fd < 0) return -1;
    if (pygo_netpoll_init() != 0) return -1;
    /* Idempotent: if the same fd is already registered, skip the
     * epoll_ctl call.  io_uring.c only ever creates one eventfd per
     * process, so this matters only if init runs twice. */
    if (pygo_iouring_eventfd_in_epoll == fd) return 0;
    /* EPOLLEXCLUSIVE: see comment in pygo_netpoll_register. */
    ev.events = EPOLLIN | EPOLLET | EPOLLEXCLUSIVE;
    ev.data.fd = fd;
    if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        if (errno == EINVAL) {
            ev.events = EPOLLIN | EPOLLET;
            if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0
                && errno != EEXIST) return -1;
        } else if (errno != EEXIST) {
            return -1;
        }
    }
    pygo_iouring_eventfd_in_epoll = fd;
    return 0;
#else
    (void)fd;
    return 0;     /* iouring is Linux-only; non-epoll backends never hit this */
#endif
}

int pygo_netpoll_add_iouring_ring(int eventfd_fd,
                                  struct pygo_iouring_ring *ring)
{
#if defined(PYGO_HAVE_EPOLL)
    struct epoll_event ev;
    int i;
    if (eventfd_fd < 0 || ring == NULL) return -1;
    if (pygo_netpoll_init() != 0) return -1;
    pygo_mutex_lock(&pygo_pool.lock);
    /* Idempotent: re-registering the same eventfd just updates the
     * ring pointer (cheap; the eventfd is already in epoll). */
    for (i = 0; i < pygo_iouring_ring_count; i++) {
        if (pygo_iouring_ring_efds[i] == eventfd_fd) {
            pygo_iouring_ring_ptrs[i] = ring;
            pygo_mutex_unlock(&pygo_pool.lock);
            return 0;
        }
    }
    if (pygo_iouring_ring_count >= PYGO_IOURING_RINGS_MAX) {
        pygo_mutex_unlock(&pygo_pool.lock);
        errno = ENOSPC;
        return -1;
    }
    pygo_iouring_ring_efds[pygo_iouring_ring_count] = eventfd_fd;
    pygo_iouring_ring_ptrs[pygo_iouring_ring_count] = ring;
    pygo_iouring_ring_count++;
    pygo_mutex_unlock(&pygo_pool.lock);

    /* EPOLLEXCLUSIVE: see comment in pygo_netpoll_register.  Kernel
     * wakes exactly one waiter per ring-eventfd hit instead of all
     * hubs racing to drain the same CQ. */
    {
        int rc;
        ev.events = EPOLLIN | EPOLLET | EPOLLEXCLUSIVE;
        ev.data.fd = eventfd_fd;
        rc = epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, eventfd_fd, &ev);
        if (rc < 0 && errno == EINVAL) {
            ev.events = EPOLLIN | EPOLLET;
            rc = epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, eventfd_fd, &ev);
        }
        if (rc < 0 && errno != EEXIST) {
            /* Undo the table insert. */
            pygo_mutex_lock(&pygo_pool.lock);
            for (i = 0; i < pygo_iouring_ring_count; i++) {
                if (pygo_iouring_ring_efds[i] == eventfd_fd) {
                    pygo_iouring_ring_efds[i] =
                        pygo_iouring_ring_efds[pygo_iouring_ring_count - 1];
                    pygo_iouring_ring_ptrs[i] =
                        pygo_iouring_ring_ptrs[pygo_iouring_ring_count - 1];
                    pygo_iouring_ring_count--;
                    break;
                }
            }
            pygo_mutex_unlock(&pygo_pool.lock);
            return -1;
        }
    }
    return 0;
#else
    (void)eventfd_fd; (void)ring;
    return 0;
#endif
}

void pygo_netpoll_remove_iouring_ring(int eventfd_fd)
{
#if defined(PYGO_HAVE_EPOLL)
    int i;
    if (eventfd_fd < 0) return;
    pygo_mutex_lock(&pygo_pool.lock);
    for (i = 0; i < pygo_iouring_ring_count; i++) {
        if (pygo_iouring_ring_efds[i] == eventfd_fd) {
            pygo_iouring_ring_efds[i] =
                pygo_iouring_ring_efds[pygo_iouring_ring_count - 1];
            pygo_iouring_ring_ptrs[i] =
                pygo_iouring_ring_ptrs[pygo_iouring_ring_count - 1];
            pygo_iouring_ring_count--;
            break;
        }
    }
    pygo_mutex_unlock(&pygo_pool.lock);
    /* Caller is about to close the eventfd; epoll auto-removes when
     * the last fd reference is closed, so no EPOLL_CTL_DEL syscall. */
#else
    (void)eventfd_fd;
#endif
}

int pygo_netpoll_any_iouring_inflight(void)
{
#if defined(PYGO_HAVE_EPOLL)
    int i, total;
    /* Global ring inflight. */
    total = pygo_iouring_inflight();
    /* Per-hub rings.  Read snapshot under the lock (matching add/remove)
     * but the inflight load itself is atomic so we don't hold the lock
     * over the loop. */
    pygo_mutex_lock(&pygo_pool.lock);
    {
        struct pygo_iouring_ring *snapshot[PYGO_IOURING_RINGS_MAX];
        int n = pygo_iouring_ring_count;
        for (i = 0; i < n; i++) snapshot[i] = pygo_iouring_ring_ptrs[i];
        pygo_mutex_unlock(&pygo_pool.lock);
        for (i = 0; i < n; i++) {
            total += pygo_iouring_ring_inflight(snapshot[i]);
        }
    }
    return total;
#else
    return 0;
#endif
}

/* Forcibly wake every parked goroutine with ready_mask=-1 (cancelled).
 * Each waiter's pygo_netpoll_wait_fd call returns -1; callers (server
 * accept loops, etc.) see that and exit their loops.  Returns count
 * woken.  Iterates every per-hub pool plus the default pool. */
int pygo_netpoll_drain_parked(void)
{
    int n = 0;
    int pi;
    pygo_parked_t *p, *next;
    for (pi = 0; pi < PYGO_PARKER_POOL_MAX; pi++) {
        pygo_parker_pool_t *pool = &pygo_pools[pi];
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        pygo_mutex_lock(&pool->lock);
        p = pool->head;
        pool->head = NULL;
        /* Clear per-fd buckets too; everything is leaving the lists. */
        if (pool->by_fd != NULL && pool->by_fd_cap > 0) {
            memset(pool->by_fd, 0,
                   pool->by_fd_cap * sizeof(*pool->by_fd));
        }
        /* Drain the deadline heap too; everything is leaving. */
        pool->dh_size = 0;
        while (p != NULL) {
            next = p->next;
            if (p->ready_out != NULL) {
                *p->ready_out = -1;   /* signal cancellation */
            }
            p->next = NULL;
            p->slot = NULL;
            p->next_by_fd = NULL;
            p->prev_by_fd = NULL;
            p->heap_index = -1;
            if (p->hub != NULL) {
                pygo_mn_wake_g(p->hub, p->g);
            } else {
                pygo_sched_wake(p->g);
            }
            __atomic_sub_fetch(&pool->total, 1, __ATOMIC_RELEASE);
            p = next;
            n++;
        }
        pygo_mutex_unlock(&pool->lock);
    }
    return n;
}

/* ---- park / wake ---- */
int pygo_netpoll_wait_fd(int fd, int events, long long timeout_ns)
{
    pygo_parked_t *park;
    pygo_sched_t *s;
    int ready_mask = 0;
    void *hub_opaque;
    pygo_g_t *current_g;
    pygo_parker_pool_t *pool;

    if (pygo_netpoll_init() != 0) return -1;

    park = pygo_parker_pool_acquire();
    if (park == NULL) { errno = ENOMEM; return -1; }

    /* Determine where this g lives so pump can route the wake.
     * If we're inside an M:N hub, the hub TLS gives us the target;
     * otherwise it's the global single-thread scheduler. */
    hub_opaque = pygo_mn_current_hub_opaque();
    s = pygo_sched_get();
    if (hub_opaque != NULL) {
        /* Hub context: current g is in TLS (set by hub_main).  We
         * don't read s->current because that's the single-thread
         * sched's slot, not the hub's. */
        current_g = pygo_mn_tls_current_g();
    } else {
        current_g = s->current;
    }

    /* Pick the parker pool by current hub.  Per-hub pools mean a
     * goroutine parked on hub H links into pool[H].head + pool[H]
     * .by_fd[fd] + pool[H].dh_arr; another hub's wait_fd contends
     * only on its own pool's lock.  Single-thread sched + non-hub
     * callers fall back to pool[DEFAULT]. */
    pool = pygo_parker_pool_for_hub(hub_opaque);
    pygo_parker_pool_lock_ensure_inited(pool);

    park->fd = fd;
    park->events = events;
    park->deadline_ns = timeout_ns < 0 ? -1 : monotonic_ns() + timeout_ns;
    park->ready_out = &ready_mask;
    park->g = current_g;
    park->hub = hub_opaque;
    /* next/slot/next_by_fd/prev_by_fd are NULL from pool acquire. */

    /* Per-g parker tracking: each g has at most one parker active.
     * Setting this lets hub_main's completion path detect+forcibly
     * unlink any leaked parker before the g's stack is returned to
     * the pool (which would otherwise let pump dereference freed
     * memory via the stale parker pointer). */
    if (current_g != NULL) {
        current_g->netpoll_parker = park;
    }

    /* ORDER MATTERS (M:N + free-threaded race fix):
     *
     *   1. Link the parker.  Now any pump that wakes on an event for
     *      this fd can find it in pool->by_fd[fd].
     *   2. Consume any wakeups that fired BEFORE we got here.  Under
     *      M:N another hub's pump may have observed an event between
     *      this g's previous unlink (on its last wake) and the link
     *      above; the pending-wake bitmap captured it.
     *   3. Only then call epoll_ctl ADD / IOCP submit.  If ADD
     *      synthesizes an edge because the fd is already ready, the
     *      parker is already visible -- the pump will route it.
     *
     * The PREVIOUS ordering (submit before link) lost wakes in two
     * ways: (a) ADD-synthesized edges processed by another hub before
     * the parker was visible; (b) edges fired during the gap between
     * a previous parker being unlinked-on-wake and a new one being
     * linked. */
    pygo_mutex_lock(&pool->lock);
    pygo_parker_link(pool, park);
    pygo_mutex_unlock(&pool->lock);
    if (current_g != NULL) pygo_g_state_set(current_g, PYGO_GST_PARKED_NETPOLL);

    /* Drain any pre-existing pending-wake bits.  If something is
     * already there (from a pump that saw an event between our last
     * unlink and this link), wake ourselves immediately instead of
     * parking. */
    {
        int pending = pygo_fd_pending_wake_consume(fd, events);
        if (pending != 0) {
            pygo_mutex_lock(&pool->lock);
            pygo_parker_unlink(pool, park);
            pygo_mutex_unlock(&pool->lock);
            if (current_g != NULL) pygo_g_state_set(current_g, PYGO_GST_RUNNING);
            pygo_parker_pool_release(park);
            return pending;
        }
    }

#if defined(PYGO_OS_WINDOWS)
    /* IOCP-AFD: submit the poll request now; pump just drains
     * completions.  Falls through to the no-op register for the
     * WSAPoll / select paths. */
    if (pygo_win_use_iocp) {
        if (pygo_iocp_submit(fd, events, timeout_ns) != 0) {
            pygo_mutex_lock(&pool->lock);
            pygo_parker_unlink(pool, park);
            pygo_mutex_unlock(&pool->lock);
            pygo_parker_pool_release(park);
            return -1;
        }
    } else {
        if (pygo_netpoll_register(fd, events) != 0) {
            pygo_mutex_lock(&pool->lock);
            pygo_parker_unlink(pool, park);
            pygo_mutex_unlock(&pool->lock);
            pygo_parker_pool_release(park);
            return -1;
        }
    }
#else
    if (pygo_netpoll_register(fd, events) != 0) {
        pygo_mutex_lock(&pool->lock);
        pygo_parker_unlink(pool, park);
        pygo_mutex_unlock(&pool->lock);
        pygo_parker_pool_release(park);
        return -1;
    }
#endif

    /* Re-check pending bits after register: the ADD may have
     * synthesized an edge that another hub's pump processed in the
     * window between link and register; that pump's "no parker"
     * fallback now sets the pending bit (because we re-arrange the
     * pump path below to do that).  Drain again before yielding. */
    {
        int pending = pygo_fd_pending_wake_consume(fd, events);
        if (pending != 0) {
            pygo_mutex_lock(&pool->lock);
            pygo_parker_unlink(pool, park);
            pygo_mutex_unlock(&pool->lock);
            pygo_parker_pool_release(park);
            return pending;
        }
    }

    /* Snapshot tstate (same as pygo_sched_yield does) so the next
     * resume restores it.  Then yield WITHOUT re-queueing -- pump
     * pushes us back when the fd becomes ready. */
    if (current_g != NULL) {
        pygo_sched_park_current();
    }
    pygo_coro_yield();
    /* On wake: pump SHOULD have set ready_mask and removed us from the
     * parked lists.  Defensive unlink covers the case where pump
     * routed the wake via a path that bypassed pygo_parker_unlink. */
    if (park->slot != NULL || park->prev_by_fd != NULL ||
        park->next_by_fd != NULL ||
        (park->fd >= 0 && (size_t)park->fd < pool->by_fd_cap &&
         pool->by_fd[park->fd] == park)) {
#ifdef PYGO_PARKER_DEBUG
        fprintf(stderr,
                "[pygo] wait_fd resumed with parker still linked: "
                "parker=%p gen=%u fd=%d g=%p slot=%p next=%p nbf=%p pbf=%p "
                "bucket=%p\n",
                (void *)park, park->gen, park->fd, (void *)park->g,
                (void *)park->slot, (void *)park->next,
                (void *)park->next_by_fd, (void *)park->prev_by_fd,
                (park->fd >= 0 && (size_t)park->fd < pool->by_fd_cap)
                    ? (void *)pool->by_fd[park->fd] : NULL);
#endif
        pygo_mutex_lock(&pool->lock);
        pygo_parker_unlink(pool, park);
        pygo_mutex_unlock(&pool->lock);
    }
    /* Clear g->netpoll_parker before release so completion's force-
     * unlink cannot dereference a freelist entry. */
    if (current_g != NULL && current_g->netpoll_parker == park) {
        current_g->netpoll_parker = NULL;
    }
    pygo_parker_pool_release(park);
    return ready_mask;
}

/* Walk every parker pool looking for a parker matching (fd, mask).
 * The first match's ready_out gets the mask, the parker is unlinked
 * from its pool, and pygo_mn_wake_g routes the wake to its hub.
 * Returns 1 if a parker was found+woken, 0 otherwise.
 *
 * Lock ordering: pool->lock then hub->sub_lock (inside pygo_mn_wake_g).
 * We never hold pool[H1].lock while taking pool[H2].lock; the for
 * loop drops each pool's lock before moving on.  Same ordering as
 * pygo_pump_drain_expired below so the two are deadlock-free against
 * each other. */
static int pygo_pump_dispatch_event(int fd, int mask)
{
    int pi;
    for (pi = 0; pi < PYGO_PARKER_POOL_MAX; pi++) {
        pygo_parker_pool_t *pool = &pygo_pools[pi];
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        pygo_mutex_lock(&pool->lock);
        if ((size_t)fd < pool->by_fd_cap) {
            pygo_parked_t *p = pool->by_fd[fd];
            while (p != NULL) {
                pygo_parked_t *next_p = p->next_by_fd;
                if (p->events & mask) {
                    *(p->ready_out) = mask & p->events;
                    pygo_parker_unlink(pool, p);
#ifdef PYGO_PARKER_DEBUG
                    if (p->g != NULL &&
                        __atomic_load_n(&p->g->done, __ATOMIC_ACQUIRE)) {
                        fprintf(stderr,
                            "[pygo] pump waking DEAD g: g=%p done=1 "
                            "refcount=%d parker=%p fd=%d\n",
                            (void *)p->g,
                            (int)__atomic_load_n(&p->g->refcount,
                                                 __ATOMIC_ACQUIRE),
                            (void *)p, fd);
                    }
#endif
                    pygo_mn_wake_g(p->hub, p->g);
                    pygo_mutex_unlock(&pool->lock);
                    return 1;
                }
                p = next_p;
            }
        }
        pygo_mutex_unlock(&pool->lock);
    }
    return 0;
}

/* Drain expired-deadline parkers from every pool.  Returns count
 * woken.  Each parker is unlinked, gets ready_out=0 (timeout), and
 * its g is routed back via pygo_mn_wake_g / pygo_sched_wake.
 * Wakes happen inside the pool lock; lock ordering matches
 * pygo_pump_dispatch_event so the two are deadlock-free. */
static int pygo_pump_drain_expired(long long now)
{
    int woke = 0;
    int pi;
    for (pi = 0; pi < PYGO_PARKER_POOL_MAX; pi++) {
        pygo_parker_pool_t *pool = &pygo_pools[pi];
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        pygo_mutex_lock(&pool->lock);
        while (pool->dh_size > 0 && pool->dh_arr[0]->deadline_ns <= now) {
            pygo_parked_t *p = pool->dh_arr[0];
            if (p->ready_out != NULL) *p->ready_out = 0;
            pygo_parker_unlink(pool, p);
            if (p->hub != NULL) pygo_mn_wake_g(p->hub, p->g);
            else                pygo_sched_wake(p->g);
            woke++;
            PYGO_EVT(PYGO_EVT_PARKER_TIMEOUT, p, p->g, (long long)p->fd);
        }
        pygo_mutex_unlock(&pool->lock);
    }
    return woke;
}

/* Min deadline across all pools.  Returns -1 if no pool has any
 * timed parker.  Takes each pool's lock briefly. */
static long long pygo_dh_peek_deadline_global(void)
{
    long long earliest = -1;
    int pi;
    for (pi = 0; pi < PYGO_PARKER_POOL_MAX; pi++) {
        pygo_parker_pool_t *pool = &pygo_pools[pi];
        long long d;
        if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) continue;
        pygo_mutex_lock(&pool->lock);
        d = pygo_dh_peek_deadline(pool);
        pygo_mutex_unlock(&pool->lock);
        if (d >= 0 && (earliest < 0 || d < earliest)) earliest = d;
    }
    return earliest;
}

int pygo_netpoll_pump(long long timeout_ns)
{
    long long now;
    long long min_deadline = -1;
    int woke = 0;

    if (!__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) {
        if (pygo_netpoll_init() != 0) return -1;
    }

    /* Earliest deadline across every pool.  O(pool_count) instead of
     * the old O(1) global-heap peek -- pool_count is bounded at
     * PYGO_PARKER_POOL_MAX and the per-pool peek is still O(1). */
    min_deadline = pygo_dh_peek_deadline_global();

    now = monotonic_ns();
    if (min_deadline >= 0) {
        long long until = min_deadline - now;
        if (until < 0) until = 0;
        if (timeout_ns < 0 || until < timeout_ns) timeout_ns = until;
    }

#if defined(PYGO_HAVE_EPOLL)
    {
        struct epoll_event evs[64];
        int n;
        int ms = timeout_ns < 0 ? -1 :
                 (timeout_ns > 1000000000LL ? 1000 : (int)(timeout_ns / 1000000LL));
        Py_BEGIN_ALLOW_THREADS
        n = epoll_wait(pygo_epoll_fd, evs, 64, ms);
        Py_END_ALLOW_THREADS
        if (n > 0) {
            int i;
            for (i = 0; i < n; i++) {
                int fd = evs[i].data.fd;
                int mask = 0;
                /* io_uring eventfd (global ring): drain its counter
                 * and walk the CQ ring to wake parked goroutines via
                 * their per-op record.  Not a normal fd-park entry;
                 * skip the parker dispatch.  No pool lock needed --
                 * iouring drain is independent of parker pools. */
                if (pygo_iouring_eventfd_in_epoll >= 0 &&
                    fd == pygo_iouring_eventfd_in_epoll) {
                    pygo_iouring_drain();
                    continue;
                }
                /* Per-hub iouring rings.  Dispatch to the matching
                 * ring's drain.  Ring lookup is a tight scan over a
                 * 64-entry array; cheap and lock-free for reads. */
                {
                    int ri;
                    struct pygo_iouring_ring *match = NULL;
                    for (ri = 0; ri < pygo_iouring_ring_count; ri++) {
                        if (pygo_iouring_ring_efds[ri] == fd) {
                            match = pygo_iouring_ring_ptrs[ri];
                            break;
                        }
                    }
                    if (match != NULL) {
                        pygo_iouring_ring_drain(match);
                        continue;
                    }
                }
                if (evs[i].events & EPOLLIN)  mask |= PYGO_NETPOLL_READ;
                if (evs[i].events & EPOLLOUT) mask |= PYGO_NETPOLL_WRITE;
                /* Walk every per-hub pool looking for the parker for
                 * this fd.  Typically the parker lives in exactly one
                 * pool (its owning hub's), so the loop short-circuits
                 * on first match.  Sub-microsecond per event at
                 * realistic hub counts. */
                if (pygo_pump_dispatch_event(fd, mask)) {
                    woke++;
                } else if (mask != 0) {
                    /* No parker for this fd (either not linked yet,
                     * or already woken once and the goroutine
                     * hasn't called wait_fd again).  Stash the
                     * event mask in the per-fd pending bitmap so
                     * the next wait_fd on this fd consumes it
                     * instead of parking forever -- EPOLLET would
                     * never refire for this transition. */
                    pygo_fd_pending_wake_set(fd, mask);
                }
            }
        }
    }
#elif defined(PYGO_HAVE_KQUEUE)
    {
        struct kevent evs[64];
        struct timespec ts;
        struct timespec *tsp = NULL;
        int n;
        if (timeout_ns >= 0) {
            ts.tv_sec = (time_t)(timeout_ns / 1000000000LL);
            ts.tv_nsec = (long)(timeout_ns % 1000000000LL);
            tsp = &ts;
        }
        Py_BEGIN_ALLOW_THREADS
        n = kevent(pygo_kqueue_fd, NULL, 0, evs, 64, tsp);
        Py_END_ALLOW_THREADS
        if (n > 0) {
            int i;
            for (i = 0; i < n; i++) {
                int fd = (int)evs[i].ident;
                int mask = (evs[i].filter == EVFILT_READ) ?
                           PYGO_NETPOLL_READ : PYGO_NETPOLL_WRITE;
                if (pygo_pump_dispatch_event(fd, mask)) woke++;
            }
        }
    }
#elif defined(PYGO_OS_WINDOWS)
    /* Windows backend.  Runtime-chosen between IOCP+AFD (NT 4.0+,
     * O(1) per ready socket), WSAPoll (Vista+, no FD_SETSIZE cap)
     * and select() (XP / older fallback).
     *
     * Important: Windows fds passed in here MUST be SOCKET handles
     * (returned by socket.socket.fileno()).  Pipe/file fds are NOT
     * pollable through Winsock -- the monkey-patch layer routes those
     * to the thread-pool backend in monkey.py. */
    if (pygo_win_use_iocp) {
        /* --- IOCP+AFD drain ---
         *
         * Every wait_fd call submitted its own AFD_POLL IRP at park
         * time (see pygo_netpoll_wait_fd's Windows branch).  Pump
         * just drains completions and wakes the matching gs.  No
         * fd_set assembly, no linear walk over parked entries to
         * test readiness -- the kernel only signals what's ready.
         *
         * Loop: pull as many completions as are immediately
         * available, then return.  The hub_main outer loop calls
         * pump again with a fresh timeout if it needs to wait. */
        long long deadline = timeout_ns;
        int local_woke = 0;
        while (1) {
            int fd, evs;
            int rc;
            long long step = (local_woke == 0) ? deadline : 0;
            Py_BEGIN_ALLOW_THREADS
            rc = pygo_iocp_wait(step, &fd, &evs);
            Py_END_ALLOW_THREADS
            if (rc <= 0) break;             /* 0 = timeout, -1 = error */
            local_woke++;
            if (pygo_pump_dispatch_event(fd, evs)) woke++;
        }
    } else if (pygo_win_wsapoll != NULL) {
        /* --- WSAPoll path --- */
        WSAPOLLFD fds_stack[128];
        WSAPOLLFD *fds = fds_stack;
        ULONG fds_cap = 128;
        ULONG n_fds = 0;
        int ms = timeout_ns < 0 ? -1 :
                 (timeout_ns > 1000000000LL ? 1000 :
                  (int)(timeout_ns / 1000000LL));
        int rc;
        pygo_parked_t *p;

        pygo_mutex_lock(&pygo_pool.lock);
        /* Two passes: count, then fill (grows the heap buffer if the
         * stack one isn't big enough).  Worst case we re-malloc once. */
        {
            ULONG need = 0;
            for (p = pygo_pool.head; p != NULL; p = p->next) need++;
            if (need > fds_cap) {
                fds = (WSAPOLLFD *)malloc(sizeof(WSAPOLLFD) * need);
                if (fds == NULL) {
                    pygo_mutex_unlock(&pygo_pool.lock);
                    goto post_wait;   /* skip wait; deadlines still get checked */
                }
                fds_cap = need;
            }
        }
        for (p = pygo_pool.head; p != NULL; p = p->next) {
            fds[n_fds].fd = (SOCKET)p->fd;
            fds[n_fds].events = 0;
            if (p->events & PYGO_NETPOLL_READ)  fds[n_fds].events |= POLLRDNORM;
            if (p->events & PYGO_NETPOLL_WRITE) fds[n_fds].events |= POLLWRNORM;
            fds[n_fds].revents = 0;
            n_fds++;
        }
        if (n_fds > 0) {
            Py_BEGIN_ALLOW_THREADS
            rc = pygo_win_wsapoll(fds, n_fds, ms);
            Py_END_ALLOW_THREADS
            if (rc > 0) {
                ULONG i;
                for (i = 0; i < n_fds; i++) {
                    int mask = 0;
                    SHORT re = fds[i].revents;
                    int fdi = (int)fds[i].fd;
                    pygo_parked_t *p;
                    if (re & (POLLRDNORM | POLLIN | POLLHUP | POLLERR))
                        mask |= PYGO_NETPOLL_READ;
                    if (re & (POLLWRNORM | POLLOUT | POLLERR))
                        mask |= PYGO_NETPOLL_WRITE;
                    if (mask == 0) continue;
                    if ((size_t)fdi >= pygo_pool.by_fd_cap) continue;
                    p = pygo_pool.by_fd[fdi];
                    while (p != NULL) {
                        pygo_parked_t *next_p = p->next_by_fd;
                        if (p->events & mask) {
                            *(p->ready_out) = mask & p->events;
                            pygo_parker_unlink(&pygo_pool, p);
                            pygo_mn_wake_g(p->hub, p->g);
                            woke++;
                            break;
                        }
                        p = next_p;
                    }
                }
            }
        }
        if (fds != fds_stack) free(fds);
        pygo_mutex_unlock(&pygo_pool.lock);
    } else {
        /* --- select() fallback (XP / Server 2003 / no WSAPoll) ---
         * Windows select() uses SOCKET handles directly; the first arg
         * is ignored.  FD_SETSIZE is 64 by default but can be raised
         * via build define (see setup.py).  This path is best-effort
         * for legacy hosts; production usage assumes WSAPoll. */
        fd_set rfds, wfds, efds;
        pygo_parked_t *p;
        int rc;
        struct timeval tv, *tvp = NULL;

        FD_ZERO(&rfds); FD_ZERO(&wfds); FD_ZERO(&efds);
        pygo_mutex_lock(&pygo_pool.lock);
        for (p = pygo_pool.head; p != NULL; p = p->next) {
            if (p->events & PYGO_NETPOLL_READ)  FD_SET((SOCKET)p->fd, &rfds);
            if (p->events & PYGO_NETPOLL_WRITE) FD_SET((SOCKET)p->fd, &wfds);
            FD_SET((SOCKET)p->fd, &efds);
        }
        if (timeout_ns >= 0) {
            tv.tv_sec  = (long)(timeout_ns / 1000000000LL);
            tv.tv_usec = (long)((timeout_ns % 1000000000LL) / 1000LL);
            tvp = &tv;
        }
        Py_BEGIN_ALLOW_THREADS
        rc = select(0, &rfds, &wfds, &efds, tvp);
        Py_END_ALLOW_THREADS
        if (rc > 0) {
            pygo_parked_t *p = pygo_pool.head;
            while (p != NULL) {
                pygo_parked_t *next_p = p->next;
                int mask = 0;
                if (FD_ISSET((SOCKET)p->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                if (FD_ISSET((SOCKET)p->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                if (FD_ISSET((SOCKET)p->fd, &efds))
                    mask |= PYGO_NETPOLL_READ | PYGO_NETPOLL_WRITE;
                if (mask & p->events) {
                    *(p->ready_out) = mask & p->events;
                    pygo_parker_unlink(&pygo_pool, p);
                    pygo_mn_wake_g(p->hub, p->g);
                    woke++;
                }
                p = next_p;
            }
        }
        pygo_mutex_unlock(&pygo_pool.lock);
    }
post_wait:
    ;
#else
    /* POSIX select() backend.  Same as kqueue/epoll absent platforms. */
    {
        fd_set rfds, wfds;
        int max_fd = -1;
        pygo_parked_t *p;
        FD_ZERO(&rfds); FD_ZERO(&wfds);
        pygo_mutex_lock(&pygo_pool.lock);
        for (p = pygo_pool.head; p != NULL; p = p->next) {
            if (p->fd > max_fd) max_fd = p->fd;
            if (p->events & PYGO_NETPOLL_READ)  FD_SET(p->fd, &rfds);
            if (p->events & PYGO_NETPOLL_WRITE) FD_SET(p->fd, &wfds);
        }
        if (max_fd >= 0) {
            struct timeval tv, *tvp = NULL;
            if (timeout_ns >= 0) {
                tv.tv_sec = (long)(timeout_ns / 1000000000LL);
                tv.tv_usec = (long)((timeout_ns % 1000000000LL) / 1000LL);
                tvp = &tv;
            }
            if (select(max_fd + 1, &rfds, &wfds, NULL, tvp) > 0) {
                pygo_parked_t *p = pygo_pool.head;
                while (p != NULL) {
                    pygo_parked_t *next_p = p->next;
                    int mask = 0;
                    if (FD_ISSET(p->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                    if (FD_ISSET(p->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                    if (mask & p->events) {
                        *(p->ready_out) = mask & p->events;
                        pygo_parker_unlink(&pygo_pool, p);
                        pygo_mn_wake_g(p->hub, p->g);
                        woke++;
                    }
                    p = next_p;
                }
            }
        }
        pygo_mutex_unlock(&pygo_pool.lock);
    }
#endif

    /* Handle timeouts across every pool: parkers whose deadline has
     * passed get ready=0.  Heap pop per pool while top <= now;
     * O(pool_count + K log K) where K is expired-this-pass. */
    now = monotonic_ns();
    woke += pygo_pump_drain_expired(now);
    return woke;
}
