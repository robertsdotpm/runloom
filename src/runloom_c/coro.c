/* coro.c -- portable stackful coroutines.  See coro.h for the contract.
 *
 * Three backends, exactly one active per build:
 *   RUNLOOM_HAVE_FCONTEXT  -- hand-rolled inline asm (x86_64 SysV).  Fast path.
 *   RUNLOOM_HAVE_FIBERS    -- Windows Fibers (XP+).
 *   RUNLOOM_HAVE_UCONTEXT  -- POSIX fallback.
 */

#include "coro.h"
#include "runloom_crash.h"
#include "runloom_diag.h"   /* runloom_delay_inject (determinism tooling #2) */
#include "plat_atomic.h"    /* __atomic_*/__ATOMIC_* shim for MSVC (Windows build) */
#include "plat_compat.h"    /* runloom_mutex_t for the shared stack depot */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>      /* /proc reads for the depot auto-cap (init only) */

/* Recycle-hygiene checker (security): runloom pools and reuses fiber stacks
 * in raw mmap'd memory that ASan treats as always-valid, so a use-after-
 * recycle of a stack (the S1 leak class, generalized) is invisible to ASan.
 * Manually poison a stack while it sits in a pool and unpoison it on reuse --
 * then the existing ASan suite flags any access to a recycled-but-not-yet-
 * reacquired stack. No-op unless built with -fsanitize=address. */
#if defined(__SANITIZE_ADDRESS__)
#  define RUNLOOM_ASAN 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define RUNLOOM_ASAN 1
#  endif
#endif
#if defined(RUNLOOM_ASAN)
#  include <sanitizer/asan_interface.h>
#  define RUNLOOM_POISON(p, n)   ASAN_POISON_MEMORY_REGION((p), (n))
#  define RUNLOOM_UNPOISON(p, n) ASAN_UNPOISON_MEMORY_REGION((p), (n))
#else
#  define RUNLOOM_POISON(p, n)   ((void)0)
#  define RUNLOOM_UNPOISON(p, n) ((void)0)
#endif

#if defined(RUNLOOM_HAVE_FCONTEXT)
#  include "fcontext.h"
#  include <sys/mman.h>
#  include <unistd.h>
#  ifndef MAP_ANONYMOUS
#    ifdef MAP_ANON
#      define MAP_ANONYMOUS MAP_ANON
#    endif
#  endif
#elif defined(RUNLOOM_HAVE_FIBERS)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN 1
#  endif
#  include <windows.h>
#elif defined(RUNLOOM_HAVE_UCONTEXT)
#  if defined(RUNLOOM_OS_MACOS) && !defined(_XOPEN_SOURCE)
#    define _XOPEN_SOURCE 600
#  endif
#  include <ucontext.h>
#  include <sys/mman.h>
#  include <unistd.h>
#  ifndef MAP_ANONYMOUS
#    ifdef MAP_ANON
#      define MAP_ANONYMOUS MAP_ANON
#    endif
#  endif
#else
#  error "no stack-switch backend on this platform"
#endif

/* ------------------------------------------------------------------ */
/* Common state                                                       */
/* ------------------------------------------------------------------ */

struct runloom_coro {
    runloom_entry_fn entry;
    void *user;
    int done;
    /* Invariant sanitizer (#3): 1 while a thread is actively swapped INTO this
     * coro (between resume's swap-in and swap-out).  Written only when
     * RUNLOOM_DBG_INVARIANTS is on; checked on release/reacquire to catch a
     * coro/stack being recycled while a fiber still executes on it. */
    int dbg_running;
    /* Free-list link for the per-thread coro recycle pool.  When the
     * coro is in use, this is undefined; when on the pool free list,
     * it points to the next pooled coro. */
    struct runloom_coro *pool_next;
#if defined(RUNLOOM_HAVE_FCONTEXT)
    runloom_asm_coro_t asm_coro;
    void *stack;
    size_t stack_size;
    int grown;             /* 1 if copy-grow enlarged this stack */
    /* Bulk fresh-flag (go_n): 1 if the initial fcontext frame has NOT yet been
     * written to the stack top.  bulk_init can defer that write (the per-g page
     * fault) off the single spawner thread; runloom_coro_resume materializes it
     * lazily on the OWNING hub at first resume, then clears the flag.  Moves the
     * 1M scattered stack faults onto the H hubs, in parallel, overlapped with the
     * run.  0 for every non-bulk coro (eager asm_make_ctx). */
    int fresh;
#elif defined(RUNLOOM_HAVE_FIBERS)
    void *fiber;
#elif defined(RUNLOOM_HAVE_UCONTEXT)
    ucontext_t ctx;
    ucontext_t caller_ctx;
    void *stack;
    size_t stack_size;
#endif
};

/* Stack size in bytes for this coro, or 0 on backends without an
 * introspectable stack (Fibers).  Used by the fiber dump. */
size_t runloom_coro_stack_size(const runloom_coro_t *c)
{
    if (c == NULL) return 0;
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    return c->stack_size;
#else
    return 0;
#endif
}

/* Lowest usable byte of c's stack; the PROT_NONE guard page is the page
 * immediately below this (see runloom_stack_map_guarded).  NULL on backends
 * with no introspectable stack (Windows Fibers). */
void *runloom_coro_stack_base(const runloom_coro_t *c)
{
    if (c == NULL) return NULL;
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    return c->stack;
#else
    return NULL;
#endif
}

/* Size in bytes of the guard page below each coro stack (0 if the backend
 * installs no guard).  Mirrors runloom_stack_guard() without depending on its
 * later definition. */
size_t runloom_coro_guard_size(void)
{
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    {
        long ps = sysconf(_SC_PAGESIZE);
        return (ps > 0) ? (size_t)ps : (size_t)4096;
    }
#else
    return 0;
#endif
}

/* Per-thread "currently executing" pointer.  Used by runloom_coro_yield
 * to find the caller context.  Thread-local. */
static RUNLOOM_TLS runloom_coro_t *runloom_tls_current = NULL;

#if defined(RUNLOOM_HAVE_FIBERS)
static RUNLOOM_TLS void *runloom_tls_caller_fiber = NULL;
static RUNLOOM_TLS int runloom_tls_thread_was_fiber = 0;
#endif

const char *runloom_coro_backend(void)
{
#if defined(RUNLOOM_HAVE_FCONTEXT)
    return "fcontext-asm";
#elif defined(RUNLOOM_HAVE_FIBERS)
    return "fibers";
#elif defined(RUNLOOM_HAVE_UCONTEXT)
    return "ucontext";
#else
    return "unknown";
#endif
}

/* Invariant sanitizer (#3): a coro about to be recycled to the pool, released,
 * or reacquired must NOT have a thread executing on it.  If it does, a live
 * fiber stack is being recycled -- the use-after-free class behind the
 * gc-churn crashes.  Fires loudly (message + flight recorder + abort) at the
 * point of the violation.  No-op unless RUNLOOM_DBG_INVARIANTS. */
static void runloom_coro_assert_idle(runloom_coro_t *c, const char *where)
{
    if (!RUNLOOM_DBG_ON(RUNLOOM_DBG_INVARIANTS) || c == NULL) return;
    if (__atomic_load_n(&c->dbg_running, __ATOMIC_ACQUIRE) != 0)
        runloom_invariant_fail(where, c, runloom_coro_stack_base(c));
}

/* ------------------------------------------------------------------ */
/* Stack pool (POSIX backends)                                        */
/* ------------------------------------------------------------------ */

#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
/* Stack pool with the next-pointer embedded INSIDE the stack at offset 0.
 *
 * The previous design allocated a tiny linked-list node per stack via
 * malloc/free on every acquire/release.  At 100k spawns/sec that's
 * 100k mallocs + 100k frees per second of pure overhead.  We sidestep
 * it by writing the "next" pointer directly into the first 16 bytes
 * of the stack memory itself: the stack grows down from the high end,
 * so the low bytes are unused while the stack is in the free pool.
 *
 * Layout when in pool:
 *   stack[0 .. 7]   = next (pointer to next pooled stack)
 *   stack[8 .. 15]  = size (so mismatched-size reuses fail safe)
 *
 * Layout when in use: whatever the coroutine's stack contents are
 * (we overwrite the next/size header on first push).
 *
 * The pool is per-thread (TLS) so single-threaded benches see O(1)
 * push/pop with zero allocator traffic.  Size mismatches (rare --
 * users almost always use the default 128 KB stack) skip the pool
 * and just munmap / mmap on the slow path. */

#define RUNLOOM_STACK_HDR_NEXT  0
#define RUNLOOM_STACK_HDR_SIZE  1

static RUNLOOM_TLS void **runloom_tls_stack_pool = NULL;
static RUNLOOM_TLS int    runloom_tls_stack_pool_n = 0;

/* Shared global stack depot (magazine model).  The TLS pool above is a
 * lock-free per-thread cache for the common balanced case; but under an
 * acceptor->worker fan-out (one fiber mn_go's many handlers that complete
 * across all hubs) stacks drain out of the acceptor thread's cache and pile
 * into the worker threads' caches, which the acceptor can never reach -- so the
 * acceptor re-mmaps forever and the worker caches grow without bound.  When a
 * thread's cache exceeds RUNLOOM_STACK_TLS_CAP it flushes the excess down to
 * this shared depot; when a thread's cache is empty it refills a batch from the
 * depot before mmap'ing.  A stack freed on any hub is thus reusable on any
 * other, and total mappings are bounded (depot past its cap -> munmap).  The
 * depot is touched only on cache overflow/underflow, so the balanced fast path
 * stays lock-free.  POSIX-only block, so the mutex can be statically inited. */
#define RUNLOOM_STACK_TLS_CAP      64    /* per-thread cache high-water */
#define RUNLOOM_STACK_TLS_KEEP     32    /* keep this many local on a flush */
#define RUNLOOM_STACK_GLOBAL_CAP   1024  /* default depot bound; beyond -> munmap */
#define RUNLOOM_STACK_REFILL_BATCH 32    /* pulled from depot on underflow */
static runloom_mutex_t runloom_global_stack_lock = RUNLOOM_MUTEX_STATIC_INIT;
static void **runloom_global_stack_pool = NULL;
static int    runloom_global_stack_n = 0;

/* The depot cap bounds retained cross-hub mappings (past it -> munmap, a
 * TLB-shootdown storm on a drain burst).  A static 1024 is wrong for runloom's
 * scale: a server with N>>1024 live fibers wants the pool near its working set so
 * completions POOL instead of munmap-churning.  Rather than make the user type a
 * number, the DEFAULT is AUTO: the cap sizes itself to the live-stack high-water-
 * mark, recomputed once per sysmon tick (runloom_stack_autocap_tick below).
 *
 * HONEST BOUND: this caps the depot's VMA (mapping) count to ~1.5x the live-stack
 * high-water, clamped to SAFE_MAX = min(VMA budget, RAM budget) and squeezed so
 * live + pool VMAs stay under vm.max_map_count.  It does NOT bound RSS directly --
 * idle entries hold MADV_FREE'd (reclaimable-under-pressure) pages; only
 * RUNLOOM_STACK_MADV=off keeps them resident.  An explicit RUNLOOM_STACK_DEPOT_CAP
 * forces a static cap (override wins). */
static int  runloom_stack_cap_mode      = -1;  /* -1 unresolved, 0 static(env), 1 auto */
static int  runloom_stack_cap_static    = 0;   /* the env value, when mode==static */
static int  runloom_stack_cap_cached    = 0;   /* AUTO: recomputed per tick (0 = no tick yet) */
static int  runloom_stack_live          = 0;   /* atomic: depot-backed stacks IN USE */
static long runloom_stack_live_hwm      = 0;   /* atomic: decaying live high-water */
static long runloom_stack_max_map_count = 0;   /* read once at init (0 = unknown) */
static int  runloom_stack_safe_max      = 8192;/* min(VMA, RAM) ceiling, resolved at init */
static long runloom_stack_autocap_last_ns = 0; /* wall-clock decay timestamp */

static int runloom_global_stack_cap(void)
{
    int mode = __atomic_load_n(&runloom_stack_cap_mode, __ATOMIC_RELAXED);
    if (mode < 0) {
        const char *e = getenv("RUNLOOM_STACK_DEPOT_CAP");
        mode = 1;                                   /* default AUTO */
        if (e != NULL) {
            long v = atol(e);
            if (v > 0 && v < (1L << 24)) {
                runloom_stack_cap_static = (int)v;
                mode = 0;                           /* explicit override -> static */
            }
        }
        __atomic_store_n(&runloom_stack_cap_mode, mode, __ATOMIC_RELAXED);
    }
    if (mode == 0) return runloom_stack_cap_static;
    {
        /* AUTO: bare atomic load -- no arithmetic/syscall on the hot lock path.
         * 0 means sysmon hasn't ticked yet (or isn't running) -> the old default. */
        int c = __atomic_load_n(&runloom_stack_cap_cached, __ATOMIC_RELAXED);
        return c > 0 ? c : RUNLOOM_STACK_GLOBAL_CAP;
    }
}

/* Guard page below each coroutine stack.  A push past the low end of
 * the usable region lands in this PROT_NONE page -> SIGSEGV, instead of
 * silently corrupting the neighbouring allocation (plain mmap-per-g has
 * no implicit guard).  IMPORTANT: the usable stack the rest of coro.c
 * sees is still [stack, stack+size) with `stack` = lowest usable byte;
 * the guard is one page BELOW `stack`, owned ONLY by acquire/release/
 * warmup here.  region_base == (char *)stack - runloom_stack_guard().  So
 * paint, HWM scan, asm_make_ctx, and the madvise sweep are unchanged --
 * they all operate on the usable region. */
static size_t runloom_stack_guard(void)
{
    long ps = sysconf(_SC_PAGESIZE);
    return (ps > 0) ? (size_t)ps : (size_t)4096;
}

/* mmap a guarded stack [guard PROT_NONE | usable RW]; return the lowest
 * USABLE byte (region_base + guard), or NULL on mmap failure.  If the
 * mprotect fails the region is still usable (just unguarded) so we fall
 * through rather than fail the spawn -- safety degrades, correctness
 * does not. */
static void *runloom_stack_map_guarded(size_t usable)
{
    size_t guard = runloom_stack_guard();
    size_t total = guard + usable;
    /* Deliberately NOT MAP_STACK.  On FreeBSD/macOS MAP_STACK requests a
     * kernel grow-down stack whose lower pages stay inaccessible until the
     * stack grows into them, so eagerly writing the usable region low->high
     * (runloom_stack_paint, and the first asm pushes) faults with "invalid
     * permissions for mapped object".  runloom installs its OWN PROT_NONE guard
     * page below the usable region (see below), so the kernel auto-grow
     * semantics are both unnecessary and harmful.  MAP_STACK is a no-op on
     * Linux, so dropping it changes nothing there. */
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    {
        void *base = mmap(NULL, total, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (base == MAP_FAILED) return NULL;
        (void)mprotect(base, guard, PROT_NONE);
        return (char *)base + guard;
    }
}

/* Unmap a guarded stack given its usable base + usable size. */
static void runloom_stack_unmap_guarded(void *usable, size_t usable_size)
{
    size_t guard = runloom_stack_guard();
    munmap((char *)usable - guard, guard + usable_size);
}

/* Pop a matching-size stack from the TLS cache, munmapping any size-mismatched
 * entries at the head (bounded work in the rare mixed-size case).  Returns NULL
 * if the cache holds no matching stack. */
static void *runloom_stack_pop_local(size_t size)
{
    void **head = runloom_tls_stack_pool;
    while (head != NULL && (size_t)head[RUNLOOM_STACK_HDR_SIZE] != size) {
        void **next = (void **)head[RUNLOOM_STACK_HDR_NEXT];
        runloom_tls_stack_pool_n--;
        runloom_stack_unmap_guarded((void *)head,
                                    (size_t)head[RUNLOOM_STACK_HDR_SIZE]);
        head = next;
    }
    runloom_tls_stack_pool = head;
    if (head == NULL) return NULL;
    runloom_tls_stack_pool = (void **)head[RUNLOOM_STACK_HDR_NEXT];
    runloom_tls_stack_pool_n--;
    RUNLOOM_UNPOISON((void *)head, size);
    return (void *)head;
}

/* Refill the TLS cache with up to RUNLOOM_STACK_REFILL_BATCH matching-size
 * stacks from the shared depot.  Size-mismatched depot entries are dropped
 * (munmap).  Called only when the TLS cache underflows. */
static void runloom_stack_refill_from_global(size_t size)
{
    int moved = 0;
    runloom_mutex_lock(&runloom_global_stack_lock);
    while (moved < RUNLOOM_STACK_REFILL_BATCH && runloom_global_stack_pool != NULL) {
        void **g = runloom_global_stack_pool;
        runloom_global_stack_pool = (void **)g[RUNLOOM_STACK_HDR_NEXT];
        runloom_global_stack_n--;
        if ((size_t)g[RUNLOOM_STACK_HDR_SIZE] != size) {
            /* Wrong size for this thread's request: drop rather than cache it
             * (mixed-size workloads are rare; depot stays single-size in steady
             * state). */
            runloom_stack_unmap_guarded((void *)g,
                                        (size_t)g[RUNLOOM_STACK_HDR_SIZE]);
            continue;
        }
        g[RUNLOOM_STACK_HDR_NEXT] = (void *)runloom_tls_stack_pool;
        runloom_tls_stack_pool = g;
        runloom_tls_stack_pool_n++;
        moved++;
    }
    runloom_mutex_unlock(&runloom_global_stack_lock);
}

/* Move all-but-KEEP entries from the TLS cache down to the shared depot.
 * Past the depot cap, munmap (the bound that makes total mappings finite). */
static void runloom_stack_flush_to_global(void)
{
    void **keep_tail, **move_head;
    int i;
    if (runloom_tls_stack_pool_n <= RUNLOOM_STACK_TLS_KEEP) return;
    keep_tail = runloom_tls_stack_pool;
    for (i = 1; i < RUNLOOM_STACK_TLS_KEEP; i++)
        keep_tail = (void **)keep_tail[RUNLOOM_STACK_HDR_NEXT];
    move_head = (void **)keep_tail[RUNLOOM_STACK_HDR_NEXT];
    keep_tail[RUNLOOM_STACK_HDR_NEXT] = NULL;            /* cut local list at KEEP */
    runloom_tls_stack_pool_n = RUNLOOM_STACK_TLS_KEEP;

    {
    int cap = runloom_global_stack_cap();
    runloom_mutex_lock(&runloom_global_stack_lock);
    while (move_head != NULL) {
        void **next = (void **)move_head[RUNLOOM_STACK_HDR_NEXT];
        if (runloom_global_stack_n < cap) {
            move_head[RUNLOOM_STACK_HDR_NEXT] = (void *)runloom_global_stack_pool;
            runloom_global_stack_pool = move_head;
            runloom_global_stack_n++;
        } else {
            runloom_stack_unmap_guarded((void *)move_head,
                                        (size_t)move_head[RUNLOOM_STACK_HDR_SIZE]);
        }
        move_head = next;
    }
    runloom_mutex_unlock(&runloom_global_stack_lock);
    }
}

/* TEST (RUNLOOM_STACK_ARENA=1): carve every stack as a slice of ONE big
 * pre-mmap'd arena -- lock-free (a single atomic bump), no per-stack mmap, no
 * global depot lock.  Each fiber still gets a DISTINCT stack (its own
 * slice), so nothing corrupts and nothing crashes; this isolates whether the
 * stack-acquire path (mmap + depot lock) is the spawn bottleneck.  Test-only:
 * no per-slice guard page, and arena slices are never reclaimed.  Slices match
 * the existing layout: a guard prefix then the usable region; we return the
 * usable base.  Size-mismatched requests / arena exhaustion fall back. */
#ifndef MAP_NORESERVE
#define MAP_NORESERVE 0
#endif
static char  *runloom_arena_base = NULL;
static size_t runloom_arena_slot = 0;
static size_t runloom_arena_cap  = 0;
static size_t runloom_arena_next = 0;   /* bump cursor (slots); guarded by the lock */
static size_t runloom_arena_live = 0;   /* slots currently allocated; guarded too */
static runloom_mutex_t runloom_arena_init_lock = RUNLOOM_MUTEX_STATIC_INIT;

static int runloom_stack_arena_on(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_STACK_ARENA");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Lazy-mmap the arena sized to `slot`.  Caller holds runloom_arena_init_lock.
 * 0 = ready for this slot size; -1 = mmap failed or a size mismatch (all slots
 * in one arena must share a size). */
static int runloom_arena_ensure_locked(size_t slot)
{
    if (runloom_arena_base == NULL) {
        const char *n = getenv("RUNLOOM_STACK_ARENA_N");
        size_t cap = (n != NULL && *n) ? (size_t)strtoull(n, NULL, 0) : 1200000;
        void *base = mmap(NULL, cap * slot, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (base == MAP_FAILED) return -1;
        runloom_arena_slot = slot;
        runloom_arena_cap  = cap;
        __atomic_store_n(&runloom_arena_base, (char *)base, __ATOMIC_RELEASE);
    }
    return (slot == runloom_arena_slot) ? 0 : -1;
}

/* Reserve n contiguous slots; 0 + *start on success, -1 on exhaustion/mismatch.
 * LOCKED, but called once per go_n / per single carve -- NEVER per fiber --
 * so it's off the hot path.  The bump cursor is rewound on free (and fully
 * reset when the arena drains to empty), so repeated spawn->drain->spawn cycles
 * reuse the same address space instead of marching the cursor to the cap. */
static int runloom_arena_alloc(long n, size_t slot, size_t *start_out)
{
    int rc = -1;
    runloom_mutex_lock(&runloom_arena_init_lock);
    if (runloom_arena_ensure_locked(slot) == 0 &&
        runloom_arena_next + (size_t)n <= runloom_arena_cap) {
        *start_out = runloom_arena_next;
        runloom_arena_next += (size_t)n;
        runloom_arena_live += (size_t)n;
        rc = 0;
    }
    runloom_mutex_unlock(&runloom_arena_init_lock);
    return rc;
}

/* Return n slots starting at `start`.  Full-reset the cursor to 0 when the arena
 * drains to empty (the common batch-spawn / drain / respawn pattern -> total
 * reuse), else rewind it if this range sits at the very top (LIFO completion).
 * An out-of-order partial range that is neither is reclaimed at the next full
 * drain; no general free-list yet (a later refinement if fragmentation bites). */
static void runloom_arena_free(size_t start, long n)
{
    runloom_mutex_lock(&runloom_arena_init_lock);
    if (runloom_arena_live >= (size_t)n) runloom_arena_live -= (size_t)n;
    if (runloom_arena_live == 0)                       runloom_arena_next = 0;
    else if (start + (size_t)n == runloom_arena_next)  runloom_arena_next = start;
    runloom_mutex_unlock(&runloom_arena_init_lock);
}

static void *runloom_stack_arena_carve(size_t size)
{
    size_t guard = runloom_stack_guard();
    size_t slot  = guard + size;
    size_t start;
    if (runloom_arena_alloc(1, slot, &start) != 0) return NULL;
    return runloom_arena_base + start * slot + guard;
}

/* Is a usable-base pointer a slice of the test arena? (so release skips it) */
static int runloom_stack_in_arena(void *usable)
{
    char *base = __atomic_load_n(&runloom_arena_base, __ATOMIC_ACQUIRE);
    char *p = (char *)usable;
    return base != NULL && p >= base &&
           p < base + runloom_arena_cap * runloom_arena_slot;
}

static void *runloom_stack_acquire(size_t size)
{
    if (runloom_stack_arena_on()) {
        void *a = runloom_stack_arena_carve(size);
        if (a != NULL) return a;                 /* arena stacks are NOT depot-backed */
    }
    /* Count this depot-backed stack as live and bump the watermark the auto-cap
     * sizes to.  Racy max is fine (a missed update is caught next sysmon tick). */
    {
        int live = __atomic_add_fetch(&runloom_stack_live, 1, __ATOMIC_RELAXED);
        long h = __atomic_load_n(&runloom_stack_live_hwm, __ATOMIC_RELAXED);
        if ((long)live > h)
            __atomic_store_n(&runloom_stack_live_hwm, (long)live, __ATOMIC_RELAXED);
    }
    void *s = runloom_stack_pop_local(size);     /* lock-free fast path */
    if (s != NULL) return s;
    runloom_stack_refill_from_global(size);       /* balance across hubs */
    s = runloom_stack_pop_local(size);
    if (s != NULL) return s;
    return runloom_stack_map_guarded(size);       /* truly out of stock */
}

/* RSS reclaim of a POOLED (about-to-be-reused) stack body.  Prefer MADV_FREE
 * (Linux 4.5+), Go's scavenger choice (sysUnused).
 *
 * MEASURED, not assumed: MADV_FREE is ~2.3x cheaper per call than MADV_DONTNEED
 * on a multi-hub process (25us vs 59us / 256KB).  The win is NOT fewer TLB
 * shootdowns -- both flush the range's TLB on a multi-thread mm, and the
 * shootdown sample count was flat in profiling.  The win is that MADV_FREE skips
 * the EAGER page reclaim AND, if the stack is reacquired before the kernel
 * reclaims under pressure, the pages revalidate with NO re-fault (MADV_DONTNEED
 * zaps the pages, forcing a zero-fill fault on the next touch).  On a stack-churn
 * workload (1M bare fibers spawned+completed) this cut wall ~1.8x and sys-time
 * ~26%.  On a socket-I/O-bound workload (p01) it is negligible -- stack reclaim
 * is a rounding error against the socket syscalls there.  So this helps mass
 * fiber spawn/complete, not request/response servers.
 *
 * Probed lazily: under GIL-off the first few concurrent hubs may each probe
 * MADV_FREE on their OWN region and store the flag (RELAXED) -- harmless, they
 * converge on the same value.  Falls back to MADV_DONTNEED where MADV_FREE is
 * unsupported.  Env RUNLOOM_STACK_MADV forces it: "free" (default), an
 * unrecognized value also taking the default; "dontneed" (eager reclaim /
 * tighter RSS / the old behaviour), or "off" (no reclaim -- keep pages resident).
 * Used for BOTH the pool release path AND the park idle-sweep
 * (runloom_coro_madvise_idle).  The only cost vs DONTNEED is lazy RSS: pages stay
 * counted until pressure -- set RUNLOOM_STACK_MADV=dontneed if RSS metrics matter
 * more than the spawn/complete CPU.
 *
 * Unlike MADV_DONTNEED, MADV_FREE does NOT zero the pages -- a pooled stack keeps
 * the prior fiber's bytes until overwritten.  Same trust domain, and the security
 * scrub (RUNLOOM_STACK_SCRUB) is a SEPARATE path that deliberately stays on
 * MADV_DONTNEED for its zero-on-next-touch guarantee. */
static int runloom_stack_madv_flag = -1;      /* -1 unknown; 0 = off; else flag */
static void runloom_stack_madv_reclaim(void *addr, size_t len)
{
#if defined(__linux__)
    int flag = __atomic_load_n(&runloom_stack_madv_flag, __ATOMIC_RELAXED);
    if (flag == -1) {
        const char *e = getenv("RUNLOOM_STACK_MADV");
        if (e != NULL && strcmp(e, "off") == 0) {
            flag = 0;
        } else if (e != NULL && strcmp(e, "dontneed") == 0) {
#if defined(MADV_DONTNEED)
            flag = MADV_DONTNEED;
#else
            flag = 0;
#endif
        } else {
#if defined(MADV_FREE)
            /* Default: probe MADV_FREE on this first call (EINVAL => kernel too
             * old).  On success the region is already reclaimed -> remember + return. */
            if (madvise(addr, len, MADV_FREE) == 0) {
                __atomic_store_n(&runloom_stack_madv_flag, MADV_FREE, __ATOMIC_RELAXED);
                return;
            }
#endif
#if defined(MADV_DONTNEED)
            flag = MADV_DONTNEED;
#else
            flag = 0;
#endif
        }
        __atomic_store_n(&runloom_stack_madv_flag, flag, __ATOMIC_RELAXED);
    }
    if (flag != 0) (void)madvise(addr, len, flag);
#else
    (void)addr; (void)len;
#endif
}

static void runloom_stack_release(void *stack, size_t size)
{
    void **hdr;
    /* TEST arena slices are never reclaimed/pooled (they belong to the one big
     * arena mapping); just drop them.  No-op when the arena is off. */
    if (runloom_stack_arena_on() && runloom_stack_in_arena(stack)) {
        size_t guard = runloom_stack_guard();
        size_t slot  = guard + size;
        char  *base  = __atomic_load_n(&runloom_arena_base, __ATOMIC_ACQUIRE);
        size_t start = ((size_t)((char *)stack - guard - base)) / slot;
        {
            long ps = sysconf(_SC_PAGESIZE);
            size_t page = (ps > 0) ? (size_t)ps : (size_t)4096;
            if (size > page) runloom_stack_madv_reclaim((char *)stack + page, size - page);
        }
        runloom_arena_free(start, 1);     /* return the slot for reuse */
        return;                           /* arena: not counted in runloom_stack_live */
    }
    /* This depot-backed stack is no longer live (balances the acquire fetch_add). */
    __atomic_fetch_sub(&runloom_stack_live, 1, __ATOMIC_RELAXED);
    /* Drop physical pages back to the OS *before* writing the header.
     * MADV_DONTNEED keeps the VA reservation but lets the kernel reclaim
     * the page frames; next touch re-faults a fresh zero page.  We have
     * to skip the first page so the pool linkage survives -- the header
     * lives in the first 16 bytes of the stack.
     *
     * Net effect with MADV_DONTNEED: pool entries hold 4 KB resident each
     * instead of the full stack_size.  With the default MADV_FREE the reclaim is
     * LAZY (pages stay counted until pressure, then drop) -- we trade a little
     * apparent RSS for killing the per-release synchronous TLB shootdown, exactly
     * like Go.  Either way the deepest-used pages re-fault/re-validate on reuse,
     * so steady-state RSS still tracks active gs, not capacity.
     *
     * (Tried "optimization A" -- mincore the resident depth and madvise only the
     * touched range -- but MEASURED it as a net LOSS: with MADV_FREE the madvise
     * of never-resident pages is already nearly free, so the per-release mincore
     * cost +6s sys / 1M fibers for no wall win.  Reverted; left as a warning.) */
    {
        long ps = sysconf(_SC_PAGESIZE);
        size_t page = (ps > 0) ? (size_t)ps : (size_t)4096;
        if (size > page) {
            runloom_stack_madv_reclaim((char *)stack + page, size - page);
        }
    }
    hdr = (void **)stack;
    hdr[RUNLOOM_STACK_HDR_NEXT] = (void *)runloom_tls_stack_pool;
    hdr[RUNLOOM_STACK_HDR_SIZE] = (void *)size;
    runloom_tls_stack_pool = hdr;
    runloom_tls_stack_pool_n++;
    /* Poison the body (skip the 16-byte pool header read by the next
     * runloom_stack_acquire) so ASan flags any access to this stack while it
     * sits free in the pool. Unpoisoned again on acquire. */
    if (size > 16) {
        RUNLOOM_POISON((char *)stack + 16, size - 16);
    }
    /* Overflowed this thread's cache -> flush the older entries down to the
     * shared depot so another hub can reuse them (and so an imbalanced producer
     * thread can't hoard stacks unboundedly).  Keeps RUNLOOM_STACK_TLS_KEEP for
     * this thread's own fast path. */
    if (runloom_tls_stack_pool_n > RUNLOOM_STACK_TLS_CAP) {
        runloom_stack_flush_to_global();
    }
}

/* Pre-warm n stacks of the given size into the per-thread pool.
 * Returns the number successfully pre-allocated (may be < n if
 * mmap starts failing partway through). */
static int runloom_stack_warmup_posix(size_t size, int n)
{
    int i;
    for (i = 0; i < n; i++) {
        void *s = runloom_stack_map_guarded(size);
        if (s == NULL) return i;
        runloom_stack_release(s, size);
    }
    return n;
}

static size_t runloom_round_to_page(size_t size)
{
    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize <= 0) pagesize = 4096;
    return ((size + (size_t)pagesize - 1) /
            (size_t)pagesize) * (size_t)pagesize;
}

#endif

/* ------------------------------------------------------------------ */
/* Stack painting / high-water-mark scan                              */
/* ------------------------------------------------------------------ */

/* Global toggle for stack high-water-mark MEASUREMENT.  On during the
 * calibration window, disabled once it freezes so steady-state spawns pay
 * nothing.  (Kept named "paint" for ABI / stats / test compatibility; the
 * measurement itself is now paint-free -- see runloom_stack_hwm_scan.) */
static int runloom_stack_paint_on = 1;

void runloom_coro_paint_set(int enabled) { runloom_stack_paint_on = enabled ? 1 : 0; }
int  runloom_coro_paint_enabled(void)    { return runloom_stack_paint_on; }

/* Security: wipe a fiber's stack when it is recycled, so the next
 * fiber to reuse that stack can't read this one's leftovers (TLS keys,
 * request bodies -- the aio bridge runs OpenSSL on these stacks). OFF by
 * default: it costs one stack-sized memset per fiber completion, and the
 * leftover is only reachable via a C extension reading uninitialised stack
 * (Python objects live on the heap, not the fiber C stack). Enable for
 * security-sensitive workloads via RUNLOOM_STACK_SCRUB=1 or set_stack_scrub(True).
 * (Painting would also overwrite the data, but it is calibrated off for
 * performance after the first few spawns -- so it can't be relied on.) */
static int runloom_stack_scrub_on = 0;
void runloom_coro_scrub_set(int enabled) { runloom_stack_scrub_on = enabled ? 1 : 0; }
int  runloom_coro_scrub_enabled(void)    { return runloom_stack_scrub_on; }

/* Wipe a whole fiber stack.  On Linux, MADV_DONTNEED frees the page
 * frames and the next touch re-faults a zero page -- a complete scrub that
 * costs an O(1) syscall instead of a stack-sized memset (a 512 KB memset
 * was ~60x the spawn cost in measurement; this is ~flat).  Elsewhere
 * MADV_DONTNEED is only advisory (may not zero), so fall back to memset for
 * a guaranteed wipe.  stack is page-aligned and size page-rounded. */
static void runloom_stack_scrub(void *stack, size_t size)
{
#if defined(__linux__) && defined(MADV_DONTNEED)
    (void)madvise(stack, size, MADV_DONTNEED);
#else
    memset(stack, 0, size);
#endif
}

#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
/* No-op, retained as a spawn-path call site.  An earlier design filled the
 * unused stack body with a sentinel WORD (0x504AE6B7C9D1F2A3) so the HWM scan
 * could find the deepest overwritten slot.  That sentinel is a fake pointer,
 * and in a rare calibration-window timing it ended up on the interpreter's
 * value stack where a live object pointer belongs -- FOR_ITER then ran
 * `Py_TYPE(iter)->tp_iternext(iter)` on it, i.e. CALLED the fake pointer as a
 * function -> jump to a garbage address -> SIGSEGV (the gc-churn crash the
 * hang-hunter found; all 6 cores deref the sentinel).  The HWM is now measured
 * paint-free via resident pages (runloom_stack_hwm_scan below), so no marker is
 * ever written into stack memory and nothing can leak into live frames. */
static void runloom_stack_paint(void *stack, size_t size)
{
    (void)stack; (void)size;
}

/* Stack high-water-mark via resident pages (mincore) -- page-granular, which
 * is ample since calibration rounds to next_pow2.  A fiber grows its stack
 * DOWN from the top, faulting pages as it deepens; those pages stay resident
 * until the stack is released (MADV_DONTNEED).  So the contiguous run of
 * resident pages measured DOWN from the top is the deepest the fiber ever
 * reached.  The lone resident pool-header page at the very bottom sits below
 * the untouched gap, so the top-down scan stops before it.  Writes nothing
 * (companion to the datastack resident-page accounting in
 * runloom_sched_datastack.c.inc).  Returns 0 where mincore is unavailable. */
static size_t runloom_stack_hwm_scan(void *stack, size_t size)
{
#if !defined(_WIN32)
    long ps = sysconf(_SC_PAGESIZE);
    size_t page = (ps > 0) ? (size_t)ps : 4096;
    size_t npages = (page > 0) ? size / page : 0;
    size_t used = 0, hi = npages;
    unsigned char vec[512];                 /* 512 pages == 2 MiB per batch */
    if (npages == 0) return 0;
    while (hi > 0) {
        size_t batch = (hi > sizeof(vec)) ? sizeof(vec) : hi;
        size_t lo = hi - batch, i;
        if (mincore((char *)stack + lo * page, batch * page, vec) != 0) break;
        for (i = batch; i-- > 0; ) {
            if (vec[i] & 1) used += page;
            else return used;               /* untouched gap -> deepest found */
        }
        hi = lo;
    }
    return used;
#else
    (void)stack; (void)size;
    return 0;
#endif
}
#endif

/* ------------------------------------------------------------------ */
/* Thread init / fini                                                 */
/* ------------------------------------------------------------------ */

int runloom_coro_thread_init(void)
{
#if defined(RUNLOOM_HAVE_FIBERS)
    if (runloom_tls_caller_fiber == NULL) {
        void *f = ConvertThreadToFiber(NULL);
        if (f == NULL) return -1;
        runloom_tls_caller_fiber = f;
        runloom_tls_thread_was_fiber = 1;
    }
#endif
    /* Arm this OS thread's sigaltstack so the crash handler can run even when
     * the fault is a fiber stack overflow.  No-op unless installed. */
    runloom_crash_thread_arm();
    return 0;
}

void runloom_coro_thread_fini(void)
{
    runloom_crash_thread_disarm();
#if defined(RUNLOOM_HAVE_FIBERS)
    if (runloom_tls_thread_was_fiber) {
        ConvertFiberToThread();
        runloom_tls_caller_fiber = NULL;
        runloom_tls_thread_was_fiber = 0;
    }
#endif
}

int runloom_coro_warmup(size_t stack_size, int n)
{
    if (n <= 0) return 0;
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    {
        size_t rounded = runloom_round_to_page(
            stack_size < 4096 ? 4096 : stack_size);
        return runloom_stack_warmup_posix(rounded, n);
    }
#else
    /* Windows Fibers: CreateFiber maintains its own pool; warmup
     * would just round-trip Create+Delete which doesn't actually
     * pre-warm anything. */
    (void)stack_size;
    return 0;
#endif
}

/* B (background prewarm): fill the GLOBAL depot directly with up to n stacks of
 * `size`, so a later spawn BURST pops them instead of mmap'ing on the latency-
 * critical path.  Measured: a cold burst of 200k long-lived fibers spent ~5.6s
 * in spawn (one mmap each); prewarmed it was ~1.5s (~4x) -- the mmap cost moved
 * off the spawn path.  Unlike runloom_coro_warmup (which fills the CALLING
 * thread's TLS cache -- unreachable from a non-hub background thread), this pushes
 * straight to the shared depot under its lock, bounded by the depot cap (so a big
 * prewarm needs RUNLOOM_STACK_DEPOT_CAP raised near the target).  Freshly mmap'd
 * stacks are lazy -- 0 RSS until first touched -- so a deep prewarm costs address
 * space + VMAs, not memory.  Returns the count retained. */
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
static int runloom_stack_prewarm_global(size_t size, int n)
{
    int cap = runloom_global_stack_cap();
    int made = 0;
    while (made < n) {
        void *s;
        int full;
        runloom_mutex_lock(&runloom_global_stack_lock);
        full = (runloom_global_stack_n >= cap);
        runloom_mutex_unlock(&runloom_global_stack_lock);
        if (full) break;                          /* depot at cap -> stop */
        s = runloom_stack_map_guarded(size);
        if (s == NULL) break;                     /* mmap exhausted (ENOMEM/VMA cap) */
        runloom_mutex_lock(&runloom_global_stack_lock);
        if (runloom_global_stack_n < cap) {
            ((void **)s)[RUNLOOM_STACK_HDR_NEXT] = (void *)runloom_global_stack_pool;
            ((void **)s)[RUNLOOM_STACK_HDR_SIZE] = (void *)size;
            runloom_global_stack_pool = (void **)s;
            runloom_global_stack_n++;
            made++;
            runloom_mutex_unlock(&runloom_global_stack_lock);
        } else {
            runloom_mutex_unlock(&runloom_global_stack_lock);
            runloom_stack_unmap_guarded(s, size); /* lost the cap race -> give back */
            break;
        }
    }
    return made;
}
#if !defined(_WIN32)
typedef struct { size_t size; int n; } runloom_prewarm_arg_t;
static void *runloom_prewarm_thread_main(void *arg)
{
    runloom_prewarm_arg_t *a = (runloom_prewarm_arg_t *)arg;
    runloom_stack_prewarm_global(a->size, a->n);
    free(a);
    return NULL;
}
#endif
#endif

/* Public: prewarm `n` stacks into the global depot.  background=1 (default for
 * the Python binding) runs it on a detached OS thread and returns 0 immediately
 * -- the "tray of clean cups refilled in the background" so a later spawn burst
 * never walks to the kernel.  background=0 runs synchronously and returns the
 * count retained.  Returns -1 if the background thread could not be started. */
int runloom_coro_prewarm(size_t stack_size, int n, int background)
{
    if (n <= 0) return 0;
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    {
        size_t rounded = runloom_round_to_page(stack_size < 4096 ? 4096 : stack_size);
        if (!background) return runloom_stack_prewarm_global(rounded, n);
#if !defined(_WIN32)
        {
            runloom_prewarm_arg_t *a =
                (runloom_prewarm_arg_t *)malloc(sizeof(*a));
            runloom_thread_t t;
            if (a == NULL) return -1;
            a->size = rounded; a->n = n;
            if (runloom_thread_create(&t, runloom_prewarm_thread_main, a) != 0) {
                free(a);
                return -1;
            }
            pthread_detach(t);                    /* fire-and-forget */
            return 0;
        }
#else
        return runloom_stack_prewarm_global(rounded, n);  /* no bg thread here */
#endif
    }
#else
    (void)stack_size; (void)n; (void)background;
    return 0;
#endif
}

/* Continuous background prewarm daemon: keeps the GLOBAL depot topped to `target`
 * so there is ALWAYS a backlog of ready stacks (refilling as a spawn burst drains
 * it -- unlike the one-shot runloom_coro_prewarm above).  One daemon per process;
 * runloom_coro_prewarm_keep starts it (or just RE-TARGETS a running one), and
 * runloom_coro_prewarm_stop halts + joins it.  It only mmaps while the depot is
 * BELOW target, in small batches that yield the mmap_lock between them; once the
 * backlog is full it idles (no syscalls, no contention).  A single daemon thread
 * cannot out-pace 8 hubs draining the pool under SUSTAINED high spawn -- it tops
 * up during lulls, which is what a "ready backlog" wants. */
#if !defined(_WIN32) && (defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT))
static runloom_thread_t runloom_prewarm_daemon_thread;
static int    runloom_prewarm_daemon_running = 0;   /* atomic: a daemon exists  */
static int    runloom_prewarm_daemon_stop    = 0;   /* atomic: asked to stop    */
static int    runloom_prewarm_daemon_target  = 0;   /* atomic: desired depth    */
static size_t runloom_prewarm_daemon_size    = 0;   /* fixed before thread start */

static void *runloom_prewarm_daemon_main(void *arg)
{
    (void)arg;
    while (!__atomic_load_n(&runloom_prewarm_daemon_stop, __ATOMIC_ACQUIRE)) {
        int target = __atomic_load_n(&runloom_prewarm_daemon_target, __ATOMIC_RELAXED);
        int cur;
        runloom_mutex_lock(&runloom_global_stack_lock);
        cur = runloom_global_stack_n;
        runloom_mutex_unlock(&runloom_global_stack_lock);
        if (cur < target) {
            int want = target - cur;
            if (want > 256) want = 256;                 /* small batch */
            runloom_stack_prewarm_global(runloom_prewarm_daemon_size, want);
            runloom_sleep_ns(200LL * 1000);             /* 200us: yield mmap_lock */
        } else {
            runloom_sleep_ns(5LL * 1000 * 1000);        /* 5ms: backlog full, idle */
        }
    }
    return NULL;
}

void runloom_coro_prewarm_stop(void)
{
    if (__atomic_load_n(&runloom_prewarm_daemon_running, __ATOMIC_ACQUIRE) == 0)
        return;
    __atomic_store_n(&runloom_prewarm_daemon_stop, 1, __ATOMIC_RELEASE);
    runloom_thread_join(runloom_prewarm_daemon_thread);
    __atomic_store_n(&runloom_prewarm_daemon_running, 0, __ATOMIC_RELEASE);
}

int runloom_coro_prewarm_keep(size_t stack_size, int target)
{
    if (target <= 0) { runloom_coro_prewarm_stop(); return 0; }
    /* Retarget first so an already-running daemon picks it up immediately. */
    __atomic_store_n(&runloom_prewarm_daemon_target, target, __ATOMIC_RELAXED);
    if (__atomic_exchange_n(&runloom_prewarm_daemon_running, 1, __ATOMIC_ACQ_REL) == 1)
        return 0;                                        /* already running */
    runloom_prewarm_daemon_size =
        runloom_round_to_page(stack_size < 4096 ? 4096 : stack_size);
    __atomic_store_n(&runloom_prewarm_daemon_stop, 0, __ATOMIC_RELEASE);
    if (runloom_thread_create(&runloom_prewarm_daemon_thread,
                              runloom_prewarm_daemon_main, NULL) != 0) {
        __atomic_store_n(&runloom_prewarm_daemon_running, 0, __ATOMIC_RELEASE);
        return -1;
    }
    return 0;
}

/* fork() child: the daemon thread did NOT survive, but the flags were copied.
 * Zero them (NO join -- the thread is gone) so a later keep() can restart and a
 * later stop() doesn't join a dead handle. */
void runloom_coro_prewarm_reset_after_fork(void)
{
    __atomic_store_n(&runloom_prewarm_daemon_running, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&runloom_prewarm_daemon_stop, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&runloom_prewarm_daemon_target, 0, __ATOMIC_RELAXED);
}
#else
int  runloom_coro_prewarm_keep(size_t stack_size, int target) { (void)stack_size; (void)target; return 0; }
void runloom_coro_prewarm_stop(void) { }
void runloom_coro_prewarm_reset_after_fork(void) { }
#endif

/* ---------------- depot auto-cap: init / per-tick / reset ----------------
 * All state is file-static above; sysmon drives the tick, mn_init/_fini/_fork
 * the lifecycle.  Keeping it here (not cross-TU) avoids extern atomics: the
 * getter and the acquire/release counters all touch the same file-statics. */

/* Resolve SAFE_MAX once: min(VMA budget, RAM budget).  Conservative -- the cap is
 * only a ceiling; the live-set squeeze in the tick is what tracks the real load. */
void runloom_stack_autocap_init(void)
{
    long mmc = 0, memkb = 0;
    FILE *f = fopen("/proc/sys/vm/max_map_count", "r");
    if (f != NULL) { if (fscanf(f, "%ld", &mmc) != 1) mmc = 0; fclose(f); }
    __atomic_store_n(&runloom_stack_max_map_count, mmc, __ATOMIC_RELAXED);
    f = fopen("/proc/meminfo", "r");
    if (f != NULL) {
        char line[256];
        while (fgets(line, sizeof line, f) != NULL)
            if (sscanf(line, "MemTotal: %ld kB", &memkb) == 1) break;
        fclose(f);
    }
    {
        /* VMA budget: spend at most 40% of vm.max_map_count on the pool (2 VMAs/stack). */
        long vma_based = (mmc > 0) ? (mmc * 40 / 100 / 2) : 8192;
        /* RAM budget: ~12% of RAM, ~64 KiB resident estimate per pooled stack.  This is
         * what actually bounds bytes on a raised-max_map_count host (where vma_based is
         * vestigial), so AUTO can never pool hundreds of GB. */
        long ram_based = 8192;
        if (memkb > 0)
            ram_based = (long)((double)memkb * 1024.0 * 0.12 / (64.0 * 1024.0));
        long sm = vma_based < ram_based ? vma_based : ram_based;
        if (sm < RUNLOOM_STACK_GLOBAL_CAP) sm = RUNLOOM_STACK_GLOBAL_CAP;
        if (sm > (1L << 24)) sm = (1L << 24);
        __atomic_store_n(&runloom_stack_safe_max, (int)sm, __ATOMIC_RELAXED);
    }
    __atomic_store_n(&runloom_stack_autocap_last_ns, 0, __ATOMIC_RELAXED);
}

/* Once per sysmon tick: wall-clock-decay the watermark, recompute the cached cap.
 * Wall-clock (not per-tick) decay makes retention independent of RUNLOOM_SYSMON_MS.
 *
 * Design rationale (validated against jemalloc/Go prior art -- read before "fixing"):
 *  - TAU controls how long a recent burst's pool stays warm, i.e. it trades
 *    re-mmap/fault churn on the NEXT burst against idle VMA headroom.  It is NOT a
 *    purge pacer: we MADV_FREE once at release and the tick issues ZERO syscalls,
 *    so jemalloc's dirty_decay_ms=10s (which paces madvise volume) is the wrong
 *    axis -- do not anchor TAU to it, and never re-set a jemalloc decay_ms per tick
 *    (that forces a synchronous bulk-purge storm).
 *  - The tick does no work that scales with reclaimed bytes, so Go's CPU-budgeted
 *    PI-controller scavenger is unnecessary; a fixed 10ms scalar update is free.
 *  - NO active trim: when the cap decays below the depot's size, nothing is munmap'd
 *    here -- munmap only happens on a flush OVERFLOW, which only occurs during a
 *    burst when the cap is high.  An idle trough is silent, so the decaying cap is
 *    naturally immune to the cap-chatter a down-side hysteresis band would guard
 *    (measured: a 1s-period 3k sawtooth -> 53 munmaps vs 12,980 at a static cap).
 *  - Posture = jemalloc `muzzy`/`-1`-decay / Go pre-1.16: front-load MADV_FREE, no
 *    timed escalation; "idle RSS looks high until pressure" is EXPECTED.  If a
 *    cgroup memory.max / observability requirement ever appears, the prior-art
 *    escape hatch is an OPTIONAL watchdog-driven MADV_DONTNEED 2nd stage (Go 1.16's
 *    default) -- not a shorter TAU, not a pacer. */
void runloom_stack_autocap_tick(void)
{
    long now  = (long)runloom_monotonic_ns();
    long last = runloom_stack_autocap_last_ns;
    int  live = __atomic_load_n(&runloom_stack_live, __ATOMIC_RELAXED);
    long hwm  = __atomic_load_n(&runloom_stack_live_hwm, __ATOMIC_RELAXED);
    if (last != 0 && now > last) {
        double dt = (double)(now - last) / 1e9;     /* seconds */
        double factor = 1.0 - dt / 1.5;             /* TAU=1.5s; linear ~ exp(-dt/TAU) */
        if (factor < 0.0) factor = 0.0;
        hwm = (long)((double)hwm * factor);
    }
    if ((long)live > hwm) hwm = live;
    __atomic_store_n(&runloom_stack_live_hwm, hwm, __ATOMIC_RELAXED);
    runloom_stack_autocap_last_ns = now;
    {
        long cap   = hwm * 3 / 2;                    /* 1.5x slack */
        long floor = 0;                              /* active prewarm target (POSIX only) */
#if !defined(_WIN32) && (defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT))
        floor = __atomic_load_n(&runloom_prewarm_daemon_target, __ATOMIC_RELAXED);
#endif
        long safe  = __atomic_load_n(&runloom_stack_safe_max, __ATOMIC_RELAXED);
        long mmc   = __atomic_load_n(&runloom_stack_max_map_count, __ATOMIC_RELAXED);
        if (floor > cap) cap = floor;               /* never below an active prewarm target */
        if (mmc > 0) {                               /* squeeze: leave room for live VMAs */
            long live_room = mmc / 2 - (long)live * 2;
            if (live_room < 0) live_room = 0;
            if (live_room < safe) safe = live_room;
        }
        if (cap < RUNLOOM_STACK_GLOBAL_CAP) cap = RUNLOOM_STACK_GLOBAL_CAP;
        if (cap > safe) cap = safe;
        if (cap < 1) cap = 1;
        __atomic_store_n(&runloom_stack_cap_cached, (int)cap, __ATOMIC_RELAXED);
    }
}

/* mn_fini / fork-child: forget the watermark + cached cap so the next session (or
 * a sysmon-less / forked context) falls back to the static default, never a stale
 * peak.  runloom_stack_live is self-correcting (acquire/release), so leave it. */
void runloom_stack_autocap_reset(void)
{
    __atomic_store_n(&runloom_stack_live_hwm, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&runloom_stack_cap_cached, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&runloom_stack_autocap_last_ns, 0, __ATOMIC_RELAXED);
}

/* ================================================================== */
/* Backend: fcontext (inline asm)                                     */
/* ================================================================== */

#if defined(RUNLOOM_HAVE_FCONTEXT)

/* Bridge from asm coro entry -> user entry.  Set as
 * runloom_asm_coro_t.entry by runloom_coro_new. */
static void runloom_fcontext_entry(void *user)
{
    runloom_coro_t *c = (runloom_coro_t *)user;
    c->entry(c->user);
    /* When we return, runloom_asm_entry sets done=1 and loops back to
     * caller via runloom_asm_swap -- never returns here. */
}

/* ---- coro recycling pool ---------------------------------------- *
 *
 * On spawn-heavy workloads (100k req/s servers) every Go-routine
 * pays for an mmap (or pool-cached stack), a calloc(coro_t), and an
 * asm_make_ctx that writes the initial register frame onto the
 * fresh stack.  Of those three, the stack pool already eliminates
 * mmap in steady state; the calloc and asm_make remain.
 *
 * Recycle the whole coro_t on destroy: keep its stack attached,
 * push to a per-thread free list.  On new(), pop, re-init entry +
 * user + done, redo asm_make_ctx (writes ~6 registers to the same
 * stack bottom), return.  Net win: ~150-250 ns / spawn.
 *
 * Size cap so we don't hoard 100k * (~140 KB stacks) after a burst.
 *
 * Kept modest (not thousands): this is a per-thread cache that keeps the
 * stack ATTACHED, so under an acceptor->worker fan-out the worker threads
 * would otherwise hoard up to CAP attached stacks each that the acceptor can
 * never reuse.  Overflow beyond CAP releases the stack to the shared,
 * cross-hub-balanced stack depot (see runloom_stack_release), so a low cap
 * bounds per-thread hoarding while the depot recycles across hubs.  The
 * balanced steady state (occupancy = live fibers) sits well under CAP, so
 * the lock-free coro-reuse fast path is unaffected.
 */
#define RUNLOOM_CORO_POOL_CAP 512
static RUNLOOM_TLS runloom_coro_t *runloom_coro_pool = NULL;
static RUNLOOM_TLS int runloom_coro_pool_size = 0;

runloom_coro_t *runloom_coro_new(size_t stack_size,
                           runloom_entry_fn entry,
                           void *user)
{
    runloom_coro_t *c;
    size_t rounded;
    void *stack_top;

    if (stack_size < 4096) stack_size = 4096;
    rounded = runloom_round_to_page(stack_size);

    /* Recycle if the pool has a compatible coro (same stack size).
     * Walking the chain to find a size match would be O(N); we just
     * peek at the head and fall through to allocation if it
     * mismatches.  In practice every spawn uses the default
     * stack_size, so the head is virtually always a match. */
    if (runloom_coro_pool != NULL && runloom_coro_pool->stack_size == rounded) {
        c = runloom_coro_pool;
        runloom_coro_pool = c->pool_next;
        runloom_coro_pool_size--;
        c->pool_next = NULL;
        runloom_delay_inject(RUNLOOM_DLY_CORO_ACQUIRE);   /* widen reuse window */
        RUNLOOM_EVT(RUNLOOM_EVT_CORO_ACQUIRE, c, c->stack, (long long)rounded);
        runloom_coro_assert_idle(c, "coro REACQUIRED while a fiber is still executing on it");
        /* Stack was poisoned when this coro was recycled (see destroy);
         * unpoison before the fiber runs on it again. */
        RUNLOOM_UNPOISON(c->stack, rounded);
        c->entry = entry;
        c->user = user;
        c->done = 0;
        c->fresh = 0;
        c->asm_coro.entry = runloom_fcontext_entry;
        c->asm_coro.user = c;
        c->asm_coro.done = 0;
        runloom_stack_paint(c->stack, rounded);
        stack_top = (void *)((uintptr_t)c->stack + rounded);
        runloom_asm_make_ctx(&c->asm_coro, stack_top);
        return c;
    }

    c = (runloom_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->stack = runloom_stack_acquire(rounded);
    if (c->stack == NULL) { free(c); return NULL; }
    c->stack_size = rounded;
    runloom_stack_paint(c->stack, rounded);

    c->asm_coro.entry = runloom_fcontext_entry;
    c->asm_coro.user = c;
    stack_top = (void *)((uintptr_t)c->stack + rounded);
    runloom_asm_make_ctx(&c->asm_coro, stack_top);

    return c;
}

/* ---- bulk/arena fast path (go_n) ---------------------------------------- *
 * Placement coro: initialise a coroutine in CALLER-PROVIDED memory `mem`
 * (>= runloom_coro_struct_size() bytes) on a CALLER-PROVIDED `stack` (lowest
 * usable byte, `stack_size` usable bytes).  No malloc, no stack-acquire, no
 * pool, no lock -- a straight-line set of stores + asm_make_ctx.  Used by the
 * bulk-spawn path where g-structs, coro-structs and stacks all come from
 * pre-allocated arenas.  The caller owns `mem` and `stack`; do NOT call
 * runloom_coro_destroy on a placement coro (it would pool/free arena memory)
 * -- the arena is reclaimed wholesale. */
runloom_coro_t *runloom_coro_init_at(void *mem, size_t stack_size, void *stack,
                                     runloom_entry_fn entry, void *user)
{
    runloom_coro_t *c = (runloom_coro_t *)mem;
    size_t rounded;
    if (stack_size < 4096) stack_size = 4096;
    rounded = runloom_round_to_page(stack_size);
    c->entry = entry;
    c->user = user;
    c->done = 0;
    c->dbg_running = 0;
    c->pool_next = NULL;
    c->stack = stack;
    c->stack_size = rounded;
    c->grown = 0;
    c->fresh = 0;
    c->asm_coro.entry = runloom_fcontext_entry;
    c->asm_coro.user = c;
    c->asm_coro.done = 0;
    runloom_asm_make_ctx(&c->asm_coro, (void *)((uintptr_t)stack + rounded));
    return c;
}

/* Bytes a placement coro needs. */
size_t runloom_coro_struct_size(void) { return sizeof(runloom_coro_t); }

/* Bulk coro init: fill an ENTIRE coro arena (n structs) inline, each on its own
 * stack carved from one reserved arena block, and write each g's coro pointer.
 * ONE call for all N -- the per-coro work (field stores + asm_make_ctx) is
 * inlined here (same TU), so the caller's spawn loop makes ZERO per-g function
 * calls into the coro layer.  The only irreducible per-g cost left is the
 * asm_make_ctx stack write (the page fault).  g_arena/g_stride/g_coro_off
 * locate each g and its `coro` field so we can set g->coro = &coro_arena[i].
 * Returns 0, or -1 if the stack arena is off/exhausted/size-mismatched (caller
 * falls back to the per-g path).  fcontext backend only. */
int runloom_coro_bulk_init(void *coro_arena, size_t coro_stride,
                           void *g_arena, size_t g_stride, size_t g_coro_off,
                           size_t stack_size, long n, runloom_entry_fn entry,
                           size_t *start_slot_out)
{
    size_t guard, rounded, slot, start;
    char *sbase;
    long i;
    /* Fresh-flag deferral gate (RUNLOOM_GON_FRESH=1): skip the per-g
     * asm_make_ctx stack write here and mark each coro `fresh`, so the owning
     * hub materializes the frame at first resume (faults move off the spawner,
     * onto the H hubs in parallel).  Read once, cached. */
    static int fresh_defer = -1;
    int defer = __atomic_load_n(&fresh_defer, __ATOMIC_RELAXED);
    if (defer < 0) {
        const char *e = getenv("RUNLOOM_GON_FRESH");
        defer = (e != NULL && *e == '1') ? 1 : 0;
        __atomic_store_n(&fresh_defer, defer, __ATOMIC_RELAXED);
    }
    if (stack_size < 4096) stack_size = 4096;
    rounded = runloom_round_to_page(stack_size);
    guard = runloom_stack_guard();
    slot = guard + rounded;
    /* Reserve a contiguous block of n slots via the locked allocator (lazy-inits
     * the arena, reuses freed space).  Report the start slot so the caller's
     * batch teardown can MADV + return the whole block when the last g finishes. */
    if (runloom_arena_alloc(n, slot, &start) != 0)
        return -1;                                  /* off/exhausted -> fallback */
    sbase = runloom_arena_base + start * slot + guard;   /* usable base of slot 0 */
    if (start_slot_out) *start_slot_out = start;
    for (i = 0; i < n; i++) {
        runloom_coro_t *c = (runloom_coro_t *)((char *)coro_arena + (size_t)i * coro_stride);
        char *g = (char *)g_arena + (size_t)i * g_stride;
        char *stk = sbase + (size_t)i * slot;
        c->entry = entry;
        c->user = g;
        c->done = 0;
        c->dbg_running = 0;
        c->pool_next = NULL;
        c->stack = stk;
        c->stack_size = rounded;
        c->grown = 0;
        c->asm_coro.entry = runloom_fcontext_entry;
        c->asm_coro.user = c;
        c->asm_coro.done = 0;
        c->fresh = defer;
        if (!defer)                                 /* eager: write frame now */
            runloom_asm_make_ctx(&c->asm_coro, (void *)((uintptr_t)stk + rounded));
        /* deferred: leave self.sp/caller.sp zero (calloc); resume materializes */
        *(void **)(g + g_coro_off) = c;             /* g->coro = c */
    }
    return 0;
}

/* Release a bulk stack block (n slots from start_slot): drop its physical pages
 * back to the OS (MADV_DONTNEED -- the virtual reservation stays) AND return the
 * slots to the allocator for reuse.  Called by the go_n batch teardown when the
 * last fiber in a batch finishes.  The block stays PROT_READ|WRITE; the next
 * fault into it gets a fresh zero page -- exactly what the fresh-flag path wants
 * (a zero stack reads back as a not-yet-materialised frame). */
void runloom_coro_arena_release(size_t start_slot, long n)
{
    char  *base = __atomic_load_n(&runloom_arena_base, __ATOMIC_ACQUIRE);
    size_t slot = runloom_arena_slot;
    if (base == NULL || n <= 0) return;
#if defined(MADV_DONTNEED)
    /* MADV is OPT-IN (RUNLOOM_GON_TRIM=1).  By default we KEEP the pages warm:
     * the fresh-flag WRITES each stack frame at resume (never reads-as-zero), so
     * zeroing buys no correctness, and since reset-when-empty reuses the SAME
     * address range, the next batch would just re-fault every page we dropped --
     * pure waste (madvise of a 1M-slot block is a ~2s page-table walk).  RSS is
     * already bounded by the live working set via the cursor reset.  Trim is for
     * the spawn-a-burst-then-go-idle case where returning RSS matters more. */
    static int trim = -1;
    if (trim < 0) {
        const char *e = getenv("RUNLOOM_GON_TRIM");
        __atomic_store_n(&trim, (e && *e == '1') ? 1 : 0, __ATOMIC_RELAXED);
    }
    if (trim) madvise(base + start_slot * slot, (size_t)n * slot, MADV_DONTNEED);
#endif
    runloom_arena_free(start_slot, n);
}

/* Carve one stack (lowest usable byte) from the bulk arena, NULL if the arena
 * is off/exhausted/size-mismatched.  Rounds like coro_new so sizes match. */
void *runloom_coro_arena_stack(size_t stack_size)
{
    size_t rounded;
    if (stack_size < 4096) stack_size = 4096;
    rounded = runloom_round_to_page(stack_size);
    return runloom_stack_arena_carve(rounded);
}

void runloom_coro_destroy(runloom_coro_t *c)
{
    if (c == NULL) return;
    runloom_coro_assert_idle(c, "coro RELEASED while a fiber is executing on it");
    /* Security scrub (opt-in): wipe the stack before it is recycled OR
     * released, so a later fiber reusing it sees zero, not this
     * fiber's leftovers.  Covers both the coro-pool fast path (which
     * keeps the stack attached, unscrubbed) and the stack-pool path. */
    if (runloom_stack_scrub_on && c->stack != NULL) {
        runloom_stack_scrub(c->stack, c->stack_size);
    }
    /* Recycle if there's room.  Stack stays attached -- next
     * runloom_coro_new pop reuses it without touching the stack pool.
     * EXCEPT a copy-grown coro: its oversized stack won't match the
     * default-size reuse check, so pooling it would just park a big
     * stack at the head and defeat the pool for every later default
     * spawn.  Release it instead so its pages go back promptly. */
    if (!c->grown && runloom_coro_pool_size < RUNLOOM_CORO_POOL_CAP
        && c->stack != NULL) {
        c->pool_next = runloom_coro_pool;
        runloom_coro_pool = c;
        runloom_coro_pool_size++;
        /* Poison the attached stack while the coro sits in the pool so ASan
         * flags any use-after-recycle of it; unpoisoned on reuse. */
        RUNLOOM_POISON(c->stack, c->stack_size);
        RUNLOOM_EVT(RUNLOOM_EVT_CORO_RELEASE, c, c->stack, (long long)c->stack_size);
        runloom_delay_inject(RUNLOOM_DLY_CORO_RELEASE);   /* widen reuse window */
        return;
    }
    if (c->stack != NULL) {
        runloom_stack_release(c->stack, c->stack_size);
    }
    free(c);
}

/* Copy-on-grow (Path A): grow a SUSPENDED coro's stack to new_usable
 * bytes.  Called only from the resume path, where the coro is suspended
 * at a swap boundary: its entire live state is the fcontext frame at
 * self.sp plus the call chain above it, and self.sp is the lowest live
 * address.  We are NOT in a signal handler -- there are no arbitrary
 * volatile registers to fix up, only self.sp + the copied stack bytes
 * (which include the saved callee-saved frame).  Stacks grow down, so
 * we align the HIGH ends (every live byte keeps its offset-from-top)
 * and add `delta` to any 8-byte word that points back into the old
 * usable range.  Returns 0 on success (coro now on the bigger stack),
 * -1 on failure (coro untouched, keeps its old stack + guard). */
static int runloom_coro_grow(runloom_coro_t *c, size_t new_usable)
{
    size_t old_usable = c->stack_size;
    uintptr_t old_lo, old_hi, sp, new_lo, new_hi;
    intptr_t delta;
    void *new_stack;
    size_t live;

    new_usable = runloom_round_to_page(new_usable);
    if (new_usable <= old_usable) return 0;

    old_lo = (uintptr_t)c->stack;
    old_hi = old_lo + old_usable;
    sp     = (uintptr_t)c->asm_coro.self.sp;
    if (sp < old_lo || sp > old_hi) return -1;   /* sp out of range: bail */

    new_stack = runloom_stack_map_guarded(new_usable);
    if (new_stack == NULL) return -1;
    new_lo = (uintptr_t)new_stack;
    new_hi = new_lo + new_usable;
    delta  = (intptr_t)(new_hi - old_hi);

    /* Copy the live region [sp, old_hi) to [sp+delta, new_hi). */
    live = (size_t)(old_hi - sp);
    memcpy((void *)(sp + (uintptr_t)delta), (const void *)sp, live);

    /* Rewrite interior stack pointers in the copied live region. */
    {
        uintptr_t *p   = (uintptr_t *)(sp + (uintptr_t)delta);
        uintptr_t *end = (uintptr_t *)new_hi;
        for (; p < end; p++) {
            uintptr_t v = *p;
            if (v >= old_lo && v < old_hi) {
                *p = (uintptr_t)((intptr_t)v + delta);
            }
        }
    }

    /* Patch the saved SP, swap in the new region, drop the old. */
    c->asm_coro.self.sp = (void *)((intptr_t)sp + delta);
    {
        void *old_stack = c->stack;
        size_t old_sz   = c->stack_size;
        c->stack      = new_stack;
        c->stack_size = new_usable;
        c->grown      = 1;
        runloom_stack_unmap_guarded(old_stack, old_sz);
    }
    return 0;
}

/* Grow heuristic, checked at every resume.  If the suspended coro is
 * using more than ~3/4 of its usable stack (little headroom below
 * self.sp), double it (page-rounded, capped at RUNLOOM_STACK_GROW_MAX).
 * This is the Path-A safe-point grow: it grows fibers that
 * legitimately deepen ACROSS yields, which is what lets us ship a small
 * default stack.  It cannot rescue a deep NON-yielding burst between
 * two yields -- that overflows into the guard page (clean SIGSEGV, not
 * silent corruption); such code must set a larger stack explicitly or
 * (for known deep stdlib paths) be pre-warmed.  Env RUNLOOM_STACK_GROW=0
 * disables. */
#define RUNLOOM_STACK_GROW_MAX (8u << 20)   /* 8 MB ceiling (matches MAX_STACK) */
static int runloom_coro_maybe_grow(runloom_coro_t *c)
{
    static int grow_on = -1;
    int on = __atomic_load_n(&grow_on, __ATOMIC_RELAXED);
    uintptr_t sp, lo, headroom, quarter;
    if (on < 0) {
        const char *e = getenv("RUNLOOM_STACK_GROW");
        on = (e != NULL && *e == '0') ? 0 : 1;     /* default ON */
        __atomic_store_n(&grow_on, on, __ATOMIC_RELAXED);
    }
    if (!on || c == NULL || c->stack == NULL || c->done) return 0;
    if (c->stack_size >= RUNLOOM_STACK_GROW_MAX) return 0;
    sp = (uintptr_t)c->asm_coro.self.sp;
    lo = (uintptr_t)c->stack;
    if (sp <= lo) return 0;            /* invalid/overflowed: guard owns it */
    headroom = sp - lo;
    quarter  = (uintptr_t)(c->stack_size >> 2);
    if (headroom < quarter) {
        size_t target = c->stack_size << 1;
        if (target > RUNLOOM_STACK_GROW_MAX) target = RUNLOOM_STACK_GROW_MAX;
        return runloom_coro_grow(c, target);
    }
    return 0;
}

void runloom_coro_resume(runloom_coro_t *c)
{
    runloom_coro_t *prev = runloom_tls_current;
    if (c->fresh) {
        /* Deferred bulk init (go_n fresh-flag): bulk_init skipped the initial
         * fcontext frame write at spawn to keep the 1M scattered stack-top page
         * faults OFF the single spawner thread.  Materialize it now, on the
         * OWNING hub, just before the first swap -- so those faults land on the
         * H hubs in parallel, overlapped with the run.  First resume only; the
         * flag self-clears.  asm_make_ctx (re)sets self.sp/caller.sp/done; the
         * calloc'd arena left them zero, which maybe_grow below treats as
         * "no headroom info" (no-op) -- but we run before it anyway. */
        runloom_asm_make_ctx(&c->asm_coro,
                             (void *)((uintptr_t)c->stack + c->stack_size));
        c->fresh = 0;
    }
    runloom_coro_maybe_grow(c);     /* Path-A copy-grow at the resume boundary */
    runloom_tls_current = c;
    if (RUNLOOM_DBG_ON(RUNLOOM_DBG_INVARIANTS))
        __atomic_store_n(&c->dbg_running, 1, __ATOMIC_RELEASE);
    runloom_asm_swap(&c->asm_coro.caller, &c->asm_coro.self);
    if (RUNLOOM_DBG_ON(RUNLOOM_DBG_INVARIANTS))
        __atomic_store_n(&c->dbg_running, 0, __ATOMIC_RELEASE);
    runloom_tls_current = prev;
}

void runloom_coro_yield(void)
{
    runloom_coro_t *c = runloom_tls_current;
    if (c == NULL) return;
    runloom_asm_swap(&c->asm_coro.self, &c->asm_coro.caller);
}

int runloom_coro_done(const runloom_coro_t *c)
{
    return c ? (c->done || c->asm_coro.done) : 1;
}

#endif  /* RUNLOOM_HAVE_FCONTEXT */

/* ================================================================== */
/* Backend: Windows Fibers                                            */
/* ================================================================== */

#if defined(RUNLOOM_HAVE_FIBERS)

static VOID CALLBACK runloom_fiber_entry(LPVOID arg)
{
    runloom_coro_t *c = (runloom_coro_t *)arg;
    c->entry(c->user);
    c->done = 1;
    SwitchToFiber(runloom_tls_caller_fiber);
    for (;;) { SwitchToFiber(runloom_tls_caller_fiber); }
}

runloom_coro_t *runloom_coro_new(size_t stack_size,
                           runloom_entry_fn entry,
                           void *user)
{
    runloom_coro_t *c;
    if (runloom_coro_thread_init() != 0) return NULL;
    c = (runloom_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    /* CreateFiberEx, not CreateFiber: CreateFiber COMMITS the whole stack_size
     * (Windows charges committed pages against the commit limit = RAM+pagefile
     * at commit time, and does NOT overcommit), so a generous stack_size would
     * cost its full size per fiber even untouched -- measured 1000x1MiB =
     * ~1017 MiB commit.  CreateFiberEx reserves stack_size but commits only a
     * small floor, growing on demand via the stack guard page -- the same
     * "reserve big, pay for what you touch" behaviour as the POSIX mmap path
     * (measured: same 1000x1MiB = ~76 MiB commit).  Commit floor = min(stack,
     * 64 KiB); a deeper stack grows committed automatically (MSVC _chkstk
     * probes each page). */
    {
        SIZE_T commit = (stack_size < (64 * 1024)) ? stack_size : (64 * 1024);
        c->fiber = CreateFiberEx(commit, stack_size, 0, runloom_fiber_entry, c);
    }
    if (c->fiber == NULL) { free(c); return NULL; }
    return c;
}

void runloom_coro_destroy(runloom_coro_t *c)
{
    if (c == NULL) return;
    if (c->fiber != NULL) DeleteFiber(c->fiber);
    free(c);
}

void runloom_coro_resume(runloom_coro_t *c)
{
    runloom_coro_t *prev = runloom_tls_current;
    void *prev_caller = runloom_tls_caller_fiber;
    runloom_tls_current = c;
    runloom_tls_caller_fiber = GetCurrentFiber();
    SwitchToFiber(c->fiber);
    runloom_tls_current = prev;
    runloom_tls_caller_fiber = prev_caller;
}

void runloom_coro_yield(void)
{
    SwitchToFiber(runloom_tls_caller_fiber);
}

int runloom_coro_done(const runloom_coro_t *c)
{
    return c ? c->done : 1;
}

#endif /* RUNLOOM_HAVE_FIBERS */

/* ================================================================== */
/* Backend: POSIX ucontext                                            */
/* ================================================================== */

#if defined(RUNLOOM_HAVE_UCONTEXT)

static void runloom_ucontext_entry_lo32_hi32(unsigned int lo, unsigned int hi)
{
    uintptr_t addr = ((uintptr_t)hi << 32) | (uintptr_t)lo;
    runloom_coro_t *c = (runloom_coro_t *)addr;
    c->entry(c->user);
    c->done = 1;
}

#if defined(RUNLOOM_ARCH_X86)
static void runloom_ucontext_entry_one(unsigned int p)
{
    runloom_coro_t *c = (runloom_coro_t *)(uintptr_t)p;
    c->entry(c->user);
    c->done = 1;
}
#endif

runloom_coro_t *runloom_coro_new(size_t stack_size,
                           runloom_entry_fn entry,
                           void *user)
{
    runloom_coro_t *c;
    uintptr_t addr;
    size_t rounded;
    (void)runloom_coro_thread_init();
    if (stack_size < 4096) stack_size = 4096;
    rounded = runloom_round_to_page(stack_size);

    c = (runloom_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->stack = runloom_stack_acquire(rounded);
    if (c->stack == NULL) { free(c); return NULL; }
    c->stack_size = rounded;
    runloom_stack_paint(c->stack, rounded);

    if (getcontext(&c->ctx) != 0) {
        runloom_stack_release(c->stack, c->stack_size);
        free(c);
        return NULL;
    }
    c->ctx.uc_stack.ss_sp = c->stack;
    c->ctx.uc_stack.ss_size = rounded;
    c->ctx.uc_stack.ss_flags = 0;
    c->ctx.uc_link = &c->caller_ctx;

    addr = (uintptr_t)c;
#if defined(RUNLOOM_ARCH_X86)
    makecontext(&c->ctx,
                (void (*)(void))runloom_ucontext_entry_one,
                1, (unsigned int)addr);
#else
    {
        unsigned int lo = (unsigned int)(addr & 0xffffffffu);
        unsigned int hi = (unsigned int)((addr >> 32) & 0xffffffffu);
        makecontext(&c->ctx,
                    (void (*)(void))runloom_ucontext_entry_lo32_hi32,
                    2, lo, hi);
    }
#endif
    return c;
}

void runloom_coro_destroy(runloom_coro_t *c)
{
    if (c == NULL) return;
    if (c->stack != NULL) {
        runloom_stack_release(c->stack, c->stack_size);
    }
    free(c);
}

void runloom_coro_resume(runloom_coro_t *c)
{
    runloom_coro_t *prev = runloom_tls_current;
    runloom_tls_current = c;
    swapcontext(&c->caller_ctx, &c->ctx);
    runloom_tls_current = prev;
}

void runloom_coro_yield(void)
{
    runloom_coro_t *c = runloom_tls_current;
    if (c == NULL) return;
    swapcontext(&c->ctx, &c->caller_ctx);
}

int runloom_coro_done(const runloom_coro_t *c)
{
    return c ? c->done : 1;
}

#endif /* RUNLOOM_HAVE_UCONTEXT */

/* ------------------------------------------------------------------ */
/* Park: drop idle stack pages without releasing the coro             */
/* ------------------------------------------------------------------ */

/* Unconditional madvise of c's below-SP idle stack pages.  Caller owns
 * the gating (the per-park env flag below, or the hub-idle sweep) and
 * the M:N safety contract (only the owning hub may run this, and only
 * while c is suspended -- see the runloom_coro_park doc in coro.h). */
void runloom_coro_madvise_idle(runloom_coro_t *c)
{
#if defined(RUNLOOM_HAVE_FCONTEXT) && defined(MADV_DONTNEED)
    if (c == NULL || c->stack == NULL) return;
    {
        long ps = sysconf(_SC_PAGESIZE);
        size_t page = (ps > 0) ? (size_t)ps : (size_t)4096;
        uintptr_t base = (uintptr_t)c->stack;
        uintptr_t top  = base + c->stack_size;
        /* Saved SP of the suspended coro = lowest live address; the
         * live region is [sp, top).  We only ever drop pages strictly
         * below sp, so no saved register or call frame is touched. */
        uintptr_t sp   = (uintptr_t)c->asm_coro.self.sp;
        uintptr_t lo, hi;
        if (sp <= base || sp > top) return;     /* sanity: sp in range */
        lo = base + page;                       /* keep first page (pool hdr) */
        hi = sp & ~(uintptr_t)(page - 1);       /* page-align DOWN below sp */
        if (hi > lo) {
            /* MADV_FREE (default): ~2.3x cheaper than DONTNEED and no re-fault if
             * this parked fiber resumes before reclaim -- the request/response
             * recv-park case.  Env RUNLOOM_STACK_MADV=dontneed forces eager. */
            runloom_stack_madv_reclaim((void *)lo, (size_t)(hi - lo));
        }
    }
#else
    (void)c;
#endif
}

/* Programmatic override for park-time idle-page reclaim, in addition to the
 * RUNLOOM_STACK_PARK_DONTNEED env.  The stack auto-sizer turns this on when it
 * starts fibers large (so the large idle pages are returned on park),
 * making "start large, learn down" RSS-free without a global env flip. */
static int runloom_park_reclaim_forced = 0;
void runloom_coro_park_reclaim_set(int on)
{
    __atomic_store_n(&runloom_park_reclaim_forced, on ? 1 : 0, __ATOMIC_RELAXED);
}

void runloom_coro_park(runloom_coro_t *c)
{
#if defined(RUNLOOM_HAVE_FCONTEXT) && defined(MADV_DONTNEED)
    /* Opt-in, evaluated once.  getenv reads are safe to race here --
     * every thread computes the same value. */
    static int park_dontneed = -1;
    int on = __atomic_load_n(&park_dontneed, __ATOMIC_RELAXED);
    if (on < 0) {
        const char *e = getenv("RUNLOOM_STACK_PARK_DONTNEED");
        on = (e != NULL && *e == '1') ? 1 : 0;
        __atomic_store_n(&park_dontneed, on, __ATOMIC_RELAXED);
    }
    if (!on && !__atomic_load_n(&runloom_park_reclaim_forced, __ATOMIC_RELAXED)) return;
    runloom_coro_madvise_idle(c);
#else
    (void)c;
#endif
}

/* ------------------------------------------------------------------ */
/* Public scan_hwm                                                    */
/* ------------------------------------------------------------------ */

size_t runloom_coro_scan_hwm(runloom_coro_t *c)
{
#if defined(RUNLOOM_HAVE_FCONTEXT) || defined(RUNLOOM_HAVE_UCONTEXT)
    if (c == NULL || c->stack == NULL || !runloom_stack_paint_on) return 0;
    return runloom_stack_hwm_scan(c->stack, c->stack_size);
#else
    /* Windows Fibers: no introspectable stack. */
    (void)c;
    return 0;
#endif
}
