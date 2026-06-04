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

#include <stdlib.h>
#include <string.h>

/* Recycle-hygiene checker (security): runloom pools and reuses goroutine stacks
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
     * coro/stack being recycled while a goroutine still executes on it. */
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
 * introspectable stack (Fibers).  Used by the goroutine dump. */
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
 * goroutine stack is being recycled -- the use-after-free class behind the
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

static void *runloom_stack_acquire(size_t size)
{
    void **head = runloom_tls_stack_pool;
    if (head != NULL) {
        size_t pooled_size = (size_t)head[RUNLOOM_STACK_HDR_SIZE];
        if (pooled_size == size) {
            runloom_tls_stack_pool = (void **)head[RUNLOOM_STACK_HDR_NEXT];
            /* Caller will overwrite the header bytes as the stack
             * grows; the new coroutine doesn't observe them. */
            RUNLOOM_UNPOISON((void *)head, size);
            return (void *)head;
        }
        /* Size mismatch (different stack_size requested than what the
         * pool has).  Don't walk -- just munmap pooled stacks until
         * the head matches or pool is empty.  Bounded work in the
         * pathological mixed-size case. */
        while (head != NULL && (size_t)head[RUNLOOM_STACK_HDR_SIZE] != size) {
            void **next = (void **)head[RUNLOOM_STACK_HDR_NEXT];
            runloom_stack_unmap_guarded((void *)head,
                                     (size_t)head[RUNLOOM_STACK_HDR_SIZE]);
            head = next;
        }
        runloom_tls_stack_pool = head;
        if (head != NULL) {
            runloom_tls_stack_pool = (void **)head[RUNLOOM_STACK_HDR_NEXT];
            RUNLOOM_UNPOISON((void *)head, size);
            return (void *)head;
        }
    }
    return runloom_stack_map_guarded(size);
}

static void runloom_stack_release(void *stack, size_t size)
{
    void **hdr;
    /* Drop physical pages back to the OS *before* writing the header.
     * MADV_DONTNEED keeps the VA reservation but lets the kernel reclaim
     * the page frames; next touch re-faults a fresh zero page.  We have
     * to skip the first page so the pool linkage survives -- the header
     * lives in the first 16 bytes of the stack.
     *
     * Net effect: pool entries hold 4 KB resident each instead of the
     * full stack_size.  At 4096 pool entries that's 16 MB instead of
     * (4096 * stack_size).  Across pool churn, the deepest-used pages
     * keep faulting fresh, so RSS scales with active gs, not capacity. */
#if defined(MADV_DONTNEED)
    {
        long ps = sysconf(_SC_PAGESIZE);
        size_t page = (ps > 0) ? (size_t)ps : (size_t)4096;
        if (size > page) {
            (void)madvise((char *)stack + page, size - page, MADV_DONTNEED);
        }
    }
#endif
    hdr = (void **)stack;
    hdr[RUNLOOM_STACK_HDR_NEXT] = (void *)runloom_tls_stack_pool;
    hdr[RUNLOOM_STACK_HDR_SIZE] = (void *)size;
    runloom_tls_stack_pool = hdr;
    /* Poison the body (skip the 16-byte pool header read by the next
     * runloom_stack_acquire) so ASan flags any access to this stack while it
     * sits free in the pool. Unpoisoned again on acquire. */
    if (size > 16) {
        RUNLOOM_POISON((char *)stack + 16, size - 16);
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

/* Security: wipe a goroutine's stack when it is recycled, so the next
 * goroutine to reuse that stack can't read this one's leftovers (TLS keys,
 * request bodies -- the aio bridge runs OpenSSL on these stacks). OFF by
 * default: it costs one stack-sized memset per goroutine completion, and the
 * leftover is only reachable via a C extension reading uninitialised stack
 * (Python objects live on the heap, not the goroutine C stack). Enable for
 * security-sensitive workloads via RUNLOOM_STACK_SCRUB=1 or set_stack_scrub(True).
 * (Painting would also overwrite the data, but it is calibrated off for
 * performance after the first few spawns -- so it can't be relied on.) */
static int runloom_stack_scrub_on = 0;
void runloom_coro_scrub_set(int enabled) { runloom_stack_scrub_on = enabled ? 1 : 0; }
int  runloom_coro_scrub_enabled(void)    { return runloom_stack_scrub_on; }

/* Wipe a whole goroutine stack.  On Linux, MADV_DONTNEED frees the page
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
 * is ample since calibration rounds to next_pow2.  A goroutine grows its stack
 * DOWN from the top, faulting pages as it deepens; those pages stay resident
 * until the stack is released (MADV_DONTNEED).  So the contiguous run of
 * resident pages measured DOWN from the top is the deepest the goroutine ever
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
     * the fault is a goroutine stack overflow.  No-op unless installed. */
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
 */
#define RUNLOOM_CORO_POOL_CAP 4096
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
        runloom_coro_assert_idle(c, "coro REACQUIRED while a goroutine is still executing on it");
        /* Stack was poisoned when this coro was recycled (see destroy);
         * unpoison before the goroutine runs on it again. */
        RUNLOOM_UNPOISON(c->stack, rounded);
        c->entry = entry;
        c->user = user;
        c->done = 0;
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

void runloom_coro_destroy(runloom_coro_t *c)
{
    if (c == NULL) return;
    runloom_coro_assert_idle(c, "coro RELEASED while a goroutine is executing on it");
    /* Security scrub (opt-in): wipe the stack before it is recycled OR
     * released, so a later goroutine reusing it sees zero, not this
     * goroutine's leftovers.  Covers both the coro-pool fast path (which
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
 * This is the Path-A safe-point grow: it grows goroutines that
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
    c->fiber = CreateFiber(stack_size, runloom_fiber_entry, c);
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
            (void)madvise((void *)lo, (size_t)(hi - lo), MADV_DONTNEED);
        }
    }
#else
    (void)c;
#endif
}

/* Programmatic override for park-time idle-page reclaim, in addition to the
 * RUNLOOM_STACK_PARK_DONTNEED env.  The stack auto-sizer turns this on when it
 * starts goroutines large (so the large idle pages are returned on park),
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
