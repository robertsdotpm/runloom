/* coro.c -- portable stackful coroutines.  See coro.h for the contract.
 *
 * Three backends, exactly one active per build:
 *   PYGO_HAVE_FCONTEXT  -- hand-rolled inline asm (x86_64 SysV).  Fast path.
 *   PYGO_HAVE_FIBERS    -- Windows Fibers (XP+).
 *   PYGO_HAVE_UCONTEXT  -- POSIX fallback.
 */

#include "coro.h"

#include <stdlib.h>
#include <string.h>

/* Recycle-hygiene checker (security): pygo pools and reuses goroutine stacks
 * in raw mmap'd memory that ASan treats as always-valid, so a use-after-
 * recycle of a stack (the S1 leak class, generalized) is invisible to ASan.
 * Manually poison a stack while it sits in a pool and unpoison it on reuse --
 * then the existing ASan suite flags any access to a recycled-but-not-yet-
 * reacquired stack. No-op unless built with -fsanitize=address. */
#if defined(__SANITIZE_ADDRESS__)
#  define PYGO_ASAN 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define PYGO_ASAN 1
#  endif
#endif
#if defined(PYGO_ASAN)
#  include <sanitizer/asan_interface.h>
#  define PYGO_POISON(p, n)   ASAN_POISON_MEMORY_REGION((p), (n))
#  define PYGO_UNPOISON(p, n) ASAN_UNPOISON_MEMORY_REGION((p), (n))
#else
#  define PYGO_POISON(p, n)   ((void)0)
#  define PYGO_UNPOISON(p, n) ((void)0)
#endif

#if defined(PYGO_HAVE_FCONTEXT)
#  include "fcontext.h"
#  include <sys/mman.h>
#  include <unistd.h>
#  ifndef MAP_ANONYMOUS
#    ifdef MAP_ANON
#      define MAP_ANONYMOUS MAP_ANON
#    endif
#  endif
#elif defined(PYGO_HAVE_FIBERS)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN 1
#  endif
#  include <windows.h>
#elif defined(PYGO_HAVE_UCONTEXT)
#  if defined(PYGO_OS_MACOS) && !defined(_XOPEN_SOURCE)
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

struct pygo_coro {
    pygo_entry_fn entry;
    void *user;
    int done;
    /* Free-list link for the per-thread coro recycle pool.  When the
     * coro is in use, this is undefined; when on the pool free list,
     * it points to the next pooled coro. */
    struct pygo_coro *pool_next;
#if defined(PYGO_HAVE_FCONTEXT)
    pygo_asm_coro_t asm_coro;
    void *stack;
    size_t stack_size;
    int grown;             /* 1 if copy-grow enlarged this stack */
#elif defined(PYGO_HAVE_FIBERS)
    void *fiber;
#elif defined(PYGO_HAVE_UCONTEXT)
    ucontext_t ctx;
    ucontext_t caller_ctx;
    void *stack;
    size_t stack_size;
#endif
};

/* Stack size in bytes for this coro, or 0 on backends without an
 * introspectable stack (Fibers).  Used by the goroutine dump. */
size_t pygo_coro_stack_size(const pygo_coro_t *c)
{
    if (c == NULL) return 0;
#if defined(PYGO_HAVE_FCONTEXT) || defined(PYGO_HAVE_UCONTEXT)
    return c->stack_size;
#else
    return 0;
#endif
}

/* Per-thread "currently executing" pointer.  Used by pygo_coro_yield
 * to find the caller context.  Thread-local. */
static PYGO_TLS pygo_coro_t *pygo_tls_current = NULL;

#if defined(PYGO_HAVE_FIBERS)
static PYGO_TLS void *pygo_tls_caller_fiber = NULL;
static PYGO_TLS int pygo_tls_thread_was_fiber = 0;
#endif

const char *pygo_coro_backend(void)
{
#if defined(PYGO_HAVE_FCONTEXT)
    return "fcontext-asm";
#elif defined(PYGO_HAVE_FIBERS)
    return "fibers";
#elif defined(PYGO_HAVE_UCONTEXT)
    return "ucontext";
#else
    return "unknown";
#endif
}

/* ------------------------------------------------------------------ */
/* Stack pool (POSIX backends)                                        */
/* ------------------------------------------------------------------ */

#if defined(PYGO_HAVE_FCONTEXT) || defined(PYGO_HAVE_UCONTEXT)
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

#define PYGO_STACK_HDR_NEXT  0
#define PYGO_STACK_HDR_SIZE  1

static PYGO_TLS void **pygo_tls_stack_pool = NULL;

/* Guard page below each coroutine stack.  A push past the low end of
 * the usable region lands in this PROT_NONE page -> SIGSEGV, instead of
 * silently corrupting the neighbouring allocation (plain mmap-per-g has
 * no implicit guard).  IMPORTANT: the usable stack the rest of coro.c
 * sees is still [stack, stack+size) with `stack` = lowest usable byte;
 * the guard is one page BELOW `stack`, owned ONLY by acquire/release/
 * warmup here.  region_base == (char *)stack - pygo_stack_guard().  So
 * paint, HWM scan, asm_make_ctx, and the madvise sweep are unchanged --
 * they all operate on the usable region. */
static size_t pygo_stack_guard(void)
{
    long ps = sysconf(_SC_PAGESIZE);
    return (ps > 0) ? (size_t)ps : (size_t)4096;
}

/* mmap a guarded stack [guard PROT_NONE | usable RW]; return the lowest
 * USABLE byte (region_base + guard), or NULL on mmap failure.  If the
 * mprotect fails the region is still usable (just unguarded) so we fall
 * through rather than fail the spawn -- safety degrades, correctness
 * does not. */
static void *pygo_stack_map_guarded(size_t usable)
{
    size_t guard = pygo_stack_guard();
    size_t total = guard + usable;
    /* Deliberately NOT MAP_STACK.  On FreeBSD/macOS MAP_STACK requests a
     * kernel grow-down stack whose lower pages stay inaccessible until the
     * stack grows into them, so eagerly writing the usable region low->high
     * (pygo_stack_paint, and the first asm pushes) faults with "invalid
     * permissions for mapped object".  pygo installs its OWN PROT_NONE guard
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
static void pygo_stack_unmap_guarded(void *usable, size_t usable_size)
{
    size_t guard = pygo_stack_guard();
    munmap((char *)usable - guard, guard + usable_size);
}

static void *pygo_stack_acquire(size_t size)
{
    void **head = pygo_tls_stack_pool;
    if (head != NULL) {
        size_t pooled_size = (size_t)head[PYGO_STACK_HDR_SIZE];
        if (pooled_size == size) {
            pygo_tls_stack_pool = (void **)head[PYGO_STACK_HDR_NEXT];
            /* Caller will overwrite the header bytes as the stack
             * grows; the new coroutine doesn't observe them. */
            PYGO_UNPOISON((void *)head, size);
            return (void *)head;
        }
        /* Size mismatch (different stack_size requested than what the
         * pool has).  Don't walk -- just munmap pooled stacks until
         * the head matches or pool is empty.  Bounded work in the
         * pathological mixed-size case. */
        while (head != NULL && (size_t)head[PYGO_STACK_HDR_SIZE] != size) {
            void **next = (void **)head[PYGO_STACK_HDR_NEXT];
            pygo_stack_unmap_guarded((void *)head,
                                     (size_t)head[PYGO_STACK_HDR_SIZE]);
            head = next;
        }
        pygo_tls_stack_pool = head;
        if (head != NULL) {
            pygo_tls_stack_pool = (void **)head[PYGO_STACK_HDR_NEXT];
            PYGO_UNPOISON((void *)head, size);
            return (void *)head;
        }
    }
    return pygo_stack_map_guarded(size);
}

static void pygo_stack_release(void *stack, size_t size)
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
    hdr[PYGO_STACK_HDR_NEXT] = (void *)pygo_tls_stack_pool;
    hdr[PYGO_STACK_HDR_SIZE] = (void *)size;
    pygo_tls_stack_pool = hdr;
    /* Poison the body (skip the 16-byte pool header read by the next
     * pygo_stack_acquire) so ASan flags any access to this stack while it
     * sits free in the pool. Unpoisoned again on acquire. */
    if (size > 16) {
        PYGO_POISON((char *)stack + 16, size - 16);
    }
}

/* Pre-warm n stacks of the given size into the per-thread pool.
 * Returns the number successfully pre-allocated (may be < n if
 * mmap starts failing partway through). */
static int pygo_stack_warmup_posix(size_t size, int n)
{
    int i;
    for (i = 0; i < n; i++) {
        void *s = pygo_stack_map_guarded(size);
        if (s == NULL) return i;
        pygo_stack_release(s, size);
    }
    return n;
}

static size_t pygo_round_to_page(size_t size)
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

/* Sentinel value used to fill unused stack memory.  Picked so that
 * the byte sequence is unlikely to appear naturally in compiler-
 * emitted code or Python interpreter state.  The 8-byte alignment of
 * the chunk lets the paint/scan loop run at memory bandwidth. */
static const uint64_t PYGO_STACK_SENTINEL = 0x504AE6B7C9D1F2A3ULL;

/* Global toggle.  Disabled after calibration finishes so steady-state
 * spawns don't pay the paint cost. */
static int pygo_stack_paint_on = 1;

void pygo_coro_paint_set(int enabled) { pygo_stack_paint_on = enabled ? 1 : 0; }
int  pygo_coro_paint_enabled(void)    { return pygo_stack_paint_on; }

/* Security: wipe a goroutine's stack when it is recycled, so the next
 * goroutine to reuse that stack can't read this one's leftovers (TLS keys,
 * request bodies -- the aio bridge runs OpenSSL on these stacks). OFF by
 * default: it costs one stack-sized memset per goroutine completion, and the
 * leftover is only reachable via a C extension reading uninitialised stack
 * (Python objects live on the heap, not the goroutine C stack). Enable for
 * security-sensitive workloads via PYGO_STACK_SCRUB=1 or set_stack_scrub(True).
 * (Painting would also overwrite the data, but it is calibrated off for
 * performance after the first few spawns -- so it can't be relied on.) */
static int pygo_stack_scrub_on = 0;
void pygo_coro_scrub_set(int enabled) { pygo_stack_scrub_on = enabled ? 1 : 0; }
int  pygo_coro_scrub_enabled(void)    { return pygo_stack_scrub_on; }

/* Wipe a whole goroutine stack.  On Linux, MADV_DONTNEED frees the page
 * frames and the next touch re-faults a zero page -- a complete scrub that
 * costs an O(1) syscall instead of a stack-sized memset (a 512 KB memset
 * was ~60x the spawn cost in measurement; this is ~flat).  Elsewhere
 * MADV_DONTNEED is only advisory (may not zero), so fall back to memset for
 * a guaranteed wipe.  stack is page-aligned and size page-rounded. */
static void pygo_stack_scrub(void *stack, size_t size)
{
#if defined(__linux__) && defined(MADV_DONTNEED)
    (void)madvise(stack, size, MADV_DONTNEED);
#else
    memset(stack, 0, size);
#endif
}

#if defined(PYGO_HAVE_FCONTEXT) || defined(PYGO_HAVE_UCONTEXT)
/* Paint every 8-byte chunk of the stack body with the sentinel.
 * Skip the first 16 bytes (reserved for the pool's free-list header)
 * and the last 256 bytes (asm_make_ctx writes the initial register
 * frame there; that area is rewritten by every resume so painting
 * it just inflates the HWM scan). */
static void pygo_stack_paint(void *stack, size_t size)
{
    uint64_t *p, *end;
    if (!pygo_stack_paint_on) return;
    p   = (uint64_t *)((uintptr_t)stack + 16);
    end = (uint64_t *)((uintptr_t)stack + size - 256);
    while (p < end) *p++ = PYGO_STACK_SENTINEL;
}

/* Scan low->high; the first non-sentinel word marks the deepest write.
 * Stack grows DOWN, so deepest write == lowest address; reported HWM
 * is (top - deepest_addr) bytes. */
static size_t pygo_stack_hwm_scan(void *stack, size_t size)
{
    uint64_t *p, *end;
    uintptr_t deepest;
    p   = (uint64_t *)((uintptr_t)stack + 16);
    end = (uint64_t *)((uintptr_t)stack + size - 256);
    while (p < end && *p == PYGO_STACK_SENTINEL) p++;
    if (p >= end) return 0;
    deepest = (uintptr_t)p;
    return (size_t)(((uintptr_t)stack + size) - deepest);
}
#endif

/* ------------------------------------------------------------------ */
/* Thread init / fini                                                 */
/* ------------------------------------------------------------------ */

int pygo_coro_thread_init(void)
{
#if defined(PYGO_HAVE_FIBERS)
    if (pygo_tls_caller_fiber == NULL) {
        void *f = ConvertThreadToFiber(NULL);
        if (f == NULL) return -1;
        pygo_tls_caller_fiber = f;
        pygo_tls_thread_was_fiber = 1;
    }
#endif
    return 0;
}

void pygo_coro_thread_fini(void)
{
#if defined(PYGO_HAVE_FIBERS)
    if (pygo_tls_thread_was_fiber) {
        ConvertFiberToThread();
        pygo_tls_caller_fiber = NULL;
        pygo_tls_thread_was_fiber = 0;
    }
#endif
}

int pygo_coro_warmup(size_t stack_size, int n)
{
    if (n <= 0) return 0;
#if defined(PYGO_HAVE_FCONTEXT) || defined(PYGO_HAVE_UCONTEXT)
    {
        size_t rounded = pygo_round_to_page(
            stack_size < 4096 ? 4096 : stack_size);
        return pygo_stack_warmup_posix(rounded, n);
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

#if defined(PYGO_HAVE_FCONTEXT)

/* Bridge from asm coro entry -> user entry.  Set as
 * pygo_asm_coro_t.entry by pygo_coro_new. */
static void pygo_fcontext_entry(void *user)
{
    pygo_coro_t *c = (pygo_coro_t *)user;
    c->entry(c->user);
    /* When we return, pygo_asm_entry sets done=1 and loops back to
     * caller via pygo_asm_swap -- never returns here. */
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
#define PYGO_CORO_POOL_CAP 4096
static PYGO_TLS pygo_coro_t *pygo_coro_pool = NULL;
static PYGO_TLS int pygo_coro_pool_size = 0;

pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user)
{
    pygo_coro_t *c;
    size_t rounded;
    void *stack_top;

    if (stack_size < 4096) stack_size = 4096;
    rounded = pygo_round_to_page(stack_size);

    /* Recycle if the pool has a compatible coro (same stack size).
     * Walking the chain to find a size match would be O(N); we just
     * peek at the head and fall through to allocation if it
     * mismatches.  In practice every spawn uses the default
     * stack_size, so the head is virtually always a match. */
    if (pygo_coro_pool != NULL && pygo_coro_pool->stack_size == rounded) {
        c = pygo_coro_pool;
        pygo_coro_pool = c->pool_next;
        pygo_coro_pool_size--;
        c->pool_next = NULL;
        /* Stack was poisoned when this coro was recycled (see destroy);
         * unpoison before the goroutine runs on it again. */
        PYGO_UNPOISON(c->stack, rounded);
        c->entry = entry;
        c->user = user;
        c->done = 0;
        c->asm_coro.entry = pygo_fcontext_entry;
        c->asm_coro.user = c;
        c->asm_coro.done = 0;
        pygo_stack_paint(c->stack, rounded);
        stack_top = (void *)((uintptr_t)c->stack + rounded);
        pygo_asm_make_ctx(&c->asm_coro, stack_top);
        return c;
    }

    c = (pygo_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->stack = pygo_stack_acquire(rounded);
    if (c->stack == NULL) { free(c); return NULL; }
    c->stack_size = rounded;
    pygo_stack_paint(c->stack, rounded);

    c->asm_coro.entry = pygo_fcontext_entry;
    c->asm_coro.user = c;
    stack_top = (void *)((uintptr_t)c->stack + rounded);
    pygo_asm_make_ctx(&c->asm_coro, stack_top);

    return c;
}

void pygo_coro_destroy(pygo_coro_t *c)
{
    if (c == NULL) return;
    /* Security scrub (opt-in): wipe the stack before it is recycled OR
     * released, so a later goroutine reusing it sees zero, not this
     * goroutine's leftovers.  Covers both the coro-pool fast path (which
     * keeps the stack attached, unscrubbed) and the stack-pool path. */
    if (pygo_stack_scrub_on && c->stack != NULL) {
        pygo_stack_scrub(c->stack, c->stack_size);
    }
    /* Recycle if there's room.  Stack stays attached -- next
     * pygo_coro_new pop reuses it without touching the stack pool.
     * EXCEPT a copy-grown coro: its oversized stack won't match the
     * default-size reuse check, so pooling it would just park a big
     * stack at the head and defeat the pool for every later default
     * spawn.  Release it instead so its pages go back promptly. */
    if (!c->grown && pygo_coro_pool_size < PYGO_CORO_POOL_CAP
        && c->stack != NULL) {
        c->pool_next = pygo_coro_pool;
        pygo_coro_pool = c;
        pygo_coro_pool_size++;
        /* Poison the attached stack while the coro sits in the pool so ASan
         * flags any use-after-recycle of it; unpoisoned on reuse. */
        PYGO_POISON(c->stack, c->stack_size);
        return;
    }
    if (c->stack != NULL) {
        pygo_stack_release(c->stack, c->stack_size);
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
static int pygo_coro_grow(pygo_coro_t *c, size_t new_usable)
{
    size_t old_usable = c->stack_size;
    uintptr_t old_lo, old_hi, sp, new_lo, new_hi;
    intptr_t delta;
    void *new_stack;
    size_t live;

    new_usable = pygo_round_to_page(new_usable);
    if (new_usable <= old_usable) return 0;

    old_lo = (uintptr_t)c->stack;
    old_hi = old_lo + old_usable;
    sp     = (uintptr_t)c->asm_coro.self.sp;
    if (sp < old_lo || sp > old_hi) return -1;   /* sp out of range: bail */

    new_stack = pygo_stack_map_guarded(new_usable);
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
        pygo_stack_unmap_guarded(old_stack, old_sz);
    }
    return 0;
}

/* Grow heuristic, checked at every resume.  If the suspended coro is
 * using more than ~3/4 of its usable stack (little headroom below
 * self.sp), double it (page-rounded, capped at PYGO_STACK_GROW_MAX).
 * This is the Path-A safe-point grow: it grows goroutines that
 * legitimately deepen ACROSS yields, which is what lets us ship a small
 * default stack.  It cannot rescue a deep NON-yielding burst between
 * two yields -- that overflows into the guard page (clean SIGSEGV, not
 * silent corruption); such code must set a larger stack explicitly or
 * (for known deep stdlib paths) be pre-warmed.  Env PYGO_STACK_GROW=0
 * disables. */
#define PYGO_STACK_GROW_MAX (8u << 20)   /* 8 MB ceiling (matches MAX_STACK) */
static int pygo_coro_maybe_grow(pygo_coro_t *c)
{
    static int grow_on = -1;
    int on = __atomic_load_n(&grow_on, __ATOMIC_RELAXED);
    uintptr_t sp, lo, headroom, quarter;
    if (on < 0) {
        const char *e = getenv("PYGO_STACK_GROW");
        on = (e != NULL && *e == '0') ? 0 : 1;     /* default ON */
        __atomic_store_n(&grow_on, on, __ATOMIC_RELAXED);
    }
    if (!on || c == NULL || c->stack == NULL || c->done) return 0;
    if (c->stack_size >= PYGO_STACK_GROW_MAX) return 0;
    sp = (uintptr_t)c->asm_coro.self.sp;
    lo = (uintptr_t)c->stack;
    if (sp <= lo) return 0;            /* invalid/overflowed: guard owns it */
    headroom = sp - lo;
    quarter  = (uintptr_t)(c->stack_size >> 2);
    if (headroom < quarter) {
        size_t target = c->stack_size << 1;
        if (target > PYGO_STACK_GROW_MAX) target = PYGO_STACK_GROW_MAX;
        return pygo_coro_grow(c, target);
    }
    return 0;
}

void pygo_coro_resume(pygo_coro_t *c)
{
    pygo_coro_t *prev = pygo_tls_current;
    pygo_coro_maybe_grow(c);     /* Path-A copy-grow at the resume boundary */
    pygo_tls_current = c;
    pygo_asm_swap(&c->asm_coro.caller, &c->asm_coro.self);
    pygo_tls_current = prev;
}

void pygo_coro_yield(void)
{
    pygo_coro_t *c = pygo_tls_current;
    if (c == NULL) return;
    pygo_asm_swap(&c->asm_coro.self, &c->asm_coro.caller);
}

int pygo_coro_done(const pygo_coro_t *c)
{
    return c ? (c->done || c->asm_coro.done) : 1;
}

#endif  /* PYGO_HAVE_FCONTEXT */

/* ================================================================== */
/* Backend: Windows Fibers                                            */
/* ================================================================== */

#if defined(PYGO_HAVE_FIBERS)

static VOID CALLBACK pygo_fiber_entry(LPVOID arg)
{
    pygo_coro_t *c = (pygo_coro_t *)arg;
    c->entry(c->user);
    c->done = 1;
    SwitchToFiber(pygo_tls_caller_fiber);
    for (;;) { SwitchToFiber(pygo_tls_caller_fiber); }
}

pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user)
{
    pygo_coro_t *c;
    if (pygo_coro_thread_init() != 0) return NULL;
    c = (pygo_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->fiber = CreateFiber(stack_size, pygo_fiber_entry, c);
    if (c->fiber == NULL) { free(c); return NULL; }
    return c;
}

void pygo_coro_destroy(pygo_coro_t *c)
{
    if (c == NULL) return;
    if (c->fiber != NULL) DeleteFiber(c->fiber);
    free(c);
}

void pygo_coro_resume(pygo_coro_t *c)
{
    pygo_coro_t *prev = pygo_tls_current;
    void *prev_caller = pygo_tls_caller_fiber;
    pygo_tls_current = c;
    pygo_tls_caller_fiber = GetCurrentFiber();
    SwitchToFiber(c->fiber);
    pygo_tls_current = prev;
    pygo_tls_caller_fiber = prev_caller;
}

void pygo_coro_yield(void)
{
    SwitchToFiber(pygo_tls_caller_fiber);
}

int pygo_coro_done(const pygo_coro_t *c)
{
    return c ? c->done : 1;
}

#endif /* PYGO_HAVE_FIBERS */

/* ================================================================== */
/* Backend: POSIX ucontext                                            */
/* ================================================================== */

#if defined(PYGO_HAVE_UCONTEXT)

static void pygo_ucontext_entry_lo32_hi32(unsigned int lo, unsigned int hi)
{
    uintptr_t addr = ((uintptr_t)hi << 32) | (uintptr_t)lo;
    pygo_coro_t *c = (pygo_coro_t *)addr;
    c->entry(c->user);
    c->done = 1;
}

#if defined(PYGO_ARCH_X86)
static void pygo_ucontext_entry_one(unsigned int p)
{
    pygo_coro_t *c = (pygo_coro_t *)(uintptr_t)p;
    c->entry(c->user);
    c->done = 1;
}
#endif

pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user)
{
    pygo_coro_t *c;
    uintptr_t addr;
    size_t rounded;
    (void)pygo_coro_thread_init();
    if (stack_size < 4096) stack_size = 4096;
    rounded = pygo_round_to_page(stack_size);

    c = (pygo_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->stack = pygo_stack_acquire(rounded);
    if (c->stack == NULL) { free(c); return NULL; }
    c->stack_size = rounded;
    pygo_stack_paint(c->stack, rounded);

    if (getcontext(&c->ctx) != 0) {
        pygo_stack_release(c->stack, c->stack_size);
        free(c);
        return NULL;
    }
    c->ctx.uc_stack.ss_sp = c->stack;
    c->ctx.uc_stack.ss_size = rounded;
    c->ctx.uc_stack.ss_flags = 0;
    c->ctx.uc_link = &c->caller_ctx;

    addr = (uintptr_t)c;
#if defined(PYGO_ARCH_X86)
    makecontext(&c->ctx,
                (void (*)(void))pygo_ucontext_entry_one,
                1, (unsigned int)addr);
#else
    {
        unsigned int lo = (unsigned int)(addr & 0xffffffffu);
        unsigned int hi = (unsigned int)((addr >> 32) & 0xffffffffu);
        makecontext(&c->ctx,
                    (void (*)(void))pygo_ucontext_entry_lo32_hi32,
                    2, lo, hi);
    }
#endif
    return c;
}

void pygo_coro_destroy(pygo_coro_t *c)
{
    if (c == NULL) return;
    if (c->stack != NULL) {
        pygo_stack_release(c->stack, c->stack_size);
    }
    free(c);
}

void pygo_coro_resume(pygo_coro_t *c)
{
    pygo_coro_t *prev = pygo_tls_current;
    pygo_tls_current = c;
    swapcontext(&c->caller_ctx, &c->ctx);
    pygo_tls_current = prev;
}

void pygo_coro_yield(void)
{
    pygo_coro_t *c = pygo_tls_current;
    if (c == NULL) return;
    swapcontext(&c->ctx, &c->caller_ctx);
}

int pygo_coro_done(const pygo_coro_t *c)
{
    return c ? c->done : 1;
}

#endif /* PYGO_HAVE_UCONTEXT */

/* ------------------------------------------------------------------ */
/* Park: drop idle stack pages without releasing the coro             */
/* ------------------------------------------------------------------ */

/* Unconditional madvise of c's below-SP idle stack pages.  Caller owns
 * the gating (the per-park env flag below, or the hub-idle sweep) and
 * the M:N safety contract (only the owning hub may run this, and only
 * while c is suspended -- see the pygo_coro_park doc in coro.h). */
void pygo_coro_madvise_idle(pygo_coro_t *c)
{
#if defined(PYGO_HAVE_FCONTEXT) && defined(MADV_DONTNEED)
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

void pygo_coro_park(pygo_coro_t *c)
{
#if defined(PYGO_HAVE_FCONTEXT) && defined(MADV_DONTNEED)
    /* Opt-in, evaluated once.  getenv reads are safe to race here --
     * every thread computes the same value. */
    static int park_dontneed = -1;
    int on = __atomic_load_n(&park_dontneed, __ATOMIC_RELAXED);
    if (on < 0) {
        const char *e = getenv("PYGO_STACK_PARK_DONTNEED");
        on = (e != NULL && *e == '1') ? 1 : 0;
        __atomic_store_n(&park_dontneed, on, __ATOMIC_RELAXED);
    }
    if (!on) return;
    pygo_coro_madvise_idle(c);
#else
    (void)c;
#endif
}

/* ------------------------------------------------------------------ */
/* Public scan_hwm                                                    */
/* ------------------------------------------------------------------ */

size_t pygo_coro_scan_hwm(pygo_coro_t *c)
{
#if defined(PYGO_HAVE_FCONTEXT) || defined(PYGO_HAVE_UCONTEXT)
    if (c == NULL || c->stack == NULL || !pygo_stack_paint_on) return 0;
    return pygo_stack_hwm_scan(c->stack, c->stack_size);
#else
    /* Windows Fibers: no introspectable stack. */
    (void)c;
    return 0;
#endif
}
