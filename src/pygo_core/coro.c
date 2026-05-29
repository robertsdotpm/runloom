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
#elif defined(PYGO_HAVE_FIBERS)
    void *fiber;
#elif defined(PYGO_HAVE_UCONTEXT)
    ucontext_t ctx;
    ucontext_t caller_ctx;
    void *stack;
    size_t stack_size;
#endif
};

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

static void *pygo_stack_acquire(size_t size)
{
    void **head = pygo_tls_stack_pool;
    if (head != NULL) {
        size_t pooled_size = (size_t)head[PYGO_STACK_HDR_SIZE];
        if (pooled_size == size) {
            pygo_tls_stack_pool = (void **)head[PYGO_STACK_HDR_NEXT];
            /* Caller will overwrite the header bytes as the stack
             * grows; the new coroutine doesn't observe them. */
            return (void *)head;
        }
        /* Size mismatch (different stack_size requested than what the
         * pool has).  Don't walk -- just munmap pooled stacks until
         * the head matches or pool is empty.  Bounded work in the
         * pathological mixed-size case. */
        while (head != NULL && (size_t)head[PYGO_STACK_HDR_SIZE] != size) {
            void **next = (void **)head[PYGO_STACK_HDR_NEXT];
            munmap((void *)head, (size_t)head[PYGO_STACK_HDR_SIZE]);
            head = next;
        }
        pygo_tls_stack_pool = head;
        if (head != NULL) {
            pygo_tls_stack_pool = (void **)head[PYGO_STACK_HDR_NEXT];
            return (void *)head;
        }
    }
    {
        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_STACK
        flags |= MAP_STACK;
#endif
        void *s = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (s == MAP_FAILED) {
            return NULL;
        }
        return s;
    }
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
}

/* Pre-warm n stacks of the given size into the per-thread pool.
 * Returns the number successfully pre-allocated (may be < n if
 * mmap starts failing partway through). */
static int pygo_stack_warmup_posix(size_t size, int n)
{
    int i;
    for (i = 0; i < n; i++) {
        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_STACK
        flags |= MAP_STACK;
#endif
        void *s = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (s == MAP_FAILED) return i;
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
    /* Recycle if there's room.  Stack stays attached -- next
     * pygo_coro_new pop reuses it without touching the stack pool. */
    if (pygo_coro_pool_size < PYGO_CORO_POOL_CAP && c->stack != NULL) {
        c->pool_next = pygo_coro_pool;
        pygo_coro_pool = c;
        pygo_coro_pool_size++;
        return;
    }
    if (c->stack != NULL) {
        pygo_stack_release(c->stack, c->stack_size);
    }
    free(c);
}

void pygo_coro_resume(pygo_coro_t *c)
{
    pygo_coro_t *prev = pygo_tls_current;
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
