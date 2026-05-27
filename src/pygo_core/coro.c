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
    void **hdr = (void **)stack;
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

pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user)
{
    pygo_coro_t *c;
    size_t rounded;
    void *stack_top;

    if (stack_size < 4096) stack_size = 4096;
    rounded = pygo_round_to_page(stack_size);

    c = (pygo_coro_t *)calloc(1, sizeof(*c));
    if (c == NULL) return NULL;
    c->entry = entry;
    c->user = user;
    c->stack = pygo_stack_acquire(rounded);
    if (c->stack == NULL) { free(c); return NULL; }
    c->stack_size = rounded;

    c->asm_coro.entry = pygo_fcontext_entry;
    c->asm_coro.user = c;
    stack_top = (void *)((uintptr_t)c->stack + rounded);
    pygo_asm_make_ctx(&c->asm_coro, stack_top);

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
