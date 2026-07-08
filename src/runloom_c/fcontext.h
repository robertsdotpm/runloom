/* fcontext.h -- abstract two-function context-switch primitive.
 *
 * Implementations:
 *   arch/swap_x86_64.S   System V x86_64 (Linux, macOS, BSD)
 *   (future)             aarch64, arm, riscv, x86_64-MS, x86 32-bit
 *   coro.c fallback      ucontext (POSIX, slow), Fibers (Windows)
 *
 * Contract:
 *   runloom_asm_ctx_t holds a single "stack pointer" word.
 *   runloom_asm_swap(from, to) saves callee-saved regs into the current
 *     stack frame, stores the resulting SP into *from, loads SP from
 *     *to, restores its callee-saved regs, RETs into the IP saved on
 *     that new stack.
 *   runloom_asm_make_ctx(coro, stack_top) pre-populates a fresh stack
 *     so the first runloom_asm_swap into it lands in runloom_asm_trampoline
 *     -> runloom_asm_entry(coro) -> coro->entry(coro->user) -> infinite
 *     swap back to coro->caller after entry returns.
 */
#ifndef RUNLOOM_FCONTEXT_H
#define RUNLOOM_FCONTEXT_H

#include "compat.h"
#include "plat.h"
#include "runloom_fiber_san.h"   /* fiber-aware TSan/ASan brackets (no-op unless -fsanitize) */

/* Enable the asm fast path on architectures we have an implementation
 * for and where the OS uses System V calling convention.  RUNLOOM_FORCE_UCONTEXT
 * (setup.py: RUNLOOM_BACKEND=ucontext / RUNLOOM_NO_ASM) suppresses it; must stay in
 * sync with the backend selection in plat.h. */
#if !defined(RUNLOOM_FORCE_UCONTEXT) \
    && (defined(RUNLOOM_OS_LINUX) || defined(RUNLOOM_OS_MACOS) || defined(RUNLOOM_OS_BSD) \
     || defined(RUNLOOM_OS_ANDROID)) \
    && (defined(RUNLOOM_ARCH_X86_64) || defined(RUNLOOM_ARCH_AARCH64))
#  define RUNLOOM_HAVE_FCONTEXT 1
#endif

#ifdef RUNLOOM_HAVE_FCONTEXT

typedef struct runloom_asm_ctx {
    void *sp;
} runloom_asm_ctx_t;

typedef struct runloom_asm_coro runloom_asm_coro_t;

struct runloom_asm_coro {
    runloom_asm_ctx_t self;
    runloom_asm_ctx_t caller;
    void (*entry)(void *user);
    void *user;
    int done;
#if defined(RUNLOOM_FIBERSAN)
    /* Fiber sanitizer state -- present ONLY on -fsanitize=thread/address builds
     * (uniform across TUs), so release layout is byte-identical to before. */
    runloom_fiber_san_t fibersan;
#endif
};

/* Asm-side: defined in arch/swap_*.S */
extern void runloom_asm_swap(runloom_asm_ctx_t *from, runloom_asm_ctx_t *to);
extern void runloom_asm_trampoline(void);

/* C-side: called from asm trampoline.  Runs the user entry, then swaps
 * back to caller in an infinite loop (in case the caller mistakenly
 * resumes a done coro, we just yield back to them again). */
void runloom_asm_entry(runloom_asm_coro_t *c);

/* Set up a fresh stack so the first swap into ctx->self runs entry(user).
 * stack_top is the high address of an mmap'd / pool stack of size
 * stack_size.  The caller must keep the stack memory alive for as long
 * as the coroutine exists. */
void runloom_asm_make_ctx(runloom_asm_coro_t *coro,
                       void *stack_top);

/* Helper bodies -- need the complete struct above. */
#include "runloom_fiber_san_impl.h"

#endif /* RUNLOOM_HAVE_FCONTEXT */

#endif /* RUNLOOM_FCONTEXT_H */
