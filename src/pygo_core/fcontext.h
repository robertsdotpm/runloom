/* fcontext.h -- abstract two-function context-switch primitive.
 *
 * Implementations:
 *   arch/swap_x86_64.S   System V x86_64 (Linux, macOS, BSD)
 *   (future)             aarch64, arm, riscv, x86_64-MS, x86 32-bit
 *   coro.c fallback      ucontext (POSIX, slow), Fibers (Windows)
 *
 * Contract:
 *   pygo_asm_ctx_t holds a single "stack pointer" word.
 *   pygo_asm_swap(from, to) saves callee-saved regs into the current
 *     stack frame, stores the resulting SP into *from, loads SP from
 *     *to, restores its callee-saved regs, RETs into the IP saved on
 *     that new stack.
 *   pygo_asm_make_ctx(coro, stack_top) pre-populates a fresh stack
 *     so the first pygo_asm_swap into it lands in pygo_asm_trampoline
 *     -> pygo_asm_entry(coro) -> coro->entry(coro->user) -> infinite
 *     swap back to coro->caller after entry returns.
 */
#ifndef PYGO_FCONTEXT_H
#define PYGO_FCONTEXT_H

#include "compat.h"
#include "plat.h"

/* Enable the asm fast path on architectures we have an implementation
 * for and where the OS uses System V calling convention. */
#if (defined(PYGO_OS_LINUX) || defined(PYGO_OS_MACOS) || defined(PYGO_OS_BSD)) \
    && defined(PYGO_ARCH_X86_64)
#  define PYGO_HAVE_FCONTEXT 1
#endif

#ifdef PYGO_HAVE_FCONTEXT

typedef struct pygo_asm_ctx {
    void *sp;
} pygo_asm_ctx_t;

typedef struct pygo_asm_coro pygo_asm_coro_t;

struct pygo_asm_coro {
    pygo_asm_ctx_t self;
    pygo_asm_ctx_t caller;
    void (*entry)(void *user);
    void *user;
    int done;
};

/* Asm-side: defined in arch/swap_*.S */
extern void pygo_asm_swap(pygo_asm_ctx_t *from, pygo_asm_ctx_t *to);
extern void pygo_asm_trampoline(void);

/* C-side: called from asm trampoline.  Runs the user entry, then swaps
 * back to caller in an infinite loop (in case the caller mistakenly
 * resumes a done coro, we just yield back to them again). */
void pygo_asm_entry(pygo_asm_coro_t *c);

/* Set up a fresh stack so the first swap into ctx->self runs entry(user).
 * stack_top is the high address of an mmap'd / pool stack of size
 * stack_size.  The caller must keep the stack memory alive for as long
 * as the coroutine exists. */
void pygo_asm_make_ctx(pygo_asm_coro_t *coro,
                       void *stack_top);

#endif /* PYGO_HAVE_FCONTEXT */

#endif /* PYGO_FCONTEXT_H */
