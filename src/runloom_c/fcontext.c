/* fcontext.c -- C side of the asm fast-path: make() + entry() */

#include "fcontext.h"

#ifdef RUNLOOM_HAVE_FCONTEXT

/* The trampoline lands here with the coro pointer in %rdi.
 * Run the entry, mark done, then swap back to caller forever.  We
 * never let this function return -- there's no real "return to where"
 * because the trampoline doesn't have a meaningful caller frame to
 * return to.  Any spurious resume after done just yields back again. */
void runloom_asm_entry(runloom_asm_coro_t *c)
{
    c->entry(c->user);
    c->done = 1;
    for (;;) {
        runloom_asm_swap(&c->self, &c->caller);
    }
}

#if defined(RUNLOOM_ARCH_X86_64)

/* x86_64 SysV: swap pops 6 regs (r15 r14 r13 r12 rbx rbp) then ret.
 * Frame layout, low to high:
 *   sp ->  r15  r14  r13  r12=coro  rbx  rbp  return_addr=trampoline
 * Stack must be 16-aligned at trampoline's call instruction. */
void runloom_asm_make_ctx(runloom_asm_coro_t *coro, void *stack_top)
{
    uintptr_t sp = (uintptr_t)stack_top;
    uintptr_t *frame;
    sp &= ~(uintptr_t)15;
    sp -= 8;                /* deliberate misalign: after callq pushes 8,
                               we're 16-aligned at runloom_asm_entry entry. */
    sp -= 7 * 8;
    frame = (uintptr_t *)sp;
    frame[0] = 0;                              /* r15 */
    frame[1] = 0;                              /* r14 */
    frame[2] = 0;                              /* r13 */
    frame[3] = (uintptr_t)coro;                /* r12 = coro_ptr */
    frame[4] = 0;                              /* rbx */
    frame[5] = 0;                              /* rbp */
    frame[6] = (uintptr_t)&runloom_asm_trampoline;

    coro->self.sp = (void *)sp;
    coro->caller.sp = NULL;
    coro->done = 0;
}

#elif defined(RUNLOOM_ARCH_AARCH64)

/* aarch64 AAPCS64: swap saves 12 GPRs (x19..x30) + 8 FPs (d8..d15)
 *   = 160 bytes.  Layout, low to high:
 *     [sp, #0..15]    x19, x20      <- x19 = coro pointer
 *     [sp, #16..31]   x21, x22
 *     [sp, #32..47]   x23, x24
 *     [sp, #48..63]   x25, x26
 *     [sp, #64..79]   x27, x28
 *     [sp, #80..95]   x29, x30      <- x30 = lr = trampoline
 *     [sp, #96..159]  d8..d15
 * Trampoline reads x19; ret to x30. */
void runloom_asm_make_ctx(runloom_asm_coro_t *coro, void *stack_top)
{
    uintptr_t sp = (uintptr_t)stack_top;
    uintptr_t *frame;
    sp &= ~(uintptr_t)15;       /* 16-align */
    sp -= 160;
    frame = (uintptr_t *)sp;
    /* zero everything first */
    for (size_t i = 0; i < 20; i++) frame[i] = 0;
    frame[0] = (uintptr_t)coro;                  /* x19 */
    frame[11] = (uintptr_t)&runloom_asm_trampoline; /* x30 (offset 88) */

    coro->self.sp = (void *)sp;
    coro->caller.sp = NULL;
    coro->done = 0;
}

#else
#  error "RUNLOOM_HAVE_FCONTEXT set but no make_ctx implementation for this arch"
#endif

#endif /* RUNLOOM_HAVE_FCONTEXT */
