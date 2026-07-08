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
    runloom_fibersan_first_entry(c);   /* learn hub bounds; no-op unless sanitized */
    c->entry(c->user);
    c->done = 1;
    for (;;) {
        runloom_fibersan_abandon(c);   /* hand history back to the hub fiber */
        runloom_asm_swap(&c->self, &c->caller);
        runloom_fibersan_reentered(c); /* defensive: spurious resume of a done coro */
    }
}

#if defined(RUNLOOM_ARCH_X86_64)

/* x86_64 SysV: swap saves a 16-byte FP-env slot, then pops 6 regs
 * (r15 r14 r13 r12 rbx rbp) then ret.
 * Frame layout, low to high:
 *   sp ->  [MXCSR@+0, x87CW@+8]  r15  r14  r13  r12=coro  rbx  rbp  ret=trampoline
 * The FP-env slot must mirror runloom_asm_swap (16 bytes so the frame stays a
 * multiple of 16 and the post-ret sp matches the no-FP layout).  It is seeded
 * with the SysV defaults so a fresh fiber starts with round-to-nearest and all
 * FP exceptions masked.  Stack must be 16-aligned at trampoline's call. */
void runloom_asm_make_ctx(runloom_asm_coro_t *coro, void *stack_top)
{
    uintptr_t sp = (uintptr_t)stack_top;
    uintptr_t *frame;
    sp &= ~(uintptr_t)15;
    sp -= 8;                /* deliberate misalign: after callq pushes 8,
                               we're 16-aligned at runloom_asm_entry entry. */
    sp -= 7 * 8;            /* r15 r14 r13 r12 rbx rbp ret                 */
    sp -= 16;               /* FP-env slot (matches swap's subq $16)       */
    frame = (uintptr_t *)sp;
    /* FP env: MXCSR (32-bit) at +0, x87 control word (16-bit) at +8. */
    ((uint32_t *)frame)[0] = 0x1F80u;          /* MXCSR: all exceptions masked */
    frame[1] = 0;                              /* zero the x87 slot first  */
    ((uint16_t *)&frame[1])[0] = 0x037Fu;      /* x87 CW: default, PC=64b   */
    frame[2] = 0;                              /* r15 */
    frame[3] = 0;                              /* r14 */
    frame[4] = 0;                              /* r13 */
    frame[5] = (uintptr_t)coro;                /* r12 = coro_ptr */
    frame[6] = 0;                              /* rbx */
    frame[7] = 0;                              /* rbp */
    frame[8] = (uintptr_t)&runloom_asm_trampoline;

    coro->self.sp = (void *)sp;
    coro->caller.sp = NULL;
    coro->done = 0;
}

#elif defined(RUNLOOM_ARCH_AARCH64)

/* aarch64 AAPCS64: swap saves 12 GPRs (x19..x30) + 8 FPs (d8..d15) + FPCR
 *   = 160 + 16 = 176 bytes.  Layout, low to high:
 *     [sp, #0..15]    x19, x20      <- x19 = coro pointer
 *     [sp, #16..31]   x21, x22
 *     [sp, #32..47]   x23, x24
 *     [sp, #48..63]   x25, x26
 *     [sp, #64..79]   x27, x28
 *     [sp, #80..95]   x29, x30      <- x30 = lr = trampoline
 *     [sp, #96..159]  d8..d15
 *     [sp, #160..175] FPCR (16-byte slot; FPCR at +160, +168 padding)
 * Trampoline reads x19; ret to x30.  The FPCR slot must mirror runloom_asm_swap
 * (16 bytes so the frame stays 16-aligned); zeroing it seeds FPCR = 0, the
 * AArch64 default (round-to-nearest even, no FP traps, FZ off) for a fresh
 * fiber -- matching how swap saves/restores it. */
void runloom_asm_make_ctx(runloom_asm_coro_t *coro, void *stack_top)
{
    uintptr_t sp = (uintptr_t)stack_top;
    uintptr_t *frame;
    sp &= ~(uintptr_t)15;       /* 16-align */
    sp -= 176;
    frame = (uintptr_t *)sp;
    /* zero everything first (frame[20] = FPCR slot -> 0 = AArch64 default) */
    for (size_t i = 0; i < 22; i++) frame[i] = 0;
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
