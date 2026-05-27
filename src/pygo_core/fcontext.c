/* fcontext.c -- C side of the asm fast-path: make() + entry() */

#include "fcontext.h"

#ifdef PYGO_HAVE_FCONTEXT

/* The trampoline lands here with the coro pointer in %rdi.
 * Run the entry, mark done, then swap back to caller forever.  We
 * never let this function return -- there's no real "return to where"
 * because the trampoline doesn't have a meaningful caller frame to
 * return to.  Any spurious resume after done just yields back again. */
void pygo_asm_entry(pygo_asm_coro_t *c)
{
    c->entry(c->user);
    c->done = 1;
    for (;;) {
        pygo_asm_swap(&c->self, &c->caller);
    }
}

/* Set up the initial stack frame so the first swap-in runs
 * trampoline().  The swap_x86_64.S pop sequence is:
 *   pop r15  pop r14  pop r13  pop r12  pop rbx  pop rbp  ret
 * Ret takes the next 8 bytes as the return address.  So the stack
 * we leave behind needs to look (top to high addresses) like:
 *
 *   sp ->  r15           (we don't care, init to 0)
 *          r14
 *          r13
 *          r12     <- coro pointer (trampoline reads this!)
 *          rbx
 *          rbp
 *          return_addr   <- pygo_asm_trampoline
 *
 * Stack grows down, so we start at stack_top and subtract.  The
 * System V ABI requires that at function entry the stack be aligned
 * to 16 bytes such that (rsp + 8) is a multiple of 16 (call instr
 * pushes 8 bytes).  We satisfy that by ensuring stack_top is
 * 16-aligned and our 7-word setup keeps it so. */
void pygo_asm_make_ctx(pygo_asm_coro_t *coro, void *stack_top)
{
    uintptr_t sp = (uintptr_t)stack_top;
    uintptr_t *frame;

    /* Align the top down to a 16-byte boundary. */
    sp &= ~(uintptr_t)15;

    /* We need 7 words: 6 saved regs + return address.  Subtract 7*8.
     * After the ret pops the return address, rsp is 16-aligned (as
     * required by the trampoline's first call). */
    sp -= 8;   /* deliberate misalign so after the call instr at the
                * start of pygo_asm_trampoline (which pushes 8 bytes for
                * the return address) we are 16-aligned for the next
                * call. */
    sp -= 7 * 8;

    frame = (uintptr_t *)sp;
    frame[0] = 0;                              /* r15 */
    frame[1] = 0;                              /* r14 */
    frame[2] = 0;                              /* r13 */
    frame[3] = (uintptr_t)coro;                /* r12 = coro_ptr */
    frame[4] = 0;                              /* rbx */
    frame[5] = 0;                              /* rbp */
    frame[6] = (uintptr_t)&pygo_asm_trampoline;

    coro->self.sp = (void *)sp;
    coro->caller.sp = NULL;       /* set by first swap */
    coro->done = 0;
}

#endif /* PYGO_HAVE_FCONTEXT */
