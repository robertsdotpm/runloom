/* swap_x86_64.S -- hand-rolled context switch for x86_64 System V ABI.
 *
 * Replaces ucontext's swapcontext, which on glibc calls rt_sigprocmask
 * twice per swap (~300 ns each = ~600 ns of syscall traffic) just to
 * save and restore the signal mask.  We don't care about the signal
 * mask -- application-level coroutines should not be touching it -- so
 * we save only what the SystemV ABI requires: rbx, rbp, r12-r15.
 *
 *   void runloom_asm_swap(runloom_asm_ctx_t *from, runloom_asm_ctx_t *to);
 *   void runloom_asm_trampoline(void);   <- jumped to on first swap-in
 *
 * runloom_asm_ctx_t is a single `void *sp` per context.  The "saved
 * state" of a context lives entirely on its own stack.
 *
 * The trampoline reads the coro pointer from %r12 (set up by
 * runloom_asm_make) and tail-calls runloom_asm_entry(c) which is a C
 * function that runs the user's entry and swaps back forever.
 *
 * SystemV calling convention:
 *   first arg  -> %rdi
 *   second arg -> %rsi
 *   callee-saved: rbx, rbp, r12, r13, r14, r15  (we also save xmm none -
 *   they are caller-saved on SysV).
 *
 * macOS prepends '_' to C symbol names; Linux/BSD do not.
 */

#if defined(__APPLE__)
#  define RUNLOOM_SYM(name) _##name
#else
#  define RUNLOOM_SYM(name) name
#endif

#if defined(__linux__) || defined(__FreeBSD__) || defined(__OpenBSD__) || \
    defined(__NetBSD__) || defined(__DragonFly__)
#  define RUNLOOM_NOTE_NX
#endif

    .text

/* -------------------------------------------------------------------- *
 *  runloom_asm_swap(from, to)                                             *
 *  Save callee-saved regs on the current stack, store SP into *from,   *
 *  load SP from *to, restore regs, RET into the new context's IP.      *
 * -------------------------------------------------------------------- */
    .globl RUNLOOM_SYM(runloom_asm_swap)
#if defined(__linux__)
    .type  RUNLOOM_SYM(runloom_asm_swap), @function
#endif
RUNLOOM_SYM(runloom_asm_swap):
    pushq   %rbp
    pushq   %rbx
    pushq   %r12
    pushq   %r13
    pushq   %r14
    pushq   %r15
    movq    %rsp, (%rdi)          /* from->sp = rsp                    */
    movq    (%rsi), %rsp          /* rsp      = to->sp                 */
    popq    %r15
    popq    %r14
    popq    %r13
    popq    %r12
    popq    %rbx
    popq    %rbp
    ret                           /* jumps to saved IP on new stack    */
#if defined(__linux__)
    .size   RUNLOOM_SYM(runloom_asm_swap), .-RUNLOOM_SYM(runloom_asm_swap)
#endif

/* -------------------------------------------------------------------- *
 *  runloom_asm_trampoline                                                 *
 *  Entered the very first time a coroutine is swapped into.  The make  *
 *  routine has pre-loaded %r12 with the runloom_asm_coro_t pointer.       *
 *  We move it into %rdi (first arg slot) and tail-call into C.         *
 * -------------------------------------------------------------------- */
    .globl RUNLOOM_SYM(runloom_asm_trampoline)
#if defined(__linux__)
    .type  RUNLOOM_SYM(runloom_asm_trampoline), @function
#endif
RUNLOOM_SYM(runloom_asm_trampoline):
    movq    %r12, %rdi             /* arg0 = coro_ptr                  */
    /* Align the stack to 16 bytes before the call.  We're at +8 from
     * a 16-byte boundary because runloom_asm_swap's ret popped 8 bytes;
     * a single subtraction puts us back in alignment. */
    subq    $8, %rsp
    callq   RUNLOOM_SYM(runloom_asm_entry)
    /* runloom_asm_entry never returns (it loops forever swapping back to
     * the caller after entry exits).  But just in case, hlt. */
    hlt
#if defined(__linux__)
    .size   RUNLOOM_SYM(runloom_asm_trampoline), .-RUNLOOM_SYM(runloom_asm_trampoline)
#endif

/* Mark the stack as non-executable on ELF targets. */
#if defined(RUNLOOM_NOTE_NX) && defined(__ELF__)
    .section .note.GNU-stack,"",%progbits
#endif
