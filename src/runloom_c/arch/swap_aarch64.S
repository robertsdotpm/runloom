/* swap_aarch64.S -- inline asm context switch for ARMv8 / aarch64
 * (Apple Silicon, modern Android phones, Graviton, Ampere, etc).
 *
 * AAPCS64 calling convention:
 *   first arg  -> x0
 *   second arg -> x1
 *   callee-saved general regs: x19-x29, sp, fp (=x29), lr (=x30)
 *   callee-saved fp/simd regs: d8-d15 (low 64 bits)
 *
 * We save x19-x30 (12 regs) + d8-d15 (8 doubles) = 12*8 + 8*8 = 160 bytes
 * per saved frame.  Stack must stay 16-byte aligned.
 *
 *   void runloom_asm_swap(runloom_asm_ctx_t *from, runloom_asm_ctx_t *to);
 *   void runloom_asm_trampoline(void);
 *
 * runloom_asm_make_ctx (in fcontext.c) builds the initial saved frame so
 * the first swap-in lands in runloom_asm_trampoline -> runloom_asm_entry(c)
 * with c (the coro pointer) in x19 (callee-saved on aarch64).
 *
 * macOS / iOS use _underscore symbol prefix; Linux / BSD don't.
 */

#if defined(__APPLE__)
#  define RUNLOOM_SYM(name) _##name
#else
#  define RUNLOOM_SYM(name) name
#endif

    .text
    .align 2

/* -------------------------------------------------------------------- *
 *  runloom_asm_swap(from, to)                                             *
 * -------------------------------------------------------------------- */
    .globl RUNLOOM_SYM(runloom_asm_swap)
#if !defined(__APPLE__)
    .type  RUNLOOM_SYM(runloom_asm_swap), %function
#endif
RUNLOOM_SYM(runloom_asm_swap):
    /* Make room for 12 GPRs + 8 FPs + FPCR = 160 + 16 = 176 bytes (a 16-byte
     * slot for FPCR keeps the frame 16-aligned: 176 = 11 * 16).  FPCR holds the
     * callee-preserved FP control state (rounding mode, exception-trap enables,
     * FZ/AH); without saving/restoring it a fiber's fesetround / feenableexcept
     * leaks into whatever fiber or scheduler runs next on this hub -- silent
     * float corruption or a process-killing FP trap.  (FPSR is caller-saved
     * status, not preserved.)  runloom_asm_make_ctx seeds the FPCR slot to 0 =
     * the AArch64 default (round-to-nearest even, no traps, FZ off). */
    sub     sp, sp, #176

    stp     x19, x20, [sp, #0]
    stp     x21, x22, [sp, #16]
    stp     x23, x24, [sp, #32]
    stp     x25, x26, [sp, #48]
    stp     x27, x28, [sp, #64]
    stp     x29, x30, [sp, #80]        /* fp, lr */
    stp     d8,  d9,  [sp, #96]
    stp     d10, d11, [sp, #112]
    stp     d12, d13, [sp, #128]
    stp     d14, d15, [sp, #144]
    mrs     x9,  fpcr
    str     x9,  [sp, #160]            /* save FPCR (16-byte slot at +160) */

    /* from->sp = sp */
    mov     x9, sp
    str     x9, [x0]

    /* sp = to->sp */
    ldr     x9, [x1]
    mov     sp, x9

    ldp     x19, x20, [sp, #0]
    ldp     x21, x22, [sp, #16]
    ldp     x23, x24, [sp, #32]
    ldp     x25, x26, [sp, #48]
    ldp     x27, x28, [sp, #64]
    ldp     x29, x30, [sp, #80]
    ldp     d8,  d9,  [sp, #96]
    ldp     d10, d11, [sp, #112]
    ldp     d12, d13, [sp, #128]
    ldp     d14, d15, [sp, #144]
    ldr     x9,  [sp, #160]            /* restore FPCR                  */
    msr     fpcr, x9

    add     sp, sp, #176
    ret                              /* jump to saved lr (x30)        */
#if !defined(__APPLE__)
    .size   RUNLOOM_SYM(runloom_asm_swap), .-RUNLOOM_SYM(runloom_asm_swap)
#endif

/* -------------------------------------------------------------------- *
 *  runloom_asm_trampoline                                                 *
 *  Entered on first swap-in.  make_ctx pre-loaded x19 with the coro    *
 *  pointer.  Move it to x0 and tail-call into C.                       *
 * -------------------------------------------------------------------- */
    .globl RUNLOOM_SYM(runloom_asm_trampoline)
#if !defined(__APPLE__)
    .type  RUNLOOM_SYM(runloom_asm_trampoline), %function
#endif
RUNLOOM_SYM(runloom_asm_trampoline):
    mov     x0, x19                  /* arg0 = coro                   */
    bl      RUNLOOM_SYM(runloom_asm_entry) /* does not return               */
    brk     #0                       /* abort if it ever did          */
#if !defined(__APPLE__)
    .size   RUNLOOM_SYM(runloom_asm_trampoline), .-RUNLOOM_SYM(runloom_asm_trampoline)
#endif

#if defined(__linux__) && defined(__ELF__)
    .section .note.GNU-stack,"",%progbits
#endif
