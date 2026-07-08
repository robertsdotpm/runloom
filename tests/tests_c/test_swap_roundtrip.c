/* test_swap_roundtrip.c -- swap_*.S callee-saved round-trip validation
 * (QA-steal-V2 #23, translation-validation of the hand-written context switch).
 *
 * Compiles fcontext.c + arch/swap_x86_64.S directly (x86_64 System V only).  The
 * caller seeds the SysV callee-saved GPRs (rbx, r12-r15) with distinct sentinels,
 * swaps into a coroutine that overwrites them with a DIFFERENT sentinel and swaps
 * back; the swap must have saved the caller's values at swap-out and restored them
 * at swap-back, so the caller observes its OWN seeds -- if runloom_asm_swap dropped
 * a push/pop (an ABI-clobber bug invisible to every C/LLVM tool), the caller sees
 * the coroutine's 0xC0DE.. value and the test fails.  rbp is the frame pointer, so
 * it is round-tripped implicitly (the compiled code would crash otherwise) rather
 * than clobbered explicitly.
 *
 * Memory operands for the swap-arg addresses and the read-backs keep register
 * pressure survivable given we clobber almost every GPR.
 */
#include "fcontext.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#if !defined(RUNLOOM_HAVE_FCONTEXT) || !defined(__x86_64__)
int main(void) { printf("SKIP: not an x86_64 fcontext build\n"); return 0; }
#else

static runloom_asm_coro_t coro;

static void coro_fn(void *user)
{
    (void)user;
    void *from = &coro.self, *to = &coro.caller;
    /* Load rdi/rsi (swap args) first, THEN overwrite the callee-saved GPRs with a
     * sentinel, then swap back -- so the coro's saved state at swap-back is the
     * sentinel.  Never returns (the caller does not resume this coro). */
    __asm__ __volatile__(
        "movq %[from], %%rdi\n\t"
        "movq %[to],   %%rsi\n\t"
        "movabs $0xC0DEC0DEC0DEC0DE, %%rbx\n\t"
        "movabs $0xC0DEC0DEC0DEC0DE, %%r12\n\t"
        "movabs $0xC0DEC0DEC0DEC0DE, %%r13\n\t"
        "movabs $0xC0DEC0DEC0DEC0DE, %%r14\n\t"
        "movabs $0xC0DEC0DEC0DEC0DE, %%r15\n\t"
        "call runloom_asm_swap\n\t"
        :
        : [from] "m"(from), [to] "m"(to)
        : "rbx", "r12", "r13", "r14", "r15", "rdi", "rsi",
          "rax", "rcx", "rdx", "r8", "r9", "r10", "r11", "memory", "cc");
    __builtin_unreachable();
}

int main(void)
{
    size_t stack_size = 64 * 1024;
    void *stack = malloc(stack_size);
    if (stack == NULL) { printf("FAIL: no stack\n"); return 2; }

    coro.entry = coro_fn;
    coro.user = &coro;
    coro.done = 0;
    runloom_asm_make_ctx(&coro, (char *)stack + stack_size);

    void *from = &coro.caller, *to = &coro.self;
    uint64_t rbx_o = 0, r12_o = 0, r13_o = 0, r14_o = 0, r15_o = 0;
    __asm__ __volatile__(
        "movq %[from], %%rdi\n\t"
        "movq %[to],   %%rsi\n\t"
        "movabs $0x1111111111111111, %%rbx\n\t"
        "movabs $0x2222222222222222, %%r12\n\t"
        "movabs $0x3333333333333333, %%r13\n\t"
        "movabs $0x4444444444444444, %%r14\n\t"
        "movabs $0x5555555555555555, %%r15\n\t"
        "call runloom_asm_swap\n\t"          /* -> coro clobbers, swaps back here */
        "movq %%rbx, %[rbxo]\n\t"
        "movq %%r12, %[r12o]\n\t"
        "movq %%r13, %[r13o]\n\t"
        "movq %%r14, %[r14o]\n\t"
        "movq %%r15, %[r15o]\n\t"
        : [rbxo] "=m"(rbx_o), [r12o] "=m"(r12_o), [r13o] "=m"(r13_o),
          [r14o] "=m"(r14_o), [r15o] "=m"(r15_o)
        : [from] "m"(from), [to] "m"(to)
        : "rbx", "r12", "r13", "r14", "r15", "rdi", "rsi",
          "rax", "rcx", "rdx", "r8", "r9", "r10", "r11", "memory", "cc");

    int ok = 1;
    struct { const char *n; uint64_t got, want; } chk[] = {
        {"rbx", rbx_o, 0x1111111111111111ULL},
        {"r12", r12_o, 0x2222222222222222ULL},
        {"r13", r13_o, 0x3333333333333333ULL},
        {"r14", r14_o, 0x4444444444444444ULL},
        {"r15", r15_o, 0x5555555555555555ULL},
    };
    for (int i = 0; i < 5; i++)
        if (chk[i].got != chk[i].want) {
            printf("FAIL %s: got %016llx want %016llx (swap dropped this "
                   "callee-saved reg)\n", chk[i].n,
                   (unsigned long long)chk[i].got, (unsigned long long)chk[i].want);
            ok = 0;
        }
    free(stack);
    if (ok) { printf("OK: rbx/r12-r15 round-tripped intact across runloom_asm_swap\n"); return 0; }
    return 1;
}
#endif
