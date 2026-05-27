/* test_arm64.c -- Phase E validation under qemu-aarch64 user emulation.
 *
 * Standalone C program (no Python) that exercises the asm context
 * switch + trampoline on aarch64.  Cross-compile with
 * aarch64-linux-gnu-gcc, run via qemu-aarch64.  If the trampoline +
 * make_ctx + swap_aarch64.S are right under AAPCS64, this prints the
 * yield sequence and exits 0.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../src/pygo_core/coro.h"

#define EXPECT(cond, msg) do {                                              \
    if (!(cond)) { fprintf(stderr, "FAIL: %s\n", msg); exit(1); }           \
} while (0)

static int step_log[16];
static int step_count = 0;

static void log_step(int n)
{
    step_log[step_count++] = n;
    printf("  step %d\n", n);
}

static void coro_entry(void *user)
{
    long *counter = (long *)user;
    log_step(1);
    pygo_coro_yield();
    log_step(3);
    pygo_coro_yield();
    log_step(5);
    *counter = 42;
    /* Falls off end -- trampoline marks done. */
}

int main(void)
{
    pygo_coro_t *c;
    long counter = 0;
    int expected[] = {0, 1, 2, 3, 4, 5, 6};
    size_t i;

    printf("pygo aarch64 test (backend=%s)\n", pygo_coro_backend());

    EXPECT(pygo_coro_thread_init() == 0, "thread_init");

    c = pygo_coro_new(65536, coro_entry, &counter);
    EXPECT(c != NULL, "coro_new");

    log_step(0);
    pygo_coro_resume(c);
    log_step(2);
    EXPECT(!pygo_coro_done(c), "not done after first yield");
    pygo_coro_resume(c);
    log_step(4);
    EXPECT(!pygo_coro_done(c), "not done after second yield");
    pygo_coro_resume(c);
    log_step(6);
    EXPECT(pygo_coro_done(c), "done after fallthrough");
    EXPECT(counter == 42, "counter set by coro");

    EXPECT(step_count == 7, "exactly 7 steps logged");
    for (i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        EXPECT(step_log[i] == expected[i], "step sequence");
    }

    pygo_coro_destroy(c);
    pygo_coro_thread_fini();

    printf("OK\n");
    return 0;
}
