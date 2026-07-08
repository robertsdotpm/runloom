/* clock_skew_shim.c -- LD_PRELOAD CLOCK_MONOTONIC skew injector (QA-steal-V2 #20).
 *
 * Intercepts clock_gettime(CLOCK_MONOTONIC) -- which runloom_monotonic_ns() and
 * the netpoll/sysmon deadline path call -- and adds a MONOTONICITY-PRESERVING
 * skew so the timer arithmetic (deadline - now, timeout clamping, overflow) is
 * exercised under a clock that jumps forward irregularly rather than ticking
 * smoothly like the DST logical clock.  Two env knobs (ns):
 *   CLOCK_SKEW_FF   -- constant forward fast-forward added to every read.
 *   CLOCK_SKEW_JIT  -- per-read forward jitter in [0, JIT) (never backward, so
 *                      the CLOCK_MONOTONIC contract holds and a hang is a real
 *                      deadline-math bug, not us violating monotonicity).
 * A per-thread running max clamps the result so it never goes backward even as
 * jitter varies.  cheap xorshift RNG (no libc rand, avoids reentrancy).
 */
#define _GNU_SOURCE
#include <time.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>

static int (*real_clock_gettime)(clockid_t, struct timespec *) = NULL;
static __thread uint64_t rng_state = 0;
static __thread long long last_ns = 0;      /* per-thread monotonic clamp */

static long long env_ns(const char *name)
{
    const char *v = getenv(name);
    return v ? atoll(v) : 0;
}

int clock_gettime(clockid_t clk, struct timespec *ts)
{
    if (real_clock_gettime == NULL)
        real_clock_gettime = (int (*)(clockid_t, struct timespec *))
            dlsym(RTLD_NEXT, "clock_gettime");
    int r = real_clock_gettime(clk, ts);
    if (r != 0 || clk != CLOCK_MONOTONIC)
        return r;

    long long ns = (long long)ts->tv_sec * 1000000000LL + ts->tv_nsec;
    ns += env_ns("CLOCK_SKEW_FF");
    long long jit = env_ns("CLOCK_SKEW_JIT");
    if (jit > 0) {
        if (rng_state == 0) rng_state = (uint64_t)(uintptr_t)ts | 1u;
        rng_state ^= rng_state << 13; rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        ns += (long long)(rng_state % (uint64_t)jit);
    }
    if (ns < last_ns) ns = last_ns;          /* preserve monotonicity */
    last_ns = ns;
    ts->tv_sec  = ns / 1000000000LL;
    ts->tv_nsec = ns % 1000000000LL;
    return 0;
}
