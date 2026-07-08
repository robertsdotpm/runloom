/* runloom_kcsan.h -- KCSAN-style delay-and-recheck exclusive-access watchpoints
 * (QA-steal-V2 #8).
 *
 * A cheap, SAMPLING complement to ThreadSanitizer for data-race detection at
 * soak scale, where TSan's 5-15x slowdown is infeasible (the 1e6-g soak).  The
 * idea, from the Linux KCSAN watchpoint sampler: at a point the DESIGN declares
 * single-owner for this window, snapshot the watched word, STALL briefly to widen
 * the race window, then re-read.  If it changed, another thread accessed it
 * concurrently -- an ASSERT_EXCLUSIVE_* violation (a data race), reported via the
 * usual runloom_invariant_fail path.  Unlike TSan it needs no instrumented build
 * and costs only the sampled stall, so it can run as an always-on second detector.
 *
 * Zero cost unless built with -DRUNLOOM_KCSAN.  Sampled 1-in-RUNLOOM_KCSAN_N
 * (power of two) per thread so the stall overhead is bounded even on a hot path.
 */
#ifndef RUNLOOM_KCSAN_H
#define RUNLOOM_KCSAN_H

#include "plat.h"          /* RUNLOOM_TLS, RUNLOOM_INLINE */

#ifdef RUNLOOM_KCSAN
#include <stdint.h>

#ifndef RUNLOOM_KCSAN_N
#define RUNLOOM_KCSAN_N 64u          /* sample 1 access in 64 (power of two) */
#endif

extern RUNLOOM_TLS uint32_t runloom_kcsan_ctr;

/* A brief (~1 us) stall that widens the race window without yielding the CPU to
 * the scheduler (we must not switch fibers mid-watchpoint). */
void runloom_kcsan_stall(void);

/* Report a watchpoint that observed a concurrent write (before != after). */
void runloom_kcsan_violation(const char *where, uint64_t before, uint64_t after);

/* ASSERT_EXCLUSIVE_ACCESS on a 64-bit atomic word: the caller asserts nothing
 * else writes *p for the duration of this window.  Sampled. */
RUNLOOM_INLINE void runloom_kcsan_check64(const char *where,
                                          const _Atomic uint64_t *p)
{
    uint64_t before, after;
    if ((++runloom_kcsan_ctr & (RUNLOOM_KCSAN_N - 1u)) != 0u) return;
    before = __atomic_load_n(p, __ATOMIC_RELAXED);
    runloom_kcsan_stall();
    after = __atomic_load_n(p, __ATOMIC_RELAXED);
    if (before != after) runloom_kcsan_violation(where, before, after);
}

#define RUNLOOM_ASSERT_EXCLUSIVE64(where, p) runloom_kcsan_check64((where), (p))

#else  /* !RUNLOOM_KCSAN -- zero cost */

#define RUNLOOM_ASSERT_EXCLUSIVE64(where, p) ((void)0)

#endif /* RUNLOOM_KCSAN */
#endif /* RUNLOOM_KCSAN_H */
