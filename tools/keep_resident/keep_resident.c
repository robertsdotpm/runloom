/* keep_resident.c -- LD_PRELOAD shim that stops madvise(MADV_DONTNEED/MADV_FREE)
 * from returning pages to the OS, so freed memory stays RESIDENT (Go's strategy).
 *
 * WHY: on a spawn-heavy free-threaded (3.13t) runloom workload, CPython's
 * mimalloc QSBR collector purges a heap segment ~once per fiber COMPLETION via
 * madvise(MADV_DONTNEED).  Each such madvise broadcasts a TLB-shootdown IPI to
 * every hub CPU (smp_call_function_many -> flush_tlb_mm_range), which dominates
 * spawn CPU (~32%) and caps parallel scaling.  See docs/dev/spawn_cost.md.  This
 * shim makes those purge madvises no-ops -> measured ~2x spawn throughput at
 * 8 issuers (240k -> 500k/s), and tighter scaling (105->337->515k @ 1/4/8).
 *
 * TRADEOFF: it is the SAME keep-resident choice Go makes -- hold memory instead
 * of giving it back to the OS.  Cost is higher RSS (the heap does not shrink back
 * to the kernel until the process exits).  Use ONLY for spawn-churn-heavy,
 * RSS-tolerant workloads; do NOT use under long-lived, memory-constrained servers.
 *
 * SCOPE: no-ops ONLY the purge advices (MADV_DONTNEED / MADV_FREE).  Every other
 * madvise (MADV_HUGEPAGE, MADV_WILLNEED, ...) passes through to the real syscall.
 *
 * BUILD:  cc -O2 -shared -fPIC -o keep_resident.so keep_resident.c -ldl
 * USE:    LD_PRELOAD=/path/to/keep_resident.so PYTHON_GIL=0 python3 your_app.py
 *         (or via the ./runloom-keep-resident wrapper in this directory)
 *
 * CLEAN ALTERNATIVE (no LD_PRELOAD): rebuild CPython with mimalloc
 * mi_option_purge_delay = -1.  The MIMALLOC_PURGE_DELAY env is IGNORED by
 * CPython's vendored mimalloc and mi_option_set is not an exported symbol, so at
 * runtime this shim is the only way to get the same effect.
 */
#define _GNU_SOURCE
#include <sys/mman.h>
#include <dlfcn.h>

static int (*real_madvise)(void *, size_t, int);

int madvise(void *addr, size_t length, int advice)
{
    if (advice == MADV_DONTNEED || advice == MADV_FREE)
        return 0;                       /* keep the pages resident */
    if (!real_madvise)
        real_madvise = (int (*)(void *, size_t, int))dlsym(RTLD_NEXT, "madvise");
    return real_madvise ? real_madvise(addr, length, advice) : 0;
}
