/* test_blockpool.c -- blocking-offload pool keeps a hub live.
 *
 * Premise (Group A, "move the work off the hub"): a goroutine that makes
 * a blocking C call must NOT wedge its hub.  pygo_blocking_call runs the
 * call on a pool thread and parks the goroutine instead.
 *
 * Setup: ONE hub, N blocker goroutines, each calling pygo_blocking_call
 * with a fn that sleeps BLOCK_MS and returns a per-g sentinel.
 *
 *   offloaded (correct): all N park, the pool runs them concurrently, so
 *       they all finish in ~BLOCK_MS regardless of N.  Wall ~= BLOCK_MS.
 *   wedged   (broken)  : the calls run inline on the one hub, serially,
 *       so wall ~= N*BLOCK_MS.
 *
 * With N=6 and one hub the two cases are ~200 ms vs ~1200 ms -- the wall
 * clock alone is decisive proof the calls parked rather than wedging the
 * hub.  We also check every call returned its correct result (so the
 * park/wake result hand-off is sound).
 */
#define _GNU_SOURCE
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>

#include "../src/pygo_core/pygo_sched.h"
#include "../src/pygo_core/mn_sched.h"
#include "../src/pygo_core/pygo_blockpool.h"

#define NHUBS     1
#define NBLOCK    6
#define BLOCK_MS  200
#define SENTINEL  100

static volatile long completed = 0;
static volatile long result_ok = 0;

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

/* The "blocking C call": just sleep, returning a recognisable result. */
static void *sleep_work(void *arg)
{
    usleep(BLOCK_MS * 1000);
    return (void *)((long)arg + SENTINEL);
}

static void blocker_fn(void *arg)
{
    void *r = pygo_blocking_call(sleep_work, arg);
    if (r == (void *)((long)arg + SENTINEL))
        __atomic_fetch_add(&result_ok, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&completed, 1, __ATOMIC_RELEASE);
}

int main(void)
{
    int i;
    double t0, wall, t;

    Py_Initialize();
    pygo_sched_set_default_stack_size(64 * 1024);

    if (pygo_mn_init(NHUBS) < 0) { fprintf(stderr, "mn_init failed\n"); return 2; }

    t0 = now_ms();
    for (i = 0; i < NBLOCK; i++) {
        if (pygo_mn_go_c(blocker_fn, (void *)(long)i) < 0) {
            fprintf(stderr, "go blocker %d\n", i); return 2;
        }
    }

    t = now_ms();
    while (__atomic_load_n(&completed, __ATOMIC_ACQUIRE) < NBLOCK) {
        if (now_ms() - t > 5000) {
            fprintf(stderr, "timeout: completed=%ld/%d\n", completed, NBLOCK);
            return 2;
        }
        usleep(1000);
    }
    wall = now_ms() - t0;

    /* Serial-on-one-hub would be ~NBLOCK*BLOCK_MS; offloaded is ~BLOCK_MS.
     * Use half the serial time as the bar -- generous against scheduling
     * noise, decisive against a wedge. */
    {
        double serial_ms = (double)NBLOCK * BLOCK_MS;
        int pass = (result_ok == NBLOCK) && (wall < serial_ms * 0.5);
        printf("blockpool: hubs=%d blockers=%d block=%dms wall=%.0fms "
               "result_ok=%ld/%d (serial would be ~%.0fms)\n",
               NHUBS, NBLOCK, BLOCK_MS, wall, result_ok, NBLOCK, serial_ms);
        printf("%s\n", pass
               ? "PASS: blocking calls offloaded -- hub stayed live, ran them concurrently"
               : "FAIL: blocking calls serialised -- hub was wedged");
        fflush(stdout);
        _exit(pass ? 0 : 1);
    }
}
