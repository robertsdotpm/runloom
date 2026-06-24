/* test_stall_steal.c -- repro for the stalled-hub work-recovery gap.
 *
 * Premise: a goroutine that occupies its hub's OS thread with a
 * non-yielding blocking C call (here: usleep) must not strand OTHER
 * goroutines whose work could run on an idle hub.
 *
 * Today even RUNLOOM_PER_G_TSTATE=1 strands them: a woken g is submitted to
 * its ORIGIN hub's owner-drained submission list, so if that hub never
 * loops (stuck in the C call) the woken g never reaches a stealable
 * deque -- no other hub can pick it up.
 *
 * Setup: park N worker goroutines (each on its own eventfd) across H
 * hubs so their origins spread over both hubs.  Then wake a "staller"
 * goroutine that occupies its hub with usleep(STALL_MS), and immediately
 * wake every worker.  Count how many respond within WINDOW_MS.
 *
 *   RED  (today, both modes)  : ~N*(H-1)/H respond -- the workers whose
 *                               origin is the stalled hub wait out the
 *                               whole usleep.
 *   GREEN (global wake queue) : N/N respond -- an idle hub drains the
 *                               global queue and runs the stalled hub's
 *                               woken workers.
 *
 * Build/run via tests_c/run_stall_test.sh.
 */
#define _GNU_SOURCE
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/eventfd.h>

#include "../../src/runloom_c/runloom_sched.h"
#include "../../src/runloom_c/mn_sched.h"
#include "../../src/runloom_c/netpoll.h"

#define N_WORKERS 64
#define NHUBS      2
#define STALL_MS   2000
#define WINDOW_MS  400

static int           w_efd[N_WORKERS];
static volatile long w_responded[N_WORKERS];
static volatile long parked_count = 0;
static int           staller_efd = -1;
static volatile int  staller_parked = 0;

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

/* A responsive worker: park on its eventfd, and on wake record that it
 * ran.  Its "origin hub" is whichever hub it first ran on here. */
static void worker_fn(void *arg)
{
    long i = (long)arg;
    __atomic_fetch_add(&parked_count, 1, __ATOMIC_RELAXED);
    if (runloom_netpoll_wait_fd(w_efd[i], RUNLOOM_NETPOLL_READ, -1LL) < 0) return;
    uint64_t v;
    (void)read(w_efd[i], &v, sizeof v);
    __atomic_store_n(&w_responded[i], 1, __ATOMIC_RELEASE);
    runloom_netpoll_unregister(w_efd[i]);
}

/* Occupies its hub's OS thread with a non-yielding blocking C call once
 * woken -- the thing time-sliced preemption can't interrupt.
 *
 * Two flavors (STALL_ALLOW_THREADS env):
 *  - default (raw usleep): the hub's tstate stays ATTACHED -> the wedge is
 *    Group-B class ATTACHED (CPU/raw-syscall), which a tstate-handoff CANNOT
 *    recover (it models a tight loop / un-wrapped cgo call).
 *  - STALL_ALLOW_THREADS=1: wrap the block in Py_BEGIN/END_ALLOW_THREADS so
 *    the hub tstate goes DETACHED for the block -- the well-behaved blocking-IO
 *    shape a standby M CAN adopt.  This is the handoff's RED baseline: today it
 *    still strands the workers (no handoff yet); GREEN once the handoff lands. */
static void staller_fn(void *arg)
{
    (void)arg;
    __atomic_store_n(&staller_parked, 1, __ATOMIC_RELEASE);
    if (runloom_netpoll_wait_fd(staller_efd, RUNLOOM_NETPOLL_READ, -1LL) < 0) return;
    uint64_t v;
    (void)read(staller_efd, &v, sizeof v);
    if (getenv("STALL_ALLOW_THREADS") != NULL) {
        Py_BEGIN_ALLOW_THREADS           /* detach hub tstate (DETACHED) */
        usleep(STALL_MS * 1000);         /* hold this hub's OS thread hostage */
        Py_END_ALLOW_THREADS             /* re-attach (contends with a standby) */
    } else {
        usleep(STALL_MS * 1000);         /* hub tstate stays ATTACHED */
    }
    runloom_netpoll_unregister(staller_efd);
}

int main(void)
{
    int i;
    Py_Initialize();
    runloom_sched_set_default_stack_size(32 * 1024);

    for (i = 0; i < N_WORKERS; i++) {
        w_efd[i] = eventfd(0, EFD_NONBLOCK);
        if (w_efd[i] < 0) { perror("eventfd"); return 2; }
    }
    staller_efd = eventfd(0, EFD_NONBLOCK);
    if (staller_efd < 0) { perror("eventfd staller"); return 2; }

    if (runloom_mn_init(NHUBS) < 0) { fprintf(stderr, "mn_init failed\n"); return 2; }

    /* staller first (so it claims an origin hub), then the workers. */
    if (runloom_mn_fiber_c(staller_fn, NULL) < 0) { fprintf(stderr, "go staller\n"); return 2; }
    for (i = 0; i < N_WORKERS; i++) {
        if (runloom_mn_fiber_c(worker_fn, (void *)(long)i) < 0) {
            fprintf(stderr, "go worker %d\n", i); return 2;
        }
    }

    /* Release the GIL while the hubs run (they attach their own PyThreadStates
     * and the staller manages one); a main thread holding it through the
     * orchestration below starves the hubs so no fiber dispatches ("setup
     * timeout: parked=0").  Below is pure atomics + eventfd syscalls and we
     * _exit() at the end, so no re-acquire is needed. */
    PyEval_SaveThread();

    /* Wait until everyone has run once and parked (origins established). */
    double t = now_ms();
    while (__atomic_load_n(&parked_count, __ATOMIC_RELAXED) < N_WORKERS ||
           !__atomic_load_n(&staller_parked, __ATOMIC_ACQUIRE)) {
        if (now_ms() - t > 5000) {
            fprintf(stderr, "setup timeout: parked=%ld staller=%d\n",
                    parked_count, staller_parked);
            return 2;
        }
        usleep(1000);
    }
    usleep(50 * 1000);   /* settle: let everyone be genuinely parked */

    /* Occupy the staller's hub, then give it a beat to enter usleep. */
    uint64_t one = 1;
    (void)write(staller_efd, &one, sizeof one);
    usleep(80 * 1000);

    /* Wake every worker.  The staller's hub is now blocked for STALL_MS. */
    double t0 = now_ms();
    for (i = 0; i < N_WORKERS; i++) (void)write(w_efd[i], &one, sizeof one);

    while (now_ms() - t0 < WINDOW_MS) usleep(1000);

    long responded = 0;
    for (i = 0; i < N_WORKERS; i++)
        if (__atomic_load_n(&w_responded[i], __ATOMIC_ACQUIRE)) responded++;

    int per_g = runloom_get_per_g_tstate_mode();
    printf("per_g_tstate=%d N=%d hubs=%d stall=%dms window=%dms responded=%ld/%d\n",
           per_g, N_WORKERS, NHUBS, STALL_MS, WINDOW_MS, responded, N_WORKERS);
    int pass = (responded == N_WORKERS);
    printf("%s\n", pass ? "PASS: stalled-hub work recovered by another hub"
                        : "FAIL: stalled-hub work stranded behind the blocked hub");
    fflush(stdout);
    _exit(pass ? 0 : 1);
}
