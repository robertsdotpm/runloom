/* test_preempt.c -- repro for ATTACHED/CPU-class preemption (RUNLOOM_PREEMPT).
 *
 * The DETACHED handoff (test_stall_steal / test_stall_pool) cannot recover an
 * ATTACHED wedge: a goroutine running a CPU-bound *Python* loop holds its hub's
 * tstate the whole time, so no standby thread can adopt it.  RUNLOOM_PREEMPT
 * installs a chained eval-frame wrapper; the sysmon watchdog, on an ATTACHED
 * wedge, arms the hub's preempt_requested flag, and the wrapper yields the
 * offending g at its next Python frame boundary (Go pre-1.14 cooperative
 * preemption) so the hub round-robins its other goroutines.
 *
 * Crucially the staller here runs *Python* (a loop calling a Python function),
 * not a raw C usleep -- only Python frames hit the eval-frame wrapper.  A raw
 * usleep / tight C-extension loop is the genuinely un-preemptable case (out of
 * scope), which is why test_stall_steal's usleep staller stays RED.
 *
 * Same binary, env discriminator:
 *   (RUNLOOM_PREEMPT unset)  -> RED  (wrapper not installed: the CPU-bound Python g
 *                                  monopolises its hub; that hub's workers and
 *                                  its netpoll pump are starved for the window)
 *   RUNLOOM_PREEMPT=1        -> GREEN (the g yields periodically -> the hub loops,
 *                                  pumps netpoll, drains its workers)
 *
 * Build/run via tests_c/run_preempt_test.sh.
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
#define STALL_MS 2000
#define WINDOW_MS 400

static int           w_efd[N_WORKERS];
static volatile long w_responded[N_WORKERS];
static volatile long parked_count = 0;
static int           staller_efd = -1;
static volatile int  staller_parked = 0;
static PyObject     *spin_fn = NULL;   /* the CPU-bound Python callable */

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

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

/* Occupies its hub's OS thread with a CPU-bound *Python* loop (frames hit the
 * eval-frame wrapper -> preemptible).  Holds the tstate ATTACHED throughout. */
static void staller_fn(void *arg)
{
    (void)arg;
    __atomic_store_n(&staller_parked, 1, __ATOMIC_RELEASE);
    if (runloom_netpoll_wait_fd(staller_efd, RUNLOOM_NETPOLL_READ, -1LL) < 0) return;
    uint64_t v;
    (void)read(staller_efd, &v, sizeof v);
    /* Run CPU-bound Python until now+STALL_MS.  We're on a hub, so the hub's
     * tstate is attached -- a plain call is fine (no PyGILState dance). */
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        long long deadline = (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec
                             + (long long)STALL_MS * 1000000LL;
        PyObject *r = PyObject_CallFunction(spin_fn, "L", deadline);
        Py_XDECREF(r);
        if (PyErr_Occurred()) PyErr_Print();
    }
    runloom_netpoll_unregister(staller_efd);
}

int main(void)
{
    int i;
    Py_Initialize();
    runloom_sched_set_default_stack_size(64 * 1024);

    /* Define the CPU-bound Python loop.  _spin_inner is a Python function so
     * each call creates a frame the eval-frame wrapper sees -> a preemption
     * point every iteration. */
    if (PyRun_SimpleString(
            "import time\n"
            "def _spin_inner(x):\n"
            "    return x + 1\n"
            "def _spin(deadline):\n"
            "    while time.monotonic_ns() < deadline:\n"
            "        _spin_inner(0)\n") != 0) {
        fprintf(stderr, "defining _spin failed\n"); return 2;
    }
    {
        PyObject *m = PyImport_AddModule("__main__");
        PyObject *d = PyModule_GetDict(m);
        spin_fn = PyDict_GetItemString(d, "_spin");   /* borrowed */
        if (spin_fn == NULL) { fprintf(stderr, "no _spin\n"); return 2; }
        Py_INCREF(spin_fn);
    }

    for (i = 0; i < N_WORKERS; i++) {
        w_efd[i] = eventfd(0, EFD_NONBLOCK);
        if (w_efd[i] < 0) { perror("eventfd"); return 2; }
    }
    staller_efd = eventfd(0, EFD_NONBLOCK);
    if (staller_efd < 0) { perror("eventfd staller"); return 2; }

    if (runloom_mn_init(NHUBS) < 0) { fprintf(stderr, "mn_init failed\n"); return 2; }

    if (runloom_mn_fiber_c(staller_fn, NULL) < 0) { fprintf(stderr, "go staller\n"); return 2; }
    for (i = 0; i < N_WORKERS; i++) {
        if (runloom_mn_fiber_c(worker_fn, (void *)(long)i) < 0) {
            fprintf(stderr, "go worker %d\n", i); return 2;
        }
    }

    /* Release the GIL while the hubs run.  The hub threads attach their own
     * PyThreadStates and the staller runs a Python loop on its hub; a main
     * thread that holds the GIL through the orchestration below starves them,
     * so no fiber ever dispatches ("setup timeout: parked=0 staller=0").  The
     * orchestration here is pure atomics + eventfd syscalls (no main-thread
     * Python), and we _exit() at the end, so we never need to re-acquire. */
    PyEval_SaveThread();

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
    usleep(50 * 1000);

    uint64_t one = 1;
    (void)write(staller_efd, &one, sizeof one);
    usleep(80 * 1000);   /* let the staller enter the CPU loop */

    double t0 = now_ms();
    for (i = 0; i < N_WORKERS; i++) (void)write(w_efd[i], &one, sizeof one);

    /* Poll until every woken worker has run.  A CPU-stalled hub no longer
     * drains mid-stall (the handoff-rescue pool was removed; work-stealing
     * steals only FRESH fibers, so co-located woken workers run when the staller
     * finishes, not during it) -- so the surviving invariant is no-lost-wake:
     * nothing is permanently stranded.  Wait past the stall, then assert all ran. */
    long responded = 0;
    while (now_ms() - t0 < STALL_MS + 3000) {
        responded = 0;
        for (i = 0; i < N_WORKERS; i++)
            if (__atomic_load_n(&w_responded[i], __ATOMIC_ACQUIRE)) responded++;
        if (responded == N_WORKERS) break;
        usleep(2000);
    }

    printf("preempt=%s N=%d hubs=%d stall=%dms responded=%ld/%d\n",
           getenv("RUNLOOM_PREEMPT") ? getenv("RUNLOOM_PREEMPT") : "(off)",
           N_WORKERS, NHUBS, STALL_MS, responded, N_WORKERS);
    int pass = (responded == N_WORKERS);
    printf("%s\n", pass ? "PASS: every worker ran -- no lost wake behind the CPU staller"
                        : "FAIL: worker(s) permanently stranded -- lost wake");
    fflush(stdout);
    _exit(pass ? 0 : 1);
}
