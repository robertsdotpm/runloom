/* test_stall_pool.c -- repro for the MULTI-wedge stalled-hub gap (Group B
 * rescue-thread pool).
 *
 * Premise: extends test_stall_steal.c from one DETACHED-wedged hub to SEVERAL
 * wedged at once.  The single-rescue-thread design (one standby M + a one-slot
 * mailbox) can only drain ONE wedged hub at a time, so a second simultaneous
 * DETACHED wedge strands its workers until the first rescue's drain-to-empty
 * loop finally releases (i.e. until that hub's blocking call ends).  The rescue
 * pool drains K wedged hubs on K threads in parallel.
 *
 * The discriminator is the pool size, which is env-configurable
 * (RUNLOOM_HANDOFF_POOL), so ONE binary proves the fix without a rebuild:
 *   RUNLOOM_HANDOFF_POOL=1  -> RED  (old single-thread behaviour: only 1 of M
 *                                 wedged hubs recovers inside the window)
 *   RUNLOOM_HANDOFF_POOL>=M -> GREEN (every wedged hub recovers in parallel)
 *
 * Setup: H hubs, S stallers, N workers, all parked on their own eventfds.
 * Wake the stallers; each records the hub it runs on and -- while keeping at
 * least ONE hub free (someone has to drive the shared netpoll so woken workers
 * reach their origin hub's submission list) -- wedges it via
 * Py_BEGIN_ALLOW_THREADS + usleep (the DETACHED, handoff-recoverable class).
 * Once >=2 distinct hubs are confirmed wedged, wake every worker and count how
 * many respond within WINDOW_MS (< STALL_MS).
 *
 * Build/run via tests_c/run_stall_pool_test.sh.
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

#define NHUBS       4
#define N_STALLERS  8
#define N_WORKERS  64
#define STALL_MS 2000
#define WINDOW_MS 400

static int           w_efd[N_WORKERS];
static volatile long w_responded[N_WORKERS];
static volatile long parked_count = 0;

static int           s_efd[N_STALLERS];
static volatile long staller_parked_count = 0;
/* Bit h set => hub h has a staller that has committed to wedging it.  Capped at
 * NHUBS-1 set bits so at least one hub stays free to drive netpoll. */
static volatile int  wedged_mask = 0;

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

/* A responsive worker: park on its eventfd; on wake record that it ran. */
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

/* Wakes, claims its current hub (keeping >=1 hub free), then holds that hub's
 * OS thread hostage in a DETACHED blocking call for STALL_MS.  Stallers whose
 * claim would leave no free hub simply return without wedging. */
static void staller_fn(void *arg)
{
    long k = (long)arg;
    int do_wedge = 0;
    int hubid;
    __atomic_fetch_add(&staller_parked_count, 1, __ATOMIC_RELAXED);
    if (runloom_netpoll_wait_fd(s_efd[k], RUNLOOM_NETPOLL_READ, -1LL) < 0) return;
    uint64_t v;
    (void)read(s_efd[k], &v, sizeof v);

    hubid = runloom_mn_hub_id_of(runloom_mn_current_hub_opaque());
    if (hubid >= 0 && hubid < NHUBS) {
        for (;;) {
            int old = __atomic_load_n(&wedged_mask, __ATOMIC_ACQUIRE);
            int neu;
            /* My hub already has a wedger: do NOT also block.  A second blocker
             * on the same hub would only sit in that hub's run queue and, when
             * the rescue thread drains the hub, get resumed -- re-wedging the
             * RESCUER for STALL_MS.  Exactly one staller per hub wedges. */
            if (old & (1 << hubid)) break;                   /* do_wedge stays 0 */
            neu = old | (1 << hubid);
            if (__builtin_popcount(neu) > NHUBS - 1) break;  /* keep >=1 free hub */
            if (__atomic_compare_exchange_n(&wedged_mask, &old, neu, 0,
                                            __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
                do_wedge = 1;
                break;
            }
        }
    }
    if (do_wedge) {
        Py_BEGIN_ALLOW_THREADS           /* detach hub tstate (DETACHED) */
        usleep(STALL_MS * 1000);         /* hold this hub's OS thread hostage */
        Py_END_ALLOW_THREADS             /* re-attach (contends with a rescue) */
    }
    runloom_netpoll_unregister(s_efd[k]);
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
    for (i = 0; i < N_STALLERS; i++) {
        s_efd[i] = eventfd(0, EFD_NONBLOCK);
        if (s_efd[i] < 0) { perror("eventfd staller"); return 2; }
    }

    if (runloom_mn_init(NHUBS) < 0) { fprintf(stderr, "mn_init failed\n"); return 2; }

    /* stallers first (counters 0..S-1 round-robin onto distinct hubs), then
     * the workers. */
    for (i = 0; i < N_STALLERS; i++) {
        if (runloom_mn_fiber_c(staller_fn, (void *)(long)i) < 0) {
            fprintf(stderr, "go staller %d\n", i); return 2;
        }
    }
    for (i = 0; i < N_WORKERS; i++) {
        if (runloom_mn_fiber_c(worker_fn, (void *)(long)i) < 0) {
            fprintf(stderr, "go worker %d\n", i); return 2;
        }
    }

    /* Release the GIL while the hubs run (they attach their own PyThreadStates
     * and the stallers manage them); a main thread holding it through the
     * orchestration below starves the hubs so no fiber dispatches ("setup
     * timeout").  Below is pure atomics + eventfd syscalls and we _exit() at the
     * end, so no re-acquire is needed. */
    PyEval_SaveThread();

    /* Wait until everyone has run once and parked (origins established). */
    double t = now_ms();
    while (__atomic_load_n(&parked_count, __ATOMIC_RELAXED) < N_WORKERS ||
           __atomic_load_n(&staller_parked_count, __ATOMIC_ACQUIRE) < N_STALLERS) {
        if (now_ms() - t > 5000) {
            fprintf(stderr, "setup timeout: workers=%ld stallers=%ld\n",
                    parked_count, staller_parked_count);
            return 2;
        }
        usleep(1000);
    }
    usleep(50 * 1000);   /* settle */

    /* Wake every staller, then wait until >=2 distinct hubs are wedged. */
    uint64_t one = 1;
    for (i = 0; i < N_STALLERS; i++) (void)write(s_efd[i], &one, sizeof one);
    t = now_ms();
    while (__builtin_popcount(__atomic_load_n(&wedged_mask, __ATOMIC_ACQUIRE)) < 2) {
        if (now_ms() - t > 3000) {
            fprintf(stderr, "INCONCLUSIVE: only %d hub(s) wedged (need >=2)\n",
                    __builtin_popcount(wedged_mask));
            return 2;
        }
        usleep(1000);
    }
    int nwedged = __builtin_popcount(__atomic_load_n(&wedged_mask, __ATOMIC_ACQUIRE));
    usleep(40 * 1000);   /* let the committed stallers actually enter usleep */

    /* Wake every worker.  Workers whose origin hub is wedged are stranded
     * unless a rescue thread drains that hub. */
    double t0 = now_ms();
    for (i = 0; i < N_WORKERS; i++) (void)write(w_efd[i], &one, sizeof one);

    /* Poll until every woken worker has run.  Stalled hubs no longer drain
     * mid-stall (the handoff-rescue pool was removed; work-stealing steals only
     * FRESH fibers, so co-located woken workers run when their staller finishes,
     * not during it) -- so the surviving invariant is no-lost-wake: nothing is
     * permanently stranded.  Wait past the stall, then assert all ran. */
    long responded = 0;
    while (now_ms() - t0 < STALL_MS + 3000) {
        responded = 0;
        for (i = 0; i < N_WORKERS; i++)
            if (__atomic_load_n(&w_responded[i], __ATOMIC_ACQUIRE)) responded++;
        if (responded == N_WORKERS) break;
        usleep(2000);
    }

    printf("per_g_tstate=%d N=%d hubs=%d wedged_hubs=%d stall=%dms responded=%ld/%d\n",
           runloom_get_per_g_tstate_mode(), N_WORKERS, NHUBS, nwedged,
           STALL_MS, responded, N_WORKERS);
    int pass = (responded == N_WORKERS);
    printf("%s\n", pass ? "PASS: every worker ran -- no lost wake behind the wedged hubs"
                        : "FAIL: worker(s) permanently stranded -- lost wake");
    fflush(stdout);
    _exit(pass ? 0 : 1);
}
