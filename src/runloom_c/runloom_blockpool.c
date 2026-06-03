/* runloom_blockpool.c -- blocking-offload thread pool.  See runloom_blockpool.h.
 *
 * A bounded set of worker OS threads drain an MPSC job queue (mutex +
 * condvar).  runloom_blocking_call enqueues one job (allocated on the
 * caller goroutine's own coroutine stack -- alive across the park),
 * parks the goroutine, and a worker runs the job and wakes it.
 *
 * Waking it integrates with BOTH schedulers exactly like an io_uring
 * completion does:
 *   - the worker re-queues the specific goroutine via runloom_mn_wake_g
 *     (hub) or runloom_sched_wake_safe (single-thread sched);
 *   - an `inflight` counter keeps the single-thread drain loop from
 *     exiting or busy-spinning while a job is outstanding (a park_safe'd
 *     goroutine has no netpoll/iouring footprint of its own);
 *   - for the single-thread sched -- which, unlike the busy-polling hubs,
 *     blocks in epoll_wait with no timeout -- the worker also pokes the
 *     netpoll pump-interrupt eventfd so the otherwise-idle scheduler
 *     wakes to drain its wake_list.  Hubs busy-poll (~1 ms) so wake_g
 *     alone suffices there.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "runloom_blockpool.h"
#include "runloom_sched.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "coro.h"
#include "runloom_crash.h"

#include <stdlib.h>
#include <string.h>

#define RUNLOOM_BLOCKPOOL_MAX     64
#define RUNLOOM_BLOCKPOOL_DEFAULT 8

/* One offloaded job.  Lives on the calling goroutine's coroutine stack,
 * which stays mapped across the park, so no heap alloc is needed. */
typedef struct runloom_block_job {
    void *(*fn)(void *);
    void *arg;
    void *result;
    runloom_g_t *g;                  /* the parked goroutine */
    void *hub;                    /* its hub, or NULL for the single-thread sched */
    int done;                     /* set (release) once the worker is fully
                                   * finished touching this job; the parked
                                   * goroutine spins on it so a spurious wake
                                   * (e.g. task.cancel() -> G.wake()) can't
                                   * return and free the stack job mid-worker. */
    struct runloom_block_job *next;
} runloom_block_job_t;

static runloom_mutex_t  bp_lock = RUNLOOM_MUTEX_STATIC_INIT;
static runloom_cond_t   bp_cond;                 /* workers wait here for jobs */
static runloom_block_job_t *bp_head = NULL;
static runloom_block_job_t *bp_tail = NULL;
static int           bp_inited   = 0;         /* 0 = not started, 1 = running */
static int           bp_failed   = 0;         /* init tried and failed -> run inline */
static volatile int  bp_stopping = 0;
static int           bp_n_workers = 0;
static int           bp_wake_armed = 0;       /* pump-interrupt available (single-thread offload) */
static volatile long bp_inflight = 0;         /* jobs submitted, not yet completed */
static runloom_thread_t bp_threads[RUNLOOM_BLOCKPOOL_MAX];

/* bp_lock uses RUNLOOM_MUTEX_STATIC_INIT.  On POSIX that is a live mutex
 * (PTHREAD_MUTEX_INITIALIZER); on Windows it is only a zeroed
 * CRITICAL_SECTION that MUST be InitializeCriticalSection'd before first
 * use -- locking it zero-initialised is undefined behaviour.  Initialise
 * it exactly once, race-free, before any lock.  We can't take bp_lock to
 * guard this (it's the very thing being set up), so use the same 0/1/2
 * CAS+spin guard the rest of runloom_c uses for one-time setup; this is
 * safe under the lock-free hub callers on free-threaded 3.13t.  No-op on
 * POSIX, where the static initialiser is already usable. */
#if defined(RUNLOOM_OS_WINDOWS)
static int bp_lock_state = 0;   /* 0 = uninit, 1 = initialising, 2 = ready */
static void bp_lock_ensure(void)
{
    int expected = 0;
    if (__atomic_load_n(&bp_lock_state, __ATOMIC_ACQUIRE) == 2) return;
    if (__atomic_compare_exchange_n(&bp_lock_state, &expected, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&bp_lock);
        __atomic_store_n(&bp_lock_state, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&bp_lock_state, __ATOMIC_ACQUIRE) != 2) {
            /* winner only runs InitializeCriticalSection -- a brief spin */
        }
    }
}
#else
#  define bp_lock_ensure() ((void)0)
#endif

long runloom_blockpool_inflight(void)
{
    return __atomic_load_n(&bp_inflight, __ATOMIC_ACQUIRE);
}

static RUNLOOM_THREAD_RET runloom_blockpool_worker(void *arg)
{
    (void)arg;
    /* Arm this worker's sigaltstack too -- offloaded user C code can fault here.
     * No-op unless the crash handler is installed. */
    runloom_crash_thread_arm();
    for (;;) {
        runloom_block_job_t *job;
        runloom_mutex_lock(&bp_lock);
        while (bp_head == NULL && !bp_stopping) {
            runloom_cond_wait(&bp_cond, &bp_lock);
        }
        if (bp_head == NULL) {            /* woken only to stop */
            runloom_mutex_unlock(&bp_lock);
            break;
        }
        job = bp_head;
        bp_head = job->next;
        if (bp_head == NULL) bp_tail = NULL;
        runloom_mutex_unlock(&bp_lock);

        /* Snapshot the wake target BEFORE publishing `done`.  Once `done` is
         * set, the parked goroutine may resume and free its stack `job` at any
         * instant, so neither the wake below nor anything after it may read
         * job->.  (The goroutine can be resumed early by a spurious wake --
         * task.cancel() -> G.wake() -- which is exactly the use-after-free that
         * crashed here: it returned from runloom_blocking_call and unwound while
         * this worker was still about to call job->fn.) */
        {
            void *hub = job->hub;
            runloom_g_t *g = job->g;

            /* Run the blocking work off the hub.  No GIL is held here. */
            job->result = job->fn(job->arg);

            /* Publish completion: release-store so the resumed goroutine sees
             * job->result, and a marker that the worker is done with job-> .
             * After this line the worker touches ONLY locals + statics. */
            __atomic_store_n(&job->done, 1, __ATOMIC_RELEASE);

            /* Re-queue the goroutine, then (single-thread only) kick the pump
             * so an idle scheduler wakes.  Re-queue BEFORE decrementing
             * inflight so the drain loop, which stays alive while inflight>0,
             * sees the goroutine on its wake_list the moment inflight hits 0. */
            if (hub != NULL) {
                runloom_mn_wake_g(hub, g);
            } else {
                runloom_sched_wake_safe(g);
                runloom_netpoll_wake_pump();
            }
            __atomic_sub_fetch(&bp_inflight, 1, __ATOMIC_ACQ_REL);
        }
    }
    RUNLOOM_THREAD_RETURN((void *)0);
}

int runloom_blockpool_init(int n_workers)
{
    int i, started;

    /* Fast path: already up, or a prior attempt failed (don't retry). */
    if (__atomic_load_n(&bp_inited, __ATOMIC_ACQUIRE)) return 0;
    if (__atomic_load_n(&bp_failed, __ATOMIC_ACQUIRE)) return -1;

    bp_lock_ensure();               /* Windows: make bp_lock usable */
    runloom_mutex_lock(&bp_lock);
    if (bp_inited) { runloom_mutex_unlock(&bp_lock); return 0; }
    if (bp_failed) { runloom_mutex_unlock(&bp_lock); return -1; }

    if (n_workers <= 0) {
        const char *e = getenv("RUNLOOM_BLOCKPOOL_WORKERS");
        n_workers = (e != NULL) ? atoi(e) : RUNLOOM_BLOCKPOOL_DEFAULT;
        if (n_workers <= 0) n_workers = RUNLOOM_BLOCKPOOL_DEFAULT;
    }
    if (n_workers > RUNLOOM_BLOCKPOOL_MAX) n_workers = RUNLOOM_BLOCKPOOL_MAX;

    if (runloom_cond_init(&bp_cond) != 0) {
        __atomic_store_n(&bp_failed, 1, __ATOMIC_RELEASE);
        runloom_mutex_unlock(&bp_lock);
        return -1;
    }
    bp_stopping = 0;
    bp_head = bp_tail = NULL;
    /* Arm the pump interrupt so single-thread-scheduler offloads can wake
     * an idle pump.  Best-effort: if the backend has no such primitive
     * (non-epoll), single-thread callers fall back to inline below. */
    bp_wake_armed = (runloom_netpoll_wake_pump_arm() == 0);
    started = 0;
    for (i = 0; i < n_workers; i++) {
        if (runloom_thread_create(&bp_threads[i], runloom_blockpool_worker,
                               NULL) != 0) {
            break;
        }
        started++;
    }
    if (started == 0) {
        runloom_cond_destroy(&bp_cond);
        __atomic_store_n(&bp_failed, 1, __ATOMIC_RELEASE);
        runloom_mutex_unlock(&bp_lock);
        return -1;
    }
    bp_n_workers = started;
    __atomic_store_n(&bp_inited, 1, __ATOMIC_RELEASE);
    runloom_mutex_unlock(&bp_lock);
    return 0;
}

void runloom_blockpool_fini(void)
{
    int i, n;
    bp_lock_ensure();               /* Windows: make bp_lock usable */
    runloom_mutex_lock(&bp_lock);
    if (!bp_inited) { runloom_mutex_unlock(&bp_lock); return; }
    bp_stopping = 1;
    n = bp_n_workers;
    runloom_cond_broadcast(&bp_cond);
    runloom_mutex_unlock(&bp_lock);

    for (i = 0; i < n; i++) runloom_thread_join(bp_threads[i]);

    runloom_mutex_lock(&bp_lock);
    runloom_cond_destroy(&bp_cond);
    bp_inited = 0;
    bp_n_workers = 0;
    bp_stopping = 0;
    bp_head = bp_tail = NULL;
    runloom_mutex_unlock(&bp_lock);
}

/* Reset the blocking-offload pool in a forked CHILD.  The worker OS threads
 * are gone, so we must NOT join them (runloom_blockpool_fini would hang) -- we
 * reset to "not started" so the next offload re-creates the pool fresh.  The
 * child is single-threaded here: re-init the sync objects (a dead worker may
 * have held bp_lock at fork), drop the inherited job queue (its jobs point at
 * parent goroutines), and zero the counters. */
void runloom_blockpool_reset_after_fork(void)
{
    runloom_mutex_init(&bp_lock);
#if defined(RUNLOOM_OS_WINDOWS)
    bp_lock_state = 2;
#endif
    bp_head = bp_tail = NULL;
    bp_inited = 0;
    bp_failed = 0;
    bp_stopping = 0;
    bp_n_workers = 0;
    bp_wake_armed = 0;
    __atomic_store_n(&bp_inflight, 0, __ATOMIC_RELAXED);
    /* bp_cond is (re)created by runloom_blockpool_init on next offload; the
     * inherited one is abandoned (no destroy -- it may be in an invalid
     * post-fork state, and destroying an invalid cond is itself UB). */
}

void *runloom_blocking_call(void *(*fn)(void *), void *arg)
{
    void *hub = runloom_mn_current_hub_opaque();
    runloom_g_t *g;
    runloom_block_job_t job;

    if (hub != NULL) {
        g = runloom_mn_tls_current_g();
    } else {
        runloom_sched_t *s = runloom_sched_get();
        g = (s != NULL) ? s->current : NULL;
    }
    /* Must be inside a goroutine to park.  Also fall back to inline when
     * the pool can't start, or -- for the single-thread sched only --
     * when the pump interrupt isn't available (no way to wake an idle
     * pump on this backend yet).  Hubs busy-poll, so they never need it. */
    if (g == NULL || runloom_blockpool_init(0) != 0 ||
        (hub == NULL && !bp_wake_armed)) {
        return fn(arg);
    }

    job.fn     = fn;
    job.arg    = arg;
    job.result = NULL;
    job.g      = g;
    job.hub    = hub;
    job.done   = 0;
    job.next   = NULL;

    /* Count the job as outstanding BEFORE enqueueing so the single-thread
     * drain loop never observes a transient "no work" between our enqueue
     * and the worker re-queueing us, which would exit the loop early. */
    __atomic_add_fetch(&bp_inflight, 1, __ATOMIC_ACQ_REL);

    runloom_mutex_lock(&bp_lock);
    if (bp_tail != NULL) bp_tail->next = &job;
    else                 bp_head = &job;
    bp_tail = &job;
    runloom_cond_signal(&bp_cond);
    runloom_mutex_unlock(&bp_lock);

    /* Park until the WORKER signals completion (job.done).  Re-park on any
     * other wake: a task.cancel() delivers G.wake() to this goroutine while it
     * is parked here, and returning then would free the stack `job` while the
     * worker still references it (use-after-free SIGSEGV).  The worker always
     * runs the job and wakes us, so the loop always terminates; cancellation is
     * delivered at the next real await-point after we return.  Hub: snap the
     * per-g tstate and yield.  Single-thread: race-safe park_safe/wake_safe. */
    if (hub != NULL) {
        while (!__atomic_load_n(&job.done, __ATOMIC_ACQUIRE)) {
            runloom_sched_park_current();
            runloom_coro_yield();
        }
    } else {
        while (!__atomic_load_n(&job.done, __ATOMIC_ACQUIRE)) {
            runloom_sched_park_safe();
        }
    }

    return job.result;
}
