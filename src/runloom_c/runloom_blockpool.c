/* runloom_blockpool.c -- blocking-offload thread pool.  See runloom_blockpool.h.
 *
 * A bounded set of worker OS threads drain a SHARDED MPSC job queue.
 * runloom_blocking_call enqueues one job (allocated on the caller fiber's own
 * coroutine stack -- alive across the park), parks the fiber, and a worker runs
 * the job and wakes it.
 *
 * SHARDING (the 100k-offload scaling fix).  The pool used to be a SINGLE global
 * mutex + condvar + queue: every submit AND every worker dequeue serialized on
 * one lock, so N hubs each offloading at once convoyed on that lock (the p23
 * offload wedge).  The queue is now split into `bp_nshard` independent shards,
 * each with its OWN lock + condvar + queue + worker subset.  A submitting hub
 * hashes to a shard by its hub pointer, so a hub's offloads contend only with
 * that one shard (mostly itself) -- the cross-hub convoy is gone.  Default
 * nshard tracks the live hub count; RUNLOOM_BLOCKPOOL_SHARDS=1 collapses to the
 * old single-queue behaviour (kept for A/B and as a safety fallback).  The
 * per-job completion handshake (the `done` FSM, wake_safe, the re-park loop) is
 * UNCHANGED -- sharding only changes which queue a job sits on and which condvar
 * its worker waits on.
 *
 * Waking a parked fiber integrates with BOTH schedulers exactly like an io_uring
 * completion does:
 *   - the worker re-queues the specific fiber via runloom_sched_wake_safe
 *     (routes by g->park_hub: runloom_mn_wake_g for a hub, the owner's wake_list
 *     for the single-thread sched);
 *   - an `inflight` counter keeps the single-thread drain loop from exiting or
 *     busy-spinning while a job is outstanding (a park_safe'd fiber has no
 *     netpoll/iouring footprint of its own);
 *   - for the single-thread sched -- which, unlike the busy-polling hubs, blocks
 *     in epoll_wait with no timeout -- the worker also pokes the netpoll
 *     pump-interrupt eventfd so the otherwise-idle scheduler wakes to drain its
 *     wake_list.  Hubs busy-poll (~1 ms) so wake_g alone suffices there.
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
#include "runloom_lockrank.h"
#include "runloom_blockpool.h"
#include "runloom_sched.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "coro.h"
#include "runloom_crash.h"
#include "runloom_fsm.h"   /* RUNLOOM_FSM_VALIDATE single-completion witness */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define RUNLOOM_BLOCKPOOL_MAX          256   /* hard cap on total worker threads  */
#define RUNLOOM_BLOCKPOOL_DEFAULT      8     /* total workers when nshard == 1     */
#define RUNLOOM_BP_SHARDS_MAX          32    /* hard cap on submit shards          */
#define RUNLOOM_BP_WORKERS_PER_SHARD   3     /* default workers per shard (>=1)    */

#if defined(_MSC_VER)
#  define BP_CACHELINE_ALIGN __declspec(align(64))
#else
#  define BP_CACHELINE_ALIGN __attribute__((aligned(64)))
#endif

/* The job-completion handshake is a ONE-WAY 2-state FSM on `done`: a job is
 * PENDING until the single worker that owns it publishes DONE (release-store)
 * exactly once; the parked fiber then resumes via the GenMC-proven
 * park_generic/wake_safe Dekker (tools/verify/genmc/sched_parkwake.c).  Single writer +
 * one-way + the atomic-is-the-proof => harden-in-place: explicit states + a
 * single-completion witness under -DRUNLOOM_FSM_VALIDATE, no runtime table. */
enum {
    RUNLOOM_BP_JOB_PENDING = 0,   /* worker still touching job-> ; fiber must spin */
    RUNLOOM_BP_JOB_DONE    = 1    /* worker finished; fiber may resume + free job  */
};

/* One offloaded job.  Lives on the calling fiber's coroutine stack,
 * which stays mapped across the park, so no heap alloc is needed. */
typedef struct runloom_block_job {
    void *(*fn)(void *);
    void *arg;
    void *result;
    runloom_g_t *g;                  /* the parked fiber */
    void *hub;                    /* its hub, or NULL for the single-thread sched */
    int done;                     /* set (release) once the worker is fully
                                   * finished touching this job; the parked
                                   * fiber spins on it so a spurious wake
                                   * (e.g. task.cancel() -> G.wake()) can't
                                   * return and free the stack job mid-worker. */
    struct runloom_block_job *next;
} runloom_block_job_t;

/* One submit shard: an independent MPSC job queue.  Cache-line aligned so two
 * shards' hot fields (and their lock/cond futex words) never share a line. */
typedef struct runloom_bp_shard {
    runloom_mutex_t      lock;
    runloom_cond_t       cond;     /* this shard's workers wait here for jobs */
    runloom_block_job_t *head;
    runloom_block_job_t *tail;
} BP_CACHELINE_ALIGN runloom_bp_shard_t;

/* Cold-path lock: guards init / fini / the bp_inited flag ONLY.  Never taken on
 * the submit hot path (that is the per-shard lock) -- this is what kills the
 * old single-global-mutex convoy. */
static runloom_mutex_t  bp_init_lock = RUNLOOM_MUTEX_STATIC_INIT;

static runloom_bp_shard_t bp_shards[RUNLOOM_BP_SHARDS_MAX];
static int           bp_nshard    = 1;        /* live shard count (>=1)        */
static int           bp_inited    = 0;        /* 0 = not started, 1 = running  */
static int           bp_failed    = 0;        /* init tried and failed -> inline */
static volatile int  bp_stopping  = 0;
static int           bp_n_workers = 0;
static int           bp_wake_armed = 0;       /* pump-interrupt available (single-thread offload) */
static volatile long bp_inflight  = 0;        /* jobs submitted, not yet completed (global) */
static runloom_thread_t bp_threads[RUNLOOM_BLOCKPOOL_MAX];
static int           bp_worker_shard[RUNLOOM_BLOCKPOOL_MAX];  /* worker i -> its shard */

/* Installed by the Python layer (see runloom_blockpool.h); NULL = pure-C use. */
void (*runloom_blockpool_worker_thread_fini)(void) = NULL;

/* bp_init_lock uses RUNLOOM_MUTEX_STATIC_INIT.  On POSIX that is a live mutex
 * (PTHREAD_MUTEX_INITIALIZER); on Windows it is only a zeroed CRITICAL_SECTION
 * that MUST be InitializeCriticalSection'd before first use.  Initialise it
 * exactly once, race-free, before any lock, using the same 0/1/2 CAS+spin guard
 * the rest of runloom_c uses for one-time setup.  No-op on POSIX. */
#if defined(RUNLOOM_OS_WINDOWS)
static int bp_init_lock_state = 0;   /* 0 = uninit, 1 = initialising, 2 = ready */
static void bp_init_lock_ensure(void)
{
    int expected = 0;
    if (__atomic_load_n(&bp_init_lock_state, __ATOMIC_ACQUIRE) == 2) return;
    if (__atomic_compare_exchange_n(&bp_init_lock_state, &expected, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&bp_init_lock);
        __atomic_store_n(&bp_init_lock_state, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&bp_init_lock_state, __ATOMIC_ACQUIRE) != 2) {
            /* winner only runs InitializeCriticalSection -- a brief spin */
        }
    }
}
#else
#  define bp_init_lock_ensure() ((void)0)
#endif

long runloom_blockpool_inflight(void)
{
    return __atomic_load_n(&bp_inflight, __ATOMIC_ACQUIRE);
}

/* Pick a submit shard for a job.  Hash the HUB pointer so a given hub always
 * lands on the same shard (its submits contend only with that shard -- mostly
 * itself, since hubs spread evenly).  Off a hub (single-thread sched) the hub is
 * NULL; hash the fiber pointer instead so top-level/single-thread offloads still
 * spread.  Low bits are dropped (allocation alignment carries no entropy). */
RUNLOOM_INLINE int bp_shard_for(void *hub, runloom_g_t *g)
{
    uintptr_t k = (uintptr_t)(hub ? hub : (void *)g);
    int n = bp_nshard;
    if (n <= 1) return 0;
    return (int)((k >> 6) % (unsigned)n);
}

static RUNLOOM_THREAD_RET runloom_blockpool_worker(void *arg)
{
    runloom_bp_shard_t *sh = &bp_shards[(int)(intptr_t)arg];
    /* Arm this worker's sigaltstack too -- offloaded user C code can fault here.
     * No-op unless the crash handler is installed. */
    runloom_crash_thread_arm();
    for (;;) {
        runloom_block_job_t *job;
        RUNLOOM_RLOCK(&sh->lock, RUNLOOM_RANK_BLOCKPOOL);
        while (sh->head == NULL && !bp_stopping) {
            runloom_cond_wait(&sh->cond, &sh->lock);
        }
        if (sh->head == NULL) {            /* woken only to stop */
            RUNLOOM_RUNLOCK(&sh->lock, RUNLOOM_RANK_BLOCKPOOL);
            /* Release this worker's persistent Python tstate (if it created one)
             * on this thread, before exit -- gilstate-TSS is thread-bound. */
            if (runloom_blockpool_worker_thread_fini != NULL)
                runloom_blockpool_worker_thread_fini();
            break;
        }
        job = sh->head;
        sh->head = job->next;
        if (sh->head == NULL) sh->tail = NULL;
        RUNLOOM_RUNLOCK(&sh->lock, RUNLOOM_RANK_BLOCKPOOL);

        /* Snapshot the wake target BEFORE publishing `done`.  Once `done` is
         * set, the parked fiber may resume and free its stack `job` at any
         * instant, so neither the wake below nor anything after it may read
         * job->.  (The fiber can be resumed early by a spurious wake --
         * task.cancel() -> G.wake() -- which is exactly the use-after-free that
         * crashed here: it returned from runloom_blocking_call and unwound while
         * this worker was still about to call job->fn.) */
        {
            void *hub = job->hub;
            runloom_g_t *g = job->g;

            /* Run the blocking work off the hub.  No GIL is held here. */
            job->result = job->fn(job->arg);

            /* Publish completion: release-store so the resumed fiber sees
             * job->result, and a marker that the worker is done with job-> .
             * After this line the worker touches ONLY locals + statics. */
#if defined(RUNLOOM_FSM_VALIDATE)
            /* Witness the one-way single-completion invariant: a job is published
             * DONE exactly once (PENDING -> DONE).  A second completion would mean
             * two workers owned the same job -> the resumed fiber's stack `job`
             * could be freed under the second store. */
            if (__atomic_load_n(&job->done, __ATOMIC_RELAXED) != RUNLOOM_BP_JOB_PENDING)
                runloom_fsm_violation("blockpool_job", RUNLOOM_BP_JOB_DONE,
                                      RUNLOOM_BP_JOB_DONE, __FILE__, __LINE__);
#endif
            __atomic_store_n(&job->done, RUNLOOM_BP_JOB_DONE, __ATOMIC_RELEASE);

            /* Re-queue the fiber via the one audited race-safe waker.  wake_safe
             * drives the parked_safe/wake_pending Dekker handshake -- WITH the
             * SEQ_CST StoreLoad fence -- that runloom_park_generic (the waiter in
             * runloom_blocking_call) waits on, and routes the enqueue by
             * g->park_hub: runloom_mn_wake_g for an M:N hub, the owner's wake_list
             * for single-thread.  It is foreign-thread-safe (peeks
             * runloom_tls_sched, never lazily allocs) -- exactly what THIS
             * blockpool worker is.
             *
             * Re-queue BEFORE decrementing inflight so the drain loop, which
             * stays alive while inflight>0, sees the fiber the moment it hits 0. */
            runloom_sched_wake_safe(g);
            if (hub == NULL)
                runloom_netpoll_wake_pump(NULL);   /* single-thread owner -> default pool */
            __atomic_sub_fetch(&bp_inflight, 1, __ATOMIC_ACQ_REL);
        }
    }
    RUNLOOM_THREAD_RETURN((void *)0);
}

/* Resolve the shard count for this run.  RUNLOOM_BLOCKPOOL_SHARDS overrides;
 * otherwise track the live hub count (each hub gets ~its own shard) clamped to
 * [1, RUNLOOM_BP_SHARDS_MAX].  nshard==1 reproduces the legacy single queue. */
static int bp_resolve_nshard(void)
{
    int n;
    const char *e = getenv("RUNLOOM_BLOCKPOOL_SHARDS");
    if (e != NULL && e[0] != '\0') {
        n = atoi(e);
    } else {
        n = runloom_mn_hub_count();     /* 0 on the single-thread sched */
        if (n <= 0) n = 1;
    }
    if (n < 1) n = 1;
    if (n > RUNLOOM_BP_SHARDS_MAX) n = RUNLOOM_BP_SHARDS_MAX;
    return n;
}

/* Resolve the TOTAL worker count.  RUNLOOM_BLOCKPOOL_WORKERS overrides;
 * otherwise nshard * RUNLOOM_BP_WORKERS_PER_SHARD, floored at the legacy default
 * and capped at the hard max.  Always >= nshard so EVERY shard gets >=1 worker
 * (a shard with no worker would deadlock jobs hashed to it). */
static int bp_resolve_workers(int nshard)
{
    int n;
    const char *e = getenv("RUNLOOM_BLOCKPOOL_WORKERS");
    if (e != NULL && e[0] != '\0') {
        n = atoi(e);
        if (n <= 0) n = RUNLOOM_BLOCKPOOL_DEFAULT;
    } else {
        n = nshard * RUNLOOM_BP_WORKERS_PER_SHARD;
        if (n < RUNLOOM_BLOCKPOOL_DEFAULT) n = RUNLOOM_BLOCKPOOL_DEFAULT;
    }
    if (n < nshard) n = nshard;                          /* >=1 worker per shard */
    if (n > RUNLOOM_BLOCKPOOL_MAX) n = RUNLOOM_BLOCKPOOL_MAX;
    return n;
}

int runloom_blockpool_init(int n_workers)
{
    int i, started, nshard;

    /* Fast path: already up, or a prior attempt failed (don't retry). */
    if (__atomic_load_n(&bp_inited, __ATOMIC_ACQUIRE)) return 0;
    if (__atomic_load_n(&bp_failed, __ATOMIC_ACQUIRE)) return -1;

    bp_init_lock_ensure();          /* Windows: make bp_init_lock usable */
    RUNLOOM_RLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
    if (bp_inited) { RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL); return 0; }
    if (bp_failed) { RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL); return -1; }

    nshard = bp_resolve_nshard();
    if (n_workers <= 0) {
        n_workers = bp_resolve_workers(nshard);
    } else {
        if (n_workers < nshard) nshard = n_workers;   /* honour an explicit small pool */
        if (n_workers > RUNLOOM_BLOCKPOOL_MAX) n_workers = RUNLOOM_BLOCKPOOL_MAX;
    }

    /* Initialise each shard's lock + cond.  Workers aren't running yet, so no
     * shard lock is contended here. */
    bp_stopping = 0;
    for (i = 0; i < nshard; i++) {
        runloom_mutex_init(&bp_shards[i].lock);
        if (runloom_cond_init(&bp_shards[i].cond) != 0) {
            /* tear down the shards initialised so far */
            int j;
            for (j = 0; j < i; j++) runloom_cond_destroy(&bp_shards[j].cond);
            __atomic_store_n(&bp_failed, 1, __ATOMIC_RELEASE);
            RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
            return -1;
        }
        bp_shards[i].head = bp_shards[i].tail = NULL;
    }
    bp_nshard = nshard;

    /* Arm the pump interrupt so single-thread-scheduler offloads can wake an
     * idle pump.  Best-effort: if the backend has no such primitive (non-epoll),
     * single-thread callers fall back to inline below. */
    bp_wake_armed = (runloom_netpoll_wake_pump_arm() == 0);

    /* Spawn workers, distributing them round-robin across shards (worker i ->
     * shard i % nshard) so shards 0..nshard-1 each get >=1 worker. */
    started = 0;
    for (i = 0; i < n_workers; i++) {
        int sidx = i % nshard;
        bp_worker_shard[i] = sidx;
        if (runloom_thread_create(&bp_threads[i], runloom_blockpool_worker,
                               (void *)(intptr_t)sidx) != 0) {
            break;
        }
        started++;
    }
    /* Need at least one worker per shard, else jobs hashed to an uncovered shard
     * would never run.  If we couldn't even cover every shard, fail (inline). */
    if (started < nshard) {
        bp_stopping = 1;
        for (i = 0; i < nshard; i++) runloom_cond_broadcast(&bp_shards[i].cond);
        for (i = 0; i < started; i++) runloom_thread_join(bp_threads[i]);
        for (i = 0; i < nshard; i++) runloom_cond_destroy(&bp_shards[i].cond);
        __atomic_store_n(&bp_failed, 1, __ATOMIC_RELEASE);
        RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
        return -1;
    }
    bp_n_workers = started;
    __atomic_store_n(&bp_inited, 1, __ATOMIC_RELEASE);
    RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
    return 0;
}

void runloom_blockpool_fini(void)
{
    int i, n, nshard;
    bp_init_lock_ensure();          /* Windows: make bp_init_lock usable */
    RUNLOOM_RLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
    if (!bp_inited) { RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL); return; }
    bp_stopping = 1;
    n = bp_n_workers;
    nshard = bp_nshard;
    /* Wake every shard's workers so they observe bp_stopping and exit. */
    for (i = 0; i < nshard; i++) runloom_cond_broadcast(&bp_shards[i].cond);
    RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);

    for (i = 0; i < n; i++) runloom_thread_join(bp_threads[i]);

    RUNLOOM_RLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
    for (i = 0; i < nshard; i++) {
        runloom_cond_destroy(&bp_shards[i].cond);
        bp_shards[i].head = bp_shards[i].tail = NULL;
    }
    bp_inited = 0;
    bp_n_workers = 0;
    bp_stopping = 0;
    bp_nshard = 1;
    RUNLOOM_RUNLOCK(&bp_init_lock, RUNLOOM_RANK_BLOCKPOOL);
}

/* Reset the blocking-offload pool in a forked CHILD.  The worker OS threads are
 * gone, so we must NOT join them (runloom_blockpool_fini would hang) -- we reset
 * to "not started" so the next offload re-creates the pool fresh.  The child is
 * single-threaded here: re-init the init lock (a dead worker may have held it at
 * fork), drop the inherited job queues (their jobs point at parent fibers), and
 * zero the counters.  Each shard's cond is (re)created by runloom_blockpool_init
 * on the next offload; the inherited ones are abandoned (no destroy -- they may
 * be in an invalid post-fork state, and destroying an invalid cond is itself
 * UB). */
void runloom_blockpool_reset_after_fork(void)
{
    int i;
    runloom_mutex_init(&bp_init_lock);
#if defined(RUNLOOM_OS_WINDOWS)
    bp_init_lock_state = 2;
#endif
    for (i = 0; i < RUNLOOM_BP_SHARDS_MAX; i++) {
        bp_shards[i].head = bp_shards[i].tail = NULL;
    }
    bp_nshard = 1;
    bp_inited = 0;
    bp_failed = 0;
    bp_stopping = 0;
    bp_n_workers = 0;
    bp_wake_armed = 0;
    __atomic_store_n(&bp_inflight, 0, __ATOMIC_RELAXED);
}

void *runloom_blocking_call(void *(*fn)(void *), void *arg)
{
    void *hub = runloom_mn_current_hub_opaque();
    runloom_g_t *g;
    runloom_block_job_t job;
    runloom_bp_shard_t *sh;

    if (hub != NULL) {
        g = runloom_mn_tls_current_g();
    } else {
        /* PEEK the current fiber -- never runloom_sched_get(), which lazily
         * allocates a sched + tstate machinery.  Off a fiber (a top-level
         * blocking() call, or one made AFTER an M:N run() has torn down), there
         * is no sched on this thread: peek returns NULL and we run fn inline. */
        g = runloom_sched_peek_current();
    }
    /* Must be inside a fiber to park.  Also fall back to inline when the pool
     * can't start, or -- for the single-thread sched only -- when the pump
     * interrupt isn't available (no way to wake an idle pump on this backend
     * yet).  Hubs busy-poll, so they never need it. */
    if (g == NULL || runloom_blockpool_init(0) != 0 ||
        (hub == NULL && !bp_wake_armed)) {
        return fn(arg);
    }

    job.fn     = fn;
    job.arg    = arg;
    job.result = NULL;
    job.g      = g;
    job.hub    = hub;
    job.done   = RUNLOOM_BP_JOB_PENDING;
    job.next   = NULL;

    /* Count the job as outstanding BEFORE enqueueing so the single-thread drain
     * loop never observes a transient "no work" between our enqueue and the
     * worker re-queueing us, which would exit the loop early. */
    __atomic_add_fetch(&bp_inflight, 1, __ATOMIC_ACQ_REL);

    /* Enqueue on THIS hub's shard -- not a single global lock -- so concurrent
     * offloads from other hubs never serialize against us (the scaling fix). */
    sh = &bp_shards[bp_shard_for(hub, g)];
    RUNLOOM_RLOCK(&sh->lock, RUNLOOM_RANK_BLOCKPOOL);
    if (sh->tail != NULL) sh->tail->next = &job;
    else                  sh->head = &job;
    sh->tail = &job;
    runloom_cond_signal(&sh->cond);
    RUNLOOM_RUNLOCK(&sh->lock, RUNLOOM_RANK_BLOCKPOOL);

    /* Park until the WORKER signals completion (job.done).  Re-park on any other
     * wake: a task.cancel() delivers G.wake() to this fiber while it is parked
     * here, and returning then would free the stack `job` while the worker still
     * references it (use-after-free SIGSEGV).  The worker always runs the job and
     * wakes us, so the loop always terminates; cancellation is delivered at the
     * next real await-point after we return.  Hub: race-safe park_generic.
     * Single-thread: race-safe park_safe/wake_safe. */
    if (hub != NULL) {
        while (!__atomic_load_n(&job.done, __ATOMIC_ACQUIRE)) {
            runloom_park_generic(1);
        }
    } else {
        while (!__atomic_load_n(&job.done, __ATOMIC_ACQUIRE)) {
            runloom_sched_park_safe();
        }
    }

    return job.result;
}
