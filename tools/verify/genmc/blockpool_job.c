/*
 * blockpool_job.c -- GenMC model of the blocking-offload JOB LIFETIME seam in
 * src/runloom_c/runloom_blockpool.c, in REAL C (pthreads + C11 atomics) under RC11.
 * The Spin model (blockpool.pml) proves the wake-ordering (re-queue before dec
 * inflight -> no lost wake); THIS proves the orthogonal CROSS-THREAD LIFETIME
 * property the source flags as a use-after-free in its own comments
 * (runloom_blockpool.c ~129-145): the job record lives on the PARKED FIBER's coroutine
 * stack -- it is freed the instant the fiber returns from the blocking call -- yet a
 * worker OS thread is concurrently touching it.  (LIFECYCLE_INVARIANTS.md Tier-1 #3.)
 *
 * THE PROTOCOL.  runloom_blocking_call puts the job on its own stack, parks, and a
 * worker runs it.  The fiber can be resumed EARLY by a spurious wake (task.cancel ->
 * G.wake) while the worker is still running -- which is exactly the UAF window.  Two
 * rules close it, both modelled:
 *   WORKER: snapshot the wake target (g, hub) into LOCALS, compute job->result, then
 *           release-store job->done = 1, and AFTER that touch NOTHING in job-> (it
 *           wakes via the local snapshot, not job->) -- because once done is visible
 *           the fiber may have already freed the stack job.
 *   FIBER:  on ANY wake (worker's or spurious), LOOP until job->done is acquire-1
 *           BEFORE reading job->result and returning (freeing the stack job).
 * So `done` is the handshake: the worker's last job-> access happens-before done
 * (release); the fiber's free happens-after observing done (acquire) -> no access to
 * job after it is freed, and the fiber reads the worker's result.
 *
 * PROVES (worker + parked fiber + a spurious waker, RC11):
 *   NO UAF       -- the worker never touches job-> after the fiber frees it.
 *   RESULT-SEEN  -- the fiber reads the worker's computed result, never the unset one.
 *
 * Negative controls (must FAIL = GenMC finds the UAF / stale read):
 *   -DBUG_FIBER_NO_DONE_WAIT  : the fiber frees the stack job on the (spurious) wake
 *                               WITHOUT waiting for done -> the worker, still mid-job,
 *                               touches a freed job.
 *   -DBUG_WORKER_LATE_READ    : the worker reads job->hub AFTER release-storing done
 *                               (instead of the pre-done snapshot) -> it touches a job
 *                               the fiber may already have freed.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define COMPUTED 7
#define UNSET    0

/* The job record -- conceptually ON THE FIBER'S STACK.  job_alive models the stack
 * frame's validity: 1 while the blocking call is in scope, 0 once the fiber returns
 * (frees it).  Every worker access to a job-> field asserts job_alive first. */
static int        job_result;       /* job->result  (written by worker, read by fiber) */
static int        job_hub;          /* job->hub      (the wake target) */
static atomic_int job_done;         /* job->done     (release/acquire handshake) */
static atomic_int job_alive;        /* 1 = stack frame valid, 0 = freed by the fiber */
/* Two SINGLE-WRITER wake flags: the legitimate worker wake and the spurious early
 * wake (task.cancel -> G.wake).  The fiber proceeds on either.  (One multi-writer
 * flag is an unordered write-write race that defeats GenMC's spin->assume.) */
static atomic_int woken_w;          /* set by the worker (after done) */
static atomic_int woken_s;          /* set by the spurious early waker */

#define TOUCH(field_read) do {                                   \
        assert(atomic_load_explicit(&job_alive,                  \
               memory_order_acquire) == 1); /* no UAF */         \
        (void)(field_read);                                      \
    } while (0)

/* The worker OS thread: runloom_blockpool_worker. */
static void *worker(void *_)
{
    (void)_;
    /* snapshot the wake target BEFORE publishing done (job is surely alive here) */
    TOUCH(0);
    int local_hub = job_hub;

    /* run the job: write job->result (still before done) */
    TOUCH(0);
    job_result = COMPUTED;

    /* publish completion: release so the resumed fiber sees job->result + done */
    atomic_store_explicit(&job_done, 1, memory_order_release);

#ifdef BUG_WORKER_LATE_READ
    /* BUG: wake via job->hub read AFTER done -> the fiber may have freed the job. */
    TOUCH(0);
    local_hub = job_hub;
#endif

    /* wake the fiber using the LOCAL snapshot, never job-> */
    (void)local_hub;
    atomic_store_explicit(&woken_w, 1, memory_order_release);
    return 0;
}

/* A spurious early wake: task.cancel() -> G.wake() can resume the parked fiber
 * before the worker has finished (the UAF window). */
static void *spurious(void *_)
{
    (void)_;
    atomic_store_explicit(&woken_s, 1, memory_order_release);
    return 0;
}

/* The parked fiber: runloom_blocking_call's park + resume + return. */
static void *fiber(void *_)
{
    (void)_;
    /* parked: GenMC turns the wake spin into an assume -- proceed once woken by
     * the worker OR a spurious early wake. */
    while (atomic_load_explicit(&woken_w, memory_order_acquire) == 0 &&
           atomic_load_explicit(&woken_s, memory_order_acquire) == 0) { }

#ifndef BUG_FIBER_NO_DONE_WAIT
    /* A wake may be spurious: wait for the worker's done before touching job->. */
    while (atomic_load_explicit(&job_done, memory_order_acquire) == 0) { }
#endif

    int r = job_result;                       /* read job->result */
    atomic_store_explicit(&job_alive, 0, memory_order_release);   /* FREE the stack job */

    assert(r == COMPUTED);                    /* RESULT-SEEN: the worker's value */
    return 0;
}

int main(void)
{
    pthread_t tw, ts, tf;
    job_result = UNSET;
    job_hub = 1;
    atomic_init(&job_done, 0);
    atomic_init(&job_alive, 1);
    atomic_init(&woken_w, 0);
    atomic_init(&woken_s, 0);

    pthread_create(&tw, 0, worker, 0);
    pthread_create(&ts, 0, spurious, 0);
    pthread_create(&tf, 0, fiber, 0);
    pthread_join(tw, 0);
    pthread_join(ts, 0);
    pthread_join(tf, 0);
    return 0;
}
