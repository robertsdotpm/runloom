/*
 * netpoll_claim.c -- GenMC model of the netpoll commit-claim race in
 * src/runloom_c/netpoll.c, in REAL C (pthreads + C11 atomics), verified under
 * GenMC's RC11 weak memory model.  Where the Spin model (netpoll_deadline.pml)
 * proves the algorithm has no bad SC interleaving, GenMC explores every RC11
 * execution of the actual atomic ops + the pool->lock mutex -- so it also
 * catches a misplaced fence / a missing lock (a data race), not just a bad
 * interleaving.
 *
 * THE PROTOCOL.  A goroutine parks: it CASes commit ARMED->PARKED (acq_rel).
 * Concurrently up to two claimers (runloom_pump_dispatch_event with R_MASK and
 * runloom_netpoll_cancel_g with R_CANCEL -- the timeout sweep is identical)
 * each, under pool->lock: claim commit ->WOKEN (the loser sees WOKEN and does
 * nothing), publish *ready_out, and -- if they claimed a PARKED g -- re-queue
 * it (woken=1).  The parking g, on a committed park, resumes only after being
 * re-queued and re-takes pool->lock before reading ready_out; on a failed CAS
 * (it saw WOKEN) it aborts, taking pool->lock once to order its read after the
 * winner's publish.
 *
 * PROVES (1 parker, 2 distinct-value claimers, RC11):
 *   - NO DATA RACE on ready_out (every access is ordered by pool->lock).
 *   - VALUE CORRECTNESS: the g reads exactly the winning claimer's value,
 *     never R_UNSET, never the other claimer's value.
 *   - EXACTLY ONCE: at most one claimer re-queues the parked g (resumes <= 1).
 *
 * Negative control -DBUG_NO_LOCK: the aborting g reads ready_out relying ONLY
 * on its acquire-load of commit seeing WOKEN, WITHOUT the pool->lock round-trip
 * (and the claimer publishes ready_out as a plain store after the release-CAS).
 * GenMC then finds the data race / stale read on ready_out -- the same gap the
 * commit_cas_then_publish.litmus test isolates, here on the real protocol.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define ARMED  0
#define PARKED 1
#define WOKEN  2

#define R_UNSET  0
#define R_MASK   1   /* fd-ready claimer's value   */
#define R_CANCEL 3   /* cancel claimer's value     */

#define NONE   0
#define FD     1
#define CANCEL 3

static atomic_int commit;           /* park->commit                          */
static atomic_int woken;            /* claimer re-queued the parked g         */
static int        ready_out;        /* *p->ready_out (guarded by pool_lock)   */
static int        winner;           /* which claimer won (guarded)            */
static atomic_int resumes;          /* re-queue count (exactly-once)          */
static pthread_mutex_t pool_lock;

#define VALUE_OK(r, w) \
    (((w) == FD && (r) == R_MASK) || ((w) == CANCEL && (r) == R_CANCEL))

/* A claimer: runloom_pump_claim under pool->lock, publish, re-queue if PARKED. */
static void claim(int who, int val)
{
    pthread_mutex_lock(&pool_lock);
    int prior = atomic_load_explicit(&commit, memory_order_acquire);
    int won = 0;
    if (prior != WOKEN) {
        /* CAS prior -> WOKEN; on failure another claimer won (prior updated) */
        won = atomic_compare_exchange_strong_explicit(
                  &commit, &prior, WOKEN,
                  memory_order_acq_rel, memory_order_acquire);
    }
    if (won) {
        ready_out = val;            /* publish under the lock */
        winner = who;
        if (prior == PARKED) {
            int n = atomic_fetch_add_explicit(&resumes, 1, memory_order_relaxed);
            assert(n == 0);         /* EXACTLY ONCE: at most one re-queue */
            atomic_store_explicit(&woken, 1, memory_order_release);
        }
    }
    pthread_mutex_unlock(&pool_lock);
}

static void *fd_claimer(void *_)     { (void)_; claim(FD, R_MASK);     return 0; }
static void *cancel_claimer(void *_) { (void)_; claim(CANCEL, R_CANCEL); return 0; }

/* The parking goroutine: runloom_netpoll_wait_fd's commit + abort/resume. */
static void *parker(void *_)
{
    (void)_;
    int expect = ARMED;
    int committed = atomic_compare_exchange_strong_explicit(
                        &commit, &expect, PARKED,
                        memory_order_acq_rel, memory_order_acquire);
    int r, w;
    if (committed) {
        /* Parked: resume only once a claimer re-queues us.  GenMC transforms
         * this spin loop into an assume (the scheduler delivers the resume). */
        while (atomic_load_explicit(&woken, memory_order_acquire) == 0) { }
        pthread_mutex_lock(&pool_lock);     /* resume re-observes pool->lock */
        r = ready_out;
        w = winner;
        pthread_mutex_unlock(&pool_lock);
    } else {
        /* expect == WOKEN: a claimer took our ARMED parker.  Abort. */
#ifndef BUG_NO_LOCK
        pthread_mutex_lock(&pool_lock);     /* order our read after the publish */
        pthread_mutex_unlock(&pool_lock);
        r = ready_out;
        w = winner;
#else
        /* BUG: trust the commit acquire alone, read without the lock round-trip */
        r = ready_out;
        w = winner;
#endif
    }
    assert(r != R_UNSET);          /* a claimer delivered a value */
    assert(VALUE_OK(r, w));        /* and it is the winner's value */
    return 0;
}

int main(void)
{
    pthread_t tp, tf, tc;
    pthread_mutex_init(&pool_lock, 0);
    atomic_init(&commit, ARMED);
    atomic_init(&woken, 0);
    atomic_init(&resumes, 0);
    ready_out = R_UNSET;
    winner = NONE;

    pthread_create(&tp, 0, parker, 0);
    pthread_create(&tf, 0, fd_claimer, 0);
    pthread_create(&tc, 0, cancel_claimer, 0);
    pthread_join(tp, 0);
    pthread_join(tf, 0);
    pthread_join(tc, 0);
    return 0;
}
