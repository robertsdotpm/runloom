/*
 * iouring_waitcommit.c -- GenMC oracle for runloom's io_uring SINGLE-op park/wake
 * commit handshake (src/runloom_c/io_uring.c: the `op->wait` word shared by
 * runloom_iouring_do's hub park path and runloom_iouring_drain's SINGLE-op case; the
 * recv/send per-hub-ring path runloom_iouring_ring_do / runloom_iouring_ring_drain is
 * byte-identical), in REAL C (pthreads + C11 atomics) under GenMC's RC11
 * weak-memory model.
 *
 * FAITHFUL SLICE (not byte-shared).  io_uring.c can't be compiled under GenMC
 * (Python.h, raw io_uring syscalls, the coroutine switch).  This reproduces the
 * EXACT atomic sequence + memory orders of the race-critical core; the
 * drift-guard in run_genmc.sh fails if io_uring.c's orderings change without
 * this being re-synced.
 *
 * THE RACE.  A hub goroutine submits a SINGLE op and PARKS (coro_yield) instead
 * of blocking the OS thread; a CONCURRENT drainer (the idle-hub netpoll pump
 * that drains the shared ring) completes the op and must wake the parker.  The
 * op's CQE is claimed by exactly one drainer upstream (the cq_head CAS in
 * runloom_iouring_drain), so this is ONE submitter vs ONE drainer.  Without a
 * handshake the inline drain -- which runs on the not-yet-parked submitter --
 * would wake a still-running g (Bug 1); and a wake racing the park must not be
 * lost (permanent hang) nor delivered to a g that didn't park (double-resume).
 * The `op->wait` word (INFLIGHT/PARKED/DONE) is the single commit point.
 *
 * SPEC -- four properties, asserted in EVERY RC11 execution:
 *   (P1) NO LOST WAKE.  If the submitter committed to parking, the drainer woke
 *        it:  forbidden end state  parked && woke == 0.
 *   (P2) NO WAKE WITHOUT PARK.  A wake is delivered only to a parker:
 *        forbidden  woke > 0 && parked == 0  (would re-resume a running g).
 *        (P1 & P2 together: park decision <=> wake decision -- they agree.)
 *   (P3) AT MOST ONE WAKE.  The exchange is the single claim:  woke <= 1.
 *   (P4) RESULT VISIBILITY.  Whenever the submitter reads op->result on resume
 *        (woken) or after observing DONE (didn't park), it sees the value the
 *        drainer stored -- never the sentinel.
 *
 * WHY IT'S CORRECT.  The submitter's CAS (INFLIGHT->PARKED) and the drainer's
 * exchange (*->DONE) are RMWs on the SAME location `wait`, so they are totally
 * ordered by its modification order (RMW atomicity, independent of memory
 * order): whichever wins, the loser observes it.  CAS-wins => exchange reads
 * PARKED => wake (P1); exchange-wins => CAS reads DONE => no park (P2).  That is
 * why P1/P2/P3 are robust even under relaxed `wait` ops -- the ROUTING rests on
 * RMW atomicity, not on fences.  The memory ORDERS only carry op->result: the
 * RELEASE result-store + the ACQ_REL exchange / RELEASE wake publish it, and the
 * submitter's ACQUIRE loads consume it (P4).
 *
 * MEMORY ORDERS pinned to io_uring.c:
 *   drainer:    op->result  store RELEASE
 *               op->wait    exchange ACQ_REL   ( *->DONE )
 *               on prev==PARKED: wake (modelled RELEASE)
 *   submitter:  op->wait    load ACQUIRE       (skip-if-DONE)
 *               op->wait    CAS ACQ_REL/ACQUIRE ( INFLIGHT->PARKED )
 *               op->result  load ACQUIRE       (on resume / after DONE)
 *
 * Negative controls (each MUST make an assertion fire):
 *   -DBUG_PARK_PLAIN_STORE : submitter blind-stores wait=PARKED instead of the
 *        CAS -> can overwrite a DONE the drainer just set, parking with no
 *        waker (P1 lost wake).
 *   -DBUG_EXCHANGE_RELAXED : drainer's exchange is relaxed -> observing DONE no
 *        longer publishes the result store (P4 stale read on the no-park path).
 *   -DBUG_WOKE_RELAXED     : the wake store is relaxed -> a resumed parker's
 *        result read isn't ordered after the result store (P4 stale, park path).
 *   -DBUG_LOAD_RELAXED     : submitter's loads are relaxed -> no acquire, so no
 *        result publication is consumed (P4 stale).
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

#define INFLIGHT 0
#define PARKED   1
#define DONE     2
#define SENTINEL (-1)
#define VALUE    7

static atomic_int wait;        /* op->wait  : INFLIGHT/PARKED/DONE */
static atomic_int result;      /* op->result: SENTINEL until completed */
static atomic_int woke;        /* # wakes the drainer issued (mn_wake_g) */
static atomic_int parked;      /* submitter committed to coro_yield */
static atomic_int bad_result;  /* submitter read op->result == SENTINEL (stale) */

/* ---- concurrent drainer: runloom_iouring_drain SINGLE-op case ---- */
static void *drainer(void *arg)
{
    (void)arg;
    /* src: __atomic_store_n(&op->result, res, __ATOMIC_RELEASE); */
    atomic_store_explicit(&result, VALUE,
#ifdef BUG_RESULT_RELAXED
        memory_order_relaxed);
#else
        memory_order_release);
#endif
    /* src: prev = __atomic_exchange_n(&op->wait, WAIT_DONE, __ATOMIC_ACQ_REL); */
    int prev = atomic_exchange_explicit(&wait, DONE,
#ifdef BUG_EXCHANGE_RELAXED
        memory_order_relaxed);
#else
        memory_order_acq_rel);
#endif
    /* src: if (prev == WAIT_PARKED) runloom_mn_wake_g(op->hub, op->g); */
    if (prev == PARKED) {
        atomic_store_explicit(&woke, 1,
#ifdef BUG_WOKE_RELAXED
            memory_order_relaxed);
#else
            memory_order_release);
#endif
    }
    return 0;
}

/* ---- submitter: runloom_iouring_do hub park path (inline drain already ran with
 * wait==INFLIGHT, i.e. our op was not completed inline -- the interesting race
 * is the CONCURRENT drainer completing it while we decide to park). ---- */
static void *submitter(void *arg)
{
    (void)arg;
    int did_park = 0;
    /* src: if (__atomic_load_n(&op.wait, ACQUIRE) != WAIT_DONE) */
    int w = atomic_load_explicit(&wait,
#ifdef BUG_LOAD_RELAXED
        memory_order_relaxed);
#else
        memory_order_acquire);
#endif
    if (w != DONE) {
#ifdef BUG_PARK_PLAIN_STORE
        /* WRONG: a blind store can clobber a DONE the drainer set between the
         * load above and here -> we park with no waker. */
        atomic_store_explicit(&wait, PARKED, memory_order_release);
        did_park = 1;
#else
        /* src: prev=INFLIGHT; CAS(&op.wait,&prev,WAIT_PARKED,ACQ_REL,ACQUIRE) */
        int prev = INFLIGHT;
        if (atomic_compare_exchange_strong_explicit(&wait, &prev, PARKED,
                memory_order_acq_rel,
#ifdef BUG_LOAD_RELAXED
                memory_order_relaxed)) {
#else
                memory_order_acquire)) {
#endif
            did_park = 1;   /* committed: runloom_sched_park_current(); coro_yield */
        }
        /* else prev==DONE: a drainer completed us; do NOT park. */
#endif
    }

    if (did_park) {
        atomic_store_explicit(&parked, 1, memory_order_release);
        /* Real code: coro_yield; the g resumes ONLY after the drainer's wake
         * (mn_wake_g -> hub_submit -> hub_main resume).  Model the resumed
         * result read as ordered after observing the wake; if not yet woken in
         * this interleaving the g is still parked and reads nothing. */
        if (atomic_load_explicit(&woke,
#ifdef BUG_LOAD_RELAXED
                memory_order_relaxed) > 0) {
#else
                memory_order_acquire) > 0) {
#endif
            int r = atomic_load_explicit(&result,
#ifdef BUG_LOAD_RELAXED
                memory_order_relaxed);
#else
                memory_order_acquire);
#endif
            if (r != VALUE) atomic_store_explicit(&bad_result, 1,
                                                  memory_order_relaxed);
        }
    } else {
        /* Did NOT park: we observed wait==DONE (first load, or CAS-fail), an
         * ACQUIRE that synchronizes with the drainer's release exchange, so
         * op->result must be visible. */
        int r = atomic_load_explicit(&result,
#ifdef BUG_LOAD_RELAXED
            memory_order_relaxed);
#else
            memory_order_acquire);
#endif
        if (r != VALUE) atomic_store_explicit(&bad_result, 1,
                                              memory_order_relaxed);
    }
    return 0;
}

int main(void)
{
    atomic_init(&wait, INFLIGHT);
    atomic_init(&result, SENTINEL);
    atomic_init(&woke, 0);
    atomic_init(&parked, 0);
    atomic_init(&bad_result, 0);

    pthread_t s, d;
    pthread_create(&s, 0, submitter, 0);
    pthread_create(&d, 0, drainer, 0);
    pthread_join(s, 0);
    pthread_join(d, 0);

    int p  = atomic_load(&parked);
    int wk = atomic_load(&woke);
    /* (P1) no lost wake -- a parked submitter must have been woken. */
    assert(!(p == 1 && wk == 0));
    /* (P2) no wake without park -- a wake targets only a parker. */
    assert(!(wk > 0 && p == 0));
    /* (P3) at most one wake. */
    assert(wk <= 1);
    /* (P4) result visibility -- the submitter never read the sentinel. */
    assert(atomic_load(&bad_result) == 0);
    return 0;
}
