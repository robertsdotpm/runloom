/*
 * sched_parkwake.c -- GenMC oracle for pygo's park_safe/wake_safe handshake
 * (pygo_sched.c: pygo_sched_park_safe / pygo_sched_wake_safe), in REAL C
 * (pthreads + C11 atomics) under GenMC's RC11 weak-memory model.
 *
 * FAITHFUL SLICE (not byte-shared).  pygo_sched.c cannot be compiled wholesale
 * under GenMC -- it pulls in Python.h and the functions call pygo_coro_yield /
 * pygo_pystate_snap / pygo_sched_get.  This harness reproduces the EXACT atomic
 * sequence and memory orders of the race-critical core of the two functions;
 * the structural drift-guard (verify/genmc/run_chase_lev.sh's sibling check, see
 * run_genmc.sh) fails if the source's orderings change without this being
 * re-synced.  The complementary iRC11 PROOF of the wake_list release/acquire
 * core is verify/iris/rc11/WakeListHandoff.v.
 *
 * THE RACE.  A goroutine g parks via park_safe on its OWNER thread while a
 * FOREIGN thread (an iouring CQE / executor-pool worker resolving a future the
 * owner awaits) calls wake_safe(g).  The handoff is coordinated by parked_safe
 * (a CAS flag) + wake_pending (a counter) + the cross-thread wake_list.  A lost
 * wake here is a permanent hang -- historically the single richest bug seam in
 * the scheduler.
 *
 * SPEC -- the two things that matter:
 *   (a) NO LOST WAKE.  A wake_safe definitely happened (the waker thread always
 *       runs).  So g must end up runnable: EITHER the parker observed the wake
 *       and did NOT yield (it consumed wake_pending and stays running), OR g was
 *       enqueued on the wake_list (the owner's drain re-runs it).  The forbidden
 *       state is  yielded && enqueued == 0  -- g parked with nothing to wake it.
 *   (b) ENQUEUED AT MOST ONCE.  The parked_safe 1->0 CAS is exclusive, so even
 *       with N racing wakers at most one routes g to the wake_list (no double
 *       run).  enqueued <= 1 in every execution.
 * Asserted in EVERY RC11 execution GenMC explores.
 *
 * MEMORY ORDERS pinned to pygo_sched.c (NO order strengthened beyond the two
 * SC fences the protocol itself needs):
 *   park_safe:  wake_pending ACQUIRE-load (early-out);
 *               parked_safe RELEASE-store (commit);
 *               SC FENCE;                                    <-- StoreLoad barrier
 *               wake_pending ACQUIRE-load (recheck);
 *               parked_safe ACQ_REL/ACQUIRE CAS (abort)
 *   wake_safe:  wake_pending ACQ_REL add (FIRST -- so the recheck observes it);
 *               SC FENCE;                                    <-- StoreLoad barrier
 *               parked_safe ACQ_REL/ACQUIRE CAS;
 *               on success: wake_list enqueue (RELEASE publish under the lock)
 *
 * HISTORY: the SC fences were ADDED after this harness found a lost wakeup in
 * the fence-free release/acquire version -- a Dekker/StoreLoad reorder where the
 * parker's recheck and the waker's CAS each read a stale value on the OTHER
 * location and BOTH miss each other (g parks, never enqueued, permanent hang).
 * SC model-checking (spin/parked_safe.pml) cannot see it; GenMC (RC11) does.
 * -DBUG_NO_SC_FENCE reproduces the bug.
 *
 * Negative controls (must FIND the lost wake = assert fails):
 *   -DBUG_NO_SC_FENCE: drop both StoreLoad fences -> the original lost wakeup.
 *   -DBUG_NO_RECHECK : park_safe omits the post-store wake_pending recheck. A
 *                      wake whose bump landed but whose CAS failed (g not yet
 *                      parked) is then lost -- the parker yields forever.
 *   -DBUG_NO_BUMP    : wake_safe omits the wake_pending bump. A wake racing a
 *                      not-yet-parked g neither aborts the park (recheck sees 0)
 *                      nor enqueues (CAS finds parked_safe==0) -- lost.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

static atomic_int parked_safe;
static atomic_int wake_pending;
static atomic_int enqueued;    /* # of wakers that routed g to the wake_list */
static atomic_int yielded;     /* parker committed to pygo_coro_yield (parked) */

/* ---- foreign thread: pygo_sched_wake_safe(g) ---- */
static void *waker(void *arg)
{
    (void)arg;
#ifndef BUG_NO_BUMP
    /* src: __atomic_add_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL) -- FIRST,
     *      so the parker's post-store recheck observes our arrival. */
    atomic_fetch_add_explicit(&wake_pending, 1, memory_order_acq_rel);
#endif
#ifndef BUG_NO_SC_FENCE
    /* src: __atomic_thread_fence(__ATOMIC_SEQ_CST) -- StoreLoad barrier so the
     * bump (above) is ordered before the CAS-load of parked_safe (below), which
     * is on a DIFFERENT location.  WITHOUT it, GenMC finds the lost wake. */
    atomic_thread_fence(memory_order_seq_cst);
#endif
    /* src: CAS parked_safe 1->0, ACQ_REL success / ACQUIRE failure. */
    int expected = 1;
    if (atomic_compare_exchange_strong_explicit(&parked_safe, &expected, 0,
            memory_order_acq_rel, memory_order_acquire)) {
        /* src: route g to its owner's wake_list under wake_list_lock; the
         *      mutex unlock is the RELEASE that publishes the handoff. */
        atomic_fetch_add_explicit(&enqueued, 1, memory_order_release);
    }
    return 0;
}

/* ---- owner thread: pygo_sched_park_safe() (race-critical core) ---- */
static void *parker(void *arg)
{
    (void)arg;
    /* src: early-out if a wake is already pending (future fired synchronously) */
    if (atomic_load_explicit(&wake_pending, memory_order_acquire) > 0) {
        atomic_fetch_sub_explicit(&wake_pending, 1, memory_order_acq_rel);
        return 0;                                  /* did NOT park */
    }
    /* src: commit to parking -- parked_safe = 1, RELEASE. */
    atomic_store_explicit(&parked_safe, 1, memory_order_release);

#ifndef BUG_NO_SC_FENCE
    /* src: __atomic_thread_fence(__ATOMIC_SEQ_CST) -- StoreLoad barrier between
     * the parked_safe store (above) and the wake_pending recheck (below), on
     * DIFFERENT locations.  Release/acquire does NOT order store-then-load;
     * without this fence the recheck reads a stale wake_pending==0 while the
     * waker's CAS reads a stale parked_safe==0 -> lost wake. */
    atomic_thread_fence(memory_order_seq_cst);
#endif
#ifndef BUG_NO_RECHECK
    /* src: recheck wake_pending (ACQUIRE) after the store -- closes the race
     *      where a waker bumped before the store but its CAS failed. */
    if (atomic_load_explicit(&wake_pending, memory_order_acquire) > 0) {
        int expected = 1;
        if (atomic_compare_exchange_strong_explicit(&parked_safe, &expected, 0,
                memory_order_acq_rel, memory_order_acquire)) {
            atomic_fetch_sub_explicit(&wake_pending, 1, memory_order_acq_rel);
            return 0;                              /* aborted park, did NOT yield */
        }
        /* Lost the CAS: a waker already claimed us and enqueued g.  Fall
         * through to yield; the owner's drain dequeues g from the wake_list. */
    }
#endif
    /* src: pygo_coro_yield() -- g is now parked; re-run relies on the wake_list. */
    atomic_store_explicit(&yielded, 1, memory_order_release);
    return 0;
}

int main(void)
{
    atomic_init(&parked_safe, 0);
    atomic_init(&wake_pending, 0);
    atomic_init(&enqueued, 0);
    atomic_init(&yielded, 0);

    pthread_t p, w1, w2;
    pthread_create(&p,  0, parker, 0);
    pthread_create(&w1, 0, waker, 0);   /* two foreign wakers race the parker */
    pthread_create(&w2, 0, waker, 0);
    pthread_join(p, 0);
    pthread_join(w1, 0);
    pthread_join(w2, 0);

    int enq = atomic_load(&enqueued);
    /* (a) NO LOST WAKE: a parked g must have been routed to the wake_list. */
    assert(!(atomic_load(&yielded) && enq == 0));
    /* (b) ENQUEUED AT MOST ONCE: the parked_safe CAS is exclusive. */
    assert(enq <= 1);
    return 0;
}
