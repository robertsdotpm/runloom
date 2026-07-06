/*
 * sched_parkwake_seam.c -- GenMC oracle for the SEAM between runloom's TWO
 * park/wake protocols, exercised only in the migratable global-runq modes
 * (RUNLOOM_PER_G_TSTATE / RUNLOOM_STEAL_WOKEN).  In REAL C (pthreads + C11
 * atomics) under GenMC's RC11 weak-memory model.
 *
 * WHY A SEAM.  A migratable fiber commits its park through BOTH protocols:
 *   (A) the Dekker handshake in runloom_park_generic (park_generic, before the
 *       coro_yield): parked_safe RELEASE-store + SC fence + wake_pending recheck.
 *       Proven in isolation by sched_parkwake.c.
 *   (B) the single-location wake_state machine, committed by hub_main AFTER the
 *       yield: CAS RUNNING->PARKED, with a RUNNING_WOKEN fallback that
 *       re-enqueues (mn_sched_hub_main.c.inc:698-712).  "Structurally immune" in
 *       isolation (one CAS location).
 * Wakes for a migratable fiber route via wake_g's wake_state CAS
 * (mn_sched_mn_api.c.inc:180-216): RUNNING->RUNNING_WOKEN (owner re-enqueues at
 * release) or PARKED->QUEUED (enqueue now).  Each protocol is individually
 * verified; their COMPOSITION on one fiber -- a park that runs the Dekker
 * commit, then the wake_state commit, racing a wake_g -- was not.  This harness
 * model-checks that composition before STEAL_WOKEN is promoted toward default.
 *
 * FAITHFUL SLICE (not byte-shared), same discipline as sched_parkwake.c: the
 * exact atomic sequence + memory orders of the race-critical core, each step
 * annotated with its source site.
 *
 * SPEC -- the two seam failure modes:
 *   (a) NO LOST WAKE.  wake_g definitely ran.  A fully-parked fiber (yielded,
 *       and its wake_state commit completed) must end ENQUEUED exactly so a hub
 *       re-runs it.  Forbidden: parked && enqueued == 0.
 *   (b) ENQUEUED AT MOST ONCE across BOTH destinations (the runq and -- under
 *       -DSEAM_MIX_DEKKER -- the wake_list).  A double enqueue is a double
 *       resume: the fiber's coro advances twice -> stack corruption.
 *
 * CONTROLS:
 *   -DSEAM_MIX_DEKKER : add a concurrent Dekker wake_safe(g) on the SAME fiber
 *                       (parked_safe CAS -> wake_list) alongside the wake_state
 *                       wake_g.  Probes whether mixing the two wake routes on one
 *                       migratable fiber can double-enqueue.  If this control
 *                       trips (a), migratable fibers must be woken via EXACTLY
 *                       ONE route (wake_g only) -- a real constraint to enforce.
 *   -DBUG_NO_RUNNING_WOKEN_REQUEUE : hub_main blindly stores PARKED instead of
 *                       CAS RUNNING->PARKED-with-RUNNING_WOKEN-fallback, dropping
 *                       a wake that landed during the park-commit window -> lost
 *                       wake (must FAIL).
 *   -DBUG_SWEEP_DROP_WOKEN : the idle sweeper ends SWEEPING->PARKED even when a
 *                       wake landed during the sweep (state SWEEPING_WOKEN),
 *                       dropping it -> lost wake (must FAIL).
 *   -DNO_SWEEPER : drop the third claimer, recovering the old 2-claimer
 *                       composition for A/B (the default now models the real
 *                       3-way race: owner park-commit vs wake_g vs idle sweeper).
 */
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>

/* wake_state values -- MUST match runloom_sched.h:31-43 exactly (the drift-guard
 * in run_genmc.sh asserts this).  Six states: the RUNNING/RUNNING_WOKEN pair for
 * the owning resumer, and the SWEEPING/SWEEPING_WOKEN pair mirroring it for the
 * idle-stack sweeper (a THIRD concurrent claimer of a PARKED g -- added by the
 * stealable-wake-queue work, modeled below). */
enum {
    WS_PARKED         = 0,
    WS_QUEUED         = 1,
    WS_RUNNING        = 2,
    WS_RUNNING_WOKEN  = 3,
    WS_SWEEPING       = 4,
    WS_SWEEPING_WOKEN = 5
};

static atomic_int wake_state;       /* the single-location machine (protocol B) */
static atomic_int parked_safe;      /* Dekker commit flag (protocol A) */
static atomic_int wake_pending;     /* Dekker counter (protocol A) */
static atomic_int enq_runq;         /* enqueues onto the global run-queue */
static atomic_int enq_wakelist;     /* enqueues onto the owner wake_list (Dekker) */
static atomic_int parked;           /* parker completed BOTH commit phases (parked) */

/* ---- waker via wake_g (wake_state path), mn_sched_mn_api.c.inc:180-216 ---- */
static void *waker_state(void *arg)
{
    (void)arg;
    int st = atomic_load_explicit(&wake_state, memory_order_acquire);
    for (;;) {
        if (st == WS_PARKED) {
            if (atomic_compare_exchange_strong_explicit(&wake_state, &st, WS_QUEUED,
                    memory_order_acq_rel, memory_order_acquire)) {
                atomic_fetch_add_explicit(&enq_runq, 1, memory_order_release);
                return 0;
            }
        } else if (st == WS_RUNNING) {
            if (atomic_compare_exchange_strong_explicit(&wake_state, &st,
                    WS_RUNNING_WOKEN, memory_order_acq_rel, memory_order_acquire)) {
                return 0;   /* owner re-enqueues at its park-commit (below) */
            }
        } else if (st == WS_SWEEPING) {
            /* the g is mid-sweep: defer the wake to the sweeper's sweep_end
             * (mn_sched_mn_api.c.inc:313-318, SWEEPING->SWEEPING_WOKEN). */
            if (atomic_compare_exchange_strong_explicit(&wake_state, &st,
                    WS_SWEEPING_WOKEN, memory_order_acq_rel, memory_order_acquire)) {
                return 0;
            }
        } else {
            return 0;       /* QUEUED / RUNNING_WOKEN / SWEEPING_WOKEN: already pending */
        }
    }
}

/* ---- idle-stack sweeper: a THIRD concurrent claimer of a PARKED g.
 * runloom_mn_sweep_try_claim (mn_sched_mn_api.c.inc:435-440) CASes PARKED->
 * SWEEPING (exclusive, like QUEUED->RUNNING for a resumer); runloom_mn_sweep_end
 * (:454-464) then CASes SWEEPING->PARKED if no wake landed, or -- if wake_g
 * flipped it to SWEEPING_WOKEN meanwhile -- stores QUEUED and re-enqueues the
 * deferred wake exactly once.  A wake that lands during the sweep MUST NOT be
 * dropped (that is a lost wake); -DBUG_SWEEP_DROP_WOKEN models exactly that. */
static void *sweeper(void *arg)
{
    (void)arg;
    int st = WS_PARKED;
    if (!atomic_compare_exchange_strong_explicit(&wake_state, &st, WS_SWEEPING,
            memory_order_acq_rel, memory_order_acquire))
        return 0;                       /* not PARKED / lost the claim: nothing to sweep */
    /* (the madvise the sweep exists to do is elided -- it touches no shared state) */
    int se = WS_SWEEPING;
    if (atomic_compare_exchange_strong_explicit(&wake_state, &se, WS_PARKED,
            memory_order_acq_rel, memory_order_acquire))
        return 0;                       /* SWEEPING->PARKED: no wake landed */
    /* se == SWEEPING_WOKEN: wake_g deferred a wake to us -- re-enqueue exactly once. */
#ifdef BUG_SWEEP_DROP_WOKEN
    atomic_store_explicit(&wake_state, WS_PARKED, memory_order_release);   /* DROPS the wake */
#else
    atomic_store_explicit(&wake_state, WS_QUEUED, memory_order_release);
    atomic_fetch_add_explicit(&enq_runq, 1, memory_order_release);
#endif
    return 0;
}

#ifdef SEAM_MIX_DEKKER
/* ---- waker via wake_safe (Dekker path), runloom_sched_parkwake.c.inc ---- */
static void *waker_dekker(void *arg)
{
    (void)arg;
    atomic_fetch_add_explicit(&wake_pending, 1, memory_order_acq_rel);
    atomic_thread_fence(memory_order_seq_cst);
    int expected = 1;
    if (atomic_compare_exchange_strong_explicit(&parked_safe, &expected, 0,
            memory_order_acq_rel, memory_order_acquire)) {
        atomic_fetch_add_explicit(&enq_wakelist, 1, memory_order_release);
    }
    return 0;
}
#endif

/* ---- owner thread: the migratable park's two-phase commit ---- */
static void *parker(void *arg)
{
    (void)arg;
    /* Phase A -- Dekker handshake (park_generic, before coro_yield). */
    if (atomic_load_explicit(&wake_pending, memory_order_acquire) > 0) {
        atomic_fetch_sub_explicit(&wake_pending, 1, memory_order_acq_rel);
        return 0;                                   /* synchronous wake; did NOT park */
    }
    atomic_store_explicit(&parked_safe, 1, memory_order_release);
    atomic_thread_fence(memory_order_seq_cst);      /* StoreLoad (proven necessary) */
    if (atomic_load_explicit(&wake_pending, memory_order_acquire) > 0) {
        int expected = 1;
        if (atomic_compare_exchange_strong_explicit(&parked_safe, &expected, 0,
                memory_order_acq_rel, memory_order_acquire)) {
            atomic_fetch_sub_explicit(&wake_pending, 1, memory_order_acq_rel);
            return 0;                               /* aborted park; did NOT park */
        }
        /* lost the CAS: a Dekker waker enqueued us on the wake_list; fall through
         * -- but we must NOT also enqueue on the runq below.  The wake_state
         * commit's CAS handles that: if a wake_g also fired it is the runq owner;
         * the Dekker enqueue + a runq enqueue is exactly the double the spec
         * forbids, which -DSEAM_MIX_DEKKER exists to expose. */
    }

    /* "coro_yield" -- control passes to hub_main, which commits phase B. */

    /* Phase B -- wake_state commit (hub_main:698-712). */
#ifdef BUG_NO_RUNNING_WOKEN_REQUEUE
    atomic_store_explicit(&wake_state, WS_PARKED, memory_order_release);
#else
    {
        int rexp = WS_RUNNING;
        if (atomic_compare_exchange_strong_explicit(&wake_state, &rexp, WS_PARKED,
                memory_order_acq_rel, memory_order_acquire)) {
            /* committed PARKED: a later wake_g will PARKED->QUEUED us. */
        } else {
            /* rexp == RUNNING_WOKEN: a wake_g fired during the commit window --
             * re-enqueue exactly once. */
            atomic_store_explicit(&wake_state, WS_QUEUED, memory_order_release);
            atomic_fetch_add_explicit(&enq_runq, 1, memory_order_release);
        }
    }
#endif
    atomic_store_explicit(&parked, 1, memory_order_release);
    return 0;
}

int main(void)
{
    atomic_init(&wake_state, WS_RUNNING);   /* fiber is running before it parks */
    atomic_init(&parked_safe, 0);
    atomic_init(&wake_pending, 0);
    atomic_init(&enq_runq, 0);
    atomic_init(&enq_wakelist, 0);
    atomic_init(&parked, 0);

    pthread_t p, ws;
#ifndef NO_SWEEPER
    pthread_t sw;
#endif
#ifdef SEAM_MIX_DEKKER
    pthread_t wd;
#endif
    pthread_create(&p,  0, parker, 0);
    pthread_create(&ws, 0, waker_state, 0);
#ifndef NO_SWEEPER
    pthread_create(&sw, 0, sweeper, 0);     /* the third concurrent claimer */
#endif
#ifdef SEAM_MIX_DEKKER
    pthread_create(&wd, 0, waker_dekker, 0);
#endif
    pthread_join(p, 0);
    pthread_join(ws, 0);
#ifndef NO_SWEEPER
    pthread_join(sw, 0);
#endif
#ifdef SEAM_MIX_DEKKER
    pthread_join(wd, 0);
#endif

    int total = atomic_load(&enq_runq) + atomic_load(&enq_wakelist);
    /* (a) NO LOST WAKE: a fully-parked fiber must be enqueued. */
    assert(!(atomic_load(&parked) && total == 0));
    /* (b) ENQUEUED AT MOST ONCE across both destinations. */
    assert(total <= 1);
    return 0;
}
