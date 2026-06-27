/* SOURCE-ANCHOR: runloom_sched_wake runloom_sched_wake_safe runloom_sched_park_safe  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * wake_state_fsm_cbmc.c -- CBMC model of the per-g `wake_state` finite state
 * machine (RUNLOOM_PER_G_TSTATE global run-queue), the single atomic that makes
 * the woken-g runq safe for any idle hub to drain WITHOUT duplicate entries,
 * double-resume, or lost wakes.
 *
 * FAITHFUL SLICE of the transition table documented at runloom_sched.h:249-284.
 * The six states and the only legal edges (each a CAS by the named actor):
 *
 *   wake_g (any thread):  PARKED         -> QUEUED          (winner enqueues)
 *                         RUNNING        -> RUNNING_WOKEN   (remember; no entry)
 *                         SWEEPING       -> SWEEPING_WOKEN  (remember; no entry)
 *                         QUEUED / *_WOKEN -> drop          (already pending)
 *   hub pull+resume:      QUEUED         -> RUNNING         (sole entry consumer)
 *   hub release:          RUNNING        -> PARKED, or
 *                         RUNNING_WOKEN  -> QUEUED          (deliver remembered)
 *   sweeper claim:        PARKED         -> SWEEPING (try; loses to non-PARKED)
 *   sweeper release:      SWEEPING       -> PARKED, or
 *                         SWEEPING_WOKEN -> QUEUED          (deliver remembered)
 *   hub timer-claim:      PARKED         -> RUNNING (expired sleeper; resumes
 *                         QUEUED         -> QUEUED   direct -- the only entry into
 *                                                    RUNNING not via QUEUED; loses
 *                                                    the CAS if a waker raced it)
 *
 * Because a SINGLE field encodes both "has a runq entry" (== QUEUED) and "is
 * owned" (== RUNNING/SWEEPING), single-entry and single-owner hold by
 * construction -- that is the whole point of unifying the two flags that used to
 * race into a re-push livelock.  The two properties left to prove over the
 * transition relation are:
 *
 *   TOTALITY    -- every event that is ENABLED in a state has a DEFINED
 *                  transition (never an undefined/INVALID cell -> never a
 *                  silently-dropped event).
 *   NO_LOST_WAKE -- once a wake has been delivered, the g cannot return to
 *                  PARKED without first becoming QUEUED (acquiring a runq entry);
 *                  i.e. a remembered wake (*_WOKEN) is always enqueued at
 *                  release, never lost.
 *
 * CBMC explores all event sequences up to the unwind bound.  Default config:
 * VERIFICATION SUCCESSFUL.  Teeth: -DBUG_LOSE_WAKE makes release drop a
 * remembered wake (RUNNING_WOKEN -> PARKED); -DBUG_TIMER_CLAIM_DROPS makes the
 * timer-claim win the CAS but leave the sleeper PARKED (the timer wake is lost) --
 * each makes NO_LOST_WAKE fail, proving the harness has teeth on both wake paths.
 *
 * Run via verify/run_verify.sh (cbmc), or directly:
 *   cbmc wake_state_fsm_cbmc.c --unwind 16
 *   cbmc wake_state_fsm_cbmc.c --unwind 16 -DBUG_LOSE_WAKE          (expect FAILED)
 *   cbmc wake_state_fsm_cbmc.c --unwind 16 -DBUG_TIMER_CLAIM_DROPS  (expect FAILED)
 */

#define WS_PARKED          0
#define WS_QUEUED          1
#define WS_RUNNING         2
#define WS_RUNNING_WOKEN   3
#define WS_SWEEPING        4
#define WS_SWEEPING_WOKEN  5
#define WS_NSTATES         6

enum {
    EV_WAKE = 0,          /* a waker (any thread) fires wake_g          */
    EV_PULL,              /* a hub pulls a queued entry -> resume       */
    EV_RELEASE,           /* a hub finishes the resume + releases       */
    EV_SWEEP_CLAIM,       /* a sweeper try-claims for an idle stack sweep */
    EV_SWEEP_RELEASE,     /* a sweeper finishes the madvise + releases  */
    EV_TIMER_CLAIM,       /* a hub claims an expired sleeper PARKED->RUNNING */
    EV_NEVENTS
};

#define INV ((signed char)-1)   /* illegal (state,event) cell */

#ifndef WAKE_FSM_BOUND
#  define WAKE_FSM_BOUND 16
#endif

int nondet_int(void);

static signed char T[WS_NSTATES][EV_NEVENTS];

static void build_table(void)
{
    int s, e;
    for (s = 0; s < WS_NSTATES; s++)
        for (e = 0; e < EV_NEVENTS; e++)
            T[s][e] = INV;

    /* wake_g (always applicable) */
    T[WS_PARKED]        [EV_WAKE] = WS_QUEUED;
    T[WS_QUEUED]        [EV_WAKE] = WS_QUEUED;          /* drop: already pending */
    T[WS_RUNNING]       [EV_WAKE] = WS_RUNNING_WOKEN;   /* remember */
    T[WS_RUNNING_WOKEN] [EV_WAKE] = WS_RUNNING_WOKEN;   /* drop */
    T[WS_SWEEPING]      [EV_WAKE] = WS_SWEEPING_WOKEN;  /* remember */
    T[WS_SWEEPING_WOKEN][EV_WAKE] = WS_SWEEPING_WOKEN;  /* drop */

    /* hub pull+resume (only the sole holder of a QUEUED entry) */
    T[WS_QUEUED]        [EV_PULL] = WS_RUNNING;

    /* hub release */
    T[WS_RUNNING]       [EV_RELEASE] =
#ifdef BUG_LOSE_WAKE
        WS_PARKED;   /* (teeth handled below at RUNNING_WOKEN) */
#else
        WS_PARKED;
#endif
#ifdef BUG_LOSE_WAKE
    T[WS_RUNNING_WOKEN] [EV_RELEASE] = WS_PARKED;       /* BUG: drops the wake */
#else
    T[WS_RUNNING_WOKEN] [EV_RELEASE] = WS_QUEUED;       /* deliver remembered */
#endif

    /* sweeper claim (try; a no-op that loses in any non-PARKED state) */
    T[WS_PARKED]        [EV_SWEEP_CLAIM] = WS_SWEEPING;
    T[WS_QUEUED]        [EV_SWEEP_CLAIM] = WS_QUEUED;
    T[WS_RUNNING]       [EV_SWEEP_CLAIM] = WS_RUNNING;
    T[WS_RUNNING_WOKEN] [EV_SWEEP_CLAIM] = WS_RUNNING_WOKEN;
    T[WS_SWEEPING]      [EV_SWEEP_CLAIM] = WS_SWEEPING;
    T[WS_SWEEPING_WOKEN][EV_SWEEP_CLAIM] = WS_SWEEPING_WOKEN;

    /* sweeper release */
    T[WS_SWEEPING]      [EV_SWEEP_RELEASE] = WS_PARKED;
    T[WS_SWEEPING_WOKEN][EV_SWEEP_RELEASE] = WS_QUEUED; /* deliver remembered */

    /* hub timer-claim of an expired sleeper.  The CAS expects PARKED and drives
     * PARKED -> RUNNING (the only entry into RUNNING that does not pass through
     * QUEUED), then resumes the g directly on the local FIFO.  If a waker raced
     * PARKED -> QUEUED first, the CAS loses and the g stays QUEUED (the waker's
     * entry resumes it -- do NOT also push, or it schedules twice). */
#ifdef BUG_TIMER_CLAIM_DROPS
    T[WS_PARKED]        [EV_TIMER_CLAIM] = WS_PARKED;   /* BUG: claims but never
                                                        * resumes -> timer wake lost */
#else
    T[WS_PARKED]        [EV_TIMER_CLAIM] = WS_RUNNING;
#endif
    T[WS_QUEUED]        [EV_TIMER_CLAIM] = WS_QUEUED;
}

/* An event is ENABLED only when the actor holds the precondition that would let
 * it fire in the real scheduler: a hub PULLs only an entry that exists (QUEUED);
 * a hub RELEASEs only a g it owns (RUNNING / RUNNING_WOKEN); a sweeper RELEASEs
 * only a stack it holds (SWEEPING / SWEEPING_WOKEN).  wake_g and the try-claim
 * are always enabled. */
static int enabled(int s, int e)
{
    switch (e) {
        case EV_PULL:          return s == WS_QUEUED;
        case EV_RELEASE:       return s == WS_RUNNING || s == WS_RUNNING_WOKEN;
        case EV_SWEEP_RELEASE: return s == WS_SWEEPING || s == WS_SWEEPING_WOKEN;
        case EV_WAKE:          return 1;
        case EV_SWEEP_CLAIM:   return 1;
        /* a timer-claim only fires for a g still on the sleep queue: PARKED, or
         * QUEUED if a waker already raced it off PARKED. */
        case EV_TIMER_CLAIM:   return s == WS_PARKED || s == WS_QUEUED;
        default:               return 0;
    }
}

int main(void)
{
    int s = WS_PARKED;
    int pending_wake = 0;   /* a delivered wake not yet turned into a runq entry */
    int step;

    build_table();

    for (step = 0; step < WAKE_FSM_BOUND; step++) {
        int e  = nondet_int();
        int ns;
        __CPROVER_assume(e >= 0 && e < EV_NEVENTS);
        __CPROVER_assume(enabled(s, e));

        ns = T[s][e];

        /* TOTALITY: an enabled event must map to a defined transition. */
        __CPROVER_assert(ns != INV,
            "wake_state FSM: enabled event has no defined transition");

        if (e == EV_WAKE || e == EV_TIMER_CLAIM)
            pending_wake = 1;             /* an explicit wake or a timer wake is outstanding */
        if (ns == WS_QUEUED || ns == WS_RUNNING)
            pending_wake = 0;             /* enqueued, or resumed directly -> the g will run */

        /* NO_LOST_WAKE: the g must never go back to sleep (PARKED) while a
         * delivered wake is still outstanding -- that is a permanent hang. */
        __CPROVER_assert(!(ns == WS_PARKED && pending_wake),
            "wake_state FSM: a delivered wake was lost (reached PARKED unenqueued)");

        s = ns;
    }
    return 0;
}
