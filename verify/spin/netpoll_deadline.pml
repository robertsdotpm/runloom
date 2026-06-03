/*
 * netpoll_deadline.pml -- Promela model of the multi-claimer wake race in
 * src/runloom_c/netpoll.c.  A parked goroutine has an fd it waits on, a finite
 * deadline in the per-pool min-heap, AND may be cancelled out-of-band.  THREE
 * different paths can wake it, each delivering a DISTINCT value, and each
 * claiming via the SAME commit CAS (runloom_pump_claim semantics):
 *
 *   runloom_pump_dispatch_event  -- the fd became ready.  *ready_out = mask
 *                                (a NONZERO event mask).
 *   runloom_pump_drain_expired   -- the deadline passed.  *ready_out = 0 (timeout).
 *   runloom_netpoll_cancel_g     -- task.cancel() targeted a g blocked in wait_fd
 *                                (no coro await-point).  *ready_out =
 *                                RUNLOOM_NETPOLL_CANCELLED; wait_fd's _wait_fd
 *                                wrapper turns it into CancelledError.
 *
 * Each claims commit->WOKEN, writes ready_out, and re-queues iff it claimed
 * from PARKED.  The parking g (runloom_netpoll_wait_fd) CASes commit ARMED->PARKED;
 * on failure (==WOKEN) it aborts the park, re-takes pool->lock (ordering it
 * after the winner's ready_out write), and returns ready_mask (starts 0).
 *
 * All three claimers serialise on pool->lock and gate their ready_out write
 * behind the single commit-CAS claim, so the interesting property is VALUE
 * CORRECTNESS under a simultaneous fd-ready + deadline-expiry + cancel race:
 * the g resumes EXACTLY ONCE and observes the value of whichever claimer won
 * the CAS -- never a spurious timeout clobbering a delivered mask, never a
 * cancel lost to a concurrent fd-ready, never the still-unset initial.
 *
 * PROVEN (1 parking g racing fd-ready dispatch + deadline drain + cancel):
 *   NO LOST WAKE        -- the g always returns from wait_fd (a lost wake leaves
 *                          it stuck at its park = an invalid end state).
 *   AT MOST ONCE        -- resumes <= 1, requeues <= 1: at most one claimer wins
 *                          from PARKED; the losers see WOKEN and touch nothing.
 *   VALUE CORRECTNESS   -- on return, ready_out was written by exactly the
 *                          claimer recorded in `winner`, and is never UNSET
 *                          (fd => mask, timeout => 0, cancel => CANCELLED).
 *   ONE WINNER          -- exactly one claimer transitions {ARMED,PARKED}->WOKEN.
 *
 * Negative controls model a claimer that SKIPS the commit CAS and
 * unconditionally writes ready_out + re-queues a parked g:
 *   -DBUG_SWEEP_NO_COMMIT  -- the naive timeout sweep the commit CAS replaced.
 *   -DBUG_CANCEL_NO_COMMIT -- a cancel that wakes without claiming.
 * Either reproduces the spurious-value / double-resume Spin catches.
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

/* ready_out values.  R_TIMEOUT models the real code's literal 0 (timeout); it is
 * kept distinct from R_UNSET only so the model can assert "a claimer actually
 * wrote it" -- in the C, timeout and never-written are both the integer 0, and
 * the only thing that disambiguates them is that the g never returns un-woken. */
#define R_UNSET     0
#define R_MASK      1   /* fd became ready: *ready_out = (nonzero) event mask  */
#define R_TIMEOUT   2   /* deadline expired: *ready_out = 0                     */
#define R_CANCELLED 3   /* cancelled: *ready_out = RUNLOOM_NETPOLL_CANCELLED       */

#define NONE   0
#define FD     1
#define TO     2        /* timeout */
#define CANCEL 3

byte commit     = ARMED;
byte ready_out  = R_UNSET;
byte winner     = NONE;    /* which claimer won the commit CAS (set once)     */
bit  g_parked   = 0;       /* g committed PARKED and yielded (suspended)      */
bit  g_returned = 0;
int  resumes    = 0;       /* times the g is made runnable again (wake_g)     */
int  requeues   = 0;       /* claimer wake_g calls (must equal resumes)       */

bit  lock = 0;             /* pool->lock                                      */

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

/* value matches whoever won the claim */
#define VALUE_OK \
    ((winner == FD     && ready_out == R_MASK)      || \
     (winner == TO     && ready_out == R_TIMEOUT)   || \
     (winner == CANCEL && ready_out == R_CANCELLED))

proctype parker()
{
    byte prev;
    /* CAS commit ARMED->PARKED -- lock-free, races all claimers. */
    atomic {
        prev = commit;
        if
        :: commit == ARMED -> commit = PARKED;   /* CAS success */
        :: else            -> skip;              /* CAS fails (commit==WOKEN) */
        fi;
    }
    if
    :: prev == ARMED ->
        g_parked = 1;
        (resumes > 0);                /* park; a lost wake wedges here forever */
        assert(ready_out != R_UNSET); /* a claimer delivered a value first     */
        assert(VALUE_OK);
        g_returned = 1;
    :: else ->
        /* prev == WOKEN: a claimer took our ARMED parker.  Abort the park; the
         * abort path re-takes pool->lock, ordering us after the claimer's
         * ready_out write (done under the same lock). */
        LOCK; UNLOCK;
        assert(g_parked == 0);        /* MUTUALLY EXCLUSIVE: never parked */
        assert(ready_out != R_UNSET);
        assert(VALUE_OK);
        g_returned = 1;
    fi;
}

/* A claimer that routes through the commit CAS (runloom_pump_dispatch_event /
 * runloom_pump_drain_expired / runloom_netpoll_cancel_g).  `who`/`val` are its
 * identity + the value it delivers. */
inline claim_and_deliver(who, val)
{
    byte prior;
    LOCK;
    atomic {                           /* runloom_pump_claim: CAS commit->WOKEN */
        prior = commit;
        if
        :: commit != WOKEN -> commit = WOKEN;
        :: else            -> skip;
        fi;
    }
    if
    :: prior == WOKEN -> skip;         /* already claimed: touch nothing */
    :: else ->
        winner = who;
        ready_out = val;               /* *ready_out = val, before any wake */
        if
        :: prior == PARKED ->
            requeues++; resumes++;
            assert(resumes  <= 1);
            assert(requeues <= 1);
        :: else -> skip;               /* ARMED: g aborts itself, do NOT wake */
        fi;
    fi;
    UNLOCK;
}

/* A buggy claimer that SKIPS the commit CAS: unconditionally writes its value
 * and re-queues a parked g, ignoring whether another claimer already won. */
inline deliver_no_claim(who, val)
{
    LOCK;
    winner = who;
    ready_out = val;                   /* clobbers any value already set */
    if
    :: g_parked == 1 ->
        requeues++; resumes++;
        assert(resumes <= 1);          /* fires on the double resume */
    :: else -> skip;
    fi;
    UNLOCK;
}

proctype fd_pump()    { claim_and_deliver(FD, R_MASK); }

proctype timeout_pump()
{
#ifndef BUG_SWEEP_NO_COMMIT
    claim_and_deliver(TO, R_TIMEOUT);
#else
    deliver_no_claim(TO, R_TIMEOUT);
#endif
}

proctype cancel_pump()
{
#ifndef BUG_CANCEL_NO_COMMIT
    claim_and_deliver(CANCEL, R_CANCELLED);
#else
    deliver_no_claim(CANCEL, R_CANCELLED);
#endif
}

init {
    atomic {
        run parker();
        run fd_pump();        /* the fd becomes ready ... */
        run timeout_pump();   /* ... as the deadline expires ... */
        run cancel_pump();    /* ... and a task.cancel() lands, all at once */
    }
}
