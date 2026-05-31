/*
 * netpoll_deadline.pml -- Promela model of the deadline-sweep claim race in
 * src/pygo_core/netpoll.c.  This is the timeout half of the netpoll: a parked
 * goroutine has BOTH an fd it waits on AND a finite deadline in the per-pool
 * min-heap.  Two DIFFERENT pump paths can wake it:
 *
 *   pygo_pump_dispatch_event  -- the fd became ready.  Claims the parker via
 *                                pygo_pump_claim (CAS commit->WOKEN), writes
 *                                *ready_out = mask (a NONZERO event mask), and
 *                                re-queues iff it claimed from PARKED.
 *
 *   pygo_pump_drain_expired   -- the deadline passed.  Pops the heap top, claims
 *                                via the SAME commit CAS, writes *ready_out = 0
 *                                (timeout), re-queues iff it claimed from PARKED.
 *
 * The parking g (pygo_netpoll_wait_fd) CASes commit ARMED->PARKED; on failure
 * (==WOKEN) it aborts the park, re-takes pool->lock (ordering it after the
 * winner's ready_out write), and returns ready_mask.  ready_mask starts 0.
 *
 * Unlike netpoll_commit.pml (two interchangeable "fd ready" pumps), here the two
 * claimers deliver DISTINCT values, so the interesting property is VALUE
 * CORRECTNESS under a simultaneous fd-ready + deadline-expiry race: the g must
 * resume EXACTLY ONCE and observe the value of whichever claimer actually won
 * the commit CAS -- never a spurious timeout (0) overwriting a delivered nonzero
 * mask, and never the still-unset initial.  Because dispatch_event and
 * drain_expired both serialise on pool->lock and both gate their ready_out write
 * behind the single commit-CAS claim, exactly one of them ever writes ready_out.
 *
 * PROVEN (1 parking g, 1 fd-ready dispatch, 1 deadline-expiry drain, all racing):
 *   NO LOST WAKE        -- the g always returns from wait_fd (a lost wake leaves
 *                          it stuck at its park = an invalid end state).
 *   AT MOST ONCE        -- resumes <= 1, requeues <= 1: at most one claimer wins
 *                          from PARKED; the loser sees WOKEN and touches nothing.
 *   VALUE CORRECTNESS   -- on return, ready_out was written by exactly the
 *                          claimer recorded in `winner`, and is never UNSET:
 *                          fd-win => mask, timeout-win => 0(timeout) sentinel.
 *   ONE WINNER          -- exactly one claimer transitions {ARMED,PARKED}->WOKEN.
 *
 * Negative control -DBUG_SWEEP_NO_COMMIT models the naive timeout sweep that the
 * commit CAS replaced: drain_expired pops the heap top and UNCONDITIONALLY sets
 * *ready_out = 0 + re-queues a parked g, without claiming via the commit CAS.
 * Spin then finds the spurious-timeout / double-resume: the fd dispatch delivers
 * a nonzero mask and re-queues, the sweep clobbers ready_out with 0 and re-queues
 * AGAIN -> the g returns a spurious timeout and/or is resumed twice.
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

/* ready_out values.  R_TIMEOUT models the real code's literal 0 (timeout); it is
 * kept distinct from R_UNSET only so the model can assert "a claimer actually
 * wrote it" -- in the C, timeout and never-written are both the integer 0, and
 * the only thing that disambiguates them is that the g never returns un-woken. */
#define R_UNSET   0
#define R_MASK    1   /* fd became ready: *ready_out = (nonzero) event mask */
#define R_TIMEOUT 2   /* deadline expired: *ready_out = 0                    */

#define NONE 0
#define FD   1
#define TO   2        /* timeout */

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

proctype parker()
{
    byte prev;
    /* CAS commit ARMED->PARKED -- lock-free, races both claimers. */
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
        /* value matches whoever won the claim */
        assert((winner == FD  && ready_out == R_MASK) ||
               (winner == TO   && ready_out == R_TIMEOUT));
        g_returned = 1;
    :: else ->
        /* prev == WOKEN: a claimer took our ARMED parker.  Abort the park; the
         * abort path re-takes pool->lock, ordering us after the claimer's
         * ready_out write (done under the same lock). */
        LOCK; UNLOCK;
        assert(g_parked == 0);        /* MUTUALLY EXCLUSIVE: never parked */
        assert(ready_out != R_UNSET);
        assert((winner == FD  && ready_out == R_MASK) ||
               (winner == TO   && ready_out == R_TIMEOUT));
        g_returned = 1;
    fi;
}

/* fd became ready: pygo_pump_dispatch_event. */
proctype fd_pump()
{
    byte prior;
    LOCK;
    atomic {                           /* pygo_pump_claim: CAS commit->WOKEN */
        prior = commit;
        if
        :: commit != WOKEN -> commit = WOKEN;
        :: else            -> skip;
        fi;
    }
    if
    :: prior == WOKEN -> skip;         /* already claimed: touch nothing */
    :: else ->
        winner = FD;
        ready_out = R_MASK;            /* *ready_out = mask, before any wake */
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

/* deadline expired: pygo_pump_drain_expired pops the heap top. */
proctype timeout_pump()
{
    byte prior;
    LOCK;
#ifndef BUG_SWEEP_NO_COMMIT
    /* ---- correct sweep: route the heap-pop through the SAME commit CAS ---- */
    atomic {
        prior = commit;
        if
        :: commit != WOKEN -> commit = WOKEN;
        :: else            -> skip;
        fi;
    }
    if
    :: prior == WOKEN -> skip;
    :: else ->
        winner = TO;
        ready_out = R_TIMEOUT;         /* *ready_out = 0 (timeout) */
        if
        :: prior == PARKED ->
            requeues++; resumes++;
            assert(resumes  <= 1);
            assert(requeues <= 1);
        :: else -> skip;
        fi;
    fi;
#else
    /* ---- BUG_SWEEP_NO_COMMIT: naive sweep, no commit claim ---------------- */
    /* Pop the heap top and unconditionally deliver timeout + re-queue a parked
     * g, ignoring whether an fd dispatch already claimed/woke it.  This is the
     * pre-commit sweep the protocol replaced. */
    winner = TO;
    ready_out = R_TIMEOUT;             /* clobbers any nonzero mask already set */
    if
    :: g_parked == 1 ->
        requeues++; resumes++;
        assert(resumes <= 1);          /* fires on the double resume */
    :: else -> skip;
    fi;
#endif
    UNLOCK;
}

init {
    atomic {
        run parker();
        run fd_pump();        /* the fd becomes ready ... */
        run timeout_pump();   /* ... at the same instant the deadline expires */
    }
}
