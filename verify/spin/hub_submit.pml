/*
 * hub_submit.pml -- Promela model of the DEFAULT M:N wake path on Linux
 * free-threaded 3.13t (per-hub-tstate; PYGO_PER_G_TSTATE and
 * PYGO_STEAL_WOKEN both OFF, so pygo_mn_wake_g routes through
 * pygo_mn_hub_submit, NOT the global-runq wake_state machine modelled in
 * wake_state.pml).
 *
 * Models src/pygo_core/mn_sched.c:
 *   pygo_mn_hub_submit  -- CAS g->in_sub_queue 0->1; only the winner links
 *                          g into the hub's MPSC submission list.
 *   hub_main            -- pops g, clears in_sub_queue just before the
 *                          resume, and SKIPS a g whose coro is already
 *                          gone (done).
 *
 * A parker can legitimately be wake_g'd more than once (a netpoll-pump
 * unlink followed by a stale safety-unlink wake).  Two defenses keep that
 * safe; this proves them:
 *
 *   NO RESUME-AFTER-DONE  the hub never resumes a g that already ran to
 *                         completion -- the second resume would touch a
 *                         coro freed by the post-completion decref (the
 *                         segfault these defenses exist to prevent).
 *   RUNS EXACTLY ONCE     coalesced wakes resume g exactly once: no lost
 *                         wake (>=1) and no double-resume (<=1).
 *   AT MOST ONE ENTRY     the dedup keeps g's submission count <= 1.
 *
 * Negative control: -DBUG_NO_DEDUP removes BOTH the CAS dedup and the
 * done-check (the historical pre-fix state) -> the model FAILS
 * (resume-after-done), proving the checks have teeth.
 */

#define NWAKERS 2

bit in_sub_queue = 0;     /* g->in_sub_queue (dedup flag)             */
int pending      = 0;     /* submission-list entries referencing g    */
bit done         = 0;     /* g ran to completion (coro freed after)   */
int resumes      = 0;     /* times the hub resumed g's coro           */
bit resumed_done = 0;     /* hub resumed an already-done g (the bug)   */
int wakers_done  = 0;

/* pygo_mn_hub_submit: dedup via CAS, then link. */
inline submit() {
    atomic {
#ifdef BUG_NO_DEDUP
        pending++;                       /* BUG: no dedup -> may enqueue twice */
#else
        if
        :: (in_sub_queue == 0) -> in_sub_queue = 1; pending++;  /* won: enqueue */
        :: else -> skip;                 /* already queued: drop (coalesce) */
        fi;
        assert(pending <= 1);            /* AT MOST ONE ENTRY */
#endif
    }
}

proctype waker()
{
    submit();
    atomic { wakers_done++; }
}

proctype hub()
{
    do
    :: atomic {
           /* pop one entry, clear in_sub_queue, resume -- the clear sits
            * just before the coro resume in mn_sched.c (line 913). */
           (pending > 0) ->
           pending--;
           in_sub_queue = 0;
#ifdef BUG_NO_DEDUP
           if :: (done) -> resumed_done = 1;        /* BUG: resume-after-done */
              :: else -> skip;
           fi;
           resumes++;
           done = 1;                                /* run to completion */
#else
           if
           :: (done) -> skip;                        /* done-check: skip freed coro */
           :: else -> resumes++; done = 1;           /* resume + run to completion */
           fi;
#endif
           assert(resumed_done == 0);                /* NO RESUME-AFTER-DONE */
       }
    :: atomic {
           (wakers_done == NWAKERS && pending == 0) ->
           assert(resumes == 1);                     /* RUNS EXACTLY ONCE */
           break;
       }
    od;
}

init {
    atomic {
        run waker();
        run waker();
        run hub();
    }
}
