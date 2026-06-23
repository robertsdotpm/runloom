/*
 * preempt_defer_cbmc.c -- CBMC proof of the preemption defer-during-destruction
 * gate (src/runloom_c/mn_sched_hub_resume_preempt.c.inc), the guard that prevents
 * the proven p69b weakref/finalizer use-after-free.
 *
 * The invariant: a fiber must NEVER be preempted (yield the baton) while CPython
 * is mid object-destruction on this tstate.  Suspending a half-finished tp_dealloc
 * (a weakref callback / finalizer, driven by the trashcan unwind or the
 * free-threaded biased-refcount cross-thread merge) freezes it on the g's coro
 * stack at a GC-safe point; a concurrent stop-the-world GC / QSBR reclaim then
 * runs against partially-destroyed objects -> UAF SIGSEGV
 * (test_weakref.test_threaded_weak_key_dict_copy).  The gate defers the preempt
 * (leaving the watchdog flag / frame counter ARMED) until the destructor unwinds.
 *
 * Two properties, over an arbitrary sequence of frame entries -- each with a
 * nondeterministic preempt trigger and in_destruction flag:
 *
 *   SAFETY          -- the gate NEVER yields while in_destruction.
 *   NO_LOST_PREEMPT -- a pending preempt is DEFERRED (not dropped) while in
 *                      destruction, and is TAKEN at the first frame that is not in
 *                      destruction: preemption is delayed a few frames, never lost.
 *
 * Teeth (each MUST report VERIFICATION FAILED):
 *   -DBUG_YIELD_IN_DEST  -- the gate ignores in_destruction (yields anyway)
 *                           -> SAFETY fails (this IS the p69b bug).
 *   -DBUG_DROP_ON_DEFER  -- the gate clears the armed trigger when deferring
 *                           -> the preempt is lost -> NO_LOST_PREEMPT fails.
 *
 * Run via verify/run_verify.sh (cbmc), or directly:
 *   cbmc preempt_defer_cbmc.c
 *   cbmc preempt_defer_cbmc.c -DBUG_YIELD_IN_DEST   (expect FAILED)
 *   cbmc preempt_defer_cbmc.c -DBUG_DROP_ON_DEFER   (expect FAILED)
 */

#ifndef PREEMPT_BOUND
#  define PREEMPT_BOUND 12
#endif

int nondet_int(void);

int main(void)
{
    int armed = 0;   /* a preempt trigger is pending (watchdog flag / frame ctr) */
    int owed  = 0;   /* a trigger fired and has not yet been honored by a yield  */
    int step;

    for (step = 0; step < PREEMPT_BOUND; step++) {
        int trigger = nondet_int() & 1;
        int in_dest = nondet_int() & 1;
        int do_yield;

        if (trigger) { armed = 1; owed = 1; }

        /* The gate: yield iff a preempt is armed AND we are not mid-destruction. */
#ifdef BUG_YIELD_IN_DEST
        do_yield = armed;                  /* BUG: ignores in_destruction */
#else
        do_yield = armed && !in_dest;
#endif

        if (do_yield) {
            armed = 0;
            owed  = 0;                      /* preempt honored */
        } else if (armed && in_dest) {
            /* defer: leave the trigger ARMED so the next safe frame takes it */
#ifdef BUG_DROP_ON_DEFER
            armed = 0;                      /* BUG: drops the pending preempt */
#endif
        }

        /* SAFETY: never yield mid-destruction (the p69b invariant). */
        __CPROVER_assert(!(do_yield && in_dest),
            "preempt gate: yielded the baton during object destruction (p69b UAF)");

        /* NO_LOST_PREEMPT: at any frame that is NOT in destruction, no preempt may
         * remain owed -- it must have been honored this frame. */
        __CPROVER_assert(!(owed && !in_dest),
            "preempt gate: a pending preempt was not taken at a safe frame");
    }
    return 0;
}
