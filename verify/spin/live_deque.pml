/*
 * live_deque.pml -- LOCK-FREE PROGRESS for the Chase-Lev steal path.
 *
 * cldeque.pml (safety) proves no loss/dup/phantom over all interleavings.
 * This model proves the complementary LIVENESS property that *defines*
 * lock-freedom:  in EVERY interleaving the system makes progress -- every
 * item is eventually consumed and every thread terminates -- and crucially
 * this holds WITHOUT any fairness assumption on the scheduler.  That is the
 * operational meaning of "lock-free": some thread always makes progress
 * regardless of how the scheduler interleaves them (unlike a lock-based
 * design, where a descheduled lock holder stalls everyone).
 *
 * So this model is checked under acceptance-cycle detection with NO weak
 * fairness:
 *     pan -a            -> errors: 0     (lock-free: progress w/o fairness)
 * and the negative control models the ALTERNATIVE design to give the
 * property teeth:
 *     -DBUG_BLOCKING    : steal takes a global mutex, and a holder can be
 *                         preempted (modelled as a busy phase loop) while
 *                         holding it.  Under no fairness that holder may
 *                         never resume to release -> waiters never progress
 *     pan -a            -> acceptance cycle (the blocking design CAN livelock
 *                         without fairness; the lock-free one cannot)
 *
 * Contrast with live_wake.pml: the per-g wake path is NOT lock-free at the
 * g granularity (a g needs its hub scheduled), so non-starvation there
 * REQUIRES fairness.  The deque does not.  The two models together draw the
 * exact line between "lock-free" and "fairness-dependent" progress.
 */

#define CAP      4
#define NITEMS   2
#define THIEVES  2
#define TOTAL    THIEVES

int  top = 0;
int  bottom = 0;
int  buf[CAP];
bit  claimed[NITEMS + 1];
int  consumed = 0;
int  nfin = 0;

#ifdef BUG_BLOCKING
bit  lock = 0;            /* global mutex for the blocking variant      */
bit  holder_phase = 0;    /* the preempted-while-holding busy loop       */
#endif

inline consume(item) {
    atomic {
        assert(item != 0);
        assert(claimed[item] == 0);
        claimed[item] = 1;
        consumed = consumed + 1;
    }
}

/*
 * The deque is PRE-POPULATED at init (see below): we isolate the steal
 * path, so there is no owner process to starve.  (Owner *scheduling* is a
 * fairness question covered by live_wake.pml; steal *progress* is the
 * lock-freedom question covered here.)  buf[k]=k+1, top=0, bottom=NITEMS.
 */
proctype thief()
{
    int t, b, item;
    do
    :: (consumed < NITEMS) ->
#ifdef BUG_BLOCKING
        /* Blocking design: acquire a global lock to steal. */
        atomic { (lock == 0) -> lock = 1; }
        t = top; b = bottom;
        if
        :: (t >= b) -> lock = 0;                 /* empty: release, loop */
        :: else ->
            item = buf[t]; top = t + 1;
            consume(item);
            /* BUG: the holder is preempted while still holding the lock --
             * modelled as a busy phase loop it must finish before release.
             * With no fairness the scheduler can spin here forever, so the
             * lock is never released and the other thief never progresses. */
            do
            :: (holder_phase == 0) -> holder_phase = 1
            :: (holder_phase == 1) -> holder_phase = 0
            :: lock = 0; break          /* (eventually) release + exit loop */
            od;
        fi;
#else
        /* Lock-free Chase-Lev steal: read top (acq), read bottom (acq),
         * CAS top t->t+1; loss just means another thief advanced top -- a
         * net system progress -- so the retry loop cannot livelock.
         *
         * The winning CAS is the linearization point: once a thief CASes
         * top, returning the item is a local, uninterruptible step no other
         * thread can prevent.  So the CAS and the consume are ONE atomic
         * here -- modelling them as separately-interleavable would invent a
         * window where a thief has "claimed" top but not yet "made progress"
         * (a model artifact, not a real stall). */
        t = top;
        b = bottom;
        if
        :: (t >= b) -> skip;                      /* empty / nothing to steal */
        :: else ->
            atomic {
                if
                :: (top == t) -> item = buf[t]; top = t + 1; consume(item);
                :: else -> skip;                  /* lost: top advanced = progress */
                fi;
            }
        fi;
#endif
    :: (consumed >= NITEMS) -> break;
    od;

    atomic {
        nfin = nfin + 1;
        if
        :: (nfin == TOTAL) -> assert(consumed == NITEMS);
        :: else -> skip;
        fi;
    }
}

init {
    atomic {
        /* pre-populate: NITEMS items already steal-visible */
        buf[0] = 1; buf[1] = 2; top = 0; bottom = NITEMS;
        run thief();
        run thief();
    }
}

/* Lock-free progress: every item is eventually consumed (system progress),
 * and this must hold WITHOUT a fairness assumption (run pan -a, no -f). */
ltl lockfree_progress { <> (consumed == NITEMS) }
