/*
 * park_generic_timed.pml -- Promela model of the fd-free TIMED in-memory park
 * (runloom_park_generic_timed in src/runloom_c/runloom_sched_parkwake.c.inc).
 *
 * A timed park is the SAME park_safe/wake_safe Dekker handshake as
 * parked_safe.pml, with ONE extra claimer: the TIMER DRAIN, which fires the
 * deadline by CASing the SAME parked_safe -- so a timeout racing a real wake of
 * the same g is resolved exactly once by the one Dekker CAS.  The timer writes
 * NO result flag (the parker reads the clock on resume), so the ONLY shared
 * coordination is the parked_safe CAS.
 *
 * Crucial timing fact encoded here: the timer drain runs on the parker's OWN
 * thread/hub, AFTER the parker yields -- it can NEVER run while the parker is
 * mid-handshake.  So the timer only acts once the parker is suspended at the
 * yield (parked == 1), or not at all if the parker already aborted/resolved
 * (parker_done == 1, the wake-before-park abort).  The REAL wake (wake_safe) is
 * the only genuinely concurrent actor -- it may arrive from a foreign thread at
 * any point, exactly as in parked_safe.pml.
 *
 * Proven (all interleavings):
 *   EXACTLY ONCE -- enqueued <= 1: a real wake and the timer never both enqueue
 *      the g (no double-resume).  The parked_safe CAS is the sole arbiter.
 *   NO LOST WAKE -- the parker never blocks forever at the yield: whichever of
 *      {wake_safe, timer} the parker did not abort against enqueues it.  Encoded
 *      as Spin's invalid-end-state (a parker stuck at (enqueued>0) is a deadlock).
 *   BALANCE      -- at quiescence wake_pending == 0.
 *
 * Negative control (-DBUG_TIMER_NO_CAS): the timer enqueues WITHOUT the
 * parked_safe CAS (unconditionally), so a real wake + the timer both enqueue ->
 * enqueued == 2 -> the assert fires.  This is the "timer must go through the same
 * exactly-once arbiter" property.
 *
 * (The seq_cst StoreLoad fence that makes the Dekker sound on weak memory is
 * proven separately + necessary in verify/genmc/sched_parkwake.c under RC11;
 * Spin is sequentially consistent, so it cannot model fence removal -- the timer
 * adds no new weak-memory edge, it only adds a second SC CAS claimer.)
 */

int  wake_pending = 0;
bit  parked_safe  = 0;
int  enqueued     = 0;     /* times g made runnable (wake_list / timer)  */
bit  parked       = 0;     /* parker suspended at the yield              */
bit  parker_done  = 0;     /* parker resolved (woke OR aborted)          */
int  nfin         = 0;

#define NPROC 3            /* parker + waker + timer */

active proctype parker()
{
    int cas;

    /* step 1: a real wake already arrived? eat one count, skip the park. */
    if
    :: (wake_pending > 0) -> wake_pending--; goto pdone;
    :: else -> skip;
    fi;

    /* step 2: commit to parking (release store). */
    parked_safe = 1;

    /* step 3: recheck -- pairs with wake_safe's "bump then CAS". */
    if
    :: (wake_pending > 0) ->
        atomic {                       /* CAS parked_safe 1 -> 0 */
            if
            :: (parked_safe == 1) -> parked_safe = 0; cas = 1;
            :: else -> cas = 0;
            fi;
        }
        if
        :: (cas == 1) -> wake_pending--; goto pdone;
        :: else -> skip;               /* lost CAS: a waker owns us -> yield */
        fi;
    :: else -> skip;
    fi;

    /* step 4: yield.  Resumed only when a waker (real OR timer) enqueued us. */
    parked = 1;
    (enqueued > 0);
    parked = 0;
    wake_pending--;                    /* eat the delivering real wake, if any */

pdone:
    parker_done = 1;
    atomic {
        nfin++;
        if
        :: (nfin == NPROC) -> assert(wake_pending == 0); assert(enqueued <= 1);
        :: else -> skip;
        fi;
    }
}

/* The real wake (wake_safe): genuinely concurrent (a foreign thread / another
 * hub).  Bumps wake_pending then CASes parked_safe; owns the wake iff it wins. */
active proctype waker()
{
    int cas;

    wake_pending++;                    /* fetch_add (ACQ_REL) */
    atomic {                           /* CAS parked_safe 1 -> 0 */
        if
        :: (parked_safe == 1) -> parked_safe = 0; cas = 1;
        :: else -> cas = 0;
        fi;
    }
    if
    :: (cas == 1) -> enqueued++;       /* we own the wake: enqueue g */
    :: else -> skip;                   /* parker will consume wake_pending */
    fi;
    assert(enqueued <= 1);             /* no double schedule */

    atomic {
        nfin++;
        if
        :: (nfin == NPROC) -> assert(wake_pending == 0); assert(enqueued <= 1);
        :: else -> skip;
        fi;
    }
}

/* The TIMER drain.  Runs on the parker's own thread/hub, so it acts only once
 * the parker has SETTLED: it is suspended at the yield (parked == 1) -> attempt
 * the claim; or it already resolved (parker_done == 1, the wake-before-park
 * abort) -> the timer is a no-op (its stale heap entry is discarded).  It does
 * NOT bump wake_pending (it is not a Dekker waker, only a parked_safe claimer). */
active proctype timer()
{
    int cas;

    if
    :: (parked == 1) ->
#ifdef BUG_TIMER_NO_CAS
        /* NEGATIVE CONTROL: skip the exactly-once CAS and enqueue blindly.
         * If a real wake already claimed the park, this double-enqueues. */
        enqueued++;
#else
        atomic {                       /* CAS parked_safe 1 -> 0 */
            if
            :: (parked_safe == 1) -> parked_safe = 0; cas = 1;
            :: else -> cas = 0;
            fi;
        }
        if
        :: (cas == 1) -> enqueued++;   /* we own the wake: deliver the timeout */
        :: else -> skip;               /* a real wake beat us -> discard */
        fi;
#endif
    :: (parker_done == 1) -> skip;     /* parker aborted/resolved -> stale entry */
    fi;
    assert(enqueued <= 1);

    atomic {
        nfin++;
        if
        :: (nfin == NPROC) -> assert(wake_pending == 0); assert(enqueued <= 1);
        :: else -> skip;
        fi;
    }
}
