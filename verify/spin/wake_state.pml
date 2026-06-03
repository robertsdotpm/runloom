/*
 * wake_state.pml -- Promela model of the per-g wake_state machine.
 *
 * Models the RUNLOOM_PER_G_TSTATE protocol documented on `struct runloom_g`
 * in src/runloom_c/runloom_sched.h (the `wake_state` field).  A single
 * atomic unifies the exactly-once-wake dedup with the exclusive-resume
 * claim.  History (see the project's stall-recovery / lost-wake arc):
 * the predecessor used two independent flags that raced into a re-push
 * livelock.  This model verifies the unified machine has none of:
 *
 *   INV1  no duplicate / no orphan run-queue entry:
 *           at every reachable state  qentries == (state == QUEUED)
 *   INV2  no double resume:  at most one hub owns the g at once:
 *           at every reachable state  owners <= 1
 *   NOLOSS no lost wake:  the g is resumed at least once after the last
 *           wake -- at the fully drained terminal state, every issued
 *           wake has been served (last_wake_unserved == 0).
 *   no deadlock: the drained state is a valid end state and is always
 *           reachable; the model has no other (invalid) end states.
 *
 * Actors (all concurrent, all interleavings explored by Spin):
 *   WAKERS  (2) -- any thread calling wake_g, one wake each.
 *   HUBS    (2) -- pull a QUEUED g (QUEUED->RUNNING), "run" it, release.
 *   SWEEPER (1) -- idle-stack sweep: claim PARKED->SWEEPING, release.
 *
 * Define BUGGY_DROP_WAKE at compile time to re-introduce the classic
 * lost-wakeup bug (a wake during RUNNING is dropped instead of
 * remembered).  The model then FAILS the NOLOSS assertion -- proving the
 * check has teeth.   spin -DBUGGY_DROP_WAKE -a wake_state.pml && ...
 */

#define PARKED          0
#define QUEUED          1
#define RUNNING         2
#define RUNNING_WOKEN   3
#define SWEEPING        4
#define SWEEPING_WOKEN  5

#define NWAKERS  2
#define NSWEEPS  2

int  state    = PARKED;   /* the g's wake_state                          */
int  qentries = 0;        /* run-queue entries referencing this g        */
int  owners   = 0;        /* hubs currently owning the g (RUNNING)       */
int  wakers_done = 0;     /* wakers that have fired                       */
bit  last_wake_unserved = 0;  /* a wake was issued and not yet resumed   */

inline check_inv() {
    assert(qentries == (state == QUEUED));   /* INV1 */
    assert(owners <= 1);                     /* INV2 */
}

/* wake_g (any thread): the only transitions that touch `state`. */
proctype waker()
{
    atomic {
        last_wake_unserved = 1;        /* this wake needs a later resume */
        if
        :: (state == PARKED)   -> state = QUEUED; qentries++;
        :: (state == SWEEPING) -> state = SWEEPING_WOKEN;
#ifdef BUGGY_DROP_WAKE
        :: (state == RUNNING)  -> skip;            /* BUG: wake dropped! */
#else
        :: (state == RUNNING)  -> state = RUNNING_WOKEN;   /* remember   */
#endif
        :: (state == QUEUED)         -> skip;  /* drop: entry pending    */
        :: (state == RUNNING_WOKEN)  -> skip;  /* drop: remembered       */
        :: (state == SWEEPING_WOKEN) -> skip;  /* drop: remembered       */
        fi;
        check_inv();
        wakers_done++;
    }
}

/* hub pull+resume, then release. */
proctype hub()
{
    do
    :: atomic {                        /* pull: QUEUED -> RUNNING (CAS)  */
           (state == QUEUED) ->
               state = RUNNING; qentries--; owners++;
               last_wake_unserved = 0; /* g got the CPU -> wake served   */
               check_inv();
       }
       atomic {                        /* release after the run          */
           if
           :: (state == RUNNING)        -> state = PARKED; owners--;
           :: (state == RUNNING_WOKEN)  -> state = QUEUED; qentries++; owners--;
           fi;
           check_inv();
       }
    :: atomic {                        /* drained: nothing left to do    */
           (wakers_done == NWAKERS && state == PARKED && qentries == 0) ->
               assert(last_wake_unserved == 0);   /* NOLOSS */
               break;
       }
    od;
}

/* idle-stack sweeper: claim a long-parked g for an MADV_DONTNEED. */
proctype sweeper()
{
    int tries = 0;
    bit mine;
    do
    :: (tries < NSWEEPS) ->
        tries++;
        atomic {                       /* try-claim PARKED -> SWEEPING   */
            if
            :: (state == PARKED) -> state = SWEEPING; mine = 1;
            :: else              -> mine = 0;   /* lost claim, skip       */
            fi;
            check_inv();
        }
        if
        :: (mine == 1) ->
            /* madvise window: a concurrent wake may land here, moving
             * SWEEPING -> SWEEPING_WOKEN.  Then release re-enqueues. */
            atomic {                   /* release the sweep              */
                if
                :: (state == SWEEPING)        -> state = PARKED;
                :: (state == SWEEPING_WOKEN)  -> state = QUEUED; qentries++;
                fi;
                check_inv();
            }
        :: else -> skip;
        fi;
    :: (tries >= NSWEEPS) -> break;
    od;
}

init {
    atomic {
        run waker();
        run waker();
        run hub();
        run hub();
        run sweeper();
    }
}
