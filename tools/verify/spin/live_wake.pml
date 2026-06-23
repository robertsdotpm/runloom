/*
 * live_wake.pml -- LIVENESS for the per-g wake protocol.
 *
 * The safety model (wake_state.pml) proves the wake machine has no bad
 * *interleaving*: no lost wake, no double resume, no dup runq entry.  But
 * "no lost wake" there is encoded structurally (a fully-drained terminal
 * state has last_wake_unserved==0) and as a *deadlock-freedom* (invalid
 * end state) check.  Neither is true LIVENESS: that a parked g which has
 * been woken *eventually runs*, even while the rest of the system keeps
 * making progress around it.  Starvation-freedom is a temporal property
 * (`[] (woken -> <> resumed)`) and it only holds *under a fairness
 * assumption* on the scheduler -- exactly the property the stall-recovery
 * / handoff arc is about.  Spin checks it with acceptance-cycle detection
 * (`pan -a`) under weak fairness (`pan -f`).
 *
 * The key honesty point this model makes load-bearing: WITHOUT weak
 * fairness the property is FALSE (a busy peer can run forever while the
 * woken g's hub never gets scheduled).  WITH weak fairness it is TRUE.
 * So `run_verify.sh` runs it BOTH ways and asserts:
 *     pan -a -f   -> errors: 0        (holds: a continuously-runnable
 *                                      hub is eventually scheduled)
 *     pan -a      -> error found      (fails: the unfair run starves it)
 * i.e. the proof's teeth are that fairness is *necessary and sufficient*.
 *
 * State machine is the live subset of wake_state.pml (PARKED/QUEUED/
 * RUNNING/RUNNING_WOKEN); the `noise` proctype is the always-enabled busy
 * peer that, absent fairness, can starve the hub forever.
 */

#define PARKED         0
#define QUEUED         1
#define RUNNING        2
#define RUNNING_WOKEN  3

int  state = PARKED;
int  qentries = 0;
bit  unserved = 0;        /* a wake was issued and not yet resumed */
bit  fired = 0;           /* the single waker has fired            */
bit  noise_phase = 0;     /* busy-peer progress marker (see noise) */

/* one wake, exactly like waker() in wake_state.pml */
proctype waker()
{
    atomic {
        unserved = 1;
        if
        :: (state == PARKED)        -> state = QUEUED; qentries++;
        :: (state == RUNNING)       -> state = RUNNING_WOKEN;
        :: (state == QUEUED)        -> skip;
        :: (state == RUNNING_WOKEN) -> skip;
        fi;
        fired = 1;
    }
}

/* the hub that owns this g: pulls QUEUED->RUNNING, runs, releases. */
proctype hub()
{
    do
    :: atomic {
           (state == QUEUED) ->
               state = RUNNING; qentries--;
               unserved = 0;          /* g got the CPU -> the wake is served */
       }
       atomic {
           if
           :: (state == RUNNING)       -> state = PARKED;
           :: (state == RUNNING_WOKEN) -> state = QUEUED; qentries++;
           fi;
       }
    :: atomic { (fired && state == PARKED && qentries == 0) -> break }
    od;
}

/*
 * The busy peer.  Always enabled, never terminates -- it models the rest
 * of the runtime making progress (other goroutines, other hubs spinning).
 * WITHOUT fairness Spin may schedule ONLY this process forever, starving
 * the hub: that is the acceptance cycle that fails the liveness property.
 * WEAK FAIRNESS forbids it (a continuously-enabled hub must eventually
 * run), which is precisely the scheduler guarantee runloom relies on.
 */
proctype noise()
{
    /* Alternates between two states (not a bare self-loop, which Spin
     * rejects in liveness mode) so it is a legitimate, fairly-schedulable
     * competitor that can run forever. */
    do
    :: (noise_phase == 0) -> noise_phase = 1
    :: (noise_phase == 1) -> noise_phase = 0
    od;
}

init {
    atomic {
        run waker();
        run hub();
        run noise();
    }
}

/* Non-starvation: a wake that is outstanding is eventually served. */
ltl non_starvation { [] ( (unserved == 1) -> <> (unserved == 0) ) }
