/*
 * parked_safe.pml -- Promela model of the race-safe park_safe/wake_safe
 * handshake (pygo_sched_park_safe / pygo_sched_wake_safe in
 * src/pygo_core/pygo_sched.c).  This is the lost-wake guard used by
 * pygo.aio's PygoTask and the blocking-offload pool: a goroutine parks
 * on the single-thread scheduler while a wake may arrive concurrently
 * from another OS thread.
 *
 * The C protocol (verbatim structure):
 *
 *   wake_safe(g):                     park_safe():
 *     wake_pending++                    if wake_pending > 0:
 *     if CAS(parked_safe,1->0):            wake_pending--; return
 *        enqueue(g)                      parked_safe = 1            (release)
 *     // else: leave pending             if wake_pending > 0:
 *                                           if CAS(parked_safe,1->0):
 *                                              wake_pending--; return
 *                                           // else fall through to yield
 *                                        yield()    # resumed by enqueue
 *                                        wake_pending--
 *
 * Proven (NWAKERS=1, all interleavings):
 *   NO LOST WAKE -- the parker never blocks forever at the yield while a
 *      wake is outstanding.  Encoded as Spin's invalid-end-state check:
 *      the waker always fires, so if the parker could park-and-never-be-
 *      enqueued, that state is a deadlock and pan reports it.
 *   BALANCE      -- at quiescence wake_pending == 0 (the one wake is
 *      consumed exactly once) and the g is enqueued at most once
 *      (enqueued <= 1: no double wake / double schedule).
 *
 * Each shared access is its own statement (= one __atomic_* op); the
 * compare-exchange is an atomic{} block (indivisible read-compare-write).
 */

#define NWAKERS 1

int  wake_pending = 0;
bit  parked_safe  = 0;
int  enqueued     = 0;     /* times g pushed to the wake_list (made runnable) */
bit  parked       = 0;     /* parker is suspended at the yield                */
int  nfin         = 0;

active proctype parker()
{
    int cas;

    /* step 1: wake already arrived? eat one count, skip the park. */
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
        :: else -> skip;               /* lost CAS: waker owns us, fall to yield */
        fi;
    :: else -> skip;
    fi;

    /* step 4: yield.  Resumed only when a waker has enqueued us.  If the
     * wake were lost this blocks forever -> Spin invalid-end-state. */
    parked = 1;
    (enqueued > 0);
    parked = 0;
    wake_pending--;                    /* eat the delivering wake */

pdone:
    atomic {
        nfin++;
        if
        :: (nfin == NWAKERS + 1) -> assert(wake_pending == 0); assert(enqueued <= 1);
        :: else -> skip;
        fi;
    }
}

active [NWAKERS] proctype waker()
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
        :: (nfin == NWAKERS + 1) -> assert(wake_pending == 0); assert(enqueued <= 1);
        :: else -> skip;
        fi;
    }
}
