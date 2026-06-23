/*
 * cross_thread_wake.pml -- Promela model of the Phase C per-thread-scheduler
 * wake routing in runloom_sched_wake_safe (src/runloom_c/runloom_sched.c, commit
 * 4bef422).  runloom now runs ONE scheduler per OS thread; runloom.aio drives each
 * event loop on its own thread.  A goroutine records its owner sched at spawn
 * (g->owner).  When a FOREIGN thread wakes it -- a run_in_executor pool worker
 * or an io_uring CQE resolving a future the owner awaits -- wake_safe must
 * enqueue the g onto the OWNER sched's wake_list (the list the owner thread
 * drains), NOT the waker thread's list (which no one drains):
 *
 *     runloom_sched_t *s = g->owner ? g->owner : runloom_sched_get();   // route to owner
 *     ... enqueue g on s->wake_list ...                           // owner drains s
 *
 * This composes the verified park_safe/wake_safe handshake (parked_safe.pml --
 * the wake_pending counter + parked_safe CAS, UNCHANGED by Phase C) with the
 * new routing dimension: WHICH sched's wake_list the woken g lands on.
 *
 * PROVEN (a goroutine owned by + parked on the owner sched, woken by a foreign
 * thread):
 *   NO LOST WAKE -- the g is always resumed: either it consumed the pending
 *                   wake at park (the waker beat it), or it parked and the
 *                   OWNER's drain pulled it off the owner wake_list.  A lost
 *                   wake leaves the g blocked at its park forever (and the
 *                   owner drain idle) = a Spin invalid end state.
 *
 * Negative control -DBUG_ROUTE_TO_WAKER enqueues the woken g onto the WAKER
 * thread's wake_list (the pre-Phase-C behavior: wake_safe used the waker's own
 * runloom_sched_get()).  The owner's drain never sees it and the foreign waker
 * thread runs no drain loop, so Spin finds the lost wake -- exactly the
 * concurrent-loop deadlock Phase C fixes.
 */

bit wake_pending  = 0;   /* g->wake_pending (park_safe/wake_safe counter)   */
bit parked_safe   = 0;   /* g->parked_safe (the CAS handoff)                */
bit owner_wl      = 0;   /* g enqueued on the OWNER sched's wake_list       */
bit waker_wl      = 0;   /* g enqueued on the WAKER thread's wake_list (bug)*/
bit g_resumed     = 0;   /* g is runnable again (resumed, or skipped park)  */

/* The goroutine: runs on its OWNER thread, parks via runloom_sched_park_safe. */
active proctype g_park()
{
    bit consumed;
    atomic {
        if
        :: wake_pending == 1 -> wake_pending = 0; consumed = 1;  /* wake beat us: no yield */
        :: else              -> parked_safe = 1; consumed = 0;   /* mark parked, then yield */
        fi;
    }
    if
    :: consumed -> g_resumed = 1;    /* consumed the wake; runnable without yielding */
    :: else     -> (g_resumed);      /* yield; the OWNER drain must resume us */
    fi;
}

/* A foreign-thread waker: runloom_sched_wake_safe(g). */
active proctype waker()
{
    atomic { wake_pending = 1; }     /* we hold the wake */
    atomic {
        if
        :: parked_safe == 1 ->       /* g is parked: we own the enqueue */
            parked_safe = 0;
#ifndef BUG_ROUTE_TO_WAKER
            owner_wl = 1;            /* Phase C: route to g->owner's wake_list */
#else
            waker_wl = 1;            /* BUG (pre-Phase-C): route to the waker's list */
#endif
        :: else -> skip;             /* not parked yet: g consumes wake_pending at park */
        fi;
    }
}

/* The OWNER sched's drain loop, on the owner thread (cooperative with g_park).
 * It drains only the OWNER wake_list -- it cannot see the waker thread's list. */
active proctype owner_drain()
{
    do
    :: atomic { (owner_wl == 1) -> owner_wl = 0; g_resumed = 1; }
    :: (g_resumed) -> break;         /* g is runnable; nothing left to do */
    od;
}
