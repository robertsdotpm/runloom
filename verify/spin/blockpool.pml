/*
 * blockpool.pml -- Promela model of the blocking-offload wake ordering in
 * src/runloom_c/runloom_blockpool.c (runloom_blocking_call + worker), the path
 * runloom.blocking / DNS resolution take by default on Linux.
 *
 * A goroutine offloads a blocking fn to a worker thread and parks.  The
 * single-thread scheduler's drain loop blocks in epoll_wait with no
 * timeout, so it cannot poll; an `inflight` counter keeps it alive while a
 * job is outstanding (a park_safe'd goroutine has no netpoll/iouring
 * footprint of its own).  The worker, when done, must RE-QUEUE the
 * goroutine BEFORE decrementing inflight -- so that the moment inflight
 * hits 0 the drain loop already sees the goroutine on its wake list.
 *
 * Proven:
 *   NO LOST WAKE  the offloaded goroutine is always resumed; the drain
 *                 loop never exits (inflight==0 && ready empty) leaving it
 *                 parked.  Encoded as Spin's invalid-end-state check: the
 *                 caller blocks on its resume, so a lost wake is a
 *                 deadlock pan reports.
 *   RESUMED ONCE  the goroutine is resumed exactly once (no double).
 *
 * Negative control: -DBUG_DEC_BEFORE_REQUEUE decrements inflight BEFORE
 * re-queuing (the wrong order) -> the drain loop can observe inflight==0
 * with the goroutine not yet queued, exit, and strand it -> deadlock.
 */

int inflight   = 0;     /* jobs submitted, not yet completed         */
bit job_ready  = 0;     /* a job is in the queue for the worker      */
bit ready      = 0;     /* the goroutine is back on the wake list    */
bit g_resumed  = 0;     /* the drain loop resumed the goroutine      */
bit parked     = 0;     /* caller has incremented inflight + parked  */

/* runloom_blocking_call: count the job, enqueue it, then park.  The caller
 * runs INSIDE the drain loop (it is a goroutine the drain resumed), so
 * inflight++ happens-before the drain re-evaluates its exit condition --
 * the `parked` gate on the drain models that. */
active proctype caller()
{
    atomic { inflight++; }          /* counted BEFORE enqueue (drain stays alive) */
    atomic { job_ready = 1; }       /* hand the job to the pool */
    atomic { parked = 1; }          /* yield back to the drain, now parked */
    (g_resumed == 1);               /* park_safe: blocks until the drain resumes us.
                                     * If the wake is lost this never unblocks
                                     * -> invalid end state (lost wake). */
}

/* One pool worker: run the job off-thread, then wake the goroutine. */
active proctype worker()
{
    (job_ready == 1);
    atomic { job_ready = 0; }       /* dequeue + run the blocking fn */
#ifdef BUG_DEC_BEFORE_REQUEUE
    atomic { inflight--; }          /* BUG: drop the keep-alive first ... */
    atomic { ready = 1; }           /* ... then re-queue (too late if drain exited) */
#else
    atomic { ready = 1; }           /* re-queue the goroutine FIRST ... */
    atomic { inflight--; }          /* ... then drop the keep-alive */
#endif
}

/* The single-thread scheduler drain loop. */
active proctype drain()
{
    (parked == 1);                      /* the drain only regains control once the
                                         * caller goroutine has parked (inflight>=1) */
    do
    :: atomic {
           (ready == 1) ->                  /* a woken g on the wake list */
           ready = 0;
           assert(g_resumed == 0);          /* RESUMED ONCE */
           g_resumed = 1;
       }
    :: atomic {
           /* exit only when nothing is runnable AND no job is outstanding */
           (ready == 0 && inflight == 0) -> break;
       }
    od;
}
