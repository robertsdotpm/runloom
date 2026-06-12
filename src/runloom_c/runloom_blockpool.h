/* runloom_blockpool.h -- blocking-offload thread pool.
 *
 * The problem: a fiber that makes a non-preemptible blocking C call
 * (the classic one is libc getaddrinfo) holds its hub's OS thread
 * hostage for the whole call.  I/O parks don't occupy a hub and
 * CPU-bound Python is time-sliced by the preemption thread, but a
 * blocking C call is invisible to both -- it strands every fiber
 * queued behind it on that hub (work-stealing can't reach a wedged
 * hub's local FIFO).
 *
 * The fix (Group A, "move the work off the hub"): run the blocking call
 * on a small dedicated pool of OS threads and PARK the calling fiber
 * until it finishes -- turning a hub-wedging blocking call into an
 * ordinary cooperative park.  The hub keeps scheduling other fibers
 * the whole time; only the pool threads block.  Pool size bounds the
 * concurrency of blocking calls (extra callers park on the job queue),
 * exactly like a resolver thread pool.
 *
 * The wake travels the same race-safe path as io_uring completions
 * (runloom_mn_wake_g on a hub, runloom_sched_wake_safe on the single-thread
 * scheduler), so a worker finishing before the caller has finished
 * parking is handled by the existing wake machinery.
 */
#ifndef RUNLOOM_BLOCKPOOL_H
#define RUNLOOM_BLOCKPOOL_H

/* Run fn(arg) on a blocking-offload pool thread, parking the current
 * fiber until it returns, and hand back fn's result.  fn runs on a
 * plain OS thread with NO GIL and must not touch Python objects (acquire
 * the GIL itself if it must).  If the caller is not inside a fiber,
 * or the pool can't be started, fn(arg) is run inline (still correct --
 * it just blocks the caller as before). */
void *runloom_blocking_call(void *(*fn)(void *), void *arg);

/* Lazily start the pool with n_workers threads (n_workers <= 0 -> a
 * sensible default, overridable via RUNLOOM_BLOCKPOOL_WORKERS).  Idempotent
 * and thread-safe; runloom_blocking_call calls it on first use.  Returns 0
 * on success (pool usable) or -1 (caller should run inline). */
int runloom_blockpool_init(int n_workers);

/* Stop the workers and free pool state.  Drains nothing -- callers must
 * have quiesced.  Mainly for the C test harness; the pool is otherwise a
 * process-lifetime singleton. */
void runloom_blockpool_fini(void);

/* Reset the pool in a forked child: the worker threads are gone, so mark it
 * "not started" (next offload re-creates it) and re-init bp_lock + drop the
 * inherited job queue.  Does NOT join the dead workers.  Single-thread child
 * only (called from the after-fork handler). */
void runloom_blockpool_reset_after_fork(void);

/* Number of offloaded jobs submitted but not yet completed.  The
 * single-thread scheduler's drain loop consults this so it neither exits
 * nor busy-spins while a fiber is parked waiting on the pool (such a
 * fiber has no netpoll/iouring footprint of its own). */
long runloom_blockpool_inflight(void);

#endif /* RUNLOOM_BLOCKPOOL_H */
