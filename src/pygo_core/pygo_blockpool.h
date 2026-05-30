/* pygo_blockpool.h -- blocking-offload thread pool.
 *
 * The problem: a goroutine that makes a non-preemptible blocking C call
 * (the classic one is libc getaddrinfo) holds its hub's OS thread
 * hostage for the whole call.  I/O parks don't occupy a hub and
 * CPU-bound Python is time-sliced by the preemption thread, but a
 * blocking C call is invisible to both -- it strands every goroutine
 * queued behind it on that hub (work-stealing can't reach a wedged
 * hub's local FIFO).
 *
 * The fix (Group A, "move the work off the hub"): run the blocking call
 * on a small dedicated pool of OS threads and PARK the calling goroutine
 * until it finishes -- turning a hub-wedging blocking call into an
 * ordinary cooperative park.  The hub keeps scheduling other goroutines
 * the whole time; only the pool threads block.  Pool size bounds the
 * concurrency of blocking calls (extra callers park on the job queue),
 * exactly like a resolver thread pool.
 *
 * The wake travels the same race-safe path as io_uring completions
 * (pygo_mn_wake_g on a hub, pygo_sched_wake_safe on the single-thread
 * scheduler), so a worker finishing before the caller has finished
 * parking is handled by the existing wake machinery.
 */
#ifndef PYGO_BLOCKPOOL_H
#define PYGO_BLOCKPOOL_H

/* Run fn(arg) on a blocking-offload pool thread, parking the current
 * goroutine until it returns, and hand back fn's result.  fn runs on a
 * plain OS thread with NO GIL and must not touch Python objects (acquire
 * the GIL itself if it must).  If the caller is not inside a goroutine,
 * or the pool can't be started, fn(arg) is run inline (still correct --
 * it just blocks the caller as before). */
void *pygo_blocking_call(void *(*fn)(void *), void *arg);

/* Lazily start the pool with n_workers threads (n_workers <= 0 -> a
 * sensible default, overridable via PYGO_BLOCKPOOL_WORKERS).  Idempotent
 * and thread-safe; pygo_blocking_call calls it on first use.  Returns 0
 * on success (pool usable) or -1 (caller should run inline). */
int pygo_blockpool_init(int n_workers);

/* Stop the workers and free pool state.  Drains nothing -- callers must
 * have quiesced.  Mainly for the C test harness; the pool is otherwise a
 * process-lifetime singleton. */
void pygo_blockpool_fini(void);

/* Number of offloaded jobs submitted but not yet completed.  The
 * single-thread scheduler's drain loop consults this so it neither exits
 * nor busy-spins while a goroutine is parked waiting on the pool (such a
 * goroutine has no netpoll/iouring footprint of its own). */
long pygo_blockpool_inflight(void);

#endif /* PYGO_BLOCKPOOL_H */
