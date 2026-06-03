/* mn_sched.h -- M:N scheduler skeleton for Phase C.
 *
 * Target: free-threaded Python 3.13t.  N OS threads, each owning a
 * scheduler hub; goroutines created on any thread go into a hub's
 * local ring queue.  When a hub's ready queue is empty, it tries to
 * steal from a neighbouring hub's queue tail (Chase-Lev work-stealing
 * deque).  Multiple hubs run Python code in parallel because the
 * GIL is gone in free-threaded builds.
 *
 *   pygo_mn_init(n_threads)      start N OS threads, each with a hub
 *   pygo_mn_go(callable)         spawn on the calling thread's hub
 *                                (or, if not in a hub, round-robin)
 *   pygo_mn_run()                join all hubs after their queues drain
 *   pygo_mn_fini()               teardown
 *
 * Design notes (NOT IMPLEMENTED YET -- this header is the spec):
 *
 *   Run queue per hub: Chase-Lev deque.  Owner pushes/pops the tail
 *   (lock-free); thieves pop the head with CAS.  Standard work-
 *   stealing primitive; ~150 LoC of careful atomics in C.
 *
 *   Global goroutine pool: thread-safe stack of fresh G structs so
 *   pygo_mn_go from outside any hub can place a g without contending.
 *
 *   Sleep heap: still per-hub.  Sleep duration includes a check for
 *   cross-hub wakeups (no -- gs cannot migrate; sleep is hub-local).
 *
 *   Netpoll: one epoll_fd shared across hubs; each hub adds parks to
 *   it.  pump() runs in any hub when its local queue is empty and
 *   wakes whichever hub's g was parked.
 *
 *   Goroutine pinning: a g is created on a hub and runs ONLY on that
 *   hub.  Greenlets / our coros have absolute stack pointers that
 *   tie them to a single OS thread.  Migration would need to suspend,
 *   re-create on the target thread, restore -- doable but adds
 *   overhead Go doesn't pay.  Work-stealing here actually steals
 *   READY goroutines (which haven't run yet, so no stack to migrate)
 *   rather than active ones.
 *
 *   Wake interrupts: when a hub steals work, it needs to inform other
 *   hubs that may be sleeping in epoll_wait.  Use eventfd / pipe
 *   per hub.
 */
#ifndef PYGO_MN_SCHED_H
#define PYGO_MN_SCHED_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pygo_sched.h"   /* for pygo_g_t forward */

/* Forward-decl avoids pulling io_uring.h into every translation unit
 * that includes mn_sched.h. */
struct pygo_iouring_ring;

int pygo_mn_init(int n_threads);
PyObject *pygo_mn_go(PyObject *callable);
/* C-only spawn: no Python callable, just a function + arg.  Distributes
 * goroutines across hubs round-robin (same as pygo_mn_go).  Returns 0 on
 * success, -1 with errno on failure (ENOMEM, EINVAL). */
int pygo_mn_go_c(pygo_c_entry_fn fn, void *arg);
Py_ssize_t pygo_mn_run(void);
void pygo_mn_fini(void);

/* Reset the M:N scheduler in a forked child (the hub threads are gone).
 * Abandons the inherited hubs, zeroes the pending counter so pygo_mn_run
 * can't hang on dead hubs, and re-inits the global run-queue lock.  After
 * this mn_hub_count()==0 and a fresh pygo_mn_init() works.  Single-thread
 * child only (called from the after-fork handler). */
void pygo_mn_reset_after_fork(void);

/* Logical clock for the controlled-replay scheduler (PYGO_MN_SEED + barrier).
 * Returns the deterministic logical time that sched_sleep deadlines and timer
 * firing are measured against; `fallback` (a wall-clock value) is returned when
 * controlled mode is off, so callers stay wall-clock in production. */
double pygo_mn_logical_now_or(double fallback);

/* Phase C v2 hook.  Called from pygo_sched_yield to give the M:N
 * scheduler a chance to handle the yield in hub context.  Returns
 * 1 if we're inside a hub and the yield was handled (g re-queued on
 * the hub's local FIFO, state snapped, asm-yield done, control will
 * return when hub re-resumes g).  Returns 0 if we're not in a hub
 * and the caller should fall through to the single-thread sched path. */
int pygo_mn_yield_current(void);

/* Returns the number of M:N hubs currently running (0 if mn_init was
 * never called or after mn_fini). */
int pygo_mn_hub_count(void);

/* Return an opaque handle to the hub running on this thread (or NULL
 * if the calling thread isn't a hub).  Used by netpoll to record where
 * to route a parked g when it becomes ready. */
void *pygo_mn_current_hub_opaque(void);

/* Map a hub_opaque (as returned by pygo_mn_current_hub_opaque, or
 * stashed on a parker/g) to the dense 0..hub_count-1 hub id.  Returns
 * -1 for NULL (single-thread sched).  Used by netpoll's per-hub
 * parker pool selector to look up the right pool. */
int pygo_mn_hub_id_of(void *hub_opaque);

/* Return the goroutine currently running on this thread's hub (or
 * NULL if not in a hub or no g is running).  Netpoll's wait_fd uses
 * this -- it can't read pygo_sched_t::current because that's the
 * single-thread sched's slot, not the per-hub slot. */
pygo_g_t *pygo_mn_tls_current_g(void);

/* Signal hub_main "don't requeue the current g on return" -- used by
 * the park path (netpoll, channels) where the parker takes ownership
 * and arranges its own wake.  Without this, hub_main's "g yielded but
 * didn't self-queue, must be a raw yield" fallback re-pushes the g to
 * the local FIFO and the next iteration re-runs it -> busy loop. */
void pygo_mn_tls_mark_parked(void);

/* Return the pygo_sched_t owned by the hub running on this thread, or
 * NULL if not in a hub.  Used by hub-aware sched primitives (e.g.,
 * sleep_until) so they push to the hub's per-thread sleep heap rather
 * than the global single-thread heap. */
pygo_sched_t *pygo_mn_current_sched(void);

/* Wake g back to its original hub (or to the global single-thread
 * sched if hub_opaque is NULL).  Thread-safe; can be called from any
 * thread (typically netpoll pump on whichever hub did epoll_wait).
 * For hubs: pushes onto the target hub's submission list under
 * sub_lock; hub_main drains submissions each iteration and dispatches
 * routes them to the deque (if g is fresh) or local FIFO (if yielded). */
void pygo_mn_wake_g(void *hub_opaque, pygo_g_t *g);

/* Idle-stack-sweep handshake for PYGO_PER_G_TSTATE (no-op-safe to call in
 * either mode; the sweep caller gates them on per-g-tstate).  try_claim CASes
 * the g's wake_state PARKED -> SWEEPING and returns 1 if it won exclusive
 * ownership of the g's stack for an MADV_DONTNEED, 0 if the g was concurrently
 * woken/owned (skip it).  claim_release ends that ownership: SWEEPING -> PARKED,
 * or, if a wake landed during the madvise, SWEEPING_WOKEN -> QUEUED and
 * re-enqueues it onto the global run-queue exactly once (so the deferred wake is
 * never lost).  See the wake_state field comment in pygo_sched.h. */
int  pygo_mn_sweep_try_claim(pygo_g_t *g);
void pygo_mn_sweep_claim_release(pygo_g_t *g);

/* The current hub's per-thread io_uring ring (NULL if not in a hub,
 * or the hub failed to create its ring at startup -- callers should
 * fall back to the global ring path).  Used by pygo_iouring_recv /
 * _send to dispatch to the hub's SINGLE_ISSUER ring instead of the
 * global ring's mutex-protected submit + legacy spin-drain. */
struct pygo_iouring_ring *pygo_mn_current_iouring_ring(void);

#endif /* PYGO_MN_SCHED_H */
