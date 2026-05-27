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

/* Phase C: not yet implemented.  Functions exist so Python-side code
 * can reference them; they currently fall back to single-thread sched. */

int pygo_mn_init(int n_threads);
PyObject *pygo_mn_go(PyObject *callable);
Py_ssize_t pygo_mn_run(void);
void pygo_mn_fini(void);

#endif /* PYGO_MN_SCHED_H */
