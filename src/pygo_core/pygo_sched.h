/* sched.h -- C-level cooperative scheduler.
 *
 * The Python-side `pygo.go(fn)` ultimately creates a goroutine here.
 * yield, sleep, run -- all do their bookkeeping in C, calling into
 * Python only to invoke the user's entry function.
 *
 * Single OS thread per scheduler in v0.  Multi-thread is Phase C
 * (free-threaded Python with one scheduler per OS thread, work-stealing).
 */
#ifndef PYGO_SCHED_H
#define PYGO_SCHED_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "coro.h"

typedef struct pygo_g pygo_g_t;
typedef struct pygo_sched pygo_sched_t;

/* One goroutine (the "G" in Go's M:P:G nomenclature).
 *
 * Lifetime: refcounted.  Two parties hold refs:
 *   - the scheduler, while g is in the ready queue or sleep heap
 *   - the PygoG Python wrapper, while the user holds it
 * Both decrement on release; the g is freed when both are gone.
 */
struct pygo_g {
    pygo_coro_t *coro;
    PyObject *callable;
    PyObject *result;
    PyObject *error;
#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
    /* CPython 3.12: split recursion counters. */
    int py_recursion_remaining;
    int c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030D0000
    /* CPython 3.13+: layout changed again.  We snapshot the public
     * counter only. */
    int py_recursion_remaining;
    int c_recursion_remaining;
#else
    int recursion_depth;
#endif
    int snapshot_valid;
    double wake_at;
    pygo_g_t *next;
    int done;
    int refcount;
};

/* Lifetime helpers. */
void pygo_g_incref(pygo_g_t *g);
void pygo_g_decref(pygo_g_t *g);

/* Per-OS-thread scheduler. */
struct pygo_sched {
    /* Ready FIFO: head pops, tail appends. */
    pygo_g_t *ready_head;
    pygo_g_t *ready_tail;
    /* Currently-running g (for yield). */
    pygo_g_t *current;
    /* Sleep heap -- min-heap by wake_at.  Stored as a growable array
     * indexed 1..size; index 0 unused. */
    pygo_g_t **sleep_heap;
    Py_ssize_t sleep_size;
    Py_ssize_t sleep_cap;
    /* Default stack size for new gs. */
    Py_ssize_t stack_size;
    /* Goroutines completed since the last sched_drain. */
    Py_ssize_t completed;
    /* When set, sched_drain returns. */
    int stopping;
};

/* Module-level: one sched per OS thread once Phase C lands.  For now
 * a single global. */
pygo_sched_t *pygo_sched_get(void);

/* Spawn a new goroutine.  Returns a NEW reference to a PygoG Python
 * object (the wrapper around pygo_g_t).  Stealing the callable. */
PyObject *pygo_sched_spawn(pygo_sched_t *s, PyObject *callable);

/* Yield the current g.  Re-queues on the ready FIFO, swaps back to
 * the scheduler stack.  Must be called from inside a g. */
void pygo_sched_yield(pygo_sched_t *s);

/* Park the current g until wake_at (monotonic seconds).  Swap back. */
void pygo_sched_sleep_until(pygo_sched_t *s, double wake_at);

/* Mark current g as parked (no ready_push); netpoll/sleep saves snap.
 * Caller must then yield via pygo_coro_yield. */
void pygo_sched_park_current(void);

/* Re-queue a previously-parked g onto the ready list. */
void pygo_sched_wake(pygo_g_t *g);

/* Drive the scheduler until ready+sleep queues are empty.  Returns
 * the number of completed goroutines. */
Py_ssize_t pygo_sched_drain(pygo_sched_t *s);

/* Free all allocated state in the scheduler (does not destroy gs
 * still referenced by Python). */
void pygo_sched_init(pygo_sched_t *s);

#endif /* PYGO_SCHED_H */
