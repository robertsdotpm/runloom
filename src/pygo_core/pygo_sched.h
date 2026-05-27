/* pygo_sched.h -- C-level cooperative scheduler.
 *
 * The Python-side `pygo.go(fn)` ultimately creates a goroutine here.
 * yield, sleep, run -- all do their bookkeeping in C, calling into
 * Python only to invoke the user's entry function.
 *
 * Single OS thread per scheduler in v0.  Multi-thread is Phase C
 * (free-threaded Python with one scheduler per OS thread, work-stealing).
 *
 * Phase B (this file): per-goroutine snapshot of the CPython thread
 * state fields that a raw C-stack swap doesn't preserve.  Algorithm
 * copied from greenlet (MIT licensed; see TPythonState.cpp).
 */
#ifndef PYGO_SCHED_H
#define PYGO_SCHED_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "coro.h"

typedef struct pygo_g pygo_g_t;
typedef struct pygo_sched pygo_sched_t;
typedef struct pygo_pystate_snap pygo_pystate_snap_t;

/* Per-goroutine CPython thread state snapshot.
 *
 * Fields here are everything the interpreter keeps on PyThreadState that
 * a raw asm stack switch cannot preserve on its own.  Each save copies
 * them out of tstate into the snap; each load copies them back AND
 * transfers ownership (context, top_frame, delete_later) so the snap is
 * empty after a load.  Save and load must be balanced.
 *
 * Layout matches greenlet's PythonState/ExceptionState, transcribed to
 * C99 with #if PY_VERSION_HEX gates for 3.12 vs 3.13 vs older.  See
 * https://github.com/python-greenlet/greenlet src/greenlet/TPythonState.cpp.
 */
struct pygo_pystate_snap {
    int valid;
#if PY_VERSION_HEX >= 0x030C0000
    /* 3.12+ common fields. */
    PyObject *context;                       /* contextvars; owned ref */
    int py_recursion_remaining;
    int c_recursion_remaining;
    _PyStackChunk *datastack_chunk;
    PyObject **datastack_top;
    PyObject **datastack_limit;
    _PyErr_StackItem *exc_info;
    _PyErr_StackItem exc_state;
#endif
#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
    /* 3.12-only fields. */
    _PyCFrame *cframe;
    int trash_delete_nesting;
#endif
#if PY_VERSION_HEX >= 0x030D0000
    /* 3.13+ fields. */
    struct _PyInterpreterFrame *current_frame;
    PyObject *delete_later;                  /* owned ref */
#endif
#if PY_VERSION_HEX < 0x030C0000
    /* Legacy: pre-3.12 stored recursion depth as a single counter. */
    int recursion_depth;
#endif
};

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
    pygo_pystate_snap_t snap;     /* saved tstate; valid only when suspended */
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

/* Internal FIFO ops, exposed for reuse from mn_sched.c (hub-local
 * yielded-g queue piggybacks on the same singly-linked list). */
void pygo_sched_ready_push(pygo_sched_t *s, pygo_g_t *g);
pygo_g_t *pygo_sched_ready_pop(pygo_sched_t *s);

/* Snap/load primitives, exposed for mn_sched.c so hub_main can do the
 * same Phase B per-g state dance as the single-thread drain. */
void pygo_pystate_snap(pygo_pystate_snap_t *snap);
void pygo_pystate_load(pygo_pystate_snap_t *snap);
void pygo_pystate_snap_clear(pygo_pystate_snap_t *snap);

/* The user's callable trampoline for a goroutine; installs an initial
 * root cframe / current_frame on g's own stack, then runs g->callable.
 * Exposed so mn_sched.c can reuse the same entry (Phase B correct). */
void pygo_g_entry(void *user);

#endif /* PYGO_SCHED_H */
