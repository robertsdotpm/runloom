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
#if PY_VERSION_HEX >= 0x030B0000
    /* 3.11+ common fields.  All of: contextvars, datastack arena
     * pointers, exc state, exist on 3.11/3.12/3.13. */
    PyObject *context;                       /* contextvars; owned ref */
    _PyStackChunk *datastack_chunk;
    PyObject **datastack_top;
    PyObject **datastack_limit;
    _PyErr_StackItem *exc_info;
    _PyErr_StackItem exc_state;
#endif
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
    /* 3.11: single recursion counter, named recursion_remaining. */
    int recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030C0000
    /* 3.12+: split into Python-level and C-level counters. */
    int py_recursion_remaining;
    int c_recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    /* 3.11 and 3.12: cframe lives on the C stack, threaded through
     * the linked list.  3.13 removed cframe; current_frame lives
     * directly on tstate instead. */
    _PyCFrame *cframe;
    int trash_delete_nesting;
#endif
#if PY_VERSION_HEX >= 0x030D0000
    /* 3.13+ fields. */
    struct _PyInterpreterFrame *current_frame;
    PyObject *delete_later;                  /* owned ref */
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
    /* Caller-asserted "this goroutine will never yield".  Spawned via
     * pygo_sched_spawn_noyield (Python: pygo_core.go_noyield(fn)).
     * When set, drain skips the per-g datastack install + drain +
     * sched_snap load/resnap dance, because g runs to completion
     * within one resume + uses the scheduler's existing Python state
     * without leaving anything behind.  Saves ~150-400 ns per g
     * lifetime depending on workload.
     *
     * If a noyield-marked g actually yields (calls sched_yield,
     * sched_sleep, wait_fd, or any monkey-patched I/O), the result
     * is undefined -- frames will alias across goroutines.  Use
     * only for pure-compute callables. */
    int noyield;
    /* Race-safe park/wake counter.  pygo_sched_park_safe decrements;
     * if >0, the wake already arrived and we skip the yield.
     * pygo_sched_wake_safe increments and (if g is currently parked)
     * adds it back to ready.  Used by pygo.aio's PygoTask to replace
     * the per-task Chan(1) wake channel with a much cheaper primitive
     * -- saves ~5 us per task at fan-out time. */
    int wake_pending;
};

/* Park current g until pygo_sched_wake_g(g) is called.  Race-safe:
 * a wake that arrives BEFORE the park (because the future fires
 * synchronously, e.g. add_done_callback on an already-done future)
 * makes the park a no-op and the goroutine continues. */
void pygo_sched_park_safe(void);

/* Wake a goroutine previously parked via pygo_sched_park_safe.  Safe
 * to call before park (wake_pending counter records the arrival). */
void pygo_sched_wake_safe(pygo_g_t *g);

/* Lifetime helpers. */
void pygo_g_incref(pygo_g_t *g);
void pygo_g_decref(pygo_g_t *g);

/* Slab allocator for pygo_g_t -- per-thread LIFO free list with cap.
 * Exposed so mn_sched.c can share the same recycle pool as the
 * single-thread spawn path.  alloc returns a zeroed g (or NULL +
 * PyErr_NoMemory on OOM); free returns to the slab. */
pygo_g_t *pygo_g_slab_alloc(void);
void pygo_g_slab_free(pygo_g_t *g);

/* Per-OS-thread scheduler. */
struct pygo_sched {
    /* Ready FIFO -- ring buffer of g pointers.  Previously a linked
     * list threaded through g->next, which meant every pop dereffed
     * a different (cache-cold) g struct just to read the next
     * pointer.  At 100k gs in flight that was the bottleneck on
     * spawn-heavy workloads.  Ring buffer keeps the queue itself in
     * a contiguous array (hot in L1 if it fits) and saves one cache
     * miss per push/pop. */
    pygo_g_t **ready_ring;            /* power-of-2 sized array */
    size_t    ready_cap;              /* power of 2 */
    size_t    ready_mask;             /* ready_cap - 1 */
    size_t    ready_head;             /* dequeue index (monotonic counter, mask to index) */
    size_t    ready_tail;             /* enqueue index */
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

/* Is the ready queue empty?  Hot-path predicate; inline-friendly. */
PYGO_INLINE int pygo_sched_ready_empty(const pygo_sched_t *s) {
    return s->ready_head == s->ready_tail;
}

/* Module-level: one sched per OS thread once Phase C lands.  For now
 * a single global. */
pygo_sched_t *pygo_sched_get(void);

/* Spawn a new goroutine.  Returns a NEW reference to a PygoG Python
 * object (the wrapper around pygo_g_t).  Stealing the callable. */
PyObject *pygo_sched_spawn(pygo_sched_t *s, PyObject *callable);

/* Spawn a goroutine marked as "noyield" -- caller asserts the
 * callable will run to completion without calling sched_yield,
 * sched_sleep, wait_fd, or any monkey-patched I/O.  The drain path
 * skips the per-g datastack install / drain / sched_snap load+
 * resnap dance, cutting ~150-400 ns / g lifetime depending on
 * workload.  Useful for CPU-bound parallel fan-out where you know
 * the handler is pure compute. */
PyObject *pygo_sched_spawn_noyield(pygo_sched_t *s, PyObject *callable);

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

/* Free the datastack-chunk chain owned by the just-completed goroutine.
 * Call AFTER pygo_coro_resume returns done=true and BEFORE loading any
 * other snapshot back into tstate (which would overwrite the chunk
 * pointers and leak the g's allocation).  Matches greenlet's did_finish.
 *
 * Returned chunks go to a per-thread pool (capped) so the next first-run
 * g can pick one up via pygo_first_run_install_datastack instead of
 * paying for an arena alloc. */
void pygo_drain_g_datastack(void);

/* Set up tstate->datastack_{chunk,top,limit} for a first-run g.  Pulls
 * a chunk off the per-thread pool if available; otherwise leaves the
 * fields NULL so PyEval will arena-allocate.  Either is correct. */
void pygo_first_run_install_datastack(void);

/* Sleep-heap helpers exposed for mn_sched.c's per-hub timer processing.
 * Single-thread drain still uses them via #define aliases. */
pygo_g_t *pygo_sched_sleep_peek(pygo_sched_t *s);
pygo_g_t *pygo_sched_sleep_pop(pygo_sched_t *s);

/* Monotonic clock used by the sleep heap.  Public so hub_main can
 * decide when sleepers are due. */
double pygo_sched_monotonic_seconds(void);

/* Time-sliced cooperative preemption (3.13t only).
 *
 * Start a timer thread that posts a Py_AddPendingCall every quantum_us
 * microseconds.  CPython's eval loop checks the pending queue at
 * bytecode back-edges and function calls; when our pending call fires,
 * it invokes pygo_sched_yield() on whichever goroutine is currently
 * running.  Lets goroutines without explicit sched_yield() calls still
 * cooperate -- the Go 1.14 model translated to CPython terms.
 *
 * Returns 0 on success, -1 on error (with a Python exception set).
 * Calling init while already running just updates the quantum.
 * Calling fini stops the timer and joins the thread.
 * Idempotent. */
int pygo_preempt_init(long quantum_us);
void pygo_preempt_fini(void);

#endif /* PYGO_SCHED_H */
