/* module.c -- Python bindings for runloom_c.
 *
 * Exposes:
 *   runloom_c.Coro(callable, stack_size=131072) -> coro object
 *      .resume()         switch into the coroutine
 *      .done             True if entry returned
 *   runloom_c.yield_()   yield from inside a coroutine
 *   runloom_c.backend()  "fibers" | "ucontext"
 *
 * Free-threaded friendly: each OS thread runs its own coroutines.
 * We do NOT release the GIL during resume() because the Python callable
 * we run inside the coro will reacquire/release as it pleases.  Under
 * free-threaded Python (3.13t) there is no global lock to release.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_VERSION_HEX < 0x030B0000
#  error "runloom requires CPython 3.11 or later -- the Phase B per-g \
PyThreadState snapshot uses 3.11+ tstate fields (cframe, \
datastack_chunk).  3.10 and earlier had a fundamentally different \
frame model (PyFrameObject linked list) and would need separate \
snap/load paths; not built today."
#endif

/* PyDict_GetItemRef arrived in CPython 3.13.  On 3.11/3.12 provide the same
 * contract (return 1 found / 0 missing / -1 error; *result = a new strong
 * reference or NULL) on top of the borrowed-ref PyDict_GetItemWithError, so
 * the fiber-safe module __getattr__ lookup builds on every target. */
#if PY_VERSION_HEX < 0x030D0000
static inline int PyDict_GetItemRef(PyObject *d, PyObject *key, PyObject **result)
{
    PyObject *value = PyDict_GetItemWithError(d, key);   /* borrowed */
    if (value != NULL) {
        *result = Py_NewRef(value);
        return 1;
    }
    *result = NULL;
    return PyErr_Occurred() ? -1 : 0;
}
#endif

#include "plat.h"
#include "plat_compat.h"
#include "coro.h"
#include "runloom_sched.h"
#include "netpoll.h"
#include "io_uring.h"   /* runloom_iouring_cancel_g for the cancel path */
#include <stdlib.h>   /* getenv -- the test-only fd-fault guard below */
#include "mn_sched.h"
#include "chan.h"
#include "runloom_tcp.h"
#include "runloom_blockpool.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_introspect.h"
#include "runloom_crash.h"
#include "runloom_stackadvice.h"

/* ---- Per-coro Python object ---- */

/* CPython thread-state snapshot.  These fields are not preserved by a
 * raw C-stack swap, but Python frame chain + recursion counters live
 * on the thread state and need to follow the coroutine.  We snapshot
 * what we can portably:
 *  - py_recursion_remaining / c_recursion_remaining (3.12+)
 *  - recursion_depth (older 3.x)
 * Other fields like the topmost frame chain are still UB territory and
 * are why we don't run unittest harness frames over yields. */
typedef struct {
#if PY_VERSION_HEX >= 0x030C0000
    int py_recursion_remaining;
    int c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    int recursion_remaining;
#else
    int recursion_depth;
#endif
    int initialised;
} RunloomTstateSnapshot;

typedef struct {
    PyObject_HEAD
    runloom_coro_t *coro;
    PyObject *callable;   /* invoked once when the coro first resumes */
    PyObject *result;     /* return value of callable, or NULL */
    PyObject *error;      /* unhandled exception caught, or NULL */
    int has_run;
    int executing;        /* 1 while inside runloom_coro_resume (re-entrancy guard) */
    RunloomTstateSnapshot tstate_snap;  /* captured at yield, restored at resume */
} RunloomCoro;

RUNLOOM_INLINE void runloom_tstate_save(RunloomTstateSnapshot *s)
{
    PyThreadState *ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    s->py_recursion_remaining = ts->py_recursion_remaining;
    s->c_recursion_remaining = ts->c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    s->recursion_remaining = ts->recursion_remaining;
#else
    s->recursion_depth = ts->recursion_depth;
#endif
    s->initialised = 1;
}

RUNLOOM_INLINE void runloom_tstate_restore(const RunloomTstateSnapshot *s)
{
    PyThreadState *ts;
    if (!s->initialised) {
        return;
    }
    ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    ts->py_recursion_remaining = s->py_recursion_remaining;
    ts->c_recursion_remaining = s->c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    ts->recursion_remaining = s->recursion_remaining;
#else
    ts->recursion_depth = s->recursion_depth;
#endif
}


/* ---------------------------------------------------------------------------
 * module.c is split across the module_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only module.c.
 * --------------------------------------------------------------------------- */
#include "module_coro.c.inc"
#include "module_tcp.c.inc"
#include "module_io.c.inc"
#include "module_fdio.c.inc"
#include "module_g.c.inc"
#include "module_chan.c.inc"
#include "module_go.c.inc"
#include "module_run.c.inc"
#include "module_introspect.c.inc"
#include "module_crash.c.inc"
#include "module_advice.c.inc"
#include "module_select.c.inc"
#include "module_machinecode.c.inc"
#include "module_init.c.inc"
