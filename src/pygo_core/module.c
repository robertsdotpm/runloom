/* module.c -- Python bindings for pygo_core.
 *
 * Exposes:
 *   pygo_core.Coro(callable, stack_size=131072) -> coro object
 *      .resume()         switch into the coroutine
 *      .done             True if entry returned
 *   pygo_core.yield_()   yield from inside a coroutine
 *   pygo_core.backend()  "fibers" | "ucontext"
 *
 * Free-threaded friendly: each OS thread runs its own coroutines.
 * We do NOT release the GIL during resume() because the Python callable
 * we run inside the coro will reacquire/release as it pleases.  Under
 * free-threaded Python (3.13t) there is no global lock to release.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_VERSION_HEX < 0x030B0000
#  error "pygo requires CPython 3.11 or later -- the Phase B per-g \
PyThreadState snapshot uses 3.11+ tstate fields (cframe, \
datastack_chunk).  3.10 and earlier had a fundamentally different \
frame model (PyFrameObject linked list) and would need separate \
snap/load paths; not built today."
#endif

#include "plat.h"
#include "plat_compat.h"
#include "coro.h"
#include "pygo_sched.h"
#include "netpoll.h"
#include <stdlib.h>   /* getenv -- the test-only fd-fault guard below */
#include "mn_sched.h"
#include "chan.h"
#include "pygo_tcp.h"
#include "pygo_blockpool.h"
#include "pygo_diag.h"

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
} PygoTstateSnapshot;

typedef struct {
    PyObject_HEAD
    pygo_coro_t *coro;
    PyObject *callable;   /* invoked once when the coro first resumes */
    PyObject *result;     /* return value of callable, or NULL */
    PyObject *error;      /* unhandled exception caught, or NULL */
    int has_run;
    PygoTstateSnapshot tstate_snap;  /* captured at yield, restored at resume */
} PygoCoro;

PYGO_INLINE void pygo_tstate_save(PygoTstateSnapshot *s)
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

PYGO_INLINE void pygo_tstate_restore(const PygoTstateSnapshot *s)
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
#include "module_fdio.c.inc"
#include "module_g.c.inc"
#include "module_chan.c.inc"
#include "module_go.c.inc"
#include "module_run.c.inc"
#include "module_select.c.inc"
#include "module_init.c.inc"
