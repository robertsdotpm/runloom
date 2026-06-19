/* runloom_iframe.h -- suspended interpreter-frame chain walker.
 *
 * The public C API can read the TOP interpreter frame of a suspended
 * execution context (PyUnstable_InterpreterFrame_GetCode/GetLine) but
 * exposes no way to walk to the previous frame -- so a fiber dump
 * could only ever show one line of stack.  This tiny module reaches into
 * the internal frame layout (internal/pycore_frame.h) to walk the whole
 * chain, and is compiled as its OWN translation unit so the Py_BUILD_CORE
 * blast radius is contained here and nowhere else in the extension.
 *
 * The walk itself is unsafe in general (frames mutate as code runs); the
 * caller is responsible for stability -- runloom_introspect_frames claims the
 * fiber (M:N) or relies on single-thread ownership before calling. */
#ifndef RUNLOOM_IFRAME_H
#define RUNLOOM_IFRAME_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Called per Python frame, deepest-first.  `code` is a borrowed
 * PyCodeObject*, `line` its current source line.  Return non-zero to stop
 * the walk early. */
typedef int (*runloom_iframe_cb)(PyCodeObject *code, int line, void *ctx);

/* Walk the suspended interpreter-frame chain whose top is `top` (an opaque
 * struct _PyInterpreterFrame*, e.g. tstate->current_frame or a saved
 * snap->current_frame), deepest-first, skipping C-trampoline shim frames.
 * Visits at most `max` frames.  Returns the number visited.  No-op (0) if
 * top is NULL or the platform has no internal frame layout. */
int runloom_iframe_walk(void *top, int max, runloom_iframe_cb cb, void *ctx);

/* True when `ts` is currently executing CPython object-destruction machinery
 * that must not be interrupted by a fiber switch -- a tp_dealloc and its
 * weakref callbacks / finalizers, driven by either the trashcan unwind
 * (tstate->delete_later) or the free-threaded biased-refcount cross-thread
 * merge drain (brc.local_objects_to_merge).  Suspending a fiber here would
 * freeze a half-finished destructor on its coro stack while the hub thread
 * reaches a GC-safe point, letting a concurrent stop-the-world GC / QSBR
 * reclaim corrupt the partially-destroyed objects.  Reaches into internal
 * tstate layout, so it lives in this Py_BUILD_CORE-isolated TU.  Returns 0 on
 * pre-3.13 / non-core builds. */
int runloom_tstate_in_destruction(PyThreadState *ts);

/* Critical-section suspend/restore across a fiber swap (free-threaded
 * 3.13t only; no-op on GIL / pre-3.13 builds).  A fiber can park while
 * holding a CPython per-object critical section (e.g. a dict's ma_mutex held
 * during a key __eq__ that yields).  runloom runs many fibers on one hub
 * tstate and a cooperative park swaps the C stack WITHOUT detaching the
 * tstate, so unless we mirror CPython's detach-time behaviour the held mutex
 * stays locked while the g is parked -- every other hub then deadlocks on it,
 * and the shared tstate's critical-section chain leaks across fibers.
 *   suspend(): release all CS mutexes held on `tstate`, return the saved chain
 *              (tagged inactive) and clear tstate->critical_section.
 *   restore(): put the saved chain back and re-lock the top section.
 * These live in this Py_BUILD_CORE-isolated TU because _PyCriticalSection_*
 * are internal.  Args are void* PyThreadState to keep the core/non-core ABI
 * boundary clean.  See runloom_sched_pystate.c.inc (snap/load). */
uintptr_t runloom_critsec_suspend(void *tstate);
void      runloom_critsec_restore(void *tstate, uintptr_t saved);

/* EXPERIMENT (docs/dev/HUB_SCALING.md, A1b): make `op` immortal so its refcount
 * is frozen and never touched again.  Cross-hub incref/decref on a shared,
 * long-lived instance (the harness/channel objects every fiber calls into)
 * then become no-ops instead of _Py_TryIncRefShared / _Py_DecRefShared atomics
 * -- the dominant cross-hub cost the hub-scaling audit measured.  ONLY safe for
 * objects that live for the whole run (immortal objects are never freed).
 * No-op on pre-3.13 / non-core builds.  Lives in this Py_BUILD_CORE TU because
 * _Py_SetImmortal is internal. */
void runloom_immortalize(PyObject *op);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_IFRAME_H */
