/* runloom_iframe.h -- suspended interpreter-frame chain walker.
 *
 * The public C API can read the TOP interpreter frame of a suspended
 * execution context (PyUnstable_InterpreterFrame_GetCode/GetLine) but
 * exposes no way to walk to the previous frame -- so a goroutine dump
 * could only ever show one line of stack.  This tiny module reaches into
 * the internal frame layout (internal/pycore_frame.h) to walk the whole
 * chain, and is compiled as its OWN translation unit so the Py_BUILD_CORE
 * blast radius is contained here and nowhere else in the extension.
 *
 * The walk itself is unsafe in general (frames mutate as code runs); the
 * caller is responsible for stability -- runloom_introspect_frames claims the
 * goroutine (M:N) or relies on single-thread ownership before calling. */
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
 * that must not be interrupted by a goroutine switch -- a tp_dealloc and its
 * weakref callbacks / finalizers, driven by either the trashcan unwind
 * (tstate->delete_later) or the free-threaded biased-refcount cross-thread
 * merge drain (brc.local_objects_to_merge).  Suspending a goroutine here would
 * freeze a half-finished destructor on its coro stack while the hub thread
 * reaches a GC-safe point, letting a concurrent stop-the-world GC / QSBR
 * reclaim corrupt the partially-destroyed objects.  Reaches into internal
 * tstate layout, so it lives in this Py_BUILD_CORE-isolated TU.  Returns 0 on
 * pre-3.13 / non-core builds. */
int runloom_tstate_in_destruction(PyThreadState *ts);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_IFRAME_H */
