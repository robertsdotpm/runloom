/* runloom_iframe.c -- the ONLY translation unit that reaches into CPython's
 * internal interpreter-frame layout.  Kept separate so Py_BUILD_CORE_MODULE
 * (and the internal headers it unlocks) never leak into the rest of the
 * build.  See runloom_iframe.h. */

#if PY_VERSION_HEX == 0   /* never true; just to silence "no PY_VERSION_HEX yet" */
#endif

/* internal/pycore_frame.h requires the core-build macro. */
#ifndef Py_BUILD_CORE_MODULE
#  define Py_BUILD_CORE_MODULE 1
#endif

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "runloom_iframe.h"

#if PY_VERSION_HEX >= 0x030D0000 && !defined(RUNLOOM_NO_IFRAME)
#  include "internal/pycore_frame.h"
#  define RUNLOOM_IFRAME_HAVE 1
#endif

#if PY_VERSION_HEX >= 0x030D0000 && !defined(RUNLOOM_NO_IFRAME)
#  include "internal/pycore_pystate.h"     /* _PyThreadStateImpl */
#  ifdef Py_GIL_DISABLED
#    include "internal/pycore_brc.h"        /* struct _brc_thread_state */
#  endif
#  define RUNLOOM_DESTRUCT_HAVE 1
#endif

int runloom_tstate_in_destruction(PyThreadState *ts)
{
#if defined(RUNLOOM_DESTRUCT_HAVE)
    if (ts == NULL) {
        return 0;
    }
    /* Trashcan chain mid-unwind: objects whose tp_dealloc was deferred because
     * the C-recursion ran low are being destroyed by _PyTrash_thread_destroy_
     * chain.  Non-NULL for the whole unwind. */
    if (ts->delete_later != NULL) {
        return 1;
    }
#  ifdef Py_GIL_DISABLED
    /* Biased-refcount cross-thread merge is draining: merge_queued_objects is
     * popping this per-thread stack and calling tp_dealloc (-> weakref
     * callbacks / finalizers) on each.  Non-empty => a destructor is in flight
     * on this tstate.  (objects_to_merge, the shared inbound queue, is NOT
     * checked: it only means work is *pending*, not that a destructor is
     * currently executing -- gating on it would needlessly throttle
     * preemption.) */
    if (((_PyThreadStateImpl *)ts)->brc.local_objects_to_merge.head != NULL) {
        return 1;
    }
#  endif
    return 0;
#else
    (void)ts;
    return 0;
#endif
}

int runloom_iframe_walk(void *top, int max, runloom_iframe_cb cb, void *ctx)
{
#if defined(RUNLOOM_IFRAME_HAVE)
    _PyInterpreterFrame *f = (_PyInterpreterFrame *)top;
    int n = 0;
    while (f != NULL && n < max) {
        /* Skip the C-stack trampoline shim frames that bracket a real
         * call; they carry no user code. */
        if (f->owner != FRAME_OWNED_BY_CSTACK) {
            PyObject *exec = f->f_executable;
            if (exec != NULL && PyCode_Check(exec)) {
                int line = PyUnstable_InterpreterFrame_GetLine(f);
                if (cb((PyCodeObject *)exec, line, ctx) != 0)
                    return n;
                n++;
            }
        }
        f = f->previous;
    }
    return n;
#else
    (void)top; (void)max; (void)cb; (void)ctx;
    return 0;
#endif
}
