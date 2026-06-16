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
#    include "internal/pycore_critical_section.h"  /* _PyCriticalSection_* */
#    define RUNLOOM_CRITSEC_HAVE 1
#  endif
#  define RUNLOOM_DESTRUCT_HAVE 1
#endif

/* See runloom_iframe.h.  Freezes op's refcount (immortal) so cross-hub
 * incref/decref become no-ops -- the A1b hub-scaling experiment lever.  We set
 * the immortal refcount fields directly (mirroring _Py_SetImmortalUntracked):
 * the real _Py_SetImmortal is internal-link-only (not exported from the shared
 * libpython), but _Py_IMMORTAL_REFCNT_LOCAL / _Py_UNOWNED_TID are public macros
 * in object.h.  Leaves the object GC-tracked (immortal objects are skipped by
 * the collector anyway), which is exactly what _Py_SetImmortalUntracked does. */
void runloom_immortalize(PyObject *op)
{
    if (op == NULL) {
        return;
    }
#if defined(Py_GIL_DISABLED)
    op->ob_tid       = _Py_UNOWNED_TID;
    op->ob_ref_local = _Py_IMMORTAL_REFCNT_LOCAL;
    op->ob_ref_shared = 0;
#elif defined(_Py_IMMORTAL_REFCNT)
    op->ob_refcnt    = _Py_IMMORTAL_REFCNT;
#endif
}

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

/* ---- critical-section suspend/restore across a fiber swap ----
 * See the header for why this is needed.  Mirrors what CPython does in
 * _PyThreadState_Detach / _Attach, but driven manually at runloom's park
 * boundary (runloom never detaches the tstate on a cooperative park). */
uintptr_t runloom_critsec_suspend(void *tstate_v)
{
#if defined(RUNLOOM_CRITSEC_HAVE)
    PyThreadState *ts = (PyThreadState *)tstate_v;
    uintptr_t saved = ts->critical_section;
    if (saved != 0) {
        /* Unlocks every CS mutex held on this tstate and tags the chain
         * inactive (chain pointer stays in ts->critical_section). */
        _PyCriticalSection_SuspendAll(ts);
        saved = ts->critical_section;   /* re-read: now tagged inactive */
        ts->critical_section = 0;       /* hand the next fiber a clean chain */
    }
    return saved;
#else
    (void)tstate_v;
    return 0;
#endif
}

void runloom_critsec_restore(void *tstate_v, uintptr_t saved)
{
#if defined(RUNLOOM_CRITSEC_HAVE)
    if (saved != 0) {
        PyThreadState *ts = (PyThreadState *)tstate_v;
        ts->critical_section = saved;
        /* Re-lock the top section (it was tagged inactive by SuspendAll).
         * Nested inner sections stay inactive until popped, each Pop resuming
         * the next -- exactly CPython's attach-time behaviour. */
        if (!_PyCriticalSection_IsActive(saved)) {
            _PyCriticalSection_Resume(ts);
        }
    }
#else
    (void)tstate_v; (void)saved;
#endif
}
