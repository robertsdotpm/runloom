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
