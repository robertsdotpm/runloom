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

/* offsetof(PyGenObject, gi_exc_state); see runloom_iframe.c. */
size_t runloom_gen_exc_state_offset(void);

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

/* Per-fiber privatisation of _PyThreadStateImpl.c_stack_refs (free-threaded
 * 3.14+).  The shared per-hub tstate's c_stack_refs list head points at
 * _PyCStackRef nodes living on the RUNNING fiber's C stack; a fiber must not
 * leave its nodes linked into the shared tstate when it parks, or a sibling
 * fiber's stack reuse corrupts the list and the free-threaded GC SIGSEGVs while
 * walking it (gc_visit_thread_stacks; the p77_weakref_storm crash).
 *   take(): return the current head and clear it (hand the next fiber a clean,
 *           empty list).  set(): restore this fiber's saved head on resume.
 * void* head keeps the core/non-core ABI boundary clean.  No-op (returns
 * NULL / ignores) on non-FT or < 3.14, where the field does not exist.
 * See runloom_iframe.c and runloom_sched_pystate.c.inc (snap/load). */
void *runloom_tstate_take_cstack_refs(void *tstate);
void  runloom_tstate_set_cstack_refs(void *tstate, void *head);

/* EXPERIMENT (docs/dev/HUB_SCALING.md, A1b): make `op` immortal so its refcount
 * is frozen and never touched again.  Cross-hub incref/decref on a shared,
 * long-lived instance (the harness/channel objects every fiber calls into)
 * then become no-ops instead of _Py_TryIncRefShared / _Py_DecRefShared atomics
 * -- the dominant cross-hub cost the hub-scaling audit measured.  ONLY safe for
 * objects that live for the whole run (immortal objects are never freed).
 * No-op on pre-3.13 / non-core builds.  Lives in this Py_BUILD_CORE TU because
 * _Py_SetImmortal is internal. */
void runloom_immortalize(PyObject *op);

/* Per-g cross-hub migration fix: make `exec` (a per-g tstate) borrow `home`'s
 * (the running hub's) allocator -- mimalloc heap + qsbr/page-reclaim -- so the
 * per-g tstate carries no live heap and nothing migrates OS threads (the
 * _mi_page_retire crash that gates RUNLOOM_PER_G_TSTATE).  Requires the optional
 * CPython patch (patches/cpython313t-tstate-alloc-home.patch); compiled as a
 * no-op against stock CPython, so the call site is unconditional. */
void runloom_iframe_borrow_alloc_home(PyThreadState *exec, PyThreadState *home);

/* True iff this build can SAFELY migrate fibers across hubs: compiled against the
 * alloc-home CPython patch (Py_TSTATE_ALLOC_HOME) and not disabled at runtime
 * (RUNLOOM_NO_ALLOC_HOME).  The scheduler's migratable-mode interlock uses this as
 * the production gate -- when true, RUNLOOM_MIGRATION enables without the unsafe
 * override; against stock CPython it returns 0 and migration stays dev-gated.
 * Exposed to Python as runloom_c.alloc_home_available. */
int runloom_alloc_home_active(void);

#if PY_VERSION_HEX >= 0x030E0000
/* 3.14: arm the SP-based C-stack overflow check at fiber c's private stack, with
 * extra reserved headroom above the guard so a deep-recursion RecursionError
 * fires before CPython's datastack-chunk-alloc burst can dip into the guard page.
 * See runloom_iframe.c for the full rationale (the p212 SIGSEGV fix).  Forward-
 * declares runloom_coro so callers need not include coro.h. */
struct runloom_coro;
void runloom_arm_fiber_stackprot(PyThreadState *ts, struct runloom_coro *c);
#endif

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_IFRAME_H */
