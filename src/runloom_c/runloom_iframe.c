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
#include "coro.h"    /* runloom_coro_stack_base/size (fiber C-stack geometry) */

#if PY_VERSION_HEX >= 0x030D0000 && !defined(RUNLOOM_NO_IFRAME)
#  include "internal/pycore_frame.h"
#  if PY_VERSION_HEX >= 0x030E0000
/* 3.14 moved the complete _PyInterpreterFrame struct + FRAME_OWNED_BY_CSTACK out
 * of pycore_frame.h (now only a forward declaration) into pycore_interpframe.h. */
#    include "internal/pycore_interpframe.h"
#  endif
#  define RUNLOOM_IFRAME_HAVE 1
#endif

#if PY_VERSION_HEX >= 0x030D0000 && !defined(RUNLOOM_NO_IFRAME)
#  include "internal/pycore_pystate.h"     /* _PyThreadStateImpl */
#  ifdef Py_GIL_DISABLED
#    include "internal/pycore_brc.h"        /* struct _brc_thread_state */
#    include "internal/pycore_critical_section.h"  /* _PyCriticalSection_* */
#    include "internal/pycore_tstate.h"   /* _PyThreadState_SetAllocHome (Py_TSTATE_ALLOC_HOME) */
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

/* Borrow `home`'s allocator (mimalloc heap + qsbr/page-reclaim) for `exec` --
 * the per-g cross-hub migration fix.  With the optional CPython patch
 * (Py_TSTATE_ALLOC_HOME, see patches/) a per-g tstate carries no live heap and
 * allocates on whichever hub is running it, so no per-fiber heap ever migrates
 * OS threads -- removing the _mi_page_retire teardown corruption that gates the
 * per-g-tstate mode.  Compiled as a no-op when built against stock CPython. */
/* True iff this build can run per-g cross-hub migration SAFELY: it was compiled
 * against the alloc-home CPython patch (Py_TSTATE_ALLOC_HOME) AND the borrow is
 * not disabled at runtime (RUNLOOM_NO_ALLOC_HOME=1).  When true, a per-g tstate
 * borrows the running hub's heap, so no per-fiber heap migrates OS threads --
 * this is the PRODUCTION safety gate the scheduler checks before enabling
 * RUNLOOM_MIGRATION without the unsafe-override.  Returns 0 against stock
 * CPython, so on an unpatched interpreter migration stays gated behind the
 * explicit RUNLOOM_ALLOW_UNSAFE_MIGRATION dev escape hatch. */
int runloom_alloc_home_active(void)
{
#if defined(Py_GIL_DISABLED) && defined(Py_TSTATE_ALLOC_HOME)
    /* RUNLOOM_NO_ALLOC_HOME=1 disables the borrow (A/B baseline: reproduces the
     * pre-patch per-g-tstate _mi_page_retire crash).  Default = borrow ON. */
    static int off = -1;
    if (off < 0) { const char *e = getenv("RUNLOOM_NO_ALLOC_HOME"); off = (e && e[0] == '1'); }
    return !off;
#else
    return 0;
#endif
}

void runloom_iframe_borrow_alloc_home(PyThreadState *exec, PyThreadState *home)
{
#if defined(Py_GIL_DISABLED) && defined(Py_TSTATE_ALLOC_HOME)
    if (runloom_alloc_home_active()) {
        _PyThreadState_SetAllocHome(exec, home);
    }
#else
    (void)exec; (void)home;
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
#if PY_VERSION_HEX >= 0x030E0000
            /* 3.14: f_executable is a tagged _PyStackRef, not a PyObject*. */
            PyObject *exec = PyStackRef_AsPyObjectBorrow(f->f_executable);
#else
            PyObject *exec = f->f_executable;
#endif
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

#if PY_VERSION_HEX >= 0x030E0000
/* Arm the live tstate's SP-based C-stack overflow check (3.14) at THIS fiber's
 * private mmap stack, with EXTRA reserved headroom above the hardware guard.
 *
 * 3.14 replaced the integer C-recursion counter with an SP check:
 * PyUnstable_ThreadState_SetStackProtection(ts, base, size) sets
 *   soft_limit = base + 2*MARGIN,  hard_limit = base + MARGIN
 * (stack grows down; _Py_MakeRecCheck raises RecursionError once SP descends
 * below soft_limit).  MARGIN is _PyOS_STACK_MARGIN_BYTES = 16 KB, so the default
 * arm leaves only 32 KB between the RecursionError trip point and the PROT_NONE
 * guard page.  On free-threaded 3.14 that 32 KB is NOT enough: once SP is just
 * below soft_limit a single deeper Python call runs CPython's datastack-chunk
 * path (_PyThreadState_PushFrame -> push_chunk -> allocate_chunk -> mmap, all on
 * the C stack) which itself consumes several KB and then writes into the new
 * chunk -- and that burst dips through the remaining margin into the guard page,
 * SIGSEGV (caught on p212: a fault in allocate_chunk on descent and a munmap on a
 * migrated unwind, both with SP within ~25-55 frames of soft_limit).  A bigger
 * fiber stack makes it WORSE (recursion runs deeper before the trip, more chunk
 * churn) -- proving the failure is the too-thin margin, not stack size.
 *
 * Fix: arm the check against a stack window that is RESERVE bytes SHORTER at the
 * low end -- pass base' = base + RESERVE so soft_limit = base + RESERVE + 2*MARGIN.
 * RecursionError then fires RESERVE earlier, leaving a comfortable cushion for the
 * chunk-alloc / frame-setup burst to complete above the guard.  RESERVE is a
 * fraction of the stack (so small fibers stay usable) with a floor sized to hold
 * the deepest single non-yielding CPython call burst, clamped so the window never
 * inverts on a tiny stack.  Inert on <3.14 (different recursion model). */
#define RUNLOOM_STACKPROT_RESERVE_MIN ((size_t)96 * 1024)   /* >= one chunk-alloc burst */
void runloom_arm_fiber_stackprot(PyThreadState *ts, runloom_coro_t *c)
{
    void  *base;
    size_t size, reserve, eff;
    if (ts == NULL || c == NULL) return;
    base = runloom_coro_stack_base(c);
    size = runloom_coro_stack_size(c);
    if (base == NULL || size == 0) return;
    /* Reserve max(min, size/8), but never more than half the stack so the usable
     * window can't collapse on a small fiber. */
    reserve = size / 8;
    if (reserve < RUNLOOM_STACKPROT_RESERVE_MIN) reserve = RUNLOOM_STACKPROT_RESERVE_MIN;
    if (reserve > size / 2) reserve = size / 2;
    eff = size - reserve;
    if (eff < RUNLOOM_STACKPROT_RESERVE_MIN) {
        /* Stack too small to reserve usefully: raw arm against the real geometry
         * (still bounds the check -- better than leaving it stale). */
        PyUnstable_ThreadState_SetStackProtection(ts, base, size);
        return;
    }
    PyUnstable_ThreadState_SetStackProtection(ts,
        (void *)((char *)base + reserve), eff);
}
#endif

/* offsetof(PyGenObject, gi_exc_state) -- computed in THIS Py_BUILD_CORE-isolated TU,
 * the only one that sees the complete _PyGenObject (on 3.14 the struct moved into
 * pycore_interpframe_structs.h; gi_exc_state is macro-generated by _PyGenObject_HEAD,
 * present on 3.13 and 3.14).  runloom_sched's exc-chain pinning calls this so it needs
 * no internal headers of its own. */
size_t runloom_gen_exc_state_offset(void)
{
    return offsetof(PyGenObject, gi_exc_state);
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
