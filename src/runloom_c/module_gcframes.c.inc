/* module_gcframes.c.inc -- GC visibility anchor for parked-fiber frames.
 *
 * #included from module.c, so it sees runloom_sched.h (runloom_g_t + the snap
 * layout), runloom_introspect.h (the fiber registry), runloom_gstate.h
 * (RUNLOOM_GST_FREED) and runloom_iframe.h (the layout-dependent visit helpers).
 * This file itself touches NO CPython-internal layout -- it only reads the
 * opaque snap pointers and the registry -- so it stays in the plain (non-core)
 * TU; the actual frame walking lives in runloom_iframe.c.
 *
 * WHY IT EXISTS (proven root cause of the big_100 p565/p524 TLBC-on SIGSEGV):
 * free-threaded CPython's GC credits PEP-703 deferred-refcount stackrefs
 * (f_executable code objects, f_funcobj functions, deferred locals) held on a
 * thread's stack ONLY by walking live tstates' current_frame chains
 * (gc_visit_thread_stacks).  A PARKED runloom fiber's suspended _PyInterpreterFrame
 * chain lives in datastack chunks referenced only by g->snap -- invisible to every
 * tstate, hence to the collector.  Objects whose only remaining references are
 * deferred stackrefs in a parked frame are then freed early; the fiber resumes,
 * uses, and re-frees them -> per-thread mimalloc free-list corruption -> SIGSEGV.
 * (TLBC's role is only to turn the specializing interpreter on, which is what
 * plants the deferred references; the crash class is greenlet's -- fixed upstream
 * in greenlet PR #511 for 3.15, from which the visit set here is transcribed.)
 *
 * THE FIX: one module-lifetime, GC-tracked sentinel ("the anchor").  Its
 * tp_traverse -- which the free-threaded collector runs only under stop-the-world
 * -- walks the fiber registry and visits every parked chain exactly as CPython
 * would a live one, so deferred referents are credited and never freed early.
 * Under STW every mutator is stopped at a safepoint, so the registry and every
 * g->snap are frozen for the walk.  The traverse MUST NOT allocate or free (its
 * subtract-pass call site runs inside the collector's heap walk). */

#if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030E0000
#  define RUNLOOM_GCFRAMES_ANCHOR 1
#endif

static PyObject *runloom_gcframes_anchor = NULL;   /* the tracked sentinel; a hidden
                                                    * C reference keeps gc_refs >= 1 so it
                                                    * is always a root (never unreachable) */
static int runloom_gcframes_on = 0;                /* 1 iff the anchor is live + active */

/* Stats: plain (non-atomic) stores only -- the traverse runs single-threaded
 * under STW and must not allocate; a torn read from m_stats is harmless. */
static unsigned long long runloom_gcframes_walks = 0;
static unsigned long long runloom_gcframes_walked_gs = 0;

#if defined(RUNLOOM_GCFRAMES_ANCHOR)

/* Forward: visit one saved snap's chains. */
static int runloom_gcframes_visit_snap(runloom_pystate_snap_t *snap,
                                       visitproc visit, void *arg, int subtract);

/* Visit one registered g's saved chains.  Skips gs with no live snap (running:
 * chain is on the hub/sched tstate and CPython walks it; fresh/done: no frames)
 * and FREED slab residents.  Returns the first non-zero visit result. */
static int runloom_gcframes_visit_g(runloom_g_t *g, visitproc visit, void *arg, int subtract)
{
    if (__atomic_load_n(&g->state, __ATOMIC_RELAXED) == RUNLOOM_GST_FREED) {
        return 0;   /* FREED slab resident, still registry-linked: no live snap */
    }
    if (!g->snap.valid) {
        return 0;   /* running (hub-tstate-visible) or fresh/done */
    }
    runloom_gcframes_walked_gs++;
    return runloom_gcframes_visit_snap(&g->snap, visit, arg, subtract);
}

/* Visit one saved snap's chains (frame chain + privatized c_stack_refs).  Shared
 * by the g walk and the base-snap walk. */
static int runloom_gcframes_visit_snap(runloom_pystate_snap_t *snap,
                                       visitproc visit, void *arg, int subtract)
{
    int r;
    if (!snap->valid) {
        return 0;
    }
    if (snap->current_frame != NULL) {
        r = runloom_gcvisit_frame_chain((void *)snap->current_frame, visit, arg, subtract);
        if (r) return r;
    }
    if (snap->c_stack_refs != NULL) {
        r = runloom_gcvisit_cstack_chain(snap->c_stack_refs, visit, arg, subtract);
        if (r) return r;
    }
    return 0;
}

static int runloom_gcframes_traverse(PyObject *self, visitproc visit, void *arg)
{
    runloom_g_t *g;
    runloom_base_snap_node_t *b;
    int subtract;
    /* Non-STW callers (gc.get_referents / get_referrers, a debugger) run
     * concurrently with parks and would read a moving registry/snap -- report
     * nothing.  Only the collector, under STW, gets the real walk. */
    if (!runloom_gc_world_stopped()) {
        return 0;
    }
    subtract = runloom_gc_in_subtract_pass(self);
    runloom_gcframes_walks++;
    /* 1. Every registered g: default-mode M:N fibers AND single-thread-scheduler
     * fibers, all of which park through runloom_pystate_snap into g->snap. */
    for (g = runloom_greg_head_for_gc(); g != NULL; g = g->reg_next) {
        int r = runloom_gcframes_visit_g(g, visit, arg, subtract);
        if (r) return r;
    }
    /* 2. Base snaps: the single-thread drain's sched_snap holds the run() caller's
     * frame chain while fibers run (ts->current_frame then points at the fiber).
     * (M:N hub snaps are frameless and the main tstate's caller frames stay
     * reachable via its own current_frame, so only single-thread drains register.) */
    for (b = runloom_base_snap_head_for_gc(); b != NULL; b = b->next) {
        int r;
        if (b->snap == NULL) {
            continue;
        }
        r = runloom_gcframes_visit_snap((runloom_pystate_snap_t *)b->snap, visit, arg, subtract);
        if (r) return r;
    }
    return 0;
}

static void runloom_gcframes_anchor_dealloc(PyObject *self)
{
    PyObject_GC_UnTrack(self);
    Py_TYPE(self)->tp_free(self);
}

/* gc.callbacks hook (AM-8): at the "start" of every collection, clear the FROZEN
 * bit gc.freeze() may have stamped on the anchor, so the anchor always
 * participates and parked frames stay visible.  Signature is gc's
 * callback(phase, info); we only act on "start".  Errors are swallowed (a GC
 * callback must not raise). */
static PyObject *runloom_gcframes_gc_callback(PyObject *self, PyObject *args)
{
    PyObject *phase = NULL, *info = NULL;
    (void)self;
    if (PyArg_UnpackTuple(args, "runloom_gc_frames_start", 0, 2, &phase, &info)
        && phase != NULL && PyUnicode_Check(phase)
        && PyUnicode_CompareWithASCIIString(phase, "start") == 0
        && runloom_gcframes_anchor != NULL) {
        runloom_gc_anchor_keep_thawed(runloom_gcframes_anchor);
    }
    Py_RETURN_NONE;
}

static PyMethodDef runloom_gcframes_gc_callback_def = {
    "runloom_gc_frames_start", runloom_gcframes_gc_callback, METH_VARARGS,
    "Internal: keep the runloom GC frames anchor un-frozen across gc.freeze()."
};

static PyTypeObject RunloomGCFramesAnchorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "runloom_c.gc_frames_anchor",
    .tp_basicsize = sizeof(PyObject),
    .tp_itemsize  = 0,
    .tp_dealloc   = runloom_gcframes_anchor_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_doc       = "Internal sentinel: makes parked-fiber frames visible to the "
                    "free-threaded GC so their deferred-refcount referents are not "
                    "freed early.",
    .tp_traverse  = runloom_gcframes_traverse,
    /* tp_clear intentionally NULL: the anchor holds no owned references (it only
     * borrows-and-visits parked frames' refs under STW), so it can neither form
     * nor break a cycle -- there is nothing to clear. */
    .tp_new       = PyType_GenericNew,
};

#endif  /* RUNLOOM_GCFRAMES_ANCHOR */

/* Initialise the anchor.  Called from PyInit_runloom_c after the Coro/G types are
 * ready and the introspect registry is inited.  Returns 0 on success (including
 * the no-op non-FT / disabled cases), -1 on a hard CPython error. */
static int runloom_gcframes_init(PyObject *m)
{
#if defined(RUNLOOM_GCFRAMES_ANCHOR)
    const char *e;
    PyObject *anchor;
    e = getenv("RUNLOOM_GC_FRAMES");
    if (e != NULL && e[0] == '0' && e[1] == '\0') {
        /* Explicitly disabled (A/B + emergency escape hatch).  The anchor stays
         * absent; runtime.py's TLBC interlock then keeps the PYTHON_TLBC=0
         * re-exec, since "anchor off + TLBC on" is the crashy configuration. */
        runloom_gcframes_on = 0;
        return 0;
    }
    /* The registry is now memory-safety load-bearing: an ablated registry
     * (RUNLOOM_GREG_OFF) would leave the walk blind.  Rather than corrupt, refuse
     * to activate -- the interlock then keeps TLBC off (safe, slow).  A proper
     * "GREG_OFF loses to the anchor" override is a follow-up. */
    if (!runloom_greg_is_linked()) {
        PySys_WriteStderr("[runloom] RUNLOOM_GREG_OFF blinds the GC frames anchor; "
                          "not activating it (TLBC stays gated off). "
                          "Unset RUNLOOM_GREG_OFF to enable TLBC.\n");
        runloom_gcframes_on = 0;
        return 0;
    }
    if (PyType_Ready(&RunloomGCFramesAnchorType) < 0) {
        return -1;
    }
    anchor = RunloomGCFramesAnchorType.tp_alloc(&RunloomGCFramesAnchorType, 0);
    if (anchor == NULL) {
        return -1;
    }
    /* PyType_GenericAlloc (the default tp_alloc for a HAVE_GC type) already
     * GC-tracks the object on 3.14, so we do NOT call PyObject_GC_Track here. */
    runloom_gcframes_anchor = anchor;   /* hidden C ref: keeps gc_refs >= 1 (a root) */
    /* Also publish it in the module dict: a first-class GC root so the collector's
     * mark-alive fast path reaches the anchor (and thus every parked chain)
     * without depending on the by-refcount root pass.  Non-fatal if it fails. */
    if (PyModule_AddObjectRef(m, "gc_frames_anchor", anchor) < 0) {
        PyErr_Clear();
    }
    /* AM-8: register a gc "start" callback that keeps the anchor un-frozen, so a
     * user's gc.freeze() (e.g. a prefork server's copy-on-write setup) cannot
     * silently reopen the crash.  Non-fatal if gc / its callbacks list is
     * unavailable -- the anchor still works for programs that never freeze. */
    {
        PyObject *gc = PyImport_ImportModule("gc");
        if (gc != NULL) {
            PyObject *callbacks = PyObject_GetAttrString(gc, "callbacks");
            if (callbacks != NULL && PyList_Check(callbacks)) {
                PyObject *cb = PyCFunction_New(&runloom_gcframes_gc_callback_def, NULL);
                if (cb != NULL) {
                    if (PyList_Append(callbacks, cb) < 0) PyErr_Clear();
                    Py_DECREF(cb);
                }
            }
            Py_XDECREF(callbacks);
            Py_DECREF(gc);
        }
        if (PyErr_Occurred()) PyErr_Clear();
    }
    runloom_gcframes_on = 1;
    return 0;
#else
    (void)m;
    runloom_gcframes_on = 0;
    return 0;
#endif
}

/* True iff the anchor is live and actively making parked frames GC-visible.
 * runtime.py queries this (via runloom_c.gc_frames_active()) as the TLBC safety
 * interlock: TLBC may only stay ON when this is true. */
static int runloom_gcframes_active(void)
{
    return runloom_gcframes_on;
}
