/* pygo_introspect_frames.c.inc -- best-effort Python stack reconstruction.
 * #included at the bottom of pygo_introspect.c (shares its includes).
 *
 * Safety is the whole game here: walking another execution context's
 * frames while it might be running or being torn down is a use-after-
 * free.  We make it safe by construction, per scheduler mode:
 *
 *   - per-g-tstate (g->tstate != NULL): the g owns a real PyThreadState.
 *     CLAIM it via the sweeper handshake (PARKED -> SWEEPING); a claimed
 *     g can neither resume nor be freed, so PyThreadState_GetFrame +
 *     PyFrame_GetBack walk its suspended stack safely.  Full stack.
 *
 *   - single-thread scheduler (no M:N hubs): the dump runs ON the one
 *     scheduler thread, so a parked g's saved frame (g->snap) is stable
 *     for the duration of this C call (nothing else can resume it).  We
 *     report the top frame via the public PyUnstable_InterpreterFrame_*
 *     accessors.  This is the pygo.aio case (asyncio is single-loop), and
 *     asyncio tasks additionally expose their full stack via the stock
 *     Task.get_stack(), so the top frame is the useful add for the raw
 *     goroutines (channels, pump, accept loops) all_tasks() never sees.
 *
 *   - default M:N with hubs running and no per-g tstate: a parked g can be
 *     resumed by its owning hub concurrently, and we hold no claim that
 *     stops it -- so we DON'T touch its frames.  Callable + structural
 *     state still come through; only the source line is withheld.
 *
 * In every case the caller already holds a live reference to g (via
 * pygo_g_try_incref), so the struct + its owned objects cannot be freed
 * mid-walk. */

/* Max frames to report per goroutine -- deep enough for real stacks,
 * bounded so a corrupt chain can't spin. */
#define PYGO_FRAMES_MAX 256

/* pygo_iframe_walk callback: append (filename, lineno, funcname) for one
 * code object + line to the ctx list.  Returns 0 to continue the walk. */
static int frames_cb(PyCodeObject *code, int line, void *ctx)
{
    PyObject *list = (PyObject *)ctx;
    PyObject *fn = NULL, *name = NULL, *tup = NULL, *lineno = NULL;
    if (code == NULL) return 0;
    fn = PyObject_GetAttrString((PyObject *)code, "co_filename");
    if (fn == NULL) { PyErr_Clear(); fn = Py_NewRef(Py_None); }
    name = PyObject_GetAttrString((PyObject *)code, "co_qualname");
    if (name == NULL) {
        PyErr_Clear();
        name = PyObject_GetAttrString((PyObject *)code, "co_name");
        if (name == NULL) { PyErr_Clear(); name = Py_NewRef(Py_None); }
    }
    lineno = PyLong_FromLong((long)line);
    if (lineno != NULL) {
        tup = PyTuple_Pack(3, fn, lineno, name);
        if (tup != NULL) { (void)PyList_Append(list, tup); }
    }
    Py_XDECREF(fn);
    Py_XDECREF(name);
    Py_XDECREF(lineno);
    Py_XDECREF(tup);
    if (PyErr_Occurred()) { PyErr_Clear(); }
    return 0;   /* best-effort: keep walking even if one frame failed */
}

/* Walk the full suspended stack reachable from `top_iframe` into list. */
static void frames_walk(void *top_iframe, PyObject *list)
{
    if (top_iframe == NULL) return;
    pygo_iframe_walk(top_iframe, PYGO_FRAMES_MAX, frames_cb, list);
}

PyObject *pygo_goroutine_frames_by_id(uint64_t id)
{
    pygo_g_t *g, *found = NULL;
    PyObject *frames, *repr = NULL, *result;
    int got = 0;

    frames = PyList_New(0);
    if (frames == NULL) return NULL;

    if (pygo_greg_inited) {
        pygo_mutex_lock(&pygo_greg_lock);
        for (g = pygo_greg_head; g != NULL; g = g->reg_next) {
            unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
            if (st == PYGO_GST_FREED) continue;
            if (__atomic_load_n(&g->id, __ATOMIC_RELAXED) == id) {
                if (pygo_g_try_incref(g)) { found = g; got = 1; }
                break;
            }
        }
        pygo_mutex_unlock(&pygo_greg_lock);
    }

    if (!got) {
        /* Unknown id, or the goroutine is being torn down. */
        result = PyTuple_Pack(2, Py_None, frames);
        Py_DECREF(frames);
        return result;
    }

    g = found;
    if (g->tstate != NULL) {
        /* per-g-tstate: claim so it can neither resume nor free, then walk
         * the g's own PyThreadState frame chain. */
        if (pygo_mn_sweep_try_claim(g)) {
            PyObject *c = g->callable;            /* stable while claimed */
            if (c != NULL) { repr = PyObject_Repr(c); if (repr == NULL) PyErr_Clear(); }
#if PY_VERSION_HEX >= 0x030D0000
            frames_walk((void *)g->tstate->current_frame, frames);
#endif
            pygo_mn_sweep_claim_release(g);
        }
    } else if (pygo_mn_hub_count() == 0) {
        /* single-thread scheduler: a parked g's saved frame is stable on
         * this (the one scheduler) thread for the duration of this call. */
        PyObject *c = g->callable;
        if (c != NULL) { repr = PyObject_Repr(c); if (repr == NULL) PyErr_Clear(); }
#if PY_VERSION_HEX >= 0x030D0000
        if (g->snap.valid)
            frames_walk((void *)g->snap.current_frame, frames);
#endif
    }
    /* else: default M:N, no safe claim -> leave (None, []). */

    pygo_g_decref(g);

    if (repr == NULL) repr = Py_NewRef(Py_None);
    result = PyTuple_Pack(2, repr, frames);
    Py_DECREF(repr);
    Py_DECREF(frames);
    return result;
}
