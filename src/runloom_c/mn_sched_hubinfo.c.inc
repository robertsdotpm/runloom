/* Part of mn_sched.c -- #included into that TU.  Per-hub diagnostic snapshot
 * for runloom.inspect.hubs(); see the runloom_hub_info_t contract in mn_sched.h.
 *
 * The interesting part is `blocked_at` -- the Python call site of a fiber
 * wedged inside a non-cooperative blocking C call.  Reading another execution
 * context's top frame from the snapshotting thread is a use-after-free in
 * general (frames mutate as code runs), so we restrict it to the ONE
 * configuration where it is safe by construction MINUS a documented, vanishing
 * residual:
 *
 *   - DEFAULT M:N mode only.  Under per-g-tstate the g runs on g->tstate (not
 *     the hub's bound tstate, which looks permanently DETACHED there), so the
 *     hub-tstate frame would be meaningless -- skip it.
 *   - the hub's bound tstate must be DETACHED and the resume must have overrun
 *     the wedge budget: a fiber that did Py_BEGIN_ALLOW_THREADS and is now
 *     parked in a blocking syscall.  The owner thread will NOT touch
 *     current_frame until END_ALLOW_THREADS re-attaches (the very property that
 *     lets the handoff rescue adopt the tstate), so the frame chain is pinned
 *     for the duration of a genuine wedge.
 *   - the handoff rescue is the OTHER thread that could mutate this tstate's
 *     frames (it resumes stranded gs on it).  We lock it out by CASing the
 *     hub's claim slot FREE->OWNED -- mimicking a rescue holding the hub:
 *     sysmon's FREE->PENDING dispatch and a rescue's PENDING->OWNED pickup both
 *     fail against OWNED, so neither runs while we hold it; we store OWNED->FREE
 *     the instant we are done (a few microseconds).  This reuses the existing
 *     claim values, so the FV-verified handoff protocol is untouched.  If the
 *     CAS loses (a rescue is already pending/active), we skip blocked_at.
 *
 * Residual: the owner thread returning from its syscall mid-read re-attaches and
 * could overwrite the frame.  We re-confirm DETACHED immediately before the walk
 * (shrinking the window to ~the walk itself) and read only the TOP frame.  For a
 * genuinely-stuck hub -- the use case -- the owner never returns, so the window
 * is zero.  This is opt-in best-effort; the always-safe full stack is
 * `py-spy dump --pid` (the stack_cmd runloom.inspect.hubs() emits). */

struct hubinfo_frame_ctx {
    char  *buf;
    size_t cap;
    int    got;
};

/* runloom_iframe_walk callback: format the TOP frame as "qualname (base:line)"
 * into ctx->buf, then stop the walk.  Best-effort -- any failure leaves the
 * buffer untouched (empty). */
static int hubinfo_frame_cb(PyCodeObject *code, int line, void *cv)
{
    struct hubinfo_frame_ctx *ctx = (struct hubinfo_frame_ctx *)cv;
    PyObject *qn = NULL, *fn = NULL;
    const char *qs = NULL, *fs = NULL, *base, *b1, *b2;
    if (code == NULL) { ctx->got = 1; return 1; }
    qn = PyObject_GetAttrString((PyObject *)code, "co_qualname");
    if (qn == NULL) { PyErr_Clear();
        qn = PyObject_GetAttrString((PyObject *)code, "co_name"); }
    fn = PyObject_GetAttrString((PyObject *)code, "co_filename");
    if (qn != NULL) qs = PyUnicode_AsUTF8(qn);
    if (fn != NULL) fs = PyUnicode_AsUTF8(fn);
    if (qs == NULL) { PyErr_Clear(); qs = "?"; }
    if (fs == NULL) { PyErr_Clear(); fs = "?"; }
    /* basename only -- the directory path is noise in a one-line view. */
    b1 = strrchr(fs, '/');
    b2 = strrchr(fs, '\\');
    base = (b1 > b2) ? b1 : b2;
    base = (base != NULL) ? base + 1 : fs;
    snprintf(ctx->buf, ctx->cap, "%s (%s:%d)", qs, base, line);
    Py_XDECREF(qn);
    Py_XDECREF(fn);
    if (PyErr_Occurred()) PyErr_Clear();
    ctx->got = 1;
    return 1;   /* top frame only */
}

runloom_hub_info_t *runloom_mn_hub_snapshot(long *count_out)
{
    runloom_hub_info_t *arr;
    int n, i, per_g, handoff_on;
    long long now;

    if (count_out != NULL) *count_out = 0;
    n = runloom_hub_count;
    if (runloom_hubs == NULL || n <= 0) return NULL;

    arr = (runloom_hub_info_t *)calloc((size_t)n, sizeof(*arr));
    if (arr == NULL) return NULL;

    now        = runloom_monotonic_ns();
    per_g      = runloom_get_per_g_tstate_mode();
    handoff_on = (runloom_handoff_enabled && runloom_handoff_claim != NULL);

    for (i = 0; i < n; i++) {
        runloom_hub_t *h = &runloom_hubs[i];
        long long      start = __atomic_load_n(&h->resume_start_ns, __ATOMIC_RELAXED);
        runloom_g_t   *rg    = (runloom_g_t *)__atomic_load_n((void **)&h->resume_g, __ATOMIC_RELAXED);
        PyThreadState *hts   = (PyThreadState *)__atomic_load_n((void **)&h->tstate, __ATOMIC_ACQUIRE);
        int            tss   = runloom_tstate_attach_state(hts);
        runloom_hub_info_t *o = &arr[i];

        o->id                = h->id;
        o->pending           = __atomic_load_n(&h->pending, __ATOMIC_RELAXED);
        o->attach_state      = tss;
        o->preempt_requested = __atomic_load_n(&h->preempt_requested, __ATOMIC_RELAXED);
        o->instrumented      = runloom_sysmon_enabled;
        o->blocked_at[0]     = '\0';
        o->has_running_g     = (start != 0) ? 1 : 0;
        o->dwell_ms          = (start != 0 && now > start)
                               ? (double)(now - start) / 1e6 : 0.0;
        o->running_g         = (start != 0 && rg != NULL) ? runloom_g_id(rg) : 0;

        /* blocked_at: only for a DETACHED-wedged hub in default mode (see the
         * safety note above).  Off the hot path entirely -- this whole block is
         * skipped unless a hub has actually overrun the wedge budget. */
        if (!per_g && start != 0 && (now - start) > runloom_sysmon_wedge_ns &&
            tss == RUNLOOM_TS_DETACHED && hts != NULL) {
            int locked = 0;
            if (handoff_on) {
                int free_st = RUNLOOM_HANDOFF_FREE;
                locked = __atomic_compare_exchange_n(
                    &runloom_handoff_claim[i], &free_st, RUNLOOM_HANDOFF_OWNED,
                    0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED);
            }
            if (!handoff_on || locked) {
                /* Re-confirm the owner is still parked in its block before
                 * chasing its frame pointer. */
                if (runloom_tstate_attach_state(hts) == RUNLOOM_TS_DETACHED) {
                    struct hubinfo_frame_ctx ctx;
                    ctx.buf = o->blocked_at;
                    ctx.cap = sizeof o->blocked_at;
                    ctx.got = 0;
#if PY_VERSION_HEX >= 0x030D0000
                    runloom_iframe_walk((void *)hts->current_frame, 1,
                                        hubinfo_frame_cb, &ctx);
#endif
                }
                if (locked) {
                    int owned = RUNLOOM_HANDOFF_OWNED;
                    __atomic_compare_exchange_n(
                        &runloom_handoff_claim[i], &owned, RUNLOOM_HANDOFF_FREE,
                        0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED);
                }
            }
        }
    }

    if (count_out != NULL) *count_out = n;
    return arr;
}
