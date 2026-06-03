/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* sysmon watchdog thread.  Holds NO GIL and NO tstate -- it only reads
 * per-hub atomics and writes to stderr, never touching the Python C-API.
 * Scans every runloom_sysmon_tick_ns; logs a hub whose current runloom_coro_resume
 * has run longer than runloom_sysmon_wedge_ns (an unanticipated non-yielding
 * block), once per wedge episode, and a matching "recovered" line so a run's
 * wedge count + dwell are measurable.  DETECT-ONLY for now (no handoff). */
static RUNLOOM_THREAD_RET runloom_sysmon_main(void *arg)
{
    int n = runloom_hub_count;
    /* w_seq[i] = the resume_seq currently logged as wedged on hub i (0 = none);
     * w_start[i] = its resume_start_ns, for the recovery-dwell report.
     * d_streak[i] = consecutive sysmon ticks hub i has been observed DETACHED
     * while wedged -- the handoff dispatch requires this to reach
     * RUNLOOM_HANDOFF_DETACH_TICKS so it fires only on a STABLE (genuine
     * blocking-call) DETACH, never a transient one (see the gate below). */
    long      *w_seq    = (long *)calloc((size_t)(n > 0 ? n : 1), sizeof(long));
    long long *w_start  = (long long *)calloc((size_t)(n > 0 ? n : 1),
                                             sizeof(long long));
    long      *d_streak = (long *)calloc((size_t)(n > 0 ? n : 1), sizeof(long));
    (void)arg;
    if (w_seq == NULL || w_start == NULL || d_streak == NULL) {
        free(w_seq); free(w_start); free(d_streak);
        RUNLOOM_THREAD_RETURN(NULL);
    }
    while (!__atomic_load_n(&runloom_sysmon_stop, __ATOMIC_ACQUIRE)) {
        long long now = runloom_monotonic_ns();
        int i;
        for (i = 0; i < n; i++) {
            runloom_hub_t *h = &runloom_hubs[i];
            long long start = __atomic_load_n(&h->resume_start_ns,
                                              __ATOMIC_RELAXED);
            long seq = __atomic_load_n(&h->resume_seq, __ATOMIC_RELAXED);
            /* Acquire load: the hub publishes its tstate on its own thread; a
             * racy/NULL read here is fine -- attach-state -1 classifies as "?"
             * and gates every handoff/preempt dispatch closed. */
            PyThreadState *hts = __atomic_load_n(&h->tstate, __ATOMIC_ACQUIRE);
            int tss = runloom_tstate_attach_state(hts);
            /* Track how long this hub has been continuously DETACHED-while-wedged.
             * A genuine blocking call (Py_BEGIN_ALLOW_THREADS) holds the tstate
             * DETACHED for the whole wedge -- the owner thread is parked in a
             * syscall and will NOT touch the tstate until it returns, so a rescue
             * may safely adopt it.  A TRANSIENT detach (an idle/world-yield blip,
             * or the instant before a stop-the-world suspend flips the state to
             * SUSPENDED) does NOT survive consecutive ticks -- and adopting one
             * races the owner re-attaching/suspending on the same tstate
             * (tstate_activate vs tstate_deactivate on the non-atomic _status,
             * which CPython only ever touches from the tstate's single owner) ->
             * use-after-free.  Requiring a stable streak before handoff dispatch
             * (below) keeps the rescue off transient detaches. */
            int detached_wedge = (start != 0 &&
                                  (now - start) > runloom_sysmon_wedge_ns &&
                                  tss == RUNLOOM_TS_DETACHED);
            d_streak[i] = detached_wedge ? d_streak[i] + 1 : 0;
            /* End of a previously-reported episode: the hub moved to a new
             * resume (seq changed) or finished resuming (start cleared). */
            if (w_seq[i] != 0 && (seq != w_seq[i] || start == 0)) {
                long pend = __atomic_load_n(&h->pending, __ATOMIC_RELAXED);
                fprintf(stderr,
                    "[RUNLOOM_SYSMON] hub %d RECOVERED after ~%.1f ms "
                    "(pending=%ld)\n",
                    i, (double)(now - w_start[i]) / 1e6, pend);
                w_seq[i] = 0;
            }
            /* Start of a wedge episode: this resume has overrun the budget and
             * we have not logged THIS seq yet. */
            if (start != 0 && (now - start) > runloom_sysmon_wedge_ns &&
                w_seq[i] != seq) {
                long pend = __atomic_load_n(&h->pending, __ATOMIC_RELAXED);
                /* Classify by the hub tstate's attach state (racy read, fine
                 * for a diagnostic): DETACHED => the wedged g released the
                 * GIL-era tstate (blocking IO) and a standby thread could
                 * adopt the hub -> handoff-RECOVERABLE.  ATTACHED => CPU-bound
                 * or a raw tstate-holding syscall -> needs preemption, not a
                 * handoff.  SUSPENDED => stop-the-world. */
                const char *cls =
                    tss == RUNLOOM_TS_DETACHED  ? "DETACHED (blocking-IO: handoff-recoverable)" :
                    tss == RUNLOOM_TS_ATTACHED  ? "ATTACHED (CPU/raw-syscall: needs preemption)" :
                    tss == RUNLOOM_TS_SUSPENDED ? "SUSPENDED (stop-the-world)" :
                                               "?";
                fprintf(stderr,
                    "[RUNLOOM_SYSMON] hub %d WEDGED %.1f ms in g=%p tstate=%s -- "
                    "%ld g(s) stranded on this hub\n",
                    i, (double)(now - start) / 1e6,
                    (void *)__atomic_load_n(&h->resume_g, __ATOMIC_RELAXED), cls,
                    pend > 0 ? pend - 1 : 0);
                w_seq[i]   = seq;
                w_start[i] = start;
            }
            /* Group B handoff dispatch.  Fires only after the hub has been
             * STABLY DETACHED for RUNLOOM_HANDOFF_DETACH_TICKS consecutive ticks
             * (d_streak) -- i.e. a genuine blocking-call detach, not a transient
             * one a rescue could race the owner on (see the d_streak note above).
             * Re-flagged every qualifying tick (a rescue released early across a
             * STW window re-arms).  The CAS only fires FREE->PENDING, so a hub
             * already PENDING (awaiting pickup) or OWNED (in rescue) is left
             * alone -- no double dispatch; a rescue thread takes it PENDING->
             * OWNED and stores OWNED->FREE at its final release. */
            if (runloom_handoff_enabled &&
                d_streak[i] >= RUNLOOM_HANDOFF_DETACH_TICKS) {
                int free_st = RUNLOOM_HANDOFF_FREE;
                __atomic_compare_exchange_n(&runloom_handoff_claim[i], &free_st,
                                            RUNLOOM_HANDOFF_PENDING, 0,
                                            __ATOMIC_ACQ_REL, __ATOMIC_RELAXED);
            }
            /* Group B step (a) preemption dispatch.  An ATTACHED wedge holds its
             * tstate (no handoff possible) -> ask the eval-frame wrapper to
             * yield the running g at its next Python frame.  Re-armed every tick
             * the wedge persists (the wrapper clears it on each yield), giving
             * periodic time-slicing.  Independent of the handoff above (that is
             * DETACHED-only). */
            if (runloom_preempt_enabled && start != 0 &&
                (now - start) > runloom_sysmon_wedge_ns &&
                tss == RUNLOOM_TS_ATTACHED) {
                __atomic_store_n(&h->preempt_requested, 1, __ATOMIC_RELAXED);
                /* Controlled-replay liveness backstop (Feature 1): the eval-frame
                 * flag above only fires at a Python FRAME boundary, so it cannot
                 * break a single-frame `while: pass` -- which, serialized under
                 * the baton, would hold it forever.  Post an eval-breaker pending
                 * call too; it rides backward-jump checks INSIDE the frame and
                 * yields the baton holder (runloom_mn_liveness_pending_cb).  Gated to
                 * controlled+barrier: the default scheduler survives single-frame
                 * loops via parallelism, and frame-bearing code never reaches this
                 * branch (frame-count preempt resolves it first), so this fires
                 * only for the genuine single-frame deadlock case. */
#ifdef RUNLOOM_MN_LIVENESS_BACKSTOP
                if (runloom_mn_ctrl.enabled && runloom_mn_ctrl.barrier &&
                    runloom_mn_interp != NULL) {
                    _PyEval_AddPendingCall(runloom_mn_interp,
                                           runloom_mn_liveness_pending_cb, NULL, 0);
                }
#endif
            }
        }
        runloom_sleep_ns(runloom_sysmon_tick_ns);
    }
    free(w_seq);
    free(w_start);
    free(d_streak);
    RUNLOOM_THREAD_RETURN(NULL);
}

/* Interpret an OPT-OUT env flag for the stall-recovery features.  Explicit
 * "0" => off; explicit anything-else => on; UNSET => the build default.  The
 * default is ON only on free-threaded 3.13+ -- the one configuration where
 * RUNLOOM_HANDOFF / RUNLOOM_PREEMPT are validated (they need the free-threaded
 * attach-state protocol, and that is where runloom's M:N scheduler actually runs
 * Python in parallel).  On GIL / pre-3.13 builds the default is OFF so an
 * untested config never silently enables them; an explicit env var still
 * forces the user's choice (subject to the per-version / per-g gates below). */
static int runloom_flag_default_on(const char *v)
{
    if (v != NULL) return (v[0] != '0') ? 1 : 0;
#if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030D0000
    return 1;
#else
    return 0;
#endif
}

/* Read RUNLOOM_SYSMON / RUNLOOM_SYSMON_MS once and set the enable flag + threshold.
 * Must run in mn_init BEFORE the hub threads start so the per-resume
 * instrumentation is live from the first resume.  RUNLOOM_HANDOFF + RUNLOOM_PREEMPT
 * default ON on free-threaded 3.13+ (opt out with =0); they force sysmon on. */
static void runloom_sysmon_config(void)
{
    const char *e = getenv("RUNLOOM_SYSMON");
    const char *ho = getenv("RUNLOOM_HANDOFF");
    const char *ms;
    runloom_sysmon_enabled = (e != NULL && e[0] != '0') ? 1 : 0;
    /* The handoff rescue depends on the sysmon detector: its per-resume
     * instrumentation (resume_start_ns) is what spots the DETACHED wedge and
     * the watchdog is what dispatches it.  RUNLOOM_HANDOFF=1 therefore forces the
     * sysmon instrumentation + watchdog on, independent of RUNLOOM_SYSMON.
     *
     * DEFAULT mode ONLY.  Under per-g-tstate the hub's bound tstate (h->tstate)
     * is DETACHED for the ENTIRE duration of every per-g resume (the hub swaps
     * it out for g->tstate), so it would constantly look like a DETACHED wedge;
     * and the rescue's snap-based default-mode drain is wrong for per-g gs
     * (they ride a per-g tstate, not the snap, and wake to the global runq, not
     * the hub sub_list).  Stand the rescue down there -- per-g-tstate already
     * recovers stalled-hub work via the global run-queue. */
    runloom_handoff_enabled = runloom_flag_default_on(ho);   /* default ON (free-threaded 3.13+) */
#if PY_VERSION_HEX < 0x030D0000
    runloom_handoff_enabled = 0;   /* no free-threaded attach states pre-3.13 */
#endif
    if (runloom_get_per_g_tstate_mode()) runloom_handoff_enabled = 0;
    if (runloom_handoff_enabled) {
        const char *pl = getenv("RUNLOOM_HANDOFF_POOL");
        const char *dbg = getenv("RUNLOOM_HANDOFF_DEBUG");
        runloom_handoff_debug = (dbg != NULL && dbg[0] != '0') ? 1 : 0;
        runloom_sysmon_enabled = 1;
        /* Pool size: default min(hub_count, 4) -- enough to recover several
         * simultaneous DETACHED wedges in parallel without standing up a
         * standby thread per hub.  At most hub_count hubs can be wedged at
         * once, so clamp there; floor at 1. */
        runloom_handoff_pool = (pl != NULL) ? atoi(pl)
                                         : (runloom_hub_count < 4 ? runloom_hub_count : 4);
        if (runloom_handoff_pool < 1) runloom_handoff_pool = 1;
        if (runloom_handoff_pool > runloom_hub_count) runloom_handoff_pool = runloom_hub_count;
    }
    /* RUNLOOM_PREEMPT: ATTACHED/CPU preemption (3.13+ only -- needs the attach
     * states to classify the wedge).  Like RUNLOOM_HANDOFF it forces the sysmon
     * instrumentation + watchdog on (that is what detects the wedge and arms
     * preempt_requested).  The eval-frame wrapper is installed in mn_init. */
    runloom_preempt_enabled = runloom_flag_default_on(getenv("RUNLOOM_PREEMPT"));  /* default ON */
#if PY_VERSION_HEX < 0x030D0000
    runloom_preempt_enabled = 0;
#endif
    /* Under per-g-tstate the hub's bound tstate is DETACHED every per-g resume,
     * so the ATTACHED-wedge arm never fires and the wrapper would only be dead
     * weight on the eval path -- stand it down (matches the handoff gating). */
    if (runloom_get_per_g_tstate_mode()) runloom_preempt_enabled = 0;
    if (runloom_preempt_enabled) runloom_sysmon_enabled = 1;
    if (!runloom_sysmon_enabled) return;
    ms = getenv("RUNLOOM_SYSMON_MS");
    if (ms != NULL) {
        long long v = atoll(ms);
        if (v > 0) runloom_sysmon_wedge_ns = v * 1000000LL;
    }
    /* Scan ~5x faster than the wedge budget (cap 10 ms) so detection latency
     * is a small fraction of the threshold without busy-spinning. */
    runloom_sysmon_tick_ns = runloom_sysmon_wedge_ns / 5;
    if (runloom_sysmon_tick_ns > 10000000LL) runloom_sysmon_tick_ns = 10000000LL;
    if (runloom_sysmon_tick_ns < 1000000LL)  runloom_sysmon_tick_ns = 1000000LL;
}

/* Spawn the watchdog thread (after hubs exist).  Enabled-gated; a spawn
 * failure is non-fatal -- the scheduler runs fine without the watchdog. */
static void runloom_sysmon_spawn(void)
{
    if (!runloom_sysmon_enabled) return;
    runloom_sysmon_stop = 0;
    if (runloom_thread_create(&runloom_sysmon_thread, runloom_sysmon_main, NULL) == 0) {
        runloom_sysmon_running = 1;
    } else {
        fprintf(stderr, "[RUNLOOM_SYSMON] watchdog thread spawn failed; "
                        "stall detection disabled\n");
    }
}

/* Stop + join the watchdog.  Called at the top of mn_fini, BEFORE the hubs
 * are torn down, so the scan never reads freed hub state. */
static void runloom_sysmon_stop_join(void)
{
    if (!runloom_sysmon_running) return;
    __atomic_store_n(&runloom_sysmon_stop, 1, __ATOMIC_RELEASE);
    runloom_thread_join(runloom_sysmon_thread);
    runloom_sysmon_running = 0;
}

/* ---- Group B rescue loop (RUNLOOM_HANDOFF) ----
 *
 * Resume ONE default-mode g on the currently-adopted hub tstate, using
 * `base` (the captured g_block slice) as the per-g register window exactly as
 * hub_main uses hub_snap.  A faithful copy of hub_main's DEFAULT branch
 * (mn_sched.c resume block) MINUS the per-g-tstate / global-runq paths (the
 * handoff is default mode) and MINUS the sysmon resume_begin/end instrument
 * (that field tracks the OWNER's g_block wedge; the rescue's own resumes are
 * unwatched).  Kept as a separate routine per the project ethos: do NOT
 * refactor hub_main, so the default scheduler path stays byte-unchanged.
 *
 * tstate-safety: every g in hub h's sub_list/FIFO/deque either is fresh (no
 * tstate affinity) or parked while running on hub h (its parker recorded
 * h->id, so a wake routes it back to h -- a g stolen to another hub records
 * THAT hub and never lands here).  So resuming on hub_ts is always the
 * same-tstate resume the H>=2 bug requires.  The rescue therefore NEVER steals
 * from a neighbour (that g parked on a different tstate). */
static void runloom_handoff_resume_g(runloom_hub_t *h, runloom_g_t *g,
                                  runloom_pystate_snap_t *base)
{
    int self_queued;

    if (g->snap.valid) {
        runloom_pystate_load(&g->snap);
    } else {
        runloom_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030D0000
        {
            PyThreadState *ts = PyThreadState_GET();
            ts->current_frame = NULL;
        }
#endif
    }
    h->sched.current = g;
    runloom_tls_current_g = g;
    runloom_tls_self_queued = 0;
    runloom_tls_parked_offqueue = 0;
    __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
    RUNLOOM_G_ASSERT_NOT(g, RUNLOOM_GST_BIT(RUNLOOM_GST_FREED));
    RUNLOOM_EVT(RUNLOOM_EVT_G_POP, g, h, 0);
    if (g->coro == NULL || __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
        h->sched.current = NULL;
        runloom_tls_current_g = NULL;
        return;
    }
    /* NB: no runloom_hub_resume_begin/end -- the rescue must not write
     * h->resume_start_ns (that is the owner's g_block wedge clock). */
    runloom_coro_resume(g->coro);
    self_queued = runloom_tls_self_queued;
    runloom_tls_self_queued = 0;
    runloom_tls_parked_offqueue = 0;
    runloom_tls_current_g = NULL;
    h->sched.current = NULL;

    if (runloom_coro_done(g->coro)) {
        runloom_drain_g_datastack();
        runloom_pystate_load(base);
        runloom_netpoll_force_unlink_g_parker(g);
        runloom_mn_pending_complete(h);
        runloom_g_decref(g);               /* scheduler ref */
        runloom_pystate_snap(base);
    } else if (!self_queued) {
        /* Raw runloom_coro_yield(): keep it runnable, hub-pinned. */
        runloom_sched_ready_push(&h->sched, g);
    } else if (runloom_g_state_in(g, RUNLOOM_GST_MASK_PARKED)) {
        /* Parked off-queue (netpoll/chan/sleep): a wake_g routes it back to
         * THIS hub's sub_list.  Drop its idle stack pages (no-op unless
         * RUNLOOM_STACK_PARK_DONTNEED=1). */
        runloom_coro_park(g->coro);
    }
    /* else: cooperative sched_yield already re-queued itself on the FIFO. */
}
