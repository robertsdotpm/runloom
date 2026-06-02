/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Mark the start of a coro resume on this hub (sysmon progress beat).
 * resume_seq is bumped FIRST so a watchdog that samples mid-update sees a
 * fresh seq with a possibly-stale ns and just waits one more tick.  Inlined
 * and gated: when the watchdog is off this is a single predictable branch. */
/* ====================================================================
 * Controlled M:N scheduler (PYGO_MN_SEED) -- CHESS-style serialization for
 * seeded exploration of the REAL hub/deque/steal/wake path (where single-hub
 * PCT, PYGO_PCT_SEED, cannot reach).
 *
 * When PYGO_MN_SEED is set, goroutine *execution segments* across all hubs are
 * serialized through one baton: a hub may run a goroutine (pygo_coro_resume)
 * only while it holds the baton (gated in resume_begin/end below), and a seeded
 * controller picks which waiting hub gets it next.  This trades real
 * parallelism for reproducibility + control: scheduling-order bugs (lost
 * wakeups, cross-hub wake ordering, deadlocks) become reproducible and seed-
 * sweepable on the real M:N code.
 *
 * A goroutine that parks/yields/finishes returns to hub_main, which releases
 * the baton, so cooperative blocking never deadlocks it.  A CPU-bound goroutine
 * that does not yield is broken off by PREEMPTION (which must stay ON -- see
 * pygo_mn_ctrl_init); disabling it wedges the baton.  While waiting a hub
 * DETACHES its tstate (PyEval_SaveThread) so it sits at a GC safepoint.
 *
 * Formal model + properties (incl. the no-preempt deadlock and the barrier-
 * rendezvous determinism target): verify/tla/PygoMNControl.tla.  Caveat: this
 * controls scheduling nondeterminism, not full deterministic replay yet (see
 * tools/mn_controlled/README.md).  Zero cost when PYGO_MN_SEED is unset.
 * ==================================================================== */
typedef struct {
    int          enabled;
    int          n;
    pygo_mutex_t lock;
    pygo_cond_t  cond;
    int          current;     /* hub id allowed to run a g now; -1 = free */
    int         *want;        /* per-hub: requesting the baton */
    uint64_t     rng;
} pygo_mn_ctrl_t;
static pygo_mn_ctrl_t pygo_mn_ctrl;

static uint64_t pygo_mn_ctrl_rand(void)      /* xorshift64 */
{
    uint64_t x = pygo_mn_ctrl.rng;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    pygo_mn_ctrl.rng = x;
    return x;
}

/* lock held: pick a requesting hub by seed, or -1 if none requesting */
static int pygo_mn_ctrl_choose(void)
{
    int cnt = 0, i, k;
    for (i = 0; i < pygo_mn_ctrl.n; i++) cnt += pygo_mn_ctrl.want[i];
    if (cnt == 0) return -1;
    k = (int)(pygo_mn_ctrl_rand() % (uint64_t)cnt);
    for (i = 0; i < pygo_mn_ctrl.n; i++)
        if (pygo_mn_ctrl.want[i] && k-- == 0) return i;
    return -1;
}

static void pygo_mn_ctrl_init(int n)
{
    const char *seed = getenv("PYGO_MN_SEED");
    pygo_mn_ctrl.enabled = 0;
    if (seed == NULL || seed[0] == '\0' || n <= 0) return;
    pygo_mn_ctrl.want = (int *)PyMem_Calloc((size_t)n, sizeof(int));
    if (pygo_mn_ctrl.want == NULL) return;     /* OOM -> stay disabled */
    pygo_mutex_init(&pygo_mn_ctrl.lock);
    pygo_cond_init(&pygo_mn_ctrl.cond);
    pygo_mn_ctrl.n = n;
    pygo_mn_ctrl.current = -1;
    pygo_mn_ctrl.rng = strtoull(seed, NULL, 10);
    if (pygo_mn_ctrl.rng == 0) pygo_mn_ctrl.rng = 0x9E3779B97F4A7C15ULL;
    pygo_mn_ctrl.enabled = 1;
    /* Handoff MUST stay off: its rescue thread resumes a goroutine OFF the
     * baton (bypasses resume_begin/end), which would break serialization. */
    pygo_handoff_enabled = 0;
    /* Preemption stays ON (liveness backstop, proven necessary by
     * verify/tla/PygoMNControl.tla's Preempt=FALSE control): a goroutine that
     * runs Python without yielding would otherwise hold the baton forever and
     * wedge every other hub.  Preempt yields it at a bytecode boundary ->
     * resume_end -> baton released -> progress. */
}

static void pygo_mn_ctrl_fini(void)
{
    if (!pygo_mn_ctrl.enabled) return;
    pygo_mn_ctrl.enabled = 0;
    pygo_cond_destroy(&pygo_mn_ctrl.cond);
    pygo_mutex_destroy(&pygo_mn_ctrl.lock);
    PyMem_Free(pygo_mn_ctrl.want);
    pygo_mn_ctrl.want = NULL;
}

/* Block until this hub holds the baton.  Detaches the Python thread state
 * across the wait so a blocked hub is at a GC safepoint (free-threaded). */
static void pygo_mn_ctrl_acquire(int hub)
{
    if (!pygo_mn_ctrl.enabled || hub < 0 || hub >= pygo_mn_ctrl.n) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    pygo_mn_ctrl.want[hub] = 1;
    if (pygo_mn_ctrl.current == -1) {
        pygo_mn_ctrl.current = pygo_mn_ctrl_choose();
        pygo_cond_broadcast(&pygo_mn_ctrl.cond);
    }
    if (pygo_mn_ctrl.current != hub) {
        PyThreadState *ts = PyEval_SaveThread();   /* detach while we block */
        while (pygo_mn_ctrl.current != hub)
            pygo_cond_wait(&pygo_mn_ctrl.cond, &pygo_mn_ctrl.lock);
        pygo_mn_ctrl.want[hub] = 0;
        pygo_mutex_unlock(&pygo_mn_ctrl.lock);
        PyEval_RestoreThread(ts);
        return;
    }
    pygo_mn_ctrl.want[hub] = 0;
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

static void pygo_mn_ctrl_release(int hub)
{
    if (!pygo_mn_ctrl.enabled || hub < 0 || hub >= pygo_mn_ctrl.n) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    if (pygo_mn_ctrl.current == hub) pygo_mn_ctrl.current = -1;
    pygo_mn_ctrl.current = pygo_mn_ctrl_choose();   /* hand to next by seed */
    pygo_cond_broadcast(&pygo_mn_ctrl.cond);
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

PYGO_INLINE void pygo_hub_resume_begin(pygo_hub_t *h, pygo_g_t *g)
{
    if (__builtin_expect(pygo_sysmon_enabled, 0)) {
        /* Relaxed-atomic like its two siblings below: the sysmon thread reads
         * resume_g concurrently for the wedge log (best-effort sample). */
        __atomic_store_n(&h->resume_g, g, __ATOMIC_RELAXED);
        __atomic_add_fetch(&h->resume_seq, 1, __ATOMIC_RELAXED);
        __atomic_store_n(&h->resume_start_ns, pygo_monotonic_ns(),
                         __ATOMIC_RELAXED);
    }
    /* controlled mode: block until this hub holds the execution baton */
    pygo_mn_ctrl_acquire(h->id);
}

/* Mark the end of a coro resume: clear resume_start_ns so the watchdog
 * stops counting this hub as "in a resume". */
PYGO_INLINE void pygo_hub_resume_end(pygo_hub_t *h)
{
    if (__builtin_expect(pygo_sysmon_enabled, 0)) {
        __atomic_store_n(&h->resume_start_ns, 0, __ATOMIC_RELAXED);
    }
    /* controlled mode: hand the baton to the next hub (seeded) */
    pygo_mn_ctrl_release(h->id);
}

/* Interpreter for per-g PyThreadState_New, captured at mn_init. */
static PyInterpreterState *pygo_mn_interp = NULL;

/* TLS pointers set at hub_main entry.  pygo_mn_yield_current() and
 * pygo_mn_current_hub() read these to route per-g operations to the
 * right hub without each call site needing to look it up. */
static PYGO_TLS pygo_hub_t *pygo_tls_hub = NULL;
static PYGO_TLS pygo_g_t   *pygo_tls_current_g = NULL;
/* Set by pygo_mn_yield_current() before pygo_coro_yield(); read by
 * hub_main after pygo_coro_resume returns.  Tells hub_main "the g
 * has already put itself on a queue, you don't need to requeue".
 * Distinguishes scheduler-aware yield (sched_yield) from raw yield
 * (pygo_core.yield_() -> pygo_coro_yield directly). */
static PYGO_TLS int pygo_tls_self_queued = 0;
/* Set ONLY by pygo_mn_tls_mark_parked (the off-queue park path: netpoll /
 * chan / sleep all route through it).  Distinguishes a g that parked off-queue
 * -- so a wake_g will route it back, and it must NOT be re-pushed -- from a
 * cooperative yield_current (self_queued too, but it already pushed itself to
 * the local FIFO).  Read by hub_main's per-g-tstate release to decide PARKED
 * (wakeable) vs RUNNING (hub-pinned) for the wake state machine.  Unused by the
 * default scheduler, which keys re-push purely on self_queued. */
static PYGO_TLS int pygo_tls_parked_offqueue = 0;
/* Consecutive trivial sched_yield fast-paths on THIS hub thread since the last
 * real yield (see pygo_mn_yield_current).  Bounds the fast path so a g looping
 * on sched_yield can't spin forever without returning to hub_main to drain
 * h->sub_head (work mn_go'd / wake_g'd onto this hub after the loop started).
 * Per-thread; only the running g touches it; reset on every real yield. */
static PYGO_TLS int pygo_tls_fastpath_streak = 0;
/* After this many consecutive trivial fast-paths, force one real yield so
 * hub_main drains sub_head + re-polls.  Large enough that a genuine lone poller
 * rarely pays the round-trip; small enough that newly-arrived work waits only
 * microseconds. */
#define PYGO_YIELD_FASTPATH_BOUND 64

#if PY_VERSION_HEX >= 0x030D0000
/* PYGO_PREEMPT eval-frame wrapper (see the block near pygo_preempt_enabled).
 * Chains to the previous eval-frame func (normally _PyEval_EvalFrameDefault) so
 * it composes with any other installed wrapper; _PyInterpreterFrame is opaque
 * (passed straight through). */
static _PyFrameEvalFunction pygo_preempt_prev_eval = NULL;

static PyObject *pygo_preempt_eval_frame(PyThreadState *ts,
                                         struct _PyInterpreterFrame *frame,
                                         int throwflag)
{
    pygo_hub_t *h = pygo_tls_hub;
    pygo_g_t *g;
    if (h != NULL &&
        __atomic_load_n(&h->preempt_requested, __ATOMIC_RELAXED) &&
        (g = pygo_tls_current_g) != NULL) {
        /* Clear first so one watchdog flag => one yield; the watchdog re-arms
         * each tick the wedge persists, giving periodic time-slicing. */
        __atomic_store_n(&h->preempt_requested, 0, __ATOMIC_RELAXED);
        /* The SNAP-saving cooperative yield (pygo_mn_yield_current's body MINUS
         * its trivial-switch fast path).  Two reasons not to reuse that helper:
         * (1) raw pygo_coro_yield would re-resume off a STALE g->snap (the g's
         * last park) -> frame-chain corruption for a Python-handler g; we must
         * snap here.  (2) its fast path bails when the hub's FIFO/deque are
         * empty and nothing is netpoll-parked -- but it does NOT look at the
         * sub_list, where another hub's pump delivers THIS hub's woken workers.
         * A preempted g runs unboundedly without ever returning to hub_main's
         * loop-top drain, so the sub_list silently fills; the fast path would
         * then skip the yield and the workers would never be drained.  Yielding
         * unconditionally is correct: the watchdog already decided this hub is
         * CPU-wedged, so a yield is always warranted.  hub_main's loop top then
         * drains the sub_list and the g (now at the FIFO back) round-robins. */
        pygo_sched_ready_push(&h->sched, g);
        pygo_pystate_snap(&g->snap);
        pygo_tls_self_queued = 1;
        pygo_coro_yield();
    }
    /* Acquire-load: install/uninstall (other thread, or main during the
     * init/fini windows while hub gs already run Python) writes this pointer
     * with release; a plain read on the per-frame hot path was a data race. */
    {
        _PyFrameEvalFunction prev =
            __atomic_load_n(&pygo_preempt_prev_eval, __ATOMIC_ACQUIRE);
        return prev(ts, frame, throwflag);
    }
}
#endif

/* Install/uninstall the preemption eval-frame wrapper on `interp`.  Install
 * runs in mn_init after pygo_sysmon_config (so pygo_preempt_enabled is set) and
 * BEFORE any goroutine runs Python; uninstall runs in mn_fini.  No-ops unless
 * PYGO_PREEMPT (and 3.13+).  Capturing the previous func means we chain rather
 * than clobber a wrapper someone else installed. */
static void pygo_preempt_install(PyInterpreterState *interp)
{
#if PY_VERSION_HEX >= 0x030D0000
    if (!pygo_preempt_enabled) return;
    /* Release-store so the eval hook's acquire-load sees a fully-published
     * prev pointer; pairs with the load in pygo_preempt_eval_frame. */
    __atomic_store_n(&pygo_preempt_prev_eval,
                     _PyInterpreterState_GetEvalFrameFunc(interp),
                     __ATOMIC_RELEASE);
    _PyInterpreterState_SetEvalFrameFunc(interp, pygo_preempt_eval_frame);
#else
    (void)interp;
#endif
}

static void pygo_preempt_uninstall(PyInterpreterState *interp)
{
#if PY_VERSION_HEX >= 0x030D0000
    {
        _PyFrameEvalFunction prev =
            __atomic_load_n(&pygo_preempt_prev_eval, __ATOMIC_ACQUIRE);
        if (!pygo_preempt_enabled || prev == NULL) return;
        _PyInterpreterState_SetEvalFrameFunc(interp, prev);
        __atomic_store_n(&pygo_preempt_prev_eval, NULL, __ATOMIC_RELEASE);
    }
#else
    (void)interp;
#endif
}
