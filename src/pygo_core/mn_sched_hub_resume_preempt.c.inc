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
 * DETERMINISTIC REPLAY (PYGO_MN_BARRIER, default on under a seed): three levers
 * turn seeded exploration into reproducible replay for closed (CPU + channel /
 * lock / sync) workloads -- (1) a barrier-rendezvous census so each seeded grant
 * is over the COMPLETE requester set, not whoever raced into acquire first;
 * (2) a startup entry gate (pygo_mn_ctrl_arm, from mn_run) so the first census
 * sees all initially-placed work; (3) no work-stealing, so each hub's stream is
 * a fixed function of the schedule; (4) census_idle re-tests the sub_list before
 * declaring idle (a wake from the just-ended segment can land post-drain, and
 * declaring idle then would run that g a round late -- a timing-decided outcome).
 * Same seed => identical run (repro_probe.py: 16/16 seeds stable; seed 1
 * identical over 500/500 reps under heavy CPU load).  Open scope: real-I/O
 * arrival timing (open-system limit) and CPU-bound-without-yield preemption,
 * still wall-clock (a future deterministic-bytecode-count lever).  Formal model +
 * properties (the no-preempt deadlock and the barrier's DeterministicGrant):
 * verify/tla/PygoMNControl.tla.  Mechanism + scope: tools/mn_controlled/README.md.
 * Zero cost when PYGO_MN_SEED is unset.
 * ==================================================================== */
typedef struct {
    int          enabled;
    int          n;
    pygo_mutex_t lock;
    pygo_cond_t  cond;
    int          current;     /* hub id allowed to run a g now; -1 = free */
    int         *want;        /* per-hub: requesting the baton (waiting) */
    uint64_t     rng;
    /* Barrier-rendezvous (PYGO_MN_BARRIER, default on under a seed) -- the
     * lever that turns seeded EXPLORATION into deterministic REPLAY.  Without
     * it, the controller hands the baton to whichever hubs happen to have
     * registered want[] by the instant of the choice, so the requester set
     * (and thus the seeded pick) is a function of OS thread timing.  With it,
     * the controller grants only once the requester set for the round is
     * COMPLETE -- every hub has "checked in" for the round, either as a wanter
     * (in acquire) or as idle (in hub_main's no-work branch).  The pick is then
     * a function of the schedule alone.  Formal counterpart: the Barrier
     * constant in verify/tla/PygoMNControl.tla (DeterministicGrant). */
    int          barrier;     /* census gate on */
    int          armed;       /* mn_run reached: hubs may begin + grants may fire */
    uint64_t     round;       /* current census round; bumped at each release */
    uint64_t    *seen;        /* per-hub: last round this hub checked in */
    /* Grant trace (PYGO_MN_TRACE=<path>).  Recorded into an in-memory byte ring
     * (one hub-id digit per grant) and dumped to the file at fini -- NOT
     * fprintf'd per grant, whose syscall/flush perturbs the very timing we are
     * trying to observe (it masked a ~1% tail race).  Near-zero per-grant cost. */
    char        *tracebuf;
    size_t       tracecap;
    size_t       tracelen;
    char         tracepath[256];
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

/* lock held: have all hubs checked in for the current round? */
static int pygo_mn_ctrl_census_complete(void)
{
    int i;
    for (i = 0; i < pygo_mn_ctrl.n; i++)
        if (pygo_mn_ctrl.seen[i] != pygo_mn_ctrl.round) return 0;
    return 1;
}

/* lock held: grant the baton if it's free and (barrier ? the requester set is
 * complete : someone wants it).  Idempotent; the single seeded RNG draw per
 * grant is what makes the handoff sequence a function of the seed+schedule. */
static void pygo_mn_ctrl_try_grant(void)
{
    int c;
    if (pygo_mn_ctrl.barrier && !pygo_mn_ctrl.armed)
        return;                                      /* startup gate (below) */
    if (pygo_mn_ctrl.current != -1) return;          /* baton held */
    if (pygo_mn_ctrl.barrier && !pygo_mn_ctrl_census_complete())
        return;                                      /* set still forming */
    c = pygo_mn_ctrl_choose();
    if (c < 0) return;                               /* nobody wants it */
    pygo_mn_ctrl.current = c;
    /* Grant trace (opt-in, PYGO_MN_TRACE): one byte into an in-memory ring, no
     * syscall on the hot path; dumped to the file at fini.  NB the residual
     * sub-segment tail race is a Heisenbug -- even this byte-write perturbs it,
     * so a clean trace does not certify a clean run. */
    if (pygo_mn_ctrl.tracebuf != NULL &&
        pygo_mn_ctrl.tracelen + 1 < pygo_mn_ctrl.tracecap)
        pygo_mn_ctrl.tracebuf[pygo_mn_ctrl.tracelen++] = (char)('0' + (c % 10));
    pygo_cond_broadcast(&pygo_mn_ctrl.cond);
}

static void pygo_mn_ctrl_init(int n)
{
    const char *seed = getenv("PYGO_MN_SEED");
    const char *bar  = getenv("PYGO_MN_BARRIER");
    const char *trc  = getenv("PYGO_MN_TRACE");
    pygo_mn_ctrl.enabled = 0;
    pygo_mn_ctrl.tracebuf = NULL;
    pygo_mn_ctrl.tracelen = 0;
    pygo_mn_ctrl.tracecap = 0;
    pygo_mn_ctrl.tracepath[0] = '\0';
    if (seed == NULL || seed[0] == '\0' || n <= 0) return;
    pygo_mn_ctrl.want = (int *)PyMem_Calloc((size_t)n, sizeof(int));
    pygo_mn_ctrl.seen = (uint64_t *)PyMem_Calloc((size_t)n, sizeof(uint64_t));
    if (pygo_mn_ctrl.want == NULL || pygo_mn_ctrl.seen == NULL) {
        PyMem_Free(pygo_mn_ctrl.want);  pygo_mn_ctrl.want = NULL;
        PyMem_Free(pygo_mn_ctrl.seen);  pygo_mn_ctrl.seen = NULL;
        return;                                 /* OOM -> stay disabled */
    }
    pygo_mutex_init(&pygo_mn_ctrl.lock);
    pygo_cond_init(&pygo_mn_ctrl.cond);
    pygo_mn_ctrl.n = n;
    pygo_mn_ctrl.current = -1;
    pygo_mn_ctrl.armed = 0;                  /* mn_run arms after initial placement */
    pygo_mn_ctrl.round = 1;                  /* seen[] starts 0 -> nobody checked in */
    /* Barrier on by default (it's the whole point of a seed); PYGO_MN_BARRIER=0
     * reverts to the timing-dependent immediate handoff for A/B comparison. */
    pygo_mn_ctrl.barrier = (bar == NULL || bar[0] != '0');
    pygo_mn_ctrl.rng = strtoull(seed, NULL, 10);
    if (pygo_mn_ctrl.rng == 0) pygo_mn_ctrl.rng = 0x9E3779B97F4A7C15ULL;
    if (trc != NULL && trc[0] != '\0') {             /* opt-in grant trace */
        pygo_mn_ctrl.tracecap = 1u << 20;            /* 1 MiB of grants */
        pygo_mn_ctrl.tracebuf = (char *)PyMem_Malloc(pygo_mn_ctrl.tracecap);
        if (pygo_mn_ctrl.tracebuf == NULL) pygo_mn_ctrl.tracecap = 0;
        strncpy(pygo_mn_ctrl.tracepath, trc, sizeof(pygo_mn_ctrl.tracepath) - 1);
        pygo_mn_ctrl.tracepath[sizeof(pygo_mn_ctrl.tracepath) - 1] = '\0';
    }
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
    PyMem_Free(pygo_mn_ctrl.seen);
    pygo_mn_ctrl.seen = NULL;
    if (pygo_mn_ctrl.tracebuf != NULL) {
        if (pygo_mn_ctrl.tracepath[0] != '\0') {
            FILE *f = fopen(pygo_mn_ctrl.tracepath, "w");
            if (f != NULL) {
                fwrite(pygo_mn_ctrl.tracebuf, 1, pygo_mn_ctrl.tracelen, f);
                fclose(f);
            }
        }
        PyMem_Free(pygo_mn_ctrl.tracebuf);
        pygo_mn_ctrl.tracebuf = NULL;
    }
}

/* Block until this hub holds the baton.  Detaches the Python thread state
 * across the wait so a blocked hub is at a GC safepoint (free-threaded).
 * Checks in as a WANTER for the current round; a waiter's want persists across
 * rounds (carried forward by release) so it never has to re-rendezvous. */
static void pygo_mn_ctrl_acquire(int hub)
{
    if (!pygo_mn_ctrl.enabled || hub < 0 || hub >= pygo_mn_ctrl.n) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    pygo_mn_ctrl.want[hub] = 1;
    pygo_mn_ctrl.seen[hub] = pygo_mn_ctrl.round;     /* checked in (wanting) */
    pygo_mn_ctrl_try_grant();
    if (pygo_mn_ctrl.current != hub) {
        PyThreadState *ts = PyEval_SaveThread();   /* detach while we block */
        while (pygo_mn_ctrl.current != hub)
            pygo_cond_wait(&pygo_mn_ctrl.cond, &pygo_mn_ctrl.lock);
        pygo_mn_ctrl.want[hub] = 0;                 /* now running, not waiting */
        pygo_mutex_unlock(&pygo_mn_ctrl.lock);
        PyEval_RestoreThread(ts);
        return;
    }
    pygo_mn_ctrl.want[hub] = 0;
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

static void pygo_mn_ctrl_release(int hub)
{
    int i;
    if (!pygo_mn_ctrl.enabled || hub < 0 || hub >= pygo_mn_ctrl.n) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    if (pygo_mn_ctrl.current == hub) pygo_mn_ctrl.current = -1;
    /* Open the next census round.  Hubs still blocked in acquire (want[]) are
     * carried forward as already-checked-in; the hub that just ran, and any
     * idle hub, must re-check-in for the new round before the next grant -- the
     * barrier that pins the requester set. */
    pygo_mn_ctrl.round++;
    for (i = 0; i < pygo_mn_ctrl.n; i++)
        if (pygo_mn_ctrl.want[i]) pygo_mn_ctrl.seen[i] = pygo_mn_ctrl.round;
    pygo_mn_ctrl_try_grant();                       /* hand to next by seed */
    pygo_cond_broadcast(&pygo_mn_ctrl.cond);        /* wake idle re-check-ins */
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

/* Check in as IDLE for the current round (called from hub_main's no-work
 * branch).  A no-op without the barrier.  Lets the census complete when a hub
 * has no goroutine to run this round, so the grant isn't held hostage to a hub
 * that will never want the baton.
 *
 * Returns 1 if the hub actually has undrained work after all (caller must
 * re-loop and drain instead of idling), else 0.  This guard is what makes the
 * replay deterministic at sub-segment granularity: a wake from the just-ended
 * segment can land on this hub's sub_list AFTER this iteration's drain but
 * BEFORE this check-in.  Declaring idle then would let the census complete
 * without this hub and run its woken goroutine a round late -- a timing-decided
 * (hence nondeterministic) outcome, the ~1% tail Heisenbug.  We re-test the
 * sub_list under sub_lock WHILE HOLDING the controller lock: having observed the
 * current round (set under the controller lock by the release that opened it),
 * we are guaranteed to see any wake the just-ended segment published before that
 * release (segment-wake -> release happens-before round-observed -> sub re-test).
 * Lock order is controller -> sub_lock; nothing takes them the other way. */
static int pygo_mn_ctrl_census_idle(pygo_hub_t *h)
{
    int hub = h->id, has_work;
    if (!pygo_mn_ctrl.enabled || !pygo_mn_ctrl.barrier ||
        hub < 0 || hub >= pygo_mn_ctrl.n) return 0;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    if (pygo_mn_ctrl.seen[hub] == pygo_mn_ctrl.round) {
        pygo_mutex_unlock(&pygo_mn_ctrl.lock);       /* already in for this round */
        return 0;
    }
    pygo_mutex_lock(&h->sub_lock);
    has_work = (h->sub_head != NULL);
    pygo_mutex_unlock(&h->sub_lock);
    if (!has_work) {
        pygo_mn_ctrl.seen[hub] = pygo_mn_ctrl.round;   /* checked in (idle) */
        pygo_mn_ctrl_try_grant();
    }
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
    return has_work;
}

/* Startup rendezvous.  Without it, the main thread places the initial
 * goroutines (mn_go) CONCURRENTLY with hubs that are already spinning, so a hub
 * can check in idle for round 1 before its share of the work lands -> the first
 * census completes over a partial requester set and the grant is timing-
 * dependent (the very nondeterminism the barrier exists to remove, but at
 * startup).  Fix: hubs block at loop entry until mn_run arms the controller,
 * which happens AFTER all pre-run mn_go calls return.  Every hub's first drain
 * then sees the complete initial work set, so the first requester set -- and
 * thus the first seeded grant -- is a function of the program, not OS timing.
 * Work created later (by a running segment) is published at that segment's
 * release and picked up in the next round, so only startup needs the gate. */
static void pygo_mn_ctrl_arm(void)
{
    if (!pygo_mn_ctrl.enabled || !pygo_mn_ctrl.barrier) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    pygo_mn_ctrl.armed = 1;
    pygo_mn_ctrl_try_grant();                 /* in case all hubs already waiting */
    pygo_cond_broadcast(&pygo_mn_ctrl.cond);  /* release the entry gate */
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

/* Block a hub at loop entry until armed.  Detaches the Python thread state
 * across the wait (GC safepoint), like acquire. */
static void pygo_mn_ctrl_wait_armed(void)
{
    if (!pygo_mn_ctrl.enabled || !pygo_mn_ctrl.barrier) return;
    pygo_mutex_lock(&pygo_mn_ctrl.lock);
    if (!pygo_mn_ctrl.armed) {
        PyThreadState *ts = PyEval_SaveThread();
        while (!pygo_mn_ctrl.armed)
            pygo_cond_wait(&pygo_mn_ctrl.cond, &pygo_mn_ctrl.lock);
        pygo_mutex_unlock(&pygo_mn_ctrl.lock);
        PyEval_RestoreThread(ts);
        return;
    }
    pygo_mutex_unlock(&pygo_mn_ctrl.lock);
}

/* True in deterministic-replay mode (controlled + barrier): hub_main then skips
 * cross-hub work-stealing.  mn_go placement is deterministic (spawn_counter %
 * hub_count) and wake-routing is hub-pinned, so with no steal each hub's
 * execution stream is a fixed function of the schedule -- the last residual
 * that made an identical grant sequence still produce different goroutine
 * orderings.  Steal is a lock-free, sub-segment CAS race; it is covered by the
 * memory-model tools (GenMC / herd7 / lincheck), not by segment-granularity
 * replay, so dropping it here loses no coverage that lives at this level. */
PYGO_INLINE int pygo_mn_ctrl_no_steal(void)
{
    return pygo_mn_ctrl.enabled && pygo_mn_ctrl.barrier;
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
