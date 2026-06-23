/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Mark the start of a coro resume on this hub (sysmon progress beat).
 * resume_seq is bumped FIRST so a watchdog that samples mid-update sees a
 * fresh seq with a possibly-stale ns and just waits one more tick.  Inlined
 * and gated: when the watchdog is off this is a single predictable branch. */
/* ====================================================================
 * Controlled M:N scheduler (RUNLOOM_MN_SEED) -- CHESS-style serialization for
 * seeded exploration of the REAL hub/deque/steal/wake path (where single-hub
 * PCT, RUNLOOM_PCT_SEED, cannot reach).
 *
 * When RUNLOOM_MN_SEED is set, fiber *execution segments* across all hubs are
 * serialized through one baton: a hub may run a fiber (runloom_coro_resume)
 * only while it holds the baton (gated in resume_begin/end below), and a seeded
 * controller picks which waiting hub gets it next.  This trades real
 * parallelism for reproducibility + control: scheduling-order bugs (lost
 * wakeups, cross-hub wake ordering, deadlocks) become reproducible and seed-
 * sweepable on the real M:N code.
 *
 * A fiber that parks/yields/finishes returns to hub_main, which releases
 * the baton, so cooperative blocking never deadlocks it.  A CPU-bound fiber
 * that does not yield is broken off by PREEMPTION (which must stay ON -- see
 * runloom_mn_ctrl_init); disabling it wedges the baton.  While waiting a hub
 * DETACHES its tstate (PyEval_SaveThread) so it sits at a GC safepoint.
 *
 * DETERMINISTIC REPLAY (RUNLOOM_MN_BARRIER, default on under a seed): three levers
 * turn seeded exploration into reproducible replay for closed (CPU + channel /
 * lock / sync) workloads -- (1) a barrier-rendezvous census so each seeded grant
 * is over the COMPLETE requester set, not whoever raced into acquire first;
 * (2) a startup entry gate (runloom_mn_ctrl_arm, from mn_run) so the first census
 * sees all initially-placed work; (3) no work-stealing, so each hub's stream is
 * a fixed function of the schedule; (4) census_idle re-tests the sub_list before
 * declaring idle (a wake from the just-ended segment can land post-drain, and
 * declaring idle then would run that g a round late -- a timing-decided outcome);
 * (5) deterministic frame-count preemption (the eval-frame wrapper yields the
 * baton after a fixed Python-frame count, not the wall-clock sysmon flag); and
 * (6) a logical clock for sched_sleep timers (advanced only at a quiescent census
 * to the earliest pending deadline, so timer firing order is schedule-decided).
 * Same seed => identical run incl. CPU-bound segments and timers (repro_probe.py
 * 16/16, seed 1 500/500 under heavy load; repro_select.py 10/10; repro_timer.py
 * 10/10).  Open scope: real-I/O arrival timing (the open-system limit) and the
 * runloom.aio call_at timer path (not yet on the logical clock).  Formal model +
 * properties (the no-preempt deadlock and the barrier's DeterministicGrant):
 * verify/tla/RunloomMNControl.tla.  Mechanism + scope: tools/mn_controlled/README.md.
 * Zero cost when RUNLOOM_MN_SEED is unset.
 * ==================================================================== */
typedef struct {
    int          enabled;
    int          n;
    runloom_mutex_t lock;
    runloom_cond_t  cond;
    int          current;     /* hub id allowed to run a g now; -1 = free */
    int         *want;        /* per-hub: requesting the baton (waiting) */
    uint64_t     rng;
    /* Barrier-rendezvous (RUNLOOM_MN_BARRIER, default on under a seed) -- the
     * lever that turns seeded EXPLORATION into deterministic REPLAY.  Without
     * it, the controller hands the baton to whichever hubs happen to have
     * registered want[] by the instant of the choice, so the requester set
     * (and thus the seeded pick) is a function of OS thread timing.  With it,
     * the controller grants only once the requester set for the round is
     * COMPLETE -- every hub has "checked in" for the round, either as a wanter
     * (in acquire) or as idle (in hub_main's no-work branch).  The pick is then
     * a function of the schedule alone.  Formal counterpart: the Barrier
     * constant in verify/tla/RunloomMNControl.tla (DeterministicGrant). */
    int          barrier;     /* census gate on */
    int          armed;       /* mn_run reached: hubs may begin + grants may fire */
    /* Deterministic preemption (RUNLOOM_MN_PREEMPT_FRAMES, >0 under the barrier).
     * The wall-clock sysmon preempt trigger is the one remaining nondeterministic
     * input to the schedule: it fires a runaway fiber's yield at a *timing*-
     * decided frame boundary, which perturbs the grant order (observable e.g. as
     * different mn_fiber spawn-counter interleavings).  In barrier mode we ignore it
     * and instead preempt after a fixed COUNT of Python frame entries on the
     * baton -- a deterministic function of the fiber's own execution.
     * Cooperative fibers park long before the count, so they never preempt
     * (natural, deterministic schedule); a CPU-bound fiber that keeps calling
     * functions hits the count and yields the baton (liveness).  Same frame-entry
     * granularity as the wall-clock trigger (neither can break a single-frame
     * tight loop), but reproducible. */
    int          preempt_frames;  /* frame-entry budget per segment; 0 = wall-clock */
    /* Logical clock (timer determinism).  sched_sleep deadlines are computed
     * against logical_now (not wall-clock), and timers fire only when the
     * controller advances it -- which it does ONLY at a quiescent census (every
     * hub idle, none wanting the baton) to the earliest pending deadline across
     * all hubs.  So timer firing order is a function of the schedule, not of when
     * each hub's wall-clock poll happens to run.  next_deadline/have_deadline
     * collect the per-round minimum reported by hubs in census_idle. */
    double       logical_now;     /* current logical time (seconds), lock-held */
    double       next_deadline;   /* min pending deadline reported this round */
    int          have_deadline;   /* any hub reported a pending timer this round */
    uint64_t     round;       /* current census round; bumped at each release */
    uint64_t    *seen;        /* per-hub: last round this hub checked in */
    /* Grant trace (RUNLOOM_MN_TRACE=<path>).  Recorded into an in-memory byte ring
     * (one hub-id digit per grant) and dumped to the file at fini -- NOT
     * fprintf'd per grant, whose syscall/flush perturbs the very timing we are
     * trying to observe (it masked a ~1% tail race).  Near-zero per-grant cost. */
    char        *tracebuf;
    size_t       tracecap;
    size_t       tracelen;
    char         tracepath[256];
    /* PCT (RUNLOOM_MN_PCT=<depth d>) -- the seeded grant order is upgraded from
     * uniform-random (runloom_mn_ctrl_choose's else branch) to the PCT algorithm
     * of Burckhardt et al. (ASPLOS 2010): every hub gets a distinct random
     * base priority, the controller always grants to the highest-priority WANTING
     * hub, and d-1 seeded "priority change points" (placed at grant-step indices
     * in [1, pct_k]) demote the hub that ran that step below all base priorities.
     * d-1 demotions force the d-1 ordering inversions a depth-d bug needs, so any
     * such bug is hit with probability >= 1/(n * pct_k^(d-1)) per seed -- a
     * provable bound the uniform draw has no analogue of.  The pick stays a pure
     * function of (seed, schedule), so determinism / DeterministicGrant are
     * untouched: PCT only refines WHICH waiting hub the already-verified grant
     * protocol hands the baton to (the TLA Grant action leaves that a free
     * choice).  Requires the barrier (the complete requester set = PCT's enabled
     * set); off unless RUNLOOM_MN_PCT is set, then the baton's own RNG is unused
     * for choose.  A separate pct_rng stream keeps non-PCT seeds bit-identical. */
    int          pct_enabled;
    int          pct_depth;       /* d */
    uint64_t     pct_k;           /* step bound change points are drawn from */
    uint64_t     pct_rng;         /* distinct stream: priorities + change points */
    uint64_t     pct_step;        /* completed segments (= PCT steps) so far */
    int         *pct_prio;        /* per-hub current priority (higher = first) */
    int          pct_ncp;         /* number of change points (= d-1) */
    uint64_t    *pct_change_at;   /* sorted step indices that trigger a demotion */
    int         *pct_change_prio; /* demotion target priority for each (d-1..1) */
} runloom_mn_ctrl_t;
static runloom_mn_ctrl_t runloom_mn_ctrl;

static uint64_t runloom_mn_ctrl_rand(void)      /* xorshift64 */
{
    uint64_t x = runloom_mn_ctrl.rng;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    runloom_mn_ctrl.rng = x;
    return x;
}

/* PCT draws from a SEPARATE xorshift stream so enabling PCT never perturbs the
 * baton's own seeded grant RNG -- a run with RUNLOOM_MN_PCT unset stays
 * bit-identical to before this feature existed. */
static uint64_t runloom_mn_pct_rand(void)
{
    uint64_t x = runloom_mn_ctrl.pct_rng;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    runloom_mn_ctrl.pct_rng = x;
    return x;
}

/* Set up the PCT priority schedule from the baton seed.  Called by
 * runloom_mn_ctrl_init AFTER the barrier flag is decided (PCT needs the barrier
 * -- the complete requester set is its "enabled" set -- so it stays off when the
 * barrier is off).  No-op unless RUNLOOM_MN_PCT=<depth d> is set.  On OOM PCT
 * stays off and the baton's uniform-random choose is used (still a valid run). */
static void runloom_mn_pct_init(uint64_t seed, int n)
{
    const char *pct = getenv("RUNLOOM_MN_PCT");
    const char *kss = getenv("RUNLOOM_MN_PCT_STEPS");
    int d, i, ncp;
    runloom_mn_ctrl.pct_enabled = 0;
    runloom_mn_ctrl.pct_prio = NULL;
    runloom_mn_ctrl.pct_change_at = NULL;
    runloom_mn_ctrl.pct_change_prio = NULL;
    runloom_mn_ctrl.pct_ncp = 0;
    runloom_mn_ctrl.pct_step = 0;
    if (pct == NULL || pct[0] == '\0' || !runloom_mn_ctrl.barrier || n <= 0)
        return;
    d = atoi(pct);
    if (d < 1) d = 1;
    ncp = d - 1;
    runloom_mn_ctrl.pct_depth = d;
    runloom_mn_ctrl.pct_k = (kss != NULL && kss[0] != '\0')
                          ? strtoull(kss, NULL, 10) : 4096;
    if (runloom_mn_ctrl.pct_k < 1) runloom_mn_ctrl.pct_k = 1;
    /* need >= ncp distinct step indices in [1, k] for the change points */
    if (runloom_mn_ctrl.pct_k < (uint64_t)ncp) runloom_mn_ctrl.pct_k = (uint64_t)ncp;
    /* distinct PRNG stream, derived from the baton seed by a fixed mix */
    runloom_mn_ctrl.pct_rng = seed ^ 0xD1B54A32D192ED03ULL;
    if (runloom_mn_ctrl.pct_rng == 0) runloom_mn_ctrl.pct_rng = 0x2545F4914F6CDD1DULL;

    runloom_mn_ctrl.pct_prio = (int *)PyMem_Calloc((size_t)n, sizeof(int));
    if (ncp > 0) {
        runloom_mn_ctrl.pct_change_at =
            (uint64_t *)PyMem_Calloc((size_t)ncp, sizeof(uint64_t));
        runloom_mn_ctrl.pct_change_prio =
            (int *)PyMem_Calloc((size_t)ncp, sizeof(int));
    }
    if (runloom_mn_ctrl.pct_prio == NULL ||
        (ncp > 0 && (runloom_mn_ctrl.pct_change_at == NULL ||
                     runloom_mn_ctrl.pct_change_prio == NULL))) {
        PyMem_Free(runloom_mn_ctrl.pct_prio);        runloom_mn_ctrl.pct_prio = NULL;
        PyMem_Free(runloom_mn_ctrl.pct_change_at);   runloom_mn_ctrl.pct_change_at = NULL;
        PyMem_Free(runloom_mn_ctrl.pct_change_prio); runloom_mn_ctrl.pct_change_prio = NULL;
        return;                                      /* OOM -> PCT off, baton still runs */
    }

    /* Base priorities: a seeded Fisher-Yates permutation of {d, d+1, ..., d+n-1}
     * -- all >= d, hence above every change priority (which live in [1, d-1]).
     * Distinct, so highest-priority-wanter is a well-defined, tie-free pick. */
    for (i = 0; i < n; i++) runloom_mn_ctrl.pct_prio[i] = d + i;
    for (i = n - 1; i > 0; i--) {
        int j = (int)(runloom_mn_pct_rand() % (uint64_t)(i + 1));
        int t = runloom_mn_ctrl.pct_prio[i];
        runloom_mn_ctrl.pct_prio[i] = runloom_mn_ctrl.pct_prio[j];
        runloom_mn_ctrl.pct_prio[j] = t;
    }

    /* d-1 DISTINCT change points in [1, k]; sort ascending so the i-th one
     * reached gets change-priority (d-1-i) -> the descending sequence d-1, d-2,
     * ..., 1.  All < d (demoted hub sinks below every base hub) and mutually
     * distinct (demoted hubs stay strictly ordered) => choose() never ties. */
    for (i = 0; i < ncp; i++) {
        uint64_t cp;
        int dup, t;
        do {
            cp = 1 + (runloom_mn_pct_rand() % runloom_mn_ctrl.pct_k);
            dup = 0;
            for (t = 0; t < i; t++)
                if (runloom_mn_ctrl.pct_change_at[t] == cp) { dup = 1; break; }
        } while (dup);
        runloom_mn_ctrl.pct_change_at[i] = cp;
    }
    for (i = 1; i < ncp; i++) {                       /* insertion sort (ncp tiny) */
        uint64_t key = runloom_mn_ctrl.pct_change_at[i];
        int j = i - 1;
        while (j >= 0 && runloom_mn_ctrl.pct_change_at[j] > key) {
            runloom_mn_ctrl.pct_change_at[j + 1] = runloom_mn_ctrl.pct_change_at[j];
            j--;
        }
        runloom_mn_ctrl.pct_change_at[j + 1] = key;
    }
    for (i = 0; i < ncp; i++)
        runloom_mn_ctrl.pct_change_prio[i] = (d - 1) - i;   /* d-1, d-2, ..., 1 */
    runloom_mn_ctrl.pct_ncp = ncp;
    runloom_mn_ctrl.pct_enabled = 1;
}

/* lock held: pick a requesting hub, or -1 if none requesting.  PCT mode picks the
 * highest-priority wanter (the priority schedule is the seed); otherwise a single
 * seeded uniform draw over the wanters (the original behavior). */
static int runloom_mn_ctrl_choose(void)
{
    int cnt = 0, i, k;
    if (runloom_mn_ctrl.pct_enabled) {
        int best = -1, bestp = 0;
        for (i = 0; i < runloom_mn_ctrl.n; i++) {
            if (!runloom_mn_ctrl.want[i]) continue;
            if (best < 0 || runloom_mn_ctrl.pct_prio[i] > bestp) {
                bestp = runloom_mn_ctrl.pct_prio[i];
                best = i;
            }
        }
        return best;                /* -1 if none wanting; priorities are distinct */
    }
    for (i = 0; i < runloom_mn_ctrl.n; i++) cnt += runloom_mn_ctrl.want[i];
    if (cnt == 0) return -1;
    k = (int)(runloom_mn_ctrl_rand() % (uint64_t)cnt);
    for (i = 0; i < runloom_mn_ctrl.n; i++)
        if (runloom_mn_ctrl.want[i] && k-- == 0) return i;
    return -1;
}

/* lock held: have all hubs checked in for the current round? */
static int runloom_mn_ctrl_census_complete(void)
{
    int i;
    for (i = 0; i < runloom_mn_ctrl.n; i++)
        if (runloom_mn_ctrl.seen[i] != runloom_mn_ctrl.round) return 0;
    return 1;
}

/* lock held: grant the baton if it's free and (barrier ? the requester set is
 * complete : someone wants it).  Idempotent; the single seeded RNG draw per
 * grant is what makes the handoff sequence a function of the seed+schedule. */
static void runloom_mn_ctrl_try_grant(void)
{
    int c;
    if (runloom_mn_ctrl.barrier && !runloom_mn_ctrl.armed)
        return;                                      /* startup gate (below) */
    if (runloom_mn_ctrl.current != -1) return;          /* baton held */
    if (runloom_mn_ctrl.barrier && !runloom_mn_ctrl_census_complete())
        return;                                      /* set still forming */
    c = runloom_mn_ctrl_choose();
    if (c < 0) {
        /* Census complete, the baton is free, and nobody wants it -- the system
         * is quiescent.  If a timer is pending, this is the only legitimate point
         * to advance the logical clock: jump it to the earliest deadline and
         * re-open the census so the hub(s) owning a now-due timer fire it at
         * their loop top and come back as wanters.  Deterministic: the deadline
         * set and the minimum are functions of the schedule, not wall time.
         * Without a pending timer there is simply no runnable work (run is
         * draining to completion) -- do nothing. */
        if (runloom_mn_ctrl.barrier && runloom_mn_ctrl.have_deadline) {
            runloom_mn_ctrl.logical_now = runloom_mn_ctrl.next_deadline;
            runloom_mn_ctrl.have_deadline = 0;
            runloom_mn_ctrl.round++;                    /* re-open: re-report + fire */
            runloom_cond_broadcast(&runloom_mn_ctrl.cond);
        }
        return;
    }
    runloom_mn_ctrl.current = c;
    /* trace conformance: the baton is granted to waiting hub c (waiting->running) */
    runloom_mn_trace_event("Grant", c);
    /* Grant trace (opt-in, RUNLOOM_MN_TRACE): one byte into an in-memory ring, no
     * syscall on the hot path; dumped to the file at fini.  (Historically the
     * grant sequence could be byte-identical yet a run still varied -- the
     * partial-publish residual, where which g ran in a grant slot depended on
     * sub_list drain timing; that is closed by the deferred-publish-at-release
     * staging, runloom_mn_ctrl_stage_flush.) */
    if (runloom_mn_ctrl.tracebuf != NULL &&
        runloom_mn_ctrl.tracelen + 1 < runloom_mn_ctrl.tracecap)
        runloom_mn_ctrl.tracebuf[runloom_mn_ctrl.tracelen++] = (char)('0' + (c % 10));
    runloom_cond_broadcast(&runloom_mn_ctrl.cond);
}

static void runloom_mn_ctrl_init(int n)
{
    const char *seed = getenv("RUNLOOM_MN_SEED");
    const char *bar  = getenv("RUNLOOM_MN_BARRIER");
    const char *trc  = getenv("RUNLOOM_MN_TRACE");
    runloom_mn_ctrl.enabled = 0;
    runloom_mn_ctrl.tracebuf = NULL;
    runloom_mn_ctrl.tracelen = 0;
    runloom_mn_ctrl.tracecap = 0;
    runloom_mn_ctrl.tracepath[0] = '\0';
    if (seed == NULL || seed[0] == '\0' || n <= 0) return;
    runloom_mn_ctrl.want = (int *)PyMem_Calloc((size_t)n, sizeof(int));
    runloom_mn_ctrl.seen = (uint64_t *)PyMem_Calloc((size_t)n, sizeof(uint64_t));
    if (runloom_mn_ctrl.want == NULL || runloom_mn_ctrl.seen == NULL) {
        PyMem_Free(runloom_mn_ctrl.want);  runloom_mn_ctrl.want = NULL;
        PyMem_Free(runloom_mn_ctrl.seen);  runloom_mn_ctrl.seen = NULL;
        return;                                 /* OOM -> stay disabled */
    }
    runloom_mutex_init(&runloom_mn_ctrl.lock);
    runloom_cond_init(&runloom_mn_ctrl.cond);
    runloom_mn_ctrl.n = n;
    runloom_mn_ctrl.current = -1;
    runloom_mn_ctrl.armed = 0;                  /* mn_run arms after initial placement */
    runloom_mn_ctrl.logical_now = 0.0;
    runloom_mn_ctrl.next_deadline = 0.0;
    runloom_mn_ctrl.have_deadline = 0;
    runloom_mn_ctrl.round = 1;                  /* seen[] starts 0 -> nobody checked in */
    /* Barrier on by default (it's the whole point of a seed); RUNLOOM_MN_BARRIER=0
     * reverts to the timing-dependent immediate handoff for A/B comparison. */
    runloom_mn_ctrl.barrier = (bar == NULL || bar[0] != '0');
    /* Deterministic frame-count preemption: on (default 4096) under the barrier,
     * off (0 -> wall-clock) without it.  A cooperative fiber parks in far
     * fewer than 4096 frame entries, so it never trips; the value only sets how
     * promptly a CPU-bound fiber yields the baton. */
    if (runloom_mn_ctrl.barrier) {
        const char *pf = getenv("RUNLOOM_MN_PREEMPT_FRAMES");
        runloom_mn_ctrl.preempt_frames = (pf != NULL) ? atoi(pf) : 4096;
        if (runloom_mn_ctrl.preempt_frames < 0) runloom_mn_ctrl.preempt_frames = 0;
    } else {
        runloom_mn_ctrl.preempt_frames = 0;
    }
    runloom_mn_ctrl.rng = strtoull(seed, NULL, 10);
    if (runloom_mn_ctrl.rng == 0) runloom_mn_ctrl.rng = 0x9E3779B97F4A7C15ULL;
    /* PCT priority schedule (RUNLOOM_MN_PCT=<depth>): refines choose() from the
     * uniform draw above to a bug-depth-guaranteed order.  Needs the barrier
     * (decided just above), so init it here. */
    runloom_mn_pct_init(strtoull(seed, NULL, 10), n);
    if (trc != NULL && trc[0] != '\0') {             /* opt-in grant trace */
        runloom_mn_ctrl.tracecap = 1u << 20;            /* 1 MiB of grants */
        runloom_mn_ctrl.tracebuf = (char *)PyMem_Malloc(runloom_mn_ctrl.tracecap);
        if (runloom_mn_ctrl.tracebuf == NULL) runloom_mn_ctrl.tracecap = 0;
        strncpy(runloom_mn_ctrl.tracepath, trc, sizeof(runloom_mn_ctrl.tracepath) - 1);
        runloom_mn_ctrl.tracepath[sizeof(runloom_mn_ctrl.tracepath) - 1] = '\0';
    }
    runloom_mn_ctrl.enabled = 1;
    /* Preemption stays ON (liveness backstop, proven necessary by
     * verify/tla/RunloomMNControl.tla's Preempt=FALSE control): a fiber that
     * runs Python without yielding would otherwise hold the baton forever and
     * wedge every other hub.  Under the barrier the trigger is the DETERMINISTIC
     * frame counter above (not the wall-clock watchdog), so a runaway fiber
     * is still broken off -- at a reproducible frame -> resume_end -> baton
     * released -> progress.  The eval-frame wrapper must be installed for this,
     * so leave runloom_preempt_enabled as the sysmon config set it (default on). */
}

static void runloom_mn_ctrl_fini(void)
{
    if (!runloom_mn_ctrl.enabled) return;
    runloom_mn_ctrl.enabled = 0;
    runloom_cond_destroy(&runloom_mn_ctrl.cond);
    runloom_mutex_destroy(&runloom_mn_ctrl.lock);
    PyMem_Free(runloom_mn_ctrl.want);
    runloom_mn_ctrl.want = NULL;
    PyMem_Free(runloom_mn_ctrl.seen);
    runloom_mn_ctrl.seen = NULL;
    PyMem_Free(runloom_mn_ctrl.pct_prio);
    runloom_mn_ctrl.pct_prio = NULL;
    PyMem_Free(runloom_mn_ctrl.pct_change_at);
    runloom_mn_ctrl.pct_change_at = NULL;
    PyMem_Free(runloom_mn_ctrl.pct_change_prio);
    runloom_mn_ctrl.pct_change_prio = NULL;
    runloom_mn_ctrl.pct_enabled = 0;
    if (runloom_mn_ctrl.tracebuf != NULL) {
        if (runloom_mn_ctrl.tracepath[0] != '\0') {
            FILE *f = fopen(runloom_mn_ctrl.tracepath, "w");
            if (f != NULL) {
                fwrite(runloom_mn_ctrl.tracebuf, 1, runloom_mn_ctrl.tracelen, f);
                fclose(f);
            }
        }
        PyMem_Free(runloom_mn_ctrl.tracebuf);
        runloom_mn_ctrl.tracebuf = NULL;
    }
}

/* Block until this hub holds the baton.  Detaches the Python thread state
 * across the wait so a blocked hub is at a GC safepoint (free-threaded).
 * Checks in as a WANTER for the current round; a waiter's want persists across
 * rounds (carried forward by release) so it never has to re-rendezvous. */
static void runloom_mn_ctrl_acquire(int hub)
{
    if (!runloom_mn_ctrl.enabled || hub < 0 || hub >= runloom_mn_ctrl.n) return;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    runloom_mn_ctrl.want[hub] = 1;
    runloom_mn_ctrl.seen[hub] = runloom_mn_ctrl.round;     /* checked in (wanting) */
    /* trace conformance: idle -> arriving -> waiting (acquire is atomic here, so
     * the two model steps are logged back-to-back). */
    runloom_mn_trace_event("Arrive", hub);
    runloom_mn_trace_event("Rendezvous", hub);
    runloom_mn_ctrl_try_grant();
    if (runloom_mn_ctrl.current != hub) {
        PyThreadState *ts = PyEval_SaveThread();   /* detach while we block */
        while (runloom_mn_ctrl.current != hub)
            runloom_cond_wait(&runloom_mn_ctrl.cond, &runloom_mn_ctrl.lock);
        runloom_mn_ctrl.want[hub] = 0;                 /* now running, not waiting */
        RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
        PyEval_RestoreThread(ts);
        return;
    }
    runloom_mn_ctrl.want[hub] = 0;
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
}

static void runloom_mn_ctrl_release(int hub)
{
    int i;
    if (!runloom_mn_ctrl.enabled || hub < 0 || hub >= runloom_mn_ctrl.n) return;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    if (runloom_mn_ctrl.current == hub) {
        runloom_mn_ctrl.current = -1;
        /* trace conformance: the holder releases the baton (running->idle) */
        runloom_mn_trace_event("Release", hub);
    }
    /* PCT: this hub just finished one execution segment = one PCT step.  If the
     * step index is a change point, demote this hub below all base priorities so
     * a different hub wins the next choose() -- the seeded ordering inversion. */
    if (runloom_mn_ctrl.pct_enabled) {
        int j;
        runloom_mn_ctrl.pct_step++;
        for (j = 0; j < runloom_mn_ctrl.pct_ncp; j++)
            if (runloom_mn_ctrl.pct_change_at[j] == runloom_mn_ctrl.pct_step)
                runloom_mn_ctrl.pct_prio[hub] = runloom_mn_ctrl.pct_change_prio[j];
    }
    /* Open the next census round.  Hubs still blocked in acquire (want[]) are
     * carried forward as already-checked-in; the hub that just ran, and any
     * idle hub, must re-check-in for the new round before the next grant -- the
     * barrier that pins the requester set. */
    runloom_mn_ctrl.round++;
    runloom_mn_ctrl.have_deadline = 0;                 /* re-collect deadlines this round */
    for (i = 0; i < runloom_mn_ctrl.n; i++)
        if (runloom_mn_ctrl.want[i]) runloom_mn_ctrl.seen[i] = runloom_mn_ctrl.round;
    runloom_mn_ctrl_try_grant();                       /* hand to next by seed */
    runloom_cond_broadcast(&runloom_mn_ctrl.cond);        /* wake idle re-check-ins */
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
}

/* Check in as IDLE for the current round (called from hub_main's no-work
 * branch).  A no-op without the barrier.  Lets the census complete when a hub
 * has no fiber to run this round, so the grant isn't held hostage to a hub
 * that will never want the baton.
 *
 * Returns 1 if the hub actually has undrained work after all (caller must
 * re-loop and drain instead of idling), else 0.  This guard is what makes the
 * replay deterministic at sub-segment granularity: a wake from the just-ended
 * segment can land on this hub's sub_list AFTER this iteration's drain but
 * BEFORE this check-in.  Declaring idle then would let the census complete
 * without this hub and run its woken fiber a round late -- a timing-decided
 * (hence nondeterministic) outcome, the ~1% tail Heisenbug.  We re-test the
 * sub_list under sub_lock WHILE HOLDING the controller lock: having observed the
 * current round (set under the controller lock by the release that opened it),
 * we are guaranteed to see any wake the just-ended segment published before that
 * release (segment-wake -> release happens-before round-observed -> sub re-test).
 * Lock order is controller -> sub_lock; nothing takes them the other way. */
static int runloom_mn_ctrl_census_idle(runloom_hub_t *h)
{
    int hub = h->id, has_work;
    if (!runloom_mn_ctrl.enabled || !runloom_mn_ctrl.barrier ||
        hub < 0 || hub >= runloom_mn_ctrl.n) return 0;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    if (runloom_mn_ctrl.seen[hub] == runloom_mn_ctrl.round) {
        RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);       /* already in for this round */
        return 0;
    }
    RUNLOOM_RLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
    has_work = (h->sub_head != NULL);
    RUNLOOM_RUNLOCK(&h->sub_lock, RUNLOOM_RANK_HUB_SUB);
    if (!has_work) {
        runloom_mn_ctrl.seen[hub] = runloom_mn_ctrl.round;   /* checked in (idle) */
        /* Report this hub's earliest pending timer so a quiescent census can
         * advance the logical clock to it (this hub owns its sleep heap, so the
         * peek is race-free even under the controller lock). */
        if (h->sched.sleep_size > 0) {
            double d = runloom_sched_sleep_peek(&h->sched)->wake_at;
            if (!runloom_mn_ctrl.have_deadline || d < runloom_mn_ctrl.next_deadline)
                runloom_mn_ctrl.next_deadline = d;
            runloom_mn_ctrl.have_deadline = 1;
        }
        runloom_mn_ctrl_try_grant();
    }
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    return has_work;
}

/* Startup rendezvous.  Without it, the main thread places the initial
 * fibers (mn_fiber) CONCURRENTLY with hubs that are already spinning, so a hub
 * can check in idle for round 1 before its share of the work lands -> the first
 * census completes over a partial requester set and the grant is timing-
 * dependent (the very nondeterminism the barrier exists to remove, but at
 * startup).  Fix: hubs block at loop entry until mn_run arms the controller,
 * which happens AFTER all pre-run mn_fiber calls return.  Every hub's first drain
 * then sees the complete initial work set, so the first requester set -- and
 * thus the first seeded grant -- is a function of the program, not OS timing.
 * Work created later (by a running segment) is published at that segment's
 * release and picked up in the next round, so only startup needs the gate. */
static void runloom_mn_ctrl_arm(void)
{
    if (!runloom_mn_ctrl.enabled || !runloom_mn_ctrl.barrier) return;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    runloom_mn_ctrl.armed = 1;
    runloom_mn_ctrl_try_grant();                 /* in case all hubs already waiting */
    runloom_cond_broadcast(&runloom_mn_ctrl.cond);  /* release the entry gate */
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
}

/* Block a hub at loop entry until armed.  Detaches the Python thread state
 * across the wait (GC safepoint), like acquire. */
static void runloom_mn_ctrl_wait_armed(void)
{
    if (!runloom_mn_ctrl.enabled || !runloom_mn_ctrl.barrier) return;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    if (!runloom_mn_ctrl.armed) {
        PyThreadState *ts = PyEval_SaveThread();
        while (!runloom_mn_ctrl.armed)
            runloom_cond_wait(&runloom_mn_ctrl.cond, &runloom_mn_ctrl.lock);
        RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
        PyEval_RestoreThread(ts);
        return;
    }
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
}

/* True in deterministic-replay mode (controlled + barrier): hub_main then skips
 * cross-hub work-stealing.  mn_fiber placement is deterministic (spawn_counter %
 * hub_count) and wake-routing is hub-pinned, so with no steal each hub's
 * execution stream is a fixed function of the schedule -- closing one way an
 * identical grant sequence could still produce different fiber orderings
 * (the other, partial cross-hub publish, is closed by the deferred-publish
 * staging, runloom_mn_ctrl_stage_flush).  Steal is a lock-free, sub-segment CAS
 * race; it is covered by the
 * memory-model tools (GenMC / herd7 / lincheck), not by segment-granularity
 * replay, so dropping it here loses no coverage that lives at this level. */
RUNLOOM_INLINE int runloom_mn_ctrl_no_steal(void)
{
    return runloom_mn_ctrl.enabled && runloom_mn_ctrl.barrier;
}

/* The clock sched_sleep deadlines and hub_main timer firing read: the logical
 * clock in controlled+barrier mode, else the wall-clock `fallback` the caller
 * passes.  Read under the controller lock (logical_now is written there at a
 * clock advance) so it is race-free; controlled mode only, so the lock cost is
 * irrelevant.  Callable across translation units (m_sched_sleep is in module.c). */
double runloom_mn_logical_now_or(double fallback)
{
    double v;
    if (!(runloom_mn_ctrl.enabled && runloom_mn_ctrl.barrier)) return fallback;
    RUNLOOM_RLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    v = runloom_mn_ctrl.logical_now;
    RUNLOOM_RUNLOCK(&runloom_mn_ctrl.lock, RUNLOOM_RANK_MN_CTRL);
    return v;
}

/* Python frame entries on the baton since this segment began (deterministic
 * preemption counter; controlled+barrier only).  TLS = per hub thread, and only
 * the baton holder runs Python, so this counts exactly the running segment's
 * frames.  Reset at each resume_begin, bumped in the eval-frame wrapper. */
static RUNLOOM_TLS int runloom_mn_preempt_ctr = 0;

/* Publish this hub's deferred cross-hub submits (barrier-mode deterministic
 * replay).  A cross-hub submit made DURING a running segment was staged per
 * target on the spawning hub (runloom_mn_hub_submit) instead of pushed
 * immediately; here we splice each target's staged list onto its sub_list in ONE
 * lock-held operation, so the target's loop-top drain observes the COMPLETE set
 * the segment produced -- never a partial mid-segment snapshot.  Called at
 * resume_end BEFORE the baton release, so the work is published (and visible via
 * the target's sub_lock acquire) before the round that release opens.  No-op
 * (one predicted-not-taken branch) when nothing was staged -- i.e. always in the
 * default scheduler, where stage_head is NULL. */
static void runloom_mn_ctrl_stage_flush(runloom_hub_t *h)
{
    int t;
    if (h->stage_head == NULL || h->stage_pending == 0) return;
    for (t = 0; t < runloom_hub_count; t++) {
        runloom_g_t *head = h->stage_head[t];
        runloom_g_t *tail = h->stage_tail[t];
        runloom_hub_t *tgt;
        if (head == NULL) continue;
        h->stage_head[t] = h->stage_tail[t] = NULL;
        tgt = &runloom_hubs[t];
        RUNLOOM_RLOCK(&tgt->sub_lock, RUNLOOM_RANK_HUB_SUB);
        if (tgt->sub_tail != NULL) tgt->sub_tail->next = head;
        else /* RELEASE: publishes to the census ACQUIRE read (Finding D #4) */
            __atomic_store_n(&tgt->sub_head, head, __ATOMIC_RELEASE);
        tgt->sub_tail = tail;
        RUNLOOM_RUNLOCK(&tgt->sub_lock, RUNLOOM_RANK_HUB_SUB);
    }
    h->stage_pending = 0;
}

RUNLOOM_INLINE void runloom_hub_resume_begin(runloom_hub_t *h, runloom_g_t *g)
{
    if (__builtin_expect(runloom_sysmon_enabled, 0)) {
        /* Relaxed-atomic like its two siblings below: the sysmon thread reads
         * resume_g concurrently for the wedge log (best-effort sample). */
        __atomic_store_n((void **)&h->resume_g, (void *)g, __ATOMIC_RELAXED);
        __atomic_add_fetch(&h->resume_seq, 1, __ATOMIC_RELAXED);
        __atomic_store_n(&h->resume_start_ns, runloom_monotonic_ns(),
                         __ATOMIC_RELAXED);
    }
    /* controlled mode: block until this hub holds the execution baton */
    runloom_mn_ctrl_acquire(h->id);
    runloom_mn_preempt_ctr = 0;     /* fresh frame budget for this segment */
}

/* Mark the end of a coro resume: clear resume_start_ns so the watchdog
 * stops counting this hub as "in a resume". */
RUNLOOM_INLINE void runloom_hub_resume_end(runloom_hub_t *h)
{
    if (__builtin_expect(runloom_sysmon_enabled, 0)) {
        __atomic_store_n(&h->resume_start_ns, 0, __ATOMIC_RELAXED);
    }
    /* Publish this segment's deferred cross-hub submits ATOMICALLY (per target)
     * before releasing the baton, so the next round sees a complete batch -- the
     * deterministic-replay fix for the partial-snapshot residual.  No-op (NULL
     * stage) in the default scheduler. */
    runloom_mn_ctrl_stage_flush(h);
    /* controlled mode: hand the baton to the next hub (seeded) */
    runloom_mn_ctrl_release(h->id);
}

/* Interpreter for per-g PyThreadState_New, captured at mn_init. */
static PyInterpreterState *runloom_mn_interp = NULL;

/* TLS pointers set at hub_main entry.  runloom_mn_yield_current() and
 * runloom_mn_current_hub() read these to route per-g operations to the
 * right hub without each call site needing to look it up. */
static RUNLOOM_TLS runloom_hub_t *runloom_tls_hub = NULL;
static RUNLOOM_TLS runloom_g_t   *runloom_tls_current_g = NULL;
/* Set by runloom_mn_yield_current() before runloom_coro_yield(); read by
 * hub_main after runloom_coro_resume returns.  Tells hub_main "the g
 * has already put itself on a queue, you don't need to requeue".
 * Distinguishes scheduler-aware yield (sched_yield) from raw yield
 * (runloom_c.yield_() -> runloom_coro_yield directly). */
static RUNLOOM_TLS int runloom_tls_self_queued = 0;
/* Set ONLY by runloom_mn_tls_mark_parked (the off-queue park path: netpoll /
 * chan / sleep all route through it).  Distinguishes a g that parked off-queue
 * -- so a wake_g will route it back, and it must NOT be re-pushed -- from a
 * cooperative yield_current (self_queued too, but it already pushed itself to
 * the local FIFO).  Read by hub_main's per-g-tstate release to decide PARKED
 * (wakeable) vs RUNNING (hub-pinned) for the wake state machine.  Unused by the
 * default scheduler, which keys re-push purely on self_queued. */
static RUNLOOM_TLS int runloom_tls_parked_offqueue = 0;
/* Consecutive trivial sched_yield fast-paths on THIS hub thread since the last
 * real yield (see runloom_mn_yield_current).  Bounds the fast path so a g looping
 * on sched_yield can't spin forever without returning to hub_main to drain
 * h->sub_head (work mn_fiber'd / wake_g'd onto this hub after the loop started).
 * Per-thread; only the running g touches it; reset on every real yield. */
static RUNLOOM_TLS int runloom_tls_fastpath_streak = 0;
/* After this many consecutive trivial fast-paths, force one real yield so
 * hub_main drains sub_head + re-polls.  Large enough that a genuine lone poller
 * rarely pays the round-trip; small enough that newly-arrived work waits only
 * microseconds. */
#define RUNLOOM_YIELD_FASTPATH_BOUND 64

#if PY_VERSION_HEX >= 0x030D0000
/* RUNLOOM_PREEMPT eval-frame wrapper (see the block near runloom_preempt_enabled).
 * Chains to the previous eval-frame func (normally _PyEval_EvalFrameDefault) so
 * it composes with any other installed wrapper; _PyInterpreterFrame is opaque
 * (passed straight through). */
static _PyFrameEvalFunction runloom_preempt_prev_eval = NULL;

static PyObject *runloom_preempt_eval_frame(PyThreadState *ts,
                                         struct _PyInterpreterFrame *frame,
                                         int throwflag)
{
    runloom_hub_t *h = runloom_tls_hub;
    runloom_g_t *g;
    int do_preempt;
    /* Decide whether to yield the baton at this frame entry.  In controlled
     * barrier mode the trigger is DETERMINISTIC -- a fixed frame-entry count on
     * the baton -- and the wall-clock watchdog flag is ignored (it would fire at
     * a timing-decided frame, perturbing the grant order).  Otherwise (default
     * scheduler, or barrier off) it is the wall-clock watchdog flag as before. */
    if (h != NULL && runloom_mn_ctrl.preempt_frames > 0) {
        do_preempt = (++runloom_mn_preempt_ctr >= runloom_mn_ctrl.preempt_frames);
    } else {
        do_preempt = (h != NULL &&
                      __atomic_load_n(&h->preempt_requested, __ATOMIC_RELAXED));
    }
    /* Never yield the fiber while CPython is mid object-destruction on this
     * tstate -- a tp_dealloc and its weakref callbacks / finalizers, driven by
     * the trashcan unwind (tstate->delete_later) or the free-threaded biased-
     * refcount cross-thread merge drain (brc.local_objects_to_merge).
     * Suspending there freezes a half-finished destructor on the g's coro stack
     * while the hub thread returns to hub_main (a GC-safe point); a concurrent
     * stop-the-world GC / QSBR reclaim (e.g. another thread's gc.collect())
     * then runs against partially-destroyed objects -> use-after-free SIGSEGV
     * (test_weakref.test_threaded_weak_key_dict_copy).  Cooperative yields never
     * hit this: they only happen at Python-level call points, never nested
     * inside a C destructor.  The trigger is left ARMED (frame counter not
     * reset / preempt_requested not cleared) so the very next frame entry after
     * the destructor unwinds takes the yield -- preemption is deferred a few
     * frames, never lost. */
    if (do_preempt && runloom_tstate_in_destruction(ts)) {
        do_preempt = 0;
    } else if (do_preempt) {
        if (runloom_mn_ctrl.preempt_frames > 0) runloom_mn_preempt_ctr = 0;
        else __atomic_store_n(&h->preempt_requested, 0, __ATOMIC_RELAXED);
    }
    if (h != NULL && do_preempt && (g = runloom_tls_current_g) != NULL) {
        /* The trigger (frame-count or watchdog flag) was already consumed above,
         * so one trigger => one yield; the wall-clock watchdog re-arms each tick
         * the wedge persists, the frame counter re-fills from the next resume.
         * The SNAP-saving cooperative yield (runloom_mn_yield_current's body MINUS
         * its trivial-switch fast path).  Two reasons not to reuse that helper:
         * (1) raw runloom_coro_yield would re-resume off a STALE g->snap (the g's
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
#if defined(RUNLOOM_FSM_VALIDATE)
        /* Runtime witness of the p69b invariant proven in
         * verify/cbmc/preempt_defer_cbmc.c: the gate above cleared do_preempt iff
         * mid-destruction, so this yield is UNREACHABLE while in_destruction.
         * Abort loudly if that ever fails to hold. */
        if (runloom_tstate_in_destruction(ts))
            runloom_fsm_violation("preempt_gate", 1 /*about-to-yield*/, 0,
                                  __FILE__, __LINE__);
#endif
        runloom_sched_ready_push(&h->sched, g);
        runloom_pystate_snap(&g->snap);
        runloom_tls_self_queued = 1;
        runloom_coro_yield();
    }
    /* Acquire-load: install/uninstall (other thread, or main during the
     * init/fini windows while hub gs already run Python) writes this pointer
     * with release; a plain read on the per-frame hot path was a data race. */
    {
        /* Cast through void** for the MSVC atomic shim's _Generic (it can't
         * name _PyFrameEvalFunction in the shared macro -- some TUs lack the
         * Python headers); a no-op on GCC's type-generic builtin. */
        _PyFrameEvalFunction prev = (_PyFrameEvalFunction)
            __atomic_load_n((void **)&runloom_preempt_prev_eval, __ATOMIC_ACQUIRE);
        return prev(ts, frame, throwflag);
    }
}
#endif

/* Controlled-scheduler liveness backstop (Feature 1, single-frame loops).
 * Frame-count preemption (lever 5) and the eval-frame wrapper both act only at a
 * Python FRAME boundary, so neither can break a fiber spinning in ONE frame
 * with no calls (`while not flag: pass`).  Under serialization (no parallel
 * sibling) such a fiber holds the baton forever and deadlocks every other
 * hub.  This Py_AddPendingCall callback rides CPython's eval-breaker -- checked
 * on BACKWARD JUMPS *inside* a frame -- so it interrupts even a call-less loop.
 * It runs on the baton holder (the only thread executing Python in controlled
 * mode) and yields that fiber UNCONDITIONALLY: the same body as the
 * eval-frame wrapper, NOT runloom_mn_yield_current (whose fast path would bail here
 * -- this hub's own queues are empty, the spinner is the only local g, and the
 * other hubs are blocked in acquire, not in our queues).  Posted by the sysmon
 * whenever a resume wedges past the watchdog threshold (default scheduler as
 * well as controlled -- a single-frame loop on the only runnable hub starves
 * its siblings no matter the mode).  Frame-bearing code is already broken off
 * by the eval-frame wrapper before its frames re-enter, so in practice only a
 * genuine single-frame loop is still spinning when this rides its backward-jump
 * check; if it does fire for frame-bearing code the extra yield is harmless (the
 * g was about to yield at the wrapper anyway).  No-op when no fiber is
 * running on this hub. */
/* Per-thread pending call (flags=0 -> runs on ANY thread's eval-breaker, not
 * just the main thread's, which is what the public Py_AddPendingCall is pinned
 * to in 3.13 -- and the main thread is parked in mn_run, so it would never
 * reach the hub running the loop).  Private-but-stable, like the eval-frame-func
 * setters runloom already uses; declared here to avoid pulling in pycore headers.
 * The 4-arg form (with flags) is 3.12+; on older builds the backstop is absent
 * and single-frame loops stay the documented controlled-mode limitation. */
#if PY_VERSION_HEX >= 0x030C0000
#define RUNLOOM_MN_LIVENESS_BACKSTOP 1
extern int _PyEval_AddPendingCall(PyInterpreterState *interp,
                                  int (*func)(void *), void *arg, int flags);

static int runloom_mn_liveness_pending_cb(void *arg)
{
    runloom_hub_t *h = runloom_tls_hub;
    runloom_g_t *g;
    (void)arg;
    /* Same destruction guard as the eval-frame wrapper: a yield mid tp_dealloc
     * freezes a half-finished destructor at a GC-safe point (use-after-free).
     * A single-frame loop -- the only thing this backstop targets -- can't be
     * inside a C destructor, so this is belt-and-suspenders; if it ever does
     * fire there, skip and let the sysmon re-post once destruction unwinds. */
    if (runloom_tstate_in_destruction(PyThreadState_GET())) {
        return 0;
    }
    if (h != NULL && (g = runloom_tls_current_g) != NULL) {
#if defined(RUNLOOM_FSM_VALIDATE)
        /* Same p69b witness as the eval-frame wrapper: the guard at the top of
         * this callback returned already if in_destruction, so this is unreachable
         * mid-destruction (verify/cbmc/preempt_defer_cbmc.c). */
        if (runloom_tstate_in_destruction(PyThreadState_GET()))
            runloom_fsm_violation("preempt_gate", 1 /*about-to-yield*/, 0,
                                  __FILE__, __LINE__);
#endif
        runloom_sched_ready_push(&h->sched, g);
        runloom_pystate_snap(&g->snap);
        runloom_tls_self_queued = 1;
        runloom_coro_yield();
    }
    return 0;
}
#endif

/* Install/uninstall the preemption eval-frame wrapper on `interp`.  Install
 * runs in mn_init after runloom_sysmon_config (so runloom_preempt_enabled is set) and
 * BEFORE any fiber runs Python; uninstall runs in mn_fini.  No-ops unless
 * RUNLOOM_PREEMPT (and 3.13+).  Capturing the previous func means we chain rather
 * than clobber a wrapper someone else installed. */
static void runloom_preempt_install(PyInterpreterState *interp)
{
#if PY_VERSION_HEX >= 0x030D0000
    if (!runloom_preempt_enabled) return;
    /* Release-store so the eval hook's acquire-load sees a fully-published
     * prev pointer; pairs with the load in runloom_preempt_eval_frame. */
    __atomic_store_n((void **)&runloom_preempt_prev_eval,
                     (void *)_PyInterpreterState_GetEvalFrameFunc(interp),
                     __ATOMIC_RELEASE);
    _PyInterpreterState_SetEvalFrameFunc(interp, runloom_preempt_eval_frame);
#else
    (void)interp;
#endif
}

static void runloom_preempt_uninstall(PyInterpreterState *interp)
{
#if PY_VERSION_HEX >= 0x030D0000
    {
        /* Cast through void** for the MSVC atomic shim's _Generic (it can't
         * name _PyFrameEvalFunction in the shared macro -- some TUs lack the
         * Python headers); a no-op on GCC's type-generic builtin. */
        _PyFrameEvalFunction prev = (_PyFrameEvalFunction)
            __atomic_load_n((void **)&runloom_preempt_prev_eval, __ATOMIC_ACQUIRE);
        if (!runloom_preempt_enabled || prev == NULL) return;
        _PyInterpreterState_SetEvalFrameFunc(interp, prev);
        __atomic_store_n((void **)&runloom_preempt_prev_eval, (void *)NULL, __ATOMIC_RELEASE);
    }
#else
    (void)interp;
#endif
}
