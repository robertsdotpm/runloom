/* Part of mn_sched.c -- #included into that translation unit, not compiled
 * standalone.  See mn_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

RUNLOOM_INLINE void runloom_mn_pending_inc(runloom_hub_t *h)
{
    /* spawn: bump THIS hub's pending only -- sharded, no global counter.
     * runloom_mn_run sums the per-hub fields.  A single global counter here was a
     * contended cache line that serialized parallel spawn (every spawn on every
     * core hit one line -> negative scaling).  RELEASE so the mn_run reader's
     * ACQUIRE sum observes the spawn before the g can run/complete.  No false 0
     * during a concurrent spawn: the SPAWNER is itself a live counted g (its
     * hub's pending >= 1), so the sum can't reach 0 while a spawn is in flight.
     * The work-stealing race is handled in runloom_mn_pending_steal. */
    __atomic_add_fetch(&h->pending, 1, __ATOMIC_RELEASE);
}

RUNLOOM_INLINE void runloom_mn_pending_complete(runloom_hub_t *h)
{
    /* complete: bump completed, drop THIS hub's pending.  No global counter --
     * runloom_mn_run sums the per-hub pending fields.  RELEASE on the dec so the
     * reader's ACQUIRE sum observes the completion (ordered after completed++). */
    __atomic_add_fetch(&h->sched.completed, 1, __ATOMIC_RELEASE);
    __atomic_sub_fetch(&h->pending, 1, __ATOMIC_RELEASE);
}

RUNLOOM_INLINE void runloom_mn_pending_steal(runloom_hub_t *victim,
                                              runloom_hub_t *thief)
{
    /* steal: work moves victim -> thief, total unchanged.  Bump the THIEF
     * BEFORE dropping the VICTIM so a concurrent runloom_mn_run sum sees the
     * stolen g transiently OVER-counted (on both hubs), NEVER under-counted (on
     * neither).  An over-count is harmless -- mn_run just polls again -- but a
     * transient sum of 0 would let run() exit while a stolen g is still live.
     * This reorder is what makes the sharded per-hub pending sum safe: the old
     * single global counter existed ONLY to dodge this steal race (see the note
     * at the steal call site in hub_main), and the reorder removes the need for
     * it.  ACQ_REL so the stolen g's writes are visible to the destination hub
     * before its later resume. */
    __atomic_add_fetch(&thief->pending,  1, __ATOMIC_ACQ_REL);
    __atomic_sub_fetch(&victim->pending, 1, __ATOMIC_ACQ_REL);
}

/* ---- Global stealable run-queue (RUNLOOM_PER_G_TSTATE only) ----
 *
 * Default mode routes a woken g back to its ORIGIN hub's owner-drained
 * submission list (runloom_mn_hub_submit).  That strands the g whenever the
 * origin hub is stuck in a non-yielding blocking C call (the classic one:
 * libc getaddrinfo): the hub never loops, never drains its sub list, so the
 * woken g never reaches a stealable deque and no idle hub can rescue it.
 * This is asyncio's signature failure -- one blocking task stalling a whole
 * fan-out -- reproduced in runloom's narrow blocking-C-call case.
 *
 * Under RUNLOOM_PER_G_TSTATE a woken g carries its own migratable
 * PyThreadState, so it can resume on ANY hub.  We exploit that: wake_g
 * pushes the woken g onto this process-global queue instead of the origin
 * hub's sub list, and every idle hub drains it in hub_main's empty-local/
 * empty-deque path (alongside neighbour-steal).  An idle hub thus recovers a
 * stalled hub's woken work.  This is Go's per-P-local + global-runq model.
 *
 * Accounting: the g stays counted in its origin hub's `pending` and the
 * global counter while it sits here (we do NOT rebalance per-hub pending on
 * pull).  That is correct because the idle path only ever reads the SUM of
 * per-hub pending, and the sum is conserved regardless of which hub holds the
 * count: spawn does +1 on one hub, completion does -1 on one hub, so the sum
 * always equals the global in-flight count.  runloom_mn_run reads the global
 * counter, which a queued g keeps non-zero until it actually completes.
 *
 * Single-owner safety (the load-bearing part).  ONE per-g state machine
 * (g->wake_state, see the field comment in runloom_sched.h) makes the MPMC queue
 * safe without per-entry refcounting and without the duplicate/double/lost-wake
 * hazards.  An earlier two-flag design (an exactly-once-wake dedup + a separate
 * exclusive-resume claim, with a re-push on claim contention) raced into a
 * livelock: the re-pushed entry survived the owner's next park (which re-armed
 * the wake flag), so a fresh wake made a SECOND entry, and the two entries
 * double-claimed and stranded the claim -> the re-push spun forever.  The state
 * machine removes BOTH the re-push and the possibility of two entries:
 *
 *   - wake_g CASes PARKED -> QUEUED and only the winner enqueues; a wake while
 *     the g is QUEUED, RUNNING, or RUNNING_WOKEN does NOT enqueue.  So a g holds
 *     AT MOST ONE entry, ever -- no stale duplicates, no spurious/double resume.
 *   - a hub CASes QUEUED -> RUNNING to claim the g it pulled.  Because the entry
 *     it pulled is the queue's SOLE reference to that g (at-most-one-entry) and
 *     only the entry's holder runs this CAS, the CAS cannot lose -> no re-push.
 *   - while RUNNING the g is owned by exactly one hub; a wake only flips RUNNING
 *     -> RUNNING_WOKEN (remembered, not enqueued), so no other hub can pull or
 *     attach the g's live tstate during the commit->detach window.  The owner,
 *     after detaching the tstate, ends the resume by CASing RUNNING -> PARKED,
 *     or, if a wake landed, RUNNING_WOKEN -> QUEUED and enqueues exactly once.
 *
 * "One entry per park" and "one resumer" are now the SAME invariant (QUEUED
 * holds the entry; RUNNING holds the owner; they are distinct states), which is
 * why no re-push is needed and no duplicate can form.  Each entry takes one
 * queue ref (incref at enqueue, decref at consume/drain); at most one entry
 * references a g, so the proven sub_list/deque single-ref lifetime model holds.
 *
 * MPMC: many producers (any thread's wake_g) and many consumers (idle hubs).
 * Mutex-protected singly-linked list through g->next -- the same field the
 * per-hub sub list uses, safe because a given g is in exactly one of {sub
 * list, global runq} at a time (a fresh g goes to its hub's sub list; a woken
 * g under per-g-tstate comes here, long after it left the sub list).
 *
 * Process-lifetime lock: POSIX gets a static initialiser (usable with no
 * init/destroy, survives mn_init/mn_fini cycles); Windows can't statically
 * init a CRITICAL_SECTION, so mn_init does a one-time init there. */
static runloom_mutex_t  runloom_global_runq_lock = RUNLOOM_MUTEX_STATIC_INIT;
static runloom_g_t     *runloom_global_runq_head = NULL;
static runloom_g_t     *runloom_global_runq_tail = NULL;
static volatile long runloom_global_runq_len  = 0;

/* Link g onto the tail of the global run-queue.  Each entry holds one queue
 * ref (incref'd by the caller -- wake_g, or the RUNNING_WOKEN release) so it
 * keeps the g alive until a hub consumes it.  Enqueue is exactly-once: only the
 * winner of the PARKED->QUEUED (or RUNNING_WOKEN->QUEUED) CAS pushes, so a g is
 * on this queue at most once -- no re-push, no duplicate. */
static void runloom_mn_global_runq_push(runloom_g_t *g)
{
    RUNLOOM_RLOCK(&runloom_global_runq_lock, RUNLOOM_RANK_GLOBAL_RUNQ);
    g->next = NULL;
    if (runloom_global_runq_tail != NULL) {
        runloom_global_runq_tail->next = g;
    } else {
        runloom_global_runq_head = g;
    }
    runloom_global_runq_tail = g;
    __atomic_add_fetch(&runloom_global_runq_len, 1, __ATOMIC_RELAXED);
    RUNLOOM_RUNLOCK(&runloom_global_runq_lock, RUNLOOM_RANK_GLOBAL_RUNQ);
    RUNLOOM_EVT(RUNLOOM_EVT_G_SUBMIT, g, NULL, 0);
    runloom_g_state_set(g, RUNLOOM_GST_SUBMITTED);
}

/* Pop one g from the global run-queue head (NULL if empty).  No per-hub
 * pending rebalance (see header: only the sum matters, and the mutex
 * release/acquire already orders the g's writes for the puller). */
static runloom_g_t *runloom_mn_global_runq_pull(void)
{
    runloom_g_t *g;
    /* Lock-free fast-out: the queue is empty in the common case (default
     * mode never touches it; per-g-tstate only when a wake is in flight),
     * so skip the mutex when there is clearly nothing to take.  A racing
     * push between this load and a lock would just be picked up next loop. */
    if (__atomic_load_n(&runloom_global_runq_len, __ATOMIC_ACQUIRE) == 0) {
        return NULL;
    }
    RUNLOOM_RLOCK(&runloom_global_runq_lock, RUNLOOM_RANK_GLOBAL_RUNQ);
    g = runloom_global_runq_head;
    if (g != NULL) {
        runloom_global_runq_head = g->next;
        if (runloom_global_runq_head == NULL) {
            runloom_global_runq_tail = NULL;
        }
        g->next = NULL;
        __atomic_sub_fetch(&runloom_global_runq_len, 1, __ATOMIC_RELAXED);
    }
    RUNLOOM_RUNLOCK(&runloom_global_runq_lock, RUNLOOM_RANK_GLOBAL_RUNQ);
    return g;
}

/* RUNLOOM_PER_G_TSTATE (default OFF, experimental, GATED): give each fiber its
 * own PyThreadState so its Python execution state is migratable across
 * hubs.  A stolen woken g then resumes on ANY hub without the cross-hub
 * corruption that per-hub-tstate + snap migration hits (the snap bakes the
 * owning hub's tstate into the suspended frames).  Trades memory (a tstate per
 * g) for a closeable p99.9 tail.  KNOWN CRASH under churn at H>=2 (the per-g
 * tstate's mimalloc heap migrates across hub threads -> SEGV; see the "KNOWN
 * BUG" block in runloom_sched_core.c.inc and docs/dev/STEAL_WOKEN_CLEANUP.md), so
 * mn_init runs this flag through runloom_resolve_migratable_mode: it activates
 * ONLY with RUNLOOM_ALLOW_UNSAFE_MIGRATION=1, else warns + stays off.  Read once. */
static int runloom_per_g_tstate_flag(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_PER_G_TSTATE");
        cur = (e != NULL && e[0] != '0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* RUNLOOM_STEAL_WOKEN (default OFF, experimental; Fix B): originally a snap-
 * migration mode -- route a woken g through the global stealable run-queue and
 * migrate its interpreter state via the per-g `snap` onto the stealing hub's
 * single bound tstate, needing NO extra PyThreadState per g.  That mechanism is
 * UNSOUND: a suspended fiber's eval frames bake in the ORIGIN hub's tstate
 * pointer (in registers / C locals on the coro stack), which the snap cannot
 * re-root; the migrated fiber then drives the origin hub's tstate from a foreign
 * OS thread (TSan-proven data race on datastack_chunk / c_recursion_remaining /
 * current_exception -> SEGV; H>=2 only).  See docs/dev/STEAL_WOKEN_CLEANUP.md.
 *
 * The flag is therefore REDIRECTED: mn_init enables RUNLOOM_PER_G_TSTATE when this
 * is set (see mn_sched_init_fini.c.inc), so RUNLOOM_STEAL_WOKEN=1 runs the per-g-
 * tstate migration (each fiber owns a migratable tstate) and the snap-migration
 * branch in hub_main is never taken.  per-g-tstate has its OWN known crash, so the
 * redirect is GATED: it activates only with RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 (see
 * runloom_resolve_migratable_mode), else warns + runs the default scheduler -- which
 * also keeps a gated request OUT of the unsound snap branch.  When OFF the default
 * scheduler is byte-unchanged. */
static int runloom_steal_woken_flag(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_STEAL_WOKEN");
        cur = (e != NULL && e[0] != '0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Safety interlock for the migratable modes.  BOTH RUNLOOM_PER_G_TSTATE and
 * RUNLOOM_STEAL_WOKEN (which redirects to per-g-tstate) carry a KNOWN crash:
 * per-g-tstate migrates a PyThreadState's mimalloc heap + QSBR state across OS
 * hub threads, which corrupts under allocation churn at H>=2 (intermittent SEGV
 * in PyThreadState_Clear -> _mi_page_retire; see runloom_sched_core.c.inc's
 * "KNOWN BUG" block at the per-g tstate teardown, and docs/dev/
 * STEAL_WOKEN_CLEANUP.md).  So setting either flag alone is GATED OFF (warn +
 * run the default scheduler); activation additionally requires
 * RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 -- the deliberate "I am developing / fuzzing
 * this and accept the crash" escape hatch (used by tools/combinatorial). */
static int runloom_unsafe_migration_acked(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_ALLOW_UNSAFE_MIGRATION");
        cur = (e != NULL && e[0] != '0' && e[0] != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* RUNLOOM_MIGRATION (default OFF): the PRODUCTION-facing master flag for cross-hub
 * fiber migration.  A stable, user-friendly alias of RUNLOOM_PER_G_TSTATE -- code
 * opts in with this name (or via runloom.enable_migration()) without needing to
 * know the internal per-g-tstate machinery.  Read once. */
static int runloom_migration_flag(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_MIGRATION");
        cur = (e != NULL && e[0] != '0' && e[0] != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Resolve the migratable-mode request through the interlock.  Returns 1 (enable
 * per-g-tstate cross-hub migration) when a migratable flag is set AND the mode is
 * safe to run; otherwise 0.  Called once from mn_init; its result feeds
 * runloom_set_per_g_tstate_mode, so a gated-off request leaves per_g_tstate_mode
 * == 0 and runloom_use_global_runq() false -> default scheduler (and, crucially,
 * NOT the unsound snap-migration branch).
 *
 * SAFETY GATE (two-tier):
 *  - PRODUCTION: built against the alloc-home CPython patch (Py_TSTATE_ALLOC_HOME,
 *    runloom_alloc_home_active()).  Each per-g tstate borrows the running hub's
 *    heap, so no heap migrates OS threads -- the crash that gated this mode is
 *    gone (validated: experiments/resume_rebuild/mpmc_pergt_repro.py 24/24 + a
 *    direct migration proof 50/60 cross-hub, zero crash).  Enable WITHOUT any
 *    unsafe override.  This is the supported way to ship migration.
 *  - STOCK CPython (no patch): per-g migration still corrupts (the per-g tstate's
 *    mimalloc heap migrates across hub threads -> SEGV under churn at H>=2; see
 *    docs/dev/STEAL_WOKEN_CLEANUP.md).  Keep the DEV/FUZZ gate: activate only with
 *    RUNLOOM_ALLOW_UNSAFE_MIGRATION=1.  Otherwise warn + run the default
 *    scheduler, so a production user who flips the flag on an unpatched
 *    interpreter gets a clean fallback, never a segfault. */
static int runloom_resolve_migratable_mode(void)
{
    int want = runloom_per_g_tstate_flag() || runloom_steal_woken_flag()
               || runloom_migration_flag();
    if (!want) return 0;
    /* Production: the patch makes it heap-safe -> enable, no override needed. */
    if (runloom_alloc_home_active()) return 1;
    /* Stock CPython: still unsound -> require the explicit dev escape hatch. */
    if (runloom_unsafe_migration_acked()) return 1;
    fprintf(stderr,
        "[runloom] cross-hub migration (RUNLOOM_MIGRATION / RUNLOOM_PER_G_TSTATE / "
        "RUNLOOM_STEAL_WOKEN) is GATED OFF: this CPython lacks the alloc-home patch, so a "
        "per-g PyThreadState's mimalloc heap would migrate across hub threads -> "
        "SEGV under churn at H>=2 (see docs/dev/STEAL_WOKEN_CLEANUP.md). For "
        "PRODUCTION, build against patches/cpython313t-tstate-alloc-home.patch "
        "(makes it heap-safe). For dev/fuzzing on stock CPython, set "
        "RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 to enable anyway. Running the default "
        "(per-hub-tstate) scheduler instead.\n");
    return 0;
}

/* True when woken gs route to the global stealable run-queue + wake-state
 * machine.  Derived from the RESOLVED mode (runloom_get_per_g_tstate_mode), which
 * the interlock above has already gated -- so a gated-off RUNLOOM_STEAL_WOKEN
 * leaves this false and the default scheduler runs, rather than falling into the
 * unsound snap branch (which is reachable only when use_global_runq is true while
 * per_g_tstate_mode is false). */
static int runloom_use_global_runq(void)
{
    return runloom_get_per_g_tstate_mode();
}

/* ---- sysmon watchdog (Group B: stalled-hub detection) ----
 *
 * RUNLOOM_SYSMON (default OFF): spawn one extra OS thread that periodically
 * scans every hub's resume_start_ns and logs a hub stuck inside a single
 * runloom_coro_resume longer than RUNLOOM_SYSMON_MS (the unanticipated-block
 * signature -- a fiber occupying its hub's OS thread with a
 * non-yielding blocking C call that Group A's offload never wrapped).
 *
 * This first cut is DETECT-ONLY (log the wedge + how many gs it strands);
 * the tstate-handoff recovery builds on top once the detector is measured.
 * Gated so the per-resume instrumentation (two stores + a clock read) is a
 * predicted-not-taken branch -- zero cost when the watchdog is off. */
/* PyThreadState->state values (cpython/pystate.h documents the field;
 * the numeric values live in the internal pycore_pystate.h).  Mirrored here
 * so the watchdog can classify a wedge WITHOUT the Python C-API: it reads
 * h->tstate->state to tell a handoff-RECOVERABLE stall (the running g
 * released the tstate via Py_BEGIN_ALLOW_THREADS -> DETACHED, a well-behaved
 * blocking-IO call a standby thread can take over) from an un-recoverable one
 * (ATTACHED = CPU-bound bytecode or a raw tstate-holding syscall; SUSPENDED =
 * a stop-the-world is in progress -- never adopt). */
#define RUNLOOM_TS_DETACHED  0
#define RUNLOOM_TS_ATTACHED  1
#define RUNLOOM_TS_SUSPENDED 2

/* Read a tstate's free-threaded attach state (DETACHED/ATTACHED/SUSPENDED).
 * The `state` field + this tri-state attach protocol are free-threading
 * infrastructure that only exists from 3.13 (3.12 has `_status` and no
 * attach/detach/stop-the-world concept).  Pre-3.13 returns -1 ("unknown"),
 * which makes the sysmon classifier print "?" and the handoff adopt/dispatch
 * gates (== / != RUNLOOM_TS_DETACHED) all fail closed -- the rescue never fires,
 * matching runloom_handoff_enabled being forced off there. */
#if PY_VERSION_HEX >= 0x030D0000
RUNLOOM_INLINE int runloom_tstate_attach_state(PyThreadState *ts)
{
    if (ts == NULL) return -1;
#if defined(RUNLOOM_OS_WINDOWS)
    /* This read is DELIBERATELY racy: the sysmon / handoff / hub-snapshot
     * threads read h->tstate->state without owning the tstate (see callers).
     * On POSIX a tstate freed under us leaves its page mapped, so the stale
     * read returns garbage harmlessly.  On Windows the allocator (mimalloc)
     * UNMAPS freed pages, so the same race faults with an access violation
     * (reproduced by big_100 under M:N free-threading).  Wrap the read in SEH:
     * a faulting read means the tstate was freed concurrently -> report -1
     * ("unknown"), identical to the NULL case, so every attach-state gate
     * fails closed and no rescue/adopt fires on a dead hub. */
    __try {
        return __atomic_load_n(&ts->state, __ATOMIC_ACQUIRE);
    } __except (EXCEPTION_EXECUTE_HANDLER) {
        return -1;
    }
#else
    return __atomic_load_n(&ts->state, __ATOMIC_ACQUIRE);
#endif
}
#else
RUNLOOM_INLINE int runloom_tstate_attach_state(PyThreadState *ts)
{
    (void)ts;
    return -1;
}
#endif

static int       runloom_sysmon_enabled = 0;
static int       runloom_sysmon_quiet = 0;   /* RUNLOOM_SYSMON_QUIET: suppress
                                              * the per-wedge WEDGED/RECOVERED
                                              * diagnostic spam (detector +
                                              * handoff/preempt stay fully on) */
static long long runloom_sysmon_wedge_ns = 50LL * 1000000LL;   /* 50 ms default */
/* ATTACHED-preempt threshold, split from the DETACHED-handoff/wedge threshold
 * above so a CPU-bound time-slice (fairness) tunes independently of the
 * blocking-call rescue latency.  Defaults to wedge_ns (backward compatible);
 * RUNLOOM_PREEMPT_MS overrides.  (Go uses forcePreemptNS=10 ms for the CPU
 * time-slice, much larger than its ~20 us syscall retake.) */
static long long runloom_sysmon_preempt_ns = 50LL * 1000000LL;
static long long runloom_sysmon_tick_ns  = 10LL * 1000000LL;   /* scan every 10 ms (base) */
static long long runloom_sysmon_tick_base_ns = 10LL * 1000000LL; /* unbacked-off tick */
static runloom_thread_t runloom_sysmon_thread;
static int       runloom_sysmon_running = 0;   /* 1 once the thread is spawned */
static volatile int runloom_sysmon_stop = 0;

/* ---- Group B: stalled-hub tstate handoff (RUNLOOM_HANDOFF, default OFF) ----
 *
 * One standby "rescue" OS thread adopts a hub's DETACHED bound tstate -- a
 * fiber inside a Py_BEGIN_ALLOW_THREADS blocking call left it free -- and
 * drains THAT hub's stranded runnable gs, handing the tstate back when the
 * block ends.  This is the DETACHED-class wedge the sysmon detector classifies
 * as handoff-recoverable (a well-behaved blocking-IO call), the asyncio-killer
 * scenario: one blocking task no longer stalls a whole hub's fan-out.
 *
 * DEFAULT mode only (no per-g-tstate).  The adopted tstate is the hub's single
 * bound tstate, so a stranded g resumes on the SAME tstate it parked under --
 * the invariant the H>=2 cross-hub snap migration violated (a stackful coro's
 * suspended eval-loop C frame caches its origin tstate; only resuming on that
 * exact tstate is correct).  The rescue moves the *tstate* (to its own OS
 * thread) rather than the fiber, so the cached pointer stays valid.
 *
 * Single-runner safety = the tstate attach state itself.  PyEval_RestoreThread
 * CASes the bound tstate DETACHED->ATTACHED; at most one thread holds it
 * ATTACHED, and ONLY the ATTACHED holder mutates hub state.  The rescue adopts
 * only while state==DETACHED (the owner provably parked in its block, not in
 * the hub loop); attaching excludes the owner's END_ALLOW_THREADS re-attach
 * until the rescue detaches.  A stop-the-world that beats the rescue to the
 * tstate (DETACHED->SUSPENDED) makes the adopt attach block until
 * start_the_world -- STW-safe for free (the per-g bug was thousands of
 * EPHEMERAL tstates churning across STW; these H bound tstates do not).
 *
 * Dispatch: a pool of standby rescue threads + a per-hub claim slot
 * (runloom_handoff_claim[hub], FREE/PENDING/OWNED).  The sysmon watchdog, on a
 * DETACHED wedge, CASes the hub's slot FREE->PENDING; an idle rescue thread
 * CASes PENDING->OWNED to take it (so at most one thread ever rescues a given
 * hub -- the single-attach invariant holds), rescues it, then stores OWNED->FREE
 * at its final release.  K simultaneous DETACHED wedges recover on K threads in
 * parallel (each owns a distinct hub).  A persistently-wedged hub is re-flagged
 * FREE->PENDING on the next sysmon scan after a premature release (e.g. a STW
 * window left the hub wedged), since sysmon only ever CASes from FREE. */
#define RUNLOOM_HANDOFF_FREE    0   /* not wedged / not in rescue */
#define RUNLOOM_HANDOFF_PENDING 1   /* sysmon flagged a DETACHED wedge, awaiting pickup */
#define RUNLOOM_HANDOFF_OWNED   2   /* a rescue thread holds this hub */
/* The sysmon must observe a hub continuously DETACHED-while-wedged for this many
 * consecutive ticks before dispatching a handoff.  A genuine blocking call
 * (Py_BEGIN_ALLOW_THREADS: owner parked in a syscall, not touching the tstate)
 * stays DETACHED across ticks and is safe to adopt; a transient detach (idle /
 * world-yield blip, or the instant before a stop-the-world flips the state to
 * SUSPENDED) does not, and adopting it races the owner re-attaching/suspending
 * on the same tstate's non-atomic _status -> use-after-free.  2 ticks
 * (>= one full tick of continuous DETACH) excludes the transients while adding
 * at most ~one tick of rescue latency to a real blocking wedge. */
#define RUNLOOM_HANDOFF_DETACH_TICKS 2
static int          runloom_handoff_enabled = 0;
static int          runloom_handoff_pool    = 0;    /* number of rescue threads */
static int         *runloom_handoff_claim   = NULL; /* per-hub FREE/PENDING/OWNED */
static runloom_thread_t *runloom_handoff_threads = NULL;
static int          runloom_handoff_running = 0;     /* count of spawned rescue threads */
static int          runloom_handoff_debug   = 0;     /* RUNLOOM_HANDOFF_DEBUG: trace adopts/drains */
/* BUG #2 fix: each rescue thread's own PyThreadState.  Created by the rescue
 * thread on its own OS thread (so biased-refcount ob_tid binds to that TID).
 * Stored here (indexed [0..running-1]) so stop_join can delete them from the
 * MAIN thread after joining -- same pattern as hub tstates in mn_sched_init_fini.
 * A rescue thread must NEVER call PyThreadState_Delete itself: doing so while a
 * GC stop-the-world is in progress (the gcer fires during mn_fini) deadlocks
 * because Delete interacts with STW bookkeeping, and the main thread (the only
 * one that safely drives that interaction) is blocked in thread_join. */
static PyThreadState **runloom_rescue_tstates = NULL;

/* ---- Group B step (a): ATTACHED/CPU preemption (RUNLOOM_PREEMPT, default OFF) ----
 *
 * The DETACHED handoff above can't recover an ATTACHED wedge (a CPU-bound or
 * raw-tstate-holding fiber -- it never releases its hub's tstate).  The
 * answer is preemption: make the offending g yield so the hub round-robins its
 * other gs, exactly as Go time-slices a long-running fiber.
 *
 * Mechanism = a chained eval-frame function (the only exported, build-stable
 * hook; `_PyEval_AddPendingCall` can't target a specific thread, and runloom
 * avoids internal headers).  On every Python frame boundary the wrapper checks
 * THIS hub's `preempt_requested` (set by the sysmon watchdog on an ATTACHED
 * wedge) and, if set, `runloom_coro_yield()`s the running g back to the hub before
 * entering the frame -- a clean safe point identical to a recv-park.  This is
 * Go pre-1.14 cooperative preemption: prompt for call-bearing CPU code; a tight
 * single-frame / C-extension loop makes no Python calls and so is NOT preempted
 * (the same class the sysmon already flags as out-of-handoff-scope).
 *
 * Cost: installing a custom eval-frame func disables CPython's
 * `eval_frame == _PyEval_EvalFrameDefault` fast path, so EVERY frame goes
 * indirect.  We therefore install it ONLY when RUNLOOM_PREEMPT is set -- default
 * mode keeps the fast path and never reads `preempt_requested`.  The wrapper
 * itself is defined below runloom_tls_hub/runloom_tls_current_g (it reads them). */
static int          runloom_preempt_enabled = 0;
static volatile int runloom_handoff_stop = 0;
