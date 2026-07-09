/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- Deterministic sim readiness plane (Slice 3, RUNLOOM_SIM) ---------------
 *
 * Slice 2 made wait_fd TIMEOUTS a function of logical time; this makes fd
 * READINESS a function of the seed, so a REAL socket workload (real send()/recv()
 * on socketpair-backed sim sockets, real wait_fd park on EAGAIN) runs
 * deterministically -- exercising the full real I/O path (register, park/commit
 * FSM, deadline heap, wake routing) under one seed.  See docs/dev/soak/SIM_IO_DST.md.
 *
 * THE LOAD-BEARING DECISION -- who is the wake source.  Under sim the pump
 * early-returns before epoll_wait (netpoll_pump.c.inc), so a socketpair's real
 * kernel readiness is INVISIBLE to the runtime and cannot wake anyone.  The SOLE
 * wake source is this per-scheduler ready LEDGER: when the model has physically
 * written bytes into the reader's real socketpair end, it appends {deliver_at,
 * conn_id, fd, dir}; the sim pump, at quiescence, calls runloom_sim_dispatch_ready
 * FIRST (before advancing the clock), which qsorts the ledger on the STABLE key
 * (deliver_at, conn_id, dir) -- never the kernel-assigned fd number -- and fires
 * every due entry through the EXISTING runloom_pump_dispatch_event (same
 * commit-CAS / unlink / owner-routed wake every real backend uses).  Real bytes
 * on the socketpair are belt-and-suspenders: an absent/late dispatch self-heals
 * because recv() returns the bytes directly.  Determinism: at H=1 the baton makes
 * the total order of send/recv syscalls before quiescence deterministic, so which
 * fibers are ready is a function of the seed; the qsort imposes a seed-stable
 * dispatch ORDER independent of epoll event order / fd assignment.
 *
 * SCOPE (increment 1): the zero-delay, no-fault plane -- one socketpair per
 * connection, delivery at the current logical instant.  Delay/loss/reorder/
 * partition via a MITM model goroutine, and WRITE-readiness, are later increments
 * of this SAME ledger (deliver_at already carries a logical time; the model's
 * sched_sleep folds into runloom_sched_sim_advance_clock as a logical sleeper).
 * H=1 only.
 *
 * LIFETIME: the conn registry grows per connection and never shrinks; the ledger
 * compacts to empty after every dispatch but its capacity persists.  Within one
 * run() that is bounded; across the MANY run()s of a fleet soak the registry
 * would accumulate, so the soak-wiring increment adds a runloom_sim_reset() to
 * clear both per seed (a leak-oracle concern for a tool meant to run millions of
 * seeds).  Not needed for the bounded test scenarios here. */

/* Sim connection registry: conn_id (a monotonic counter assigned at register in
 * deterministic connect/accept order) -> its socketpair fd pair.  Grow-on-demand;
 * never shrinks within a run.
 *
 * LOCKING (MN_SIM_DST_PLAN.md I1): the registry + the ready ledger below were
 * "H=1 single thread so no lock" statics; the native mn-sim plane reaches them
 * from running fibers on ANY hub (m_sim_deliver_ready / m_sim_conn_register)
 * and from the census dispatcher, so both are guarded by
 * runloom_sim_ledger_lock (rank RUNLOOM_RANK_SIM_LEDGER=15: taken under
 * ctrl(5), ALWAYS released before pool(80)/sub(60) -- the three-phase dispatch
 * below never holds it across a wake).  Self-initialising (the cal_lock
 * pattern) so it needs no PyInit hook on Windows. */
typedef struct { int fd_a, fd_b, closed; } runloom_sim_conn_t;
static runloom_sim_conn_t *runloom_sim_conns;
static long long runloom_sim_conn_n, runloom_sim_conn_cap;

static runloom_mutex_t runloom_sim_ledger_lock = RUNLOOM_MUTEX_STATIC_INIT;
static int runloom_sim_ledger_lock_inited = 0;

static void runloom_sim_ledger_ensure_lock(void)
{
#if defined(RUNLOOM_OS_WINDOWS)
    int st = __atomic_load_n(&runloom_sim_ledger_lock_inited, __ATOMIC_ACQUIRE);
    if (st == 2) return;
    if (st == 0 &&
        __atomic_compare_exchange_n(&runloom_sim_ledger_lock_inited, &st, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&runloom_sim_ledger_lock);
        __atomic_store_n(&runloom_sim_ledger_lock_inited, 2, __ATOMIC_RELEASE);
        return;
    }
    while (__atomic_load_n(&runloom_sim_ledger_lock_inited, __ATOMIC_ACQUIRE) != 2) { }
#else
    (void)runloom_sim_ledger_lock_inited;  /* POSIX: static initialiser suffices */
#endif
}

long long runloom_sim_conn_register(int fd_a, int fd_b)
{
    long long id;
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    if (runloom_sim_conn_n >= runloom_sim_conn_cap) {
        long long ncap = runloom_sim_conn_cap ? runloom_sim_conn_cap * 2 : 16;
        runloom_sim_conn_t *nw = (runloom_sim_conn_t *)realloc(
            runloom_sim_conns, (size_t)ncap * sizeof(*nw));
        if (nw == NULL) {
            RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
            return -1;
        }
        runloom_sim_conns = nw;
        runloom_sim_conn_cap = ncap;
    }
    id = runloom_sim_conn_n++;
    runloom_sim_conns[id].fd_a = fd_a;
    runloom_sim_conns[id].fd_b = fd_b;
    runloom_sim_conns[id].closed = 0;
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    return id;
}

/* The ready ledger: pending readiness deliveries.  A plain grow-on-demand vector;
 * dispatch qsorts it on a TOTAL key so the (unstable) qsort's ordering of ties
 * never leaks into the dispatch order.  `seq` is the append order -- a monotonic
 * per-run counter that is program order, hence seed-deterministic at H=1; it is
 * the FINAL tie key so that same-instant same-conn same-dir entries for DIFFERENT
 * fds (which the WRITE-readiness increment makes reachable: a send's READ-for-peer
 * and a shuttler's READ-for-reader can share deliver_at/conn_id/dir) dispatch in
 * a deterministic order.  The raw fd is NEVER a key. */
typedef struct { long long deliver_at, conn_id, seq; int fd, dir; } runloom_sim_ready_ent_t;
static runloom_sim_ready_ent_t *runloom_sim_ready;
static long long runloom_sim_ready_n, runloom_sim_ready_cap, runloom_sim_ready_seq;

/* Settle-reap tally (increment O): the sim pump adds every parker it reaps at a
 * settled deadlock (netpoll_sim.c.inc) here.  A workload asserts its EXPECTED
 * infra-reap total (e.g. 2 per MITM conn still parked at end) and flags any excess
 * as a stranded fiber -- a netpoll-plane lost wake the chan/safe count_deadlocked
 * census cannot see.  Reset per run() in runloom_sim_reset. */
static long long runloom_sim_reap_total;
long long runloom_sim_reap_count(void) { return runloom_sim_reap_total; }

/* Append a readiness delivery for fd/dir, keyed by conn_id, due at logical
 * deliver_at.  No-op when sim is off (the real epoll pump owns readiness then, so
 * the ledger must not accumulate).  Best-effort on OOM: the real socketpair bytes
 * self-heal a dropped entry. */
void runloom_sim_deliver_ready(long long conn_id, int fd, int dir, long long deliver_at)
{
    if (!runloom_sim_enabled()) return;
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    if (runloom_sim_ready_n >= runloom_sim_ready_cap) {
        long long ncap = runloom_sim_ready_cap ? runloom_sim_ready_cap * 2 : 32;
        runloom_sim_ready_ent_t *nw = (runloom_sim_ready_ent_t *)realloc(
            runloom_sim_ready, (size_t)ncap * sizeof(*nw));
        if (nw == NULL) {
            RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
            return;
        }
        runloom_sim_ready = nw;
        runloom_sim_ready_cap = ncap;
    }
    {
        runloom_sim_ready_ent_t *e = &runloom_sim_ready[runloom_sim_ready_n++];
        e->deliver_at = deliver_at;
        e->conn_id = conn_id;
        e->seq = runloom_sim_ready_seq++;
        e->fd = fd;
        e->dir = dir;
    }
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
}

/* Total order: earliest deliver_at, then conn_id (seed-order connect index), then
 * dir (READ=1 before WRITE=2), then seq (append/program order -- the tie key that
 * makes fully-equal (deliver_at,conn_id,dir) entries for different fds
 * deterministic).  The raw fd is NEVER a key.  seq is unique per entry, so this is
 * a strict total order and the qsort's stability is irrelevant. */
static int runloom_sim_ready_cmp(const void *pa, const void *pb)
{
    const runloom_sim_ready_ent_t *a = (const runloom_sim_ready_ent_t *)pa;
    const runloom_sim_ready_ent_t *b = (const runloom_sim_ready_ent_t *)pb;
    if (a->deliver_at != b->deliver_at) return a->deliver_at < b->deliver_at ? -1 : 1;
    if (a->conn_id != b->conn_id) return a->conn_id < b->conn_id ? -1 : 1;
    if (a->dir != b->dir) return a->dir < b->dir ? -1 : 1;
    if (a->seq != b->seq) return a->seq < b->seq ? -1 : 1;
    return 0;
}

/* Peek the earliest not-yet-due deliver_at in the ledger (ns, logical), or -1 if
 * every entry is already due / the ledger is empty.  The sim pump folds this into
 * its clock advance so a delivery scheduled for a FUTURE logical instant makes
 * time compress to that instant (like a logical sleeper).  In increment 1 every
 * entry is due-now, so this returns -1. */
static long long runloom_sim_ready_next_ns(void)
{
    long long i, earliest = -1;
    long long nowl = runloom_sched_logical_ns();
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    for (i = 0; i < runloom_sim_ready_n; i++) {
        long long d = runloom_sim_ready[i].deliver_at;
        if (d > nowl && (earliest < 0 || d < earliest)) earliest = d;
    }
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    return earliest;
}

/* Public peek (MN_SIM_DST_PLAN.md I1): the earliest deliver_at OVERALL (due or
 * future), or -1 if the ledger is empty.  Unlike the static next_ns above this
 * takes NO clock reading -- the mn census computes its advance target against
 * the ctrl ns clock, and reading the (possibly-unmirrored) global clock here
 * would be a cross-plane bug (design review fix #8). */
long long runloom_sim_ready_peek_ns(void)
{
    long long i, earliest = -1;
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    for (i = 0; i < runloom_sim_ready_n; i++) {
        long long d = runloom_sim_ready[i].deliver_at;
        if (earliest < 0 || d < earliest) earliest = d;
    }
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    return earliest;
}

/* Dispatch every ledger entry whose logical deliver_at has arrived, in the stable
 * (deliver_at, conn_id, dir) order, through the EXISTING runloom_pump_dispatch_event
 * -- so the claim/commit-CAS, unlink, and owner-routed wake are the identical
 * primitive every real backend uses.  Not-yet-due entries are compacted to the
 * front and kept for a later pass.  Returns the number of parkers woken.  Called
 * by the sim pump at quiescence BEFORE the clock advance, so ready I/O at the
 * current instant beats a same-instant wait_fd timeout. */
/* Reset the sim readiness plane for a fresh run: clear the ledger + the conn
 * registry and zero the logical clock.  A fleet soak runs each seed in its own
 * subprocess (fresh statics), so this is for multi-scenario-PER-PROCESS replay /
 * tests -- it makes back-to-back run()s bit-exact (same absolute clock magnitude)
 * and bounds the otherwise-monotonic conn registry.  Call only between run()s. */
void runloom_sim_reset(void)
{
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    runloom_sim_ready_n = 0;
    runloom_sim_ready_seq = 0;
    runloom_sim_conn_n = 0;
    runloom_sim_reap_total = 0;
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    runloom_sched_logical_reset();
    runloom_mn_logical_reset();     /* all clock planes reset together (I1) */
}

/* Dispatch every ledger entry with deliver_at <= now_ns, in the strict total
 * order, through the EXISTING runloom_pump_dispatch_event.  THREE-PHASE lock
 * discipline (MN_SIM_DST_PLAN.md I1/I2): (A) snapshot due entries into a local
 * scratch under the ledger lock (future entries compact in place); (B/C) the
 * wakes run with the ledger lock RELEASED -- dispatch_event takes pool(80) and
 * the wake takes sub(60)/idle(55), so holding ledger(15) across them would
 * pin the lock-rank chain the checker flags.  Returns parkers woken.  A
 * no-parker dispatch returns 0 and the entry is dropped -- the socketpair
 * bytes are the belt-and-suspenders wake.  OOM on the scratch keeps every
 * entry for a later pass (fail-safe, no loss). */
/* Persistent phase-A scratch: reused across dispatches (grow-on-demand, never
 * shrinks; bounded by the ledger's own high-water mark).  Guarded by the
 * ledger lock.  Keeping it persistent makes the steady state ALLOCATION-FREE
 * like the pre-I1 dispatch -- a per-call malloc turned OOM into a new,
 * silent "nothing due" failure mode the H=1 pump could mis-read as settled
 * (adversarial-review finding). */
static runloom_sim_ready_ent_t *runloom_sim_due_scratch;
static long long runloom_sim_due_scratch_cap;

int runloom_sim_dispatch_due(long long now_ns)
{
    long long i, keep, ndue = 0;
    int woke = 0;
    runloom_sim_ledger_ensure_lock();
    RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    if (runloom_sim_ready_n == 0) {
        RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
        return 0;
    }
    qsort(runloom_sim_ready, (size_t)runloom_sim_ready_n,
          sizeof(runloom_sim_ready[0]), runloom_sim_ready_cmp);
    /* Sorted by deliver_at first, so the due entries are exactly the PREFIX. */
    while (ndue < runloom_sim_ready_n &&
           runloom_sim_ready[ndue].deliver_at <= now_ns) ndue++;
    if (ndue == 0) {
        RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
        return 0;
    }
    if (ndue > runloom_sim_due_scratch_cap) {
        long long ncap = runloom_sim_due_scratch_cap ? runloom_sim_due_scratch_cap : 32;
        runloom_sim_ready_ent_t *nw;
        while (ncap < ndue) ncap *= 2;
        nw = (runloom_sim_ready_ent_t *)realloc(
            runloom_sim_due_scratch, (size_t)ncap * sizeof(*nw));
        if (nw != NULL) {
            runloom_sim_due_scratch = nw;
            runloom_sim_due_scratch_cap = ncap;
        }
    }
    if (ndue <= runloom_sim_due_scratch_cap) {
        /* Steady state: copy the due prefix out, compact the future suffix,
         * release the lock, dispatch with no lock held (three-phase). */
        memcpy(runloom_sim_due_scratch, runloom_sim_ready,
               (size_t)ndue * sizeof(*runloom_sim_due_scratch));
        keep = runloom_sim_ready_n - ndue;
        if (keep > 0)
            memmove(runloom_sim_ready, runloom_sim_ready + ndue,
                    (size_t)keep * sizeof(*runloom_sim_ready));
        runloom_sim_ready_n = keep;
        RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
        for (i = 0; i < ndue; i++) {
            if (runloom_pump_dispatch_event(runloom_sim_due_scratch[i].fd,
                                            runloom_sim_due_scratch[i].dir, 0))
                woke++;
        }
        return woke;
    }
    /* Scratch-grow OOM: allocation-free degraded path -- pop the due head one
     * at a time (re-locking between dispatches so the lock is never held
     * across a wake).  Slower, but NO entry is lost and progress is made, so
     * OOM can neither strand a delivery nor masquerade as "settled". */
    RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
    for (;;) {
        runloom_sim_ready_ent_t head;
        RUNLOOM_RLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
        if (runloom_sim_ready_n == 0 ||
            runloom_sim_ready[0].deliver_at > now_ns) {
            RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
            break;
        }
        head = runloom_sim_ready[0];
        runloom_sim_ready_n--;
        if (runloom_sim_ready_n > 0)
            memmove(runloom_sim_ready, runloom_sim_ready + 1,
                    (size_t)runloom_sim_ready_n * sizeof(*runloom_sim_ready));
        RUNLOOM_RUNLOCK(&runloom_sim_ledger_lock, RUNLOOM_RANK_SIM_LEDGER);
        if (runloom_pump_dispatch_event(head.fd, head.dir, 0)) woke++;
    }
    return woke;
}

/* Legacy H=1 sim-pump entry: dispatch against the single-thread logical clock
 * (byte-for-byte the pre-I1 semantics; the pump calls this at quiescence
 * BEFORE advancing the clock). */
static int runloom_sim_dispatch_ready(void)
{
    return runloom_sim_dispatch_due(runloom_sched_logical_ns());
}
