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
 * deterministic connect/accept order) -> its socketpair fd pair.  H=1 / single
 * thread under sim, so no lock.  Grow-on-demand; never shrinks within a run. */
typedef struct { int fd_a, fd_b, closed; } runloom_sim_conn_t;
static runloom_sim_conn_t *runloom_sim_conns;
static long long runloom_sim_conn_n, runloom_sim_conn_cap;

long long runloom_sim_conn_register(int fd_a, int fd_b)
{
    if (runloom_sim_conn_n >= runloom_sim_conn_cap) {
        long long ncap = runloom_sim_conn_cap ? runloom_sim_conn_cap * 2 : 16;
        runloom_sim_conn_t *nw = (runloom_sim_conn_t *)realloc(
            runloom_sim_conns, (size_t)ncap * sizeof(*nw));
        if (nw == NULL) return -1;
        runloom_sim_conns = nw;
        runloom_sim_conn_cap = ncap;
    }
    {
        long long id = runloom_sim_conn_n++;
        runloom_sim_conns[id].fd_a = fd_a;
        runloom_sim_conns[id].fd_b = fd_b;
        runloom_sim_conns[id].closed = 0;
        return id;
    }
}

/* The ready ledger: pending readiness deliveries.  A plain grow-on-demand vector;
 * dispatch qsorts it so append order (which could vary with the still-unlanded
 * equal-deadline heap tie-break) never leaks into the dispatch order. */
typedef struct { long long deliver_at, conn_id; int fd, dir; } runloom_sim_ready_ent_t;
static runloom_sim_ready_ent_t *runloom_sim_ready;
static long long runloom_sim_ready_n, runloom_sim_ready_cap;

/* Append a readiness delivery for fd/dir, keyed by conn_id, due at logical
 * deliver_at.  No-op when sim is off (the real epoll pump owns readiness then, so
 * the ledger must not accumulate).  Best-effort on OOM: the real socketpair bytes
 * self-heal a dropped entry. */
void runloom_sim_deliver_ready(long long conn_id, int fd, int dir, long long deliver_at)
{
    if (!runloom_sim_enabled()) return;
    if (runloom_sim_ready_n >= runloom_sim_ready_cap) {
        long long ncap = runloom_sim_ready_cap ? runloom_sim_ready_cap * 2 : 32;
        runloom_sim_ready_ent_t *nw = (runloom_sim_ready_ent_t *)realloc(
            runloom_sim_ready, (size_t)ncap * sizeof(*nw));
        if (nw == NULL) return;
        runloom_sim_ready = nw;
        runloom_sim_ready_cap = ncap;
    }
    {
        runloom_sim_ready_ent_t *e = &runloom_sim_ready[runloom_sim_ready_n++];
        e->deliver_at = deliver_at;
        e->conn_id = conn_id;
        e->fd = fd;
        e->dir = dir;
    }
}

/* Stable order: earliest deliver_at, then conn_id (seed-order connect index),
 * then dir (READ=1 before WRITE=2).  The raw fd is NEVER a key. */
static int runloom_sim_ready_cmp(const void *pa, const void *pb)
{
    const runloom_sim_ready_ent_t *a = (const runloom_sim_ready_ent_t *)pa;
    const runloom_sim_ready_ent_t *b = (const runloom_sim_ready_ent_t *)pb;
    if (a->deliver_at != b->deliver_at) return a->deliver_at < b->deliver_at ? -1 : 1;
    if (a->conn_id != b->conn_id) return a->conn_id < b->conn_id ? -1 : 1;
    if (a->dir != b->dir) return a->dir < b->dir ? -1 : 1;
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
    for (i = 0; i < runloom_sim_ready_n; i++) {
        long long d = runloom_sim_ready[i].deliver_at;
        if (d > nowl && (earliest < 0 || d < earliest)) earliest = d;
    }
    return earliest;
}

/* Dispatch every ledger entry whose logical deliver_at has arrived, in the stable
 * (deliver_at, conn_id, dir) order, through the EXISTING runloom_pump_dispatch_event
 * -- so the claim/commit-CAS, unlink, and owner-routed wake are the identical
 * primitive every real backend uses.  Not-yet-due entries are compacted to the
 * front and kept for a later pass.  Returns the number of parkers woken.  Called
 * by the sim pump at quiescence BEFORE the clock advance, so ready I/O at the
 * current instant beats a same-instant wait_fd timeout. */
static int runloom_sim_dispatch_ready(void)
{
    long long nowl = runloom_sched_logical_ns();
    long long i, keep = 0;
    int woke = 0;
    if (runloom_sim_ready_n == 0) return 0;
    qsort(runloom_sim_ready, (size_t)runloom_sim_ready_n,
          sizeof(runloom_sim_ready[0]), runloom_sim_ready_cmp);
    for (i = 0; i < runloom_sim_ready_n; i++) {
        runloom_sim_ready_ent_t *e = &runloom_sim_ready[i];
        if (e->deliver_at <= nowl) {
            /* A no-parker dispatch (reader not parked yet, or already consumed the
             * real bytes directly) returns 0 and the entry is simply dropped --
             * the socketpair bytes are the belt-and-suspenders wake. */
            if (runloom_pump_dispatch_event(e->fd, e->dir, 0)) woke++;
        } else {
            runloom_sim_ready[keep++] = *e;   /* future delivery -- keep */
        }
    }
    runloom_sim_ready_n = keep;
    return woke;
}
