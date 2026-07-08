/* runloom_diag.c -- diagnostic infrastructure: env-driven flags,
 * lock-free per-thread lifecycle event rings, self_check invariant
 * pass.  See runloom_diag.h for the contract. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "runloom_diag.h"
#include "plat.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"
#include "plat_atomic.h"
#include "rl_handle.h"
#include "runloom_kcsan.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#if defined(_WIN32)
#  include <io.h>
#else
#  include <unistd.h>
#endif

/* ---------------------------------------------------------------- *
 *  Flag parsing                                                    *
 * ---------------------------------------------------------------- */

unsigned int runloom_debug_flags = 0;

static unsigned int parse_one_token(const char *t, size_t n)
{
    if (n == 0) return 0;
    if (n == 4 && memcmp(t, "none", 4) == 0)    return 0;
    if (n == 3 && memcmp(t, "all",  3) == 0)    return RUNLOOM_DBG_ALL;
    if (n == 6 && memcmp(t, "parker", 6) == 0)  return RUNLOOM_DBG_PARKER;
    if (n == 6 && memcmp(t, "gstate", 6) == 0)  return RUNLOOM_DBG_GSTATE;
    if (n == 10 && memcmp(t, "invariants", 10) == 0) return RUNLOOM_DBG_INVARIANTS;
    if (n == 4 && memcmp(t, "ring", 4) == 0)    return RUNLOOM_DBG_RING;
    /* "1" -- legacy: tag for "build-style debug" only, no diag flags. */
    /* Unknown token: keep silent.  setup.py also uses RUNLOOM_DEBUG=1
     * which we ignore here. */
    return 0;
}

static void parse_runloom_debug_env(void)
{
    const char *env = getenv("RUNLOOM_DEBUG_DIAG");
    if (env == NULL || *env == '\0') {
        /* Fall back to RUNLOOM_DEBUG, but ignore the legacy "=1" form
         * (that's the build-style debug flag, not a diag selector). */
        env = getenv("RUNLOOM_DEBUG");
        if (env == NULL || *env == '\0') return;
        /* Skip if it looks like the build-style "1"/"0"/"true"/etc.
         * value -- we don't want RUNLOOM_DEBUG=1 to enable runtime
         * checks unintentionally. */
        if ((env[0] == '0' || env[0] == '1') && env[1] == '\0') return;
        if (strcmp(env, "true") == 0 || strcmp(env, "false") == 0) return;
        if (strcmp(env, "yes")  == 0 || strcmp(env, "no")    == 0) return;
    }
    {
        const char *p = env;
        unsigned int flags = 0;
        while (*p != '\0') {
            const char *start;
            size_t n;
            while (*p == ',' || *p == ' ' || *p == '\t') p++;
            start = p;
            while (*p != '\0' && *p != ',' && *p != ' ' && *p != '\t') p++;
            n = (size_t)(p - start);
            /* lowercase in place would mutate the env; do a stack
             * copy for short tokens. */
            if (n > 0 && n < 32) {
                char buf[32];
                size_t i;
                for (i = 0; i < n; i++) buf[i] = (char)tolower((unsigned char)start[i]);
                flags |= parse_one_token(buf, n);
            }
        }
        runloom_debug_flags = flags;
    }
}


/* ---------------------------------------------------------------- *
 *  Lifecycle event ring                                            *
 * ---------------------------------------------------------------- */

/* Power-of-two size; head is monotonic, slot = head & mask.  Each
 * record is 32 bytes so the ring fits in cache lines cleanly. */
#define RUNLOOM_RING_LOG2 10        /* 1024 entries = 32 KiB per thread */
#define RUNLOOM_RING_CAP  (1u << RUNLOOM_RING_LOG2)
#define RUNLOOM_RING_MASK (RUNLOOM_RING_CAP - 1u)

typedef struct runloom_evt {
    unsigned int   op;            /* runloom_evt_op_t */
    unsigned int   tid;           /* lightweight: owning ring's seq id */
    const void    *p1;
    const void    *p2;
    long long      aux;
    long long      ts_ns;
} runloom_evt_t;

typedef struct runloom_ring {
    runloom_evt_t      slots[RUNLOOM_RING_CAP];
    unsigned long   head;         /* monotonic counter; only owner writes */
    unsigned int    tid;          /* registry seq */
    struct runloom_ring *next;       /* registry list */
} runloom_ring_t;

/* TLS: each thread's own ring.  Lazily allocated. */
static RUNLOOM_TLS runloom_ring_t *runloom_tls_ring = NULL;

/* Registry list head + lock.  Used only during ring creation and
 * during runloom_diag_dump; emission never touches the registry. */
static runloom_ring_t  *runloom_ring_list = NULL;
static runloom_mutex_t  runloom_ring_list_lock;
static int           runloom_ring_list_lock_inited = 0;
static unsigned int  runloom_ring_next_tid = 0;

static long long monotonic_ns(void)
{
#if defined(CLOCK_MONOTONIC)
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0)
        return (long long)ts.tv_sec * 1000000000LL + (long long)ts.tv_nsec;
#endif
    return 0;
}

static runloom_ring_t *ring_acquire(void)
{
    runloom_ring_t *r = runloom_tls_ring;
    if (r != NULL) return r;
    r = (runloom_ring_t *)calloc(1, sizeof(*r));
    if (r == NULL) return NULL;
    RUNLOOM_RLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    r->tid  = ++runloom_ring_next_tid;
    r->next = runloom_ring_list;
    runloom_ring_list = r;
    RUNLOOM_RUNLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    runloom_tls_ring = r;
    return r;
}

void runloom_evt_log_(runloom_evt_op_t op,
                   const void *p1, const void *p2, long long aux)
{
    runloom_ring_t *r;
    runloom_evt_t  *s;
    unsigned long idx;
    if (!runloom_ring_list_lock_inited) return;     /* before init */
    r = ring_acquire();
    if (r == NULL) return;
    idx = r->head++;
    s = &r->slots[idx & RUNLOOM_RING_MASK];
    s->op    = (unsigned int)op;
    s->tid   = r->tid;
    s->p1    = p1;
    s->p2    = p2;
    s->aux   = aux;
    s->ts_ns = monotonic_ns();
}

int runloom_diag_registered_thread_count(void)
{
    int n = 0;
    runloom_ring_t *r;
    if (!runloom_ring_list_lock_inited) return 0;
    RUNLOOM_RLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    for (r = runloom_ring_list; r != NULL; r = r->next) n++;
    RUNLOOM_RUNLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    return n;
}

static const char *op_name(unsigned int op)
{
    switch ((runloom_evt_op_t)op) {
    case RUNLOOM_EVT_PARKER_LINK:    return "PARK_LINK";
    case RUNLOOM_EVT_PARKER_UNLINK:  return "PARK_UNLINK";
    case RUNLOOM_EVT_PARKER_WAKE:    return "PARK_WAKE";
    case RUNLOOM_EVT_PARKER_TIMEOUT: return "PARK_TIMEOUT";
    case RUNLOOM_EVT_PARKER_GHOST:   return "PARK_GHOST";
    case RUNLOOM_EVT_PARKER_FORCE:   return "PARK_FORCE";
    case RUNLOOM_EVT_G_TRANSITION:   return "G_TRANSITION";
    case RUNLOOM_EVT_G_SUBMIT:       return "G_SUBMIT";
    case RUNLOOM_EVT_G_POP:          return "G_POP";
    case RUNLOOM_EVT_G_DECREF:       return "G_DECREF";
    case RUNLOOM_EVT_G_COMPLETE:     return "G_COMPLETE";
    case RUNLOOM_EVT_CHAN_PARK:      return "CHAN_PARK";
    case RUNLOOM_EVT_CHAN_WAKE:      return "CHAN_WAKE";
    case RUNLOOM_EVT_CORO_ACQUIRE:   return "CORO_ACQUIRE";
    case RUNLOOM_EVT_CORO_RELEASE:   return "CORO_RELEASE";
    case RUNLOOM_EVT_CAL_FREEZE:     return "CAL_FREEZE";
    case RUNLOOM_EVT_HANDOFF_ADOPT:  return "HANDOFF_ADOPT";
    case RUNLOOM_EVT_WORLD_YIELD:    return "WORLD_YIELD";
    case RUNLOOM_EVT_SNAP_SAVE:      return "SNAP_SAVE";
    case RUNLOOM_EVT_SNAP_LOAD:      return "SNAP_LOAD";
    default:                      return "?";
    }
}

static void emit(int fd, const char *buf, size_t len)
{
    if (fd < 0) { (void)fwrite(buf, 1, len, stderr); return; }
#if defined(_WIN32)
    (void)_write(fd, buf, (unsigned)len);
#else
    ssize_t off = 0;
    while ((size_t)off < len) {
        ssize_t w = write(fd, buf + off, len - off);
        if (w <= 0) break;
        off += w;
    }
#endif
}

void runloom_diag_dump(int fd)
{
    runloom_ring_t *r;
    char hdr[256];
    int n;
    if (!runloom_ring_list_lock_inited) {
        emit(fd, "[runloom-diag] not initialised\n", 28);
        return;
    }
    n = snprintf(hdr, sizeof hdr,
        "[runloom-diag] flags=0x%x threads=%d ring_cap=%u\n",
        runloom_debug_flags, runloom_diag_registered_thread_count(),
        (unsigned)RUNLOOM_RING_CAP);
    if (n > 0) emit(fd, hdr, (size_t)n);

    RUNLOOM_RLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    for (r = runloom_ring_list; r != NULL; r = r->next) {
        unsigned long head = r->head;
        unsigned long count = head < RUNLOOM_RING_CAP ? head : RUNLOOM_RING_CAP;
        unsigned long i;
        n = snprintf(hdr, sizeof hdr,
            "[runloom-diag] tid=%u events=%lu (head=%lu)\n",
            r->tid, count, head);
        if (n > 0) emit(fd, hdr, (size_t)n);
        /* Newest-first walk. */
        for (i = 0; i < count; i++) {
            unsigned long idx = (head - 1 - i) & RUNLOOM_RING_MASK;
            const runloom_evt_t *e = &r->slots[idx];
            char line[200];
            int m;
            m = snprintf(line, sizeof line,
                "  ts=%lld op=%-12s p1=%p p2=%p aux=%lld\n",
                e->ts_ns, op_name(e->op),
                (void *)e->p1, (void *)e->p2, e->aux);
            if (m > 0) emit(fd, line, (size_t)m);
        }
    }
    RUNLOOM_RUNLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
}


/* Determinism tooling #1: flight-recorder dump for the fatal-signal handler.
 * The most recent `max_per_thread` events of every thread's ring, newest-first
 * -- the schedule that led to the crash.  Unlike runloom_diag_dump this takes
 * NO lock (the registry is append-at-head under the creation lock; a torn read
 * at worst garbles one line) and is bounded, so it is safe enough to call from
 * the crash handler on the sigaltstack.  No-op if the ring was never enabled
 * (RUNLOOM_DEBUG=ring).  The full 1024-event rings remain in the core for
 * offline inspection via runloom_diag_dump / a gdb script. */
void runloom_evt_crash_dump(int fd, unsigned max_per_thread)
{
    static const char banner[] =
        "\n[runloom] flight recorder -- recent scheduler events (newest first):\n";
    runloom_ring_t *r = (runloom_ring_t *)__atomic_load_n((void **)&runloom_ring_list, __ATOMIC_ACQUIRE);
    if (r == NULL) return;   /* ring never enabled / no events */
    emit(fd, banner, sizeof banner - 1);
    for (; r != NULL; r = r->next) {
        unsigned long head  = r->head;
        unsigned long count = head < RUNLOOM_RING_CAP ? head : RUNLOOM_RING_CAP;
        unsigned long i;
        char hdr[96];
        int n;
        if (count == 0) continue;
        if (count > max_per_thread) count = max_per_thread;
        n = snprintf(hdr, sizeof hdr, "[runloom]   tid=%u (last %lu of %lu):\n",
                     r->tid, count, head);
        if (n > 0) emit(fd, hdr, (size_t)n);
        for (i = 0; i < count; i++) {
            unsigned long idx = (head - 1 - i) & RUNLOOM_RING_MASK;
            const runloom_evt_t *e = &r->slots[idx];
            char line[160];
            int m = snprintf(line, sizeof line,
                "[runloom]     %-13s p1=%p p2=%p aux=%lld\n",
                op_name(e->op), (void *)e->p1, (void *)e->p2, e->aux);
            if (m > 0) emit(fd, line, (size_t)m);
        }
    }
}


/* ---------------------------------------------------------------- *
 *  Self check                                                      *
 *                                                                  *
 *  Implemented as a friend of netpoll.c: netpoll exposes the       *
 *  inspection primitives below, this just orchestrates the walk.   *
 * ---------------------------------------------------------------- */

/* Hooks into netpoll.c.  Implemented there because they need access
 * to the static parker structures; declared here so callers don't
 * have to include netpoll.h directly. */
struct runloom_self_check_stats {
    int  global_list_count;
    int  global_list_cycle;
    int  parked_total_atomic;
    int  bucket_count_total;
    int  bucket_self_loops;
    int  bucket_unreachable;
};

extern int runloom_netpoll_inspect_for_self_check(
    struct runloom_self_check_stats *out);

/* Setter exposed so netpoll.c can fill in the stats without including
 * runloom_diag.c's private struct definition. */
void runloom_self_check_stats_set(struct runloom_self_check_stats *out,
                               int global_list_count,
                               int global_list_cycle,
                               int parked_total_atomic,
                               int bucket_count_total,
                               int bucket_self_loops,
                               int bucket_unreachable)
{
    if (out == NULL) return;
    out->global_list_count    = global_list_count;
    out->global_list_cycle    = global_list_cycle;
    out->parked_total_atomic  = parked_total_atomic;
    out->bucket_count_total   = bucket_count_total;
    out->bucket_self_loops    = bucket_self_loops;
    out->bucket_unreachable   = bucket_unreachable;
}

int runloom_self_check(int verbose)
{
    struct runloom_self_check_stats s;
    int violations = 0;
    char buf[400];
    int n;
    memset(&s, 0, sizeof s);
    if (runloom_netpoll_inspect_for_self_check(&s) != 0) {
        emit(-1, "[runloom-diag] self_check: netpoll inspect failed\n", 47);
        return 1;
    }
    if (s.global_list_cycle) {
        n = snprintf(buf, sizeof buf,
            "[runloom-diag] self_check: GLOBAL LIST CYCLE detected "
            "(walked %d entries before cycle)\n", s.global_list_count);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (s.bucket_self_loops > 0) {
        n = snprintf(buf, sizeof buf,
            "[runloom-diag] self_check: %d per-fd buckets self-loop\n",
            s.bucket_self_loops);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (!s.global_list_cycle &&
        s.global_list_count != s.parked_total_atomic) {
        n = snprintf(buf, sizeof buf,
            "[runloom-diag] self_check: parked_total=%d but walked=%d\n",
            s.parked_total_atomic, s.global_list_count);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (s.bucket_unreachable > 0) {
        n = snprintf(buf, sizeof buf,
            "[runloom-diag] self_check: %d bucket entries not in global list\n",
            s.bucket_unreachable);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    /* Handle-table integrity (runtime-fsck extension, QA-steal-V2 #2): the
     * self_check was netpoll-only; also sweep the rl_handle table so a lost
     * incref/decref (live count != rl_handle_live) or a pin-vs-reclaim race
     * (a live slot with a NULL ptr) is caught structurally, not as a later UAF. */
    {
        long hw = 0, ha = 0, hd = 0;
        int hv = rl_handle_self_check(&hw, &ha, &hd);
        if (hv) {
            n = snprintf(buf, sizeof buf,
                "[runloom-diag] self_check: handle-table -- live_walked=%ld "
                "rl_handle_live=%ld dangling(rc>0,ptr=NULL)=%ld\n", hw, ha, hd);
            emit(-1, buf, (size_t)n);
            violations += hv;
        }
    }
    if (verbose && violations == 0) {
        n = snprintf(buf, sizeof buf,
            "[runloom-diag] self_check: OK (parked=%d buckets=%d)\n",
            s.global_list_count, s.bucket_count_total);
        emit(-1, buf, (size_t)n);
    }
    return violations;
}


#ifdef RUNLOOM_KCSAN
/* ---- KCSAN-style delay-and-recheck exclusive-access watchpoints (item #8) ---- */
#include <time.h>
RUNLOOM_TLS uint32_t runloom_kcsan_ctr = 0;

void runloom_kcsan_stall(void)
{
    /* ~1 us busy-ish sleep to widen the race window; NOT a fiber yield (we must
     * not switch mid-watchpoint). */
    struct timespec ts = { 0, 1000 };
    nanosleep(&ts, NULL);
}

void runloom_kcsan_violation(const char *where, uint64_t before, uint64_t after)
{
    char buf[256];
    int n = snprintf(buf, sizeof buf,
        "[runloom-diag] KCSAN exclusive-access violation at %s: watched word "
        "0x%016llx -> 0x%016llx across a supposedly-single-owner window "
        "(a concurrent writer -- a data race)\n",
        where, (unsigned long long)before, (unsigned long long)after);
    emit(-1, buf, (size_t)n);
    runloom_invariant_fail("kcsan_exclusive_access",
                           (const void *)(uintptr_t)before,
                           (const void *)(uintptr_t)after);
}
#endif /* RUNLOOM_KCSAN */


/* ---------------------------------------------------------------- *
 *  Init / fini                                                     *
 * ---------------------------------------------------------------- */

static int runloom_diag_inited = 0;

/* ---- gilstate-lifecycle trace (TLA+ trace conformance, RUNLOOM_GILSTATE_TRACE) ----
 * Opened once in runloom_diag_init (single-thread module load); the emit is a
 * cold path (once per hub tstate create/delete) so a mutex + line append is fine.
 * No-op when the file is NULL (env unset). */
static FILE           *runloom_gil_trace_fp = NULL;
static runloom_mutex_t runloom_gil_trace_lock;

void runloom_gilstate_trace(const char *action, int hub, int deleter)
{
    if (runloom_gil_trace_fp == NULL) return;
    RUNLOOM_RLOCK(&runloom_gil_trace_lock, RUNLOOM_RANK_TRACE);
    fprintf(runloom_gil_trace_fp,
            "{\"a\":\"%s\",\"h\":%d,\"d\":%d}\n", action, hub, deleter);
    fflush(runloom_gil_trace_fp);
    RUNLOOM_RUNLOCK(&runloom_gil_trace_lock, RUNLOOM_RANK_TRACE);
}

/* ---- controlled-baton event trace (TLA+ trace conformance, RUNLOOM_MN_EVENTS) ----
 * Emits the baton protocol events (Arrive/Rendezvous/Grant/Release, hub id) so
 * tools/tla_trace_conform.py can drive RunloomMNControl's OWN actions from a real
 * run and have TLC check MutualExclusion + the grant/release protocol against the
 * code.  Opened once in runloom_diag_init; no-op when the file is NULL.  Cold-ish
 * (per baton transition); a mutex + line append is fine -- analysis mode, the
 * slight perturbation just yields a different (still legal) schedule. */
static FILE           *runloom_mn_trace_fp = NULL;
static runloom_mutex_t runloom_mn_trace_lock;

void runloom_mn_trace_event(const char *action, int hub)
{
    if (runloom_mn_trace_fp == NULL) return;
    RUNLOOM_RLOCK(&runloom_mn_trace_lock, RUNLOOM_RANK_TRACE);
    fprintf(runloom_mn_trace_fp, "{\"a\":\"%s\",\"h\":%d}\n", action, hub);
    fflush(runloom_mn_trace_fp);
    RUNLOOM_RUNLOCK(&runloom_mn_trace_lock, RUNLOOM_RANK_TRACE);
}

/* ---- netpoll-drain WAKE protocol trace (TLA+ trace conformance, RUNLOOM_WAKE_TRACE) ----
 * Emits the wake-handshake transitions of the SINGLE-THREAD drain vs a foreign
 * waker (blockpool worker) -- FOREIGN_WAKE (durable wake_list append) / POKE /
 * DRAIN_DEC on the worker, DRAIN_CONSUME / DRAIN_BLOCK / DRAIN_UNBLOCK / RESUME on
 * the owner -- so tools/wake_trace_conform.py can replay them through
 * RunloomWake.tla's OWN actions under TLC and check the binary's wake behaviour is
 * a SAFETY refinement of the proven model (ResumeIsTerminal + every observed
 * transition enabled; a resume/consume with no durable append deadlocks TLC).  The
 * `g` field is the raw fiber-pointer token (opaque; only matched for episode
 * identity by the driver), `cap` is meaningful only on DRAIN_BLOCK (1 == the 2 ms
 * foreign-wake backstop was armed).  Opened once in runloom_diag_init; a NULL fp
 * is a single predictable-not-taken load => inert/zero-cost in production.  All
 * emit sites are cold (park/wake/drain-block), never the same-thread fast path. */
static FILE           *runloom_wake_trace_fp = NULL;
static runloom_mutex_t runloom_wake_trace_lock;

void runloom_wake_trace_event(const char *action, unsigned long g, int cap)
{
    if (runloom_wake_trace_fp == NULL) return;
    RUNLOOM_RLOCK(&runloom_wake_trace_lock, RUNLOOM_RANK_TRACE);
    fprintf(runloom_wake_trace_fp,
            "{\"a\":\"%s\",\"g\":%lu,\"cap\":%d}\n", action, g, cap);
    fflush(runloom_wake_trace_fp);
    RUNLOOM_RUNLOCK(&runloom_wake_trace_lock, RUNLOOM_RANK_TRACE);
}

/* ---- M:N HUB-SUBMIT wake protocol trace (TLA+ trace conformance, RUNLOOM_MNWAKE_TRACE) ----
 * Sibling of runloom_wake_trace_event for the M:N hub-submit route (route A):
 * FOREIGN_WAKE (sub_head durable append) / SIGNAL (the idle_cond + wake-pump
 * kicks) on the foreign waker; HUB_DRAIN / HUB_BLOCK / HUB_UNBLOCK / RESUME on the
 * owner hub -- replayed against RunloomMNWake.tla by tools/mnwake_trace_conform.py.
 * `cap` is meaningful only on HUB_BLOCK (1 == the hub's idle wait is the bounded
 * ~1ms timed poll, the model's BoundedPoll arm).  All emit sites are at wake /
 * drain-with-work / idle-block / resume frequency -- NEVER the steady empty-poll
 * spin (HUB_DRAIN is emitted inside the non-empty drain branch only), so a NULL
 * fp is one predictable-not-taken load and production is unperturbed. */
static FILE           *runloom_mnwake_trace_fp = NULL;
static runloom_mutex_t runloom_mnwake_trace_lock;

/* True only when RUNLOOM_MNWAKE_TRACE opened the fp (set once at init).  Lets a
 * caller SHORT-CIRCUIT reads of advisory fields it would only pass to the trace,
 * so those reads never execute in production -- e.g. the g->snap.valid reads at
 * mn_sched_mn_api.c.inc:203/249, which otherwise race the owner hub's snap.valid
 * write at park/resume (a benign diagnostics-only race TSan-GOLD flags 1158x). */
int runloom_mnwake_trace_active(void)
{
    return runloom_mnwake_trace_fp != NULL;
}

void runloom_mnwake_trace_event(const char *action, unsigned long g, int cap)
{
    if (runloom_mnwake_trace_fp == NULL) return;
    RUNLOOM_RLOCK(&runloom_mnwake_trace_lock, RUNLOOM_RANK_TRACE);
    fprintf(runloom_mnwake_trace_fp,
            "{\"a\":\"%s\",\"g\":%lu,\"cap\":%d}\n", action, g, cap);
    fflush(runloom_mnwake_trace_fp);
    RUNLOOM_RUNLOCK(&runloom_mnwake_trace_lock, RUNLOOM_RANK_TRACE);
}

/* ---- io_uring CQE wake protocol trace (TLA+ trace conformance, RUNLOOM_IOUWAKE_TRACE) ----
 * Sibling of runloom_wake_trace_event for the io_uring CQE drain route: SUBMIT (an
 * SQE submitted + the fiber about to park) on the submitter; DRAIN_FLUSH (the
 * GETEVENTS overflow flush -- the CQ-overflow heal fired), DRAIN_CONSUME (a CQE
 * walked + its fiber readied), DRAIN_BLOCK / DRAIN_UNBLOCK (the pump block while an
 * iouring op is inflight) and RESUME (the woken submitter returns from park) on the
 * drainer/owner -- replayed against RunloomIouringWake.tla by
 * tools/iouwake_trace_conform.py.  `cap` is meaningful only on DRAIN_BLOCK (1 == the
 * drain-first overflow flush is armed while inflight>0, the model's Heal arm).  All
 * emit sites are cold (submit / overflow-flush / CQE-consume / pump-block / resume),
 * never a same-thread fast path; a NULL fp is one predictable-not-taken load so
 * production is unperturbed. */
static FILE           *runloom_iouwake_trace_fp = NULL;
static runloom_mutex_t runloom_iouwake_trace_lock;

void runloom_iouwake_trace_event(const char *action, unsigned long g, int cap)
{
    if (runloom_iouwake_trace_fp == NULL) return;
    RUNLOOM_RLOCK(&runloom_iouwake_trace_lock, RUNLOOM_RANK_TRACE);
    fprintf(runloom_iouwake_trace_fp,
            "{\"a\":\"%s\",\"g\":%lu,\"cap\":%d}\n", action, g, cap);
    fflush(runloom_iouwake_trace_fp);
    RUNLOOM_RUNLOCK(&runloom_iouwake_trace_lock, RUNLOOM_RANK_TRACE);
}

void runloom_diag_init(void)
{
    if (runloom_diag_inited) return;
    runloom_mutex_init(&runloom_ring_list_lock);
    runloom_ring_list_lock_inited = 1;
    parse_runloom_debug_env();
    {
        const char *gt = getenv("RUNLOOM_GILSTATE_TRACE");
        if (gt != NULL && gt[0] != '\0') {
            runloom_mutex_init(&runloom_gil_trace_lock);
            runloom_gil_trace_fp = fopen(gt, "w");
        }
        {
            const char *mt = getenv("RUNLOOM_MN_EVENTS");
            if (mt != NULL && mt[0] != '\0') {
                runloom_mutex_init(&runloom_mn_trace_lock);
                runloom_mn_trace_fp = fopen(mt, "w");
            }
        }
        {
            const char *wt = getenv("RUNLOOM_WAKE_TRACE");
            if (wt != NULL && wt[0] != '\0') {
                runloom_mutex_init(&runloom_wake_trace_lock);
                runloom_wake_trace_fp = fopen(wt, "w");
            }
        }
        {
            const char *mw = getenv("RUNLOOM_MNWAKE_TRACE");
            if (mw != NULL && mw[0] != '\0') {
                runloom_mutex_init(&runloom_mnwake_trace_lock);
                runloom_mnwake_trace_fp = fopen(mw, "w");
            }
        }
        {
            const char *iw = getenv("RUNLOOM_IOUWAKE_TRACE");
            if (iw != NULL && iw[0] != '\0') {
                runloom_mutex_init(&runloom_iouwake_trace_lock);
                runloom_iouwake_trace_fp = fopen(iw, "w");
            }
        }
    }
    runloom_diag_inited = 1;
}

void runloom_diag_reset_after_fork(void)
{
    /* Forked child: re-init the registry lock (a dead thread may have held
     * it at fork) and abandon the inherited per-thread ring list (those
     * rings belonged to threads that no longer exist; leak them rather than
     * free, since their owning threads are gone).  Single-thread child. */
    if (!runloom_diag_inited) return;
    runloom_mutex_init(&runloom_ring_list_lock);
    runloom_ring_list = NULL;
    runloom_ring_next_tid = 0;
    runloom_tls_ring = NULL;
}

void runloom_diag_fini(void)
{
    runloom_ring_t *r, *next;
    if (!runloom_diag_inited) return;
    RUNLOOM_RLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    r = runloom_ring_list;
    runloom_ring_list = NULL;
    RUNLOOM_RUNLOCK(&runloom_ring_list_lock, RUNLOOM_RANK_RING_LIST);
    while (r != NULL) {
        next = r->next;
        free(r);
        r = next;
    }
    runloom_mutex_destroy(&runloom_ring_list_lock);
    runloom_ring_list_lock_inited = 0;
    runloom_diag_inited = 0;
    runloom_tls_ring = NULL;   /* don't free TLS rings on fini: they're freed above */
}


/* ====================================================================== *
 * Determinism tooling #2: seeded delay injection                          *
 * ====================================================================== */

/* -1 = env not yet read, 0 = off, 1 = on. */
static int                runloom_delay_on      = -1;
static unsigned long long runloom_delay_seed    = 0;
static long long          runloom_delay_max_ns  = 50000;   /* 50 us default */
/* BUGGIFY (FoundationDB): -1 unread / 0 off / 1 on.  Unlike RUNLOOM_DELAY (all
 * sites, every hit), BUGGIFY randomly ENABLES ~half the sites per seeded run and
 * an enabled site fires only ~25% of hits -- so each seed co-activates a
 * DIFFERENT deep-state fault subset, making rare internal interleavings common
 * and cheap.  Pairs with _delay_freeze() (the recovery deadline) + the
 * liveness/drain oracle: fault under one seed, freeze, then prove drain. */
static int                runloom_buggify_on    = -1;
static unsigned long long runloom_buggify_seed  = 0;
/* Per-site monotonic call counter.  Under the controlled (serial) scheduler
 * the increment order is deterministic, so (seed, site, count) -> the same
 * delay every run == replayable.  Under parallel execution the count a given
 * call observes is racy, so it is seeded stress (still amplifies the window). */
static unsigned long long runloom_delay_ctr[RUNLOOM_DLY_NSITES];

static unsigned long long runloom_splitmix64(unsigned long long x)
{
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

static void runloom_delay_init_once(void)
{
    const char *e = getenv("RUNLOOM_DELAY");
    if (e == NULL || e[0] == '\0') {
        __atomic_store_n(&runloom_delay_on, 0, __ATOMIC_RELEASE);
        return;
    }
    runloom_delay_seed = strtoull(e, NULL, 0);
    {
        const char *m = getenv("RUNLOOM_DELAY_MAX_NS");
        if (m != NULL && m[0] != '\0') {
            long long v = atoll(m);
            if (v >= 0) runloom_delay_max_ns = v;
        }
    }
    __atomic_store_n(&runloom_delay_on, 1, __ATOMIC_RELEASE);
}

static void runloom_buggify_init_once(void)
{
    const char *e = getenv("RUNLOOM_BUGGIFY");
    if (e == NULL || e[0] == '\0') {
        __atomic_store_n(&runloom_buggify_on, 0, __ATOMIC_RELEASE);
        return;
    }
    runloom_buggify_seed = strtoull(e, NULL, 0);
    {   /* BUGGIFY reuses the delay machinery (RUNLOOM_DELAY_MAX_NS honored). */
        const char *m = getenv("RUNLOOM_DELAY_MAX_NS");
        if (m != NULL && m[0] != '\0') {
            long long v = atoll(m);
            if (v >= 0) runloom_delay_max_ns = v;
        }
    }
    __atomic_store_n(&runloom_buggify_on, 1, __ATOMIC_RELEASE);
}

/* Per-site enable: deterministic from (seed, site) so a rerun with the same seed
 * activates the SAME subset (replayable), while different seeds pick different
 * subsets.  ~50% of sites active per run. */
static int runloom_buggify_site_enabled(int site)
{
    unsigned long long h = runloom_splitmix64(
        runloom_buggify_seed ^ ((unsigned long long)site * 0x9E3779B97F4A7C15ULL));
    return (int)(h & 1ULL);
}

void runloom_delay_inject(runloom_delay_site_t site)
{
    int on  = __atomic_load_n(&runloom_delay_on, __ATOMIC_ACQUIRE);
    int bon = __atomic_load_n(&runloom_buggify_on, __ATOMIC_ACQUIRE);
    unsigned long long n, h, seed;
    long long ns;
    if (on < 0)  { runloom_delay_init_once();  on  = __atomic_load_n(&runloom_delay_on, __ATOMIC_ACQUIRE); }
    if (bon < 0) { runloom_buggify_init_once(); bon = __atomic_load_n(&runloom_buggify_on, __ATOMIC_ACQUIRE); }
    if ((int)site < 0 || site >= RUNLOOM_DLY_NSITES) return;
    if (runloom_delay_max_ns <= 0) return;
    if (bon == 1) {
        /* BUGGIFY: only sites active this run, and only ~25% of their hits. */
        if (!runloom_buggify_site_enabled((int)site)) return;
        seed = runloom_buggify_seed;
        n = __atomic_fetch_add(&runloom_delay_ctr[site], 1ULL, __ATOMIC_RELAXED);
        h = runloom_splitmix64(seed ^ ((unsigned long long)site << 56)
                            ^ runloom_splitmix64(n));
        if ((h & 3ULL) != 0) return;                /* fire ~25% of hits */
    } else {
        if (on != 1) return;
        seed = runloom_delay_seed;
        n = __atomic_fetch_add(&runloom_delay_ctr[site], 1ULL, __ATOMIC_RELAXED);
        /* Mix seed, site and the per-site count into a uniform delay. */
        h = runloom_splitmix64(seed ^ ((unsigned long long)site << 56)
                            ^ runloom_splitmix64(n));
    }
    ns = (long long)(h % (unsigned long long)(runloom_delay_max_ns + 1));
    if (ns > 0) runloom_sleep_ns(ns);
}

int runloom_delay_enabled(void)
{
    int on  = __atomic_load_n(&runloom_delay_on, __ATOMIC_ACQUIRE);
    int bon = __atomic_load_n(&runloom_buggify_on, __ATOMIC_ACQUIRE);
    if (on < 0)  { runloom_delay_init_once();  on  = __atomic_load_n(&runloom_delay_on, __ATOMIC_ACQUIRE); }
    if (bon < 0) { runloom_buggify_init_once(); bon = __atomic_load_n(&runloom_buggify_on, __ATOMIC_ACQUIRE); }
    return on == 1 || bon == 1;
}

/* Runtime freeze for the liveness/drain oracle (TigerBeetle
 * freeze-non-core-and-assert-drain): stop injecting scheduler delays so the
 * runtime must now PROVE forward progress -- drain every pending goroutine with
 * nothing parked forever.  The chaos phase (RUNLOOM_DELAY) widened park/wake/
 * steal/migration windows; this is the "stop faulting, now prove liveness"
 * transition.  Idempotent; any thread may call it. */
void runloom_delay_freeze(void)
{
    __atomic_store_n(&runloom_delay_on, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&runloom_buggify_on, 0, __ATOMIC_RELEASE);   /* freeze BUGGIFY too */
}


/* ====================================================================== *
 * Determinism tooling #3: invariant sanitizer                             *
 * ====================================================================== */

/* Report a violated runtime invariant LOUDLY and at the point it breaks --
 * print the message, dump the flight recorder (the schedule that led here),
 * and abort with a core -- instead of letting the corruption it implies turn
 * into a confusing crash later.  Called from gated checks (RUNLOOM_DBG_ON(
 * RUNLOOM_DBG_INVARIANTS)); never on a hot path when the flag is off. */
void runloom_invariant_fail(const char *msg, const void *p1, const void *p2)
{
    char buf[256];
    int n = snprintf(buf, sizeof buf,
        "\n[runloom-invariant] FATAL: %s (p1=%p p2=%p)\n"
        "[runloom-invariant] this is a runtime-invariant violation caught at the\n"
        "[runloom-invariant] point it occurred; the flight recorder below shows the\n"
        "[runloom-invariant] schedule that led here.\n",
        msg != NULL ? msg : "(no message)", p1, p2);
    if (n > 0) emit(2, buf, (size_t)n);   /* fd 2 directly: immediate, abort() won't flush stdio */
    runloom_evt_crash_dump(2, 32);
    runloom_diag_dump(2);
    abort();
}

/* ---- lock-rank checker storage (debug-only) ---- */
#ifdef RUNLOOM_LOCKRANK
#include "runloom_lockrank.h"
RUNLOOM_TLS int runloom_lockrank_held[RUNLOOM_LOCKRANK_DEPTH];
RUNLOOM_TLS int runloom_lockrank_depth = 0;

void runloom_lockrank_violation(int held, int acquired)
{
    /* Report each offending (held, acquired) pair once per process so a suite
     * run reveals the whole out-of-order set in one pass.  A small fixed table
     * is plenty -- the lock graph has a handful of classes. */
    static int seen_held[64];
    static int seen_acq[64];
    static int seen_n = 0;
    int i;
    for (i = 0; i < seen_n; i++)
        if (seen_held[i] == held && seen_acq[i] == acquired) return;
    if (seen_n < 64) { seen_held[seen_n] = held; seen_acq[seen_n] = acquired; seen_n++; }
    fprintf(stderr, "[runloom-lockrank] OUT-OF-ORDER: acquiring rank %d while "
                    "holding rank %d (locks must be taken in increasing rank "
                    "order)\n", acquired, held);
    fflush(stderr);
#ifdef RUNLOOM_LOCKRANK_ABORT
    abort();
#endif
}
#endif

/* ---- park/yield-safety checker storage (item 10, debug-only) ---- */
#ifdef RUNLOOM_CTXCHECK
RUNLOOM_TLS int runloom_ctx_noyield_depth = 0;

void runloom_ctx_parkable_violation(const char *where, int held_rank, int noyield)
{
    static const char *seen_where[64];
    static int seen_rank[64];
    static int seen_n = 0;
    int i;
    for (i = 0; i < seen_n; i++)
        if (seen_where[i] == where && seen_rank[i] == held_rank) return;
    if (seen_n < 64) { seen_where[seen_n] = where; seen_rank[seen_n] = held_rank;
                       seen_n++; }
    if (held_rank > 0)
        fprintf(stderr, "[runloom-ctxcheck] UNSAFE PARK at %s: yielding while "
                        "holding ranked lock %d (a woken sibling can hit that "
                        "lock / lock-order invert -> hang)\n", where, held_rank);
    else
        fprintf(stderr, "[runloom-ctxcheck] UNSAFE PARK at %s: yielding inside a "
                        "NO-YIELD region (depth %d; destructor/finalizer/preempt "
                        "-> frozen half-dead object)\n", where, noyield);
    fflush(stderr);
#ifdef RUNLOOM_CTXCHECK_ABORT
    abort();
#endif
}
#endif

/* ---- named reachability ("Sometimes()") counters (runloom_cover.h) -------- */
#include "runloom_cover.h"

static const char *const runloom_cov_names[RUNLOOM_COV__COUNT] = {
    "steal_hit",
    "deque_full_fallback",
    "global_runq_pull",
    "g_slab_spill",
    "g_slab_refill",
    "coro_pool_miss",
};

#if defined(RUNLOOM_COVER)
static unsigned long runloom_cov_hits[RUNLOOM_COV__COUNT];
void runloom_cover_bump(runloom_cov_point_t pt)
{
    if ((int)pt >= 0 && (int)pt < RUNLOOM_COV__COUNT)
        __atomic_add_fetch(&runloom_cov_hits[pt], 1, __ATOMIC_RELAXED);
}
unsigned long runloom_cover_get(int pt)
{
    if (pt < 0 || pt >= RUNLOOM_COV__COUNT) return 0;
    return __atomic_load_n(&runloom_cov_hits[pt], __ATOMIC_RELAXED);
}
void runloom_cover_reset(void)
{
    int i;
    for (i = 0; i < RUNLOOM_COV__COUNT; i++)
        __atomic_store_n(&runloom_cov_hits[i], 0, __ATOMIC_RELAXED);
}
int runloom_cover_enabled(void) { return 1; }
#else
unsigned long runloom_cover_get(int pt) { (void)pt; return 0; }
void runloom_cover_reset(void) { }
int runloom_cover_enabled(void) { return 0; }
#endif

const char *runloom_cover_name(int pt)
{
    if (pt < 0 || pt >= RUNLOOM_COV__COUNT) return NULL;
    return runloom_cov_names[pt];
}
int runloom_cover_num(void) { return RUNLOOM_COV__COUNT; }
