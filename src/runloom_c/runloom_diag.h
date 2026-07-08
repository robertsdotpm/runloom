/* runloom_diag.h -- runtime diagnostic infrastructure.
 *
 * Three things, all opt-in via the RUNLOOM_DEBUG env var (read once at
 * module init):
 *
 *   1. Lifecycle event ring per OS thread.  Lock-free, ~30 ns/event.
 *      Records (op, p1, p2, aux, ts).  Dumped on demand via
 *      runloom_diag_dump(fd) -- from gdb, a Python helper, or SIGUSR1.
 *      Off in release; turn on with RUNLOOM_DEBUG=ring (or =all).
 *
 *   2. runloom_self_check(verbose).  Walks every live data structure
 *      (parker lists, per-fd buckets, parked_total counter) and asserts
 *      invariants.  Returns the count of violations; prints details to
 *      stderr.  Safe to call from any thread; takes the parker lock.
 *      Cheap enough to run between bench iterations.
 *
 *   3. RUNLOOM_DEBUG env-var parsing.  Comma-separated token list:
 *        parker, gstate, invariants, ring, all, none.
 *      Tokens turn on the corresponding bit in runloom_debug_flags; checks
 *      throughout the codebase use bitwise & against that global.
 *
 * Threading: every state in this module is either TLS or atomic.  Safe
 * to call from any thread, including inside the parker lock. */
#ifndef RUNLOOM_DIAG_H
#define RUNLOOM_DIAG_H

#include "plat.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ---- runtime debug flags ----
 *
 * Read once at module init from the RUNLOOM_DEBUG env var.  Tokens
 * (comma-separated): parker | gstate | invariants | ring | all | none.
 * Default is 0 (all off).  Hot-path checks use RUNLOOM_DBG_ON(BIT). */
extern unsigned int runloom_debug_flags;

#define RUNLOOM_DBG_PARKER     (1u << 0)   /* extra checks in parker link/unlink */
#define RUNLOOM_DBG_GSTATE     (1u << 1)   /* g-state transition asserts */
#define RUNLOOM_DBG_INVARIANTS (1u << 2)   /* run self_check after each park/unpark */
#define RUNLOOM_DBG_RING       (1u << 3)   /* record lifecycle events */
#define RUNLOOM_DBG_ALL        (RUNLOOM_DBG_PARKER | RUNLOOM_DBG_GSTATE \
                             | RUNLOOM_DBG_INVARIANTS | RUNLOOM_DBG_RING)

#define RUNLOOM_DBG_ON(bit) \
    (__builtin_expect((runloom_debug_flags & (bit)) != 0, 0))

void runloom_diag_init(void);
void runloom_diag_fini(void);

/* Re-init the diag ring lock + drop the inherited ring list in a forked
 * child (the rings' owning threads are gone).  Single-thread child only. */
void runloom_diag_reset_after_fork(void);


/* ---- lifecycle event ring ----
 *
 * Op codes are dense so a printer can dispatch via a name table.
 * Add new ones at the end; the ring is purely advisory. */
typedef enum runloom_evt_op {
    RUNLOOM_EVT_NONE              = 0,
    RUNLOOM_EVT_PARKER_LINK       = 1,
    RUNLOOM_EVT_PARKER_UNLINK     = 2,
    RUNLOOM_EVT_PARKER_WAKE       = 3,
    RUNLOOM_EVT_PARKER_TIMEOUT    = 4,
    RUNLOOM_EVT_PARKER_GHOST      = 5,   /* defensive clear fired in link */
    RUNLOOM_EVT_PARKER_FORCE      = 6,   /* netpoll_force_unlink_g_parker */
    RUNLOOM_EVT_G_TRANSITION      = 7,   /* aux = (from << 8) | to */
    RUNLOOM_EVT_G_SUBMIT          = 8,
    RUNLOOM_EVT_G_POP             = 9,
    RUNLOOM_EVT_G_DECREF          = 10,
    RUNLOOM_EVT_G_COMPLETE        = 11,
    RUNLOOM_EVT_CHAN_PARK         = 12,
    RUNLOOM_EVT_CHAN_WAKE         = 13,
    /* Determinism tooling #1: scheduler transition events for the flight
     * recorder.  aux carries a site-specific scalar where useful. */
    RUNLOOM_EVT_CORO_ACQUIRE      = 14,  /* p1=coro p2=stack aux=size */
    RUNLOOM_EVT_CORO_RELEASE      = 15,  /* p1=coro p2=stack aux=size */
    RUNLOOM_EVT_CAL_FREEZE        = 16,  /* aux=chosen stack size */
    RUNLOOM_EVT_HANDOFF_ADOPT     = 17,  /* p1=hub p2=tstate */
    RUNLOOM_EVT_WORLD_YIELD       = 18,  /* p1=hub aux=pause_ns */
    RUNLOOM_EVT_SNAP_SAVE         = 19,  /* p1=g */
    RUNLOOM_EVT_SNAP_LOAD         = 20,  /* p1=g */
    RUNLOOM_EVT__LAST
} runloom_evt_op_t;

/* Append one event to the calling thread's TLS ring.  No-op if
 * RUNLOOM_DBG_RING is off.  Always callable; never blocks. */
void runloom_evt_log_(runloom_evt_op_t op,
                   const void *p1, const void *p2, long long aux);

#define RUNLOOM_EVT(op, p1, p2, aux)                                          \
    do {                                                                   \
        if (RUNLOOM_DBG_ON(RUNLOOM_DBG_RING))                                    \
            runloom_evt_log_((op), (const void *)(p1),                        \
                          (const void *)(p2), (long long)(aux));           \
    } while (0)

/* Dump every live thread's ring to fd, newest-first.  fd may be -1 to
 * route to stderr.  Takes the diag registry lock to keep the per-thread
 * list stable, but each ring is read non-blocking (we snapshot the head
 * index and walk backwards). */
void runloom_diag_dump(int fd);

/* Flight-recorder dump for the fatal-signal handler (determinism tooling #1):
 * the most recent `max_per_thread` events of every thread's ring, newest-first.
 * Lock-free + bounded, so it is safe to call from the crash handler.  No-op
 * unless RUNLOOM_DEBUG=ring was enabled. */
void runloom_evt_crash_dump(int fd, unsigned max_per_thread);


/* ---- self check ----
 *
 * Walks:
 *   - global parked list, asserts no cycle (Floyd), counts entries
 *   - every per-fd bucket, asserts no self-loop, counts entries
 *   - runloom_parked_total atomic counter, asserts matches walk count
 *
 * Returns the number of violations found.  When verbose != 0 also
 * prints a one-line OK summary on a clean pass.  Takes the parker
 * lock; cheap (O(N parked)), suitable for between-iteration calls in
 * stress benchmarks. */
int runloom_self_check(int verbose);


/* ---- thread registry ----
 *
 * Each OS thread that emits events registers its TLS ring exactly once,
 * lazily on the first RUNLOOM_EVT call.  runloom_diag_dump walks the registry
 * to dump every thread's ring without polling individual threads.
 * Exposed only for tests / sanity asserts. */
int runloom_diag_registered_thread_count(void);


/* ---- Determinism tooling #2: seeded delay injection ----
 *
 * OFF unless the RUNLOOM_DELAY env var is set; its value is the PRNG seed
 * (decimal or 0x...).  At each instrumented scheduler transition site,
 * runloom_delay_inject() sleeps a deterministic-per-(seed, site, per-site call
 * count) interval in [0, RUNLOOM_DELAY_MAX_NS] (default 50 us).  This widens
 * the narrow race windows -- STW / calibration-freeze / coro-reuse /
 * handoff-adopt / snap -- that the fuzzer otherwise hits ~1/56k, turning
 * "statistically findable" into "reliably reproducible".  Under the controlled
 * (serial) M:N scheduler the per-site call order is deterministic, so a given
 * seed replays the same schedule; under parallel execution it is seeded stress
 * (amplifies, not bit-reproducible).  Sites are dense; add at the end. */
typedef enum runloom_delay_site {
    RUNLOOM_DLY_PAINT = 0,          /* around the (now no-op) HWM paint */
    RUNLOOM_DLY_CAL_FREEZE,         /* the calibration freeze transition */
    RUNLOOM_DLY_CORO_ACQUIRE,       /* a coro/stack taken from the pool */
    RUNLOOM_DLY_CORO_RELEASE,       /* a coro/stack returned to the pool */
    RUNLOOM_DLY_HANDOFF_ADOPT,      /* rescue thread adopts a hub tstate */
    RUNLOOM_DLY_WORLD_YIELD,        /* the monopoly world-yield detach */
    RUNLOOM_DLY_SNAP_SAVE,          /* pystate snap (g yields) */
    RUNLOOM_DLY_SNAP_LOAD,          /* pystate load (g resumes) */
    RUNLOOM_DLY_G_ENTRY,            /* a fiber starts running */
    RUNLOOM_DLY_G_COMPLETE,         /* a fiber completes */
    RUNLOOM_DLY_HUB_RESUME,         /* a hub picks a g to resume */
    RUNLOOM_DLY_G_RESURRECT,        /* hub_submit try_incref -> in_sub_queue CAS
                                     * (the g-resurrection ABA window, sched_qref) */
    RUNLOOM_DLY_NSITES
} runloom_delay_site_t;

void runloom_delay_inject(runloom_delay_site_t site);
int  runloom_delay_enabled(void);   /* 1 if RUNLOOM_DELAY is set (chaos active) */
void runloom_delay_freeze(void);    /* stop injecting: liveness/drain "stop faulting" */


/* ---- Determinism tooling #3: invariant sanitizer ----
 *
 * Report a violated runtime invariant at the point it breaks (message +
 * flight-recorder dump + abort), so a lifecycle/ownership bug surfaces as a
 * clean, located abort instead of a confusing downstream crash.  Call only
 * from checks gated on RUNLOOM_DBG_ON(RUNLOOM_DBG_INVARIANTS) so production
 * (flag off) pays nothing.  Does not return. */
void runloom_invariant_fail(const char *msg, const void *p1, const void *p2);


/* ---- gilstate-lifecycle trace (TLA+ trace conformance) ----
 *
 * When RUNLOOM_GILSTATE_TRACE=<path> is set, the hub-tstate create/delete sites
 * append one ndjson event per line ({"a":<action>,"h":<hub id>,"d":<acting/
 * deleter thread, -1 = main>}).  tools/gen_trace_spec.py turns the file into a
 * RunloomGilstateTrace.tla that TLC validates against the REAL RunloomGilstate
 * actions -- i.e. it runs the model against the actual extension.  Off (and
 * zero-cost) unless the env var is set.  Cold path; takes a private lock. */
void runloom_gilstate_trace(const char *action, int hub, int deleter);


/* ---- controlled-baton event trace (TLA+ trace conformance, RUNLOOM_MN_EVENTS) ----
 * Emits one ndjson line per baton protocol transition ({"a":<Arrive|Rendezvous|
 * Grant|Release>,"h":<hub id>}); tools/tla_trace_conform.py drives RunloomMNControl
 * directly from it under TLC.  Off (zero-cost) unless the env var is set. */
void runloom_mn_trace_event(const char *action, int hub);


/* ---- netpoll-drain WAKE protocol trace (TLA+ trace conformance, RUNLOOM_WAKE_TRACE) ----
 * Emits one ndjson line per wake-handshake transition ({"a":<FOREIGN_WAKE|POKE|
 * DRAIN_DEC|DRAIN_CONSUME|DRAIN_BLOCK|DRAIN_UNBLOCK|RESUME>,"g":<fiber-ptr token>,
 * "cap":<0|1, backstop-armed, DRAIN_BLOCK only>}); tools/wake_trace_conform.py
 * replays it through RunloomWake.tla's OWN actions under TLC.  Off (zero-cost --
 * one predictable-NULL load) unless the env var is set. */
void runloom_wake_trace_event(const char *action, unsigned long g, int cap);


/* ---- M:N hub-submit wake trace (TLA+ trace conformance, RUNLOOM_MNWAKE_TRACE) ----
 * One ndjson line per M:N wake transition ({"a":<FOREIGN_WAKE|SIGNAL|HUB_DRAIN|
 * HUB_BLOCK|HUB_UNBLOCK|RESUME>,"g":<fiber-ptr token>,"cap":<0|1 bounded-poll,
 * HUB_BLOCK only>}); tools/mnwake_trace_conform.py replays it through
 * RunloomMNWake.tla.  Off (zero-cost -- one predictable-NULL load) unless set. */
void runloom_mnwake_trace_event(const char *action, unsigned long g, int cap);
int  runloom_mnwake_trace_active(void);   /* fp != NULL: gate advisory reads off in production */


/* ---- io_uring CQE wake trace (TLA+ trace conformance, RUNLOOM_IOUWAKE_TRACE) ----
 * Sibling of the wake/mnwake emitters for the io_uring CQE drain route: one
 * ndjson line per transition ({"a":<SUBMIT|DRAIN_FLUSH|DRAIN_CONSUME|RESUME|
 * DRAIN_BLOCK|DRAIN_UNBLOCK>,"g":<op-pointer token>,"cap":<0|1, DRAIN_BLOCK
 * only>}); tools/iouwake_trace_conform.py lowers it to RunloomIouringWake.tla's
 * OWN actions and TLC checks the binary is a SAFETY refinement (ResumeIsTerminal
 * + NoStrandedCompletion).  DRAIN_FLUSH is emitted ONLY on the GETEVENTS branch
 * of runloom_iouring_flush_cq_overflow (the CQ-overflow heal actually fired), so
 * its presence is the witness that overflow was induced.  Off (zero-cost -- one
 * predictable-NULL load) unless RUNLOOM_IOUWAKE_TRACE is set. */
void runloom_iouwake_trace_event(const char *action, unsigned long g, int cap);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_DIAG_H */
