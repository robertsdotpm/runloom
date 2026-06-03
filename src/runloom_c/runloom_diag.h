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

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_DIAG_H */
