/* pygo_diag.h -- runtime diagnostic infrastructure.
 *
 * Three things, all opt-in via the PYGO_DEBUG env var (read once at
 * module init):
 *
 *   1. Lifecycle event ring per OS thread.  Lock-free, ~30 ns/event.
 *      Records (op, p1, p2, aux, ts).  Dumped on demand via
 *      pygo_diag_dump(fd) -- from gdb, a Python helper, or SIGUSR1.
 *      Off in release; turn on with PYGO_DEBUG=ring (or =all).
 *
 *   2. pygo_self_check(verbose).  Walks every live data structure
 *      (parker lists, per-fd buckets, parked_total counter) and asserts
 *      invariants.  Returns the count of violations; prints details to
 *      stderr.  Safe to call from any thread; takes the parker lock.
 *      Cheap enough to run between bench iterations.
 *
 *   3. PYGO_DEBUG env-var parsing.  Comma-separated token list:
 *        parker, gstate, invariants, ring, all, none.
 *      Tokens turn on the corresponding bit in pygo_debug_flags; checks
 *      throughout the codebase use bitwise & against that global.
 *
 * Threading: every state in this module is either TLS or atomic.  Safe
 * to call from any thread, including inside the parker lock. */
#ifndef PYGO_DIAG_H
#define PYGO_DIAG_H

#include "plat.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ---- runtime debug flags ----
 *
 * Read once at module init from the PYGO_DEBUG env var.  Tokens
 * (comma-separated): parker | gstate | invariants | ring | all | none.
 * Default is 0 (all off).  Hot-path checks use PYGO_DBG_ON(BIT). */
extern unsigned int pygo_debug_flags;

#define PYGO_DBG_PARKER     (1u << 0)   /* extra checks in parker link/unlink */
#define PYGO_DBG_GSTATE     (1u << 1)   /* g-state transition asserts */
#define PYGO_DBG_INVARIANTS (1u << 2)   /* run self_check after each park/unpark */
#define PYGO_DBG_RING       (1u << 3)   /* record lifecycle events */
#define PYGO_DBG_ALL        (PYGO_DBG_PARKER | PYGO_DBG_GSTATE \
                             | PYGO_DBG_INVARIANTS | PYGO_DBG_RING)

#define PYGO_DBG_ON(bit) \
    (__builtin_expect((pygo_debug_flags & (bit)) != 0, 0))

void pygo_diag_init(void);
void pygo_diag_fini(void);


/* ---- lifecycle event ring ----
 *
 * Op codes are dense so a printer can dispatch via a name table.
 * Add new ones at the end; the ring is purely advisory. */
typedef enum pygo_evt_op {
    PYGO_EVT_NONE              = 0,
    PYGO_EVT_PARKER_LINK       = 1,
    PYGO_EVT_PARKER_UNLINK     = 2,
    PYGO_EVT_PARKER_WAKE       = 3,
    PYGO_EVT_PARKER_TIMEOUT    = 4,
    PYGO_EVT_PARKER_GHOST      = 5,   /* defensive clear fired in link */
    PYGO_EVT_PARKER_FORCE      = 6,   /* netpoll_force_unlink_g_parker */
    PYGO_EVT_G_TRANSITION      = 7,   /* aux = (from << 8) | to */
    PYGO_EVT_G_SUBMIT          = 8,
    PYGO_EVT_G_POP             = 9,
    PYGO_EVT_G_DECREF          = 10,
    PYGO_EVT_G_COMPLETE        = 11,
    PYGO_EVT_CHAN_PARK         = 12,
    PYGO_EVT_CHAN_WAKE         = 13,
    PYGO_EVT__LAST
} pygo_evt_op_t;

/* Append one event to the calling thread's TLS ring.  No-op if
 * PYGO_DBG_RING is off.  Always callable; never blocks. */
void pygo_evt_log_(pygo_evt_op_t op,
                   const void *p1, const void *p2, long long aux);

#define PYGO_EVT(op, p1, p2, aux)                                          \
    do {                                                                   \
        if (PYGO_DBG_ON(PYGO_DBG_RING))                                    \
            pygo_evt_log_((op), (const void *)(p1),                        \
                          (const void *)(p2), (long long)(aux));           \
    } while (0)

/* Dump every live thread's ring to fd, newest-first.  fd may be -1 to
 * route to stderr.  Takes the diag registry lock to keep the per-thread
 * list stable, but each ring is read non-blocking (we snapshot the head
 * index and walk backwards). */
void pygo_diag_dump(int fd);


/* ---- self check ----
 *
 * Walks:
 *   - global parked list, asserts no cycle (Floyd), counts entries
 *   - every per-fd bucket, asserts no self-loop, counts entries
 *   - pygo_parked_total atomic counter, asserts matches walk count
 *
 * Returns the number of violations found.  When verbose != 0 also
 * prints a one-line OK summary on a clean pass.  Takes the parker
 * lock; cheap (O(N parked)), suitable for between-iteration calls in
 * stress benchmarks. */
int pygo_self_check(int verbose);


/* ---- thread registry ----
 *
 * Each OS thread that emits events registers its TLS ring exactly once,
 * lazily on the first PYGO_EVT call.  pygo_diag_dump walks the registry
 * to dump every thread's ring without polling individual threads.
 * Exposed only for tests / sanity asserts. */
int pygo_diag_registered_thread_count(void);

#ifdef __cplusplus
}
#endif

#endif /* PYGO_DIAG_H */
