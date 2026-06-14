/* runloom_gstate.h -- observational fiber state machine.
 *
 * Records WHERE in its lifecycle a g is.  Independent of (but
 * consistent with) the existing CAS-gated membership flags
 * (in_sub_queue, done, coro, netpoll_parker).  Provides:
 *
 *   - a single atomic byte field on runloom_g_t encoding the current
 *     state (no extra alloc, no extra synchronisation)
 *   - RUNLOOM_G_TRANSITION(g, expected_from, to) macro: atomic CAS,
 *     records an event in the diag ring, asserts on illegal edges
 *     under RUNLOOM_DBG_GSTATE.  Release-store on success.
 *   - RUNLOOM_G_ASSERT_NOT(g, state_mask): cheap predicate for
 *     "this g must not currently be in any of these states"
 *
 * The state machine is intentionally a strict superset of the
 * existing implicit-state code: every legal transition still happens
 * exactly as before, the new field just records it.  Production
 * code (RUNLOOM_DBG_GSTATE off) gets a single atomic store per
 * transition and otherwise zero overhead.
 *
 * States are dense small integers so a single byte holds them, and
 * the transition matrix fits in a 64-bit lookup table.
 *
 * Why not replace the existing flags wholesale?  Because they're load-
 * bearing in concurrent code already shipped to bench-stable.  Adding
 * an observational layer surfaces violations the same as a hard
 * machine, but the cleanup of the dual-state representation is a
 * follow-up that can be done one site at a time. */
#ifndef RUNLOOM_GSTATE_H
#define RUNLOOM_GSTATE_H

#include "plat.h"
#include "plat_atomic.h"
#include "runloom_diag.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Dense enum.  Values are bit positions so we can build masks for
 * RUNLOOM_G_ASSERT_NOT (an enum-of-flags representation would double
 * the byte requirement). */
typedef enum runloom_g_state {
    RUNLOOM_GST_INIT          = 0,     /* freshly allocated, no coro yet */
    RUNLOOM_GST_SPAWNING      = 1,     /* coro allocated, not yet queued */
    RUNLOOM_GST_RUNNABLE      = 2,     /* in a ready queue (sched, hub deque, hub FIFO) */
    RUNLOOM_GST_SUBMITTED     = 3,     /* in a hub sub_head, awaiting drain */
    RUNLOOM_GST_RUNNING       = 4,     /* currently executing on a thread */
    RUNLOOM_GST_PARKED_NETPOLL= 5,     /* parker in netpoll global+bucket */
    RUNLOOM_GST_PARKED_CHAN   = 6,     /* on a chan send/recv waiter list */
    RUNLOOM_GST_PARKED_SLEEP  = 7,     /* in a sched sleep_heap */
    RUNLOOM_GST_PARKED_SAFE   = 8,     /* park_safe + wake_pending dance */
    RUNLOOM_GST_WAKING        = 9,     /* wake_g chose us, on its way to a queue */
    RUNLOOM_GST_DONE          = 10,    /* coro exited, awaiting last decref */
    RUNLOOM_GST_FREED         = 11,    /* in slab freelist; never observed in C */
    RUNLOOM_GST__LAST         = 12
} runloom_g_state_t;

/* Masks for assertion predicates. */
#define RUNLOOM_GST_BIT(s)        (1u << (unsigned)(s))
#define RUNLOOM_GST_MASK_PARKED   (RUNLOOM_GST_BIT(RUNLOOM_GST_PARKED_NETPOLL) \
                                | RUNLOOM_GST_BIT(RUNLOOM_GST_PARKED_CHAN)  \
                                | RUNLOOM_GST_BIT(RUNLOOM_GST_PARKED_SLEEP) \
                                | RUNLOOM_GST_BIT(RUNLOOM_GST_PARKED_SAFE))
#define RUNLOOM_GST_MASK_DEAD     (RUNLOOM_GST_BIT(RUNLOOM_GST_DONE) \
                                | RUNLOOM_GST_BIT(RUNLOOM_GST_FREED))

/* Wait-reason taxonomy.  The g state distinguishes the broad block class
 * (chan / netpoll / sleep / safe); this byte subdivides the otherwise-opaque
 * PARKED_SAFE class so the deadlock/wedge dump can say WHY a fiber is blocked
 * (a Future await vs a WaitGroup vs a lock vs an executor handoff).  Append-only
 * (0 = unset); set by the higher-level primitive before it parks, consumed at
 * park_safe.  A diagnostic aid only -- never load-bearing. */
typedef enum runloom_wait_reason {
    RUNLOOM_WR_NONE      = 0,   /* unset -> park_safe defaults to SYNC */
    RUNLOOM_WR_SYNC      = 1,   /* generic park_safe, no finer reason given */
    RUNLOOM_WR_FUTURE    = 2,   /* awaiting a Future / Task result */
    RUNLOOM_WR_WAITGROUP = 3,   /* WaitGroup.wait */
    RUNLOOM_WR_LOCK      = 4,   /* mutex / RLock acquire */
    RUNLOOM_WR_EVENT     = 5,   /* Event.wait */
    RUNLOOM_WR_CONDITION = 6,   /* Condition.wait */
    RUNLOOM_WR_BARRIER   = 7,   /* Barrier.wait */
    RUNLOOM_WR_SELECT    = 8,   /* channel select */
    RUNLOOM_WR_EXECUTOR  = 9,   /* run_in_executor / blockpool handoff */
    RUNLOOM_WR_SEMAPHORE = 10,  /* Semaphore.acquire */
    RUNLOOM_WR_QUEUE     = 11,  /* Queue get/put */
    RUNLOOM_WR__LAST     = 12
} runloom_wait_reason_t;

/* Forward; the actual unsigned char field lives on runloom_g_t. */
struct runloom_g;

/* Set state unconditionally; release-store.  Records a diag event.
 * Cheap (single byte store + branch); cost when RUNLOOM_DBG_RING is off
 * is just the byte store + branch. */
void runloom_g_state_set(struct runloom_g *g, runloom_g_state_t to);

/* CAS the state: from `from` to `to`.  Returns 1 on success, 0 on
 * mismatch.  Records an event on success.  When RUNLOOM_DBG_GSTATE is
 * on and the CAS fails, also logs the unexpected actual value. */
int  runloom_g_state_cas(struct runloom_g *g,
                      runloom_g_state_t from, runloom_g_state_t to);

/* Predicate: returns 1 if g is in any of the states whose bit is set
 * in `mask`.  Lock-free; uses acquire-load. */
int  runloom_g_state_in(const struct runloom_g *g, unsigned int mask);

/* Read current state (acquire-load). */
runloom_g_state_t runloom_g_state_get(const struct runloom_g *g);

/* Hard assert: aborts under RUNLOOM_DBG_GSTATE if g is in any state in
 * mask.  No-op in release.  Use sparingly at boundaries that should
 * NEVER see those states (e.g., "submit must not see DONE"). */
#define RUNLOOM_G_ASSERT_NOT(g, mask)                                          \
    do {                                                                    \
        if (RUNLOOM_DBG_ON(RUNLOOM_DBG_GSTATE) &&                                 \
            runloom_g_state_in((g), (mask))) {                                 \
            runloom_g_assert_failure_((g), (mask), __FILE__, __LINE__);        \
        }                                                                   \
    } while (0)

/* Internal: called by the assert macro on failure. */
void runloom_g_assert_failure_(const struct runloom_g *g, unsigned int mask,
                            const char *file, int line);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_GSTATE_H */
