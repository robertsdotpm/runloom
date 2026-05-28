/* pygo_gstate.h -- observational goroutine state machine.
 *
 * Records WHERE in its lifecycle a g is.  Independent of (but
 * consistent with) the existing CAS-gated membership flags
 * (in_sub_queue, done, coro, netpoll_parker).  Provides:
 *
 *   - a single atomic byte field on pygo_g_t encoding the current
 *     state (no extra alloc, no extra synchronisation)
 *   - PYGO_G_TRANSITION(g, expected_from, to) macro: atomic CAS,
 *     records an event in the diag ring, asserts on illegal edges
 *     under PYGO_DBG_GSTATE.  Release-store on success.
 *   - PYGO_G_ASSERT_NOT(g, state_mask): cheap predicate for
 *     "this g must not currently be in any of these states"
 *
 * The state machine is intentionally a strict superset of the
 * existing implicit-state code: every legal transition still happens
 * exactly as before, the new field just records it.  Production
 * code (PYGO_DBG_GSTATE off) gets a single atomic store per
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
#ifndef PYGO_GSTATE_H
#define PYGO_GSTATE_H

#include "plat.h"
#include "plat_atomic.h"
#include "pygo_diag.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Dense enum.  Values are bit positions so we can build masks for
 * PYGO_G_ASSERT_NOT (an enum-of-flags representation would double
 * the byte requirement). */
typedef enum pygo_g_state {
    PYGO_GST_INIT          = 0,     /* freshly allocated, no coro yet */
    PYGO_GST_SPAWNING      = 1,     /* coro allocated, not yet queued */
    PYGO_GST_RUNNABLE      = 2,     /* in a ready queue (sched, hub deque, hub FIFO) */
    PYGO_GST_SUBMITTED     = 3,     /* in a hub sub_head, awaiting drain */
    PYGO_GST_RUNNING       = 4,     /* currently executing on a thread */
    PYGO_GST_PARKED_NETPOLL= 5,     /* parker in netpoll global+bucket */
    PYGO_GST_PARKED_CHAN   = 6,     /* on a chan send/recv waiter list */
    PYGO_GST_PARKED_SLEEP  = 7,     /* in a sched sleep_heap */
    PYGO_GST_PARKED_SAFE   = 8,     /* park_safe + wake_pending dance */
    PYGO_GST_WAKING        = 9,     /* wake_g chose us, on its way to a queue */
    PYGO_GST_DONE          = 10,    /* coro exited, awaiting last decref */
    PYGO_GST_FREED         = 11,    /* in slab freelist; never observed in C */
    PYGO_GST__LAST         = 12
} pygo_g_state_t;

/* Masks for assertion predicates. */
#define PYGO_GST_BIT(s)        (1u << (unsigned)(s))
#define PYGO_GST_MASK_PARKED   (PYGO_GST_BIT(PYGO_GST_PARKED_NETPOLL) \
                                | PYGO_GST_BIT(PYGO_GST_PARKED_CHAN)  \
                                | PYGO_GST_BIT(PYGO_GST_PARKED_SLEEP) \
                                | PYGO_GST_BIT(PYGO_GST_PARKED_SAFE))
#define PYGO_GST_MASK_DEAD     (PYGO_GST_BIT(PYGO_GST_DONE) \
                                | PYGO_GST_BIT(PYGO_GST_FREED))

/* Forward; the actual unsigned char field lives on pygo_g_t. */
struct pygo_g;

/* Set state unconditionally; release-store.  Records a diag event.
 * Cheap (single byte store + branch); cost when PYGO_DBG_RING is off
 * is just the byte store + branch. */
void pygo_g_state_set(struct pygo_g *g, pygo_g_state_t to);

/* CAS the state: from `from` to `to`.  Returns 1 on success, 0 on
 * mismatch.  Records an event on success.  When PYGO_DBG_GSTATE is
 * on and the CAS fails, also logs the unexpected actual value. */
int  pygo_g_state_cas(struct pygo_g *g,
                      pygo_g_state_t from, pygo_g_state_t to);

/* Predicate: returns 1 if g is in any of the states whose bit is set
 * in `mask`.  Lock-free; uses acquire-load. */
int  pygo_g_state_in(const struct pygo_g *g, unsigned int mask);

/* Read current state (acquire-load). */
pygo_g_state_t pygo_g_state_get(const struct pygo_g *g);

/* Hard assert: aborts under PYGO_DBG_GSTATE if g is in any state in
 * mask.  No-op in release.  Use sparingly at boundaries that should
 * NEVER see those states (e.g., "submit must not see DONE"). */
#define PYGO_G_ASSERT_NOT(g, mask)                                          \
    do {                                                                    \
        if (PYGO_DBG_ON(PYGO_DBG_GSTATE) &&                                 \
            pygo_g_state_in((g), (mask))) {                                 \
            pygo_g_assert_failure_((g), (mask), __FILE__, __LINE__);        \
        }                                                                   \
    } while (0)

/* Internal: called by the assert macro on failure. */
void pygo_g_assert_failure_(const struct pygo_g *g, unsigned int mask,
                            const char *file, int line);

#ifdef __cplusplus
}
#endif

#endif /* PYGO_GSTATE_H */
