/* chan.c -- Go-style channel implementation.
 *
 * State machine (under the channel's lock):
 *
 *   send path
 *     closed                -> error
 *     receivers waiting     -> direct handoff to first receiver, wake it
 *     buffer has room       -> push to ring
 *     else                  -> park self as sender, hold value, yield
 *
 *   recv path
 *     senders waiting + buffer empty
 *                           -> take value from first sender, wake them
 *     buffer non-empty      -> pop from ring (also wake one parked sender
 *                              if any -- that sender's value goes into
 *                              the now-freed buffer slot)
 *     closed                -> return (None, ok=0)
 *     else                  -> park self as receiver, yield
 *
 *   close path
 *     mark closed
 *     wake every parked sender with "channel closed" -> they raise
 *     wake every parked receiver -> they each return (None, ok=0)
 *
 * Lock-only synchronisation: the park/wake path itself is unlocked
 * (runloom_sched_park_current / runloom_sched_wake on global; mn variants
 * for hubs), so we drop the channel lock BEFORE yielding to avoid
 * holding it across an asm context switch.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"
#include "chan.h"
#include "coro.h"
#include "runloom_sched.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_fsm.h"

#include <stdlib.h>
#include <string.h>

/* ---- waiter records ---------------------------------------------- */
struct runloom_select_park;          /* forward decl */

typedef struct runloom_chan_waiter {
    runloom_g_t *g;
    void *hub;                       /* M:N hub opaque; NULL = global */
    /* For senders: value to deliver (we hold a ref, transferred to
     *    receiver on handoff).
     * For receivers: slot the producer fills with (value, ok). */
    PyObject *value;
    int ok;                          /* receiver out-flag: 1 = got value,
                                      *  0 = closed, -1 = unset */
    /* Result for senders: 0 = delivered, -1 = closed-while-parked. */
    int send_result;
    /* 1 while this waiter is LINKED in a channel queue; 0 once a producer (or
     * close) pops it.  park_waiter loops on this: a spurious / foreign wake
     * (stale dup wake_g, a wake meant for a prior park) can resume the parked
     * fiber while still queued, and returning then would (a) report a bogus
     * result and (b) leave this STACK-allocated waiter linked for a later send
     * to pop -> use-after-free (benign stale-read on POSIX; hard access
     * violation on Windows, where mimalloc unmaps the freed page).  Written +
     * read under ch->lock. */
    int queued;
    /* Non-NULL iff this waiter belongs to a select() set.  When a
     * channel goes to deliver to this waiter it first CASes
     * select->fired_case from -1 to its own case_index; only the
     * winning CAS proceeds with the handoff.  Stale tombstones from
     * losing channels get skipped via the same CAS check + removed
     * from their queues by the woken g (see runloom_chan_select). */
    struct runloom_select_park *select;
    int case_index;                  /* index into the select's cases[] */
    struct runloom_chan_waiter *next;
} runloom_chan_waiter_t;

struct runloom_chan {
    runloom_mutex_t lock;
    PyObject **buf;
    Py_ssize_t cap;
    Py_ssize_t head;                 /* next slot to pop from */
    Py_ssize_t tail;                 /* next slot to push to */
    Py_ssize_t len;                  /* number of values currently buffered */
    runloom_chan_waiter_t *senders;     /* FIFO queue of parked senders */
    runloom_chan_waiter_t *senders_tail;
    runloom_chan_waiter_t *receivers;
    runloom_chan_waiter_t *receivers_tail;
    int closed;
    int refcount;
};


/* ---- channel waiter `queued` FSM (OBSERVATIONAL, partial) -------------------
 * The waiter `queued` flag is a LOCK-PROTECTED binary lifecycle (written + read
 * under ch->lock): waiter_push sets it 1 (QUEUED), waiter_pop clears it 0
 * (NOT_QUEUED), and park_waiter's re-park loop (chan_waiters.c.inc) spins until a
 * producer/close actually pops it -- which is what already prevents the proven
 * p34 Windows UAF (a spurious wake returning while still linked left the
 * stack-allocated waiter for a later send to pop -> use-after-free).  Per the
 * FSM_ADOPTION.md decision rule this is HARDENED IN PLACE (explicit states +
 * documented invariant), not full-converted: the lock + the re-park loop already
 * make a missing-handler gap impossible, and a NOTE at waiter_push would read an
 * UNINITIALIZED `queued` on a fresh stack waiter (its FROM is garbage before the
 * first push).  So we NOTE only the ONE edge whose FROM is known-valid:
 * waiter_pop's QUEUED->NOT_QUEUED (a popped waiter was, by construction, pushed
 * QUEUED first).  The table documents both edges; only POP is asserted. */
enum {
    RUNLOOM_WQ_NOT_QUEUED = 0,   /* unlinked; park_waiter may return            */
    RUNLOOM_WQ_QUEUED     = 1,    /* linked in a channel queue                   */
    RUNLOOM_WQ_STATE_COUNT
};
enum {
    RUNLOOM_WQ_EV_PUSH = 0,      /* waiter_push: NOT_QUEUED -> QUEUED (not NOTE'd) */
    RUNLOOM_WQ_EV_POP,           /* waiter_pop:  QUEUED -> NOT_QUEUED              */
    RUNLOOM_WQ_EV_COUNT
};
static const signed char runloom_wq_table
        [RUNLOOM_WQ_STATE_COUNT][RUNLOOM_WQ_EV_COUNT]
        __attribute__((unused)) = {
    /*                          PUSH                  POP */
    [RUNLOOM_WQ_NOT_QUEUED] = { RUNLOOM_WQ_QUEUED,     RUNLOOM_FSM_INVALID  },
    [RUNLOOM_WQ_QUEUED]     = { RUNLOOM_FSM_INVALID,   RUNLOOM_WQ_NOT_QUEUED },
};
RUNLOOM_FSM_ASSERT_TABLE(runloom_wq_table, RUNLOOM_WQ_STATE_COUNT,
                         RUNLOOM_WQ_EV_COUNT, "chan_waiter_queued");
#define RUNLOOM_WQ_NOTE(from, to)                                             \
    RUNLOOM_FSM_NOTE("chan_waiter_queued", runloom_wq_table,                  \
                     RUNLOOM_WQ_STATE_COUNT, RUNLOOM_WQ_EV_COUNT, (from), (to))

/* ---- select `fired_case` claim FSM (OBSERVATIONAL) --------------------------
 * A select's multi-party claim race (the firing channel's waiter_claim vs the
 * select's own install-time readiness CAS vs other channels), Spin-verified in
 * verify/spin/select_claim.pml.  fired_case is -1 (UNCLAIMED) until the FIRST CAS
 * wins, then frozen at the winner's case_index (CLAIMED); a second claimer's CAS
 * fails and skips the tombstone -- exactly-once.  Two logical states: the field's
 * raw value is -1 or a case_index>=0, mapped to UNCLAIMED/CLAIMED for the
 * relation.  Every claim CAS only succeeds from expected==-1, so the asserted
 * edge is always UNCLAIMED->CLAIMED; CLAIMED is terminal (frozen). */
enum {
    RUNLOOM_SEL_UNCLAIMED = 0,   /* fired_case == -1                            */
    RUNLOOM_SEL_CLAIMED   = 1,    /* fired_case == some case_index >= 0          */
    RUNLOOM_SEL_STATE_COUNT
};
enum {
    RUNLOOM_SEL_EV_CLAIM = 0,    /* a channel/select claims the case            */
    RUNLOOM_SEL_EV_COUNT
};
static const signed char runloom_sel_table
        [RUNLOOM_SEL_STATE_COUNT][RUNLOOM_SEL_EV_COUNT]
        __attribute__((unused)) = {
    /*                        CLAIM */
    [RUNLOOM_SEL_UNCLAIMED] = { RUNLOOM_SEL_CLAIMED  },
    [RUNLOOM_SEL_CLAIMED]   = { RUNLOOM_FSM_INVALID  },   /* frozen: exactly-once */
};
RUNLOOM_FSM_ASSERT_TABLE(runloom_sel_table, RUNLOOM_SEL_STATE_COUNT,
                         RUNLOOM_SEL_EV_COUNT, "select_fired_case");
#define RUNLOOM_SEL_NOTE(from, to)                                            \
    RUNLOOM_FSM_NOTE("select_fired_case", runloom_sel_table,                  \
                     RUNLOOM_SEL_STATE_COUNT, RUNLOOM_SEL_EV_COUNT, (from), (to))


/* ---------------------------------------------------------------------------
 * chan.c is split across the chan_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only chan.c.
 * --------------------------------------------------------------------------- */
#include "chan_waiters.c.inc"
#include "chan_ops.c.inc"
#include "chan_select_helpers.c.inc"
#include "chan_select_main.c.inc"
