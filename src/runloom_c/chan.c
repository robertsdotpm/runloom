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
