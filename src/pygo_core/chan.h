/* chan.h -- Go-style channel: send / recv / close, blocking + non-blocking.
 *
 * Semantics (matches Go modulo names):
 *
 *   unbuffered (cap=0):
 *     send blocks until a receiver is ready
 *     recv blocks until a sender is ready
 *     handoff is direct (sender's value goes straight to receiver, no copy)
 *
 *   buffered (cap>0):
 *     send blocks only when the buffer is full
 *     recv blocks only when the buffer is empty AND no senders are parked
 *
 *   close:
 *     subsequent send raises (Go panics; we raise ValueError)
 *     pending sends still in the buffer drain normally
 *     recv returns the close sentinel after the buffer is empty
 *     double-close raises
 *
 * Concurrency:
 *   The channel lock is the only synchronisation primitive.  Park/wake
 *   piggybacks on the existing pygo_sched_wake / pygo_mn_wake_g path;
 *   each waiter records its hub_opaque + g so wake routes back to the
 *   right scheduler under M:N.
 *
 * Reference counting:
 *   send INCREFs the value into the channel; recv transfers that ref
 *   to the caller (no extra INCREF on the recv path).  close DECREFs
 *   anything left in the buffer.
 */
#ifndef PYGO_CHAN_H
#define PYGO_CHAN_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"

typedef struct pygo_chan pygo_chan_t;

/* Construct.  cap >= 0.  Returns NULL + PyErr_NoMemory on OOM. */
pygo_chan_t *pygo_chan_new(Py_ssize_t cap);

/* Refcount.  Channels are reference-counted because the Python wrapper
 * and any parked waiters may outlive the original creator scope. */
void pygo_chan_incref(pygo_chan_t *ch);
void pygo_chan_decref(pygo_chan_t *ch);

/* Send a value.  Steals NOTHING -- caller still owns its reference.
 *   on success: returns 0; the channel now holds an INCREF'd ref.
 *   on closed-send: returns -1, PyErr set to ValueError.
 *   in a goroutine: may park (yield) until a receiver appears or buffer
 *   has room.
 */
int pygo_chan_send(pygo_chan_t *ch, PyObject *value);

/* Non-blocking send.  Like send but returns:
 *   0  = sent  (channel got a new INCREF'd ref)
 *   1  = full / no receiver waiting  (no action taken)
 *  -1  = error (closed or memory) -- PyErr set
 */
int pygo_chan_try_send(pygo_chan_t *ch, PyObject *value);

/* Receive a value.  Returns a NEW reference to the caller.  The channel
 * loses its ref on the value (it was transferred to the caller).
 *
 *   on success: returns the value (new ref)
 *   on closed + empty: returns Py_None with the OUT-param ok set to 0.
 *                      (Matches Go's `v, ok := <-ch` idiom; if ok==0
 *                       the value is the channel's zero -- here None.)
 *   on error: returns NULL with PyErr set.
 *
 * In a goroutine: may park until a sender appears.
 */
PyObject *pygo_chan_recv(pygo_chan_t *ch, int *ok);

/* Non-blocking recv.
 *   *out:
 *     non-NULL = value (new ref); *ok = 1
 *     Py_None  + *ok = 0 = closed-and-empty (Go-style "no value")
 *     NULL     + *ok = -1 = would-block (no sender, no buffered value)
 *   Returns -1 on error (PyErr set), 0 otherwise.
 */
int pygo_chan_try_recv(pygo_chan_t *ch, PyObject **out, int *ok);

/* Close.  Wakes all parked senders (they raise) and all parked
 * receivers (they return the closed sentinel).  Idempotent? -- no:
 * matches Go, double-close raises.
 *
 * Returns 0 on success, -1 on error (PyErr set).
 */
int pygo_chan_close(pygo_chan_t *ch);

/* Introspection (mostly for tests). */
int  pygo_chan_is_closed(pygo_chan_t *ch);
Py_ssize_t pygo_chan_len(pygo_chan_t *ch);
Py_ssize_t pygo_chan_cap(pygo_chan_t *ch);

/* ---- select() ---- */
typedef enum {
    PYGO_SELECT_RECV = 0,
    PYGO_SELECT_SEND = 1,
} pygo_select_op_t;

typedef struct {
    pygo_chan_t *ch;
    pygo_select_op_t op;
    PyObject *send_value;       /* for SEND: ref-borrowed from caller */
    PyObject *recv_value;       /* for RECV: filled in (new ref) on hit */
    int recv_ok;                /* for RECV: 0/1 ok flag */
} pygo_select_case_t;

/* Wait on N cases.  If `default_ready` is non-zero, behave like Go's
 * `default:` branch -- if no case is immediately ready, return -1
 * instead of parking.  Otherwise block until one fires.
 *
 * Returns the index of the case that fired (>= 0), or -1 if
 * default-fired (no cases ready), or -2 on error (PyErr set).
 *
 * On a fired SEND case: the channel got an INCREF'd ref to send_value.
 * On a fired RECV case: recv_value holds a new ref, recv_ok is set.
 */
int pygo_chan_select(pygo_select_case_t *cases, int n, int default_ready);

#endif /* PYGO_CHAN_H */
