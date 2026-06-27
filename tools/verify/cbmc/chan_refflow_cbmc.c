/* SOURCE-ANCHOR: runloom_chan_send runloom_chan_recv  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * chan_refflow_cbmc.c -- CBMC proof that a PyObject sent through a runloom channel
 * has its reference count CONSERVED across every send/recv/close/free path
 * (src/runloom_c/chan_ops.c.inc + chan_waiters.c.inc).  (LIFECYCLE_INVARIANTS.md
 * Tier-2 #8.)
 *
 * THE FLOW.  Each value the channel machinery accepts takes exactly ONE strong ref
 * (Py_INCREF) when it enters, via one of three send paths:
 *   - direct to a waiting RECEIVER  (chan_ops: Py_INCREF; rx->value = value)
 *   - into the ring BUFFER          (Py_INCREF; buf_push)
 *   - held by a parked SENDER       (Py_INCREF; w.value = value)
 * That one ref is released EXACTLY ONCE, by the value's terminal transition:
 *   - a RECEIVER consumes it (recv steals/transfers the ref, then drops it), OR
 *   - close() drops a parked sender's value (Py_DECREF, send_result = -1), OR
 *   - the final runloom_chan_decref drains the buffer (Py_XDECREF each remaining).
 *
 * INVARIANT: every value ends balanced -- acquired once, released once; its
 * machinery refcount is never < 0 (no double-drop / over-free) and never leaks
 * (no INCREF without a matching release).
 *
 * Negative controls (must FAIL = CBMC finds the imbalance):
 *   -DBUG_CLOSE_NO_SENDER_DROP : close() forgets the parked-sender Py_DECREF -> the
 *                                sender's value leaks.
 *   -DBUG_FREE_NO_BUFFER_DRAIN : the final decref frees without draining the buffer
 *                                -> every still-buffered value leaks.
 *   -DBUG_DOUBLE_CONSUME       : a value delivered to a receiver is ALSO dropped by
 *                                close/free -> its refcount goes negative (over-free).
 */

extern _Bool nondet_bool(void);
extern int   nondet_int(void);

#define NV 3                 /* values flowing through the channel */

/* per-value machinery refcount and location */
#define LOC_NONE     0       /* not yet sent */
#define LOC_RECEIVER 1       /* handed to a waiting receiver (pending consume) */
#define LOC_BUFFER   2       /* in the ring buffer */
#define LOC_SENDER   3       /* held by a parked sender */
#define LOC_DONE     4       /* consumed or dropped (terminal) */

static int rc[NV];           /* machinery refs Py_INCREF'd, not yet released */
static int loc[NV];
static int closed;
static int freed;

/* runloom_chan_send: take one ref and place the value.  A send that finds a
 * WAITING receiver delivers synchronously -- the receiver is woken and consumes
 * the ref immediately (net zero); the BUFFER / parked-SENDER paths hold the ref. */
static void chan_send(int v)
{
    if (loc[v] != LOC_NONE) return;
    if (closed) return;                       /* send on closed -> raises, no ref taken */
    int dest = nondet_int();
    __CPROVER_assume(dest >= LOC_RECEIVER && dest <= LOC_SENDER);
    rc[v] += 1;                               /* Py_INCREF (every send path) */
    if (dest == LOC_RECEIVER) {
        rc[v] -= 1;                           /* the woken receiver consumes it now */
        loc[v] = LOC_DONE;
    } else {
        loc[v] = dest;                        /* BUFFER or parked SENDER: ref held */
    }
}

/* runloom_chan_recv: consume one deliverable value -- the receiver takes the ref
 * (transfer) and drops it. */
static void chan_recv(void)
{
    for (int v = 0; v < NV; v++) {
        if ((loc[v] == LOC_BUFFER || loc[v] == LOC_SENDER) && nondet_bool()) {
            rc[v]  -= 1;                       /* receiver consumes -> Py_DECREF */
            loc[v]  = LOC_DONE;
            return;
        }
    }
}

/* runloom_chan_close: mark closed; drop every parked sender's held value. */
static void chan_close(void)
{
    if (closed) return;
    closed = 1;
    for (int v = 0; v < NV; v++) {
        if (loc[v] == LOC_SENDER) {
#ifndef BUG_CLOSE_NO_SENDER_DROP
            rc[v] -= 1;                        /* Py_DECREF(value); send_result = -1 */
#endif
            loc[v] = LOC_DONE;
        }
#ifdef BUG_DOUBLE_CONSUME
        else if (loc[v] == LOC_DONE && nondet_bool())
            rc[v] -= 1;                        /* BUG: drop an already-consumed value */
#endif
    }
}

/* runloom_chan_decref final free: drain the buffer (Py_XDECREF each), then free. */
static void chan_free(void)
{
    if (freed) return;
    for (int v = 0; v < NV; v++) {
        if (loc[v] == LOC_BUFFER) {
#ifndef BUG_FREE_NO_BUFFER_DRAIN
            rc[v] -= 1;                        /* Py_XDECREF on free */
#endif
            loc[v] = LOC_DONE;
        }
    }
    freed = 1;
}

#define NOPS 5

int main(void)
{
    for (int v = 0; v < NV; v++) { rc[v] = 0; loc[v] = LOC_NONE; }
    closed = 0; freed = 0;

    for (int s = 0; s < NOPS; s++) {
        int op = nondet_int();
        __CPROVER_assume(op >= 0 && op <= 2);
        if (op == 0) { int v = nondet_int(); __CPROVER_assume(v >= 0 && v < NV); chan_send(v); }
        else if (op == 1) chan_recv();
        else chan_close();

        /* refs never go negative at any point (no over-free / double-drop). */
        for (int v = 0; v < NV; v++)
            __CPROVER_assert(rc[v] >= 0, "value machinery refcount never negative (no over-free)");
    }

    /* teardown: close (if not already) then the final decref drains + frees. */
    chan_close();
    chan_free();

    /* CONSERVATION: every value's machinery ref is balanced -- acquired once,
     * released once -- so nothing leaked and nothing was over-freed. */
    for (int v = 0; v < NV; v++) {
        __CPROVER_assert(rc[v] == 0, "every sent value is released exactly once (no leak/over-free)");
        __CPROVER_assert(loc[v] == LOC_NONE || loc[v] == LOC_DONE,
                         "every value reached a terminal (consumed or dropped)");
    }
    return 0;
}
