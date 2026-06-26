/* Part of chan.c -- #included into that translation unit, not compiled
 * standalone.  See chan.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- send -------------------------------------------------------- */

/* Internal: caller holds the lock.  Common code for blocking + try.
 *   blocking != 0  : park on full; return 0 only after delivery
 *   blocking == 0  : on full, release lock + return 1 (would-block)
 *
 *   on success: returns 0, channel holds an INCREF'd ref to value.
 *   on closed: returns -1 + PyErr set + LOCK RELEASED.
 */
/* True iff there is NO runloom goroutine on this OS thread (a foreign thread, or a
 * top-level call outside run()).  Such a caller MUST NOT reach park_waiter: it has
 * no fiber to park, so park_waiter would busy-spin forever (its coro_yield is a
 * no-op and wake_waiter can't wake a NULL g) -- a CPU-pegging hang.  Detected with
 * a NON-allocating peek: never runloom_sched_get(), which would malloc a sched +
 * arm the wake-pump on a foreign thread (the foreign-thread-safety invariant in
 * CLAUDE.md).  Cooperative channel ops are goroutine-only by design, matching
 * runloom.sync's "current_g() is None -> RuntimeError" guard. */
static int chan_no_goroutine(void)
{
    if (runloom_mn_current_hub_opaque() != NULL)
        return runloom_mn_tls_current_g() == NULL;   /* on a hub: in a fiber? */
    return runloom_sched_peek_current() == NULL;       /* single-thread: a current g? */
}

static int chan_send_locked(runloom_chan_t *ch, PyObject *value, int blocking)
{
    runloom_chan_waiter_t *rx;

    if (ch->closed) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        PyErr_SetString(PyExc_ValueError, "send on closed channel");
        return -1;
    }

    /* Receivers waiting? -- direct handoff. */
    rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
    if (rx != NULL) {
        Py_INCREF(value);
        rx->value = value;
        rx->ok = 1;
        wake_waiter(rx);
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        return 0;
    }

    /* Buffered + room? */
    if (ch->cap > 0 && ch->len < ch->cap) {
        Py_INCREF(value);
        buf_push(ch, value);
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        return 0;
    }

    /* Full / unbuffered + no receivers. */
    if (!blocking) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        return 1;
    }

    /* Would block, but no goroutine to park -> reject cleanly instead of letting
     * park_waiter busy-spin forever (audit hang: park_waiter from a non-fiber). */
    if (chan_no_goroutine()) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        PyErr_SetString(PyExc_RuntimeError,
                        "channel send would block, but no goroutine is running "
                        "(blocking channel ops require a runloom goroutine context)");
        return -1;
    }

    /* Block: park as sender.  We hold our own ref on `value` until
     * the receiver picks it up (or close wakes us with an error). */
    {
        runloom_chan_waiter_t w;
        memset(&w, 0, sizeof(w));
        Py_INCREF(value);
        w.value = value;
        w.send_result = -1;          /* set by receiver/close */
        park_waiter(ch, &ch->senders, &ch->senders_tail, &w);
        /* Lock is released.  On wake, w.send_result is set. */
        if (w.send_result == 0) {
            /* Delivered -- receiver / buffer took our ref. */
            return 0;
        }
        /* Closed-while-parked.  Drop our ref. */
        Py_DECREF(value);
        PyErr_SetString(PyExc_ValueError, "send on closed channel");
        return -1;
    }
}

int runloom_chan_send(runloom_chan_t *ch, PyObject *value)
{
    if (runloom_mn_seg_track) runloom_mn_seg_touch(ch);   /* DPOR conflict object */
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return chan_send_locked(ch, value, 1);
}

int runloom_chan_try_send(runloom_chan_t *ch, PyObject *value)
{
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return chan_send_locked(ch, value, 0);
}

/* ---- recv -------------------------------------------------------- */

/* Internal: caller holds the lock.
 *   blocking != 0: park on empty until a sender appears
 *   blocking == 0: on empty, release lock and return *ok=-1
 *
 *   On success: returns the value (new ref), *ok = 1
 *   On closed-and-empty: returns Py_None (new ref), *ok = 0
 *   On would-block: returns NULL (no ref), *ok = -1
 *   On error: returns NULL (no ref), *ok = -1, PyErr set
 */
static PyObject *chan_recv_locked(runloom_chan_t *ch, int *ok, int blocking)
{
    runloom_chan_waiter_t *tx;
    PyObject *result;

    /* Buffered values take priority over closed status: pending sends
     * still in the buffer drain normally even after close. */
    if (ch->len > 0) {
        result = buf_pop(ch);
        /* If a sender is parked (buffer was full), pull its value
         * into the freed slot and wake it. */
        tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
        if (tx != NULL) {
            buf_push(ch, tx->value);    /* transfers tx's ref */
            tx->value = NULL;
            tx->send_result = 0;
            wake_waiter(tx);
        }
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        *ok = 1;
        return result;
    }

    /* Buffer empty.  Sender waiting? -> unbuffered handoff. */
    tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
    if (tx != NULL) {
        result = tx->value;          /* steals tx's ref */
        tx->value = NULL;
        tx->send_result = 0;
        wake_waiter(tx);
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        *ok = 1;
        return result;
    }

    /* Empty + no senders. */
    if (ch->closed) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        Py_INCREF(Py_None);
        *ok = 0;
        return Py_None;
    }

    if (!blocking) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        *ok = -1;
        return NULL;
    }

    /* Would block, but no goroutine to park -> reject cleanly instead of letting
     * park_waiter busy-spin forever (audit hang: park_waiter from a non-fiber). */
    if (chan_no_goroutine()) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        PyErr_SetString(PyExc_RuntimeError,
                        "channel recv would block, but no goroutine is running "
                        "(blocking channel ops require a runloom goroutine context)");
        *ok = -1;
        return NULL;
    }

    /* Block: park as receiver. */
    {
        runloom_chan_waiter_t w;
        memset(&w, 0, sizeof(w));
        w.ok = -1;
        park_waiter(ch, &ch->receivers, &ch->receivers_tail, &w);
        /* Lock released.  On wake, w.value + w.ok are set. */
        if (w.ok == 1) {
            *ok = 1;
            return w.value;          /* new ref handed off from sender */
        }
        /* Closed-and-empty wake. */
        Py_INCREF(Py_None);
        *ok = 0;
        return Py_None;
    }
}

PyObject *runloom_chan_recv(runloom_chan_t *ch, int *ok)
{
    if (runloom_mn_seg_track) runloom_mn_seg_touch(ch);   /* DPOR conflict object */
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return chan_recv_locked(ch, ok, 1);
}

int runloom_chan_try_recv(runloom_chan_t *ch, PyObject **out, int *ok)
{
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    *out = chan_recv_locked(ch, ok, 0);
    if (*out == NULL && PyErr_Occurred()) return -1;
    return 0;
}

/* ---- close ------------------------------------------------------- */
int runloom_chan_close(runloom_chan_t *ch)
{
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    if (ch->closed) {
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        PyErr_SetString(PyExc_ValueError, "close on closed channel");
        return -1;
    }
    ch->closed = 1;

    /* Wake every parked sender with "closed" error.  Their value ref
     * stays with them; they DECREF on the error path. */
    while (1) {
        runloom_chan_waiter_t *tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
        if (tx == NULL) break;
        tx->send_result = -1;
        wake_waiter(tx);
    }
    /* Wake every parked receiver with ok=0 (channel-closed). */
    while (1) {
        runloom_chan_waiter_t *rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
        if (rx == NULL) break;
        rx->value = NULL;
        rx->ok = 0;
        wake_waiter(rx);
    }
    RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return 0;
}

/* ---- introspection ---------------------------------------------- */
int runloom_chan_is_closed(runloom_chan_t *ch)
{
    int c;
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    c = ch->closed;
    RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return c;
}

Py_ssize_t runloom_chan_len(runloom_chan_t *ch)
{
    Py_ssize_t n;
    RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    n = ch->len;
    RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    return n;
}

Py_ssize_t runloom_chan_cap(runloom_chan_t *ch)
{
    return ch->cap;
}
