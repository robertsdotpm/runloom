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
static int chan_send_locked(pygo_chan_t *ch, PyObject *value, int blocking)
{
    pygo_chan_waiter_t *rx;

    if (ch->closed) {
        pygo_mutex_unlock(&ch->lock);
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
        pygo_mutex_unlock(&ch->lock);
        return 0;
    }

    /* Buffered + room? */
    if (ch->cap > 0 && ch->len < ch->cap) {
        Py_INCREF(value);
        buf_push(ch, value);
        pygo_mutex_unlock(&ch->lock);
        return 0;
    }

    /* Full / unbuffered + no receivers. */
    if (!blocking) {
        pygo_mutex_unlock(&ch->lock);
        return 1;
    }

    /* Block: park as sender.  We hold our own ref on `value` until
     * the receiver picks it up (or close wakes us with an error). */
    {
        pygo_chan_waiter_t w;
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

int pygo_chan_send(pygo_chan_t *ch, PyObject *value)
{
    pygo_mutex_lock(&ch->lock);
    return chan_send_locked(ch, value, 1);
}

int pygo_chan_try_send(pygo_chan_t *ch, PyObject *value)
{
    pygo_mutex_lock(&ch->lock);
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
static PyObject *chan_recv_locked(pygo_chan_t *ch, int *ok, int blocking)
{
    pygo_chan_waiter_t *tx;
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
        pygo_mutex_unlock(&ch->lock);
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
        pygo_mutex_unlock(&ch->lock);
        *ok = 1;
        return result;
    }

    /* Empty + no senders. */
    if (ch->closed) {
        pygo_mutex_unlock(&ch->lock);
        Py_INCREF(Py_None);
        *ok = 0;
        return Py_None;
    }

    if (!blocking) {
        pygo_mutex_unlock(&ch->lock);
        *ok = -1;
        return NULL;
    }

    /* Block: park as receiver. */
    {
        pygo_chan_waiter_t w;
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

PyObject *pygo_chan_recv(pygo_chan_t *ch, int *ok)
{
    pygo_mutex_lock(&ch->lock);
    return chan_recv_locked(ch, ok, 1);
}

int pygo_chan_try_recv(pygo_chan_t *ch, PyObject **out, int *ok)
{
    pygo_mutex_lock(&ch->lock);
    *out = chan_recv_locked(ch, ok, 0);
    if (*out == NULL && PyErr_Occurred()) return -1;
    return 0;
}

/* ---- close ------------------------------------------------------- */
int pygo_chan_close(pygo_chan_t *ch)
{
    pygo_mutex_lock(&ch->lock);
    if (ch->closed) {
        pygo_mutex_unlock(&ch->lock);
        PyErr_SetString(PyExc_ValueError, "close on closed channel");
        return -1;
    }
    ch->closed = 1;

    /* Wake every parked sender with "closed" error.  Their value ref
     * stays with them; they DECREF on the error path. */
    while (1) {
        pygo_chan_waiter_t *tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
        if (tx == NULL) break;
        tx->send_result = -1;
        wake_waiter(tx);
    }
    /* Wake every parked receiver with ok=0 (channel-closed). */
    while (1) {
        pygo_chan_waiter_t *rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
        if (rx == NULL) break;
        rx->value = NULL;
        rx->ok = 0;
        wake_waiter(rx);
    }
    pygo_mutex_unlock(&ch->lock);
    return 0;
}

/* ---- introspection ---------------------------------------------- */
int pygo_chan_is_closed(pygo_chan_t *ch)
{
    int c;
    pygo_mutex_lock(&ch->lock);
    c = ch->closed;
    pygo_mutex_unlock(&ch->lock);
    return c;
}

Py_ssize_t pygo_chan_len(pygo_chan_t *ch)
{
    Py_ssize_t n;
    pygo_mutex_lock(&ch->lock);
    n = ch->len;
    pygo_mutex_unlock(&ch->lock);
    return n;
}

Py_ssize_t pygo_chan_cap(pygo_chan_t *ch)
{
    return ch->cap;
}
