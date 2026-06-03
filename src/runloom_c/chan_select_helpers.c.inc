/* Part of chan.c -- #included into that translation unit, not compiled
 * standalone.  See chan.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- select() ---------------------------------------------------- */

/* Phase 1: caller holds NO locks.  Iterate cases in caller-given
 * order.  For each, lock the channel, check if the op is immediately
 * fireable.  If yes, perform the op under lock (like send/recv
 * would), unlock, return the case index.  Else unlock and continue.
 * Returns -1 if no case is immediately ready. */
static int select_try_each(runloom_select_case_t *cases, int n)
{
    int i;
    for (i = 0; i < n; i++) {
        runloom_select_case_t *c = &cases[i];
        runloom_chan_t *ch = c->ch;
        runloom_mutex_lock(&ch->lock);
        if (c->op == RUNLOOM_SELECT_SEND) {
            runloom_chan_waiter_t *rx;
            if (ch->closed) {
                runloom_mutex_unlock(&ch->lock);
                PyErr_SetString(PyExc_ValueError, "select send on closed channel");
                return -2;
            }
            rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
            if (rx != NULL) {
                Py_INCREF(c->send_value);
                rx->value = c->send_value;
                rx->ok = 1;
                wake_waiter(rx);
                runloom_mutex_unlock(&ch->lock);
                return i;
            }
            if (ch->cap > 0 && ch->len < ch->cap) {
                Py_INCREF(c->send_value);
                buf_push(ch, c->send_value);
                runloom_mutex_unlock(&ch->lock);
                return i;
            }
        } else {  /* RECV */
            runloom_chan_waiter_t *tx;
            if (ch->len > 0) {
                c->recv_value = buf_pop(ch);
                c->recv_ok = 1;
                tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
                if (tx != NULL) {
                    buf_push(ch, tx->value);
                    tx->value = NULL;
                    tx->send_result = 0;
                    wake_waiter(tx);
                }
                runloom_mutex_unlock(&ch->lock);
                return i;
            }
            tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
            if (tx != NULL) {
                c->recv_value = tx->value;
                tx->value = NULL;
                tx->send_result = 0;
                wake_waiter(tx);
                c->recv_ok = 1;
                runloom_mutex_unlock(&ch->lock);
                return i;
            }
            if (ch->closed) {
                Py_INCREF(Py_None);
                c->recv_value = Py_None;
                c->recv_ok = 0;
                runloom_mutex_unlock(&ch->lock);
                return i;
            }
        }
        runloom_mutex_unlock(&ch->lock);
    }
    return -1;
}

/* Walk all OTHER channels under their locks, splice out the stale
 * tombstone waiter we left behind during the park phase.  Without
 * this, the next operation on those channels would see a phantom
 * waiter, fail to claim it (CAS lose), pop the next -- so it's
 * eventually-cleaned, but a slow-trickle accumulation of stale
 * pointers into freed stack frames is asking for use-after-free.
 * Better to evict eagerly. */
static void select_evict_self(runloom_select_case_t *cases, int n,
                              runloom_chan_waiter_t *waiters,
                              int fired)
{
    int i;
    for (i = 0; i < n; i++) {
        runloom_chan_t *ch;
        runloom_chan_waiter_t *target;
        runloom_chan_waiter_t **head;
        runloom_chan_waiter_t **tail;
        runloom_chan_waiter_t **pp;

        if (i == fired) continue;            /* the firing channel already popped us */
        ch = cases[i].ch;
        target = &waiters[i];
        head = (cases[i].op == RUNLOOM_SELECT_SEND) ? &ch->senders : &ch->receivers;
        tail = (cases[i].op == RUNLOOM_SELECT_SEND) ? &ch->senders_tail : &ch->receivers_tail;

        runloom_mutex_lock(&ch->lock);
        pp = head;
        while (*pp != NULL) {
            if (*pp == target) {
                *pp = target->next;
                if (*pp == NULL) *tail = target;  /* fix tail if we removed the last */
                if (*pp == NULL) *tail = NULL;
                target->next = NULL;
                break;
            }
            pp = &(*pp)->next;
        }
        /* Tail fix-up: if we just removed the tail entry, walk to find
         * the new tail.  Cheap because queues are short. */
        if (*head != NULL) {
            runloom_chan_waiter_t *w = *head;
            while (w->next != NULL) w = w->next;
            *tail = w;
        } else {
            *tail = NULL;
        }
        runloom_mutex_unlock(&ch->lock);
    }
}
