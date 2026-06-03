/* Part of chan.c -- #included into that translation unit, not compiled
 * standalone.  See chan.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static void waiter_push(runloom_chan_waiter_t **head,
                        runloom_chan_waiter_t **tail,
                        runloom_chan_waiter_t *w)
{
    w->next = NULL;
    if (*tail == NULL) {
        *head = *tail = w;
    } else {
        (*tail)->next = w;
        *tail = w;
    }
}

static runloom_chan_waiter_t *waiter_pop(runloom_chan_waiter_t **head,
                                      runloom_chan_waiter_t **tail)
{
    runloom_chan_waiter_t *w = *head;
    if (w == NULL) return NULL;
    *head = w->next;
    if (*head == NULL) *tail = NULL;
    w->next = NULL;
    return w;
}

/* For select waiters: try to claim this waiter (CAS fired_case from
 * -1 to our case index).  Returns 1 on win, 0 on lose (someone else
 * already claimed -- skip this stale tombstone).  Non-select waiters
 * always "win".  Forward decl of the select park struct. */
struct runloom_select_park {
    volatile int fired_case;     /* -1 until first CAS wins */
    int n_cases;
    PyObject *fired_value;       /* RECV: new ref filled in by firing channel
                                  * SEND: NULL */
    int fired_ok;
    /* Each entry's waiter + bookkeeping; sized [n_cases] -- C99 VLA */
    runloom_chan_t *channels[1];    /* variable length tail */
};

static int waiter_claim(runloom_chan_waiter_t *w)
{
    int expected;
    if (w->select == NULL) return 1;     /* not a select waiter */
    expected = -1;
    return __atomic_compare_exchange_n(&w->select->fired_case, &expected,
                                       w->case_index, 0,
                                       __ATOMIC_ACQ_REL,
                                       __ATOMIC_ACQUIRE);
}

/* Lock held.  Does this waiter queue hold any waiter that does NOT belong to
 * `park`?  A select installs its waiters case-by-case and then re-checks each
 * case for a ready counterparty; counting its OWN already-installed waiter (a
 * self-select: send AND recv on one channel) as that counterparty makes the
 * install abort and retry -- forever under serial execution, where no other
 * goroutine can interleave to consume it (the GIL-on / controlled-scheduler
 * self-select livelock).  Only a FOREIGN waiter -- a plain channel op, or a
 * different select -- is a real rendezvous, so the readiness re-check uses this
 * instead of a bare non-NULL test.  Go avoids the issue structurally by checking
 * all cases ready BEFORE enqueuing any sudog; this is the same guarantee kept
 * within runloom's incremental-install scheme. */
static int waiter_has_foreign(runloom_chan_waiter_t *head,
                              struct runloom_select_park *park)
{
    runloom_chan_waiter_t *w;
    for (w = head; w != NULL; w = w->next)
        if (w->select != park) return 1;   /* select==NULL (plain) or other park */
    return 0;
}

/* Pop the first claimable waiter from the queue.  Stale select
 * tombstones (already claimed by a different channel) are popped and
 * discarded.  Returns NULL when queue exhausted. */
static runloom_chan_waiter_t *waiter_pop_claimable(runloom_chan_waiter_t **head,
                                                runloom_chan_waiter_t **tail)
{
    while (1) {
        runloom_chan_waiter_t *w = waiter_pop(head, tail);
        if (w == NULL) return NULL;
        if (waiter_claim(w)) return w;
        /* Tombstone -- skip.  No wake; the winning channel already
         * woke the g.  Memory ownership: select-waiter storage lives
         * on the goroutine's stack frame, so we don't free it here. */
    }
}

/* ---- lifecycle --------------------------------------------------- */
runloom_chan_t *runloom_chan_new(Py_ssize_t cap)
{
    runloom_chan_t *ch;
    if (cap < 0) {
        PyErr_SetString(PyExc_ValueError, "channel capacity must be >= 0");
        return NULL;
    }
    ch = (runloom_chan_t *)PyMem_Calloc(1, sizeof(*ch));
    if (ch == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    if (cap > 0) {
        ch->buf = (PyObject **)PyMem_Calloc((size_t)cap, sizeof(PyObject *));
        if (ch->buf == NULL) {
            PyMem_Free(ch);
            PyErr_NoMemory();
            return NULL;
        }
    }
    ch->cap = cap;
    ch->refcount = 1;
    runloom_mutex_init(&ch->lock);
    return ch;
}

void runloom_chan_incref(runloom_chan_t *ch)
{
    if (ch) __atomic_add_fetch(&ch->refcount, 1, __ATOMIC_RELAXED);
}

void runloom_chan_decref(runloom_chan_t *ch)
{
    if (ch == NULL) return;
    if (__atomic_sub_fetch(&ch->refcount, 1, __ATOMIC_ACQ_REL) > 0) return;
    /* Last ref -- free.  Anything still in the buffer needs its
     * reference dropped.  Parked waiters should have been woken by
     * close(), but if not -- they hold their own refs which are
     * dropped on their stack frames. */
    if (ch->buf != NULL) {
        Py_ssize_t i, n = ch->len;
        for (i = 0; i < n; i++) {
            PyObject *v = ch->buf[(ch->head + i) % ch->cap];
            Py_XDECREF(v);
        }
        PyMem_Free(ch->buf);
    }
    runloom_mutex_destroy(&ch->lock);
    PyMem_Free(ch);
}

/* ---- helpers ----------------------------------------------------- */

/* Push to the ring buffer.  Caller must hold the lock and have
 * already verified ch->len < ch->cap.  Steals (transfers) the ref. */
static void buf_push(runloom_chan_t *ch, PyObject *value)
{
    ch->buf[ch->tail] = value;
    ch->tail = (ch->tail + 1) % ch->cap;
    ch->len++;
}

static PyObject *buf_pop(runloom_chan_t *ch)
{
    PyObject *v = ch->buf[ch->head];
    ch->buf[ch->head] = NULL;
    ch->head = (ch->head + 1) % ch->cap;
    ch->len--;
    return v;
}

/* Park the current goroutine in the given waiter queue.  Must be
 * called WITH the channel lock held; the lock is released BEFORE
 * yielding, then the goroutine yields via runloom_coro_yield.  Wake
 * happens via runloom_sched_wake / runloom_mn_wake_g from the producer/
 * consumer side.
 *
 * PyHandle pin: parked waiters hold an incref on the channel for
 * the duration of the park.  Without this, the last Python reference
 * to the channel can decref to 0 while we are yielded, freeing the
 * channel (including runloom_mutex_destroy on a lock we may yet release
 * from the wake path) and orphaning every waiter.  The decref after
 * wake balances the incref taken here. */
static void park_waiter(runloom_chan_t *ch,
                        runloom_chan_waiter_t **head,
                        runloom_chan_waiter_t **tail,
                        runloom_chan_waiter_t *w)
{
    void *hub = runloom_mn_current_hub_opaque();
    runloom_g_t *current_g;

    if (hub != NULL) {
        current_g = runloom_mn_tls_current_g();
    } else {
        current_g = runloom_sched_get()->current;
    }
    w->g = current_g;
    w->hub = hub;

    /* Pin the channel.  Acquired before unlock so a concurrent
     * decref-to-zero from another thread cannot race with our park. */
    runloom_chan_incref(ch);

    waiter_push(head, tail, w);

    /* Snapshot tstate + take g off any ready list.  Same dance as
     * runloom_netpoll_wait_fd uses. */
    if (current_g != NULL) {
        runloom_sched_park_current();
        runloom_g_state_set(current_g, RUNLOOM_GST_PARKED_CHAN);
        RUNLOOM_EVT(RUNLOOM_EVT_CHAN_PARK, ch, current_g, 0);
    }

    /* Critical: release the channel lock BEFORE yielding.  Otherwise
     * the wake-side would deadlock trying to acquire it. */
    runloom_mutex_unlock(&ch->lock);

    runloom_coro_yield();
    /* On wake, the producer has already filled w->value / w->ok /
     * w->send_result before unparking us. */
    if (current_g != NULL) {
        runloom_g_state_set(current_g, RUNLOOM_GST_RUNNING);
        RUNLOOM_EVT(RUNLOOM_EVT_CHAN_WAKE, ch, current_g, 0);
    }

    /* Drop the pin.  If we were the last reference (rare: caller code
     * usually still holds a Python wrapper that outlives the park),
     * the channel is freed here.  We only touch our own waiter struct
     * after this point, never ch. */
    runloom_chan_decref(ch);

    /* Re-acquire is NOT necessary on this side -- we read our own
     * waiter struct and return.  The caller does not need the lock. */
}

/* Wake a parked waiter.  Caller holds the lock; the wake routine
 * itself is lock-free (it pokes the scheduler's ready list, which
 * has its own internal locking under M:N). */
static void wake_waiter(runloom_chan_waiter_t *w)
{
    if (w->hub != NULL) {
        runloom_mn_wake_g(w->hub, w->g);
    } else {
        runloom_sched_wake(w->g);
    }
}
