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
 * (pygo_sched_park_current / pygo_sched_wake on global; mn variants
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
#include "chan.h"
#include "coro.h"
#include "pygo_sched.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "pygo_diag.h"
#include "pygo_gstate.h"

#include <stdlib.h>
#include <string.h>

/* ---- waiter records ---------------------------------------------- */
struct pygo_select_park;          /* forward decl */

typedef struct pygo_chan_waiter {
    pygo_g_t *g;
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
     * from their queues by the woken g (see pygo_chan_select). */
    struct pygo_select_park *select;
    int case_index;                  /* index into the select's cases[] */
    struct pygo_chan_waiter *next;
} pygo_chan_waiter_t;

struct pygo_chan {
    pygo_mutex_t lock;
    PyObject **buf;
    Py_ssize_t cap;
    Py_ssize_t head;                 /* next slot to pop from */
    Py_ssize_t tail;                 /* next slot to push to */
    Py_ssize_t len;                  /* number of values currently buffered */
    pygo_chan_waiter_t *senders;     /* FIFO queue of parked senders */
    pygo_chan_waiter_t *senders_tail;
    pygo_chan_waiter_t *receivers;
    pygo_chan_waiter_t *receivers_tail;
    int closed;
    int refcount;
};

static void waiter_push(pygo_chan_waiter_t **head,
                        pygo_chan_waiter_t **tail,
                        pygo_chan_waiter_t *w)
{
    w->next = NULL;
    if (*tail == NULL) {
        *head = *tail = w;
    } else {
        (*tail)->next = w;
        *tail = w;
    }
}

static pygo_chan_waiter_t *waiter_pop(pygo_chan_waiter_t **head,
                                      pygo_chan_waiter_t **tail)
{
    pygo_chan_waiter_t *w = *head;
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
struct pygo_select_park {
    volatile int fired_case;     /* -1 until first CAS wins */
    int n_cases;
    PyObject *fired_value;       /* RECV: new ref filled in by firing channel
                                  * SEND: NULL */
    int fired_ok;
    /* Each entry's waiter + bookkeeping; sized [n_cases] -- C99 VLA */
    pygo_chan_t *channels[1];    /* variable length tail */
};

static int waiter_claim(pygo_chan_waiter_t *w)
{
    int expected;
    if (w->select == NULL) return 1;     /* not a select waiter */
    expected = -1;
    return __atomic_compare_exchange_n(&w->select->fired_case, &expected,
                                       w->case_index, 0,
                                       __ATOMIC_ACQ_REL,
                                       __ATOMIC_ACQUIRE);
}

/* Pop the first claimable waiter from the queue.  Stale select
 * tombstones (already claimed by a different channel) are popped and
 * discarded.  Returns NULL when queue exhausted. */
static pygo_chan_waiter_t *waiter_pop_claimable(pygo_chan_waiter_t **head,
                                                pygo_chan_waiter_t **tail)
{
    while (1) {
        pygo_chan_waiter_t *w = waiter_pop(head, tail);
        if (w == NULL) return NULL;
        if (waiter_claim(w)) return w;
        /* Tombstone -- skip.  No wake; the winning channel already
         * woke the g.  Memory ownership: select-waiter storage lives
         * on the goroutine's stack frame, so we don't free it here. */
    }
}

/* ---- lifecycle --------------------------------------------------- */
pygo_chan_t *pygo_chan_new(Py_ssize_t cap)
{
    pygo_chan_t *ch;
    if (cap < 0) {
        PyErr_SetString(PyExc_ValueError, "channel capacity must be >= 0");
        return NULL;
    }
    ch = (pygo_chan_t *)PyMem_Calloc(1, sizeof(*ch));
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
    pygo_mutex_init(&ch->lock);
    return ch;
}

void pygo_chan_incref(pygo_chan_t *ch)
{
    if (ch) __atomic_add_fetch(&ch->refcount, 1, __ATOMIC_RELAXED);
}

void pygo_chan_decref(pygo_chan_t *ch)
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
    pygo_mutex_destroy(&ch->lock);
    PyMem_Free(ch);
}

/* ---- helpers ----------------------------------------------------- */

/* Push to the ring buffer.  Caller must hold the lock and have
 * already verified ch->len < ch->cap.  Steals (transfers) the ref. */
static void buf_push(pygo_chan_t *ch, PyObject *value)
{
    ch->buf[ch->tail] = value;
    ch->tail = (ch->tail + 1) % ch->cap;
    ch->len++;
}

static PyObject *buf_pop(pygo_chan_t *ch)
{
    PyObject *v = ch->buf[ch->head];
    ch->buf[ch->head] = NULL;
    ch->head = (ch->head + 1) % ch->cap;
    ch->len--;
    return v;
}

/* Park the current goroutine in the given waiter queue.  Must be
 * called WITH the channel lock held; the lock is released BEFORE
 * yielding, then the goroutine yields via pygo_coro_yield.  Wake
 * happens via pygo_sched_wake / pygo_mn_wake_g from the producer/
 * consumer side.
 *
 * PyHandle pin: parked waiters hold an incref on the channel for
 * the duration of the park.  Without this, the last Python reference
 * to the channel can decref to 0 while we are yielded, freeing the
 * channel (including pygo_mutex_destroy on a lock we may yet release
 * from the wake path) and orphaning every waiter.  The decref after
 * wake balances the incref taken here. */
static void park_waiter(pygo_chan_t *ch,
                        pygo_chan_waiter_t **head,
                        pygo_chan_waiter_t **tail,
                        pygo_chan_waiter_t *w)
{
    void *hub = pygo_mn_current_hub_opaque();
    pygo_g_t *current_g;

    if (hub != NULL) {
        current_g = pygo_mn_tls_current_g();
    } else {
        current_g = pygo_sched_get()->current;
    }
    w->g = current_g;
    w->hub = hub;

    /* Pin the channel.  Acquired before unlock so a concurrent
     * decref-to-zero from another thread cannot race with our park. */
    pygo_chan_incref(ch);

    waiter_push(head, tail, w);

    /* Snapshot tstate + take g off any ready list.  Same dance as
     * pygo_netpoll_wait_fd uses. */
    if (current_g != NULL) {
        pygo_sched_park_current();
        pygo_g_state_set(current_g, PYGO_GST_PARKED_CHAN);
        PYGO_EVT(PYGO_EVT_CHAN_PARK, ch, current_g, 0);
    }

    /* Critical: release the channel lock BEFORE yielding.  Otherwise
     * the wake-side would deadlock trying to acquire it. */
    pygo_mutex_unlock(&ch->lock);

    pygo_coro_yield();
    /* On wake, the producer has already filled w->value / w->ok /
     * w->send_result before unparking us. */
    if (current_g != NULL) {
        pygo_g_state_set(current_g, PYGO_GST_RUNNING);
        PYGO_EVT(PYGO_EVT_CHAN_WAKE, ch, current_g, 0);
    }

    /* Drop the pin.  If we were the last reference (rare: caller code
     * usually still holds a Python wrapper that outlives the park),
     * the channel is freed here.  We only touch our own waiter struct
     * after this point, never ch. */
    pygo_chan_decref(ch);

    /* Re-acquire is NOT necessary on this side -- we read our own
     * waiter struct and return.  The caller does not need the lock. */
}

/* Wake a parked waiter.  Caller holds the lock; the wake routine
 * itself is lock-free (it pokes the scheduler's ready list, which
 * has its own internal locking under M:N). */
static void wake_waiter(pygo_chan_waiter_t *w)
{
    if (w->hub != NULL) {
        pygo_mn_wake_g(w->hub, w->g);
    } else {
        pygo_sched_wake(w->g);
    }
}

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

/* ---- select() ---------------------------------------------------- */

/* Phase 1: caller holds NO locks.  Iterate cases in caller-given
 * order.  For each, lock the channel, check if the op is immediately
 * fireable.  If yes, perform the op under lock (like send/recv
 * would), unlock, return the case index.  Else unlock and continue.
 * Returns -1 if no case is immediately ready. */
static int select_try_each(pygo_select_case_t *cases, int n)
{
    int i;
    for (i = 0; i < n; i++) {
        pygo_select_case_t *c = &cases[i];
        pygo_chan_t *ch = c->ch;
        pygo_mutex_lock(&ch->lock);
        if (c->op == PYGO_SELECT_SEND) {
            pygo_chan_waiter_t *rx;
            if (ch->closed) {
                pygo_mutex_unlock(&ch->lock);
                PyErr_SetString(PyExc_ValueError, "select send on closed channel");
                return -2;
            }
            rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
            if (rx != NULL) {
                Py_INCREF(c->send_value);
                rx->value = c->send_value;
                rx->ok = 1;
                wake_waiter(rx);
                pygo_mutex_unlock(&ch->lock);
                return i;
            }
            if (ch->cap > 0 && ch->len < ch->cap) {
                Py_INCREF(c->send_value);
                buf_push(ch, c->send_value);
                pygo_mutex_unlock(&ch->lock);
                return i;
            }
        } else {  /* RECV */
            pygo_chan_waiter_t *tx;
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
                pygo_mutex_unlock(&ch->lock);
                return i;
            }
            tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
            if (tx != NULL) {
                c->recv_value = tx->value;
                tx->value = NULL;
                tx->send_result = 0;
                wake_waiter(tx);
                c->recv_ok = 1;
                pygo_mutex_unlock(&ch->lock);
                return i;
            }
            if (ch->closed) {
                Py_INCREF(Py_None);
                c->recv_value = Py_None;
                c->recv_ok = 0;
                pygo_mutex_unlock(&ch->lock);
                return i;
            }
        }
        pygo_mutex_unlock(&ch->lock);
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
static void select_evict_self(pygo_select_case_t *cases, int n,
                              pygo_chan_waiter_t *waiters,
                              int fired)
{
    int i;
    for (i = 0; i < n; i++) {
        pygo_chan_t *ch;
        pygo_chan_waiter_t *target;
        pygo_chan_waiter_t **head;
        pygo_chan_waiter_t **tail;
        pygo_chan_waiter_t **pp;

        if (i == fired) continue;            /* the firing channel already popped us */
        ch = cases[i].ch;
        target = &waiters[i];
        head = (cases[i].op == PYGO_SELECT_SEND) ? &ch->senders : &ch->receivers;
        tail = (cases[i].op == PYGO_SELECT_SEND) ? &ch->senders_tail : &ch->receivers_tail;

        pygo_mutex_lock(&ch->lock);
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
            pygo_chan_waiter_t *w = *head;
            while (w->next != NULL) w = w->next;
            *tail = w;
        } else {
            *tail = NULL;
        }
        pygo_mutex_unlock(&ch->lock);
    }
}

int pygo_chan_select(pygo_select_case_t *cases, int n, int default_ready)
{
    int rc;

    if (n <= 0) {
        PyErr_SetString(PyExc_ValueError, "select needs at least 1 case");
        return -2;
    }

    /* Phase 1: try each case (no parking).  This handles the
     * common case (some channel is ready) and matches Go's select-
     * scan-then-park semantics. */
    rc = select_try_each(cases, n);
    if (rc != -1) return rc;          /* fired, errored, or PyErr set */

    if (default_ready) return -1;     /* Go's default: branch */

    /* Phase 2: park on every channel.  Waiter records are heap-
     * allocated; their lifetime is bounded by this function call.
     * We could stack-allocate with a cap fallback, but most selects
     * have small N and the malloc cost is dwarfed by the asm
     * context switch.  Important: do NOT return without freeing
     * `waiters`. */
    {
        struct pygo_select_park park;
        pygo_chan_waiter_t *waiters;
        int i, fired;
        int select_rc;

        waiters = (pygo_chan_waiter_t *)PyMem_Calloc(
            (size_t)n, sizeof(pygo_chan_waiter_t));
        if (waiters == NULL) {
            PyErr_NoMemory();
            return -2;
        }
        park.fired_case = -1;
        park.n_cases = n;
        park.fired_value = NULL;
        park.fired_ok = 0;

        /* PyHandle pin: every channel in cases[] must stay alive across
         * the park + eviction phase.  Without this, a concurrent decref
         * of the last Python reference would free the channel mid-park,
         * and the eviction walk after wake would dereference freed
         * memory.  Balance with a decref-loop before returning. */
        for (i = 0; i < n; i++) {
            if (cases[i].ch != NULL) pygo_chan_incref(cases[i].ch);
        }
        #define PYGO_SELECT_UNPIN()                                          \
            do {                                                             \
                int _i;                                                      \
                for (_i = 0; _i < n; _i++) {                                 \
                    if (cases[_i].ch != NULL) pygo_chan_decref(cases[_i].ch);\
                }                                                            \
            } while (0)

        /* Determine our hub + g once for all entries. */
        {
            void *hub = pygo_mn_current_hub_opaque();
            pygo_g_t *cur_g = (hub != NULL) ?
                pygo_mn_tls_current_g() : pygo_sched_get()->current;
            for (i = 0; i < n; i++) {
                waiters[i].g = cur_g;
                waiters[i].hub = hub;
                waiters[i].select = &park;
                waiters[i].case_index = i;
                if (cases[i].op == PYGO_SELECT_SEND) {
                    Py_INCREF(cases[i].send_value);
                    waiters[i].value = cases[i].send_value;
                    waiters[i].send_result = -1;
                } else {
                    waiters[i].ok = -1;
                }
            }
        }

        /* Push each waiter under that channel's lock.  Order doesn't
         * matter for correctness because the claim-CAS is global to
         * the park, not the channel locks. */
        for (i = 0; i < n; i++) {
            pygo_chan_t *ch = cases[i].ch;
            pygo_mutex_lock(&ch->lock);
            /* It's possible the channel went ready/closed since
             * phase-1.  Re-check before parking. */
            if (cases[i].op == PYGO_SELECT_SEND) {
                if (ch->closed || (ch->cap > 0 && ch->len < ch->cap) ||
                    ch->receivers != NULL) {
                    pygo_mutex_unlock(&ch->lock);
                    /* Race: someone freed space / opened a slot.
                     * Tear down what we've installed and retry try-each. */
                    /* Mark park as fired-by-us-on-this-case so other
                     * channels skip our tombstones. */
                    {
                        int expected = -1;
                        __atomic_compare_exchange_n(&park.fired_case,
                                                    &expected, i, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE);
                    }
                    /* Already-installed waiters are tombstones; the
                     * channels they're on will skip them.  We need
                     * to evict them so freed stack memory isn't
                     * dangling. */
                    select_evict_self(cases, n, waiters, /*fired*/-1);
                    /* Drop SEND refs we incref'd. */
                    {
                        int j;
                        for (j = 0; j < n; j++) {
                            if (cases[j].op == PYGO_SELECT_SEND && waiters[j].value != NULL) {
                                Py_DECREF(waiters[j].value);
                            }
                        }
                    }
                    select_rc = select_try_each(cases, n);
                    PyMem_Free(waiters);
                    PYGO_SELECT_UNPIN();
                    return select_rc;
                }
                waiter_push(&ch->senders, &ch->senders_tail, &waiters[i]);
                pygo_mutex_unlock(&ch->lock);
                continue;
            }
            if (cases[i].op == PYGO_SELECT_RECV) {
                if (ch->closed || ch->len > 0 || ch->senders != NULL) {
                    pygo_mutex_unlock(&ch->lock);
                    {
                        int expected = -1;
                        __atomic_compare_exchange_n(&park.fired_case,
                                                    &expected, i, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE);
                    }
                    select_evict_self(cases, n, waiters, /*fired*/-1);
                    {
                        int j;
                        for (j = 0; j < n; j++) {
                            if (cases[j].op == PYGO_SELECT_SEND && waiters[j].value != NULL) {
                                Py_DECREF(waiters[j].value);
                            }
                        }
                    }
                    select_rc = select_try_each(cases, n);
                    PyMem_Free(waiters);
                    PYGO_SELECT_UNPIN();
                    return select_rc;
                }
                waiter_push(&ch->receivers, &ch->receivers_tail, &waiters[i]);
            }
            pygo_mutex_unlock(&ch->lock);
        }

        /* All installed.  Park self (snap tstate) and yield. */
        if (pygo_mn_current_hub_opaque() != NULL) {
            pygo_sched_park_current();
        } else if (pygo_sched_get()->current != NULL) {
            pygo_sched_park_current();
        }
        pygo_coro_yield();

        /* Woken.  park.fired_case is the winning index. */
        fired = park.fired_case;
        if (fired < 0) {
            /* Shouldn't happen -- defensive. */
            PyMem_Free(waiters);
            PYGO_SELECT_UNPIN();
            return -2;
        }

        /* Evict our tombstone waiters from all losing channels. */
        select_evict_self(cases, n, waiters, fired);

        /* Drop the value refs from non-firing SEND cases. */
        {
            int j;
            for (j = 0; j < n; j++) {
                if (j == fired) continue;
                if (cases[j].op == PYGO_SELECT_SEND && waiters[j].value != NULL) {
                    Py_DECREF(waiters[j].value);
                }
            }
        }

        /* If the fired case is a SEND, the channel took our value via
         * waiters[fired].send_result.  Nothing to do on our side
         * other than check for closed-while-parked. */
        if (cases[fired].op == PYGO_SELECT_SEND) {
            if (waiters[fired].send_result != 0) {
                Py_DECREF(waiters[fired].value);  /* still ours */
                PyMem_Free(waiters);
                PYGO_SELECT_UNPIN();
                PyErr_SetString(PyExc_ValueError, "select send on closed channel");
                return -2;
            }
            /* Delivered.  The channel/receiver took our ref. */
            PyMem_Free(waiters);
            PYGO_SELECT_UNPIN();
            return fired;
        }
        /* Fired RECV: waiters[fired].value + .ok hold the result. */
        cases[fired].recv_value = waiters[fired].value;
        cases[fired].recv_ok = (waiters[fired].ok == 1);
        PyMem_Free(waiters);
        PYGO_SELECT_UNPIN();
        return fired;
        #undef PYGO_SELECT_UNPIN
    }
}
