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

#include <stdlib.h>
#include <string.h>

/* ---- waiter records ---------------------------------------------- */
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
 * consumer side. */
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

    waiter_push(head, tail, w);

    /* Snapshot tstate + take g off any ready list.  Same dance as
     * pygo_netpoll_wait_fd uses. */
    if (current_g != NULL) {
        pygo_sched_park_current();
    }

    /* Critical: release the channel lock BEFORE yielding.  Otherwise
     * the wake-side would deadlock trying to acquire it. */
    pygo_mutex_unlock(&ch->lock);

    pygo_coro_yield();
    /* On wake, the producer has already filled w->value / w->ok /
     * w->send_result before unparking us. */

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
    rx = waiter_pop(&ch->receivers, &ch->receivers_tail);
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
        tx = waiter_pop(&ch->senders, &ch->senders_tail);
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
    tx = waiter_pop(&ch->senders, &ch->senders_tail);
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
        pygo_chan_waiter_t *tx = waiter_pop(&ch->senders, &ch->senders_tail);
        if (tx == NULL) break;
        tx->send_result = -1;
        wake_waiter(tx);
    }
    /* Wake every parked receiver with ok=0 (channel-closed). */
    while (1) {
        pygo_chan_waiter_t *rx = waiter_pop(&ch->receivers, &ch->receivers_tail);
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
