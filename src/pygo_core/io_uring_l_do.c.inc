/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static pygo_iouring_ssize_t pygo_iouring_do(struct io_uring_sqe sqe)
{
    pygo_iouring_op_t op;
    void *hub;

    if (!pygo_iouring_available()) {
        errno = ENOSYS;
        return -1;
    }

    hub = pygo_mn_current_hub_opaque();
    op.type   = PYGO_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = (hub != NULL) ? pygo_mn_tls_current_g()
                              : pygo_sched_get()->current;
    op.result = INT32_MIN;        /* sentinel: not yet completed */
    op.wait   = PYGO_IOURING_WAIT_INFLIGHT;

    if (pygo_iouring_submit_sqe(sqe, &op) != 0) return -1;

    /* Inline drain after submit.  With IORING_FEAT_FAST_POLL (5.7+)
     * the kernel completes inline when data is already ready, so the
     * CQE is in the ring before io_uring_enter even returns.
     * Draining here turns "submit + park + pump + drain + wake +
     * resume" into "submit + drain + return" for the data-ready
     * case, saving an entire scheduler round-trip per RT in the
     * common echo workload.  Runs while op.wait == INFLIGHT, so for
     * OUR op the drain just sets result + flips wait to DONE (no wake);
     * we observe that below and skip the park. */
    pygo_iouring_drain();

    if (hub != NULL) {
        /* Hub (M:N) path: PARK -- do NOT block the OS thread.  A blocking
         * io_uring_enter here holds this hub's tstate ATTACHED across the
         * wait, which deadlocks any stop-the-world (free-threaded GC, the
         * per-g-tstate attach protocol) whose completion depends on a thread
         * frozen at the STW barrier -- and it monopolizes the hub so its
         * other goroutines can't run.  Yield instead: the hub returns to its
         * (STW-safe, handoff-compatible) main loop, and when idle pumps the
         * shared ring's eventfd; a drainer wakes us via the wait handshake.
         * The inline drain above already completed us inline in the common
         * data-ready case (wait == DONE) -- skip the park then. */
        if (__atomic_load_n(&op.wait, __ATOMIC_ACQUIRE)
                != PYGO_IOURING_WAIT_DONE) {
            int prev = PYGO_IOURING_WAIT_INFLIGHT;
            if (__atomic_compare_exchange_n(&op.wait, &prev,
                                            PYGO_IOURING_WAIT_PARKED, 0,
                                            __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                /* Committed to park: any drainer completing the op now
                 * observes PARKED and wakes us.  park_current marks the g
                 * off-queue (hub_main won't re-enqueue on yield-return);
                 * the wake routes it back onto the hub sub-list, and the
                 * in_sub_queue CAS in hub_submit dedups a wake that races
                 * our coro_yield. */
                pygo_sched_park_current();
                pygo_coro_yield();
            }
            /* else prev == DONE: a concurrent drainer completed the op
             * between the load and the CAS; op.result is set, fall through. */
        }
    } else if (op.g != NULL) {
        /* Single-thread sched path: park cooperatively.  When the
         * eventfd fires the netpoll pump calls pygo_iouring_drain
         * which calls pygo_sched_wake_safe(op.g).  park_safe yields
         * via pygo_coro_yield; on resume we eat the pending wake. */
        pygo_sched_park_safe();
    } else {
        /* Not inside a goroutine (plain thread / main thread calling
         * file_read directly).  Block, but RELEASE the tstate around the
         * wait so a stop-the-world isn't held off by this syscall. */
        while (__atomic_load_n(&op.result, __ATOMIC_ACQUIRE) == INT32_MIN) {
            int n;
            Py_BEGIN_ALLOW_THREADS
            n = sys_io_uring_enter(pygo_iouring_state.ring_fd, 0, 1,
                                   IORING_ENTER_GETEVENTS, NULL, 0);
            Py_END_ALLOW_THREADS
            if (n < 0 && errno != EINTR) return -1;
            pygo_iouring_drain();
        }
    }

    {
        int32_t r = __atomic_load_n(&op.result, __ATOMIC_ACQUIRE);
        if (r < 0) { errno = -r; return -1; }
        return r;
    }
}

pygo_iouring_ssize_t pygo_iouring_pread(int fd, void *buf, size_t n,
                                        pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_READ;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return pygo_iouring_do(sqe);
}

pygo_iouring_ssize_t pygo_iouring_pwrite(int fd, const void *buf, size_t n,
                                         pygo_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_WRITE;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return pygo_iouring_do(sqe);
}

/* ============================================================
 * Multishot recv handle (Linux 6.0+ multishot + 5.19+ provided
 * buffer ring).
 *
 * Per fd, one in-flight multishot SQE; the kernel produces a CQE
 * each time data arrives, picking a buffer from the bgid=0 provided
 * buffer ring.  The handle queues received-but-not-yet-consumed
 * buffers and serves them to ms_recv calls.
 * ============================================================ */

typedef struct ms_entry {
    uint16_t  bid;
    uint16_t  len;
    struct ms_entry *next;
} ms_entry_t;

struct pygo_iouring_ms {
    pygo_iouring_op_t op;        /* must be first; SQE user_data */
    int               fd;
    int               closing;   /* ms_close called */
    int               err;       /* sticky errno or 0 */
    int               eof;       /* kernel signalled orderly EOF */
    int               armed;     /* multishot SQE in-flight */

    /* Ready buffer queue (head=oldest, tail=newest). */
    ms_entry_t       *ready_head;
    ms_entry_t       *ready_tail;

    /* Partially-consumed buffer carried between ms_recv calls. */
    int               inflight_bid;    /* -1 if none */
    uint32_t          inflight_off;
    uint32_t          inflight_len;

    /* Single waiter (most TCPConns have one consumer goroutine). */
    pygo_g_t         *waiter_g;
    void             *waiter_hub;

    pygo_mutex_t      lock;
};

/* Submit a multishot recv SQE for handle h.  Sets h->armed on
 * success.  Caller must hold h->lock. */
static int pygo_iouring_ms_submit(pygo_iouring_ms_t *h)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.flags     = IOSQE_BUFFER_SELECT;
    sqe.fd        = h->fd;
    /* addr/len = 0 with BUFFER_SELECT means "kernel picks one from the
     * provided-buffer ring at buf_group". */
    sqe.ioprio    = IORING_RECV_MULTISHOT;
    sqe.buf_group = PYGO_IOURING_PBUF_BGID;
    if (pygo_iouring_submit_sqe(sqe, &h->op) != 0) return -1;
    h->armed = 1;
    return 0;
}

pygo_iouring_ms_t *pygo_iouring_ms_open(int fd)
{
    pygo_iouring_ms_t *h;
    if (!pygo_iouring_pbuf_available()) return NULL;
    h = (pygo_iouring_ms_t *)calloc(1, sizeof(*h));
    if (h == NULL) return NULL;
    h->op.type = PYGO_IOURING_OP_MULTISHOT;
    h->fd      = fd;
    h->inflight_bid = -1;
    pygo_mutex_init(&h->lock);

    pygo_mutex_lock(&h->lock);
    if (pygo_iouring_ms_submit(h) != 0) {
        pygo_mutex_unlock(&h->lock);
        pygo_mutex_destroy(&h->lock);
        free(h);
        return NULL;
    }
    pygo_mutex_unlock(&h->lock);
    return h;
}

/* Drain handler.  Called from pygo_iouring_drain when a CQE arrives
 * with user_data pointing to a multishot handle.  Appends the
 * delivered buffer to the handle's ready queue and wakes any
 * parked consumer goroutine.  If the multishot ended (no F_MORE),
 * clears the armed flag so the next recv re-submits. */
static void pygo_iouring_ms_on_cqe(pygo_iouring_ms_t *h,
                                   int32_t res, uint32_t flags)
{
    int has_buffer = (flags & IORING_CQE_F_BUFFER) != 0;
    int more       = (flags & IORING_CQE_F_MORE) != 0;
    pygo_g_t *wake_g  = NULL;
    void     *wake_hub = NULL;
    int       was_closing;

    pygo_mutex_lock(&h->lock);

    if (has_buffer && res > 0) {
        ms_entry_t *e = (ms_entry_t *)malloc(sizeof(*e));
        if (e != NULL) {
            e->bid  = (uint16_t)(flags >> IORING_CQE_BUFFER_SHIFT);
            e->len  = (uint16_t)res;
            e->next = NULL;
            if (h->ready_tail) h->ready_tail->next = e;
            else               h->ready_head = e;
            h->ready_tail = e;
        } else {
            /* OOM: data is lost; return the buffer so the kernel can
             * reuse it.  Conn stays usable. */
            uint16_t bid = (uint16_t)(flags >> IORING_CQE_BUFFER_SHIFT);
            pygo_iouring_pbuf_return(bid);
        }
    } else if (res == 0 && !more) {
        h->eof = 1;
    } else if (res < 0) {
        /* -ENOBUFS = kernel ran out of buffers; re-arm on next recv.
         * -ECANCELED arrives in response to ms_close's cancel SQE. */
        if (res != -ENOBUFS && res != -ECANCELED) {
            h->err = -res;
        }
    }

    if (!more) h->armed = 0;

    /* Capture the waiter under the lock so we don't double-wake.
     * Actual wake call goes outside the lock for latency. */
    wake_g   = h->waiter_g;
    wake_hub = h->waiter_hub;
    h->waiter_g = NULL;

    was_closing = h->closing;
    pygo_mutex_unlock(&h->lock);

    if (wake_g != NULL) {
        if (wake_hub) pygo_mn_wake_g(wake_hub, wake_g);
        else          pygo_sched_wake_safe(wake_g);
    }

    /* If the handle is closing AND the multishot has ended, this
     * was the last CQE we'll ever see for it -- safe to free. */
    if (was_closing && !more) {
        ms_entry_t *e, *next;
        e = h->ready_head;
        while (e != NULL) {
            next = e->next;
            pygo_iouring_pbuf_return(e->bid);
            free(e);
            e = next;
        }
        if (h->inflight_bid >= 0)
            pygo_iouring_pbuf_return((unsigned)h->inflight_bid);
        pygo_mutex_destroy(&h->lock);
        free(h);
    }
}

pygo_iouring_ssize_t pygo_iouring_ms_recv(pygo_iouring_ms_t *h,
                                          void *buf, size_t n)
{
    char *out = (char *)buf;
    size_t out_off = 0;

    if (h == NULL) { errno = EINVAL; return -1; }
    if (n == 0)    return 0;

    pygo_mutex_lock(&h->lock);
    for (;;) {
        /* 1. Drain any partially-consumed in-flight buffer first. */
        if (h->inflight_bid >= 0) {
            size_t avail = h->inflight_len - h->inflight_off;
            size_t want  = n - out_off;
            size_t take  = (avail < want) ? avail : want;
            void *src = pygo_iouring_pbuf_addr((unsigned)h->inflight_bid);
            if (src != NULL && take > 0) {
                memcpy(out + out_off,
                       (char *)src + h->inflight_off, take);
                out_off += take;
                h->inflight_off += (uint32_t)take;
            }
            if (h->inflight_off >= h->inflight_len) {
                pygo_iouring_pbuf_return((unsigned)h->inflight_bid);
                h->inflight_bid = -1;
                h->inflight_off = 0;
                h->inflight_len = 0;
            }
            if (out_off >= n) {
                pygo_mutex_unlock(&h->lock);
                return (pygo_iouring_ssize_t)out_off;
            }
            /* User wants more.  Loop to grab another buffer. */
        }

        /* 2. Move next ready buffer into the in-flight slot. */
        if (h->ready_head != NULL) {
            ms_entry_t *e = h->ready_head;
            h->ready_head = e->next;
            if (h->ready_head == NULL) h->ready_tail = NULL;
            h->inflight_bid = e->bid;
            h->inflight_off = 0;
            h->inflight_len = e->len;
            free(e);
            continue;
        }

        /* 3. No data right now.  If we already copied some, return. */
        if (out_off > 0) {
            pygo_mutex_unlock(&h->lock);
            return (pygo_iouring_ssize_t)out_off;
        }

        /* 4. EOF / sticky error. */
        if (h->eof) {
            pygo_mutex_unlock(&h->lock);
            return 0;
        }
        if (h->err) {
            int e = h->err;
            pygo_mutex_unlock(&h->lock);
            errno = e;
            return -1;
        }

        /* 5. Re-arm if multishot ended (e.g. earlier -ENOBUFS). */
        if (!h->armed) {
            if (pygo_iouring_ms_submit(h) != 0) {
                int e = errno;
                pygo_mutex_unlock(&h->lock);
                errno = e;
                return -1;
            }
        }

        /* 6. Park.  Capture our g under the lock, release, then yield. */
        {
            void *hub = pygo_mn_current_hub_opaque();
            h->waiter_hub = hub;
            h->waiter_g   = (hub != NULL) ? pygo_mn_tls_current_g()
                                          : pygo_sched_get()->current;
            pygo_mutex_unlock(&h->lock);
            if (hub != NULL) {
                /* PARK -- do NOT spin-drain.  A blocking io_uring_enter here
                 * holds this hub's tstate ATTACHED across the wait, which
                 * deadlocks any stop-the-world (free-threaded GC) whose
                 * completion depends on a thread frozen at the STW barrier
                 * (e.g. the socket peer) -- this is the DEFAULT TCP recv path,
                 * so it's a live hang.  Yield instead: ms_on_cqe (run by the
                 * idle-hub pump draining the global ring) appends the buffer
                 * and wakes us via mn_wake_g.  No lost wakeup: waiter_g was
                 * published above under h->lock, and ms_on_cqe captures+NULLs
                 * it under the same lock -- and ms_recv holds h->lock
                 * continuously from the ready-queue check through publishing
                 * waiter_g, so a buffer either lands before our check (we
                 * consume it, never parking) or after we publish (ms_on_cqe
                 * wakes us).  The in_sub_queue CAS dedups a wake that races
                 * our coro_yield. */
                pygo_sched_park_current();
                pygo_coro_yield();
            } else {
                pygo_sched_park_safe();
            }
            pygo_mutex_lock(&h->lock);
            /* Loop back to top to consume whatever drain delivered. */
        }
    }
}

