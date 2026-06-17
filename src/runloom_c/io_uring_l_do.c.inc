/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static runloom_iouring_ssize_t runloom_iouring_do(struct io_uring_sqe sqe)
{
    runloom_iouring_op_t op;
    void *hub;

    if (!runloom_iouring_available()) {
        errno = ENOSYS;
        return -1;
    }

    hub = runloom_mn_current_hub_opaque();
    op.type   = RUNLOOM_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = (hub != NULL) ? runloom_mn_tls_current_g()
                              : runloom_sched_get()->current;
    op.result = INT32_MIN;        /* sentinel: not yet completed */
    op.wait   = RUNLOOM_IOURING_WAIT_INFLIGHT;
    op.ring   = NULL;             /* global ring */

    if (runloom_iouring_submit_sqe(sqe, &op) != 0) return -1;

    /* Inline drain after submit.  With IORING_FEAT_FAST_POLL (5.7+)
     * the kernel completes inline when data is already ready, so the
     * CQE is in the ring before io_uring_enter even returns.
     * Draining here turns "submit + park + pump + drain + wake +
     * resume" into "submit + drain + return" for the data-ready
     * case, saving an entire scheduler round-trip per RT in the
     * common echo workload.  Runs while op.wait == INFLIGHT, so for
     * OUR op the drain just sets result + flips wait to DONE (no wake);
     * we observe that below and skip the park. */
    runloom_iouring_drain();

    if (hub != NULL) {
        /* Hub (M:N) path: PARK -- do NOT block the OS thread.  A blocking
         * io_uring_enter here holds this hub's tstate ATTACHED across the
         * wait, which deadlocks any stop-the-world (free-threaded GC, the
         * per-g-tstate attach protocol) whose completion depends on a thread
         * frozen at the STW barrier -- and it monopolizes the hub so its
         * other fibers can't run.  Yield instead: the hub returns to its
         * (STW-safe, handoff-compatible) main loop, and when idle pumps the
         * shared ring's eventfd; a drainer wakes us via the wait handshake.
         * The inline drain above already completed us inline in the common
         * data-ready case (wait == DONE) -- skip the park then. */
        if (__atomic_load_n(&op.wait, __ATOMIC_ACQUIRE)
                != RUNLOOM_IOURING_WAIT_DONE) {
            int prev = RUNLOOM_IOURING_WAIT_INFLIGHT;
            if (__atomic_compare_exchange_n(&op.wait, &prev,
                                            RUNLOOM_IOURING_WAIT_PARKED, 0,
                                            __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                RUNLOOM_IOU_NOTE(RUNLOOM_IOURING_WAIT_INFLIGHT,
                                 RUNLOOM_IOURING_WAIT_PARKED);
                /* Committed to park: any drainer completing the op now
                 * observes PARKED and wakes us.  park_current marks the g
                 * off-queue (hub_main won't re-enqueue on yield-return);
                 * the wake routes it back onto the hub sub-list, and the
                 * in_sub_queue CAS in hub_submit dedups a wake that races
                 * our coro_yield. */
                /* Publish the op so a cross-thread task.cancel can target it
                 * with an ASYNC_CANCEL (runloom_iouring_cancel_g).  Set after the
                 * PARKED CAS so the canceller's wait==PARKED gate is consistent;
                 * cleared on resume before the op leaves scope. */
                if (op.g != NULL)
                    __atomic_store_n(&op.g->iouring_op, &op, __ATOMIC_RELEASE);
                runloom_sched_park_current();
                runloom_coro_yield();
                if (op.g != NULL)
                    __atomic_store_n(&op.g->iouring_op, NULL, __ATOMIC_RELEASE);
            }
            /* else prev == DONE: a concurrent drainer completed the op
             * between the load and the CAS; op.result is set, fall through. */
        }
    } else if (op.g != NULL) {
        /* Single-thread sched path: park cooperatively.  When the
         * eventfd fires the netpoll pump calls runloom_iouring_drain
         * which calls runloom_sched_wake_safe(op.g).  park_safe yields
         * via runloom_coro_yield; on resume we eat the pending wake.
         *
         * RE-PARK on a SPURIOUS wake: park_safe returns on ANY wake, but the
         * generic park can be resumed by a stale/duplicate wake before the CQE
         * drain has set op.result.  Returning then would hand back the INT32_MIN
         * sentinel as the result AND leave the real completion's later wake to
         * land on an unparked g (dropped).  Loop until op.result is actually set
         * -- the same "park until the condition holds" discipline as the chan /
         * netpoll / multishot park loops.  park_safe's wake_pending Dekker makes
         * the recheck-then-re-park race-free.  (audit round-3 single-op lost wake.) */
        do {
            runloom_sched_park_safe();
        } while (__atomic_load_n(&op.result, __ATOMIC_ACQUIRE) == INT32_MIN);
    } else {
        /* Not inside a fiber (plain thread / main thread calling
         * file_read directly).  Block, but RELEASE the tstate around the
         * wait so a stop-the-world isn't held off by this syscall. */
        while (__atomic_load_n(&op.result, __ATOMIC_ACQUIRE) == INT32_MIN) {
            int n;
            Py_BEGIN_ALLOW_THREADS
            n = sys_io_uring_enter(runloom_iouring_state.ring_fd, 0, 1,
                                   IORING_ENTER_GETEVENTS, NULL, 0);
            Py_END_ALLOW_THREADS
            if (n < 0 && errno != EINTR) return -1;
            runloom_iouring_drain();
        }
    }

    {
        int32_t r = __atomic_load_n(&op.result, __ATOMIC_ACQUIRE);
#if defined(RUNLOOM_FSM_VALIDATE)
        /* Route the io_uring CQE completion through the SAME total I/O FSM as the
         * sync backends (its res<0 is -errno).  One NOTE here covers every
         * io_uring op; READ/RECV classify like recv, WRITE/SEND like send
         * (pread/pwrite of regular files share that >=0 / <0-errno shape). */
        {
            int io_err = (r < 0) ? -(int)r : 0;
            switch (sqe.opcode) {
            case IORING_OP_READ:  case IORING_OP_RECV:
                RUNLOOM_IO_NOTE(RUNLOOM_IO_RECV,
                    runloom_io_classify(RUNLOOM_IO_RECV, (long long)r, io_err));
                break;
            case IORING_OP_WRITE: case IORING_OP_SEND:
                RUNLOOM_IO_NOTE(RUNLOOM_IO_SEND,
                    runloom_io_classify(RUNLOOM_IO_SEND, (long long)r, io_err));
                break;
            default: break;   /* other opcodes are not byte-stream I/O */
            }
        }
#endif
        if (r < 0) { errno = -r; return -1; }
        return r;
    }
}

runloom_iouring_ssize_t runloom_iouring_pread(int fd, void *buf, size_t n,
                                        runloom_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_READ;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return runloom_iouring_do(sqe);
}

runloom_iouring_ssize_t runloom_iouring_pwrite(int fd, const void *buf, size_t n,
                                         runloom_iouring_off_t offset)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_WRITE;
    sqe.fd     = fd;
    sqe.addr   = (uintptr_t)buf;
    sqe.len    = (unsigned)n;
    sqe.off    = (uint64_t)offset;
    return runloom_iouring_do(sqe);
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

struct runloom_iouring_ms {
    runloom_iouring_op_t op;        /* must be first; SQE user_data */
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

    /* Single waiter (most TCPConns have one consumer fiber). */
    runloom_g_t         *waiter_g;
    void             *waiter_hub;

    /* Lifetime refcount, guarded by `lock`.  Refs: (1) the "live" ref taken at
     * ms_open and dropped by the terminal closing CQE in ms_on_cqe (or by
     * ms_close when the multishot already ended); (2) an in-progress ms_recv
     * holds a ref for its whole call -- crucially ACROSS its park -- so a closing
     * CQE that wakes the parked recv and drops the live ref cannot free the handle
     * out from under the recv when it resumes and re-locks; (3) ms_close holds a
     * ref across its own body.  Whoever drops the LAST ref frees.  This replaces
     * the old wake-then-free, which UAF'd a resuming parked ms_recv on a shared
     * conn (reader+closer split).  (audit io_uring multishot ms_close-vs-ms_recv UAF.) */
    int               refcount;

    runloom_mutex_t      lock;
};

/* Free the handle: drain ready/inflight buffers, destroy the lock, free.  Caller
 * must hold NO lock and must have observed refcount==0, so no other thread can
 * reach `h`. */
static void runloom_iouring_ms_free(runloom_iouring_ms_t *h)
{
    ms_entry_t *e = h->ready_head, *next;
    while (e != NULL) { next = e->next; runloom_iouring_pbuf_return(e->bid); free(e); e = next; }
    if (h->inflight_bid >= 0)
        runloom_iouring_pbuf_return((unsigned)h->inflight_bid);
    runloom_mutex_destroy(&h->lock);
    free(h);
}

/* Drop one lifetime ref; free iff it was the last.  Caller must NOT hold h->lock. */
static void runloom_iouring_ms_put(runloom_iouring_ms_t *h)
{
    int last;
    RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);
    last = (--h->refcount == 0);
    RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
    if (last) runloom_iouring_ms_free(h);
}

/* Submit a multishot recv SQE for handle h.  Sets h->armed on
 * success.  Caller must hold h->lock. */
static int runloom_iouring_ms_submit(runloom_iouring_ms_t *h)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.flags     = IOSQE_BUFFER_SELECT;
    sqe.fd        = h->fd;
    /* addr/len = 0 with BUFFER_SELECT means "kernel picks one from the
     * provided-buffer ring at buf_group". */
    sqe.ioprio    = IORING_RECV_MULTISHOT;
    sqe.buf_group = RUNLOOM_IOURING_PBUF_BGID;
    if (runloom_iouring_submit_sqe(sqe, &h->op) != 0) return -1;
    h->armed = 1;
    return 0;
}

runloom_iouring_ms_t *runloom_iouring_ms_open(int fd)
{
    runloom_iouring_ms_t *h;
    if (!runloom_iouring_pbuf_available()) return NULL;
    h = (runloom_iouring_ms_t *)calloc(1, sizeof(*h));
    if (h == NULL) return NULL;
    h->op.type = RUNLOOM_IOURING_OP_MULTISHOT;
    h->fd      = fd;
    h->inflight_bid = -1;
    h->refcount = 1;             /* the "live" ref; dropped by the terminal CQE / ms_close */
    runloom_mutex_init(&h->lock);

    RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);
    if (runloom_iouring_ms_submit(h) != 0) {
        RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
        runloom_mutex_destroy(&h->lock);
        free(h);
        return NULL;
    }
    RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
    return h;
}

/* Drain handler.  Called from runloom_iouring_drain when a CQE arrives
 * with user_data pointing to a multishot handle.  Appends the
 * delivered buffer to the handle's ready queue and wakes any
 * parked consumer fiber.  If the multishot ended (no F_MORE),
 * clears the armed flag so the next recv re-submits. */
static void runloom_iouring_ms_on_cqe(runloom_iouring_ms_t *h,
                                   int32_t res, uint32_t flags)
{
    int has_buffer = (flags & IORING_CQE_F_BUFFER) != 0;
    int more       = (flags & IORING_CQE_F_MORE) != 0;
    runloom_g_t *wake_g  = NULL;
    void     *wake_hub = NULL;
    int       was_closing;
    int       ms_from;

    RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);

    /* Observational multishot FSM: capture the lifecycle state BEFORE this CQE
     * mutates {armed,eof,err}, then NOTE the edge after.  Does not touch the
     * setting logic below. */
    ms_from = runloom_ms_state(h->armed, h->eof, h->err);

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
            runloom_iouring_pbuf_return(bid);
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

    /* The CQE has now settled {armed,eof,err}; assert the lifecycle edge.
     * (void)ms_from keeps it used in a normal build, where RUNLOOM_MS_NOTE is a
     * no-op and ms_from would otherwise be set-but-unused.) */
    (void)ms_from;
    RUNLOOM_MS_NOTE(ms_from, runloom_ms_state(h->armed, h->eof, h->err));

    /* Capture the waiter under the lock so we don't double-wake.
     * Actual wake call goes outside the lock for latency. */
    wake_g   = h->waiter_g;
    wake_hub = h->waiter_hub;
    h->waiter_g = NULL;

    was_closing = h->closing;
    RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);

    if (wake_g != NULL) {
        if (wake_hub) runloom_mn_wake_g(wake_hub, wake_g);
        else          runloom_sched_wake_safe(wake_g);
    }

    /* If the handle is closing AND the multishot has ended, this was the last CQE
     * we'll ever see for it -- drop the "live" ref.  ms_put frees ONLY if no other
     * ref is held: a parked ms_recv we just woke holds its own recv-ref (so we do
     * NOT free under it -- it frees when it resumes, re-locks safely, and returns);
     * with no in-flight recv, this is the last ref and frees here.  Replaces the
     * old unconditional free, which raced a resuming parked ms_recv into a UAF. */
    if (was_closing && !more) {
        runloom_iouring_ms_put(h);
    }
}

runloom_iouring_ssize_t runloom_iouring_ms_recv(runloom_iouring_ms_t *h,
                                          void *buf, size_t n)
{
    char *out = (char *)buf;
    size_t out_off = 0;

    if (h == NULL) { errno = EINVAL; return -1; }
    if (n == 0)    return 0;

    RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);
    h->refcount++;   /* pin the handle for this whole call -- ACROSS our park --
                      * so a concurrent ms_close + terminal closing CQE cannot free
                      * it out from under us when we resume and re-lock (the
                      * shared-conn reader-vs-closer UAF).  Dropped at `done:`. */
    {
    runloom_iouring_ssize_t ret;
    int saved_errno = 0;
    for (;;) {
        /* 1. Drain any partially-consumed in-flight buffer first. */
        if (h->inflight_bid >= 0) {
            size_t avail = h->inflight_len - h->inflight_off;
            size_t want  = n - out_off;
            size_t take  = (avail < want) ? avail : want;
            void *src = runloom_iouring_pbuf_addr((unsigned)h->inflight_bid);
            if (src != NULL && take > 0) {
                memcpy(out + out_off,
                       (char *)src + h->inflight_off, take);
                out_off += take;
                h->inflight_off += (uint32_t)take;
            }
            if (h->inflight_off >= h->inflight_len) {
                runloom_iouring_pbuf_return((unsigned)h->inflight_bid);
                h->inflight_bid = -1;
                h->inflight_off = 0;
                h->inflight_len = 0;
            }
            if (out_off >= n) { ret = (runloom_iouring_ssize_t)out_off; goto done; }
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
        if (out_off > 0) { ret = (runloom_iouring_ssize_t)out_off; goto done; }

        /* 4. Sticky error / EOF.  DEFENSIVE: check `err` BEFORE `eof` so an
         * error wins.  ms_on_cqe sets exactly one of {eof, err} per CQE -- eof
         * (res==0 && !more) and err (res<0) are MUTUALLY EXCLUSIVE per CQE (one
         * CQE has one `res`), and after either terminal the multishot is
         * un-armed and the kernel posts no further CQEs -- so they cannot
         * coexist today and this order is not a live-bug fix.  But IF a future
         * change ever let both be set, surfacing the error (return -1/errno)
         * strictly beats masking it as a clean EOF (return 0); the err-first
         * order makes that the invariant by construction. */
        if (h->err) { saved_errno = h->err; ret = -1; goto done; }
        if (h->eof) { ret = 0; goto done; }

        /* 4b. Closed while we looped/parked -- possibly by ANOTHER fiber on a
         * shared conn (ms_close set h->closing).  Do NOT re-arm a closing handle
         * (that would re-submit a multishot on a fd being torn down); report EOF.
         * Without this, a resumed waiter after the closing terminal CQE would fall
         * into the re-arm branch below.  (audit io_uring ms_close-vs-ms_recv.) */
        if (h->closing) { ret = 0; goto done; }

        /* 5. Re-arm if multishot ended (e.g. earlier -ENOBUFS).  Reached only
         * past the err/eof/closing returns above, so the handle is genuinely
         * UNARMED (not a terminal state) -- a real UNARMED->ARMED lifecycle edge. */
        if (!h->armed) {
            if (runloom_iouring_ms_submit(h) != 0) { saved_errno = errno; ret = -1; goto done; }
            RUNLOOM_MS_NOTE(RUNLOOM_MS_UNARMED, RUNLOOM_MS_ARMED);
        }

        /* 6. Park.  Capture our g under the lock, release, then yield. */
        {
            void *hub = runloom_mn_current_hub_opaque();
            h->waiter_hub = hub;
            h->waiter_g   = (hub != NULL) ? runloom_mn_tls_current_g()
                                          : runloom_sched_get()->current;
            RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
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
                runloom_sched_park_current();
                runloom_coro_yield();
            } else {
                runloom_sched_park_safe();
            }
            RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);
            /* Loop back to top to consume whatever drain delivered. */
        }
    }
done:
    /* Drop our recv-ref under the held lock.  If we are the last referencer (the
     * conn was closed + the terminal CQE dropped the live ref while we ran), free
     * the handle here; otherwise the owner / a concurrent ms_close still holds a
     * ref and frees later.  Either way no other thread can touch a refcount==0 h. */
    {
        int last = (--h->refcount == 0);
        RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
        if (last) runloom_iouring_ms_free(h);
        if (saved_errno) errno = saved_errno;
        return ret;
    }
    }
}

