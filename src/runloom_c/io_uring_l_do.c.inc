/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

/* ============================================================
 * In-fiber delivery of signal-handler exceptions to fibers parked on an
 * io_uring op (single-thread scheduler path).
 *
 * The scheduler hands a raised signal-handler exception (Ctrl-C / alarm) to a
 * blocked fiber IN-FIBER -- so the fiber's own try/except/finally runs -- by
 * waking a parked fiber with the exception stashed on its scheduler
 * (runloom_sched_drain -> runloom_netpoll_signal_wake, for NETPOLL parkers).  A
 * fiber blocked on an io_uring CQE is NOT a netpoll parker, so without a matching
 * hook the exception bypasses the fiber and is carried out of run(), skipping its
 * except/finally (backend-dependent control flow, audited bug).
 *
 * Single-thread io_uring parkers register a stack-allocated node in a
 * THREAD-LOCAL list: park and resume run on the scheduler's own OS thread,
 * cooperatively (never concurrently), and runloom_iouring_signal_wake() is called
 * from that same thread's drain, so the list needs no lock.  signal_wake marks
 * one parker SIGNALED and wakes it; on resume the parker restores the stashed
 * exception into its own tstate and returns -1, mirroring runloom_netpoll_wait_fd.
 * (M:N hub parkers are not registered here; hub signal delivery is separate.) */
typedef struct runloom_iou_sigwaiter {
    runloom_g_t                    *g;
    int                             signaled;
    struct runloom_iou_sigwaiter   *next;
    struct runloom_iou_sigwaiter  **pprev;   /* &(the pointer that points at us) */
} runloom_iou_sigwaiter_t;

static RUNLOOM_TLS runloom_iou_sigwaiter_t *runloom_iou_sigwaiters;

static void runloom_iou_sigwaiter_link(runloom_iou_sigwaiter_t *w, runloom_g_t *g)
{
    w->g        = g;
    w->signaled = 0;
    w->next     = runloom_iou_sigwaiters;
    w->pprev    = &runloom_iou_sigwaiters;
    if (w->next != NULL) w->next->pprev = &w->next;
    runloom_iou_sigwaiters = w;
}

static void runloom_iou_sigwaiter_unlink(runloom_iou_sigwaiter_t *w)
{
    if (w->pprev == NULL) return;          /* not linked */
    *w->pprev = w->next;
    if (w->next != NULL) w->next->pprev = w->pprev;
    w->pprev = NULL;
    w->next  = NULL;
}

/* Restore the scheduler's stashed signal-handler exception into the CURRENT
 * (this fiber's) tstate, so it propagates out of the parked op through the
 * fiber's own stack.  Returns 1 if an exception was restored.  Mirrors the
 * RUNLOOM_NETPOLL_SIGNALED handling in runloom_netpoll_wait_fd. */
static int runloom_iou_deliver_signal(runloom_g_t *g)
{
    runloom_sched_t *owner = (g != NULL && g->owner != NULL)
                             ? g->owner : runloom_sched_get();
    PyObject *exc = (owner != NULL) ? owner->signal_exc : NULL;
    if (owner != NULL) owner->signal_exc = NULL;
    if (exc != NULL) {
        PyObject *tb = PyException_GetTraceback(exc);   /* new ref or NULL */
        /* PyErr_Restore steals all three refs. */
        PyErr_Restore((PyObject *)Py_NewRef(Py_TYPE(exc)), exc, tb);
        return 1;
    }
    return 0;
}

/* Submit a global-ring ASYNC_CANCEL matching `target` by user_data so the kernel
 * completes `target` with -ECANCELED (best-effort; mirrors the global-ring branch
 * of runloom_iouring_cancel_g).  Used to unblock a parked single-shot op WITHOUT
 * returning while its SQE is still in flight -- which would leave the kernel
 * writing through the returned frame's op record (a stack use-after-free). */
static void runloom_iou_submit_cancel_global(runloom_iouring_op_t *target)
{
    runloom_iouring_op_t *cancel_op;
    struct io_uring_sqe sqe;
    cancel_op = (runloom_iouring_op_t *)calloc(1, sizeof(*cancel_op));
    if (cancel_op == NULL) return;
    cancel_op->type = RUNLOOM_IOURING_OP_CANCEL;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_ASYNC_CANCEL;
    sqe.addr   = (uint64_t)(uintptr_t)target;   /* match original SQE by user_data */
    sqe.fd     = -1;
    if (runloom_iouring_submit_sqe(sqe, cancel_op) != 0) free(cancel_op);
}

/* Hand the calling thread's stashed signal-handler exception to ONE io_uring
 * parker owned by that scheduler: mark it SIGNALED and wake it, so it restores
 * the exception in-fiber on resume.  Returns 1 if a parker took it, 0 if none are
 * registered (the scheduler then carries the exception out of run(), as before).
 * The io_uring analogue of runloom_netpoll_signal_wake; the single-thread drain
 * calls it after netpoll_signal_wake finds no parker. */
int runloom_iouring_signal_wake(void)
{
    runloom_iou_sigwaiter_t *w;
    for (w = runloom_iou_sigwaiters; w != NULL; w = w->next) {
        if (w->g != NULL && !w->signaled) {
            w->signaled = 1;
            runloom_sched_wake_safe(w->g);
            return 1;
        }
    }
    return 0;
}

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
         * (STW-safe) main loop, and when idle pumps the
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
        {
            runloom_iou_sigwaiter_t sw;
            int cancelled = 0;
            /* Register for in-fiber signal delivery (see runloom_iou_sigwaiter_*):
             * a Ctrl-C/alarm handler that raises while we're parked should
             * propagate through THIS fiber, not out of run(). */
            runloom_iou_sigwaiter_link(&sw, op.g);
            do {
                runloom_sched_park_safe();
                if (sw.signaled && !cancelled) {
                    /* Signal raised for this fiber.  We must NOT return while the
                     * single-shot SQE is still in flight -- its CQE would later
                     * write through this frame's `op` (a stack use-after-free) --
                     * so cancel the op and keep parking until its terminal
                     * (-ECANCELED) CQE lands; surface the signal afterwards. */
                    cancelled = 1;
                    runloom_iou_submit_cancel_global(&op);
                }
            } while (__atomic_load_n(&op.result, __ATOMIC_ACQUIRE) == INT32_MIN);
            runloom_iou_sigwaiter_unlink(&sw);
            if (sw.signaled &&
                __atomic_load_n(&op.result, __ATOMIC_ACQUIRE) < 0) {
                /* Op ended cancelled/errored: deliver the signal in-fiber.  (If it
                 * raced to success we fall through and return the data; the
                 * scheduler drops the undelivered exception at drain exit.) */
                (void)runloom_iou_deliver_signal(op.g);
                errno = EINTR;
                return -1;
            }
        }
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

/* A parked consumer fiber.  A conn may have several (concurrent recv() on one
 * socket is legal); each node lives on its parked fiber's stack and is linked
 * into h->waiters under h->lock. */
typedef struct ms_waiter {
    runloom_g_t       *g;
    void              *hub;
    int                on_list;   /* linked in h->waiters (guards double-link) */
    struct ms_waiter  *next;
} ms_waiter_t;

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

    /* Parked consumers.  Usually one, but concurrent recv() on a single conn
     * from multiple fibers is legal (stdlib blocking-socket semantics), so a
     * LIST is kept: a single slot silently dropped all but the last parker,
     * leaving earlier readers parked forever (lost wakeup / starvation). */
    ms_waiter_t      *waiters;

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
    ms_waiter_t *woken;
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

    /* Detach ALL parked consumers under the lock and wake each below (outside the
     * lock, for latency).  Waking every waiter -- not just one -- is required for
     * correctness: a single CQE can carry enough data for several readers, and
     * this mirrors level-triggered epoll, which re-delivers to every parker.  Each
     * woken fiber re-checks the queue and re-parks if it finds nothing.  The old
     * single waiter slot silently dropped all but the last parker (lost wakeup). */
    woken = h->waiters;
    h->waiters = NULL;
    {
        ms_waiter_t *w = woken;
        while (w != NULL) { w->on_list = 0; w = w->next; }
    }

    was_closing = h->closing;
    RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);

    while (woken != NULL) {
        /* Read the fields AND the successor link BEFORE waking: a woken fiber can
         * resume (on another thread) and return from ms_recv -- freeing this
         * stack-allocated node -- the instant we wake it. */
        ms_waiter_t *wnext = woken->next;
        runloom_g_t *wake_g   = woken->g;
        void        *wake_hub = woken->hub;
        /* IOUWAKE-TRACE: a multishot CQE was walked -- its buffer is appended to
         * the ready queue and this parked recv fiber is readied -- the model's
         * DrainConsume(g).  Emitted just before the wake, keyed by the woken g so
         * the driver pairs it with that fiber's SUBMIT/RESUME episode. */
        runloom_iouwake_trace_event("DRAIN_CONSUME",
                                    (unsigned long)(uintptr_t)wake_g, 0);
        if (wake_hub) runloom_mn_wake_g(wake_hub, wake_g);
        else          runloom_sched_wake_safe(wake_g);
        woken = wnext;
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
    ms_waiter_t self_w;                 /* our node in h->waiters (multi-reader fix) */
    runloom_iou_sigwaiter_t sw;         /* our node in the signal-delivery list */
    int deliver_signal = 0;
    runloom_g_t *sig_g = NULL;
    self_w.on_list = 0;
    self_w.next    = NULL;
    sw.signaled    = 0;
    sw.g           = NULL;
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
            runloom_g_t *park_g = (hub != NULL) ? runloom_mn_tls_current_g()
                                                : runloom_sched_get()->current;
            self_w.g   = park_g;
            self_w.hub = hub;
            /* Link our waiter node ONCE.  A spurious wake can resume us with the
             * node still linked (ms_on_cqe detaches + clears on_list only on a real
             * CQE wake); don't double-link then. */
            if (!self_w.on_list) {
                self_w.next    = h->waiters;
                h->waiters     = &self_w;
                self_w.on_list = 1;
            }
            /* IOUWAKE-TRACE: an op (the armed multishot recv) is submitted and
             * this fiber is about to park on its CQE -- the model's Submit(g).
             * Keyed by the waiter g pointer so the driver pairs it with the
             * matching DRAIN_CONSUME (ms_on_cqe) + RESUME below. */
            runloom_iouwake_trace_event("SUBMIT",
                                        (unsigned long)(uintptr_t)park_g, 0);
            RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
            if (hub != NULL) {
                /* PARK -- do NOT spin-drain.  A blocking io_uring_enter here
                 * holds this hub's tstate ATTACHED across the wait, which
                 * deadlocks any stop-the-world (free-threaded GC) whose
                 * completion depends on a thread frozen at the STW barrier
                 * (e.g. the socket peer) -- this is the DEFAULT TCP recv path,
                 * so it's a live hang.  Yield instead: ms_on_cqe (run by the
                 * idle-hub pump draining the global ring) appends the buffer
                 * and wakes us via mn_wake_g.  No lost wakeup: our waiter node
                 * was linked into h->waiters above under h->lock, and ms_on_cqe
                 * detaches the whole list under the same lock -- and ms_recv
                 * holds h->lock continuously from the ready-queue check through
                 * linking the waiter, so a buffer either lands before our check
                 * (we consume it, never parking) or after we link (ms_on_cqe
                 * wakes us).  The in_sub_queue CAS dedups a wake that races
                 * our coro_yield. */
                runloom_sched_park_current();
                runloom_coro_yield();
            } else {
                /* Single-thread: also register for in-fiber signal delivery so a
                 * Ctrl-C/alarm handler that raises while we're parked propagates
                 * through THIS fiber (its except/finally), not out of run(). */
                runloom_iou_sigwaiter_link(&sw, park_g);
                runloom_sched_park_safe();
                runloom_iou_sigwaiter_unlink(&sw);
            }
            /* IOUWAKE-TRACE: the woken submitter has returned from its park and
             * re-reads the handle -- the model's RESUME(g) (iouring-specific
             * resume point, l_do post-yield).  Pairs with the SUBMIT above. */
            runloom_iouwake_trace_event("RESUME",
                                        (unsigned long)(uintptr_t)park_g, 0);
            RUNLOOM_RLOCK(&h->lock, RUNLOOM_RANK_HUB);
            /* Woken to deliver a signal-handler exception?  Surface it out of
             * recv().  Any buffers already queued on the handle stay in the ready
             * queue (not consumed here), so no data is lost. */
            if (sw.signaled) {
                deliver_signal = 1;
                sig_g          = park_g;
                ret            = -1;
                goto done;
            }
            /* Loop back to top to consume whatever drain delivered. */
        }
    }
done:
    /* Unlink our waiter node if it's still linked (returning after a spurious
     * wake that left it linked, or via a signal): it lives on THIS frame's stack,
     * so it must not remain in h->waiters after we return.  Under the held lock. */
    if (self_w.on_list) {
        ms_waiter_t **pp = &h->waiters;
        while (*pp != NULL) {
            if (*pp == &self_w) { *pp = self_w.next; break; }
            pp = &(*pp)->next;
        }
        self_w.on_list = 0;
    }
    /* Drop our recv-ref under the held lock.  If we are the last referencer (the
     * conn was closed + the terminal CQE dropped the live ref while we ran), free
     * the handle here; otherwise the owner / a concurrent ms_close still holds a
     * ref and frees later.  Either way no other thread can touch a refcount==0 h. */
    {
        int last = (--h->refcount == 0);
        RUNLOOM_RUNLOCK(&h->lock, RUNLOOM_RANK_HUB);
        if (last) runloom_iouring_ms_free(h);
        if (deliver_signal) {
            /* Restore the signal-handler exception into this fiber's tstate and
             * return -1; the recv caller must propagate a set Python error rather
             * than raise OSError (see runloom_netpoll_wait_fd's SIGNALED path). */
            (void)runloom_iou_deliver_signal(sig_g);
            errno = EINTR;
            return -1;
        }
        if (saved_errno) errno = saved_errno;
        return ret;
    }
    }
}

