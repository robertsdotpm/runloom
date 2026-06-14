/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on.
 *
 * io_uring-as-loop backend (RUNLOOM_IOURING_LOOP=1, default OFF).
 * ----------------------------------------------------------------------------
 * The default M:N hub blocks in epoll_wait (runloom_netpoll_pump) and io_uring
 * completions are delivered indirectly: each per-hub ring registers an eventfd
 * into the shared epoll set, the pump wakes on the eventfd and drains the ring.
 * That eventfd -> epoll -> pump -> drain bridge thrashes under socket load.
 *
 * With the loop backend the hub blocks DIRECTLY in its own ring via
 * io_uring_submit_and_wait_timeout (the kernel's native EXT_ARG timeout, no
 * SQE/CQE, Linux 5.11+) -- the ring IS the event loop, per the liburing /
 * gain / tokio-uring blueprint.  Two non-io_uring wake sources are bridged in
 * as persistent multishot POLL_ADDs identified by sentinel user_data:
 *
 *   - the hub's per-hub wake eventfd (RUNLOOM_LOOP_UD_WAKE): a cross-hub submit
 *     (runloom_mn_hub_submit) writes it to interrupt a ring-blocked hub, so a
 *     newly-submitted fiber is run promptly instead of after the idle tick.
 *   - the shared netpoll epoll fd (RUNLOOM_LOOP_UD_EPOLL): every existing
 *     wait_fd-based readiness path is UNCHANGED (it still epoll_ctl-registers
 *     and links a parker); when the epoll fd becomes readable the loop reports
 *     it and the hub does a non-blocking runloom_netpoll_pump(0) to drain the
 *     ready fds -> wake their parkers.  This keeps the whole parker/commit/
 *     by-fd/deadline machinery untouched -- only the block primitive changes.
 *
 * Ordinary op records (recv/send/..., user_data = an 8-byte-aligned pointer,
 * never a tiny sentinel) are drained inline here exactly as
 * runloom_iouring_ring_drain does -- that is the Stage-2 proactor hot path.
 *
 * Reaping is per-hub (DEFER_TASKRUN: only the ring's creating thread may
 * io_uring_enter(GETEVENTS)), so the loop drain runs single-reaper and needs
 * no per-CQE CAS (unlike runloom_iouring_ring_drain, which can race the shared
 * epoll pump on another idle hub).
 */

/* user_data sentinels for the persistent infrastructure polls.  Op records are
 * heap/stack pointers (page-ish addresses, 8-byte aligned), never these. */
#define RUNLOOM_LOOP_UD_WAKE   ((uint64_t)1)
#define RUNLOOM_LOOP_UD_EPOLL  ((uint64_t)2)

/* Bits OR'd into the caller's flags by the drain. */
#define RUNLOOM_LOOP_F_EPOLL   0x1   /* epoll fd readable: caller should pump(0) */
#define RUNLOOM_LOOP_F_WAKE    0x2   /* wake eventfd fired: re-drain the sub list */

/* Read RUNLOOM_IOURING_LOOP once.  1 = loop backend on.  Linux-only path. */
int runloom_iouring_loop_enabled(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_IOURING_LOOP");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Write one multishot POLL_ADD(fd, POLLIN) SQE into the SQ with `ud` as
 * user_data.  Does NOT enter the kernel (deferred); the next loop_wait submits
 * it.  Does NOT touch the inflight counter -- these polls are infrastructure,
 * always armed, and must not keep the hub "busy".  Single-issuer: caller is the
 * ring's owning hub. */
static void runloom_iouring_loop_enqueue_poll(runloom_iouring_ring_t *r,
                                              int fd, uint64_t ud)
{
    unsigned tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
    unsigned idx  = tail & r->sq_mask;
    struct io_uring_sqe *sqe = &r->sqes[idx];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode        = IORING_OP_POLL_ADD;
    sqe->fd            = fd;
    sqe->poll32_events = POLLIN;
    sqe->len           = IORING_POLL_ADD_MULTI;
    sqe->user_data     = ud;
    r->sq_array[idx]   = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
}

/* Submit every pending (un-consumed) SQE on the ring with a non-blocking
 * io_uring_enter, advancing sq_submitted_tail by what the kernel consumed.
 * EINTR-safe.  No-op when nothing is pending (the common Stage-1 case: the only
 * SQEs are the one-time poll arms, already submitted). */
static void runloom_iouring_loop_flush(runloom_iouring_ring_t *r)
{
    unsigned tail = __atomic_load_n(r->sq_tail, __ATOMIC_ACQUIRE);
    unsigned sub  = r->sq_submitted_tail;
    int n;
    if (tail == sub) return;
    n = sys_io_uring_submit_eintr(r->ring_fd, tail - sub);
    if (n > 0) r->sq_submitted_tail = sub + (unsigned)n;
}

/* Drain every CQE currently in the ring (single-reaper -- this hub only).
 * Sentinel CQEs set flags (and re-arm a dead multishot poll); op-record CQEs
 * publish their result and wake the parked fiber via the op->wait
 * handshake.  Advances the CQ head once for the whole batch. */
static void runloom_iouring_loop_drain(runloom_iouring_ring_t *r, int *flags_out)
{
    unsigned head, tail, i, seen = 0;
    if (r == NULL) return;
    head = __atomic_load_n(r->cq_head, __ATOMIC_ACQUIRE);
    tail = __atomic_load_n(r->cq_tail, __ATOMIC_ACQUIRE);
    for (i = head; i != tail; i++) {
        struct io_uring_cqe *cqe = &r->cqes[i & r->cq_mask];
        uint64_t ud     = cqe->user_data;
        int32_t  res    = cqe->res;
        uint32_t cflags = cqe->flags;
        seen++;
        if (ud == RUNLOOM_LOOP_UD_WAKE) {
            if (flags_out) *flags_out |= RUNLOOM_LOOP_F_WAKE;
            /* Multishot poll de-armed (should not happen for a long-lived
             * eventfd, but F_MORE clear means it won't fire again): re-arm. */
            if (!(cflags & IORING_CQE_F_MORE) && r->loop_wake_fd >= 0)
                runloom_iouring_loop_enqueue_poll(r, r->loop_wake_fd,
                                                  RUNLOOM_LOOP_UD_WAKE);
            continue;
        }
        if (ud == RUNLOOM_LOOP_UD_EPOLL) {
            if (flags_out) *flags_out |= RUNLOOM_LOOP_F_EPOLL;
            if (!(cflags & IORING_CQE_F_MORE) && r->loop_epoll_fd >= 0)
                runloom_iouring_loop_enqueue_poll(r, r->loop_epoll_fd,
                                                  RUNLOOM_LOOP_UD_EPOLL);
            continue;
        }
        /* Op record (Stage-2 proactor recv/send/etc).  Single-reaper, so no
         * CAS on the CQ -- but the op->wait handshake still gates the wake so a
         * not-yet-parked submitter (impossible same-thread, but kept for the
         * shared-ring safety contract) isn't double-resumed. */
        {
            runloom_iouring_op_t *op = (runloom_iouring_op_t *)(uintptr_t)ud;
            if (op != NULL) {
                __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
                if (op->hub != NULL) {
                    int prev = __atomic_exchange_n(&op->wait,
                                                   RUNLOOM_IOURING_WAIT_DONE,
                                                   __ATOMIC_ACQ_REL);
                    if (prev == RUNLOOM_IOURING_WAIT_PARKED)
                        runloom_mn_wake_g(op->hub, op->g);
                } else if (op->g != NULL) {
                    runloom_sched_wake_safe(op->g);
                }
            }
            __atomic_sub_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
            __atomic_sub_fetch(&runloom_iouring_inflight_count, 1,
                               __ATOMIC_ACQ_REL);
        }
    }
    if (seen) __atomic_store_n(r->cq_head, head + seen, __ATOMIC_RELEASE);
}

/* Create the hub's wake eventfd and arm the two persistent multishot polls
 * (wake eventfd + shared epoll fd) on its ring.  Called once from hub_main
 * after the ring is created, only when the loop backend is on.  Returns the
 * wake fd (>=0) on success; -1 on any failure (caller then discards the ring
 * and the hub falls back to the epoll pump, so no half-armed state survives).
 * epoll_fd may be -1 (no epoll backend) -- then only the wake poll is armed. */
int runloom_iouring_loop_hub_arm(runloom_iouring_ring_t *r, int epoll_fd)
{
    int wfd;
    if (r == NULL) return -1;
    wfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wfd < 0) return -1;
    r->loop_wake_fd  = wfd;
    r->loop_epoll_fd = epoll_fd;
    runloom_iouring_loop_enqueue_poll(r, wfd, RUNLOOM_LOOP_UD_WAKE);
    if (epoll_fd >= 0)
        runloom_iouring_loop_enqueue_poll(r, epoll_fd, RUNLOOM_LOOP_UD_EPOLL);
    /* Submit the arms now (non-blocking) so they are live before the first
     * wait.  On failure, undo: the caller will discard the ring. */
    runloom_iouring_loop_flush(r);
    if (r->sq_submitted_tail != __atomic_load_n(r->sq_tail, __ATOMIC_ACQUIRE)) {
        close(wfd);
        r->loop_wake_fd = r->loop_epoll_fd = -1;
        return -1;
    }
    return wfd;
}

/* Block the calling hub in its ring until >=1 completion or timeout_ns elapses
 * (timeout_ns < 0 = wait forever).  Submits any pending SQEs first, then drains
 * the whole completion batch.  *flags_out accumulates RUNLOOM_LOOP_F_* bits the
 * caller acts on (pump(0) on F_EPOLL; the sub-list is re-drained at the hub
 * loop top regardless, so F_WAKE needs no extra caller action).  Must run on
 * the ring's owning hub thread (DEFER_TASKRUN reaping constraint). */
void runloom_iouring_loop_wait(runloom_iouring_ring_t *r, long long timeout_ns,
                               int *flags_out)
{
    struct io_uring_getevents_arg arg;
    struct __kernel_timespec ts;
    int rc;
    if (r == NULL) return;

    /* Submit deferred SQEs (poll re-arms; Stage-2 ops).  Separated from the
     * wait enter below so an EINTR during submit can't be mistaken for a wait
     * timeout -- to_submit is 0 in the steady Stage-1 case so this is free. */
    runloom_iouring_loop_flush(r);

    memset(&arg, 0, sizeof arg);
    if (timeout_ns >= 0) {
        ts.tv_sec  = (int64_t)(timeout_ns / 1000000000LL);
        ts.tv_nsec = (int64_t)(timeout_ns % 1000000000LL);
        arg.ts = (uint64_t)(uintptr_t)&ts;
    }
    /* Wait for >=1 CQE or the timeout.  GETEVENTS also flushes DEFER_TASKRUN
     * task work so completions materialise here.  -ETIME on timeout (normal);
     * any other error just falls through to the drain (process what's there). */
    do {
        rc = sys_io_uring_enter(r->ring_fd, 0, 1,
                                IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG,
                                &arg, sizeof arg);
    } while (rc < 0 && errno == EINTR);

    runloom_iouring_loop_drain(r, flags_out);
}

/* ---- Stage-2 proactor I/O: recv/send as ring ops (the throughput path) ----
 *
 * The data op runs in the kernel; the fiber parks until the CQE arrives,
 * drained inline by the hub's loop_wait -- no epoll bridge, no readiness round
 * trip.  Submission is DEFERRED (write the SQE, don't enter): loop_wait submits
 * the whole batch in its single submit_and_wait_timeout syscall, so N parked
 * connections' ops are submitted + reaped together (the gain/liburing batching
 * win).  The op record lives on the parked fiber's coro stack -- valid
 * across the park, and the kernel reads/writes the fiber's buffer only
 * while it is parked (single-reaper, same-thread), so there is no aliasing.
 * (These all-C echo fibers are not cancellable, so the completion-model
 * buffer-ownership-on-cancel hazard does not arise here.) */

/* Write an op SQE into the SQ WITHOUT entering (deferred).  Bumps inflight so
 * the hub idle path keeps blocking in the ring while ops are outstanding.  If
 * the SQ is full, flush + drain to free slots (single-issuer, same thread). */
static void runloom_iouring_loop_enqueue_op(runloom_iouring_ring_t *r,
                                            struct io_uring_sqe sqe,
                                            runloom_iouring_op_t *op)
{
    unsigned tail, head, idx;
    for (;;) {
        tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(r->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < r->sq_entries) break;
        runloom_iouring_loop_flush(r);
        runloom_iouring_loop_drain(r, NULL);
    }
    idx = tail & r->sq_mask;
    r->sqes[idx] = sqe;
    r->sqes[idx].user_data = (uint64_t)(uintptr_t)op;
    r->sq_array[idx] = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
    __atomic_add_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
    __atomic_add_fetch(&runloom_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
}

/* Submit `sqe` (deferred) on the current hub's ring, park the fiber, and
 * return the CQE result (bytes, 0 on EOF, -1 with errno on error).  Must run on
 * a fiber whose hub owns r. */
static runloom_iouring_ssize_t runloom_iouring_loop_io(runloom_iouring_ring_t *r,
                                                       struct io_uring_sqe sqe)
{
    runloom_iouring_op_t op;
    void *hub;
    if (r == NULL) { errno = EINVAL; return -1; }
    hub = runloom_mn_current_hub_opaque();
    if (hub == NULL) { errno = EINVAL; return -1; }
    op.type   = RUNLOOM_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = runloom_mn_tls_current_g();
    op.result = INT32_MIN;
    op.wait   = RUNLOOM_IOURING_WAIT_INFLIGHT;
    op.ring   = r;            /* this hub ring (cancel_g symmetry; these all-C
                              * echo ops aren't cancellable, but keep the field
                              * defined rather than reading back garbage) */
    runloom_iouring_loop_enqueue_op(r, sqe, &op);
    /* Commit to parking.  loop_wait runs on THIS hub thread, so it cannot
     * complete the op mid-call; the CAS wins here and the later loop_drain sees
     * PARKED and wakes us.  (The handshake is kept for the shared contract.) */
    {
        int prev = RUNLOOM_IOURING_WAIT_INFLIGHT;
        if (__atomic_compare_exchange_n(&op.wait, &prev,
                                        RUNLOOM_IOURING_WAIT_PARKED, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            runloom_sched_park_current();
            runloom_coro_yield();
        }
    }
    {
        int32_t rr = __atomic_load_n(&op.result, __ATOMIC_ACQUIRE);
        if (rr < 0) { errno = -rr; return -1; }
        return rr;
    }
}

runloom_iouring_ssize_t runloom_iouring_loop_recv(runloom_iouring_ring_t *r,
                                                  int fd, void *buf, size_t n,
                                                  int flags)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return runloom_iouring_loop_io(r, sqe);
}

runloom_iouring_ssize_t runloom_iouring_loop_send(runloom_iouring_ring_t *r,
                                                  int fd, const void *buf,
                                                  size_t n, int flags)
{
    struct io_uring_sqe sqe;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_SEND;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return runloom_iouring_loop_io(r, sqe);
}

/* Write a hub's wake eventfd to interrupt its ring wait (called by a peer in
 * runloom_mn_hub_submit when the target hub is ring_waiting).  No-op on -1. */
void runloom_iouring_loop_wake(int wake_fd)
{
    if (wake_fd >= 0) {
        uint64_t one = 1;
        ssize_t w = write(wake_fd, &one, sizeof one);
        (void)w;   /* EAGAIN (counter saturated) is harmless: a wake is pending */
    }
}

/* Tear down the loop wake fd (the polls die with the ring at destroy). */
void runloom_iouring_loop_hub_disarm(runloom_iouring_ring_t *r)
{
    if (r == NULL) return;
    if (r->loop_wake_fd >= 0) { close(r->loop_wake_fd); r->loop_wake_fd = -1; }
    r->loop_epoll_fd = -1;
}
