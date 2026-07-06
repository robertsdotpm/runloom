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

/* Burn-in belt-and-suspenders: RUNLOOM_IOURING_LOOP_PUMP_ALWAYS=1 restores the
 * old every-idle-tick pump(0) symptom-guard on top of the one-shot fix.  Default
 * OFF -- the one-shot epoll poll now re-derives the epoll fd's level every wait
 * cycle, so F_EPOLL is reliable and the unconditional pump is redundant (kept
 * available in case a soak surfaces a residual path). */
int runloom_iouring_loop_pump_always(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_IOURING_LOOP_PUMP_ALWAYS");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* ======================= Stage-3 multishot recv ===========================
 * One persistent IORING_OP_RECV | MULTISHOT | BUFFER_SELECT SQE per connection
 * delivers a CQE per chunk into a PER-HUB provided buffer ring -- no re-submit
 * and no submit/park per recv, amortising the per-op cost the single-shot
 * Stage-2 path pays.  Everything here runs ONLY on the ring's owning hub
 * thread: it is the sole SQE issuer (SINGLE_ISSUER), the sole CQE reaper
 * (loop_drain), and the sole buffer-returner -- the echo fibers are
 * hub-pinned (a woken fiber routes to the hub-local FIFO, never the stealable
 * deque, so it never migrates off the hub its multishot is armed on).  Hence
 * NO LOCKS: the fiber and loop_drain are the same OS thread and never run
 * concurrently.  Gated behind RUNLOOM_IOURING_MS (requires the loop backend),
 * default off. */

#ifndef RUNLOOM_LOOP_MS_BGID
#  define RUNLOOM_LOOP_MS_BGID 0
#endif
#define RUNLOOM_LOOP_MS_PBUF_COUNT_DEFAULT 1024
#define RUNLOOM_LOOP_MS_PBUF_SIZE          2048
/* user_data sentinel for the ASYNC_CANCEL loop_ms_close submits; its own
 * completion CQE is discarded by loop_drain (op records are 8-byte-aligned
 * pointers, never this tiny value, alongside UD_WAKE=1 / UD_EPOLL=2). */
#define RUNLOOM_LOOP_UD_MSCANCEL  ((uint64_t)3)

typedef struct runloom_loop_ms runloom_loop_ms_t;
static void runloom_loop_ms_on_cqe(runloom_loop_ms_t *h,
                                   int32_t res, uint32_t flags);

/* 1 if the multishot recv path is enabled (RUNLOOM_IOURING_MS set); read once.
 * Only consulted on the loop backend. */
int runloom_iouring_loop_ms_enabled(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_IOURING_MS");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* True iff SOME io_uring feature is opted in -- the loop backend
 * (RUNLOOM_IOURING_LOOP), multishot recv (RUNLOOM_IOURING_MS, which REQUIRES
 * loop, so loop_enabled already covers it), or the TCP proactor transport
 * (RUNLOOM_TCPCONN_IOURING=1|auto).  All default OFF.  When none is set, the
 * per-hub io_uring ring carries no ops and its periodic GETEVENTS flush is pure
 * waste, so mn_sched_hub_main skips creating the ring.  Read once. */
int runloom_iouring_any_enabled(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *t = getenv("RUNLOOM_TCPCONN_IOURING");
        int tcp = (t != NULL && (t[0] == '1' || strcmp(t, "auto") == 0)) ? 1 : 0;
        cur = (runloom_iouring_loop_enabled() || tcp) ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Per-hub provided buffer ring count (env RUNLOOM_IOURING_MS_BUFS, default
 * 1024).  Rounded up to a power of two, clamped to [16, 65536] (bid is 16-bit). */
static unsigned runloom_loop_ms_pbuf_count(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_IOURING_MS_BUFS");
        long n = (e != NULL) ? atol(e) : RUNLOOM_LOOP_MS_PBUF_COUNT_DEFAULT;
        unsigned p = 16;
        if (n < 16) n = 16;
        if (n > 65536) n = 65536;
        while (p < (unsigned)n) p <<= 1;
        cur = (int)p;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return (unsigned)cur;
}

/* Register a provided buffer ring on THIS hub ring (bgid RUNLOOM_LOOP_MS_BGID).
 * Returns 0 on success, -1 on failure (caller leaves ms_pool NULL -> multishot
 * ms_open returns NULL -> the echo falls back to single-shot).  Mirrors the
 * process-global pool setup in io_uring_l_sys.c.inc, but per-ring and lock-free
 * (the owning hub is the sole tail producer). */
static int runloom_iouring_loop_ms_pbuf_setup(runloom_iouring_ring_t *r)
{
    struct runloom_iouring_buf_reg reg;
    struct runloom_iouring_buf *bufs;
    unsigned count = runloom_loop_ms_pbuf_count();
    unsigned size  = RUNLOOM_LOOP_MS_PBUF_SIZE;
    size_t pool_total = (size_t)count * size;
    size_t bring_size = sizeof(struct runloom_iouring_buf) * count;
    long page = sysconf(_SC_PAGESIZE);
    void *pool = NULL, *bring = NULL;
    unsigned i;
    if (page <= 0) page = 4096;
    if (posix_memalign(&pool, (size_t)page, pool_total) != 0)  pool = NULL;
    if (posix_memalign(&bring, (size_t)page, bring_size) != 0) bring = NULL;
    if (pool == NULL || bring == NULL) { free(pool); free(bring); return -1; }
    memset(bring, 0, bring_size);
    memset(&reg, 0, sizeof(reg));
    reg.ring_addr    = (uintptr_t)bring;
    reg.ring_entries = count;
    reg.bgid         = RUNLOOM_LOOP_MS_BGID;
    reg.flags        = 0;
    if (sys_io_uring_register(r->ring_fd, IORING_REGISTER_PBUF_RING,
                              &reg, 1) != 0) {
        free(pool); free(bring); return -1;
    }
    bufs = (struct runloom_iouring_buf *)bring;
    for (i = 0; i < count; i++) {
        bufs[i].addr = (uintptr_t)((char *)pool + (size_t)i * size);
        bufs[i].len  = size;
        bufs[i].bid  = (uint16_t)i;
        bufs[i].resv = 0;
    }
    /* Kernel tail overlays bufs[0].resv; release so it sees all descriptors. */
    __atomic_store_n(&bufs[0].resv, (uint16_t)count, __ATOMIC_RELEASE);
    r->ms_pool  = pool;
    r->ms_bring = bring;
    r->ms_count = count;
    r->ms_size  = size;
    r->ms_mask  = count - 1;
    return 0;
}

static void runloom_iouring_loop_ms_pbuf_teardown(runloom_iouring_ring_t *r)
{
    struct runloom_iouring_buf_reg reg;
    if (r == NULL || r->ms_pool == NULL) return;
    /* Unregister BEFORE freeing so the kernel drops its reference to bring/pool
     * (the ring fd is closed right after, which would also drop it, but the
     * close happens in ring_destroy after this returns). */
    memset(&reg, 0, sizeof(reg));
    reg.bgid = RUNLOOM_LOOP_MS_BGID;
    sys_io_uring_register(r->ring_fd, IORING_UNREGISTER_PBUF_RING, &reg, 1);
    free(r->ms_pool);  r->ms_pool  = NULL;
    free(r->ms_bring); r->ms_bring = NULL;
    r->ms_count = r->ms_size = r->ms_mask = 0;
}

static void *runloom_iouring_loop_ms_buf_addr(runloom_iouring_ring_t *r,
                                              unsigned bid)
{
    if (r->ms_pool == NULL || bid >= r->ms_count) return NULL;
    return (char *)r->ms_pool + (size_t)bid * r->ms_size;
}

/* Return a consumed buffer to this hub's ring (lock-free: the owning hub is the
 * sole tail producer). */
static void runloom_iouring_loop_ms_buf_return(runloom_iouring_ring_t *r,
                                               unsigned bid)
{
    struct runloom_iouring_buf *bufs;
    uint16_t tail, idx;
    if (r->ms_pool == NULL || bid >= r->ms_count) return;
    bufs = (struct runloom_iouring_buf *)r->ms_bring;
    tail = __atomic_load_n(&bufs[0].resv, __ATOMIC_ACQUIRE);
    idx  = (uint16_t)(tail & r->ms_mask);
    bufs[idx].addr = (uintptr_t)((char *)r->ms_pool + (size_t)bid * r->ms_size);
    bufs[idx].len  = r->ms_size;
    bufs[idx].bid  = (uint16_t)bid;
    __atomic_store_n(&bufs[0].resv, (uint16_t)(tail + 1), __ATOMIC_RELEASE);
}

/* Write one POLL_ADD(fd, POLLIN) SQE into the SQ with `ud` as user_data.  Does
 * NOT enter the kernel (deferred); the next loop_wait submits it.  Does NOT
 * touch the inflight counter -- these polls are infrastructure, always armed,
 * and must not keep the hub "busy".  Single-issuer: caller is the ring's owning
 * hub.
 *
 * `multishot`: a MULTISHOT poll (IORING_POLL_ADD_MULTI) stays armed and re-fires
 * per readiness TRANSITION, but the kernel never re-samples the fd's standing
 * LEVEL between transitions.  That is fatal when polling the SHARED EPOLL FD: an
 * interior socket left LEVEL-readable with no fresh transition (a partial recv +
 * re-park, a >64-event pump truncation, a late-linked parker whose readiness was
 * only stashed) is invisible to a standing multishot -> no CQE -> the parked
 * reader hangs (the reproduced loop-backend hang).  A ONE-SHOT poll instead runs
 * an arm-time vfs_poll that re-samples current LEVEL and fires immediately if the
 * fd is still readable -- so re-arming it every cycle makes the bridge re-derive
 * ground truth exactly like plain epoll_wait.  Use multishot=0 for the epoll fd;
 * the wake eventfd can stay multishot (its miss is masked by the Dekker sub_head
 * re-check + the global-ring direct drain). */
static void runloom_iouring_loop_enqueue_poll(runloom_iouring_ring_t *r,
                                              int fd, uint64_t ud, int multishot)
{
    unsigned tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
    unsigned idx  = tail & r->sq_mask;
    struct io_uring_sqe *sqe = &r->sqes[idx];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode        = IORING_OP_POLL_ADD;
    sqe->fd            = fd;
    sqe->poll32_events = POLLIN;
    sqe->len           = multishot ? IORING_POLL_ADD_MULTI : 0;
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
                                                  RUNLOOM_LOOP_UD_WAKE, /*multishot=*/1);
            continue;
        }
        if (ud == RUNLOOM_LOOP_UD_EPOLL) {
            if (flags_out) *flags_out |= RUNLOOM_LOOP_F_EPOLL;
            /* ONE-SHOT re-arm, UNCONDITIONALLY (a one-shot CQE is always
             * terminal; no F_MORE).  loop_flush submits this deferred SQE before
             * the next blocking enter, and a one-shot POLL_ADD's arm-time
             * vfs_poll re-samples the epoll fd's CURRENT level -- firing an
             * immediate CQE if it is still readable.  So every wait cycle begins
             * with a fresh ground-truth sample, closing the standing-multishot
             * edge-drop (see the enqueue_poll header). */
            if (r->loop_epoll_fd >= 0)
                runloom_iouring_loop_enqueue_poll(r, r->loop_epoll_fd,
                                                  RUNLOOM_LOOP_UD_EPOLL, /*multishot=*/0);
            continue;
        }
        if (ud == RUNLOOM_LOOP_UD_MSCANCEL) {
            /* ASYNC_CANCEL completion from loop_ms_close: nothing to do -- the
             * cancelled multishot's own terminal (no-F_MORE) CQE is what clears
             * its handle.  Did not bump inflight, so don't decrement. */
            continue;
        }
        /* Op record (Stage-2 proactor recv/send, or a Stage-3 multishot recv).
         * Single-reaper, so no CAS on the CQ -- but the op->wait handshake still
         * gates the SINGLE wake so a not-yet-parked submitter (impossible
         * same-thread, but kept for the shared-ring safety contract) isn't
         * double-resumed. */
        {
            runloom_iouring_op_t *op = (runloom_iouring_op_t *)(uintptr_t)ud;
            if (op != NULL && op->type == RUNLOOM_IOURING_OP_MULTISHOT) {
                /* Stage-3 multishot recv: deliver the buffer / EOF / error to
                 * the per-connection handle (op is its first field).  The op
                 * stays armed on the SAME SQE across chunks (F_MORE); only its
                 * terminal CQE (no F_MORE) ends it, so decrement inflight just
                 * then -- submit and each re-submit bumped it once. */
                runloom_loop_ms_on_cqe((runloom_loop_ms_t *)op, res, cflags);
                if (!(cflags & IORING_CQE_F_MORE)) {
                    __atomic_sub_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
                    __atomic_sub_fetch(&runloom_iouring_inflight_count, 1,
                                       __ATOMIC_ACQ_REL);
                }
                continue;
            }
            if (op != NULL) {
                __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
                if (op->hub != NULL) {
                    int prev = __atomic_exchange_n(&op->wait,
                                                   RUNLOOM_IOURING_WAIT_DONE,
                                                   __ATOMIC_ACQ_REL);
                    RUNLOOM_IOU_NOTE(prev, RUNLOOM_IOURING_WAIT_DONE);
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
    runloom_iouring_loop_enqueue_poll(r, wfd, RUNLOOM_LOOP_UD_WAKE, /*multishot=*/1);
    if (epoll_fd >= 0)   /* ONE-SHOT: re-derives the epoll fd's level each cycle */
        runloom_iouring_loop_enqueue_poll(r, epoll_fd, RUNLOOM_LOOP_UD_EPOLL, /*multishot=*/0);
    /* Submit the arms now (non-blocking) so they are live before the first
     * wait.  On failure, undo: the caller will discard the ring. */
    runloom_iouring_loop_flush(r);
    if (r->sq_submitted_tail != __atomic_load_n(r->sq_tail, __ATOMIC_ACQUIRE)) {
        close(wfd);
        r->loop_wake_fd = r->loop_epoll_fd = -1;
        return -1;
    }
    /* Stage-3: register this hub's provided buffer ring for multishot recv.
     * Best-effort -- on failure ms_pool stays NULL and ms_open returns NULL, so
     * the echo falls back to the single-shot Stage-2 path (no hard failure). */
    if (runloom_iouring_loop_ms_enabled())
        runloom_iouring_loop_ms_pbuf_setup(r);
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
            RUNLOOM_IOU_NOTE(RUNLOOM_IOURING_WAIT_INFLIGHT,
                             RUNLOOM_IOURING_WAIT_PARKED);
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
    runloom_iouring_loop_ms_pbuf_teardown(r);   /* unregister + free the pool */
    if (r->loop_wake_fd >= 0) { close(r->loop_wake_fd); r->loop_wake_fd = -1; }
    r->loop_epoll_fd = -1;
}

/* ---- Stage-3 multishot recv handle (per connection) ----------------------
 * Heap-allocated (opaque void* to the caller in module_io's all-C echo, which
 * lives in a different translation unit).  Accessed ONLY by the owning hub
 * thread -- the echo fiber AND loop_drain are that one thread and never run
 * concurrently (the fiber is hub-pinned once armed) -- so no locks. */
struct runloom_loop_ms {
    runloom_iouring_op_t op;        /* FIRST: SQE user_data; op.type==MULTISHOT */
    runloom_iouring_ring_t *ring;
    int fd;
    int armed;                      /* 1 while a multishot SQE is in flight */
    int eof;                        /* peer closed (a res==0 CQE) */
    int err;                        /* sticky -errno (NOT ENOBUFS / ECANCELED) */
    runloom_g_t *waiter_g;          /* set while the fiber is parked for data */
    void *waiter_hub;
    /* FIFO of delivered-but-unconsumed buffers.  Capacity q_cap == ms_count, so
     * it can never overflow: the pool holds at most ms_count buffers total, so
     * a single handle can hold at most that many un-returned.  q_bid/q_len point
     * into the one allocation that backs this handle (no per-recv malloc). */
    uint16_t *q_bid;
    uint16_t *q_len;
    unsigned  q_cap, q_head, q_count;
    /* partially-consumed head buffer */
    int      cur_bid;               /* -1 = none */
    unsigned cur_off, cur_len;
};

/* Write the persistent multishot recv SQE into the hub ring SQ (deferred;
 * loop_wait submits the batch).  Bumps inflight so the hub keeps reaping while
 * armed.  Marks the handle armed.  Returns 0 (always succeeds: SQ-full is
 * resolved by flush+drain, single issuer). */
static int runloom_iouring_loop_ms_submit(runloom_loop_ms_t *h)
{
    runloom_iouring_ring_t *r = h->ring;
    unsigned tail, head, idx;
    struct io_uring_sqe *sqe;
    for (;;) {
        tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(r->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < r->sq_entries) break;
        runloom_iouring_loop_flush(r);
        runloom_iouring_loop_drain(r, NULL);
    }
    idx = tail & r->sq_mask;
    sqe = &r->sqes[idx];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_RECV;
    sqe->fd        = h->fd;
    sqe->ioprio    = IORING_RECV_MULTISHOT;
    sqe->flags     = IOSQE_BUFFER_SELECT;
    sqe->buf_group = RUNLOOM_LOOP_MS_BGID;
    sqe->user_data = (uint64_t)(uintptr_t)&h->op;
    r->sq_array[idx] = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
    __atomic_add_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
    __atomic_add_fetch(&runloom_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    h->armed = 1;
    return 0;
}

/* Drain handler: a multishot CQE delivered a buffer / EOF / error / de-arm.
 * Runs on the hub thread inside loop_drain while the consumer fiber is parked
 * (same thread -> no lock).  Pushes data into the ready FIFO and wakes the
 * fiber if it is waiting. */
static void runloom_loop_ms_on_cqe(runloom_loop_ms_t *h, int32_t res,
                                   uint32_t flags)
{
    if (res > 0 && (flags & IORING_CQE_F_BUFFER)) {
        unsigned bid = (unsigned)(flags >> IORING_CQE_BUFFER_SHIFT);
        if (h->q_count < h->q_cap) {       /* invariant: cannot exceed the pool */
            unsigned slot = (h->q_head + h->q_count) % h->q_cap;
            h->q_bid[slot] = (uint16_t)bid;
            h->q_len[slot] = (uint16_t)res;
            h->q_count++;
        } else {
            /* Should be unreachable (depth bounded by pool size).  Don't corrupt
             * memory: return the buffer instead of enqueuing (drops the chunk). */
            runloom_iouring_loop_ms_buf_return(h->ring, bid);
        }
    } else if (res == 0) {
        h->eof = 1;                        /* orderly peer close */
    } else if (res < 0) {
        int e = -res;
        if (e != ENOBUFS && e != ECANCELED && h->err == 0)
            h->err = e;                    /* a real, sticky error */
        /* ENOBUFS (pool momentarily empty) / ECANCELED (close): not sticky;
         * the multishot de-arms and recv re-submits when it next needs data. */
    }
    if (!(flags & IORING_CQE_F_MORE))
        h->armed = 0;                      /* terminated; recv re-arms if needed */
    if (h->waiter_g != NULL) {
        runloom_g_t *g = h->waiter_g;
        void *hub = h->waiter_hub;
        h->waiter_g = NULL; h->waiter_hub = NULL;
        if (hub != NULL) runloom_mn_wake_g(hub, g);
        else             runloom_sched_wake_safe(g);
    }
}

/* Open a multishot recv stream on fd on ring r.  Returns an opaque handle, or
 * NULL if multishot isn't available on this hub ring (no pbuf pool / alloc
 * failure) -- the caller then falls back to single-shot recv.  Arms the
 * persistent SQE immediately. */
void *runloom_iouring_loop_ms_open(runloom_iouring_ring_t *r, int fd)
{
    runloom_loop_ms_t *h;
    unsigned cap;
    size_t need;
    if (r == NULL || r->ms_pool == NULL) return NULL;
    cap  = r->ms_count;
    need = sizeof(*h) + (size_t)cap * 2u * sizeof(uint16_t);
    h = (runloom_loop_ms_t *)malloc(need);
    if (h == NULL) return NULL;
    memset(&h->op, 0, sizeof(h->op));
    h->op.type = RUNLOOM_IOURING_OP_MULTISHOT;
    h->op.ring = r;
    h->ring = r; h->fd = fd;
    h->armed = 0; h->eof = 0; h->err = 0;
    h->waiter_g = NULL; h->waiter_hub = NULL;
    h->q_bid = (uint16_t *)(h + 1);
    h->q_len = h->q_bid + cap;
    h->q_cap = cap; h->q_head = 0; h->q_count = 0;
    h->cur_bid = -1; h->cur_off = 0; h->cur_len = 0;
    runloom_iouring_loop_ms_submit(h);
    return h;
}

/* Cooperatively read up to n bytes.  Returns bytes (>0), 0 on EOF, or -1 with
 * errno on error.  Serves from buffered data first, parks for more if needed. */
runloom_iouring_ssize_t runloom_iouring_loop_ms_recv(void *handle,
                                                     void *buf, size_t n)
{
    runloom_loop_ms_t *h = (runloom_loop_ms_t *)handle;
    runloom_iouring_ring_t *r = h->ring;
    for (;;) {
        /* 1. Serve from the partially-consumed current buffer. */
        if (h->cur_bid >= 0) {
            unsigned avail = h->cur_len - h->cur_off;
            unsigned take  = (avail < n) ? avail : (unsigned)n;
            char *src = (char *)runloom_iouring_loop_ms_buf_addr(
                            r, (unsigned)h->cur_bid);
            if (src != NULL && take > 0)
                memcpy(buf, src + h->cur_off, take);
            h->cur_off += take;
            if (h->cur_off >= h->cur_len) {
                runloom_iouring_loop_ms_buf_return(r, (unsigned)h->cur_bid);
                h->cur_bid = -1; h->cur_off = h->cur_len = 0;
            }
            if (take > 0) return (runloom_iouring_ssize_t)take;
            /* a zero-length buffer: drop it and keep going */
            continue;
        }
        /* 2. Promote the next queued buffer to current. */
        if (h->q_count > 0) {
            h->cur_bid = (int)h->q_bid[h->q_head];
            h->cur_len = h->q_len[h->q_head];
            h->cur_off = 0;
            h->q_head  = (h->q_head + 1) % h->q_cap;
            h->q_count--;
            continue;
        }
        /* 3. No buffered data: report terminal conditions. */
        if (h->eof) return 0;
        if (h->err) { errno = h->err; return -1; }
        /* Re-arm if the multishot de-armed (ENOBUFS / kernel ended it). */
        if (!h->armed)
            runloom_iouring_loop_ms_submit(h);
        /* 4. Park until loop_drain delivers a buffer / EOF / error.  Same thread
         * as the drainer, so publishing the waiter BEFORE coro_yield is
         * race-free (the drain cannot run until we yield). */
        h->waiter_g   = runloom_mn_tls_current_g();
        h->waiter_hub = runloom_mn_current_hub_opaque();
        runloom_sched_park_current();
        runloom_coro_yield();
        h->waiter_g = NULL; h->waiter_hub = NULL;
    }
}

/* Close a multishot stream.  If still armed (a mid-stream close, e.g. a send
 * error), cancel the SQE and park until its terminal CQE clears the handle, so
 * no in-flight completion dereferences freed memory.  Returns all held buffers
 * to the pool and frees the handle. */
void runloom_iouring_loop_ms_close(void *handle)
{
    runloom_loop_ms_t *h = (runloom_loop_ms_t *)handle;
    runloom_iouring_ring_t *r;
    if (h == NULL) return;
    r = h->ring;
    if (h->armed) {
        unsigned tail, head, idx;
        struct io_uring_sqe *sqe;
        for (;;) {
            tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
            head = __atomic_load_n(r->sq_head, __ATOMIC_ACQUIRE);
            if ((tail - head) < r->sq_entries) break;
            runloom_iouring_loop_flush(r);
            runloom_iouring_loop_drain(r, NULL);
        }
        idx = tail & r->sq_mask;
        sqe = &r->sqes[idx];
        memset(sqe, 0, sizeof(*sqe));
        sqe->opcode    = IORING_OP_ASYNC_CANCEL;
        sqe->addr      = (uint64_t)(uintptr_t)&h->op;   /* cancel by user_data */
        sqe->user_data = RUNLOOM_LOOP_UD_MSCANCEL;       /* drain discards it */
        r->sq_array[idx] = idx;
        __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
        while (h->armed) {
            h->waiter_g   = runloom_mn_tls_current_g();
            h->waiter_hub = runloom_mn_current_hub_opaque();
            runloom_sched_park_current();
            runloom_coro_yield();
            h->waiter_g = NULL; h->waiter_hub = NULL;
        }
    }
    /* Reclaim every buffer still held so the pool doesn't leak across conns. */
    if (h->cur_bid >= 0)
        runloom_iouring_loop_ms_buf_return(r, (unsigned)h->cur_bid);
    while (h->q_count > 0) {
        runloom_iouring_loop_ms_buf_return(r, h->q_bid[h->q_head]);
        h->q_head = (h->q_head + 1) % h->q_cap;
        h->q_count--;
    }
    free(h);
}
