/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

struct runloom_iouring_ring {
    int ring_fd;
    int eventfd_fd;
    int defer_taskrun;          /* 1 if DEFER_TASKRUN is enabled */

    /* SQ ring */
    void  *sq_mmap;             size_t sq_mmap_size;
    unsigned *sq_head;          unsigned *sq_tail;
    unsigned  sq_mask;          unsigned  sq_entries;
    unsigned *sq_array;
    struct io_uring_sqe *sqes;
    void  *sqe_mmap;            size_t sqe_mmap_size;

    /* CQ ring */
    void  *cq_mmap;             size_t cq_mmap_size;
    unsigned *cq_head;          unsigned *cq_tail;
    unsigned  cq_mask;          unsigned  cq_entries;
    struct io_uring_cqe *cqes;

    /* Per-ring inflight counter.  Hub_main checks this when deciding
     * pump-vs-sleep so the hub keeps spinning the pump while any of
     * its iouring ops are outstanding. */
    volatile int inflight;
};

/* Hub-ring init.  Tries SINGLE_ISSUER + (optionally) DEFER_TASKRUN;
 * if the kernel rejects either flag, retries without it once.  This
 * keeps creation portable across 5.1+ kernels without per-syscall
 * feature probing. */
runloom_iouring_ring_t *runloom_iouring_ring_create(int defer_taskrun)
{
    struct io_uring_params p;
    runloom_iouring_ring_t *r;
    int fd, efd;
    void *sq_map = MAP_FAILED, *cq_map = MAP_FAILED, *sqe_map = MAP_FAILED;
    size_t sq_size = 0, cq_size = 0, sqe_size = 0;
    int reg_arg;
    unsigned flags;

    r = (runloom_iouring_ring_t *)calloc(1, sizeof(*r));
    if (r == NULL) { errno = ENOMEM; return NULL; }
    r->ring_fd = r->eventfd_fd = -1;

    /* Attempt full flag set; downgrade on EINVAL. */
    flags = IORING_SETUP_SINGLE_ISSUER;
    if (defer_taskrun) flags |= IORING_SETUP_DEFER_TASKRUN;
    memset(&p, 0, sizeof(p));
    p.flags = flags;
    fd = sys_io_uring_setup(64, &p);
    if (fd < 0 && errno == EINVAL && defer_taskrun) {
        /* DEFER_TASKRUN unsupported.  Retry without it; SINGLE_ISSUER
         * came in 5.18 so it's the more likely survivor. */
        memset(&p, 0, sizeof(p));
        p.flags = IORING_SETUP_SINGLE_ISSUER;
        fd = sys_io_uring_setup(64, &p);
        defer_taskrun = 0;
    }
    if (fd < 0 && errno == EINVAL) {
        /* SINGLE_ISSUER unsupported too (5.1-5.17 kernels).  Plain
         * setup -- still correct, just multi-issuer-tolerant. */
        memset(&p, 0, sizeof(p));
        fd = sys_io_uring_setup(64, &p);
    }
    if (fd < 0) {
        free(r);
        return NULL;
    }
    r->defer_taskrun = defer_taskrun;

    sq_size = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    sq_map = mmap(NULL, sq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq_map == MAP_FAILED) goto fail;

    cq_size = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    cq_map = mmap(NULL, cq_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
    if (cq_map == MAP_FAILED) goto fail;

    sqe_size = p.sq_entries * sizeof(struct io_uring_sqe);
    sqe_map = mmap(NULL, sqe_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (sqe_map == MAP_FAILED) goto fail;

    efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) goto fail;

    reg_arg = efd;
    if (sys_io_uring_register(fd, IORING_REGISTER_EVENTFD, &reg_arg, 1) < 0) {
        close(efd);
        goto fail;
    }

    r->ring_fd       = fd;
    r->eventfd_fd    = efd;
    r->sq_mmap       = sq_map;       r->sq_mmap_size  = sq_size;
    r->cq_mmap       = cq_map;       r->cq_mmap_size  = cq_size;
    r->sqe_mmap      = sqe_map;      r->sqe_mmap_size = sqe_size;
    r->sq_head    = (unsigned *)((char *)sq_map + p.sq_off.head);
    r->sq_tail    = (unsigned *)((char *)sq_map + p.sq_off.tail);
    r->sq_mask    = *(unsigned *)((char *)sq_map + p.sq_off.ring_mask);
    r->sq_entries = *(unsigned *)((char *)sq_map + p.sq_off.ring_entries);
    r->sq_array   = (unsigned *)((char *)sq_map + p.sq_off.array);
    r->sqes       = (struct io_uring_sqe *)sqe_map;
    r->cq_head    = (unsigned *)((char *)cq_map + p.cq_off.head);
    r->cq_tail    = (unsigned *)((char *)cq_map + p.cq_off.tail);
    r->cq_mask    = *(unsigned *)((char *)cq_map + p.cq_off.ring_mask);
    r->cq_entries = *(unsigned *)((char *)cq_map + p.cq_off.ring_entries);
    r->cqes       = (struct io_uring_cqe *)((char *)cq_map + p.cq_off.cqes);
    return r;

fail:
    if (sq_map  != MAP_FAILED) munmap(sq_map,  sq_size);
    if (cq_map  != MAP_FAILED) munmap(cq_map,  cq_size);
    if (sqe_map != MAP_FAILED) munmap(sqe_map, sqe_size);
    close(fd);
    free(r);
    return NULL;
}

void runloom_iouring_ring_destroy(runloom_iouring_ring_t *r)
{
    if (r == NULL) return;
    if (r->sq_mmap  != NULL) munmap(r->sq_mmap,  r->sq_mmap_size);
    if (r->cq_mmap  != NULL) munmap(r->cq_mmap,  r->cq_mmap_size);
    if (r->sqe_mmap != NULL) munmap(r->sqe_mmap, r->sqe_mmap_size);
    if (r->eventfd_fd >= 0) close(r->eventfd_fd);
    if (r->ring_fd    >= 0) close(r->ring_fd);
    free(r);
}

int runloom_iouring_ring_eventfd(const runloom_iouring_ring_t *r)
{
    return r != NULL ? r->eventfd_fd : -1;
}

int runloom_iouring_ring_inflight(const runloom_iouring_ring_t *r)
{
    if (r == NULL) return 0;
    return __atomic_load_n(&r->inflight, __ATOMIC_ACQUIRE);
}

/* Submit one SQE to a hub ring.  No submission mutex: the caller is
 * the SINGLE issuer (this ring's owning hub thread).  Returns 0 on
 * success, -1 with errno on failure. */
static int runloom_iouring_ring_submit_sqe(runloom_iouring_ring_t *r,
                                        struct io_uring_sqe sqe_template,
                                        runloom_iouring_op_t *op)
{
    unsigned tail, head, idx;
    int n;
    while (1) {
        tail = __atomic_load_n(r->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(r->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < r->sq_entries) break;
        /* SQ full.  Drain CQ to free SQEs (the SINGLE issuer assumption
         * also means we drain inline here without competing with anyone
         * else). */
        runloom_iouring_ring_drain(r);
    }
    idx = tail & r->sq_mask;
    r->sqes[idx] = sqe_template;
    r->sqes[idx].user_data = (uint64_t)(uintptr_t)op;
    r->sq_array[idx] = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
    n = sys_io_uring_submit_eintr(r->ring_fd, 1);   /* retry on EINTR */
    if (n < 0) return -1;
    __atomic_add_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
    /* Also bump the process-wide counter so hub_main's idle decision
     * can do a single atomic load instead of walking the registered-
     * rings list under a lock. */
    __atomic_add_fetch(&runloom_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    return 0;
}

void runloom_iouring_ring_drain(runloom_iouring_ring_t *r)
{
    if (r == NULL) return;
    if (r->eventfd_fd >= 0) {
        uint64_t scratch;
        while (read(r->eventfd_fd, &scratch, sizeof(scratch))
               == (ssize_t)sizeof(scratch))
            { /* drain */ }
    }
    /* CAS-claim per CQE: SINGLE_ISSUER only restricts SQE submission,
     * not CQ drain.  The netpoll pump (running on whichever hub goes
     * idle) can race with this ring's owning hub doing its own inline
     * drain.  Without the CAS both would process the same CQE and
     * runloom_mn_wake_g would push the goroutine onto the submission
     * list twice -> a second resume after the first one already
     * advanced the coro, corrupting the stack. */
    for (;;) {
        unsigned head = __atomic_load_n(r->cq_head, __ATOMIC_ACQUIRE);
        unsigned ct   = __atomic_load_n(r->cq_tail, __ATOMIC_ACQUIRE);
        struct io_uring_cqe *cqe;
        runloom_iouring_op_t *op;
        int32_t  res;
        unsigned expected;
        if (head == ct) return;
        cqe = &r->cqes[head & r->cq_mask];
        res = cqe->res;
        op  = (runloom_iouring_op_t *)(uintptr_t)cqe->user_data;
        expected = head;
        if (!__atomic_compare_exchange_n(r->cq_head, &expected, head + 1,
                                         0, __ATOMIC_ACQ_REL,
                                         __ATOMIC_ACQUIRE)) {
            continue;
        }
        if (op != NULL) {
            /* Hub rings only carry SINGLE ops -- multishot stays on the
             * global ring.  Same park/wake handshake as the global-ring
             * SINGLE path: publish result, then wake ONLY if the submitter
             * committed to parking (wait == PARKED).  The inline ring_drain
             * runs on the not-yet-parked submitter (wait == INFLIGHT), so it
             * records the completion without waking -- the submitter sees
             * DONE on its own CAS and skips the park.  Without this the inline
             * drain would mn_wake_g a still-running g and double-resume it. */
            __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
            if (op->hub != NULL) {
                int prev = __atomic_exchange_n(&op->wait,
                                               RUNLOOM_IOURING_WAIT_DONE,
                                               __ATOMIC_ACQ_REL);
                if (prev == RUNLOOM_IOURING_WAIT_PARKED) {
                    runloom_mn_wake_g(op->hub, op->g);
                }
            } else if (op->g != NULL) {
                runloom_sched_wake_safe(op->g);
            }
        }
        __atomic_sub_fetch(&r->inflight, 1, __ATOMIC_ACQ_REL);
        __atomic_sub_fetch(&runloom_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    }
}

void runloom_iouring_ring_get_events(runloom_iouring_ring_t *r)
{
    if (r == NULL || !r->defer_taskrun) return;
    /* DEFER_TASKRUN: kernel only flushes task work + posts CQEs when
     * the user calls io_uring_enter(GETEVENTS).  We don't wait
     * (min_complete=0) -- just trigger the flush so the eventfd fires
     * if anything is pending.  Best-effort; ignore errors. */
    (void)sys_io_uring_enter(r->ring_fd, 0, 0,
                             IORING_ENTER_GETEVENTS, NULL, 0);
}

/* Common submit-and-park for hub-ring recv/send.  Caller must be a
 * goroutine running on the hub that owns r.  Returns bytes or -1
 * with errno. */
static runloom_iouring_ssize_t runloom_iouring_ring_do(runloom_iouring_ring_t *r,
                                                 struct io_uring_sqe sqe)
{
    runloom_iouring_op_t op;
    void *hub;

    if (r == NULL) { errno = EINVAL; return -1; }
    hub = runloom_mn_current_hub_opaque();
    if (hub == NULL) {
        /* Shouldn't happen if callers respect the contract, but fall
         * back gracefully: park via the global ring path. */
        errno = EINVAL;
        return -1;
    }
    op.type   = RUNLOOM_IOURING_OP_SINGLE;
    op.hub    = hub;
    op.g      = runloom_mn_tls_current_g();
    op.result = INT32_MIN;
    op.wait   = RUNLOOM_IOURING_WAIT_INFLIGHT;

    if (runloom_iouring_ring_submit_sqe(r, sqe, &op) != 0) return -1;

    /* Inline drain: with FAST_POLL the CQE may already be in the ring.
     * Drain locally so the data-ready case avoids a park+wake round-trip.
     * Runs while op.wait == INFLIGHT, so for OUR op the drain records the
     * result + flips wait to DONE WITHOUT waking (we're not parked yet) --
     * we observe that below and skip the park. */
    runloom_iouring_ring_drain(r);

    /* Park the hub g unless the op already completed (inline or via a
     * concurrent drainer).  runloom_sched_park_current snaps the per-g tstate
     * slice AND marks self_queued so hub_main won't re-enqueue on return
     * from yield.  Commit to parking with the wait CAS so any drainer that
     * completes the op after this observes PARKED and wakes us; a drainer
     * that already completed it set wait=DONE, so the CAS fails and we
     * don't park.  Wake routes via mn_wake_g -> hub sub-list; the
     * in_sub_queue CAS dedups a wake that races our coro_yield. */
    if (__atomic_load_n(&op.wait, __ATOMIC_ACQUIRE) != RUNLOOM_IOURING_WAIT_DONE) {
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

runloom_iouring_ssize_t runloom_iouring_ring_recv(runloom_iouring_ring_t *r,
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
    return runloom_iouring_ring_do(r, sqe);
}

runloom_iouring_ssize_t runloom_iouring_ring_send(runloom_iouring_ring_t *r,
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
    return runloom_iouring_ring_do(r, sqe);
}

