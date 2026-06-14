/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

int runloom_iouring_available(void)
{
    if (__atomic_load_n(&runloom_iouring_state.initialised, __ATOMIC_ACQUIRE) == 0) {
        /* Serialize lazy init.  Concurrent first-callers -- e.g. several M:N
         * hubs each running a fiber that touches io_uring at the same
         * instant, with no prior single-threaded available() call -- must NOT
         * each run lazy_init: that io_uring_setup()s multiple rings and races
         * the shared ring-state pointers (ring_fd / sq_tail / sqes / ...), so
         * submits land in different/short-lived rings, SQE slots get
         * overwritten, and ops vanish without ever completing (an intermittent
         * multi-hub "lost completion" hang -- masked whenever a test happens to
         * call iouring_available() once on the main thread first).
         * lock_ensure_inited gives us an initialized sub_lock; double-check
         * `initialised` under it so EXACTLY ONE caller runs lazy_init, and hold
         * it across init so no submit_sqe (also sub_lock) sees a half-built
         * ring. */
        runloom_iouring_lock_ensure_inited();
        runloom_mutex_lock(&runloom_iouring_state.sub_lock);
        if (runloom_iouring_state.initialised == 0) {
            runloom_iouring_lazy_init();
        }
        runloom_mutex_unlock(&runloom_iouring_state.sub_lock);
    }
    return __atomic_load_n(&runloom_iouring_state.initialised, __ATOMIC_ACQUIRE) == 1;
}

int runloom_iouring_eventfd(void)
{
    if (!runloom_iouring_available()) return -1;
    return runloom_iouring_state.eventfd_fd;
}

int runloom_iouring_inflight(void)
{
    return __atomic_load_n(&runloom_iouring_inflight_count, __ATOMIC_ACQUIRE);
}

int runloom_iouring_pbuf_available(void)
{
    if (!runloom_iouring_available()) return 0;
    return runloom_iouring_state.pool_base != NULL;
}

unsigned runloom_iouring_pbuf_size(void)
{
    return runloom_iouring_state.pool_buf_size;
}

unsigned runloom_iouring_pbuf_count(void)
{
    return runloom_iouring_state.pool_n;
}

void *runloom_iouring_pbuf_addr(unsigned bid)
{
    runloom_iouring_state_t *s = &runloom_iouring_state;
    if (s->pool_base == NULL) return NULL;
    if (bid >= s->pool_n) return NULL;
    return (char *)s->pool_base + (size_t)bid * s->pool_buf_size;
}

void runloom_iouring_pbuf_return(unsigned bid)
{
    runloom_iouring_state_t *s = &runloom_iouring_state;
    struct runloom_iouring_buf *bufs;
    uint16_t tail, idx;
    if (s->pool_base == NULL) return;
    if (bid >= s->pool_n) return;
    bufs = (struct runloom_iouring_buf *)s->bring_mem;

    runloom_mutex_lock(&s->bring_lock);
    /* The kernel's tail is overlaid on bufs[0].resv.  Load with
     * acquire so we observe consumer-side advancement (the kernel
     * advances head as it takes buffers, but tail is purely producer-
     * side; we're the only producer, so the load is mostly for
     * ordering against the store below). */
    tail = __atomic_load_n(&bufs[0].resv, __ATOMIC_ACQUIRE);
    idx  = tail & s->bring_mask;
    bufs[idx].addr = (uintptr_t)((char *)s->pool_base +
                     (size_t)bid * s->pool_buf_size);
    bufs[idx].len  = s->pool_buf_size;
    bufs[idx].bid  = (uint16_t)bid;
    /* idx==0 means we just overwrote bufs[0].addr/len/bid; the resv
     * field (kernel tail) is the same memory location as our local
     * `tail` variable, but we're about to bump tail by 1 in the store
     * below so the kernel sees the new entry. */
    __atomic_store_n(&bufs[0].resv,
                     (uint16_t)(tail + 1), __ATOMIC_RELEASE);
    runloom_mutex_unlock(&s->bring_lock);
}

/* Submit one SQE.  Caller has filled sqe_template (opcode/fd/addr/len/
 * off); we set user_data to the op pointer and submit without waiting.
 * Returns 0 on success, -1 with errno on failure. */
static int runloom_iouring_submit_sqe(struct io_uring_sqe sqe_template,
                                   runloom_iouring_op_t *op)
{
    runloom_iouring_state_t *s = &runloom_iouring_state;
    unsigned tail, head, idx;
    int n;

    runloom_mutex_lock(&s->sub_lock);

    /* Wait for a free SQE slot.  Ring is 64 deep; in practice we
     * never spin here for the workloads runloom targets, but if every
     * slot is in-flight we must block submitting until the kernel
     * consumes some.  io_uring_enter with submit=0,wait=0 is cheap;
     * but the kernel doesn't progress the SQ ring without an enter,
     * so we just drain CQ inline here to make room. */
    while (1) {
        tail = __atomic_load_n(s->sq_tail, __ATOMIC_RELAXED);
        head = __atomic_load_n(s->sq_head, __ATOMIC_ACQUIRE);
        if ((tail - head) < s->sq_entries) break;
        /* Ring full -- spin-drain CQ to free slots. */
        runloom_mutex_unlock(&s->sub_lock);
        runloom_iouring_drain();
        runloom_mutex_lock(&s->sub_lock);
    }

    idx = tail & s->sq_mask;
    s->sqes[idx] = sqe_template;
    s->sqes[idx].user_data = (uint64_t)(uintptr_t)op;
    s->sq_array[idx] = idx;
    __atomic_store_n(s->sq_tail, tail + 1, __ATOMIC_RELEASE);

    /* Submit without waiting -- the kernel takes the SQE and we
     * return immediately.  The CQE will arrive whenever the op
     * completes; the eventfd notifies the netpoll pump. */
    n = sys_io_uring_submit_eintr(s->ring_fd, 1);   /* retry on EINTR */
    runloom_mutex_unlock(&s->sub_lock);
    if (n < 0) return -1;
    __atomic_add_fetch(&runloom_iouring_inflight_count, 1, __ATOMIC_ACQ_REL);
    return 0;
}

void runloom_iouring_drain(void)
{
    runloom_iouring_state_t *s = &runloom_iouring_state;
    if (s->initialised != 1) return;

    /* Drain the eventfd counter so the kernel re-arms the EPOLLET edge
     * on the next CQE post.  Idempotent: if the eventfd isn't set
     * (manual drain call) the non-blocking read returns -EAGAIN
     * immediately. */
    if (s->eventfd_fd >= 0) {
        uint64_t scratch;
        while (read(s->eventfd_fd, &scratch, sizeof(scratch))
               == (ssize_t)sizeof(scratch))
            { /* drain */ }
    }

    /* Multi-drainer safety: the netpoll pump (whichever hub goes idle)
     * AND the inline drain after submit AND the hub spin-drain path
     * all call this without any external mutex.  On free-threaded
     * Python (3.13t) these can run truly in parallel; under the GIL
     * they only serialise at Python-level boundaries which most of
     * this function doesn't cross.  Without a CAS on cq_head two
     * drainers could load the same head, process the same CQE
     * (double-wake the fiber, free a cancel record twice), and
     * each blindly store head+1 -- a single CQE consumed twice with
     * inflight decremented twice.  Pre-fix this manifested as
     * gradual M:N hangs (the spurious wake left the g in a "ready
     * twice" state that drove an unrelated parker into use-after-
     * free).  The CAS turns the head advance into a per-CQE claim:
     * exactly one drainer succeeds for each CQE; the loser re-loads
     * and retries. */
    /* Pull any kernel-side overflowed completions back into the ring before
     * draining -- multishot recv can post more CQEs than the ring holds, and a
     * dropped completion permanently strands the parked fiber. */
    runloom_iouring_flush_cq_overflow(s->sq_flags, s->ring_fd);
    for (;;) {
        unsigned head = __atomic_load_n(s->cq_head, __ATOMIC_ACQUIRE);
        unsigned ct   = __atomic_load_n(s->cq_tail, __ATOMIC_ACQUIRE);
        struct io_uring_cqe *cqe;
        runloom_iouring_op_t *op;
        int32_t  res;
        uint32_t flags;
        int      type;
        int      cqe_final;
        unsigned expected;
        if (head == ct) return;
        cqe   = &s->cqes[head & s->cq_mask];
        res   = cqe->res;
        flags = cqe->flags;
        op    = (runloom_iouring_op_t *)(uintptr_t)cqe->user_data;
        type      = (op != NULL) ? op->type : RUNLOOM_IOURING_OP_SINGLE;
        cqe_final = (type != RUNLOOM_IOURING_OP_MULTISHOT) ||
                    !(flags & IORING_CQE_F_MORE);
        /* Claim this CQE via CAS on cq_head.  On contention, the
         * other drainer wins -- we re-load and try the next CQE.
         * SEQ_CST so multi-thread observers see a coherent head. */
        expected = head;
        if (!__atomic_compare_exchange_n(s->cq_head, &expected, head + 1,
                                         0, __ATOMIC_ACQ_REL,
                                         __ATOMIC_ACQUIRE)) {
            continue;   /* another drainer took it; reload */
        }

        if (op != NULL) {
            switch (type) {
            case RUNLOOM_IOURING_OP_SINGLE:
                /* Publish the result BEFORE the wake handshake so a woken
                 * (or self-recovering) submitter sees it. */
                __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
                if (op->hub != NULL) {
                    /* Hub (M:N) op: the submitter parks via coro_yield.
                     * Claim the completion with an exchange and wake ONLY if
                     * the submitter had already committed to parking
                     * (wait == PARKED).  If it is still INFLIGHT -- e.g. this
                     * is the submitter's own INLINE drain, before it parks --
                     * we must NOT wake: runloom_mn_wake_g would hub-submit a
                     * still-running g and double-resume it (Bug 1).  The
                     * submitter will instead observe DONE on its own CAS and
                     * skip the park entirely. */
                    int prev = __atomic_exchange_n(&op->wait,
                                                   RUNLOOM_IOURING_WAIT_DONE,
                                                   __ATOMIC_ACQ_REL);
                    if (prev == RUNLOOM_IOURING_WAIT_PARKED) {
                        runloom_mn_wake_g(op->hub, op->g);
                    }
                } else if (op->g != NULL) {
                    /* Single-thread sched: race-safe via wake_pending (if the
                     * submitter hasn't parked yet park_safe just decrements). */
                    runloom_sched_wake_safe(op->g);
                }
                break;
            case RUNLOOM_IOURING_OP_MULTISHOT:
                runloom_iouring_ms_on_cqe((runloom_iouring_ms_t *)op, res, flags);
                break;
            case RUNLOOM_IOURING_OP_CANCEL:
                /* Fire-and-forget: the cancel op record is heap-
                 * allocated by ms_close; once the CQE arrives the
                 * record is no longer needed. */
                free(op);
                break;
            }
        }
        if (cqe_final) {
            __atomic_sub_fetch(&runloom_iouring_inflight_count, 1,
                               __ATOMIC_ACQ_REL);
        }
    }
}

/* Common submit-and-wait path for pread/pwrite.  Returns CQE result
 * (>= 0 on success) or -errno on failure. */
