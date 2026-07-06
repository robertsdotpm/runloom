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
        RUNLOOM_RLOCK(&runloom_iouring_state.sub_lock, RUNLOOM_RANK_IOURING_SUB);
        if (runloom_iouring_state.initialised == 0) {
            runloom_iouring_lazy_init();
        }
        RUNLOOM_RUNLOCK(&runloom_iouring_state.sub_lock, RUNLOOM_RANK_IOURING_SUB);
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

    RUNLOOM_RLOCK(&s->bring_lock, RUNLOOM_RANK_IOURING_BRING);
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
    RUNLOOM_RUNLOCK(&s->bring_lock, RUNLOOM_RANK_IOURING_BRING);
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

    RUNLOOM_RLOCK(&s->sub_lock, RUNLOOM_RANK_IOURING_SUB);

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
        RUNLOOM_RUNLOCK(&s->sub_lock, RUNLOOM_RANK_IOURING_SUB);
        runloom_iouring_drain();
        RUNLOOM_RLOCK(&s->sub_lock, RUNLOOM_RANK_IOURING_SUB);
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
    RUNLOOM_RUNLOCK(&s->sub_lock, RUNLOOM_RANK_IOURING_SUB);
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

    /* SINGLE-DRAINER.  Many threads call this without any external mutex -- the
     * netpoll pump on whichever hub goes idle, the inline drain after submit,
     * the SQ-full spin-drain, and the sched/hub idle CQ-overflow flush -- and on
     * free-threaded Python (3.13t) they run truly in parallel.  The previous
     * per-CQE CAS-claim let two threads claim CONSECUTIVE CQEs and process them
     * concurrently.  That is harmless for single-shot ops (each result is
     * independent), but WRONG for MULTISHOT recv, whose CQEs carry a byte
     * STREAM: an out-of-order ms_on_cqe append reorders the per-handle ready
     * queue and corrupts the stream (right length, wrong bytes -- a real
     * many-concurrent-connection bug).  So exactly ONE thread walks the CQ at a
     * time and appends in head order: win `draining` or return.  After clearing
     * the flag the winner RE-CHECKS the CQ -- a completion posted after our last
     * head==ct but before we cleared would be skipped by a concurrent caller
     * that saw draining==1 and returned, so re-acquire and loop to catch it.
     * (Exactly-once is now structural: a single walker advances head once per
     * CQE, so the old double-wake / double-free / double-decrement races are
     * gone without the CAS.) */
    if (__atomic_exchange_n(&s->draining, 1, __ATOMIC_ACQ_REL) != 0)
        return;   /* another thread owns the drain; it processes every CQE */

    for (;;) {
        /* Pull any kernel-side overflowed completions back into the ring first
         * -- multishot recv can post more CQEs than the ring holds, and a
         * dropped completion would permanently strand the parked fiber. */
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
            if (head == ct) break;
            cqe   = &s->cqes[head & s->cq_mask];
            res   = cqe->res;
            flags = cqe->flags;
            op    = (runloom_iouring_op_t *)(uintptr_t)cqe->user_data;
            type      = (op != NULL) ? op->type : RUNLOOM_IOURING_OP_SINGLE;
            cqe_final = (type != RUNLOOM_IOURING_OP_MULTISHOT) ||
                        !(flags & IORING_CQE_F_MORE);
            /* Single walker: consume the slot by advancing head in order (no
             * CAS).  Done after copying res/flags/op out, so the kernel may
             * reuse the slot while we process from the locals. */
            __atomic_store_n(s->cq_head, head + 1, __ATOMIC_RELEASE);

            if (op != NULL) {
                switch (type) {
                case RUNLOOM_IOURING_OP_SINGLE:
                    /* Publish the result BEFORE the wake handshake so a woken
                     * (or self-recovering) submitter sees it. */
                    __atomic_store_n(&op->result, res, __ATOMIC_RELEASE);
                    if (op->hub != NULL) {
                        /* Hub (M:N) op: wake ONLY if the submitter already
                         * committed to parking (wait == PARKED).  If still
                         * INFLIGHT -- e.g. the submitter's own inline drain
                         * before it parks -- do NOT wake (it observes DONE on
                         * its own CAS and skips the park; waking a still-running
                         * g would double-resume it). */
                        int prev = __atomic_exchange_n(&op->wait,
                                                       RUNLOOM_IOURING_WAIT_DONE,
                                                       __ATOMIC_ACQ_REL);
                        RUNLOOM_IOU_NOTE(prev, RUNLOOM_IOURING_WAIT_DONE);
                        if (prev == RUNLOOM_IOURING_WAIT_PARKED) {
                            runloom_mn_wake_g(op->hub, op->g);
                        }
                    } else if (op->g != NULL) {
                        /* Single-thread sched: race-safe via wake_pending. */
                        runloom_sched_wake_safe(op->g);
                    }
                    break;
                case RUNLOOM_IOURING_OP_MULTISHOT:
                    runloom_iouring_ms_on_cqe((runloom_iouring_ms_t *)op, res, flags);
                    break;
                case RUNLOOM_IOURING_OP_CANCEL:
                    /* Fire-and-forget: the cancel op record came from the CANCEL
                     * pool (or heap fallback); once the CQE arrives it is no
                     * longer needed.  rl_cancel_op_free routes it back correctly. */
                    rl_cancel_op_free(op);
                    break;
                }
            }
            if (cqe_final) {
                __atomic_sub_fetch(&runloom_iouring_inflight_count, 1,
                                   __ATOMIC_ACQ_REL);
            }
        }
        __atomic_store_n(&s->draining, 0, __ATOMIC_RELEASE);
        /* Re-check (see header): catch a completion that raced in just as we
         * released the drain. */
        if (__atomic_load_n(s->cq_head, __ATOMIC_ACQUIRE)
                == __atomic_load_n(s->cq_tail, __ATOMIC_ACQUIRE))
            break;
        if (__atomic_exchange_n(&s->draining, 1, __ATOMIC_ACQ_REL) != 0)
            break;   /* another thread became the drainer; it owns the rest */
    }
}

/* Common submit-and-wait path for pread/pwrite.  Returns CQE result
 * (>= 0 on success) or -errno on failure. */
