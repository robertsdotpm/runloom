/* Part of io_uring.c -- #included into that translation unit, not compiled
 * standalone.  See io_uring.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

void runloom_iouring_ms_close(runloom_iouring_ms_t *h)
{
    runloom_iouring_op_t *cancel_op;
    struct io_uring_sqe sqe;
    int armed_snapshot;

    if (h == NULL) return;

    runloom_mutex_lock(&h->lock);
    h->closing = 1;
    armed_snapshot = h->armed;
    runloom_mutex_unlock(&h->lock);

    if (!armed_snapshot) {
        /* Multishot already terminated; safe to free immediately. */
        ms_entry_t *e, *next;
        e = h->ready_head;
        while (e != NULL) {
            next = e->next;
            runloom_iouring_pbuf_return(e->bid);
            free(e);
            e = next;
        }
        if (h->inflight_bid >= 0)
            runloom_iouring_pbuf_return((unsigned)h->inflight_bid);
        runloom_mutex_destroy(&h->lock);
        free(h);
        return;
    }

    /* Fire-and-forget cancel.  drain frees the cancel op record when
     * the cancel CQE arrives; the multishot's final CQE (with
     * F_MORE clear) triggers the handle free in on_cqe. */
    cancel_op = (runloom_iouring_op_t *)calloc(1, sizeof(*cancel_op));
    if (cancel_op == NULL) {
        /* OOM: leak the handle; the kernel will eventually deliver
         * -ECANCELED when the fd closes, and on_cqe with closing=1
         * will free.  Best-effort. */
        return;
    }
    cancel_op->type = RUNLOOM_IOURING_OP_CANCEL;
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = IORING_OP_ASYNC_CANCEL;
    sqe.addr   = (uintptr_t)&h->op;     /* match SQE by its user_data */
    sqe.fd     = -1;
    if (runloom_iouring_submit_sqe(sqe, cancel_op) != 0) {
        free(cancel_op);
    }
}

runloom_iouring_ssize_t runloom_iouring_recv(int fd, void *buf, size_t n, int flags)
{
    struct io_uring_sqe sqe;
    /* Route through the hub's per-thread ring if we're inside a hub
     * and the hub created its ring successfully.  That bypasses the
     * global ring's submission mutex and the legacy spin-drain hub
     * path -- the hub g parks via coro_yield and the shared netpoll
     * pump drains the hub ring's eventfd. */
    runloom_iouring_ring_t *hub_ring = runloom_mn_current_iouring_ring();
    if (hub_ring != NULL) {
        return runloom_iouring_ring_recv(hub_ring, fd, buf, n, flags);
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode    = IORING_OP_RECV;
    sqe.fd        = fd;
    sqe.addr      = (uintptr_t)buf;
    sqe.len       = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;     /* MSG_* recv flags */
    return runloom_iouring_do(sqe);
}

runloom_iouring_ssize_t runloom_iouring_send(int fd, const void *buf, size_t n, int flags)
{
    struct io_uring_sqe sqe;
    runloom_iouring_ring_t *hub_ring = runloom_mn_current_iouring_ring();
    if (hub_ring != NULL) {
        return runloom_iouring_ring_send(hub_ring, fd, buf, n, flags);
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode   = IORING_OP_SEND;
    sqe.fd       = fd;
    sqe.addr     = (uintptr_t)buf;
    sqe.len      = (unsigned)n;
    sqe.msg_flags = (uint32_t)flags;
    return runloom_iouring_do(sqe);
}

/* ============================================================
 * Per-hub rings (Linux 5.18+ SINGLE_ISSUER, 6.1+ DEFER_TASKRUN).
 *
 * One ring per M:N hub.  Hub thread is the SINGLE issuer + drainer of
 * its ring, so no submission mutex is needed and SINGLE_ISSUER + (opt-
 * in) DEFER_TASKRUN are kernel-side correct.  See io_uring.h for the
 * public API contract.
 *
 * Memory layout mirrors the global ring (sq/cq mmaps, sqe array,
 * eventfd) minus the provided-buffer ring -- multishot stays on the
 * global ring so the buffer pool isn't fragmented N ways.
 * ============================================================ */

/* Setup-flag values that older glibc UAPI headers might miss.  Fall
 * back to the kernel-stable values. */
#ifndef IORING_SETUP_SINGLE_ISSUER
#  define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_DEFER_TASKRUN
#  define IORING_SETUP_DEFER_TASKRUN (1U << 13)
#endif

