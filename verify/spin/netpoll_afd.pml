/*
 * netpoll_afd.pml -- Promela model of the IOCP+AFD poll-context LIFETIME in
 * runloom's Windows netpoll backend.  Unlike epoll/kqueue (whose hazard is a
 * lost wake from a mis-armed fd), the AFD backend's hazard is a USE-AFTER-FREE
 * / double-free of the per-poll heap context, because completions and the
 * cross-thread pump-wake share ONE IOCP queue.
 *
 * Source: src/runloom_c/netpoll_iocp.c (submit :367-420, wait :422-489,
 * wake :509-513).
 *
 * THE CONTEXT (runloom_poll_ctx_t) holds the OVERLAPPED (first field), the
 * IO_STATUS_BLOCK and the AFD_POLL_INFO.  Lifecycle:
 *   submit():  calloc one ctx, then NtDeviceIoControlFile(IOCTL_AFD_POLL).
 *     - STATUS_SUCCESS or STATUS_PENDING -> the kernel will deliver EXACTLY
 *       ONE IOCP completion carrying this ctx (we did NOT pass
 *       FILE_SKIP_COMPLETION_PORT_ON_SUCCESS, so even sync success posts one).
 *       The ctx MUST stay alive until that completion is consumed.
 *     - hard error (neither SUCCESS nor PENDING) -> NO completion is queued;
 *       submit free()s the ctx immediately (netpoll_iocp.c:418).
 *   wait():  GetQueuedCompletionStatus returns an OVERLAPPED pointer.
 *     - ov == NULL  -> a pump-wake posted by runloom_iocp_wake()
 *       (PostQueuedCompletionStatus with a NULL overlapped, the IOCP analogue
 *       of the epoll eventfd).  Recognised BEFORE CONTAINING_RECORD and
 *       returned as "woke, nothing ready" -- it frees nothing.
 *     - ov != NULL -> CONTAINING_RECORD back to the ctx, read events,
 *       free(ctx) (netpoll_iocp.c:481/487).  Exactly one consume per ctx.
 *
 * PROVEN (one submit + an optional shared-IOCP wake + one pump):
 *   - NO USE-AFTER-FREE: a completion is never consumed against a freed ctx.
 *   - FREED EXACTLY ONCE: no double free, and (end state) no leak -- every
 *     allocated ctx is freed.
 *   - the NULL-overlapped wake is NEVER dereferenced/free()d as a ctx.
 *
 * Negative controls (each MUST make Spin report an assertion violation):
 *   -DBUG_FREE_ON_PENDING   submit free()s the ctx on STATUS_PENDING too (as
 *                           if pending were an error) -- but the kernel still
 *                           delivers the completion -> wait consumes a freed
 *                           ctx -> use-after-free.
 *   -DBUG_WAKE_AS_COMPLETION  wait omits the ov==NULL check -> the pump-wake
 *                           is CONTAINING_RECORD'd and free()d as if it were a
 *                           ctx -> wild free of a non-context.
 */

#define NONE  0
#define LIVE  1
#define FREED 2

byte ctx_state   = NONE;   /* the single poll context's lifecycle           */
bit  submit_done = 0;
bit  waker_done  = 0;
bit  uaf         = 0;      /* set if a completion is consumed after free     */
bit  wildfree    = 0;      /* set if a NULL-ov wake is treated as a ctx      */

/* The IOCP completion queue.  Each entry's value is `is_real`:
 *   1 = a real AFD poll completion (carries the ctx via its OVERLAPPED)
 *   0 = a pump-wake (NULL OVERLAPPED), posted by runloom_iocp_wake() */
chan iocp = [2] of { bit };

active proctype submit()
{
    ctx_state = LIVE;                 /* calloc(1, sizeof(ctx)) */
    if
    :: /* NtDeviceIoControlFile -> STATUS_SUCCESS or STATUS_PENDING:
        * the kernel will post EXACTLY ONE completion for this ctx. */
       iocp ! 1;
#ifdef BUG_FREE_ON_PENDING
       /* WRONG: free the ctx now, even though a completion is still queued. */
       ctx_state = FREED;
#endif
    :: /* hard error: neither SUCCESS nor PENDING -> no completion queued. */
       ctx_state = FREED;             /* netpoll_iocp.c:418 free(ctx) */
    fi;
    submit_done = 1;
}

/* Models the cross-thread pump-wake sharing the IOCP (0 or 1 wake this run). */
active proctype waker()
{
    if
    :: iocp ! 0;                      /* PostQueuedCompletionStatus(.., NULL) */
    :: skip;
    fi;
    waker_done = 1;
}

active proctype pump()
{
    bit real;
    do
    :: iocp ? real ->
        if
        :: real == 0 ->
            /* ov == NULL: a pump-wake.  Must be caught BEFORE CONTAINING_RECORD. */
#ifndef BUG_WAKE_AS_COMPLETION
            skip;                     /* return 0; free nothing */
#else
            /* BUG: not disambiguated -> CONTAINING_RECORD(NULL) + free() of a
             * pointer that was never a ctx. */
            wildfree = 1;
#endif
        :: real == 1 ->
            /* real completion: recover ctx, read events, free exactly once. */
            if :: ctx_state == FREED -> uaf = 1;   /* USE-AFTER-FREE */
               :: else               -> skip;
            fi;
            ctx_state = FREED;
        fi;
    :: empty(iocp) && submit_done && waker_done -> break;
    od;

    assert(uaf == 0);
    assert(wildfree == 0);
    assert(ctx_state == FREED);       /* no leak: the ctx was freed */
}
