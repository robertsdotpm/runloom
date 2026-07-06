/*
 * hub_fanout.pml -- the SPANNING model of hub_submit's three-way waker-route
 * fanout (mn_sched_mn_api.c.inc:199-239).  The individual routes are modelled
 * elsewhere (hub_submit.pml = the wake_g dedup, netpoll_pump_kick.pml = the pump
 * Dekker), but NO model proved the COMPOSITION: that whichever wait state the
 * target hub is in, at least one route reaches it, so a cross-hub submit is
 * never lost.  This is that model.
 *
 * A submitted g is pushed to sub_head, then the submitter fans out:
 *   - ring_waiting hint set  -> write the io_uring loop wake eventfd;
 *   - idle_waiting hint set  -> signal the per-hub idle_cond;
 *   - ALWAYS                 -> kick the shared netpoll pump eventfd.
 * The target hub is in exactly ONE wait mode, each woken by a DIFFERENT route:
 *   RUNNING  -- busy; drains sub_head on its next loop (no wake needed);
 *   IDLE     -- blocked on idle_cond  -> needs the idle_cond signal;
 *   RING     -- blocked in io_uring   -> needs the ring eventfd;
 *   PUMP     -- blocked in epoll_wait -> needs the pump eventfd.
 * Before committing to a wait, a hub ANNOUNCES its mode (idle_waiting/ring_waiting
 * store + SEQ_CST fence) and RE-CHECKS sub_head -- the Dekker handshake that pairs
 * with the submitter's push-then-check-hint, so a submit racing the announce
 * either lands in sub_head (the hub sees it on re-check) or observes the hint
 * (the submitter signals).  epoll/idle hubs re-drain sub_head before blocking too.
 *
 * PROVEN: the pushed work is ALWAYS processed -- no wait mode loses the submit.
 *
 * Negative controls (each proves a route is load-bearing, not redundant):
 *   -DBUG_NO_PUMP     : drop the UNCONDITIONAL pump kick -> a PUMP-mode hub (no
 *                       idle/ring hint) is stranded; the pump is the backstop.
 *   -DBUG_NO_IDLE_SIG : never fire the idle_cond signal -> an IDLE-mode hub is
 *                       stranded (the pump kick does NOT reach a condvar wait) --
 *                       so idle_cond is REQUIRED, not just a latency optimization.
 */

mtype = { RUNNING, IDLE, RING, PUMP };

bit work      = 0;   /* the submit pushed to the target hub's sub_head */
bit idle_wait = 0;   /* hub announced it is about to block on idle_cond */
bit ring_wait = 0;   /* hub announced it is about to block in io_uring */
bit idle_sig  = 0;   /* idle_cond signal delivered */
bit ring_sig  = 0;   /* io_uring loop wake eventfd byte */
bit pump_kick = 0;   /* shared netpoll pump eventfd byte */
bit processed = 0;   /* the hub drained the work -- the no-lost-wake goal */

/* The submitter: push, then fan out per the announced hints + the unconditional
 * pump kick (mn_sched_mn_api.c.inc:207-239). */
proctype submitter()
{
    work = 1;                               /* push to sub_head (RELEASE) */
    /* SEQ_CST fence pairs with each hub announce store + re-check. */
    if
    :: ring_wait -> ring_sig = 1            /* ring hint set -> ring eventfd */
    :: else      -> skip
    fi;
    if
#ifndef BUG_NO_IDLE_SIG
    :: idle_wait -> idle_sig = 1            /* idle hint set -> idle_cond signal */
#endif
    :: else      -> skip
    fi;
#ifndef BUG_NO_PUMP
    pump_kick = 1                            /* ALWAYS kick the shared pump */
#endif
}

/* The target hub: pick one wait mode, do its Dekker announce + re-check, then
 * block on its route until woken, then drain. */
proctype hub()
{
    mtype mode;
    if
    :: mode = RUNNING
    :: mode = IDLE
    :: mode = RING
    :: mode = PUMP
    fi;

    if
    :: mode == RUNNING ->
        /* busy hub: drains sub_head on its next loop -- eventually sees work. */
        (work) -> work = 0; processed = 1

    :: mode == IDLE ->
        idle_wait = 1;                       /* announce, then SEQ_CST + re-check */
        if
        :: work -> work = 0; processed = 1   /* re-check found it -> don't block */
        :: else ->
            (idle_sig) -> idle_sig = 0;      /* blocked; idle_cond signal wakes us */
            work = 0; processed = 1
        fi

    :: mode == RING ->
        ring_wait = 1;
        if
        :: work -> work = 0; processed = 1
        :: else ->
            (ring_sig) -> ring_sig = 0;      /* ring eventfd wakes us */
            work = 0; processed = 1
        fi

    :: mode == PUMP ->
        /* epoll_wait: no idle/ring hint; drains sub_head before blocking, else
         * the unconditional pump kick wakes it. */
        if
        :: work -> work = 0; processed = 1
        :: else ->
            (pump_kick) -> pump_kick = 0;
            work = 0; processed = 1
        fi
    fi
}

init {
    atomic { run submitter(); run hub(); }
    /* Both finished => the submit was processed by whatever route matched the
     * hub's wait mode.  Under -DBUG_NO_PUMP a PUMP-mode hub blocks forever
     * instead -> invalid end state (Spin's -a finds it). */
    (_nr_pr == 1) -> assert(processed)
}
