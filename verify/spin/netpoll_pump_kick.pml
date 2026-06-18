/*
 * netpoll_pump_kick.pml -- the cross-hub pump-wake DEDUP (RUNLOOM_WAKE_DEDUP).
 *
 * runloom_netpoll_wake_pump kicks a target hub's wake eventfd to break its idle
 * epoll_wait so it re-drains its cross-hub submission list (sub_head).  Writing
 * the eventfd on EVERY cross-hub wake is amplification (measured ~22k writes/2s
 * under work-stealing); the dedup coalesces them: a waker writes the eventfd
 * ONLY on the 0->1 transition of a per-pool `wake_pending` flag, and the pump
 * clears the flag when it drains the eventfd.
 *
 * The danger is a LOST KICK: a waker that pushes work but COALESCES (sees
 * wake_pending already 1) must not let the hub park without seeing that work.
 * The protocol is Dekker:
 *
 *   waker:  push work (sub_head) ; exchange wake_pending<-1 ;  write iff was 0
 *   hub:    drain eventfd ; clear wake_pending ; RE-CHECK sub_head ; park iff empty
 *
 * The interesting race needs an IN-FLIGHT kick (wake_pending==1, eventfd byte
 * present) that the hub is about to drain -- that is the only state in which a
 * concurrent waker coalesces.  Modelled below as the initial state.
 *
 * PROVEN: the pushed work is always processed -- the hub either re-checks and
 *         finds it, or the waker saw wake_pending==0 (after the clear) and wrote
 *         the eventfd, re-waking the parked hub.  No lost kick.
 *
 * Negative control -DBUG_NO_RECHECK: the hub parks WITHOUT re-checking sub_head
 * after clearing wake_pending.  Then a coalesced waker's work is stranded with
 * no kick coming -> the hub blocks forever -> Spin finds the invalid end state.
 */

bit work      = 0;   /* cross-hub work pushed to the target hub's sub list */
bit pending   = 1;   /* wake_pending: a kick is in flight (about to be drained) */
bit kicked    = 1;   /* the in-flight kick's wake-eventfd byte (level) */
bit processed = 0;   /* the hub processed the work -- the no-lost-kick goal */

/* A waker: push work, exchange wake_pending<-1, write the eventfd ONLY on the
 * 0->1 transition (else a write is already pending -> coalesce / skip). */
proctype waker()
{
    bit old;
    work = 1;                                   /* push work to sub_head */
    atomic { old = pending; pending = 1; }      /* SEQ_CST exchange */
    if
    :: old == 0 -> kicked = 1                   /* 0->1: write the eventfd */
    :: else     -> skip                          /* coalesced: SKIP the write */
    fi
}

/* The hub pump: drain the in-flight kick (epoll_wait returned, read the eventfd),
 * clear wake_pending, then re-check sub_head before parking. */
proctype hub()
{
    (kicked) -> kicked = 0;                       /* read/drain the eventfd */
    pending = 0;                                  /* clear wake_pending after draining */
#ifndef BUG_NO_RECHECK
    if
    :: work -> work = 0; processed = 1           /* RE-CHECK found the pushed work */
    :: else ->                                    /* genuinely empty -> park */
        (kicked) -> kicked = 0;                   /* a later kick re-wakes us */
        work = 0; processed = 1
    fi
#else
    /* BUG: park WITHOUT re-checking sub_head after the clear. */
    (kicked) -> kicked = 0;                       /* blocks forever if coalesced */
    work = 0; processed = 1
#endif
}

init {
    atomic { run waker(); run hub(); }
    /* Once both finish, the pushed work must have been processed (no lost kick).
     * In -DBUG_NO_RECHECK the hub blocks forever instead -> invalid end state. */
    (_nr_pr == 1) -> assert(processed)
}
