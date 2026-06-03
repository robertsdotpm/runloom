/*
 * netpoll_rearm.pml -- Promela model of the OTHER half of netpoll's lost-wake
 * guard (netpoll_commit.pml models the parker-claim commit; this models the
 * arming discipline): the LEVEL-triggered + EPOLLONESHOT re-arm in
 * runloom_netpoll_register, working with the per-fd pending-wake bitmap.
 *
 * THE NOT-YET-LINKED WINDOW.  An fd can become ready while no parker is linked
 * for it (the g unlinked on its last wake and has not re-linked, or has not
 * reached wait_fd yet).  A pump that processes such a delivery finds no parker
 * and stashes the readiness in the per-fd pending-wake bitmap; the next
 * wait_fd consumes it before parking.  But the bitmap ALONE does not close the
 * window: a pump can be preempted between "found no parker" and the lock-free
 * runloom_fd_pending_wake_set (netpoll.c:2185-2195), letting the g link, consume
 * the (still-empty) bitmap twice, commit, and park BEFORE the bit is set.
 *
 * WHAT ACTUALLY CLOSES IT (the documented T1.5 fix, netpoll.c:1158-1207):
 * arm LEVEL-triggered + EPOLLONESHOT, RE-ARMED via EPOLL_CTL_MOD on EVERY
 * park, strictly AFTER linking the parker (link at 1803, register at 1845).
 * Because MOD is level-triggered it re-reports a still-ready fd, queueing a
 * FRESH delivery -- and that delivery, generated after the link, finds the
 * linked parker and wakes it.  EPOLLONESHOT means the prior arm delivered once
 * and disarmed, so no delivery is in flight before the re-arm: the only
 * delivery this cycle is the post-link one.  Hence the not-yet-linked window
 * is unreachable.
 *
 * PROVEN (one parking g + one pump, fd ready):
 *   NO LOST WAKE -- the g always becomes runnable.  Under LT+ONESHOT the
 *   re-arm (post-link) delivers to the pump, which finds the linked parker;
 *   the bitmap is provably never even needed.  A lost wake = the g stuck at
 *   its park = a Spin invalid end state.
 *
 * Negative control -DBUG_EDGE_TRIGGERED models the OLD scheme (item 2 of the
 * lost-wake history: EPOLLET, registered once, never re-armed).  register is a
 * cached no-op and an already-ready fd is NOT re-reported, so a pre-link edge
 * the pump dropped is gone for good.  Spin finds the lost wake: pump consumes
 * the lone edge before the link, is preempted, the g links + double-consumes
 * the empty bitmap + parks, THEN the pump sets the bit -- too late, and no
 * re-arm delivery ever comes.  (Matches "EPOLLET+ONESHOT+re-arm hung 96/96;
 * only LEVEL fixed it".)  This is exactly why the bitmap needs the LT re-arm.
 */

#define ARMED  0
#define PARKED 1
#define WOKEN  2

bit  lock       = 0;
bit  linked     = 0;      /* parker linked into by_fd[fd]                  */
byte commit     = ARMED;  /* parker->commit                                */
bit  pending    = 0;      /* per-fd pending-wake bitmap bit                */
bit  g_runnable = 0;      /* g will run again (woken, or never parked)     */
bit  g_parked   = 0;      /* g committed to PARKED and yielded             */
bit  g_done     = 0;      /* g returned from wait_fd                       */

bit  fd_ready   = 1;      /* level condition: data present this cycle      */
byte deliv      = 0;      /* epoll deliveries queued to the pump           */
bit  arm        = 0;      /* epoll arm state (EPOLLONESHOT)                 */

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

active proctype waitfd()
{
    byte prev;
    bit got;

    LOCK; linked = 1; UNLOCK;                  /* link the parker (1803) */

    atomic { got = pending; pending = 0; }     /* consume #1 (1812) */
    if
    :: got -> LOCK; linked = 0; UNLOCK; g_runnable = 1; g_done = 1;
    :: else ->
#ifndef BUG_EDGE_TRIGGERED
        /* register = EPOLL_CTL_MOD, LEVEL-triggered + ONESHOT: re-arm AND, the
         * fd being ready, queue a FRESH delivery -- strictly after the link. */
        atomic { arm = 1; if :: fd_ready -> deliv++ :: else -> skip; fi; }
#else
        /* OLD scheme: EPOLLET, registered once, never re-armed -> cached
         * no-op; an already-ready fd is NOT re-reported. */
        skip;
#endif
        atomic { got = pending; pending = 0; }  /* consume #2 (1860) */
        if
        :: got -> LOCK; linked = 0; UNLOCK; g_runnable = 1; g_done = 1;
        :: else ->
            atomic {                            /* commit CAS ARMED->PARKED (1880) */
                prev = commit;
                if :: commit == ARMED -> commit = PARKED;
                   :: else            -> skip;
                fi;
            }
            if
            :: prev == ARMED -> g_parked = 1; (g_runnable); g_done = 1;  /* park */
            :: else          -> LOCK; linked = 0; UNLOCK;               /* claimed */
                                g_runnable = 1; g_done = 1;
            fi;
        fi;
    fi;
}

active proctype pump()
{
    byte prior;
#ifdef BUG_EDGE_TRIGGERED
    /* EPOLLET: the fd was armed once by a prior wait; a not-ready->ready edge
     * has fired and is pending.  EPOLLET will not refire it. */
    atomic { arm = 1; deliv = 1; }
#endif
    do
    :: atomic { (deliv > 0) -> deliv--; arm = 0; }   /* ONESHOT: delivery disarms */
       /* runloom_pump_dispatch_event: walk by_fd under pool->lock */
       LOCK;
       if
       :: linked ->
           atomic {                              /* runloom_pump_claim */
               prior = commit;
               if :: commit != WOKEN -> commit = WOKEN;
                  :: else            -> skip;
               fi;
           }
           if
           :: prior == WOKEN -> skip;
           :: else -> linked = 0;
                      if :: prior == PARKED -> g_runnable = 1;   /* wake_g */
                         :: else            -> skip;             /* ARMED: aborts */
                      fi;
           fi;
           UNLOCK;
       :: else ->
           UNLOCK;
           pending = 1;          /* no parker -> stash (lock-free, post-unlock) */
       fi;
    :: (g_done) -> break;
    od;
}
