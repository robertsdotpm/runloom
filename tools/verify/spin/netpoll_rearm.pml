/*
 * netpoll_rearm.pml -- Promela model of the OTHER half of netpoll's lost-wake
 * guard (netpoll_commit.pml models the parker-claim commit; this models the
 * arming discipline): the LEVEL-triggered REGISTER-PER-DIRECTION-ONCE arm in
 * runloom_netpoll_register (the "LEVEL register-PER-DIRECTION-once" scheme).
 *
 * SHIPPED ARMING (src/runloom_c/netpoll_register.c.inc, runloom_netpoll_register).
 * Each fd is ADDed LEVEL-triggered (EPOLLIN|EPOLLRDHUP / EPOLLOUT, NO EPOLLET,
 * NO EPOLLONESHOT) ONCE per direction, and a re-park whose direction is already
 * armed SKIPS the epoll_ctl entirely (0 syscalls on the recv-after-recv hot
 * path -- the arm mask runloom_fd_armed doubles as registration state).  The
 * registration is PERSISTENT: it is not disarmed by a delivery, and it is never
 * re-MOD'd on a re-park.  OUT is armed only when a WRITE waiter exists (the
 * always-writable OUT would otherwise level-busy-loop the pump); IN never
 * busy-loops (a not-readable socket produces no IN event), so IN is safe
 * register-once.
 *
 * THE NOT-YET-LINKED WINDOW.  An fd can become ready while no parker is linked
 * for it (the g unlinked on its last wake and has not re-linked, or has not
 * reached wait_fd yet).  A pump that processes such a delivery finds no parker;
 * it stashes the readiness in the per-fd pending-wake bitmap (a belt-and-braces
 * backstop), but the bitmap is NOT what closes the window here.
 *
 * WHAT ACTUALLY CLOSES IT (the LEVEL register-once contract):
 * because the registration PERSISTS and is LEVEL-triggered, the kernel
 * RE-REPORTS a still-ready fd on EVERY epoll_wait.  The pump loops on
 * epoll_wait; so long as the fd stays ready and registered, each loop produces
 * a fresh delivery.  An early delivery that found no parker is therefore
 * harmless: the NEXT epoll_wait re-reports the same still-ready fd, and that
 * later delivery -- arriving after the g has linked -- finds the linked parker
 * and wakes it.  No re-arm syscall is needed (register is a cached no-op on the
 * re-park), and there is NO pending-bitmap dependency: LEVEL persistence alone
 * makes a late-linking parker un-droppable.
 *
 * Faithful to wait_fd ordering (runloom_netpoll_wait_fd): link (parker_link) ->
 * pending_wake_consume #1 -> runloom_netpoll_register (register-once, skip if
 * already armed) -> pending_wake_consume #2 -> commit CAS ARMED->PARKED.
 *
 * PROVEN (one parking g + one pump, fd ready):
 *   NO LOST WAKE -- the g always becomes runnable.  Under register-once LEVEL,
 *   the persistent registration re-reports the still-ready fd on every
 *   epoll_wait, so a delivery generated AFTER the link finds the linked parker.
 *   The bitmap is provably never even needed (the model never relies on it).
 *   A lost wake = the g stuck at its park = a Spin invalid end state.
 *
 * Negative control -DBUG_EDGE_TRIGGERED models the OLD scheme (item 2 of the
 * lost-wake history: EPOLLET, registered ONCE, never re-armed -- the
 * register.c.inc header's "Do NOT restore an EPOLLET register-once arm").
 * register is the SAME cached register-once no-op, but EPOLLET is
 * EDGE-triggered: a still-ready fd is reported only on the not-ready->ready
 * EDGE, NOT on every epoll_wait.  Once a pump drains that lone edge finding no
 * parker, it never refires.  Spin finds the lost wake: the pump consumes the
 * pre-link edge, the g then links + parks, and no further delivery ever comes
 * because LEVEL re-report is exactly what EPOLLET removes.  (Matches the
 * recorded "EPOLLET register-once hung; only LEVEL fixed it".)
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
/* registered: the LEVEL register-PER-DIRECTION-once arm.  The arm mask
 * (runloom_fd_armed) is PERSISTENT -- ADDed once and NOT disarmed by a delivery
 * (NO EPOLLONESHOT) and NOT re-MOD'd on a re-park.  Starts 1 here: this fd was
 * registered LEVEL on a PRIOR park and the registration survived the prior
 * wake, so this park takes register's already-armed zero-syscall SKIP. */
bit  registered = 1;

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

active proctype waitfd()
{
    byte prev;
    bit got;

    LOCK; linked = 1; UNLOCK;                  /* runloom_parker_link */

    atomic { got = pending; pending = 0; }     /* pending_wake_consume #1 */
    if
    :: got -> LOCK; linked = 0; UNLOCK; g_runnable = 1; g_done = 1;
    :: else ->
        /* runloom_netpoll_register: LEVEL register-PER-DIRECTION-once.  This
         * direction is ALREADY armed (registered == 1) from a prior park, so
         * register takes its zero-syscall already-armed SKIP -- it issues NO
         * epoll_ctl and queues NO delivery.  (Under either trigger mode the
         * re-park register is a cached no-op; the difference is what the
         * PERSISTENT registration does on the pump's epoll_wait, below.) */
        skip;
        atomic { got = pending; pending = 0; }  /* pending_wake_consume #2 */
        if
        :: got -> LOCK; linked = 0; UNLOCK; g_runnable = 1; g_done = 1;
        :: else ->
            atomic {                            /* commit CAS ARMED->PARKED */
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

    /* The fd is ready and was registered LEVEL on a prior park.  Under the
     * SHIPPED scheme the persistent LEVEL registration re-reports the
     * still-ready fd on EVERY epoll_wait, so the first epoll_wait already has a
     * delivery queued -- and so will every subsequent one while the fd stays
     * ready and registered.  Under -DBUG_EDGE_TRIGGERED the SAME register-once
     * arm exists, but EPOLLET reports only the not-ready->ready EDGE: one
     * delivery, and once drained it never refires. */
    atomic { deliv = 1; }

    do
    :: atomic { (deliv > 0) -> deliv-- }      /* epoll_wait returns a delivery */
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
#ifndef BUG_EDGE_TRIGGERED
           /* LEVEL register-once: the registration PERSISTS and re-reports the
            * still-ready fd on the NEXT epoll_wait.  So a delivery that found no
            * parker is re-generated by the kernel on the following wait -- this
            * is exactly what makes a late-linking parker un-droppable, no re-arm
            * syscall and no pending-bitmap dependency.  Modelled as the
            * persistent LEVEL arm re-queueing while the fd stays ready. */
           atomic { if :: (fd_ready && registered) -> deliv++ :: else -> skip fi }
#else
           /* EPOLLET register-once: the registration also persists, but
            * EDGE-triggered means a still-ready fd is NOT re-reported on the
            * next epoll_wait -- only a fresh not-ready->ready edge would, and
            * the fd never went un-ready.  The lone edge is gone; nothing
            * re-queues.  This is the dropped edge that never refires. */
           skip;
#endif
       fi;
    :: (g_done) -> break;
    od;
}
