/*
 * netpoll_kqueue.pml -- Promela model of the kqueue arming discipline in
 * runloom's netpoll backend: the BSD/macOS counterpart of netpoll_rearm.pml
 * (which models the epoll LEVEL+EPOLLONESHOT re-arm).  The shared
 * parker-claim commit and the per-fd pending-wake bitmap are the SAME C code
 * on every backend (netpoll_commit.pml / netpoll_rearm.pml already cover them
 * under epoll semantics); only the ARM differs, so this model isolates the
 * kqueue arm and proves it closes the same not-yet-linked window.
 *
 * Source: src/runloom_c/netpoll_register.c.inc:85-123.
 *
 * KQUEUE ARMING.  runloom arms ONLY the requested direction(s) with
 * EV_ADD | EV_ONESHOT and RE-ISSUES that kevent on EVERY park, strictly
 * AFTER linking the parker into by_fd[fd] (link before arm, as on epoll):
 *   - EV_ADD re-checks readiness NOW.  kqueue reports a level-ready fd at
 *     add time, so a still-ready fd queues a FRESH kevent delivery -- and
 *     that delivery, generated after the link, finds the linked parker and
 *     wakes it.  (The kqueue analogue of epoll's EPOLL_CTL_MOD level
 *     re-report.)
 *   - EV_ONESHOT delivers once, then the kernel auto-DELETES the knote (no
 *     thundering herd, no stale persistent arm).  Because the knote is gone
 *     after a delivery, the next park must EV_ADD again (re-ADD), not merely
 *     re-enable an existing knote.
 *
 * THE NOT-YET-LINKED WINDOW (identical to netpoll_rearm.pml).  An fd can go
 * ready while no parker is linked; a pump processing that delivery finds no
 * parker and stashes the readiness in the per-fd pending bitmap (lock-free,
 * after dropping pool->lock).  The bitmap ALONE does not close the window --
 * the post-unlock store can be delayed past the g's commit+park.  What closes
 * it is the post-link EV_ADD re-checking readiness and queueing a fresh
 * delivery to the now-linked parker.
 *
 * PROVEN (one parking g + one pump, fd ready):
 *   NO LOST WAKE -- the g always becomes runnable.  A lost wake = the g stuck
 *   forever at its park = a Spin invalid end state.
 *
 * Negative controls (each MUST make Spin find the lost wake):
 *   -DBUG_EV_CLEAR  the OLD scheme (netpoll_register.c.inc:87-95): register
 *                   ONCE with EV_CLEAR (edge-triggered) and SKIP the kevent on
 *                   re-park.  A pre-link edge the pump drained never refires --
 *                   exactly the epoll EPOLLET trap, in kqueue form.
 *   -DBUG_REENABLE_NOT_READD  kqueue-SPECIFIC (no epoll analogue): re-arm via
 *                   EV_ENABLE (modify an existing knote) instead of EV_ADD.
 *                   EV_ONESHOT auto-DELETED the knote on the prior delivery,
 *                   so ENABLE hits ENOENT and silently arms nothing -> the
 *                   post-link re-check never happens -> no delivery -> lost
 *                   wake even though the fd is level-ready.
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
byte deliv      = 0;      /* kevent deliveries queued to the pump          */
/* kq_armed: is the knote currently present/enabled in the kqueue?  Starts 0:
 * the previous park's EV_ONESHOT delivery auto-DELETED the knote, so this park
 * is re-arming from "no knote" (the re-park case the bug controls hinge on). */
bit  kq_armed   = 0;

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

active proctype waitfd()
{
    byte prev;
    bit got;

    LOCK; linked = 1; UNLOCK;                  /* link the parker (1803) */

    atomic { got = pending; pending = 0; }     /* consume #1 */
    if
    :: got -> LOCK; linked = 0; UNLOCK; g_runnable = 1; g_done = 1;
    :: else ->
#ifdef BUG_EV_CLEAR
        /* register-once + EV_CLEAR: re-park issues NO kevent (cached no-op);
         * EV_CLEAR is edge-triggered so an already-ready fd is NOT re-reported. */
        skip;
#elif defined(BUG_REENABLE_NOT_READD)
        /* re-arm via EV_ENABLE: arms only if the knote still exists.  The prior
         * ONESHOT delivery auto-deleted it (kq_armed == 0) -> ENOENT, no arm. */
        atomic {
            if :: kq_armed -> if :: fd_ready -> deliv++ :: else -> skip fi
               :: else     -> skip                       /* ENOENT: nothing armed */
            fi
        }
#else
        /* EV_ADD | EV_ONESHOT: re-ADD the knote and re-check readiness NOW --
         * a still-ready fd queues a fresh delivery, strictly after the link. */
        atomic { kq_armed = 1; if :: fd_ready -> deliv++ :: else -> skip fi }
#endif
        atomic { got = pending; pending = 0; }  /* consume #2 */
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
#ifdef BUG_EV_CLEAR
    /* EV_CLEAR registered once: a not-ready->ready edge has already fired and
     * is pending; EV_CLEAR will not refire it once drained. */
    atomic { kq_armed = 1; deliv = 1; }
#endif
    do
    :: atomic { (deliv > 0) -> deliv--; kq_armed = 0; }  /* ONESHOT auto-deletes the knote */
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
