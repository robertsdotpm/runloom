/*
 * netpoll_iouring_loop.pml -- Promela model of the io_uring-AS-LOOP backend
 * wake / re-arm / drain protocol (RUNLOOM_IOURING_LOOP=1).  A hub blocks
 * DIRECTLY in its per-hub io_uring (runloom_iouring_loop_wait), which
 * multiplexes every wake source: cross-hub submits (the per-hub wake eventfd,
 * the UD_WAKE multishot poll), op-record completions (Stage-2/3 recv/send
 * CQEs), the shared epoll fd (UD_EPOLL multishot poll), and the EXT_ARG
 * idle_ns timeout.  This models the three lock-free hazards the protocol closes:
 *
 *   (A) THE DEKKER HANDSHAKE (no lost cross-hub wake -- properties S1/S2).
 *       Blocker  (mn_sched_hub_main.c.inc:633-654):
 *           store(ring_waiting=1, RELEASE)         (L636 announce)
 *           __atomic_thread_fence(SEQ_CST)         (L643 the fence under test)
 *           have_sub = (sub_head != NULL)          (L644-646 re-check, locked)
 *           if (!have_sub) loop_wait(idle_ns)      (L647-653 TIMED block)
 *           store(ring_waiting=0, RELEASE)         (L654)
 *       Waker    (mn_sched_mn_api.c.inc:129-155):
 *           push g onto sub_head (RELEASE)         (L129)
 *           if (ring_waiting RELAXED) {            (L151 fast-path hint)
 *             __atomic_thread_fence(SEQ_CST)       (L152 the paired fence)
 *             if (ring_waiting ACQUIRE) loop_wake()(L153-154 write eventfd)
 *           }
 *       Two threads, symmetric store-then-load on (ring_waiting, sub_head) with
 *       a SEQ_CST fence between each side's store and its load.  This is the
 *       classic Dekker / store-buffer litmus: only the dual SEQ_CST fences
 *       forbid BOTH stores sinking past BOTH loads (the StoreLoad reorder) --
 *       the interleaving where the hub reads sub_head==0 AND the waker reads
 *       ring_waiting==0 (the lost wake).  Modeled with an explicit per-thread
 *       store buffer (hub_sb / wkr_sb) that a SEQ_CST fence flushes and an
 *       acq/rel fence (or no fence) does NOT.
 *
 *       The Dekker GUARANTEE proven (S2): at the point the hub commits to block,
 *       NOT (the waker pushed work AND the waker skipped the kick) -- i.e. at
 *       least one side observes the other's store.  Encoded as an assertion
 *       (dekker_bad).  S1 is the consequence: a blocked hub with work pending
 *       always has a kick queued (or saw the work and skipped the block), so it
 *       is woken; modeled as an invalid-end-state (the hub loops until the g is
 *       run, and a buffered push always eventually commits).
 *
 *   (B) MULTISHOT RE-ARM (no wake source goes permanently un-polled -- S3).
 *       io_uring_l_loop.c.inc:257-271: a UD_WAKE/UD_EPOLL CQE that arrives with
 *       IORING_CQE_F_MORE clear (multishot terminated) is re-enqueue_poll'd in
 *       the SAME drain.  A fiber parked on that source (modeled by
 *       infra_consumer) is woken only when the hub drains an infra-poll CQE;
 *       without the re-arm a later edge produces NO CQE, so that fiber's wake is
 *       lost -- it parks forever.  (The hub's idle_ns timeout does NOT save it:
 *       no CQE is ever generated for the de-armed source, so every tick drains
 *       nothing.)
 *
 *   (C) AT-MOST-ONCE OP RESUME (no double-resume of a parked fiber -- S4).
 *       io_uring_l_loop.c.inc:300-316 (drain) + :447-461 (submit):
 *       submitter CAS op->wait INFLIGHT->PARKED commits to yield; drainer
 *       exchange(op->wait, DONE) wakes IFF prev==PARKED.  A drainer that wakes
 *       on prev==INFLIGHT double-resumes a still-running fiber.
 *
 * PROVEN (one ring-blocked hub + one cross-hub waker + one infra-poll source
 * with a parked consumer + one op completion, all racing the block):
 *   NO LOST WAKE  -- dekker_bad is unreachable (assert); the cross-hub g always
 *                    runs and the infra consumer is always woken (both encoded
 *                    as blocking guards -> invalid end state on a real lost
 *                    wake).
 *   AT MOST ONCE  -- op_resumes <= 1 (assert) and the op fiber never both parks
 *                    and aborts.
 *
 * Negative controls (each MUST make pan report nonzero errors):
 *   -DBUG_NO_FENCE      drops BOTH SEQ_CST fences -> the store buffers are not
 *                       flushed before the cross load -> the StoreLoad reorder:
 *                       hub reads stale sub_head==0 and blocks, waker reads stale
 *                       ring_waiting==0 and skips loop_wake -> dekker_bad fires.
 *   -DBUG_NO_RECHECK    drops the sub_head re-check (hub blocks unconditionally
 *                       after the announce).  Isolates the OTHER half of the
 *                       handshake: even with a correct fence, a push that landed
 *                       before ring_waiting=1 was globally visible loses the wake
 *                       -> dekker_bad (the "announce/submit race").
 *   -DBUG_NO_REARM      a terminal (F_MORE-clear) infra-poll CQE is NOT re-armed
 *                       -> the source's next edge produces no CQE -> the parked
 *                       infra consumer is never woken (invalid end state).
 *   -DBUG_DOUBLE_RESUME the op drainer wakes unconditionally instead of gating on
 *                       prev==PARKED -> a fiber that lost the CAS is resumed ->
 *                       op_resumes==2 / parks-and-aborts (assert fails).
 *   -DBUG_NO_TIMEOUT    removes the idle_ns spontaneous-wake escape.  STANDALONE
 *                       it must still PASS (the handshake + re-arm alone are
 *                       sufficient -- the timeout is a latency backstop, NOT the
 *                       correctness mechanism, property S5).  Paired with
 *                       BUG_NO_FENCE it shows the lost wake is a hard hang.
 */

#define INFLIGHT 0
#define PARKED   1
#define DONE     2

/* ---- shared globals: named after the C fields they mirror ---------------- */
bit  sub_lock      = 0;    /* h->sub_lock                                       */
bit  eventfd_set   = 0;    /* the per-hub wake eventfd counter (loop_wake write)*/
bit  g_ran         = 0;    /* the submitted cross-hub g was drained + ran       */
bit  dekker_bad    = 0;    /* SET iff hub blocks with work pushed + no kick     */

/* The Dekker pair, modeled as a store-buffer litmus.  *_committed = the value
 * other threads can observe in memory; *_sb = a store pending in this thread's
 * private buffer (not yet visible).  A SEQ_CST fence drains the buffer; an
 * acq/rel fence does not.  (ring_waiting + sub_head are RELEASE/ACQUIRE in the
 * C; the StoreLoad reorder is what the SEQ_CST upgrade closes.) */
bit  rw_committed  = 0;    /* h->ring_waiting visible in memory                 */
bit  sh_committed  = 0;    /* h->sub_head     visible in memory                 */
bit  hub_sb        = 0;    /* hub's buffered ring_waiting=1 store                */
bit  wkr_sb        = 0;    /* waker's buffered sub_head push                     */
bit  pushed        = 0;    /* the waker has pushed work (committed or buffered)  */
bit  waker_done    = 0;    /* the waker finished its decision (kick or not)      */

/* infra-poll (UD_WAKE / UD_EPOLL) multishot arm + delivery + parked consumer -*/
bit  poll_armed    = 1;    /* an infra multishot poll is live (armed at start)  */
bit  infra_deliv   = 0;    /* an infra-poll CQE is queued to the drain          */
bit  infra_woken   = 0;    /* the parked infra consumer has been woken          */

/* op-record park/wake FSM (the per-fiber recv/send op) ----------------------*/
byte op_wait       = INFLIGHT;  /* op->wait FSM                                 */
bit  op_parked     = 0;         /* submitter committed PARKED + yielded         */
bit  op_aborted    = 0;         /* submitter saw DONE, skipped the park         */
int  op_resumes    = 0;         /* times the drainer woke the op fiber          */

#define LOCK   d_step { (sub_lock == 0) -> sub_lock = 1 }
#define UNLOCK sub_lock = 0

/* ---- the ring-blocked hub: mn_sched_hub_main.c.inc:633-688 ---------------
 * Modeled as the idle loop: announce, fence, re-check, (maybe) block, wake,
 * de-announce, then UNCONDITIONALLY drain (re-drain sub_head at the loop top +
 * pump(0)+iouring_drain reaping infra CQEs, HUB:655-688) -- repeating until all
 * the wake sources it owns (the cross-hub g and the parked infra consumer) have
 * been serviced.  Each iteration is one trip loop-top -> idle branch -> continue. */
active proctype hub()
{
    bit have_sub;

    do
    :: (g_ran == 0 || infra_woken == 0) ->
        /* L636: store(ring_waiting=1, RELEASE) -> this thread's store buffer. */
        hub_sb = 1;
#ifndef BUG_NO_FENCE
        /* L643: SEQ_CST fence -> flush, so ring_waiting=1 is visible BEFORE the
         * sub_head load below. */
        atomic { rw_committed = 1; hub_sb = 0; }
#else
        /* BUG: no fence -> the announce store stays buffered across the load. */
        skip;
#endif

#ifndef BUG_NO_RECHECK
        /* L644-646: have_sub = (sub_head != NULL) under sub_lock. */
        LOCK; have_sub = sh_committed; UNLOCK;
#else
        /* BUG: drop the re-check -> block unconditionally after the announce. */
        have_sub = 0;
#endif

        if
        :: have_sub != 0 ->
            /* saw the work: skip the wait, fall to the unconditional drain. */
            skip;
        :: else ->
            /* About to commit to the TIMED block.  Dekker safety check (S1/S2):
             * if work was pushed and the waker has finished deciding without
             * queuing a kick, this is the lost-wake state the handshake must
             * make unreachable.  (If the waker has not decided yet, the kick may
             * still arrive -- captured by the block's eventfd branch.) */
            atomic {
                if
                :: pushed && (g_ran == 0) && waker_done && (eventfd_set == 0) ->
                    dekker_bad = 1;
                :: else -> skip;
                fi;
            }
            assert(dekker_bad == 0);

            /* L647-653: block in io_uring_enter.  Escapes: the eventfd kick
             * (UD_WAKE CQE), an infra-poll CQE, or the idle_ns timeout.  The
             * timeout (S5) is the "re-check the source of truth on timeout"
             * backstop (HUB:668-670): it lets the hub leave the block and
             * re-drain ONLY when there is pending state a re-check could
             * surface -- a queued eventfd byte, a pending CQE, or a buffered/
             * committed-but-undrained push.  Once everything has quiesced (no
             * such pending state) a timeout respin would observe the SAME state
             * and just re-block -- so it is NOT an escape, and the hub is
             * genuinely stuck if a wake was truly lost (the deadlock the bug
             * controls expose).  Modeling the timeout as an UNCONDITIONAL escape
             * would mask every lost wake as a livelock the checker can't see. */
            do
            :: atomic { eventfd_set == 1 -> eventfd_set = 0; break }
            :: atomic { infra_deliv == 1 -> break }       /* infra CQE wakes us */
#ifndef BUG_NO_TIMEOUT
            :: atomic { (eventfd_set || infra_deliv || wkr_sb ||
                         (sh_committed && g_ran == 0)) -> break }   /* idle_ns re-check */
#endif
            od;
        fi;

        /* L654: store(ring_waiting=0, RELEASE) -- committed at once. */
        rw_committed = 0; hub_sb = 0;

        /* L655-688: UNCONDITIONAL post-wait drain.  (i) iouring_drain reaps any
         * infra-poll CQE and wakes its parked consumer.  (ii) the loop top
         * re-drains sub_head (HUB:301-326).  A buffered waker push always
         * eventually commits (StoreLoad delays, never loses), so a re-drain on a
         * later iteration sees it -- this is why the loop, not a one-shot drain. */
        atomic { if :: infra_deliv == 1 -> infra_deliv = 0; infra_woken = 1;
                    :: else -> skip fi }
        LOCK;
        if
        :: sh_committed != 0 -> g_ran = 1; sh_committed = 0;  /* drain empties sub_head */
        :: else -> skip;
        fi;
        UNLOCK;
    :: (g_ran == 1 && infra_woken == 1) -> break;
    od;
}

/* ---- the cross-hub waker: mn_sched_mn_api.c.inc:129-155 ------------------ */
active proctype waker()
{
    bit rw;

    /* L129: push g onto sub_head, RELEASE store -> this thread's store buffer. */
    LOCK; UNLOCK;
    pushed = 1;
    wkr_sb = 1;

#ifndef BUG_NO_FENCE
    /* L152: SEQ_CST fence -> flush, so the push is visible BEFORE the
     * ring_waiting load. */
    atomic { sh_committed = 1; wkr_sb = 0; }
#else
    /* BUG: no fence -> the push stays buffered across the load. */
    skip;
#endif

    /* L151/153: load ring_waiting (the globally-visible value).  A buffered hub
     * announce (BUG_NO_FENCE) reads as stale 0. */
    rw = rw_committed;
    if
    :: rw != 0 -> eventfd_set = 1;        /* L154: loop_wake -> write eventfd */
    :: else    -> skip;                   /* hint says not parked: skip kick */
    fi;
    waker_done = 1;

    /* The buffered push eventually commits (StoreLoad: delayed, never lost). */
    atomic { if :: wkr_sb -> sh_committed = 1; wkr_sb = 0; :: else -> skip fi }
}

/* ---- the infra-poll source + drain re-arm: io_uring_l_loop.c.inc:257-271 -
 * The UD_WAKE/UD_EPOLL multishot poll de-arms on a terminal (F_MORE-clear) CQE;
 * the drain re-arms it so a later edge is still delivered.  This proc generates
 * the source's two edges; infra_consumer is the fiber parked on the source. */
active proctype infra_poll()
{
    /* first edge fires while armed; the hub drains its terminal CQE elsewhere.
     * Model the terminal CQE consuming + de-arming the poll directly here (the
     * F_MORE-clear branch of the drain), then re-arm per the protocol. */
    atomic { (poll_armed == 1) -> poll_armed = 0; }
#ifndef BUG_NO_REARM
    /* L261-263 / L268-270: re-enqueue_poll the same sentinel. */
    poll_armed = 1;
#else
    /* BUG: skip the re-arm -> the source is now permanently un-polled. */
    skip;
#endif
    /* the SECOND edge (the one the parked consumer is waiting for) fires.  It
     * produces a deliverable CQE ONLY if a poll is live to catch it. */
    if
    :: poll_armed == 1 -> infra_deliv = 1;   /* re-armed: CQE delivered, hub wakes consumer */
    :: else            -> skip;              /* BUG_NO_REARM: edge produces no CQE -> lost */
    fi;
}

/* The fiber parked on the infra source (e.g. a g parked on a socket whose
 * readiness arrives via the UD_EPOLL poll -> pump, or on the wake eventfd).  It
 * blocks until the hub drains the source's CQE and wakes it.  A dropped re-arm
 * (BUG_NO_REARM) means no CQE is ever produced -> this guard never passes ->
 * invalid end state = the lost wake. */
active proctype infra_consumer()
{
    (infra_woken == 1);
}

/* ---- the op-record completion + park/wake FSM ----------------------------
 * Submitter: io_uring_l_loop.c.inc:447-461 (CAS INFLIGHT->PARKED).
 * Drainer:   io_uring_l_loop.c.inc:300-316  (exchange(*->DONE), wake iff PARKED).
 * Single-reaper on the loop ring, but the CAS handshake is the shared-ring
 * contract; we model both orderings so the FSM proof is honest. */
active proctype op_submit()
{
    byte prev;
    /* L452-455: CAS INFLIGHT->PARKED. */
    atomic {
        prev = op_wait;
        if :: op_wait == INFLIGHT -> op_wait = PARKED;
           :: else                -> skip;
        fi;
    }
    if
    :: prev == INFLIGHT ->
        op_parked = 1;                 /* L458-459: park_current + coro_yield */
        (op_resumes > 0);              /* PARK: the drainer resumes us */
        op_resumes--;                  /* consume our runq entry exactly once */
    :: else ->
        /* lost CAS (op already DONE, L462-466): return synchronously, skip the
         * park.  A correct drainer left op_resumes == 0, so we are NOT also
         * sitting on the runq.  If a buggy drainer enqueued us anyway, that
         * entry resumes a fiber that already returned -> double-resume. */
        op_aborted = 1;
        assert(op_parked == 0);        /* mutual exclusion: never park AND abort */
        assert(op_resumes == 0);       /* AT-MOST-ONCE: not enqueued while running */
    fi;
}

active proctype op_drain()
{
    byte prev;
    /* L301: publish result; L303-305: exchange(op->wait, DONE). */
    atomic {
        prev = op_wait;
        op_wait = DONE;
    }
#ifndef BUG_DOUBLE_RESUME
    /* L307-308: wake (enqueue) IFF prev == PARKED. */
    if
    :: prev == PARKED -> op_resumes++; assert(op_resumes <= 1);  /* runloom_mn_wake_g */
    :: else           -> skip;          /* INFLIGHT: record only; submitter aborts */
    fi;
#else
    /* BUG: wake unconditionally -- enqueue even a fiber that lost the CAS and
     * will return synchronously (prev == INFLIGHT). */
    op_resumes++; assert(op_resumes <= 1);
#endif
}
