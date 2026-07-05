/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- registration ----
 * LEVEL-triggered, NOT edge-triggered.  (The per-backend comments in the body
 * are the authoritative contract; this header only summarises them.)
 *   epoll:  each fd is ADDed LEVEL-triggered (EPOLLIN|EPOLLRDHUP / EPOLLOUT,
 *           NO EPOLLET, NO EPOLLONESHOT), armed once per direction; a re-park
 *           whose direction is already armed skips the epoll_ctl entirely
 *           (0 syscalls on the recv-after-recv hot path).  Because LEVEL
 *           re-reports a still-ready fd on every epoll_wait, a parker that
 *           links late -- or partially consumes and re-parks -- is never
 *           edge-dropped.
 *   kqueue: each requested direction is armed EV_ADD|EV_ONESHOT and re-armed
 *           on EVERY park (NOT EV_CLEAR -- EV_CLEAR is edge-triggered and would
 *           edge-drop a wakeup missed in the EV_ADD..kevent window).
 * Do NOT "restore" an EPOLLET / EV_CLEAR register-once arm here: that was the
 * OLD scheme and it reintroduced exactly the edge-drop hangs the body comments
 * describe (caught by test_netpoll_conformance's fd-reuse case).
 *
 * Caller contract: try the operation first and only call wait_fd after EAGAIN.
 * This is required for correctness regardless of trigger mode -- it serialises
 * the "kernel observed not-ready" state with our parking.  (A full
 * drain-to-EAGAIN is NOT required under LEVEL; it would be under ET.)
 *
 * Stale-fd recovery: socket close auto-clears the kernel
 * registration when the last fd reference goes away.  monkey.py's
 * close hook calls runloom_netpoll_unregister so the bitmap stays in
 * sync with the kernel for fd reuse. */

#if defined(RUNLOOM_HAVE_EPOLL)
/* RUNLOOM_DBG_NETPOLL -- a debug-gated tripwire for the stale-arm / fd-reuse
 * hazard.  The per-fd arm cache (runloom_fd_armed) is PROCESS-GLOBAL and is
 * cleared only by netpoll_unregister.  An fd closed WITHOUT unregister leaves
 * the cache saying "armed" while the kernel has already dropped the
 * registration; the most common way to hit this is NOT a raw wait_fd user but
 * a socket DROPPED TO THE GC -- CPython's socket C-dealloc closes the raw fd
 * directly, bypassing the monkey-patched close() that calls unregister.  The
 * next wait_fd on that fd NUMBER then takes register's already-armed skip and
 * never re-ADDs.
 *
 * PRODUCTION correctness is carried by the stale-arm PROBE (default on,
 * RUNLOOM_STALE_ARM_PROBE_MS): a predicted-skip park carries an internal
 * deadline tick; the expiry sweep validates + heals the arm within ~one probe
 * interval with zero hot-path syscalls (see the parker-struct comment in
 * netpoll.c and runloom_netpoll_validate_arm below).  This flag additionally
 * validates the skip path INLINE -- one EPOLL_CTL_MOD per skip -- so a stale
 * arm is diagnosed loudly at the exact offending park instead of ~250 ms
 * later.  OFF by default so the recv-after-recv hot path keeps its
 * zero-syscall skip. */
static int runloom_netpoll_dbg_stale_check(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_DBG_NETPOLL");
        cur = (e != NULL && e[0] != '\0' && e[0] != '0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Stale-arm probe interval (see the parker-struct comment in netpoll.c for
 * the full mechanism).  RUNLOOM_STALE_ARM_PROBE_MS: default 250 ms; 0
 * disables probing (restores the pre-probe behaviour where a poisoned fd
 * number parks to its ceiling).  Read once, cached. */
static long long runloom_netpoll_stale_probe_ns(void)
{
    static long long v = -1;
    long long cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_STALE_ARM_PROBE_MS");
        cur = (e != NULL) ? atoll(e) * 1000000LL : 250000000LL;
        if (cur < 0) cur = 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Count of stale arms healed / cleared by runloom_netpoll_validate_arm.
 * Every heal means some fd was closed WITHOUT netpoll_unregister (commonly
 * a socket leaked to the GC) -- an app-level hygiene bug this runtime
 * absorbs.  Relaxed: a diagnostic tally, not a synchronisation point. */
static unsigned long long runloom_stale_arm_heals;

/* ADVISORY, LOCK-FREE: will runloom_netpoll_register(fd, events) take the
 * zero-syscall already-armed skip?  wait_fd calls this BEFORE linking, so a
 * predicted-skip park can carry the stale-arm probe deadline from the start
 * (the heap insert at link + the new-earliest poller-break then just work).
 * Race analysis -- the cache can change between this read and register's
 * locked read; every outcome is benign:
 *   - predict skip, register fresh-ADDs (a concurrent unregister raced us):
 *     the arm is kernel-confirmed but the probe stays scheduled -> one spare
 *     validate MOD if the park dwells past the probe tick.  Harmless.
 *   - predict fresh, register skips (a concurrent register raced us): the
 *     sibling's ADD just kernel-confirmed the arm microseconds ago, so the
 *     skip is trustworthy; the probe is simply not scheduled this episode.
 *     A POISONED arm can never appear this way: poison requires a close,
 *     and closing an fd another fiber is concurrently waiting on is an
 *     app-level use-after-close outside this defence's scope.
 * The poisoned case itself always predicts skip (the stale mask is exactly
 * what the cache still holds), so the probe is always scheduled for it. */
static int runloom_netpoll_arm_predict_skip(int fd, int events)
{
    int need = events & (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE);
    unsigned char *base;
    size_t cap = __atomic_load_n(&runloom_fd_pending_wake_cap, __ATOMIC_ACQUIRE);
    if (need == 0 || fd < 0 || (size_t)fd >= cap) return 0;
    base = __atomic_load_n(&runloom_fd_armed, __ATOMIC_ACQUIRE);
    if (base == NULL) return 0;
    {
        int cur = __atomic_load_n(&base[fd], __ATOMIC_RELAXED);
        return (cur & need) == need;
    }
}

/* Validate one fd's LEVEL arm against the kernel and self-heal a stale one.
 * Called WITHOUT any RUNLOOM_RANK_PARKER_POOL lock held (takes
 * runloom_pool.lock itself): from the expiry sweep's post-lock probe batch,
 * and from the RUNLOOM_DBG_NETPOLL skip-path tripwire.
 *
 * Outcomes:
 *   0  arm is genuine (MOD succeeded), or was already unregistered, or a
 *      stale arm was HEALED by re-ADD -- the LEVEL epoll re-reports any
 *      present readiness, which flows to the parker through the normal
 *      pump dispatch/claim path (the commit protocol is never touched here).
 *  -1  the fd itself is DEAD or not pollable (EBADF: closed without
 *      unregister and not yet reused; EPERM: the number was reused by a
 *      non-pollable file).  The cache is cleared exactly like unregister so
 *      the next park on a reused number re-arms fresh; the caller should
 *      error-wake any parkers still waiting on this fd (they can never be
 *      woken by the kernel again). */
static int runloom_netpoll_validate_arm(int fd)
{
    int cur, ep, err;
    struct epoll_event chk;
    if (fd < 0) return 0;
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    cur = runloom_fd_armed_get(fd);
    if (cur == 0) {   /* unregistered since the probe was scheduled */
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return 0;
    }
    ep = runloom_epoll_fd;
    if (runloom_perhub_epoll()) {
        int owner = runloom_fd_epoll_pool_get(fd);
        if (owner >= 0) ep = runloom_pools[owner].epoll_fd;
    }
    chk.events = 0;
    if (cur & RUNLOOM_NETPOLL_READ)  chk.events |= EPOLLIN | EPOLLRDHUP;
    if (cur & RUNLOOM_NETPOLL_WRITE) chk.events |= EPOLLOUT;
    chk.data.fd = fd;
    if (epoll_ctl(ep, EPOLL_CTL_MOD, fd, &chk) == 0) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return 0;                                  /* arm is genuine */
    }
    err = errno;
    if (err == ENOENT) {
        /* Stale arm: the fd is open and pollable but NOT in the epoll set --
         * it was closed without unregister and the number has been reused.
         * Re-ADD to heal; LEVEL delivers any already-present readiness. */
        if (epoll_ctl(ep, EPOLL_CTL_ADD, fd, &chk) == 0) {
            __atomic_add_fetch(&runloom_stale_arm_heals, 1, __ATOMIC_RELAXED);
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            if (runloom_netpoll_dbg_stale_check()) {
                fprintf(stderr,
                    "[runloom-netpoll] STALE ARM healed on fd %d (armed mask "
                    "0x%x): the fd number was closed WITHOUT netpoll_unregister "
                    "(a socket dropped to the GC bypasses the monkey close "
                    "hook) and has been reused.  Re-ADDed; the wait_fd would "
                    "otherwise have parked to its ceiling.\n",
                    fd, (unsigned)cur);
            }
            return 0;
        }
        err = errno;
    }
    /* EBADF / EPERM / anything else: the cached arm cannot correspond to a
     * pollable open fd -- clear it exactly like unregister. */
    runloom_fd_armed_set(fd, 0);
    runloom_fd_bit_clear(fd);
    if (runloom_perhub_epoll()) runloom_fd_epoll_pool_set(fd, -1);
    __atomic_add_fetch(&runloom_stale_arm_heals, 1, __ATOMIC_RELAXED);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    if (runloom_netpoll_dbg_stale_check()) {
        fprintf(stderr,
            "[runloom-netpoll] DEAD ARM cleared on fd %d (armed mask 0x%x, "
            "errno=%d): the fd was closed WITHOUT netpoll_unregister and is "
            "gone (or its number reused by a non-pollable file).  Parkers "
            "still waiting on it are error-woken.\n",
            fd, (unsigned)cur, err);
    }
    return -1;
}
#endif

static int runloom_netpoll_register(int fd, int events)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    int cur, target, need;
    struct epoll_event ev;
    /* RUNLOOM_PERHUB_EPOLL: route ADD/MOD to THIS hub's own epoll (a fresh fd) or
     * to the epoll the fd already lives in (a widen of an existing registration).
     * `ep` is the epoll to ctl against; it defaults to the shared runloom_epoll_fd
     * and the per-hub path overrides it.  See docs/dev/PERHUB_EPOLL.md. */
    int perhub = runloom_perhub_epoll();
    int ep = runloom_epoll_fd;
    int my_pool_idx = -1;
    if (perhub) {
        runloom_parker_pool_t *mp =
            runloom_parker_pool_for_hub(runloom_mn_current_hub_opaque());
        runloom_parker_pool_lock_ensure_inited(mp);
        if (mp->epoll_fd >= 0) {
            ep = mp->epoll_fd;
            my_pool_idx = (int)(mp - runloom_pools);
        }   /* else: pool epoll create failed -> fall back to the shared epoll */
    }

    /* LEVEL register-PER-DIRECTION-once.  Arm each fd LEVEL-triggered (NO
     * EPOLLET, NO EPOLLONESHOT) for whichever directions have an active waiter,
     * and skip epoll_ctl on any re-park whose direction is already armed.
     * Because LEVEL re-reports a still-ready fd on EVERY epoll_wait, a parker
     * that links after an edge -- or that partially consumes and re-parks -- is
     * still delivered (no edge-drop, no pending-bitmap dependency).  The hot
     * path is a READ socket re-parking recv-after-recv: IN stays armed, so it
     * costs ZERO syscalls -- the win over the old LEVEL+EPOLLONESHOT scheme's
     * one epoll_ctl/park.
     *
     * EPOLLONESHOT is gone, so the always-writable OUT could level-busy-loop
     * the pump; OUT is armed only when a WRITE waiter exists (and a long-lived
     * WRITE-heavy fd's lazy OUT-disarm is a documented follow-up -- the READ
     * hot path never arms OUT; short WRITE waits drop their registration on
     * close).  IN never busy-loops (a not-readable socket produces no IN
     * event), so IN is safe register-once.  EPOLLEXCLUSIVE stays OFF.
     *
     * The arm mask (runloom_fd_armed) doubles as registration state: armed==0
     * means "not in the epoll set" (-> ADD), armed!=0 means "already ADDed"
     * (-> MOD to widen the direction set).
     *
     * FSM SCOPE (RUNLOOM_FSM_VALIDATE): the arm mask is NOT a transition FSM and is
     * deliberately HARDENED IN PLACE, not converted (FSM_ADOPTION.md tier-3/4).  It
     * is a monotonically-widened atomic DIRECTION-SET bitmask (target = cur | need),
     * cleared to 0 on unregister / release_if_idle, all under runloom_pool.lock.
     * Every cur|need is valid by construction -- there is no enumerable state with
     * an illegal (state,event) cell for a NOTE to catch -- so the bitmask + the lock
     * (plus the documented cross-pool stale-arm migration and the stale-arm
     * self-heal below) ARE the proof; a NOTE-edge here would be ceremony. */
    need = events & (RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE);

    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    cur = runloom_fd_armed_get(fd);
    target = cur | need;
    if (perhub && cur != 0) {
        /* Widen: the fd already lives in its OWNER's epoll -- MOD there, not in
         * this hub's epoll, because that is where it physically is. */
        int owner = runloom_fd_epoll_pool_get(fd);
        ep = (owner >= 0) ? runloom_pools[owner].epoll_fd : runloom_epoll_fd;
        /* CROSS-POOL STALE ARM: the arm cache (runloom_fd_armed / _epoll_pool) is
         * a process-global keyed by fd NUMBER and is NOT reset when the scheduler
         * mode changes.  A pooled wake-pipe / reused fd registered into one pool's
         * epoll (e.g. the DEFAULT pool during a single-thread run()) can be re-used
         * by a fiber on a DIFFERENT hub in a later M:N run(); the stale arm makes
         * the "already armed" skip below fire, so the fd is never EPOLL_CTL_ADD'd
         * into THIS hub's epoll.  It stays only in the old owner's epoll -- which
         * no current hub waits on -- so its readiness (an os.write wake-byte) is
         * delivered to nobody and the parked fiber hangs forever (test_monkey_
         * offload single-thread-then-M:N).  When the recorded owner is not THIS
         * hub's pool, the registration must MIGRATE: DEL from the old epoll and
         * re-ADD fresh into ours.  Force the fresh-ADD path by treating it as
         * unarmed (cur=0) so the ADD + owner record below target my_pool_idx.
         * owner<0 (shared fallback) is already correct for THIS hub since the
         * fallback epoll is shared, so only a >=0 owner mismatch migrates. */
        if (owner >= 0 && owner != my_pool_idx) {
            /* MULTI-POOL fd: the SAME fd is now wanted by a second hub's pool.
             * The old behaviour ping-ponged it between per-hub epolls (DEL from
             * the owner, fresh ADD into mine) on every cross-hub re-park.  Under
             * many fibers parking CONCURRENTLY on ONE fd across hubs, that churn
             * raced the park/unlink bookkeeping (the parker-on-coro-stack reuse
             * + per-pool by_fd/poolmask) into a LOST PARK: a committed-PARKED g
             * dropped from its bucket, never woken, never timed out, then freed
             * -- reproduced ~1% single-copy (docs/dev/repro/LOST_PARK_FINDING.md;
             * vanished with PERHUB_EPOLL=0 or one-fd-per-waiter).
             *
             * Fix: migrate the fd ONCE to the SHARED epoll (runloom_epoll_fd),
             * which is NESTED inside every per-hub pump drain, so EVERY hub
             * delivers its events -- no orphan, and crucially NO further
             * migration (record owner = -1 so later parkers on any pool take the
             * already-armed skip).  Single-hub fds keep the per-hub fast path;
             * only a genuinely multi-pool fd falls back to shared.  This still
             * fixes the original stale-arm hang (test_monkey_offload): the fd
             * lands in the shared epoll, which IS pumped, never a dead pool. */
            (void)epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL);
            /* PRESERVE every already-armed direction.  The fresh ADD into the
             * shared epoll must re-arm the FULL mask (cur | need), not just this
             * caller's `need`: narrowing to `need` dropped the direction(s) a
             * currently-parked waiter (in `cur`) needs -- the fd landed in the
             * shared epoll for ONE direction only, the arm cache recorded that
             * narrowed mask (so no later register re-armed the lost one, the
             * widen path computing target==cur and skipping), and the dropped
             * direction's waiter never received its event (a deterministic lost
             * wakeup / hang).  Compute the full mask BEFORE zeroing cur. */
            target = cur | need;
            cur = 0;                 /* fresh ADD into the shared epoll */
            ep = runloom_epoll_fd;
            my_pool_idx = -1;        /* record as shared -> no future migration */
        }
    }
    if (cur != 0 && target == cur) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        /* RUNLOOM_DBG_NETPOLL tripwire: validate the zero-syscall skip
         * IMMEDIATELY (one EPOLL_CTL_MOD per skip) instead of waiting for the
         * production stale-arm probe tick -- turns a stale arm into a loud,
         * pinpointed diagnostic at the exact park that would have hung.
         * OFF by default so the recv-after-recv hot path keeps its
         * zero-syscall skip; production correctness is carried by the
         * probe_pending deadline the predicting wait_fd scheduled. */
        if (runloom_netpoll_dbg_stale_check())
            (void)runloom_netpoll_validate_arm(fd);
        return 0;                               /* already armed -> 0 syscalls */
    }
    if (runloom_fd_armed_set(fd, target) != 0) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        errno = ENOMEM;
        return -1;
    }
    /* Fresh ADD: record which hub's epoll now owns this fd (read by a later widen
     * and by release_if_idle's DEL).  my_pool_idx is -1 on the shared fallback. */
    if (perhub && cur == 0) runloom_fd_epoll_pool_set(fd, my_pool_idx);

    ev.events = 0;
    if (target & RUNLOOM_NETPOLL_READ)  ev.events |= EPOLLIN | EPOLLRDHUP;
    if (target & RUNLOOM_NETPOLL_WRITE) ev.events |= EPOLLOUT;
    ev.data.fd = fd;

    /* Issue the epoll_ctl STILL HOLDING runloom_pool.lock -- the SAME critical
     * section that read `cur` and published the arm cache above.  Dropping the
     * lock before this syscall let two threads registering the SAME fd
     * concurrently (READ on one hub, WRITE on another, fd not yet armed)
     * interleave their ADD/MOD out of order with respect to their cache updates:
     * the later MOD could hit ENOENT before the sibling's ADD landed and then
     * EEXIST with both fallbacks exhausted -> a spurious FileExistsError raised
     * out of a healthy wait_fd, or the kernel event set was silently narrowed to
     * a single direction while the arm cache claimed both -> the other
     * direction's waiter never woke.  epoll_ctl is a brief non-blocking syscall
     * that does NOT release the GIL, so holding the lock across it adds no
     * blocking path and no lock-order inversion. */
    if (cur == 0) {
        if (epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev) == 0) {
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return 0;
        }
        if (errno == EEXIST &&
            epoll_ctl(ep, EPOLL_CTL_MOD, fd, &ev) == 0) {
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return 0;
        }
    } else {
        if (epoll_ctl(ep, EPOLL_CTL_MOD, fd, &ev) == 0) {
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return 0;
        }
        if (errno == ENOENT &&
            epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev) == 0) {
            RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
            return 0;
        }
    }
    /* Failed: roll the arm mask back so a future caller can retry.  Still under
     * the lock taken at the top of the function. */
    runloom_fd_armed_set(fd, cur);
    if (perhub && cur == 0) runloom_fd_epoll_pool_set(fd, -1);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    return -1;
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Arm ONLY the requested direction(s), ONE-SHOT (NOT EV_CLEAR), re-arming
     * on EVERY park -- the kqueue counterpart of the epoll EPOLLONESHOT re-arm
     * fix above.  The old scheme (register-once + EV_CLEAR, and SKIP the kevent
     * when the per-fd bit was already set) lost wakeups two ways, both caught by
     * test_netpoll_conformance's fd-reuse case:
     *   - EV_CLEAR is edge-triggered: an edge missed in the window between
     *     EV_ADD and the next kevent() never refired (same trap as epoll's old
     *     EPOLLET register-once);
     *   - a reused fd NUMBER whose close-hook had not run (raw socket.close(),
     *     no runloom wrapper) kept its bitmap bit set, so the second register
     *     RETURNED EARLY without issuing the kevent -> the new socket was never
     *     armed and its reader parked forever (timed out via the deadline, or
     *     hung outright with no timeout).
     * EV_ADD re-checks readiness NOW (kqueue reports a level-ready fd at add
     * time, so data that arrived before the parker linked is still delivered),
     * and EV_ONESHOT delivers once then auto-disables (no thundering herd; the
     * always-writable OUT can't busy-loop the pump).  The bitmap is still kept
     * in sync for unregister, but no longer gates the kevent.
     *
     * PER-HUB kqueue: the kevent goes to the CURRENT hub's OWN kqueue, the same
     * pool wait_fd links the parker into (pool_for_hub(current hub), this thread)
     * -- so register and link agree, and the event is collected by THIS hub's
     * pump (or its periodic non-blocking self-pump while busy).  Per-hub kqueues
     * remove the shared-kqueue kq_lock contention that collapsed throughput with
     * hub count. */
    {
    runloom_parker_pool_t *kpool =
        runloom_parker_pool_for_hub(runloom_mn_current_hub_opaque());
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    if (runloom_fd_bit_set(fd) != 0) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        errno = ENOMEM;
        return -1;
    }
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    if (kpool->kqueue_fd < 0) {                  /* pool backend never came up */
        RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        runloom_fd_bit_clear(fd);
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        errno = EINVAL;
        return -1;
    }
    {
        struct kevent kev[2];
        int n = 0;
        if (events & RUNLOOM_NETPOLL_READ)
            EV_SET(&kev[n++], fd, EVFILT_READ,  EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (events & RUNLOOM_NETPOLL_WRITE)
            EV_SET(&kev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
        if (n == 0) return 0;
        {   /* test-only: force the registration kevent() to fail; the error
             * surfaces as OSError(errno) to the fiber parking on this fd
             * (the fd-bit is rolled back below). */
            int finj = runloom_fault_inject(RUNLOOM_FAULT_KQUEUE_CTL);
            if (finj) errno = finj;
            else if (kevent(kpool->kqueue_fd, kev, n, NULL, 0, NULL) == 0) return 0;
        }
    }
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    runloom_fd_bit_clear(fd);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    return -1;
    }
#else
    (void)fd;
    return 0;  /* select doesn't need pre-registration */
#endif
}

#if defined(RUNLOOM_HAVE_EPOLL)
/* Lazy OUT-disarm -- the follow-up runloom_netpoll_register's body comment defers.
 * OUT is armed LEVEL-triggered (no EPOLLET / EPOLLONESHOT) and a connected socket
 * is almost always writable, so once a fd's single WRITE waiter has been served
 * the kernel re-reports EPOLLOUT on EVERY epoll_wait; the pump finds no WRITE
 * parker, stashes a pending-wake bit and returns, and the scheduler's idle loop
 * re-pumps instantly -> a 100% CPU busy-spin for as long as the fd stays open with
 * any fiber netpoll-parked.  This is not niche: the cooperative connect() path
 * (EINPROGRESS -> wait_fd(fd, WRITE)) arms OUT on every outbound connection, so a
 * plain connect-then-recv client pegs a core while merely waiting for a reply.
 *
 * The pump calls this from runloom_epoll_handle_event when an EPOLLOUT-bearing
 * event for `fd` was NOT consumed by any parker (dispatch returned 0): drop OUT
 * from the arm cache and the kernel event set (DEL if nothing is left armed, else
 * MOD down to IN-only).  A later WRITE waiter re-arms OUT through
 * runloom_netpoll_register's normal fresh-ADD / widen path.
 *
 * Race-safe against a WRITE waiter that re-registers concurrently: a waiter links
 * its parker (runloom_parker_link) BEFORE it arms (runloom_netpoll_register), the
 * arm runs under runloom_pool.lock, and the pump sets the fd's pending-wake bit on
 * this same no-parker path -- so a writer that raced in either re-arms OUT after
 * we drop it (its register sees `cur` without WRITE and re-ADDs the direction) or
 * consumes the pending bit at its post-register re-check instead of parking. */
static void runloom_netpoll_disarm_out(int fd)
{
    int cur, new_mask, ep;
    struct epoll_event ev;
    if (fd < 0) return;
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    cur = runloom_fd_armed_get(fd);
    if (!(cur & RUNLOOM_NETPOLL_WRITE)) {          /* OUT already disarmed */
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return;
    }
    new_mask = cur & ~RUNLOOM_NETPOLL_WRITE;       /* keep IN if a READ waiter armed it */
    /* DEL/MOD against the epoll the fd actually lives in: its owning hub's under
     * RUNLOOM_PERHUB_EPOLL, the shared runloom_epoll_fd otherwise (owner < 0). */
    ep = runloom_epoll_fd;
    if (runloom_perhub_epoll()) {
        int owner = runloom_fd_epoll_pool_get(fd);
        if (owner >= 0) ep = runloom_pools[owner].epoll_fd;
    }
    if (new_mask == 0) {
        /* No direction left armed -> drop the whole registration (the fd is still
         * OPEN, so the kernel will not auto-remove it; mirrors release_if_idle). */
        runloom_fd_armed_set(fd, 0);
        runloom_fd_bit_clear(fd);
        if (runloom_perhub_epoll()) runloom_fd_epoll_pool_set(fd, -1);
        epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL);
    } else {
        /* READ stays armed -> narrow the kernel event set to IN only. */
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = fd;
        runloom_fd_armed_set(fd, new_mask);
        if (epoll_ctl(ep, EPOLL_CTL_MOD, fd, &ev) != 0 && errno == ENOENT)
            (void)epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev);
    }
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
}
#endif  /* RUNLOOM_HAVE_EPOLL */

void runloom_netpoll_unregister(int fd)
{
#if defined(RUNLOOM_HAVE_EPOLL) || defined(RUNLOOM_HAVE_KQUEUE)
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    runloom_fd_bit_clear(fd);
    /* Reset the epoll LEVEL arm mask: a reused fd number with a stale "armed"
     * mask would skip its EPOLL_CTL_ADD and never be re-armed (a hang).  All
     * real close paths call this hook, so the mask tracks fd identity. */
    runloom_fd_armed_set(fd, 0);
    /* Clear the per-hub-epoll owner too, so a reused fd NUMBER does not inherit a
     * stale owning-pool (which would mis-route a later widen/DEL).  No syscall:
     * close auto-removes the fd from whichever epoll held it. */
    runloom_fd_epoll_pool_set(fd, -1);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    /* No kernel syscall: epoll/kqueue auto-remove the fd when the
     * last reference closes.  Calling EPOLL_CTL_DEL after close
     * would race with fd reuse anyway. */
#else
    (void)fd;
#endif
}

/* Release an fd's epoll registration IFF no fiber is currently parked on it.
 *
 * The aio bridge's low-level loop.sock_* operate on USER-OWNED sockets that the
 * caller closes with a plain socket.close() -- which does NOT run the monkey /
 * _close_sock unregister hook (only the bridge's OWN sockets go through that).
 * Without clearing the per-fd LEVEL arm cache, a reused fd NUMBER inherits the
 * stale "armed" mask and runloom_netpoll_register's already-armed skip path never
 * re-arms it in the kernel -> the new socket's wait_fd parks forever (the
 * long-standing test_recvfrom / fast-socket-churn hang).  The bridge calls this
 * after each low-level sock_* op so the registration is dropped once the op is
 * done, and a later raw close + fd reuse re-registers cleanly.
 *
 * Guarded on "no parker remains on this fd in ANY pool": a concurrent waiter on
 * the same fd (e.g. a WRITE parker while a READ op finishes) keeps its arm -- we
 * never EPOLL_CTL_DEL out from under a live direction, and never drop a wakeup.
 * Unlike unregister this DOES issue EPOLL_CTL_DEL (the fd is still OPEN here, so
 * the kernel will not auto-remove it; leaving it LEVEL-armed with no parker would
 * busy-report on every epoll_wait).  Monkey and the bridge transports are
 * untouched -- they keep the register-once fast path and clear the cache via
 * their own close hooks, so the throughput hot path pays zero extra syscalls. */
void runloom_netpoll_release_if_idle(int fd)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (fd < 0) return;
    /* "No parker on this fd in ANY pool" -- the documented contract.  The arm-
     * cache clear and the EPOLL_CTL_DEL run under runloom_pool.lock (the SAME lock
     * register() takes to set the arm), and we check BOTH the DEFAULT pool's
     * bucket (read directly under that lock) AND the 64 per-hub M:N pools (via the
     * fd's dispatch poolmask -- the per-fd atomic bitmask, bit i set while hub
     * pool i holds a parker for fd, set at parker_link BEFORE the arm and cleared
     * when the bucket empties; the same bitmap runloom_pump_dispatch_event trusts).
     * Checking only the DEFAULT pool (the old guard) could EPOLL_CTL_DEL an fd a
     * live hub fiber was parked on -- runloom_c.netpoll_release_if_idle is exported
     * to Python with no bridge-only restriction, and the aio bridge can run
     * concurrently with M:N hubs -- stranding that fiber's readiness forever.
     * Ordering: a hub waiter links (poolmask set, RELEASE) BEFORE it arms, and the
     * arm is set under this lock, so any hub waiter whose registration we could
     * tear down is guaranteed visible in the poolmask once we hold the lock; a
     * waiter still mid-flight (linked, not yet armed) re-ADDs fresh in its own
     * register() after our DEL clears the arm cache (armed==0). */
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    {
        int default_empty = ((size_t)fd >= runloom_pool.by_fd_cap ||
                             runloom_pool.by_fd[fd] == NULL);
        int hub_empty = 1;
        unsigned long long *pm =
            __atomic_load_n(&runloom_fd_poolmask, __ATOMIC_ACQUIRE);
        if (pm != NULL && (size_t)fd < runloom_fd_poolmask_cap &&
            __atomic_load_n(&pm[fd], __ATOMIC_ACQUIRE) != 0)
            hub_empty = 0;
        if (default_empty && hub_empty && runloom_fd_armed_get(fd) != 0) {
            /* DEL from the epoll the fd actually lives in.  Under
             * RUNLOOM_PERHUB_EPOLL that is its owning hub's epoll (bridge fds are
             * the DEFAULT pool's); the shared runloom_epoll_fd otherwise.
             * owner<0 -> shared fallback. */
            int del_ep = runloom_epoll_fd;
            if (runloom_perhub_epoll()) {
                int owner = runloom_fd_epoll_pool_get(fd);
                if (owner >= 0) del_ep = runloom_pools[owner].epoll_fd;
                runloom_fd_epoll_pool_set(fd, -1);
            }
            runloom_fd_armed_set(fd, 0);
            runloom_fd_bit_clear(fd);
            epoll_ctl(del_ep, EPOLL_CTL_DEL, fd, NULL);
        }
    }
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
#else
    /* kqueue re-arms ONE-SHOT on every park (no persistent arm to go stale) and
     * select needs no pre-registration, so neither has the epoll fd-reuse bug. */
    (void)fd;
#endif
}

int runloom_netpoll_parked_count(void)
{
    int total = 0;
    int i;
    for (i = 0; i < RUNLOOM_PARKER_POOL_MAX; i++) {
        total += __atomic_load_n(&runloom_pools[i].total, __ATOMIC_ACQUIRE);
    }
    return total;
}

/* Hub-idle dwell-based stack reclaim (RUNLOOM_STACK_PARK_SWEEP).  Walk the
 * CALLING hub's parker pool and madvise the below-SP idle stack pages of
 * fibers whose current park has already exceeded threshold_ns.
 *
 * SAFE under M:N: every parker in pool[hub] is owned by `hub` (wakes
 * route to p->hub and land in that hub's non-stealable local FIFO, so
 * `hub` is the sole resumer).  The caller IS that hub and is idle (not
 * resuming) here, so nothing runs on the stacks we madvise; a concurrent
 * pump on another hub may unlink+re-queue a parker but never touches its
 * stack, and a re-queued g only waits in this hub's FIFO until this
 * sweep returns -- it cannot run, complete, or be freed meanwhile.  We
 * snapshot candidate g's under the pool lock (marking them reclaimed),
 * then madvise OUTSIDE the lock so the syscall doesn't stall pumps that
 * search this pool.  Bounded to RUNLOOM_SWEEP_BATCH per call; the next idle
 * sweep takes the rest.  Returns the number of stacks reclaimed. */
#define RUNLOOM_SWEEP_BATCH 128       /* max stacks madvised per sweep call */
#define RUNLOOM_SWEEP_MAX_VISIT 1024  /* max parkers examined per sweep (lock-hold bound) */
/* Churn throttle default: skip the sweep when this pool sees more than
 * this many parker arrivals/sec (per hub).  Calibrated so a genuinely
 * idle keepalive workload (the N=1M target: parkers dwelling seconds,
 * low arrival rate) always sweeps and reclaims, while an active-churn
 * workload (sub-second per-connection cycling, high arrival rate) skips
 * -- degrading the sweep to a no-op rather than paying lock contention.
 * Override with RUNLOOM_SWEEP_MAX_CHURN (0 disables the throttle).
 *
 * 600 calibrated on the N=65K keepalive bench (H=16): the idle target
 * (think=10s, ~250 arrivals/s/hub) sweeps FULLY at 600 -- RSS 3419 MB
 * (full -32% win), p99 50 ms (the OFF floor, zero cost) -- while the
 * active-churn case (think=2s, ~1200/s/hub) throttles (RSS 4929 ~= OFF,
 * sweep degrades to a no-op).  600 sits cleanly between the two rates. */
#define RUNLOOM_SWEEP_DEFAULT_MAX_CHURN 600LL
int runloom_netpoll_sweep_idle(void *hub_opaque, long long threshold_ns)
{
    /* No backend #if here: the actual madvise lives in
     * runloom_coro_madvise_idle (coro.c), which no-ops on backends without
     * an inspectable SP / MADV_DONTNEED.  netpoll.c does NOT define
     * MADV_DONTNEED (no <sys/mman.h>), so gating this body on it -- as an
     * earlier draft did -- silently compiled the whole sweep away. */
    runloom_parker_pool_t *pool = runloom_parker_pool_for_hub(hub_opaque);
    runloom_g_t *batch[RUNLOOM_SWEEP_BATCH];
    int n = 0, i, visited = 0;
    int per_g = runloom_get_per_g_tstate_mode();
    long long now;
    if (pool == NULL ||
        __atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2) {
        return 0;
    }
    now = monotonic_ns();
    RUNLOOM_RLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    /* Churn throttle.  Compute this pool's arrival rate since the last
     * sweep eval; if it exceeds RUNLOOM_SWEEP_MAX_CHURN links/sec, skip the
     * walk entirely.  Rationale (perf-confirmed): the active-churn tail
     * cost is the sweep's lock-hold competing with the wake path on
     * pool->lock, not refault; and a fast-churning pool's parkers aren't
     * long-idle, so there's little to reclaim.  Skipping is always safe
     * -- a not-yet-reclaimed stack just stays resident a bit longer.  We
     * still update the churn baseline so the rate window stays recent. */
    {
        static long long max_churn = -1;     /* links/sec; -1 = read env once */
        long long mc = __atomic_load_n(&max_churn, __ATOMIC_RELAXED);
        if (mc < 0) {
            const char *e = getenv("RUNLOOM_SWEEP_MAX_CHURN");
            mc = (e != NULL) ? atoll(e) : RUNLOOM_SWEEP_DEFAULT_MAX_CHURN;
            if (mc < 0) mc = 0;
            __atomic_store_n(&max_churn, mc, __ATOMIC_RELAXED);
        }
        if (mc > 0) {
            unsigned long long links = pool->link_count;
            long long dt = now - pool->churn_last_ns;
            int throttle = 0;
            if (pool->churn_last_ns != 0 && dt > 0) {
                unsigned long long arrivals = links - pool->churn_last_links;
                /* arrivals/dt[s] >= mc  <=>  arrivals*1e9 >= mc*dt  (ns) */
                if (arrivals * 1000000000ULL >=
                    (unsigned long long)mc * (unsigned long long)dt) {
                    throttle = 1;
                }
            }
            pool->churn_last_links = links;
            pool->churn_last_ns = now;
            if (throttle) {
                RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
                return 0;
            }
        }
    }
    {
        /* Resume from the saved cursor -- but VALIDATE it WITHOUT dereferencing
         * it.  A parker unlinked between sweeps can be free()d
         * (runloom_parker_pool_release frees once the releasing thread's TLS
         * freelist is full -- a fan-in wake of >64 waiters overflows it), so the
         * old "parkers are pool-recycled, never freed" assumption was wrong and
         * reading p->gen off the saved pointer to validate it was a use-after-free
         * (and a coincidental gen match then walked p->next garbage).
         *
         * Safe test with no deref: the saved cursor was LINKED when we stored it,
         * and a linked parker can only be freed AFTER being unlinked.  link/unlink
         * both run under this pool lock, and the cumulative unlink count
         * (link_count - total) is monotonic, so an UNCHANGED count since we saved
         * the cursor proves it was never unlinked -- hence never freed -- and is
         * still linked, so resuming from it is safe.  Any unlink since (count
         * changed) means the cursor MIGHT be freed, so restart from head (the
         * documented fallback).  Plain LINKS (head-inserts) don't disturb the
         * cursor's linkage, so they don't force a restart.  The whole walk runs
         * under the lock, so the live chain is stable here; MAX_VISIT bounds it
         * either way. */
        unsigned int unlinks_now = (unsigned int)(pool->link_count -
            (unsigned long long)__atomic_load_n(&pool->total, __ATOMIC_RELAXED));
        runloom_parked_t *p = pool->sweep_cursor;
        if (p == NULL || unlinks_now != pool->sweep_cursor_gen) {
            p = pool->head;
        }
        while (p != NULL && n < RUNLOOM_SWEEP_BATCH &&
               visited < RUNLOOM_SWEEP_MAX_VISIT) {
            if (!p->reclaimed && p->park_ts != 0 &&
                p->g != NULL && p->g->coro != NULL &&
                __atomic_load_n(&p->commit, __ATOMIC_ACQUIRE)
                    == RUNLOOM_PARK_PARKED &&
                (now - p->park_ts) >= threshold_ns) {
                /* Default mode: this hub is the g's sole resumer, so marking it
                 * reclaimed and madvising below is race-free.  Per-g-tstate: a
                 * woken g can be stolen by ANY hub, so claim it exclusively
                 * (PARKED->SWEEPING) first; if the claim loses (the g is being
                 * woken/owned) skip it -- never madvise a resumable stack. */
                if (!per_g || runloom_mn_sweep_try_claim(p->g)) {
                    p->reclaimed = 1;
                    batch[n++] = p->g;
                }
            }
            p = p->next;
            visited++;
        }
        pool->sweep_cursor = p;                 /* NULL at end -> next resets to head */
        /* Snapshot the cumulative-unlink count (NOT p->gen): the next sweep uses
         * it to prove -- without dereferencing p -- that the cursor was not
         * unlinked/freed in between.  The lock is held continuously across the
         * walk, so no link/unlink ran since unlinks_now was read above; reuse it. */
        pool->sweep_cursor_gen = unlinks_now;
    }
    RUNLOOM_RUNLOCK(&pool->lock, RUNLOOM_RANK_PARKER_POOL);
    for (i = 0; i < n; i++) {
        runloom_coro_madvise_idle(batch[i]->coro);
        /* Companion reclaim: drop the parked Python g's idle datastack-
         * chunk tail too (the C-stack madvise above never touches it).
         * Same owning-hub safety contract; gated by RUNLOOM_DATASTACK_SWEEP. */
        runloom_sched_madvise_datastack_idle(batch[i]);
        /* Per-g-tstate: release the exclusive sweep claim now that this g's
         * madvise has completed (SWEEPING->PARKED, or re-enqueue a wake that
         * landed mid-madvise).  Per-g release bounds a woken g's extra latency
         * to its own single madvise, not the whole batch. */
        if (per_g) {
            runloom_mn_sweep_claim_release(batch[i]);
        }
    }
    return n;
}
