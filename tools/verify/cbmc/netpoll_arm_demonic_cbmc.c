/* netpoll_arm_demonic_cbmc.c -- DEMONIC-ORACLE verification of the netpoll
 * register/arm coherence, INDEPENDENT of how the kernel behaves.
 *
 * The idea (the "model every possible syscall return" one): every epoll_ctl call
 * is a demon -- it may SUCCEED (applying the requested registration) or FAIL with
 * ANY error, at CBMC's choosing.  We prove the runloom arm logic keeps this
 * invariant for EVERY possible sequence of those returns:
 *
 *     LOST-WAKE-FREE:  no fiber is left committed-PARKED on a direction that the
 *                      kernel epoll is not registered to deliver.
 *
 * A parked fiber whose direction is not in any pumped epoll never receives its
 * event -> hangs forever.  This is THE networking lost-wake, and modelling epoll
 * as demonic makes the proof hold regardless of the real kernel's behaviour --
 * the only thing assumed is the SET of values epoll_ctl can return (0 or -1),
 * not that it ever succeeds.
 *
 * Models the SHIPPED cross-hub migration path (netpoll_register.c.inc ~:290-348):
 * on a multi-pool fd, DEL from the old epoll then fresh-ADD into the shared epoll
 * with target = cur|need.  Faithful to the real code, INCLUDING that the DEL
 * return is ignored (the (void) cast at :334).
 *
 * AUDIT 2026-07-06 (an independent 4-lens adversarial attack on this harness):
 * the ORIGINAL default modelled a fictional post-ADD-fail recovery (k_arm=waiter)
 * with NO counterpart in the shipped code, which made the default PASS VACUOUS on
 * the safety-critical DEL-ok+ADD-fail migration sub-path (the assertion degenerated
 * to `waiter & ~waiter == 0`).  Corrected: the default now models what register()
 * ACTUALLY does (clears the cache, returns -1, re-arms nothing) and so REPORTS the
 * window instead of hiding it.  The recovery is now opt-in (-DASSUME_ADD_RECOVERY)
 * and clearly labelled an assumption, not code behaviour.
 *
 * Configs (compile-time), each a separate CBMC run:
 *   (default)              FAITHFUL model of register() -> FAILS, exposing a
 *                          SUSPECTED lost-wake window: migration DEL-ok + ADD-fail
 *                          leaves a pre-existing untimed-park waiter registered in
 *                          no epoll.  Whether this is a PERMANENT hang in the real
 *                          runtime depends on reachability facts NOT yet established
 *                          (does an untimed wait_fd park exist here? is a later
 *                          register on the fd guaranteed? does the pump re-arm?) --
 *                          so this is a lead to investigate, NOT a confirmed bug.
 *
 * EMPIRICAL FOLLOW-UP 2026-07-06 (LD_PRELOAD forced-ENOMEM repro, 2/3-hub
 * socketpair, pre-existing untimed reader + writer(s) triggering cross-hub
 * migration on the same fd):  the window did NOT manifest as a permanent silent
 * hang in ANY reachable configuration built.  The migration ADD-fail fired
 * (verified via the shim), yet the stranded reader ALWAYS recovered -- either it
 * woke with data (a LATER successful register on the fd re-ADDed it -> LEVEL
 * re-report), or, under sustained ENOMEM (every ADD failing), it woke with the
 * ENOMEM surfaced as an OSError rather than hanging.  The pure sole-reader /
 * one-off-writer-cross-hub shape that permanence needs could NOT be constructed
 * from Python (mn_fiber has no hub-pinning; placement is round-robin).  NET: the
 * CODE asymmetry is real (this ADD-fail path at netpoll_register.c.inc:422-425
 * does NOT error-wake pre-existing parkers, unlike the validate_arm DEAD path
 * at :133 which documents you must) and the demonic proof exposes it kernel-
 * independently -- but its PERMANENT-hang REACHABILITY in the shipped runtime is
 * NOT demonstrated; every exercised path self-recovered.  A defensive error-wake
 * at :422 (mirroring the already-shipped :133 recovery) would close it whether or
 * not it is reachable.
 *
 * RESOLUTION 2026-07-06: that error-wake SHIPPED (netpoll_register.c.inc failure
 * epilogue: on migration ADD-fail, runloom_pump_dispatch_event(fd, R|W, wake_all)
 * with the pool lock dropped and errno preserved -- the exact recovery the probe
 * batch already used for DEAD fds).  CONFIG MEANINGS SINCE THEN:
 *   -DASSUME_ADD_RECOVERY  is now the FAITHFUL model of shipped code -> must PASS.
 *   (default, no recovery) models the PRE-fix code -> must FAIL.  Kept as the
 *                          regression teeth: if a refactor ever drops the
 *                          error-wake, the default config documents exactly what
 *                          breaks and why.
 * Verified post-fix: forced sustained-ENOMEM repro wakes the stranded reader with
 * a loud OSError(ENOMEM); healthy path unchanged; netpoll affinity suite green.
 *   -DBUG_ARM_DROP         the 2026-07-02 migration bug (target=need) -> FAILS
 *                          (a second, independent way to reach the window: teeth).
 *   -DASSUME_ADD_RECOVERY  ADD-adds the (unproven) assumption that a later register
 *                          / park timeout re-delivers to the stranded waiter ->
 *                          PASSES, showing the window is closed IFF that recovery
 *                          is real.  This is the hypothesis to confirm or refute.
 */

#define IN  1
#define OUT 2

int nondet_int(void);
_Bool nondet_bool(void);

/* --- the demon: an epoll_ctl that may fail with any error, any time --------- */
static int k_arm;   /* kernel ground truth: directions epoll will actually deliver */
static int cache;   /* runloom's arm cache (runloom_fd_armed): what it THINKS is armed */

/* EPOLL_CTL_ADD/MOD with `target`: succeeds (kernel now == target) or fails
 * (kernel unchanged), demonically.  Returns 0 / -1 like the real syscall. */
static int demonic_ctl(int target)
{
    if (nondet_bool()) return -1;   /* demon: this epoll_ctl failed (any errno) */
    k_arm = target;                 /* success: kernel registered exactly target */
    return 0;
}

/* EPOLL_CTL_DEL: the real code ignores its return (the (void) cast).  Model both:
 * the demon decides whether the kernel actually dropped the registration. */
static void demonic_del(void)
{
    if (nondet_bool()) k_arm = 0;   /* DEL took effect (fd now unregistered) */
    /* else: DEL "failed"/no-op -- fd still registered in the old epoll.  The code
     * ignores the return either way, so we must survive both. */
}

/* One register(need) on a fd that may already be multi-pool (migrating). `waiter`
 * carries the directions with a fiber ALREADY committed-parked before this call
 * (e.g. a reader parked on IN while a writer now registers OUT -- the migration
 * scenario).  Returns the NEW committed-waiter mask. */
static int reg(int need, int migrating, int waiter, int *ok)
{
    int cur = cache;
    if (migrating && cur != 0) {
        demonic_del();                 /* DEL old epoll (return ignored, as shipped) */
#ifdef BUG_ARM_DROP
        int target = need;             /* BUG: drops the already-armed `cur` */
#else
        int target = cur | need;       /* FIX: preserve every armed direction */
#endif
        cur = 0;                        /* fresh ADD into the shared epoll */
        int rc = demonic_ctl(target);
        if (rc == 0) {
            cache = target;
            *ok = 1;                    /* this caller's park is committed */
            return waiter | need;
        }
        /* ADD failed after DEL: register() clears the arm cache and returns -1 to
         * THIS caller; the fd is now in NO epoll and the arm cache is 0 (faithful
         * to netpoll_register.c.inc :422 runloom_fd_armed_set(fd,cur==0) + :425
         * return -1).  A PRE-EXISTING waiter (in `waiter`) is left parked and
         * registered nowhere -- a genuine lost-wake window unless something later
         * re-registers the fd. */
        cache = 0;
        *ok = 0;
#ifdef ASSUME_ADD_RECOVERY
        /* OPT-IN assumption (NOT shipped-code behaviour): a later register on the
         * fd, or the park's own timeout, eventually re-delivers to the stranded
         * waiter.  Toggle this on to see the invariant hold IFF such a recovery
         * exists; leave it OFF (the default) to model what register() ACTUALLY
         * does -> the assertion then exposes the unrecovered window.  NB: the
         * stale-arm self-heal probe does NOT recover this state (cache==0 makes
         * runloom_netpoll_validate_arm no-op), so of the plausible recoveries only
         * a timed-park timeout / a subsequent register remain -- neither fires for
         * an UNTIMED wait_fd park with no later register. */
        k_arm = waiter;
#endif
        return waiter;                  /* pre-existing waiters unchanged by us */
    }
    /* non-migrating fresh/​widen park */
    int target = cur | need;
    if (cur != 0 && target == cur) {    /* already-armed skip (zero syscall) */
        *ok = 1;
        return waiter | need;
    }
    int rc = demonic_ctl(target);
    if (rc == 0) { cache = target; *ok = 1; return waiter | need; }
    cache = (cur != 0) ? cur : 0;       /* arm failed: cache reflects reality */
    *ok = 0;
    return waiter;                      /* caller doesn't park on a failed arm */
}

int main(void)
{
    /* Arbitrary COHERENT start: cache == kernel, a pre-existing committed waiter
     * only on an actually-armed direction. */
    k_arm  = nondet_int() & (IN | OUT);
    cache  = k_arm;
    int waiter = nondet_int() & cache;      /* parked only where truly armed */

    /* An arbitrary register: fresh or migrating, for either direction. */
    int need = nondet_int() & (IN | OUT);
    __CPROVER_assume(need != 0);
    int migrating = nondet_bool();
    int ok = 0;
    waiter = reg(need, migrating, waiter, &ok);
    if (ok) waiter |= need;                 /* the committing caller is now parked */

    /* THE INVARIANT: every committed-parked fiber's direction is registered in the
     * kernel epoll -- else its event is delivered to nobody (a lost wake). */
    __CPROVER_assert((waiter & ~k_arm) == 0,
        "no fiber is parked on a direction the kernel epoll will not deliver");
    return 0;
}
