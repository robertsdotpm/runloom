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
 * Configs (compile-time), each a separate CBMC run:
 *   (default)          shipped fix, with the ADD-failure recovery modelled -> PASS
 *   -DBUG_ARM_DROP     the 2026-07-02 migration bug (target = need) -> must FAIL
 *                      (the negative control: proves the harness has teeth)
 *   -DNO_ADD_RECOVERY  shipped fix but assume NOTHING recovers a failed ADD ->
 *                      must FAIL, isolating the EXACT kernel-return the fix leans
 *                      on a recovery (stale-arm probe / park timeout) to survive.
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
        /* ADD failed after DEL: the caller propagates the error and does NOT park
         * (no new committed waiter).  The arm cache is cleared (as :221 does). */
        cache = 0;
        *ok = 0;
#ifndef NO_ADD_RECOVERY
        /* SHIPPED recovery for a pre-existing waiter stranded by DEL-ok+ADD-fail:
         * level-triggered re-report on the NEXT register, the stale-arm self-heal
         * probe, and the park's own timeout all re-register the fd so `waiter`'s
         * direction is delivered again.  Model that recovery as: the kernel ends
         * up registered for the still-parked waiters. */
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
