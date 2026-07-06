/* netpoll_refinement_cbmc.c -- TWO-LEDGER refinement check for the netpoll arm
 * cache vs the kernel epoll registration, under a demonic OUT-OF-BAND event
 * alphabet (item 9).  Generalises netpoll_arm_demonic_cbmc.c (which covers the
 * register/migrate seam) to the whole fd lifecycle: register, migrate, close,
 * fd-number reuse, fork, and scheduler-mode switch, applied in an arbitrary
 * bounded sequence chosen by the demon.
 *
 * THE LEDGERS:
 *   cache  -- runloom's arm mask (runloom_fd_armed): directions it BELIEVES are
 *             registered, keyed by fd NUMBER (outlives the fd -- that is the trap).
 *   kern   -- directions the epoll will ACTUALLY deliver, and whether that epoll
 *             is pumped by a live hub.
 *   wait   -- directions with a fiber committed-PARKED, waiting for a wake.
 *   cancel -- have the parkers been given a pending cancel/error-wake?
 *
 * THE REFINEMENT INVARIANT (a committed parker never waits on nothing):
 *   wait != 0  ==>  ( (wait & kern) == wait AND kern_pumped )   -- a live epoll
 *                                                                   will deliver it
 *                OR  cancel                                      -- or it is being
 *                                                                   error-woken.
 * A parker that satisfies NEITHER is a silent lost wake -> hang forever.
 *
 * Each out-of-band letter has a -DBUG_* negative control that omits the cache
 * maintenance the real code must do; each MUST make the invariant FAIL (teeth --
 * else the letter is unmodelled / the assert is dead).  The default models what
 * the code does today and MUST verify.
 *
 * Configs:
 *   (default)                    -> VERIFY
 *   -DBUG_CLOSE_NO_INVALIDATE    close() leaves cache set + parkers unwoken -> FAIL
 *   -DBUG_FORK_NO_RESET          child keeps parent cache; child epoll empty -> FAIL
 *   -DBUG_REUSE_NO_CLEAR         reused fd number keeps stale cache -> FAIL
 *   -DBUG_MODESWITCH_NO_REPUMP   mode switch leaves fd in an unpumped epoll -> FAIL
 */

#define R 1
#define W 2

int  nondet_int(void);
_Bool nondet_bool(void);

/* the two ledgers + parker state */
static int  cache;        /* arm mask runloom thinks is registered */
static int  kern;         /* mask actually in the epoll */
static int  kern_pumped;  /* is that epoll pumped by a live hub? */
static int  wait;         /* directions with a committed parker */
static int  cancel;       /* parkers have a pending cancel/error-wake */

static int dir(void) { return nondet_int() & (R | W); }

/* register(need): the faithful shipped logic.  A demonic epoll_ctl may fail; on
 * failure the code clears the cache and error-wakes pre-existing parkers (the
 * fix in netpoll_register.c.inc).  On success the fd sits in a pumped epoll. */
static void op_register(void)
{
    int need = dir();
    if (need == 0) return;
    /* ALREADY-ARMED SKIP (the zero-syscall hot path): if the cache already
     * claims `need`, the code TRUSTS it, issues no epoll_ctl, and parks.  If the
     * cache is stale (a reused fd number, an un-invalidated close), this parks on
     * a direction the kernel is not registered to deliver -> the lost wake.  This
     * is the whole reason the cache MUST be cleared on close/reuse/fork. */
    if ((cache & need) == need) {
        wait |= need;                    /* park now; kern is trusted, not re-checked */
        return;
    }
    cache |= need;
    if (nondet_bool()) {                 /* epoll_ctl succeeded */
        kern = cache;
        kern_pumped = 1;
    } else {                             /* epoll_ctl failed after any DEL */
        cache = 0;
        kern = 0;
        cancel = 1;                      /* error-wake pre-existing parkers */
    }
    /* a committing caller may now be parked on `need` in a live epoll */
    if (kern_pumped && (need & kern) == need)
        wait |= need;
}

/* close(fd): the kernel auto-removes the registration (kern=0).  The runtime
 * close hook MUST invalidate the cache and cancel-wake any parkers -- the kernel
 * emits no event for a closed fd, so the wake is the runtime's job. */
static void op_close(void)
{
    kern = 0;
    kern_pumped = 0;
#ifndef BUG_CLOSE_NO_INVALIDATE
    cache = 0;
    if (wait) cancel = 1;
#endif
}

/* fd-number reuse: a fresh object takes the same fd number.  A stale cache from
 * the old fd must be cleared before the first park, or the "already armed" skip
 * fires and the new fd is never actually registered. */
static void op_reuse(void)
{
    kern = 0;
    kern_pumped = 0;
    wait = 0;                            /* the old fd's parkers are gone */
    cancel = 0;
#ifndef BUG_REUSE_NO_CLEAR
    cache = 0;                           /* close/reuse hook clears the stale arm */
#endif
}

/* fork(): the child's epoll instance does not inherit the parent's registrations
 * (an epoll fd is not usefully shared across fork); the child's cache, copied
 * from the parent, claims registrations the child epoll does not have.  A
 * pthread_atfork child handler must reset the cache. */
static void op_fork_child(void)
{
    kern = 0;
    kern_pumped = 0;
#ifndef BUG_FORK_NO_RESET
    cache = 0;
    if (wait) cancel = 1;
#endif
}

/* scheduler-mode / owner switch: the fd migrates ownership; if the new owner's
 * epoll is not (yet) pumped, a re-register/re-pump must restore delivery. */
static void op_modeswitch(void)
{
#ifndef BUG_MODESWITCH_NO_REPUMP
    if (cache != 0) { kern = cache; kern_pumped = 1; }   /* migrate + re-pump */
#else
    kern_pumped = 0;                                     /* left unpumped */
#endif
}

static void check(void)
{
    __CPROVER_assert(
        wait == 0 || (((wait & kern) == wait) && kern_pumped) || cancel,
        "no committed parker waits on a direction no live epoll will deliver");
}

int main(void)
{
    /* coherent start: nothing armed, nothing parked. */
    cache = 0; kern = 0; kern_pumped = 0; wait = 0; cancel = 0;

    /* apply a bounded arbitrary sequence of out-of-band letters, checking the
     * refinement invariant after every step. */
    for (int i = 0; i < 4; i++) {
        switch (nondet_int() & 7) {
            case 0: op_register();   break;
            case 1: op_close();      break;
            case 2: op_reuse();      break;
            case 3: op_fork_child(); break;
            case 4: op_modeswitch(); break;
            default: /* a spontaneous wake clears a parker (no-op on the ledger) */
                break;
        }
        check();
    }
    return 0;
}
