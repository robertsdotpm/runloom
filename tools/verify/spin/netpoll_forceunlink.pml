/*
 * netpoll_forceunlink.pml -- Promela model of the parker-release lifetime in
 * src/runloom_c/netpoll.c: runloom_netpoll_force_unlink_g_parker (the g-completion
 * safety net) racing the pump for the same parker.  This is the "exactly-once
 * pool_release" question.
 *
 * THE SETUP.  A parker `p` lives on the parking g's coroutine stack and is
 * tracked by g->netpoll_parker (the "token").  runloom_parker_unlink (under
 * pool->lock) clears that token whenever it actually removes p from the pool
 * (netpoll_parker_link.c.inc, runloom_parker_unlink).  Three sites touch p:
 *
 *   - pump (runloom_pump_dispatch_event / drain_expired): under pool->lock,
 *     unlinks p (clearing the token) and -- if it claimed a PARKED g --
 *     re-queues it.  The pump NEVER releases p.  The woken g resumes in
 *     wait_fd and releases p itself (the release rides on the wake).
 *   - wait_fd: on every exit path releases p, AFTER clearing the token.
 *   - force_unlink (g completion, hub_main): the safety net for a parker still
 *     tracked when the g is torn down without wait_fd cleaning up.  It takes
 *     pool->lock, RE-READS the token under the lock (netpoll_wake_iouring.c.inc, runloom_netpoll_force_unlink_g_parker --
 *     "in case g->netpoll_parker was cleared by a concurrent unlink between the
 *     check above and the lock acquire"), unlinks + clears + releases ONLY if it
 *     still saw the token set.
 *
 * wait_fd and force_unlink run on the SAME thread in program order (the g's
 * coroutine, then hub_main's completion), so they never race each other; the
 * token wait_fd clears is visible to force_unlink.  The genuine race is
 * force_unlink (completion thread) vs the pump (a poller thread) for the same p,
 * which is about to be released back to the pool and re-issued to another g.
 *
 * PROVEN (force_unlink racing one pump for one parker; the pump's unlink hands
 * the release to the resumed g):
 *   EXACTLY-ONCE RELEASE -- `released <= 1`: p is released by exactly one of
 *                    {the resumed g (riding the pump's wake) , force_unlink},
 *                    never both.  The under-lock token re-check is what makes
 *                    the loser observe the cleared token and decline to release.
 *   NO USE-AFTER-FREE -- no actor unlinks or releases p after it is freed
 *                    (`assert(!freed)` guards every access): once unlinked under
 *                    the lock, a later pump pass cannot find p, and force_unlink
 *                    cannot release a parker the resumed g already returned.
 *
 * Negative control -DBUG_NO_RECHECK drops the under-lock re-read: force_unlink
 * trusts the stale cheap-path token it sampled before taking the lock and
 * releases unconditionally.  Spin finds the double-free: the pump unlinks +
 * wakes the g (which resumes and releases p), and force_unlink -- still holding
 * the stale "token set" -- releases the same parker again.
 */

bit token  = 1;   /* g->netpoll_parker set: parker is tracked        */
bit linked = 1;   /* parker is in the pool's lists/bucket/heap        */
bit freed  = 0;   /* parker has been returned to the pool (released)  */
int released = 0; /* number of pool_release calls on this parker      */

bit pump_unlinked = 0;  /* pump removed p (and thus woke its g)        */

bit lock = 0;     /* pool->lock */

#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

/* The pump delivers a late fd event for p's fd.  If it finds p still linked it
 * claims + unlinks it (clearing the token) and re-queues the g; the woken g
 * then resumes wait_fd and releases p.  The release rides causally on the wake,
 * so we model it as the pump's downstream effect. */
proctype pump()
{
    LOCK;
    if
    :: linked ->
        assert(!freed);            /* NO UAF: pump must not touch a freed parker */
        linked = 0;
        token  = 0;                /* runloom_parker_unlink clears g->netpoll_parker */
        pump_unlinked = 1;
    :: else -> skip;               /* already unlinked (force_unlink beat us) */
    fi;
    UNLOCK;

    /* The woken g resumes in wait_fd and releases p (only if the pump actually
     * woke it -- i.e. it found p linked).  Causally after the wake above. */
    if
    :: pump_unlinked ->
        assert(!freed);            /* NO double-free / UAF */
        freed = 1;
        released++;
        assert(released <= 1);
    :: else -> skip;
    fi;
}

/* g-completion safety net: runloom_netpoll_force_unlink_g_parker. */
proctype force_unlink()
{
    bit cheap;
    bit recheck;
    bit do_release = 0;

    /* Cheap path: sample the token without the lock. */
    cheap = token;
    if
    :: cheap == 0 -> goto done;    /* nothing tracked: return early */
    :: else -> skip;
    fi;

    LOCK;
#ifndef BUG_NO_RECHECK
    recheck = token;               /* RE-READ under the lock (runloom_netpoll_force_unlink_g_parker) */
#else
    recheck = 1;                   /* BUG: trust the stale cheap-path sample */
#endif
    if
    :: recheck ->
        assert(!freed);            /* NO UAF: never unlink a freed parker */
        linked = 0;
        token  = 0;
        do_release = 1;
    :: else -> skip;               /* token cleared under us: decline */
    fi;
    UNLOCK;

    if
    :: do_release ->
        assert(!freed);            /* NO double-free */
        freed = 1;
        released++;
        assert(released <= 1);
    :: else -> skip;
    fi;
done:
    skip;
}

init {
    atomic {
        run pump();
        run force_unlink();
    }
}
