/*
 * select_close.pml -- Promela model of runloom's select() Phase-2 park path
 * racing a concurrent send + close on the selected channel.
 *
 * This models the protocol where the 2026-05-31 select()+close() bug arc
 * lived (chan.c runloom_chan_select Phase 2 + runloom_chan_close); the abstract
 * fired_case CAS alone is in select_claim.pml, but the FOUR real bugs were
 * in how the selecting goroutine installs / aborts / parks / wakes and how
 * close delivers.  A faithful model of that protocol catches all four.
 *
 * One blocking select on one RECV case.  Concurrently: a sender may
 * deliver one value, and a closer may close.  Each, if it finds our waiter
 * installed and unclaimed, CASes fired_case -1 -> 0, fills the waiter, and
 * wakes us; otherwise it buffers the value / sets closed.  The selector
 * runs the real control flow: Phase-1 try, Phase-2 install, abort-on-ready
 * (checking the CAS result), park, and woken-result determination with a
 * spurious-wake retry.
 *
 * Properties (asserted at the selector's terminal `result`):
 *   WELL-FORMED   result is exactly one of: a real sent value, or CLOSED.
 *                 Never NULL (the close-wake-NULL crash) and never the
 *                 NO_CASE sentinel (a blocking select must never report
 *                 "nothing ready").
 *   CONSERVATION  if the sender CLAIMED our waiter (handed its value to
 *                 us), the selector returns exactly that value -- the
 *                 abort path must not evict+drop a just-delivered value.
 *   PROGRESS      the selector always terminates (no deadlock / no
 *                 infinite spurious-wake spin within the bounded model).
 *
 * Negative controls (each reintroduces one fixed bug; the model then
 * FAILS, proving the property has teeth):
 *   -DBUG_CLOSE_NULL   close-wake yields NULL instead of CLOSED        (#1)
 *   -DBUG_ABORT_NOCASE abort returns NO_CASE for a blocking select     (#2)
 *   -DBUG_ABORT_DROP   abort ignores its CAS result, evicts+drops value (#3)
 *   -DBUG_SPURIOUS     spurious wake returns an error instead of retry  (#4)
 */

#define NONE     0
#define VAL      7      /* the one value the sender may deliver */
#define CLOSED  -1
#define NO_CASE -3      /* "no case ready" sentinel (legal only with default) */
#define NULLV   -9      /* malformed NULL result (a bug)                       */
#define UNSET   -2

/* shared channel + select-park state */
bit closed       = 0;
int buffered     = NONE;   /* a value sitting in the ring buffer        */
int fired        = -1;     /* select fired_case: -1 unset, 0 = our case */
int waiter_val   = NONE;   /* value a sender handed into our waiter     */
bit waiter_closed = 0;     /* close handed "closed" into our waiter     */
bit installed    = 0;      /* our waiter is in the receiver queue       */
bit woken        = 0;      /* a wake is pending for the selector        */

int delivered = NONE;      /* value a deliverer CLAIMED us with (owe to us) */
bit sent      = 0;         /* VAL was successfully produced (claimed or buffered) */
int result    = UNSET;     /* selector's final result                       */
int nfin      = 0;

/* Sender: models chan_send_locked's locked decision in order --
 *   closed             -> send raises (nothing produced),
 *   claimable waiter    -> direct handoff (claim + wake),
 *   buffer has room     -> buffer it.
 * Guards are mutually exclusive so the order is faithful. */
proctype sender()
{
    atomic {
        if
        :: (closed) -> skip;                              /* send on closed: raises */
        :: (!closed && installed && fired == -1) ->
            fired = 0;                                     /* claim (CAS -1 -> 0) */
            waiter_val = VAL;
            delivered = VAL;
            sent = 1;
            woken = 1;
        :: (!closed && !(installed && fired == -1) && buffered == NONE) ->
            buffered = VAL; sent = 1;                      /* into the ring buffer */
        :: (!closed && !(installed && fired == -1) && buffered != NONE) -> skip;
        fi;
    }
    atomic { nfin++; }
}

/* The closer closes; if our waiter is installed + unclaimed, it claims us
 * with a "closed" result and wakes us. */
proctype closer()
{
    atomic {
        closed = 1;
        if
        :: (installed && fired == -1) ->
            fired = 0;                 /* claim */
#ifdef BUG_CLOSE_NULL
            waiter_val = NONE;         /* BUG #1: leaves a NULL result */
#else
            waiter_closed = 1;         /* fixed: deliver a proper CLOSED */
#endif
            woken = 1;
        :: else -> skip;
        fi;
    }
    atomic { nfin++; }
}

/* Optional spurious wake: the scheduler resumes the selector with no
 * channel having claimed it (a stale hub-submission).  Models the
 * fired_case < 0 wake the retry guard must absorb. */
proctype spurious()
{
    atomic {
        if :: (installed && fired == -1) -> woken = 1;   /* wake, but no claim */
           :: else -> skip;
        fi;
    }
    atomic { nfin++; }
}

proctype selector()
{
    int tries = 0;
    bit ab;

retry:
    tries++;
    assert(tries < 8);                 /* PROGRESS: bounded retries */

    /* ---- Phase 1: try without parking ----
     * Buffered values take PRIORITY over closed (chan.c drains the ring
     * before reporting closed); the guards are mutually exclusive so the
     * model enforces that order rather than picking nondeterministically. */
    atomic {
        if
        :: (buffered != NONE) -> result = buffered; buffered = NONE; goto done;
        :: (buffered == NONE && closed) -> result = CLOSED; goto done;
        :: (buffered == NONE && !closed) -> skip;
        fi;
    }

    /* ---- Phase 2: install our waiter ---- */
    atomic { installed = 1; }

    /* abort re-check: channel went ready (closed/value) since Phase 1? */
    atomic {
        if
        :: (closed || buffered != NONE) ->
            /* try to abort on this case: CAS fired_case -1 -> 0 */
            if
            :: (fired == -1) -> fired = 0; ab = 1;   /* we won the abort */
            :: else          -> ab = 0;              /* a delivery already claimed us */
            fi;
        :: else -> ab = 2;                           /* not ready: go park */
        fi;
    }

    if
    :: (ab == 1) ->
        /* Truly aborted on a ready channel.  Evict and re-scan. */
        atomic { installed = 0; fired = -1; }
#ifdef BUG_ABORT_NOCASE
        /* BUG #2: return select_try_each() directly -- which is NO_CASE
         * when the "ready" thing raced away to someone else. */
        atomic {
            if :: (buffered != NONE) -> result = buffered; buffered = NONE;
               :: (closed) -> result = CLOSED;
               :: else -> result = NO_CASE;       /* bare -1 for a blocking select! */
            fi;
        }
        goto done;
#else
        /* fixed: re-scan via retry (re-park if it raced away), never NO_CASE */
        woken = 0;
        goto retry;
#endif
    :: (ab == 0) ->
        /* A delivery claimed us during install (fired already 0). */
#ifdef BUG_ABORT_DROP
        /* BUG #3: ignore the lost CAS, evict everything and retry --
         * dropping the value already delivered into our waiter. */
        atomic { installed = 0; waiter_val = NONE; waiter_closed = 0; }
        woken = 0;
        goto retry;
#else
        /* fixed: stop, fall through to park; the pending wake resumes us
         * and we return the delivered value/closed below. */
        skip;
#endif
    :: (ab == 2) -> skip;     /* not ready: park */
    fi;

    /* ---- park: wait for a wake ---- */
    (woken == 1);

    /* ---- woken ----
     * Evict our waiter FIRST: once it is out of every channel's queue no
     * further claim can occur, so fired_case is henceforth stable.  THEN
     * read fired_case.  This closes the race where a delivery claims our
     * still-installed waiter between the wake and our decision: its value
     * is captured (waiter_val), not dropped by a premature retry. */
    atomic { installed = 0; }
    if
    :: (fired == -1) ->
        /* truly spurious: no channel claimed us (can't change now). */
#ifdef BUG_SPURIOUS
        result = NULLV;       /* BUG #4: return -2/NULL-ish (caller crashes) */
        goto done;
#else
        atomic { woken = 0; }                       /* retry the select */
        goto retry;
#endif
    :: (fired != -1) ->
        if
        :: (waiter_val != NONE) ->
            result = waiter_val; goto done;        /* real delivery */
        :: (waiter_val == NONE && waiter_closed) ->
            /* close-wake: re-scan so a value buffered in the
             * Phase-1->install window (or a now-ready sibling) drains
             * before we report closed.  Phase-1 returns VAL if buffered,
             * else CLOSED. */
            atomic { woken = 0; waiter_closed = 0; fired = -1; }
            goto retry;
        :: (waiter_val == NONE && !waiter_closed) ->
            result = NULLV; goto done;             /* neither set: bug #1 */
        fi;
    fi;

done:
    /* WELL-FORMED: a real value or CLOSED, never NULL or NO_CASE. */
    assert(result == VAL || result == CLOSED);
    /* CONSERVATION: if VAL was produced (claimed into our waiter OR
     * buffered), the sole receiver must return it -- buffered/claimed
     * values take priority over closed and must never be dropped by the
     * abort path.  (delivered implies sent, so this subsumes the claim
     * case too.) */
    assert(!sent || result == VAL);

    atomic { nfin++; }
}

init {
    atomic {
        run selector();
        run sender();
        run closer();
        run spurious();
    }
}
