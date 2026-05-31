/*
 * select_claim.pml -- Promela model of the select() cross-channel claim
 * CAS in src/pygo_core/chan.c (waiter_claim / pygo_select_park).
 *
 * A goroutine blocked in select() installs a waiter on EVERY case's
 * channel, all sharing one `pygo_select_park` with a single
 * `fired_case` (init -1).  When any channel goes to deliver to such a
 * waiter it first CASes fired_case from -1 to its own case index; only
 * the winner performs the handoff + wakes the goroutine.  Losing
 * channels see a stale tombstone (CAS fails) and skip it.  This is the
 * lock-free heart of select: the per-channel locks do NOT serialise the
 * claim (it is global to the park), so multiple channels can race to
 * fire the same blocked select simultaneously.
 *
 * Proven (K=3 cases, every interleaving):
 *   AT MOST ONE FIRES   -- wins <= 1 at all times (select never fires two
 *                          cases / never double-delivers).
 *   EXACTLY-ONCE WAKE   -- wake_count == wins always: a win wakes the g
 *                          exactly once; a loss never wakes it.
 *   CONSISTENT RESULT   -- if anything fired, fired_case is a valid index
 *                          and equals the unique winner.
 *   NO DEADLOCK         -- no invalid end states.
 *
 * Each channel non-deterministically either attempts delivery (it had a
 * ready counterparty) or not (no counterparty) -- so wins may be 0 or 1.
 */

#define K 3

int fired_case = -1;
int wins       = 0;     /* channels that won the claim                 */
int wake_count = 0;     /* times the selecting goroutine was woken      */
int winner     = -1;    /* index of the winning case                   */
int nfin       = 0;

proctype deliverer(int idx)
{
    bit won;

    if
    :: skip ->                          /* this channel has a ready counterparty */
        atomic {                        /* waiter_claim: CAS fired_case -1 -> idx */
            if
            :: (fired_case == -1) -> fired_case = idx; won = 1;
            :: else               -> won = 0;     /* stale tombstone: skip  */
            fi;
        }
        if
        :: (won == 1) ->
            atomic {
                wins++;
                winner = idx;
                wake_count++;           /* wake the goroutine exactly once */
                assert(wins <= 1);
                assert(wake_count == wins);
            }
        :: else -> skip;               /* losing channel: no wake          */
        fi;
    :: skip -> skip;                    /* no counterparty: this case stays parked */
    fi;

    atomic {
        nfin++;
        if
        :: (nfin == K) ->
            assert(wins <= 1);
            assert(wake_count == wins);
            if
            :: (wins == 1) ->
                assert(fired_case == winner);
                assert(fired_case >= 0 && fired_case < K);
            :: else ->
                assert(fired_case == -1);
            fi;
        :: else -> skip;
        fi;
    }
}

init {
    atomic {
        run deliverer(0);
        run deliverer(1);
        run deliverer(2);
    }
}
