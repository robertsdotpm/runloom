"""big_100 / 410 -- Future.add_done_callback fired inline during completion,
while as_completed() / result() waiters park on other hubs.

No existing program drives the fiber-backed ThreadPoolExecutor.  Under
runloom.monkey.patch() concurrent.futures.ThreadPoolExecutor is the
CoThreadPoolExecutor: each submitted callable runs as an M:N fiber, and the
stock concurrent.futures.Future resolves through a (now cooperative) Condition.
The FT hazard lives in the completion path:

  * Future.add_done_callback runs every registered callback INLINE inside
    set_result / set_exception (and inside the cancel path's
    set_running_or_notify_cancel), on whichever fiber completed the future --
    NOT on the fiber that called result()/as_completed();
  * meanwhile as_completed() and result() waiters are parked on the future's
    Condition on OTHER hubs.  Completing a future therefore (a) walks and fires
    its callback list, (b) flips its state, and (c) wakes the cross-hub waiter,
    all from the completing fiber while the waiter is descheduled.

That is callback-during-completion plus cross-hub wake.  If a callback that
touches shared state, or one that RAISES, can corrupt the waiter set, drop a
callback, double-fire one, or swallow the wakeup, the conservation counts below
break or as_completed wedges (HANG).

== The closed-world oracle ==
Each worker round submits a batch of N futures to a private executor and checks
two race-free invariants:

  * PER-FUTURE callback firing: every future is given add_done_callback(cb)
    where cb bumps that future's OWN one-element cell.  concurrent.futures
    guarantees each registered callback fires EXACTLY ONCE, for finished AND
    cancelled futures alike, so after the round every cell MUST equal 1 -- a 0
    is a lost inline invocation (swallowed under the cross-hub wake), a >1 is a
    double fire / corrupted callback re-walk.  CRUCIAL: the callbacks fire INLINE
    on the COMPLETING fibers, which run concurrently on different hubs, so a
    SINGLE shared `tally += 1` bumped by all N callbacks is itself a raced
    read-modify-write that loses updates with the GIL off -- a flaw in the
    OBSERVER that masquerades as a "lost callback".  Per-future single-writer
    cells (each written by exactly one callback) are race-free and keep the
    real invariant ("each callback fires once") intact.  Likewise the per-worker
    aggregate uses index==wid slots, NOT wid&1023 (which aliases past 1024
    workers and would corrupt the global sum).

  * the as_completed() pass MUST yield each submitted future EXACTLY ONCE: we
    collect id(f) into a set and require it to equal the submitted id-set, and
    require the count of yields to equal N (a double-yield or a dropped future
    both fail).  Result values are checked against the deterministic g(payload)
    so a torn/mis-routed result is caught too.

== Three round-robined cases (coverage, NOT random) ==
A callback that RAISES is the sharp probe: concurrent.futures._base catches it
per-callback and must still fire every OTHER callback and complete the future.
So we exercise three cases, ROUND-ROBINED by worker id over the first ops (pure
random selection reliably misses a case at the handful-of-ops-per-run that a
timeout-bound load permits -- the p125/p126/p172 flaky-coverage lesson):

  * case 0 CLEAN   : only the tally callback on each future.
  * case 1 RAISING : each future ALSO gets a callback that raises ValueError;
                     it must not lose the tally callback nor any as_completed
                     item.  (_invoke_callbacks logs each such exception; we
                     raise the concurrent.futures logger to CRITICAL in setup
                     so the deliberate, tested noise doesn't swamp the run.)
  * case 2 CANCEL  : we try to cancel a third of the futures before they start;
                     a cancelled future STILL fires its done-callback exactly
                     once and STILL appears once in as_completed, so the same
                     conservation counts hold across the cancel path.

Invariant (hot, fail-fast + post conservation): per round every future's cell
== 1, as_completed yields the submitted id-set exactly once (count==N), every
non-cancelled result==g(payload); globally total callbacks fired == N*rounds;
and each of the three cases ran at least once across the run.  Any drift is an
M:N completion-path corruption / lost wake.

Stresses: Future.add_done_callback inline-during-set_result/set_exception,
raising-callback isolation, cancel-path callback, cross-hub as_completed/result
park-and-wake, fiber-backed ThreadPoolExecutor under many concurrent M:N
workers.
"""
import logging

import harness
import runloom

# Futures per worker round.  Big enough that the executor's max_workers bound
# forces real queue backlog (so some submits race completion of earlier ones and
# the cancel case has un-started futures to cancel), small enough that a
# timeout-bound worker still finishes whole rounds.
BATCH = 24

# Executor concurrency cap per worker.  < BATCH so the work queue genuinely backs
# up: completing fibers fire callbacks while later submits / the as_completed
# waiter are still parked, which is the cross-hub window we want.
MAX_WORKERS = 4

NCASES = 3
CASE_CLEAN = 0
CASE_RAISING = 1
CASE_CANCEL = 2


def g(payload):
    """Deterministic task result.  A non-cancelled future whose result is not
    g(its payload) is a torn/mis-routed completion."""
    return (payload ^ 0x6C6C6C6C) + 0x9E3779B9


def raising_cb(fut):
    """A done-callback that always raises.  concurrent.futures._base catches it
    per-callback (logs via LOGGER.exception, silenced in setup), so it must NOT
    prevent any OTHER callback on the same future from firing, nor corrupt the
    future's completion or the as_completed/result waiter set."""
    raise ValueError("deliberate done-callback failure (case RAISING)")


def run_batch(H, wid, rng, case):
    """One round: submit BATCH futures to a private fiber-backed executor, attach
    a per-future tally callback (plus a raising callback in the RAISING case),
    optionally cancel a third (CANCEL case), then drain via as_completed().
    Returns the number of callbacks that fired (== BATCH) on a clean,
    conservation-consistent round, or False on any invariant break (H.fail
    already recorded).

    COUNTING DISCIPLINE (the FT trap, and why we DON'T share a counter): each
    future's done-callback fires INLINE inside set_result/set_exception (and the
    cancel path), on whichever fiber COMPLETED the future -- and with the fiber-
    backed executor those completing fibers run CONCURRENTLY on different hubs.
    A single shared `fired += 1` bumped by all BATCH callbacks is therefore a
    read-modify-write raced across hubs, and loses updates with the GIL off
    (the harness's "NEVER a shared x += 1 across hubs" rule).  That is a flaw in
    the OBSERVER, not the system, so instead each future gets its OWN one-element
    cell that ONLY that future's callback writes (single-writer, lock-free).  The
    real, falsifiable invariant survives intact: every future's done-callback
    must fire EXACTLY once (cell == 1) -- a 0 is a lost inline invocation under
    the cross-hub wake, a >1 is a double fire / corrupted callback re-walk."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ex = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    submitted_ids = set()
    payload_of = {}
    cell_of = {}                # id(fut) -> [count]; one single-writer cell each
    try:
        futs = []
        for i in range(BATCH):
            payload = (wid << 20) ^ (rng.getrandbits(24) + i)
            fut = ex.submit(g, payload)
            fid = id(fut)
            payload_of[fid] = payload
            cell = [0]
            cell_of[fid] = cell

            def tally_cb(f, cell=cell):
                # Fires INLINE on the COMPLETING fiber (possibly a different hub
                # than the one that registered it).  Writes ONLY its own cell.
                cell[0] += 1

            # tally_cb on EVERY future in every case -> the per-future invariant
            # (cell == 1) is uniform and case-independent.
            fut.add_done_callback(tally_cb)
            if case == CASE_RAISING:
                # Registered AFTER tally_cb; it raises, and must NOT prevent
                # tally_cb (already fired) nor corrupt completion / the waiter set.
                fut.add_done_callback(raising_cb)
            submitted_ids.add(fid)
            futs.append(fut)

        if case == CASE_CANCEL:
            # Try to cancel a third before they start.  A successful cancel STILL
            # fires the done-callback exactly once (via cancel()'s own
            # _invoke_callbacks) and STILL surfaces in as_completed once, so the
            # per-future invariant is unchanged; an unsuccessful cancel (already
            # running/done) is fine too.
            for i, fut in enumerate(futs):
                if i % 3 == 0:
                    fut.cancel()

        seen_ids = set()
        seen = 0
        for fut in as_completed(futs):
            seen += 1
            fid = id(fut)
            if fid in seen_ids:
                H.fail("as_completed yielded future {0:#x} TWICE -- duplicate "
                       "wake / corrupted waiter set (M:N completion path)"
                       .format(fid))
                return False
            if fid not in submitted_ids:
                H.fail("as_completed yielded an UNSUBMITTED future {0:#x} -- "
                       "torn waiter set".format(fid))
                return False
            seen_ids.add(fid)
            if not fut.cancelled():
                val = fut.result()
                want = g(payload_of[fid])
                if val != want:
                    H.fail("future result {0!r} != g(payload) {1!r} -- torn / "
                           "mis-routed completion (case {2})"
                           .format(val, want, case))
                    return False

        # Conservation 1: as_completed yielded EVERY submitted future EXACTLY
        # once (count and identity set both).
        if seen != BATCH:
            H.fail("as_completed yielded {0} futures, expected {1} -- a future "
                   "was dropped or double-yielded (case {2})"
                   .format(seen, BATCH, case))
            return False
        if seen_ids != submitted_ids:
            H.fail("as_completed id-set != submitted id-set (missing={0} "
                   "extra={1}) -- waiter-set corruption (case {2})"
                   .format(len(submitted_ids - seen_ids),
                           len(seen_ids - submitted_ids), case))
            return False
    finally:
        # shutdown(wait=True) joins every task fiber; since set_result runs
        # _invoke_callbacks before the task's done-event is set, after this
        # returns every (non-cancelled) future's callback has run.  Cancelled
        # futures fired theirs synchronously inside cancel().  So the cells are
        # stable to read once shutdown returns.
        ex.shutdown(wait=True)

    # Conservation 2 (hot): each future's done-callback fired EXACTLY once.  Read
    # via the SINGLE-WRITER per-future cells (no shared counter, so no lost
    # update can masquerade as a lost callback).
    fired = 0
    for fid in submitted_ids:
        c = cell_of[fid][0]
        if c != 1:
            H.fail("future {0:#x} done-callback fired {1} times, expected 1 -- "
                   "{2} inline callback under cross-hub completion (case {3}, "
                   "wid {4})".format(fid, c,
                                     "LOST" if c == 0 else "DOUBLE-FIRED",
                                     case, wid))
            return False
        fired += c
    return fired


def worker(H, wid, rng, state):
    # Per-WORKER-unique slots (index == wid, sized to the worker count): this
    # worker is the SOLE writer of fired_total[wid] and case_hits[c][wid], so the
    # global post() conservation has no cross-worker aliasing (a wid&1023 slot
    # aliases once funcs>1024 and two concurrent workers would corrupt the sum).
    fired_total = state["fired_total"]
    case_hits = state["case_hits"]
    i = 0
    cb_sum = 0
    hits = [0, 0, 0]
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases over the first ops keyed off worker id, so
        # coverage holds whether one worker does NCASES ops or NCASES workers do
        # one each (the suite's flaky-random-coverage fix; see p125).  Random
        # after that to preserve the concurrent mix.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        registered = run_batch(H, wid, rng, case)
        if registered is False:
            return                  # H.fail already recorded (conservation 2)
        cb_sum += registered        # accumulate THIS worker's verified callbacks
        hits[case] += 1
        H.op(wid)
        H.task_done(wid)
    # Publish this worker's totals to its OWN slots once, at the end of its life.
    fired_total[wid] = cb_sum
    for c in range(NCASES):
        case_hits[c][wid] = hits[c]


def setup(H):
    # The RAISING case deliberately raises inside a done-callback;
    # concurrent.futures._base logs each via logging.exception, which would dump
    # a traceback per future and swamp the run.  Silence that one logger -- the
    # behaviour under test (every OTHER callback still fires, future still
    # completes) is asserted by the conservation counts, not the log.
    logging.getLogger("concurrent.futures").setLevel(logging.CRITICAL)
    n = H.funcs
    H.state = {
        "fired_total": [0] * n,
        "case_hits": [[0] * n for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = sum(H.state["case_hits"][CASE_CLEAN])
    raising = sum(H.state["case_hits"][CASE_RAISING])
    cancel = sum(H.state["case_hits"][CASE_CANCEL])
    total_cb = sum(H.state["fired_total"])
    rounds = clean + raising + cancel
    H.log("rounds: clean={0} raising={1} cancel={2} (total {3}); callbacks "
          "fired={4} ops={5}".format(clean, raising, cancel, rounds,
                                     total_cb, H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(clean > 0, "CLEAN case never exercised")
    H.check(raising > 0, "RAISING case never exercised")
    H.check(cancel > 0, "CANCEL case never exercised")
    # Global conservation: every round registered BATCH callbacks and each must
    # have fired exactly once, so total callbacks fired == BATCH * rounds.  (Each
    # round's per-round delta was already checked in run_batch; this aggregate is
    # the independent end-of-run cross-check across the whole worker population.)
    H.check(total_cb == BATCH * rounds,
            "total done-callbacks fired {0} != BATCH*rounds {1} -- a callback "
            "was lost or double-fired across the run".format(
                total_cb, BATCH * rounds))
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p410_future_done_callback", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="fiber-backed ThreadPoolExecutor: add_done_callback "
                          "fires inline during set_result/set_exception/cancel "
                          "while as_completed waiters park cross-hub; every "
                          "callback fires once, as_completed yields each future "
                          "once, raising callback loses nothing")
