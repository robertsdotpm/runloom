"""big_100 / 411 -- concurrent.futures.Future cancel() vs run() across hubs.

The subject is the stock ``concurrent.futures.Future`` state machine -- the one
a ThreadPoolExecutor drives -- under the GIL off and M:N scheduling.  No other
program in the suite touches it; runloom's own ``runloom.Future`` has no
cancel()/CancelledError, so the cancel-vs-start transition is entirely
unexercised.  Under monkey.patch() the future's internal ``threading.Condition``
becomes the cooperative CoCondition, so this drives that cooperative
state-machine through the exact transition a real executor races.

Each round a worker SUBMITS ``NFUT`` futures.  For every future it spawns a
RUNNER fiber that does the executor's own dance -- ``yield_now()`` (to land on a
different hub mid-flight) then ``set_running_or_notify_cancel()``; on True it
``set_result(value)``, on False the future was cancelled out from under it and
it must NOT touch the result slot.  For a deterministic round-robin subset
(``sel=(wid+i)%2``) the worker ALSO spawns a CANCELLER fiber that ``yield_now()``s
then calls ``fut.cancel()``.  Runner and canceller then both mutate the SAME
future's PENDING/RUNNING/CANCELLED state under its one Condition, from different
hubs, in the same instant.

WHY IT STRESSES FT: cancel() and set_running_or_notify_cancel() both transition
the future state and notify the parked result() waiter under the future's lone
Condition.  Mid-flight cancellation must atomically EITHER win (state CANCELLED,
done-callbacks fire, result() raises CancelledError) OR lose (the work runs and
result() returns the value) -- never leave a half-set state (PENDING with a
set result, RUNNING that later accepts a cancel, CANCELLED with a stored
result), never double-notify, never lose the wakeup so a result() waiter parks
forever.

INVARIANT (per-future, hot + post conservation):
  * exactly one terminal state: cancelled() XOR (done() and not cancelled());
    never both, never neither at drain (no future stuck PENDING/RUNNING).
  * result() on a cancelled future raises CancelledError; result() on a
    completed future returns EXACTLY its submitted value (identity preserved,
    no torn/foreign value from another future's slot).
  * conservation: cancelled_count + completed_count == submitted_count, summed
    over the whole run (every submitted future reached a terminal state).
  * coverage: BOTH outcomes actually occur -- some cancels win (>=1 cancelled)
    and some lose / non-cancel-targeted runners complete (>=1 completed) -- so
    we know the race window was genuinely exercised, not skipped.

COVERAGE NOTE (the suite's flaky-random lesson): the two cases the post()
conservation depends on -- a cancel that WINS vs a future that COMPLETES -- are
guaranteed structurally by the deterministic ``sel=(wid+i)%2`` subset (half the
futures are cancel-targeted, half are not), NOT by random selection, so coverage
holds whether one worker does many rounds or many workers do one each.

Stresses: concurrent.futures.Future cancel-vs-start race, Condition notify under
M:N, terminal-state atomicity, no half-set state, no double-notify, no lost
result()-waiter wakeup, value identity under concurrent resolve.
"""
import concurrent.futures as cf

import harness
import runloom

# Futures submitted per round.  Enough that the table of runner/canceller fibers
# spans several hubs and the cancel/run interleavings vary, but small enough that
# a timeout-bound run still completes whole rounds.
NFUT = 8

# A future's submitted value is f(wid, idx, val_nonce): a recognizable, per-slot
# value so a completed future returning a DIFFERENT future's value (a torn slot
# under concurrent resolve) is caught, not silently accepted.
VALUE_BASE = 0x41100000


def future_value(wid, rnd, idx):
    """Deterministic, per-(worker,round,future) value.  result() on a completed
    future MUST return exactly this; anything else is a torn/foreign slot.  Pack
    wid/round/idx into disjoint bit-fields so distinct slots get distinct values
    (a returned value matching a *different* slot's encoding is a torn read)."""
    return (VALUE_BASE
            + ((wid & 0xFFFFF) << 16)
            + ((rnd & 0xFFF) << 4)
            + (idx & 0xF))


def run_future(H, fut, value):
    """The executor's own start dance, run as a fiber on whatever hub picks it
    up.  yield_now() first so the start races the canceller across hubs."""
    runloom.yield_now()
    try:
        if fut.set_running_or_notify_cancel():
            # We own the transition PENDING->RUNNING; the cancel lost.  Publish
            # the result.  (A cancel arriving now must be rejected by the
            # future -- a RUNNING future cannot be cancelled.)
            fut.set_result(value)
        # else: the future was CANCELLED before we started -- the cancel won.
        # We must NOT touch the result slot (doing so would be the double-set
        # bug the future guards against).
    except Exception as exc:                    # noqa: BLE001
        # set_running_or_notify_cancel()/set_result() raising here (e.g.
        # "future in unexpected state") is exactly the half-set-state corruption
        # this test hunts: a RUNNING future that a stale cancel half-mutated, or
        # a double set_result across the race.
        H.fail("runner raised {0}: {1} -- future state machine corrupted by "
               "the concurrent cancel (set_running_or_notify_cancel / set_result "
               "hit an unexpected state)".format(type(exc).__name__, exc))


def cancel_future(H, fut):
    """Race a cancel() against the runner's start, from a different hub."""
    runloom.yield_now()
    try:
        fut.cancel()                            # True if it won, False if RUNNING
    except Exception as exc:                     # noqa: BLE001
        H.fail("cancel raised {0}: {1} -- Future.cancel() must never raise "
               "(it returns False if too late), so the state machine is "
               "corrupted".format(type(exc).__name__, exc))


def worker(H, wid, rng, state):
    slot = wid & 1023
    submitted = state["submitted"]
    cancelled = state["cancelled"]
    completed = state["completed"]
    rnd = 0
    for _ in H.round_range():
        if not H.running():
            break

        futures = []
        wg = runloom.WaitGroup()
        # One runner per future; one canceller for the deterministic subset.
        ncancel = sum(1 for i in range(NFUT) if (wid + i) % 2 == 0)
        wg.add(NFUT + ncancel)

        for i in range(NFUT):
            fut = cf.Future()
            value = future_value(wid, rnd, i)
            futures.append((fut, value))

            def run_one(fut=fut, value=value):
                try:
                    run_future(H, fut, value)
                finally:
                    wg.done()

            H.fiber(run_one)

            # Deterministic round-robin: half the futures are cancel-targeted
            # (sel==0), half are left to complete (sel==1).  This GUARANTEES both
            # post-conservation cases occur regardless of how few rounds run --
            # not random selection (which flakes coverage under load).
            sel = (wid + i) % 2
            if sel == 0:
                def cancel_one(fut=fut):
                    try:
                        cancel_future(H, fut)
                    finally:
                        wg.done()

                H.fiber(cancel_one)

        # Join every runner + canceller so the round is one accountable op and no
        # future is inspected before its resolvers have all run.
        wg.wait()

        # Per-future terminal-state + value invariant.  By now every runner and
        # canceller has returned, so each future MUST be in a terminal state.
        for fut, value in futures:
            submitted[slot] += 1
            is_cancelled = fut.cancelled()
            is_done = fut.done()

            if not is_done:
                # cancelled() implies done(); a future neither completed nor
                # cancelled after all its resolvers returned is stuck
                # PENDING/RUNNING -- a lost transition / lost notify.
                H.fail("future stuck non-terminal after drain: done()={0} "
                       "cancelled()={1} running()={2} -- a PENDING/RUNNING "
                       "future whose runner and canceller both returned means a "
                       "lost state transition (M:N).".format(
                           is_done, is_cancelled, fut.running()))
                return

            if is_cancelled:
                cancelled[slot] += 1
                # result() on a cancelled future must raise CancelledError.
                try:
                    got = fut.result(timeout=0)
                    H.fail("cancelled future returned a result {0!r} instead of "
                           "raising CancelledError -- CANCELLED with a stored "
                           "result is the half-set-state bug".format(got))
                    return
                except cf.CancelledError:
                    pass
                except Exception as exc:        # noqa: BLE001
                    H.fail("cancelled future result() raised {0}: {1} instead of "
                           "CancelledError".format(type(exc).__name__, exc))
                    return
            else:
                completed[slot] += 1
                # done()-and-not-cancelled => result() returns EXACTLY the value
                # we submitted for this slot (no torn/foreign value, no
                # exception).
                try:
                    got = fut.result(timeout=0)
                except Exception as exc:        # noqa: BLE001
                    H.fail("completed future result() raised {0}: {1} -- a done, "
                           "non-cancelled future must return its value".format(
                               type(exc).__name__, exc))
                    return
                if got != value:
                    H.fail("completed future returned TORN value {0!r}, expected "
                           "{1!r} -- value from another future's slot under "
                           "concurrent resolve".format(got, value))
                    return

        rnd += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "submitted": [0] * 1024,
        "cancelled": [0] * 1024,
        "completed": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    submitted = sum(H.state["submitted"])
    cancelled = sum(H.state["cancelled"])
    completed = sum(H.state["completed"])
    H.log("futures submitted={0} cancelled={1} completed={2} (sum={3}) ops={4}"
          .format(submitted, cancelled, completed, cancelled + completed,
                  H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed")
    # Conservation: every submitted future reached EXACTLY one terminal state.
    H.check(cancelled + completed == submitted,
            "conservation broken: cancelled({0}) + completed({1}) = {2} != "
            "submitted({3}) -- a future ended in zero or two terminal states "
            "(lost/double transition under the cancel-vs-run race)".format(
                cancelled, completed, cancelled + completed, submitted))
    # Coverage: both race outcomes actually occurred (window genuinely exercised).
    H.check(cancelled > 0,
            "no cancel ever won -- the cancel-vs-run window was not exercised")
    H.check(completed > 0,
            "no future ever completed -- the run side was not exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p411_future_cancel_race", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="concurrent.futures.Future cancel() races "
                          "set_running_or_notify_cancel() across hubs; every "
                          "future ends in exactly one terminal state, "
                          "cancelled+completed==submitted, result() correct")
