"""big_100 / 202 -- channel close vs select race.

Each round, a coordinator goroutine creates a small batch of fresh unbuffered
channels and spawns a group of SELECTOR goroutines that all block in a multi-way
`select` over that batch (no default -> a real park).  A CLOSER goroutine then
`close()`s every channel in the batch.

A blocking `select` recv-case on a closed channel must wake and report
`(idx, (value, ok=False))` -- exactly once, never a phantom value, never a
missed wake (which would hang the watchdog).  Every selector must wake with
ok=False; the coordinator joins them (`JoinSet`) so a single lost wake stalls
the round and the watchdog catches it.

Stresses: close-while-parked-in-select, ok=False delivery on a closed channel,
no double-wake, no missed wake (no hang), wake of MANY selectors by one close.

Invariant (post): every selector that parked woke exactly once with ok=False
(wakes_okfalse == selectors_parked); zero phantom values seen after close.
"""
import harness
import runloom
import runloom.sync as sync

BATCH_CHANS = 6            # channels per round
SELECTORS = 8             # goroutines blocked in select per round


def setup(H):
    n = H.funcs
    H.state = {
        # per-coordinator single-writer slots
        "parked": [0] * n,         # selector parks initiated
        "okfalse": [0] * n,        # wakes that reported ok=False (correct)
        "phantom": [0] * n,        # wakes that reported a value after close (BUG)
    }


def coordinator(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        chans = [runloom.Chan(0) for _ in range(BATCH_CHANS)]
        cases = [("recv", ch) for ch in chans]
        parked = [0]
        okf = [0]
        phantom = [0]

        def selector():
            parked[0] += 1            # single-writer: only this coordinator's
            # goroutines touch these lists, and JoinSet runners on this batch
            # run on the scheduler; the increments race only with siblings.
            # Use per-selector return value instead to stay race-free:
            try:
                idx, (val, ok) = runloom.select(cases)
            except Exception:
                return ("err", None)
            if ok:
                return ("phantom", val)   # value delivered after close -> BUG
            return ("okfalse", idx)

        js = sync.JoinSet()
        for _ in range(SELECTORS):
            js.spawn(selector)

        # Give the selectors a beat to actually park in select, then close all
        # channels so every parked select must wake with ok=False.
        runloom.sleep(0.0005)
        for ch in chans:
            ch.close()

        results = js.join_all()       # MUST return -- a lost wake hangs here
        okcount = 0
        ph = 0
        for kind, _payload in results:
            if kind == "okfalse":
                okcount += 1
            elif kind == "phantom":
                ph += 1
        state["parked"][wid] += len(results)
        state["okfalse"][wid] += okcount
        state["phantom"][wid] += ph
        H.op(wid, okcount)
        H.task_done(wid)


def body(H):
    # Each coordinator round spawns SELECTORS sub-goroutines; cap concurrent
    # coordinators so we don't explode the live-goroutine count.
    H.run_pool(H.funcs, coordinator, H.state, max_concurrent=3000)


def post(H):
    parked = sum(H.state["parked"])
    okfalse = sum(H.state["okfalse"])
    phantom = sum(H.state["phantom"])
    H.check(phantom == 0,
            "PHANTOM value(s) after close: {0} selects got a value with "
            "ok=True on a closed channel".format(phantom))
    H.check(okfalse == parked,
            "missed/extra wake: {0} selectors parked but {1} woke with "
            "ok=False (diff {2})".format(parked, okfalse, parked - okfalse))
    H.log("selectors_parked={0} ok_false_wakes={1} phantom={2}".format(
        parked, okfalse, phantom))


if __name__ == "__main__":
    harness.main("p202_channel_close_select_race", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="close() wakes every parked select with ok=False "
                          "exactly once; no phantom value, no missed wake")
