"""big_100 / 131 -- one result, many joiners (broadcast correctness).

Each round a single "child" goroutine computes an outcome and publishes it to
MANY waiter goroutines via a broadcast: it stores the outcome in a shared cell
and then CLOSES a `ready` channel.  J joiners each block on `ready.recv()`
(a closed channel wakes every blocked receiver with ok=False -- a broadcast),
then read the stored cell.  Every joiner must observe the SAME outcome exactly
once.

The child randomly succeeds (stores ("ok", value)), fails (stores
("err", tag)), or is "cancelled" (stores ("cancel", None)) -- the outcome is
written BEFORE the channel close, so the happens-before of close->recv-wake
guarantees every joiner reads the finished cell.

Per-tree counters: each joiner records what it saw into its own slot of a
per-round list (single writer per joiner index), and the parent checks all J
entries are identical to the published outcome.  A cross-talk (a joiner seeing
another round's value) or a missed broadcast (a joiner that never woke -> the
WaitGroup never completes -> watchdog) fails.

Stresses: channel-close broadcast wake, many-receiver fan-out, shared-cell
publication ordering, no lost wake.
"""
import harness
import runloom


def joiner(ready, cell, results, idx, wg):
    """Block until ready closes, then record the published outcome."""
    try:
        _v, ok = ready.recv()           # closed channel -> ok False, broadcast
        # ok must be False (the channel is only ever closed, never sent to).
        # Read the published cell; it was stored before the close.
        results[idx] = (ok, cell[0])
    finally:
        wg.done()


def worker(H, wid, rng, state):
    joiner_obs = state["joiner_obs"]
    rounds_ok = state["rounds_ok"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        J = rng.randint(3, 12)
        ready = runloom.Chan(0)
        cell = [None]
        results = [None] * J            # one writer slot per joiner
        wg = runloom.WaitGroup()
        wg.add(J)
        for i in range(J):
            H.go(joiner, ready, cell, results, i, wg)

        # Compute and PUBLISH the outcome, then broadcast via close.
        r = rng.random()
        if r < 0.6:
            outcome = ("ok", (wid << 20) | (slot & 0xFFFFF))
        elif r < 0.85:
            outcome = ("err", "fail-{0}".format(wid))
        else:
            outcome = ("cancel", None)
        cell[0] = outcome
        ready.close()                   # wake every blocked joiner (broadcast)

        wg.wait()                       # every joiner must wake and record

        # Every joiner saw the identical outcome with ok=False.
        all_same = True
        for i in range(J):
            got = results[i]
            if got is None:
                all_same = False
                H.check(False, "joiner {0} never recorded (lost broadcast "
                               "wake)".format(i))
                break
            ok, val = got
            if ok is not False or val != outcome:
                all_same = False
                H.check(False, "joiner {0} saw {1!r} (ok={2}) expected outcome "
                               "{3!r} with ok=False".format(i, val, ok, outcome))
                break
        if not all_same:
            return
        joiner_obs[slot] += J
        rounds_ok[slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"joiner_obs": [0] * 1024, "rounds_ok": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    obs = sum(H.state["joiner_obs"])
    rok = sum(H.state["rounds_ok"])
    H.log("joiner_observations={0} rounds_ok={1} ops={2}".format(
        obs, rok, H.total_ops()))
    H.check(H.total_ops() > 0, "no broadcast rounds completed")
    H.check(obs > 0, "no joiners observed a result")


if __name__ == "__main__":
    harness.main("p131_multiple_joiners", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="one child result broadcast to J joiners via channel "
                          "close; every joiner sees the same outcome once")
