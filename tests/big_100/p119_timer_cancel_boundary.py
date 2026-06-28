"""big_100 / 119 -- timer/data race at the cancellation boundary.

Each round arranges a data delivery and a timeout to fire at ~the same instant
and `select`s over them.  A helper goroutine `ch.send`s after a delay d; a
`runloom.time.After(d')` timer races it with d' drawn near d (plus jitter).
Exactly one case wins; the loser is left to be drained/GC'd; the op resolves
exactly once -- no double-resume, no lost wakeup that wedges the round.

The invariant is conservation: every round resolves as EITHER a data-win OR a
timeout-win, counted once.  data_wins + timeout_wins == ops, and per-worker the
two slots sum to the rounds that completed.  A double-resume would over-count;
a lost wakeup would hang (watchdog).

Stresses: select at the timer/data boundary, After() one-shot timer, park/wake
race resolution, no double-resume.
"""
import harness
import runloom
import runloom.time as rtime


def helper(ch, delay):
    """Sleep `delay`, then try to deliver a token.  If the recv side already
    took the timeout branch nobody is listening; the unbuffered send would park
    forever, so use a 1-buffered channel (created by the worker) and try_send."""
    runloom.sleep(delay)
    try:
        ch.try_send(b"D")
    except Exception:
        pass


def worker(H, wid, rng, state):
    data_wins = state["data_wins"]
    timeout_wins = state["timeout_wins"]
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # A 1-buffered data channel so the helper never blocks even if the
        # worker already resolved on the timeout branch.
        ch = runloom.Chan(1)
        # The post-check requires BOTH branches to win at least once, but at
        # small op counts the near-tie race occasionally hands every round to
        # the same side (data_wins=ops, timeout_wins=0 or vice-versa) and the
        # coverage check flakes -- the select itself is correct (conservation
        # always holds; no double-resume).  Seed each worker's first two ops to
        # round-robin which side WINS, keyed off its id, by skewing the two
        # delays far apart so the chosen branch deterministically wins.  This
        # guarantees coverage whether one worker manages two ops or many workers
        # manage one each.  After that, fall back to the original near-tie race
        # so the genuine boundary mix is preserved.
        if i < 2:
            if (wid + i) & 1 == 0:
                # Force a data win: token arrives well before the timer fires.
                d = rng.uniform(0.0005, 0.004)
                dprime = d + rng.uniform(0.01, 0.02)
            else:
                # Force a timeout win: timer fires well before the token arrives.
                d = rng.uniform(0.01, 0.02)
                dprime = 0.0
        else:
            d = rng.uniform(0.0005, 0.004)
            # Timer fires at ~d with jitter so the two genuinely race.
            dprime = d + rng.uniform(-0.0015, 0.0015)
            if dprime < 0.0:
                dprime = 0.0
        i += 1
        H.fiber(helper, ch, d)
        timer = rtime.After(dprime)
        idx, _payload = runloom.select([("recv", ch), ("recv", timer)])
        if idx == 0:
            data_wins[slot] += 1
        else:
            timeout_wins[slot] += 1
        # The loser is abandoned: a 1-buffered ch holds the late token harmlessly
        # (GC'd with ch); the After timer either already closed or closes itself.
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"data_wins": [0] * 1024, "timeout_wins": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dw = sum(H.state["data_wins"])
    tw = sum(H.state["timeout_wins"])
    ops = H.total_ops()
    H.log("data_wins={0} timeout_wins={1} sum={2} ops={3}".format(
        dw, tw, dw + tw, ops))
    H.check(dw + tw == ops,
            "resolution conservation: data_wins+timeout_wins={0} != ops={1} "
            "(double-resume or lost resolution)".format(dw + tw, ops))
    H.check(ops > 0, "no rounds resolved")
    # Both branches should win at least sometimes given the jitter -- a guard
    # against a degenerate select that always takes the same case.
    H.check(dw > 0, "data branch never won (select stuck on timeout)")
    H.check(tw > 0, "timeout branch never won (select stuck on data)")


if __name__ == "__main__":
    harness.main("p119_timer_cancel_boundary", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="data vs timeout race at the select boundary; "
                          "exactly-once resolution per round")
