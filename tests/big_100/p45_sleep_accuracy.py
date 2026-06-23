"""big_100 / 45 -- sleep accuracy benchmark.

Goroutines schedule a continuous stream of sleeps with durations spanning 0 to
10 seconds (weighted toward short ones), measuring the jitter (actual minus
requested).  The hard invariant: a sleep must never wake EARLY (actual >=
requested minus a tiny epsilon); lateness is measured and reported.

Stresses: the timer subsystem, clock handling, wake-up accuracy across scales.
"""
import time

import harness
import runloom

EPSILON = 0.01          # allowed early-wake slack (clock granularity)


def pick_duration(rng):
    r = rng.random()
    if r < 0.7:
        return rng.uniform(0.0, 0.05)
    if r < 0.95:
        return rng.uniform(0.05, 1.0)
    return rng.uniform(1.0, 10.0)


def sleeper(H, wid, rng, state):
    maxjit = state["maxjit"]
    early = state["early"]
    while H.running():
        target = pick_duration(rng)
        t0 = time.perf_counter()
        runloom.sleep(target)
        actual = time.perf_counter() - t0
        jitter = actual - target
        if jitter > maxjit[wid & 1023]:
            maxjit[wid & 1023] = jitter
        if actual < target - EPSILON:
            early[wid & 1023] += 1
            H.fail("woke EARLY: target={0:.4f} actual={1:.4f} wid={2}".format(
                target, actual, wid))
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"maxjit": [0.0] * 1024, "early": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, sleeper, H.state)


def post(H):
    H.log("max_jitter={0:.3f}s early_wakeups={1}".format(
        max(H.state["maxjit"]), sum(H.state["early"])))


if __name__ == "__main__":
    harness.main("p45_sleep_accuracy", body, setup=setup, post=post,
                 default_funcs=20000,
                 describe="sleeps 0-10s; never wake early, measure jitter")
