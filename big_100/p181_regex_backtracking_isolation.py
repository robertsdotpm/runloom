"""big_100 / 181 -- catastrophic-backtracking regex isolation.

A MODEST fraction of the goroutines run a catastrophic-backtracking regex
(`(a+)+$` against `'a'*N + '!'`, N tuned so each match burns ~tens of ms inside
the C regex engine, well under a second).  That match holds its hub thread in C
with no cooperative yield -- exactly the class only sysmon preemption (on by
default on 3.13t) can rescue.  The remaining goroutines do purely cooperative
ops (a per-slot counter + yield/sleep).  An auditor samples the cooperative
counter over time and asserts it keeps RISING: the C-bound regex must never
fully stall the cooperative goroutines.

Stresses: sysmon preemption / fairness between a non-yielding C regex and
cooperative goroutines; no full scheduler stall under C-bound work.
"""
import re

import harness
import runloom

# A small N keeps each backtracking match in the ~tens-of-ms range (it is
# exponential in N, so a tiny bump is a big cost change).  The trailing '!'
# forces the engine to exhaust every grouping before it can fail $.
EVIL_RE = re.compile(r"(a+)+$")
EVIL_N = 24
EVIL_SUBJECT = "a" * EVIL_N + "!"


def heavy_regex(H, wid, rng, state):
    matched = 0
    while H.running():
        # This call backtracks catastrophically in C; it does NOT yield.
        m = EVIL_RE.match(EVIL_SUBJECT)
        # The subject never matches (the '!' defeats $) -- the point is the burn.
        if m is None:
            matched += 1
        state["heavy_ticks"][wid & 1023] += 1
        H.op(wid)
    state["heavy_matched"][wid & 1023] = matched


def coop_worker(H, wid, rng, state):
    while H.running():
        runloom.sleep(0.003)
        state["coop_ticks"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "coop_ticks": [0] * 1024,
        "heavy_ticks": [0] * 1024,
        "heavy_matched": [0] * 1024,
        "stall_windows": [0],
    }


def body(H):
    # Keep the heavy fraction modest so it cannot occupy ALL hubs at once
    # (a quarter of the goroutines, and never more than (hubs-1) at a time is
    # not guaranteed, but a 1:3 ratio leaves cooperative work plenty of room).
    heavy = max(1, H.funcs // 4)
    coop = max(1, H.funcs - heavy)
    H.run_pool(heavy, heavy_regex, H.state)
    H.run_pool(coop, coop_worker, H.state)

    def auditor():
        # Let the pool warm up before sampling.
        H.sleep(2.0)
        last = sum(H.state["coop_ticks"])
        while H.running():
            H.sleep(2.0)
            now = sum(H.state["coop_ticks"])
            progress = now - last
            if progress <= 0:
                H.state["stall_windows"][0] += 1
            if not H.check(
                progress > 0,
                "cooperative goroutines starved by C-bound regex (no progress "
                "in a 2s window) -- sysmon preemption failed",
            ):
                return
            last = now
        H.log("coop_ticks={0} heavy_ticks={1}".format(
            sum(H.state["coop_ticks"]), sum(H.state["heavy_ticks"])))

    H.go(auditor)


def post(H):
    coop = sum(H.state["coop_ticks"])
    heavy = sum(H.state["heavy_ticks"])
    H.check(coop > 0, "no cooperative progress at all")
    H.check(heavy > 0, "the heavy regex goroutines never ran")
    H.check(H.state["stall_windows"][0] == 0,
            "{0} sampling window(s) saw a full cooperative stall".format(
                H.state["stall_windows"][0]))
    H.log("coop_ticks={0} heavy_ticks={1} stall_windows={2}".format(
        coop, heavy, H.state["stall_windows"][0]))


if __name__ == "__main__":
    harness.main("p181_regex_backtracking_isolation", body, setup=setup,
                 post=post, default_funcs=500,
                 describe="catastrophic-backtracking regex must not starve "
                          "cooperative goroutines (sysmon preemption)")
