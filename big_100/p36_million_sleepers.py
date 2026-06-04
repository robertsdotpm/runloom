"""big_100 / 36 -- million sleeper test.

Spawn a very large number of goroutines that each sleep a random short
duration in a loop, measuring how late each wake-up is.  A healthy scheduler
wakes them close to on time; pathological timer-heap or fairness bugs show up
as large wake-up latencies.

Stresses: the timer heap, memory at scale, wake-up fairness.

Run the full million with smaller stacks: the project shrinks the per-goroutine
stack itself (sleepers never recurse), so a million parked timers stay cheap.
"""
import time

import harness
import runloom
import runloom_c

LATE_THRESHOLD = 0.5      # a wake-up later than this counts as "late"
FAIL_THRESHOLD = 5.0      # later than this is treated as starvation


def sleeper(H, wid, rng, state):
    late = state["late"]
    maxlat = state["maxlat"]
    while H.running():
        target = rng.uniform(0.001, 0.25)
        t0 = time.perf_counter()
        runloom.sleep(target)
        lateness = (time.perf_counter() - t0) - target
        if lateness > maxlat[wid & 1023]:
            maxlat[wid & 1023] = lateness
        if lateness > LATE_THRESHOLD:
            late[wid & 1023] += 1
        if not H.check(lateness < FAIL_THRESHOLD,
                       "wake-up latency {0:.2f}s (starvation) wid={1}".format(
                           lateness, wid)):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"late": [0] * 1024, "maxlat": [0.0] * 1024}


def body(H):
    # Sleepers don't recurse into deep C, so a small stack is plenty and keeps
    # a million parked goroutines affordable.
    runloom_c.set_stack_size(96 * 1024)
    H.run_pool(H.funcs, sleeper, H.state)

    def reporter():
        while H.running():
            H.sleep(5.0)
        H.log("max_wakeup_latency={0:.3f}s late_wakeups={1}".format(
            max(H.state["maxlat"]), sum(H.state["late"])))

    H.go(reporter)


if __name__ == "__main__":
    harness.main("p36_million_sleepers", body, setup=setup,
                 default_funcs=500000,
                 describe="hundreds of thousands of sleepers; wake-up latency")
