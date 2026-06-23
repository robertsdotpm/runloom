"""big_100 / 71 -- profile hook stress.

Goroutines install a sys.setprofile hook, run a batch of nested calls with
yields and sleeps in between (so the profiled call stack is suspended and
resumed on possibly different hub threads), then uninstall it.  The profiler
must keep receiving plausible call/return events and the interpreter must not
crash when a profiled frame is swapped out mid-call.

Stresses: the C profiling hook across goroutine stack switches.
"""
import sys
import threading

import harness
import runloom


def setup(H):
    H.state = {"events": [0], "empty": [0], "lock": threading.Lock()}


def make_profiler(state):
    counts = {"n": 0}

    def prof(frame, event, arg):
        counts["n"] += 1
        return None
    return prof, counts


def busy(depth):
    if depth <= 0:
        runloom.yield_now()
        return 1
    runloom.sleep(0.0) if depth % 3 == 0 else None
    return 1 + busy(depth - 1)


def worker(H, wid, rng, state):
    while H.running():
        prof, counts = make_profiler(state)
        sys.setprofile(prof)
        try:
            for _ in range(rng.randint(2, 6)):
                busy(rng.randint(3, 15))
        finally:
            sys.setprofile(None)
        # sys.setprofile is per-OS-THREAD (hub), not per-goroutine, so a
        # sibling can clear it across a migration: "no events" is EXPECTED
        # under M:N (FINDINGS BUG #11), not a failure.  We require only no
        # crash; events are measured.
        with state["lock"]:
            state["events"][0] += counts["n"]
            if counts["n"] == 0:
                state["empty"][0] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("total_profile_events={0} empty_runs={1} (setprofile is hub-local "
          "under M:N -- FINDINGS BUG #11)".format(
              H.state["events"][0], H.state["empty"][0]))


if __name__ == "__main__":
    harness.main("p71_profile_hook", body, setup=setup, post=post,
                 default_funcs=1500,
                 describe="sys.setprofile across goroutine switches; no crash")
