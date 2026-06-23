"""big_100 / 72 -- trace hook stress.

Like the profile-hook test but with sys.settrace (line/call tracing, heavier
and more intrusive).  Goroutines install a tracer, run traced functions that
yield and block, then remove it.  The frame-tracing state must survive the
goroutine being suspended and resumed without corrupting the interpreter.

Stresses: per-frame tracing state across cooperative switches.
"""
import sys
import threading

import harness
import runloom


def setup(H):
    H.state = {"events": [0], "empty": [0], "lock": threading.Lock()}


def make_tracer():
    counts = {"n": 0}

    def tracer(frame, event, arg):
        counts["n"] += 1
        return tracer                    # trace lines within the frame too
    return tracer, counts


def traced(depth):
    total = 0
    for i in range(depth):
        total += i
        if i % 4 == 0:
            runloom.yield_now()
    runloom.sleep(0.0003)
    return total


def worker(H, wid, rng, state):
    while H.running():
        depth = rng.randint(4, 20)
        tracer, counts = make_tracer()
        sys.settrace(tracer)
        try:
            got = traced(depth)
        finally:
            sys.settrace(None)
        # The traced function must still compute the right value while traced.
        if not H.check(got == depth * (depth - 1) // 2,
                       "traced result wrong wid={0}: {1}".format(wid, got)):
            return
        # sys.settrace is per-OS-THREAD (hub), not per-goroutine: a sibling on
        # the same hub can clear it, or this goroutine can migrate to a hub
        # that never had it -- so "no events" is EXPECTED under M:N, not a bug.
        # We only require no crash + correct compute; events are measured.
        with state["lock"]:
            state["events"][0] += counts["n"]
            if counts["n"] == 0:
                state["empty"][0] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("total_trace_events={0} empty_runs={1} (settrace is hub-local, not "
          "goroutine-local under M:N -- FINDINGS BUG #11)".format(
              H.state["events"][0], H.state["empty"][0]))


if __name__ == "__main__":
    harness.main("p72_trace_hook", body, setup=setup, post=post,
                 default_funcs=1500,
                 describe="sys.settrace across goroutine switches; no crash")
