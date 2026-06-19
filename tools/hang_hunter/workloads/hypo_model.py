"""Hypothesis-driven scheduler fuzz target.

Generates random *always-terminating* M:N programs -- N goroutines each running a
bounded op sequence drawn from {yield, churn a ref-cycle, self ping a buffered
channel, bounded gc.collect}, across a random hub count -- then runs each to
completion and asserts the scheduler self-check passes.

Because every generated program is constructed to terminate, ANY hang is a real
scheduler bug: the whole process wedges and the hang-hunter's per-job timeout
catches it (then gdb-triages the live process).  AssertionError / crash on a
shrunk example is likewise a real bug, and Hypothesis prints the minimal repro.

Run standalone:  python hypo_model.py [SEED]   (HH_MAX_EXAMPLES tunes count)
"""
import gc
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))
import runloom_c

import os as _crashos
if _crashos.environ.get("RUNLOOM_CRASH"):
    runloom_c.install_crash_handler(_crashos.environ["RUNLOOM_CRASH"],
                                 _crashos.environ.get("RUNLOOM_CRASH_FILE"))

from hypothesis import given, settings, seed as hseed, strategies as st, HealthCheck

# op codes
YIELD, CYCLE, PING, NOP = 0, 1, 2, 3
ops = st.lists(st.integers(min_value=0, max_value=3), min_size=0, max_size=40)
# a program: hub count, per-goroutine op lists, number of bounded collectors
program = st.fixed_dictionaries({
    "nhub": st.integers(min_value=1, max_value=6),
    "gs": st.lists(ops, min_size=1, max_size=24),
    "ncoll": st.integers(min_value=0, max_value=3),
    "coll_rounds": st.integers(min_value=1, max_value=20),
})


def run_program(p):
    nhub = p["nhub"]
    specs = p["gs"]
    ncoll = p["ncoll"]
    crounds = p["coll_rounds"]
    done = runloom_c.Chan(len(specs) + ncoll)
    stop = [False]

    def mk(spec):
        ch = runloom_c.Chan(1)

        def body():
            for op in spec:
                if op == YIELD:
                    runloom_c.sched_yield_classic()
                elif op == CYCLE:
                    a = {}; b = {}; a["b"] = b; b["a"] = a; a["self"] = a
                    del a, b
                elif op == PING:
                    ch.send(1); ch.recv()          # self ping; cap 1 -> never blocks
                else:
                    pass
            done.send(1)
        return body

    def collector():
        for _ in range(crounds):
            if stop[0]:
                break
            gc.collect()
            runloom_c.sched_yield_classic()
        done.send(2)

    runloom_c.mn_init(nhub)
    for s in specs:
        runloom_c.mn_fiber(mk(s))
    for _ in range(ncoll):
        runloom_c.mn_fiber(collector)

    def reaper():
        for _ in range(len(specs)):
            done.recv()
        stop[0] = True
        for _ in range(ncoll):
            done.recv()

    runloom_c.mn_fiber(reaper)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    assert runloom_c._self_check(0) == 0, "self_check failed for {0}".format(p)


def main():
    sd = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("HH_SEED", "0"))
    maxex = int(os.environ.get("HH_MAX_EXAMPLES", "150"))

    @settings(max_examples=maxex, deadline=None, derandomize=False,
              suppress_health_check=list(HealthCheck))
    @hseed(sd)
    @given(program)
    def fuzz(p):
        run_program(p)

    fuzz()
    print("PASS")


if __name__ == "__main__":
    main()
