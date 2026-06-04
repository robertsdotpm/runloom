"""Workload: gc.collect() stop-the-world churn across M:N hubs.

This is the shape that exposed the stop-the-world MONOPOLY deadlock
(test_gc_stw_under_goroutine_churn). Kept as a permanent fuzz target so any
regression of that class resurfaces immediately. Params come from env so the
hang-hunter can vary them; print PASS on clean completion.
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

NHUB = int(os.environ.get("HH_NHUB", "4"))
NWORK = int(os.environ.get("HH_NWORK", "48"))
ROUNDS = int(os.environ.get("HH_ROUNDS", "200"))
NCOLL = int(os.environ.get("HH_NCOLL", "1"))           # how many collector goroutines

done = runloom_c.Chan(NWORK + NCOLL)
stop = [False]


def worker():
    for _ in range(ROUNDS):
        a = {}; b = {}
        a["b"] = b; b["a"] = a; a["self"] = a
        del a, b
        runloom_c.sched_yield_classic()
    done.send(1)


def collector():
    n = 0
    while not stop[0]:
        gc.collect()
        n += 1
        runloom_c.sched_yield_classic()
    done.send(("gc", n))


def stopper():
    for _ in range(NWORK):
        done.recv()
    stop[0] = True
    for _ in range(NCOLL):
        done.recv()


runloom_c.mn_init(NHUB)
for _ in range(NCOLL):
    runloom_c.mn_go(collector)
for _ in range(NWORK):
    runloom_c.mn_go(worker)
runloom_c.mn_go(stopper)
runloom_c.mn_run()
runloom_c.mn_fini()
assert runloom_c._self_check(0) == 0, "self_check failed"
print("PASS")
