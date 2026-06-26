"""Tiny late-observe workload for the CHESS explorer (chess_explore.py) -- the
SAME narrow order-dependent bug pct_find.py hits probabilistically, here made the
explorer's exhaustive target.  A increments a shared counter N times, yielding
(sched_sleep(0) -> a baton grant point) after each; B reads it once.  B's observed
value == the number of increments before its single read got scheduled, a pure
function of the interleaving.  BUG iff B reads exactly TARGET (a LATE value),
reachable by switching the baton to B at one precise grant (one preemption).

Uses the low-level mn_init/mn_fiber/sched_sleep/mn_run API (like pct_find.py) --
that is the grant structure the controlled baton orders.  Sentinels: "BUG ..."
iff seen==TARGET else "OK ...".  Exit 0 either way.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
import runloom_c

N = int(os.environ.get("CHESS_N", "4"))
TARGET = int(os.environ.get("CHESS_TARGET", "2"))

runloom_c.mn_init(2)
st = {"x": 0, "seen": None}


def A():
    for _ in range(N):
        st["x"] += 1
        runloom_c.sched_sleep(0)        # yield -> a baton grant point


def B():
    st["seen"] = st["x"]                # one read = one segment the baton places


runloom_c.mn_fiber(A)
runloom_c.mn_fiber(B)
runloom_c.mn_run()
runloom_c.mn_fini()

seen = st["seen"]
if seen == TARGET:
    print("BUG seen=%d == TARGET=%d" % (seen, TARGET))
else:
    print("OK seen=%s" % seen)
