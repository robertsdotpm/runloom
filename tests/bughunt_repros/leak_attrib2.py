"""Bisect: is the 8.6kB/iter leak per-run() or per-fiber? Test drain-only run(),
raw mn_fiber cycle, and per-fiber scaling inside one run."""
import os, sys, gc
import runloom, runloom_c

MODE = sys.argv[1]
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 300

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def noop():
    pass

def cycle_drain():                 # runloom.run(4) with no main_fn
    runloom.run(4)

def cycle_runwrap():               # runloom.run(4, noop): the leaking case
    runloom.run(4, noop)

def cycle_rawfiber():              # raw C: one fiber per cycle
    runloom_c.mn_init(4)
    runloom_c.mn_fiber(noop)
    runloom_c.mn_run()
    runloom_c.mn_fini()

def cycle_manyfiber():             # raw C: 100 fibers per cycle
    runloom_c.mn_init(4)
    for _ in range(100):
        runloom_c.mn_fiber(noop)
    runloom_c.mn_run()
    runloom_c.mn_fini()

f = {"drain": cycle_drain, "runwrap": cycle_runwrap,
     "rawfiber": cycle_rawfiber, "manyfiber": cycle_manyfiber}[MODE]

for _ in range(20):
    f()
gc.collect()
r0 = rss_kb()
for _ in range(ITERS):
    f()
gc.collect()
r1 = rss_kb()
print("%s: iters=%d rss %d->%d kB (%.2f kB/iter)" % (MODE, ITERS, r0, r1, (r1 - r0) / float(ITERS)))
