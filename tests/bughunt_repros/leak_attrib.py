"""Attribute the ~8.6kB/cycle leak: raw mn_init+mn_fini vs mn_run, and Python object counts."""
import os, sys, gc
import runloom, runloom_c

MODE = sys.argv[1]
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 300

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def cycle_initfini():
    runloom_c.mn_init(4)
    runloom_c.mn_fini()

def cycle_full():
    runloom_c.mn_init(4)
    runloom_c.mn_run()
    runloom_c.mn_fini()

def cycle_run1():
    runloom_c.run()

f = {"initfini": cycle_initfini, "full": cycle_full, "run1": cycle_run1}[MODE]

for _ in range(20):
    f()
gc.collect()
r0 = rss_kb(); o0 = len(gc.get_objects())
for _ in range(ITERS):
    f()
gc.collect()
r1 = rss_kb(); o1 = len(gc.get_objects())
print("%s: iters=%d rss %d->%d kB (%.1f kB/iter), gc objects %d->%d (%+d)" % (
    MODE, ITERS, r0, r1, (r1 - r0) / float(ITERS), o0, o1, o1 - o0))
