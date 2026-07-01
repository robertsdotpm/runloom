"""Isolate the per-run() leak: N empty mn cycles vs run(1) cycles."""
import os, sys, gc
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 500

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def noop():
    pass

# warm up
for _ in range(20):
    runloom.run(HUBS, noop)
gc.collect()
r0 = rss_kb()
for i in range(ITERS):
    runloom.run(HUBS, noop)
gc.collect()
r1 = rss_kb()
print("hubs=%d iters=%d rss %d -> %d kB, delta=%d kB (%.1f kB/iter)" % (
    HUBS, ITERS, r0, r1, r1 - r0, (r1 - r0) / float(ITERS)))
