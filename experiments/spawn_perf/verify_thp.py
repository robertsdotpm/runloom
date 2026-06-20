# Spawn a batch, keep the arena warm, then report AnonHugePages of THIS process.
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import runloom
def noop(): pass
def root():
    for _ in range(80000):
        runloom.fiber(noop)
runloom.run(8, root)
ahp = thp_eligible = 0
for line in open("/proc/self/smaps_rollup"):
    if line.startswith("AnonHugePages:"): ahp = int(line.split()[1])
print("HUGE=%s  AnonHugePages=%d kB (%d MB)" % (os.environ.get("RUNLOOM_STACK_ARENA_HUGE",""), ahp, ahp//1024))
