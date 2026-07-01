"""Within ONE long-lived runtime: spawn waves of fibers, sample RSS per wave.
If RSS grows per wave, fibers leak even in a single runtime (server death)."""
import os, sys, gc
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
WAVES = int(sys.argv[2]) if len(sys.argv) > 2 else 20
PER = int(sys.argv[3]) if len(sys.argv) > 3 else 3000

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def main():
    done = runloom.Chan(256)
    samples = []
    def noop():
        done.send(1)
    def driver():
        for w in range(WAVES):
            for _ in range(PER):
                runloom.fiber(noop)
            for _ in range(PER):
                done.recv()
            gc.collect()
            samples.append((w, rss_kb()))
        for s in samples:
            print("wave=%d rss=%dkB" % s)
        base = samples[2][1]; last = samples[-1][1]
        print("growth waves 2..%d: %d kB (%.2f kB per fiber)" % (
            WAVES - 1, last - base, (last - base) / float((WAVES - 3) * PER)))
    runloom.fiber(driver)

runloom.run(HUBS, main)
