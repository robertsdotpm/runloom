"""Resource-leak probe: run a workload in a loop, watch RSS + fd count."""
import os, sys, gc
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 50

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def nfds():
    return len(os.listdir("/proc/self/fd"))

def workload():
    ch = runloom.Chan(16)
    done = runloom.Chan(0)
    def prod():
        for i in range(2000):
            ch.send(("payload", i, b"z" * 512))
        ch.close()
    def cons():
        n = 0
        for _ in ch:
            n += 1
        done.send(n)
    for _ in range(4):
        runloom.fiber(prod)
    def consumer_group():
        pass
    total = [0]
    def cons_all():
        n = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            n += 1
        done.send(n)
    for _ in range(4):
        runloom.fiber(cons_all)
    def wait():
        t = 0
        for _ in range(4):
            v, ok = done.recv()
            t += v
        assert t == 8000, t
    runloom.fiber(wait)

samples = []
for it in range(ITERS):
    runloom.run(HUBS, workload)
    if it % 5 == 0 or it == ITERS - 1:
        gc.collect()
        samples.append((it, rss_kb(), nfds()))
for s in samples:
    print("iter=%d rss=%dkB fds=%d" % s)
