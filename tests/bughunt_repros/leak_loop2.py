"""Resource-leak probe v2: correct workload (single closer), plus an EMPTY-run control."""
import os, sys, gc, threading
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 100
MODE = sys.argv[3] if len(sys.argv) > 3 else "chan"

def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

def nfds():
    return len(os.listdir("/proc/self/fd"))

def workload_chan():
    ch = runloom.Chan(16)
    done = runloom.Chan(0)
    NP, NC, PER = 4, 4, 2000
    def prod():
        for i in range(PER):
            ch.send(("payload", i, b"z" * 512))
        done.send(("p", PER))
    def cons_all():
        n = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            n += 1
        done.send(("c", n))
    for _ in range(NP):
        runloom.fiber(prod)
    for _ in range(NC):
        runloom.fiber(cons_all)
    def wait():
        pdone = 0
        ctotal = 0
        cdone = 0
        while pdone < NP or cdone < NC:
            (k, n), ok = done.recv()
            if k == "p":
                pdone += 1
                if pdone == NP:
                    ch.close()
            else:
                cdone += 1
                ctotal += n
        assert ctotal == NP * PER, ctotal
    runloom.fiber(wait)

def workload_empty():
    pass

def workload_spawn():
    def w():
        pass
    for _ in range(5000):
        runloom.fiber(w)

wl = {"chan": workload_chan, "empty": workload_empty, "spawn": workload_spawn}[MODE]

samples = []
for it in range(ITERS):
    runloom.run(HUBS, wl)
    if it % 10 == 0 or it == ITERS - 1:
        gc.collect()
        samples.append((it, rss_kb(), nfds()))
for s in samples:
    print("mode=%s iter=%d rss=%dkB fds=%d" % ((MODE,) + s))
