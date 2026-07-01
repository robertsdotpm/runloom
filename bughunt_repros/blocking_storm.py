"""blocking() offload storm: many fibers offloading concurrently; verify results and no hang."""
import sys, time, threading
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
N = 500

def main():
    out = runloom.Chan(64)
    def w(i):
        r = runloom.blocking(lambda x: (time.sleep(0.001), x * 2)[1], i)
        out.send((i, r))
    for i in range(N):
        runloom.fiber(w, i)
    def collect():
        seen = {}
        for _ in range(N):
            i, r = out.recv()[0]
        # recv returns ((i,r), ok)
    def collect2():
        seen = {}
        for _ in range(N):
            (i, r), ok = out.recv()
            assert ok and r == i * 2, (i, r)
            assert i not in seen
            seen[i] = r
        print("blocking storm hubs=%d N=%d OK" % (HUBS, N))
    runloom.fiber(collect2)

runloom.run(HUBS, main)

# blocking() raising an exception must propagate to the fiber
def main2():
    def bad():
        raise ValueError("inside blocking")
    try:
        runloom.blocking(bad)
        print("blocking exc: NOT PROPAGATED (bug)")
    except ValueError:
        print("blocking exc: propagated OK")
runloom.run(HUBS, main2)
