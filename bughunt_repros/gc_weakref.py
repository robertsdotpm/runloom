"""GC interplay: gc.collect() from fibers during churn; weakrefs to G; dropping refs mid-op."""
import sys, gc, weakref
import runloom, runloom_c

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8

# 1: gc.collect storm while channel churn
def main():
    ch = runloom.Chan(8)
    done = runloom.Chan(0)
    def gccer():
        for _ in range(200):
            gc.collect()
            runloom.yield_now()
        done.send("gc")
    def prod():
        for i in range(5000):
            ch.send([i, "x" * 64, {i: i}])
        ch.close()
    def cons():
        n = 0
        s = 0
        for v in ch:
            n += 1
            s += v[0]
        assert n == 5000 and s == sum(range(5000)), (n, s)
        done.send("cons")
    runloom.fiber(gccer)
    runloom.fiber(gccer)
    runloom.fiber(prod)
    runloom.fiber(cons)
    def wait():
        for _ in range(3):
            done.recv()
        print("gc churn OK")
    runloom.fiber(wait)
runloom.run(HUBS, main)

# 2: weakref to fiber handle (run(1) returns Goroutine)
def main2():
    refs = []
    def w():
        runloom.yield_now()
    for _ in range(100):
        g = runloom.fiber(w)
        if g is not None:
            try:
                refs.append(weakref.ref(g._g))
            except TypeError:
                pass
    gc.collect()
runloom.run(1, main2)
gc.collect()
print("weakref OK")

# 3: drop channel refs while a fiber is parked on it -> fiber leaks (Go-like), but
# process must not crash; run() with a permanently-parked fiber should... observe.
def main3():
    ch = runloom.Chan(0)
    def parked():
        ch.recv()   # no sender ever
    runloom.fiber(parked)
    # main drops its ref
print("starting permanently-parked fiber test (expect hang or deadlock error)...", flush=True)
import faulthandler; faulthandler.dump_traceback_later(10, exit=True)
n = runloom.run(HUBS, main3)
print("run returned", n)
