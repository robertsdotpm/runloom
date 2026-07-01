import sys, time
import runloom
def main():
    ch = runloom.Chan(0)
    def parked():
        ch.recv()
    runloom.fiber(parked)
t0 = time.time()
try:
    n = runloom.run(4, main)
    print("run returned n=%r after %.1fs" % (n, time.time() - t0))
except Exception as e:
    print("raised %r after %.1fs" % (e, time.time() - t0))
