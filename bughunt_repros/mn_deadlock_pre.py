import sys, time
import runloom
# preamble: two normal M:N cycles first (like gc_weakref.py did)
runloom.run(4, lambda: None)
runloom.run(4, lambda: None)
def main():
    ch = runloom.Chan(0)
    def parked():
        ch.recv()
    runloom.fiber(parked)
t0 = time.time()
n = runloom.run(4, main)
print("run returned n=%r after %.1fs" % (n, time.time() - t0))
