import time
import runloom
def main():
    ch = runloom.Chan(0)
    def parked():
        ch.recv()   # no sender ever
    runloom.fiber(parked)
t0 = time.time()
n = runloom.run(8, main)   # default RUNLOOM_DEADLOCK (warn): expect DEADLOCK banner on stderr within ~200ms
print('run returned', n, time.time() - t0)
