import time as wall, runloom, runloom.time
def main():
    t = runloom.time.NewTimer(3.0)
    print("Stop() ->", t.Stop())
t0 = wall.monotonic()
runloom.run(1, main)
print("run() took %.2fs (expected ~0s)" % (wall.monotonic() - t0))
