import time as wall, runloom, runloom.time
def main():
    t = runloom.time.NewTimer(3.0)
    print("Stop() ->", t.Stop())
t0 = wall.monotonic()
runloom.run(4, main)   # M:N scheduler path
print("mn run() took %.2fs" % (wall.monotonic() - t0))

# baseline: no timer -> run returns immediately
t0 = wall.monotonic()
runloom.run(1, lambda: None)
print("baseline run() took %.2fs" % (wall.monotonic() - t0))

# time.After has same property
def main2():
    runloom.time.After(3.0)
t0 = wall.monotonic()
runloom.run(1, main2)
print("After(3.0) run() took %.2fs" % (wall.monotonic() - t0))
