import time as wall
import runloom, runloom.time

# 1. runloom.sleep outside a fiber (documented fallback path)
t0 = wall.monotonic()
runloom.sleep(0.3)
print("runloom.sleep(0.3) outside fiber: %.3fs" % (wall.monotonic() - t0))

# 2. runloom.time.Sleep outside a fiber (claimed no-op)
t0 = wall.monotonic()
runloom.time.Sleep(0.3)
print("runloom.time.Sleep(0.3) outside fiber: %.3fs" % (wall.monotonic() - t0))

# 3. Sanity: Sleep inside a fiber works
def f():
    t0 = wall.monotonic()
    runloom.time.Sleep(0.3)
    print("runloom.time.Sleep(0.3) inside fiber: %.3fs" % (wall.monotonic() - t0))

import runloom_c
runloom_c.fiber(f)
runloom_c.run()
