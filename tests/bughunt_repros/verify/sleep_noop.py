import time as wall, runloom.time
t0 = wall.monotonic()
runloom.time.Sleep(0.5)
print("Sleep(0.5) outside a fiber returned after %.3fs" % (wall.monotonic() - t0))
