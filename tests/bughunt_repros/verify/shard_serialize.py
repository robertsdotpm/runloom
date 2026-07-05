import runloom.monkey as monkey
monkey.patch()
import time
import runloom_c as rc
from runloom.monkey import offload, _raw_time_sleep

N = 4
for i in range(N):
    rc.fiber(lambda: offload(_raw_time_sleep, 0.5))
t0 = time.monotonic(); rc.run(); wall = time.monotonic() - t0
print("N=%d concurrent 0.5s offloads took %.2fs wall" % (N, wall))
print("backend size:", monkey._get_backend().size)
print("BUG" if wall > 1.5 else "OK")
