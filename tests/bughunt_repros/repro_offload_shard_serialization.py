"""_ThreadPoolBackend shards jobs by _thread.get_ident() % size with exactly
ONE worker per shard and no work stealing.  Under the single-thread scheduler
every fiber shares one OS thread id, so ALL offloads land on ONE shard/worker:
the 8-worker pool degrades to 1 worker, and one slow/blocking offloaded call
(os.system, getpass, open() on a FIFO, fsync on slow disk...) head-of-line
blocks every other offloaded call (open/os.stat/os.system/...) from every fiber.

This measures 4 concurrent 0.5s offloads: expected ~0.5s wall (parallel pool),
observed ~2.0s (fully serialized on one worker).
"""
import runloom.monkey as monkey
monkey.patch()

import time
import runloom_c as rc
from runloom.monkey import offload, _raw_time_sleep

N = 4
t = {}


def worker(i):
    def fn():
        offload(_raw_time_sleep, 0.5)   # blocking call on the backend pool
    return fn


def main():
    t0 = time.monotonic()
    gs = [rc.fiber(worker(i)) for i in range(N)]


rc.fiber(main)
t0 = time.monotonic()
rc.run()
wall = time.monotonic() - t0
print("N=%d concurrent 0.5s offloads took %.2fs wall" % (N, wall))
print("backend size:", monkey._get_backend().size)
if wall > 1.5:
    print("BUG CONFIRMED: offloads serialized on one shard worker")
else:
    print("OK: offloads ran in parallel")
