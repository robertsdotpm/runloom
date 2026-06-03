"""Small pygo workload for the valgrind memcheck run (S4). Single-hub chan
ping-pong + spawn churn -- enough to exercise the scheduler, channels, stack
paint/pool, and the recycle path under memcheck without a long run."""
import sys
sys.path.insert(0, "src")
import pygo_core

for _ in range(3):
    a, b = pygo_core.Chan(), pygo_core.Chan()

    def pinger():
        for i in range(200):
            a.send(i)
            b.recv()

    def ponger():
        for _ in range(200):
            v, _ = a.recv()
            b.send(v)

    pygo_core.go(pinger)
    pygo_core.go(ponger)
    pygo_core.run()
print("workload done")
