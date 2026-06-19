"""Small runloom workload for the valgrind memcheck run (S4). Single-hub chan
ping-pong + spawn churn -- enough to exercise the scheduler, channels, stack
paint/pool, and the recycle path under memcheck without a long run."""
import sys
sys.path.insert(0, "src")
import runloom_c

for _ in range(3):
    a, b = runloom_c.Chan(), runloom_c.Chan()

    def pinger():
        for i in range(200):
            a.send(i)
            b.recv()

    def ponger():
        for _ in range(200):
            v, _ = a.recv()
            b.send(v)

    runloom_c.fiber(pinger)
    runloom_c.fiber(ponger)
    runloom_c.run()
print("workload done")
