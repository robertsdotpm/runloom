#!/usr/bin/env python3
"""Validate that @runloom.hot recovers per-core ctxswitch scaling vs a plain
shared handler -- the user-facing API delivering the runloom_epoll_py_fiber.py --distinct
result with no manual code-object juggling.  One mode per process, pinned,
preempt off (the CPU-preempt watchdog is microbenchmark noise; see
SCHEDULER_SCALING_FINDINGS.md):

  taskset -c 16-59 env PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_PREEMPT=0 RUNLOOM_SYSMON=0 \
      python3.13t benchmark/suite/speed/hot_validate.py --mode hot --hubs 44
"""
import argparse
import time

import runloom
import runloom_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["shared", "hot"], required=True)
    ap.add_argument("--hubs", type=int, default=44)
    ap.add_argument("--n", type=int, default=1_000_000)
    a = ap.parse_args()
    G = a.hubs * 16
    K = max(1, a.n // G)
    SW = G * K
    SYC = runloom_c.sched_yield

    # A realistic shared-closure handler: it CAPTURES the thing it calls per
    # round (here the yielder, standing in for a captured `config`/client).  One
    # closure instance is shared by every fiber == every server sharing one
    # handler == the shared-cell contention.  @hot gives each core its own cells.
    def make_worker(K, y):
        def worker():
            for _ in range(K):
                y()
        return worker

    if a.mode == "hot":
        w = runloom.hot(make_worker(K, SYC))
    else:
        w = make_worker(K, SYC)

    def root():
        for _ in range(G):
            runloom.fiber(w)

    t0 = time.perf_counter()
    runloom.run(a.hubs, root)
    dt = time.perf_counter() - t0
    print("%-7s %2d hubs  %12.0f switches/s  %8.0f ns/switch"
          % (a.mode, a.hubs, SW / dt, dt * 1e9 / SW), flush=True)


if __name__ == "__main__":
    main()
