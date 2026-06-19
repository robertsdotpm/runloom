"""Repro: per-thread stack/coro pool imbalance leak (see LEAK_ANALYSIS.md).

One acceptor goroutine mn_go's N short-lived workers per round; the workers run
across all hubs and complete there, draining stacks out of the acceptor hub's
pool into the worker hubs' pools.  The acceptor re-mmaps every round -> the
process's mapping count climbs ~2 per worker (guard + stack VMA) and never
plateaus.

Run:
    PYTHON_GIL=0 PYTHONPATH=../src python3.13t repro_pool_leak.py

A bounded allocator (global overflow pool) would make `maps` plateau across
rounds instead of climbing.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import runloom_c

ROUNDS = int(os.environ.get("ROUNDS", "8"))
WORKERS_PER_ROUND = int(os.environ.get("WORKERS", "200"))


def maps_count():
    try:
        with open("/proc/self/maps") as fp:
            return sum(1 for _ in fp)
    except OSError:
        return -1


def worker():
    # Touch a little stack + yield once so it runs on (and completes on) a hub.
    buf = bytearray(1024)
    runloom_c.sched_sleep(0.0)
    buf[0] = 1


def acceptor():
    for r in range(ROUNDS):
        done = runloom_c.Chan(WORKERS_PER_ROUND)

        def w(ch=done):
            worker()
            ch.send(1)

        for _ in range(WORKERS_PER_ROUND):
            runloom_c.mn_fiber(w)
        for _ in range(WORKERS_PER_ROUND):
            done.recv()
        time.sleep(0.05)
        print("round {:2d}: maps={}".format(r, maps_count()), flush=True)


def main():
    print("baseline maps={}".format(maps_count()), flush=True)
    runloom_c.mn_init(4)
    runloom_c.mn_fiber(acceptor)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    print("final    maps={}  (climbing across rounds == the leak)".format(maps_count()), flush=True)


if __name__ == "__main__":
    main()
