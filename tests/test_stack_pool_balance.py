"""Regression: goroutine-stack pools must not grow unboundedly under an
acceptor->worker fan-out.

One acceptor goroutine mn_go's many short-lived workers per round; the workers
run and complete spread across all hubs.  The stack recycle pools are
per-thread, so stacks drain out of the acceptor hub's pool into the worker
hubs' pools.  Before the fix (a shared global stack depot below the per-thread
caches + a bounded coro pool) the acceptor re-mmap'd every round and the worker
pools grew without bound -> the process mapping count climbed ~2/worker/round
forever and eventually hit vm.max_map_count -> mmap ENOMEM.

This asserts the mapping count PLATEAUS: once the per-thread caches saturate and
overflow to the shared depot, later rounds add ~nothing.
"""
import os
import sys

import pytest
import runloom_c

if not os.path.exists("/proc/self/maps"):
    pytest.skip("needs /proc/self/maps (Linux)", allow_module_level=True)

ROUNDS = 40
WORKERS = 200


def maps_count():
    with open("/proc/self/maps") as fp:
        return sum(1 for _ in fp)


def test_stack_pool_plateaus_under_fanout():
    samples = []

    def worker(ch):
        buf = bytearray(2048)        # touch some stack
        runloom_c.sched_sleep(0.0)   # yield so it runs on / completes on a hub
        buf[0] = 1
        ch.send(1)

    def acceptor():
        for r in range(ROUNDS):
            ch = runloom_c.Chan(WORKERS)
            for _ in range(WORKERS):
                runloom_c.mn_go(lambda c=ch: worker(c))
            for _ in range(WORKERS):
                ch.recv()
            samples.append(maps_count())

    runloom_c.mn_init(4)
    try:
        runloom_c.mn_go(acceptor)
        runloom_c.mn_run()
    finally:
        runloom_c.mn_fini()

    mid = samples[ROUNDS // 2]
    end = samples[-1]
    growth = end - mid
    # Second-half growth must be tiny: caches have saturated, the shared depot
    # recycles across hubs.  Pre-fix this was ~WORKERS*ROUNDS/2*~2 mappings
    # (thousands), so the bound only needs to be far below that -- the few-dozen
    # jitter in where the plateau settles across 4 hubs (observed up to ~66) is
    # not a leak, so allow generous slack while still catching the regression.
    assert growth <= 256, (
        "stack pool not bounded: maps grew {} in the second half "
        "(mid={} end={} samples={})".format(growth, mid, end, samples)
    )


if __name__ == "__main__":
    test_stack_pool_plateaus_under_fanout()
    print("ok")
