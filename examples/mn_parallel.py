"""M:N scheduler — real multi-core parallelism (free-threaded 3.13t).

This is runloom's headline trick: with the GIL off, the M:N scheduler runs
goroutines across a pool of hub threads, one per core.  mn_init(n)
starts n hubs, mn_go schedules onto them round-robin, mn_run waits for
them all.  SHA-256 releases the GIL while it hashes, so adding hubs
gives near-linear speedup here.

Needs free-threaded CPython 3.13t with the GIL disabled:
    PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 examples/mn_parallel.py

On a normal (GIL) build it still runs correctly — just single-core, so
the timings won't improve with more hubs.
"""
import hashlib
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import runloom


NUM_TASKS = 64
ROUNDS = 4000


def work():
    data = b"x" * 1024
    for _ in range(ROUNDS):
        data = hashlib.sha256(data).digest()
    return data


def run_with_hubs(n_hubs):
    runloom.mn_init(n_hubs)
    start = time.perf_counter()
    for _ in range(NUM_TASKS):
        runloom.mn_go(work)
    runloom.mn_run()
    elapsed = time.perf_counter() - start
    runloom.mn_fini()
    return elapsed


def main():
    gil_on = getattr(sys, "_is_gil_enabled", lambda: True)()
    if gil_on:
        print("NOTE: the GIL is ENABLED -- this runs single-core.")
        print("      For parallelism use free-threaded 3.13t with PYTHON_GIL=0.\n")

    baseline = None
    for hubs in (1, 2, 4, 8):
        elapsed = run_with_hubs(hubs)
        if baseline is None:
            baseline = elapsed
        print("{0:2d} hubs: {1:6.3f}s   speedup x{2:.2f}".format(
            hubs, elapsed, baseline / elapsed))


if __name__ == "__main__":
    main()
