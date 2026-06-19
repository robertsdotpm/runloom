"""M:N scheduler — runloom's headline trick: use ALL your cores.

With the GIL off, `run(n, main_fn)` spreads fibers across n hub threads —
one per core — for genuine multi-core parallelism.  The whole
mn_init / mn_fiber / mn_run / mn_fini envelope collapses into that single call;
inside it, `runloom.fiber()` lands each fiber on a hub automatically.  Here we
fan out CPU-bound SHA-256 work (which releases the GIL while it hashes) and run
it across every core, so the speedup over a single thread is near-linear.

Needs free-threaded CPython 3.13t with the GIL disabled:
    PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 examples/mn_parallel.py

On a normal (GIL) build, `run(n>1)` RAISES rather than silently running serial
— parallelism is opt-in and only honest with the GIL off — so there we just run
the single-thread path.
"""
import hashlib
import os
import sys
import time

import runloom

NCPU = os.cpu_count() or 4
NUM_TASKS = NCPU * 4
ROUNDS = 3000

def work():
    data = b"x" * 1024
    for _ in range(ROUNDS):
        data = hashlib.sha256(data).digest()
    return data

def spawn_all():
    # Root fiber: fan out the CPU-bound workers.  Inside an M:N run,
    # runloom.fiber() lands each one on a hub (round-robin) for us.
    for _ in range(NUM_TASKS):
        runloom.fiber(work)

def time_run(n_hubs):
    start = time.perf_counter()
    runloom.run(n_hubs, spawn_all)   # collapses mn_init/mn_fiber/mn_run/mn_fini
    return time.perf_counter() - start

def main():
    print("workload: {0} fibers x {1} SHA-256 rounds; cores available: {2}\n"
          .format(NUM_TASKS, ROUNDS, NCPU))

    gil_on = getattr(sys, "_is_gil_enabled", lambda: True)()
    if gil_on:
        print("NOTE: the GIL is ENABLED -- run(n>1) would raise, so this can")
        print("      only run single-threaded.  For real multi-core speedup use")
        print("      free-threaded CPython 3.13t with PYTHON_GIL=0.\n")
        print("   1 hub      : {0:6.3f}s   (single-thread)".format(time_run(1)))
        return

    base = time_run(1)            # run(1, ...)    -- one thread
    full = time_run(NCPU)         # run(NCPU, ...) -- every core
    print("   1 hub      : {0:6.3f}s".format(base))
    print("  {0:2d} hubs (all): {1:6.3f}s   speedup x{2:.2f}".format(
        NCPU, full, base / full))

if __name__ == "__main__":
    main()
