"""M:N scheduler — real multi-core parallelism (free-threaded 3.13t).

This is runloom's headline trick: with the GIL off, run(n, main_fn) spreads
goroutines across n hub threads — one per core — for genuine parallelism.
The whole mn_init / mn_go / mn_run / mn_fini envelope collapses into a single
run(n, ...) call; inside it, runloom.go() dispatches each goroutine onto a hub
automatically.  SHA-256 releases the GIL while it hashes, so adding hubs gives
near-linear speedup here.

Needs free-threaded CPython 3.13t with the GIL disabled:
    PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 examples/mn_parallel.py

On a normal (GIL) build, run(n>1) RAISES rather than silently running serial —
parallelism is opt-in and only honest with the GIL off — so there we just run
the single-thread path.
"""
import hashlib
import sys
import time

import runloom

NUM_TASKS = 64
ROUNDS = 4000

def work():
    data = b"x" * 1024
    for _ in range(ROUNDS):
        data = hashlib.sha256(data).digest()
    return data

def spawn_all():
    # Root goroutine: fan out the CPU-bound workers.  Inside an M:N run,
    # runloom.go() lands each one on a hub (round-robin) for us.
    for _ in range(NUM_TASKS):
        runloom.go(work)

def time_hubs(n_hubs):
    start = time.perf_counter()
    runloom.run(n_hubs, spawn_all)   # collapses mn_init/mn_go/mn_run/mn_fini
    return time.perf_counter() - start

def main():
    gil_on = getattr(sys, "_is_gil_enabled", lambda: True)()
    if gil_on:
        print("NOTE: the GIL is ENABLED -- run(n>1) would raise, so this can")
        print("      only run single-threaded.  For real parallelism use")
        print("      free-threaded CPython 3.13t with PYTHON_GIL=0.\n")
        elapsed = time_hubs(1)
        print(" 1 hub: {0:6.3f}s   (single-thread; add hubs with the GIL off)"
              .format(elapsed))
        return

    baseline = None
    for hubs in (1, 2, 4, 8):
        elapsed = time_hubs(hubs)
        if baseline is None:
            baseline = elapsed
        print("{0:2d} hubs: {1:6.3f}s   speedup x{2:.2f}".format(
            hubs, elapsed, baseline / elapsed))

if __name__ == "__main__":
    main()
