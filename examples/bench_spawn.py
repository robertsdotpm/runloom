"""Spawn cost: time pygo.go() in a tight loop, run all gs once.

Spawn touches:
  - PyMem_Calloc of pygo_g_t (small, fast)
  - pygo_coro_new which pops/pushes a per-thread stack pool (fast once warm)
  - PyCapsule_New for the handle (Python alloc)

Each run primes the stack pool first, then re-measures so we see steady
state, not first-time mmap cost.
"""
import sys
import time

sys.path.insert(0, "src")
import pygo_core


def noop():
    pass


def measure(n, repeats=5):
    best = float("inf")
    for _ in range(repeats):
        t0 = time.perf_counter()
        for _ in range(n):
            pygo_core.go(noop)
        pygo_core.run()
        dt = time.perf_counter() - t0
        if dt < best:
            best = dt
    return best


def main():
    print("pygo spawn microbench")
    print("backend:", pygo_core.backend())
    print()
    # Warm the stack pool with one big run.
    measure(10_000, repeats=1)
    print("steady-state spawn+run (stack pool warm):")
    for n in (1_000, 10_000, 100_000):
        dt = measure(n)
        rate = n / dt
        print("  {:>7} gs  ->  {:>6.1f} ms  ({:>7.0f} K spawn+run/s, "
              "{:>5.0f} ns/g)".format(
                  n, dt * 1000.0, rate / 1000.0, dt * 1e9 / n))


if __name__ == "__main__":
    main()
