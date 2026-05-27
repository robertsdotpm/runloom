"""Per-yield latency microbench targeting the Phase B snap/load path.

Single goroutine, tight loop of sched_yield -- each iteration is one
full save-tstate + asm-yield + resume + load-tstate cycle.  Lets us
measure the snap path in isolation, away from spawn/scheduling cost.
"""
import sys
import time

sys.path.insert(0, "src")
import pygo_core


def make_yielder(n):
    def w():
        for _ in range(n):
            pygo_core.sched_yield()
    return w


def measure(n_coros, n_yields, repeats=5):
    """Each coro yields n_yields times; total yields = n_coros * n_yields."""
    best = float("inf")
    for _ in range(repeats):
        for _ in range(n_coros):
            pygo_core.go(make_yielder(n_yields))
        t0 = time.perf_counter()
        pygo_core.run()
        dt = time.perf_counter() - t0
        if dt < best:
            best = dt
    return best


def main():
    print("pygo snap microbench")
    print("backend:", pygo_core.backend())
    print()
    print("FAST PATH (1 coro tight loop -- nothing else ready, snap skipped):")
    for n in (100_000, 1_000_000, 5_000_000):
        dt = measure(1, n)
        rate = n / dt
        print("  {:>7} yields  ->  {:>6.1f} ms  ({:>5.2f} M y/s, "
              "{:>4.0f} ns/yield)".format(
                  n, dt * 1000.0, rate / 1e6, dt * 1e9 / n))
    print()
    print("SLOW PATH (2 coros ping-pong -- full snap + asm yield per cycle):")
    for n in (50_000, 500_000, 2_000_000):
        total = 2 * n
        dt = measure(2, n)
        rate = total / dt
        print("  {:>7} yields  ->  {:>6.1f} ms  ({:>5.2f} M y/s, "
              "{:>4.0f} ns/yield)".format(
                  total, dt * 1000.0, rate / 1e6, dt * 1e9 / total))


if __name__ == "__main__":
    main()
