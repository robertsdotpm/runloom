"""Per-yield latency microbench targeting the Phase B snap/load path.

Single goroutine, tight loop of sched_yield -- each iteration is one
full save-tstate + asm-yield + resume + load-tstate cycle.  Lets us
measure the snap path in isolation, away from spawn/scheduling cost.
"""
import sys
import time

sys.path.insert(0, "src")
import runloom_c


def make_yielder(fn, n):
    def w():
        for _ in range(n):
            fn()
    return w


def measure(fn, n_coros, n_yields, repeats=5):
    best = float("inf")
    for _ in range(repeats):
        for _ in range(n_coros):
            runloom_c.fiber(make_yielder(fn, n_yields))
        t0 = time.perf_counter()
        runloom_c.run()
        dt = time.perf_counter() - t0
        if dt < best:
            best = dt
    return best


def main():
    print("runloom snap microbench")
    print("backend:", runloom_c.backend())
    print()
    variants = [
        ("vectorcall (default)", runloom_c.sched_yield),
        ("METH_NOARGS (classic)", runloom_c.sched_yield_classic),
    ]
    print("FAST PATH (1 coro tight loop -- nothing else ready):")
    for label, fn in variants:
        dt = measure(fn, 1, 2_000_000)
        rate = 2_000_000 / dt
        print("  {:<24}  {:>5.2f} M y/s  ({:>4.0f} ns/yield)".format(
            label, rate / 1e6, dt * 1e9 / 2_000_000))
    print()
    print("SLOW PATH (2 coros ping-pong -- full snap + asm yield per cycle):")
    for label, fn in variants:
        dt = measure(fn, 2, 1_000_000)
        rate = 2_000_000 / dt
        print("  {:<24}  {:>5.2f} M y/s  ({:>4.0f} ns/yield)".format(
            label, rate / 1e6, dt * 1e9 / 2_000_000))


if __name__ == "__main__":
    main()
