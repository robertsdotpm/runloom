"""Concurrent-yield stress test for Phase B.

Spawns N goroutines, each yielding K times.  Before Phase B, runloom
crashed somewhere around 150-200 concurrent yielded goroutines because
each suspended goroutine's frames stayed linked into one shared cframe
chain on the OS thread, and CPython traceback walks / recursion checks
walked the whole chain and overflowed the C stack.

After Phase B, each goroutine snapshots its own cframe / current_frame /
recursion budget at yield time and the chains are independent.  We
should see clean runs at 500, 1000, 2000 concurrent.
"""
import sys, time
sys.path.insert(0, "src")
import runloom_c


def make_worker(yields):
    def worker():
        for _ in range(yields):
            runloom_c.sched_yield()
    return worker


def run_burst(n, yields):
    t0 = time.perf_counter()
    for _ in range(n):
        runloom_c.fiber(make_worker(yields))
    runloom_c.run()
    dt = time.perf_counter() - t0
    total_yields = n * yields
    rate = total_yields / dt if dt > 0 else 0
    print("  {:>5} coros x {:>4} yields = {:>9} yields in {:>6.3f}s "
          "({:>7.2f}K y/s)".format(
              n, yields, total_yields, dt, rate / 1000.0))


def main():
    print("runloom concurrent-yield stress test (Phase B)")
    print("--------------------------------------------")
    # Start small to confirm baseline, then ramp up past where the
    # cliff used to be.
    for n in (50, 100, 200, 500, 1000, 2000):
        run_burst(n, 50)
    print("--- all bursts completed; no frame-chain cliff ---")


if __name__ == "__main__":
    main()
