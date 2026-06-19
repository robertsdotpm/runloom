"""Sustained spawn-rate bench: how many gs/sec can runloom handle when
each one does a small amount of work and finishes?  This is the
load pattern of a real server -- not "spawn 100k upfront" (which
maxes out the working set + thrashes cache), but "1k batches of
100 short-lived gs" (which is what a 100k req/s server actually
looks like).

Two modes:
  - "noop": worker does nothing.  Pure scheduler overhead.
  - "cpu":  worker does a small int math loop (~1 us of work).
            Closer to a real request handler.
"""
import sys
import time

sys.path.insert(0, "src")
import runloom_c


def bench(total, batch, mode):
    """Spawn `total` gs in groups of `batch`, run after each batch.
    Peak concurrency stays at `batch`.  Returns total wall time."""
    if mode == "noop":
        def w():
            pass
    elif mode == "cpu":
        def w():
            s = 0
            for i in range(50):       # ~1 us of int math
                s += i * i
    else:
        raise ValueError(mode)

    t0 = time.perf_counter()
    spawned = 0
    while spawned < total:
        n = min(batch, total - spawned)
        for _ in range(n):
            runloom_c.fiber(w)
        runloom_c.run()
        spawned += n
    return time.perf_counter() - t0


def main():
    print("runloom sustained-rate bench")
    print("backend:", runloom_c.backend())
    print()
    print("(spawn + run, peak concurrency = batch size)")
    print()
    cases = [
        # (total, batch, mode)
        (100_000,   100, "noop"),
        (100_000,  1000, "noop"),
        (100_000, 10000, "noop"),
        (100_000,   100, "cpu"),
        (100_000,  1000, "cpu"),
    ]
    fmt = "  total={:>7} batch={:>5} mode={:<5}  {:>8.3f}s   {:>7.0f} K gs/s   {:>5.0f} ns/g"
    for total, batch, mode in cases:
        dt = bench(total, batch, mode)
        rate = total / dt
        ns = dt * 1e9 / total
        print(fmt.format(total, batch, mode, dt, rate / 1000, ns))


if __name__ == "__main__":
    main()
