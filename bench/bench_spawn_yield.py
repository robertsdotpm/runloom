"""runloom bench: spawn cost + yield cost vs asyncio.

Both libraries run the same shape -- N coros, each yields M times.
"""
import asyncio
import sys
import time

sys.path.insert(0, "src")

import runloom
import runloom_c


def bench_runloom(n_coros, yields_per_coro):
    def worker(n):
        for _ in range(n):
            runloom.yield_()

    t0 = time.perf_counter()
    for _ in range(n_coros):
        runloom.go(worker, yields_per_coro)
    spawn = time.perf_counter() - t0

    t0 = time.perf_counter()
    runloom.run()
    run_t = time.perf_counter() - t0
    total = n_coros * yields_per_coro
    return spawn, run_t, total


def bench_asyncio(n_coros, yields_per_coro):
    async def worker(n):
        for _ in range(n):
            await asyncio.sleep(0)

    async def main():
        tasks = [asyncio.create_task(worker(yields_per_coro))
                 for _ in range(n_coros)]
        await asyncio.gather(*tasks)

    t0 = time.perf_counter()
    asyncio.run(main())
    total_t = time.perf_counter() - t0
    total = n_coros * yields_per_coro
    return total_t, total


def fmt_throughput(t, ops):
    if t <= 0:
        return "inf"
    return "{0:>10.1f} K/s".format(ops / t / 1000)


def main():
    print("backend:", runloom_c.backend())
    print()
    cases = [
        (100,   100),
        (1000,  100),
        (1000,  1000),
        (10000, 100),
    ]
    print("{0:>8}  {1:>8}  {2:>14}  {3:>14}  {4:>14}  {5:>14}  {6:>10}".format(
        "N coros", "yields", "runloom spawn", "runloom run", "runloom K/s",
        "asyncio run", "asyncio K/s"))
    for n, y in cases:
        ps, pr, total = bench_runloom(n, y)
        at, _ = bench_asyncio(n, y)
        print("{0:>8d}  {1:>8d}  {2:>11.3f} ms  {3:>11.3f} ms  {4:>14}  {5:>11.3f} ms  {6:>14}".format(
            n, y, ps * 1000, pr * 1000, fmt_throughput(pr, total),
            at * 1000, fmt_throughput(at, total)))


if __name__ == "__main__":
    main()
