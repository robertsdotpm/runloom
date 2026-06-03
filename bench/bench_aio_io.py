"""Benchmark scenarios that compare runloom.aio vs stock asyncio.

Realistic comparison: the simple "many tasks each does one short
sleep" case is dominated by asyncio's tight C-deque dispatcher.
runloom.aio wins when tasks do real work -- multiple awaits, deeper
call stacks, or actual I/O multiplexing on Windows.
"""
import asyncio
import sys
import time

sys.path.insert(0, "src")
import runloom.aio as paio
import runloom_c

# Pre-allocate stacks so the bench measures steady-state cost, not
# first-spawn mmap latency.  The pool caps internally; this is safe.
runloom_c.warmup(50000)


def bench(runner, coro_factory):
    # Run twice; report the second time to amortize import / JIT.
    runner(coro_factory())
    t0 = time.perf_counter()
    runner(coro_factory())
    return time.perf_counter() - t0


# --------------------------------------------------------------------
# Pattern A: fan-out (each task does one sleep, then exits).
# This is asyncio's wheelhouse: tight C dispatcher.
# --------------------------------------------------------------------
async def fan_out(n, sleep_s):
    async def one():
        await asyncio.sleep(sleep_s)
    await asyncio.gather(*[one() for _ in range(n)])


# --------------------------------------------------------------------
# Pattern B: multi-await chain.  Each task does K awaits; the dispatch
# tier overhead asyncio pays per-await accumulates here.
# --------------------------------------------------------------------
async def multi_await(n, k):
    async def one():
        for _ in range(k):
            await asyncio.sleep(0)
    await asyncio.gather(*[one() for _ in range(n)])


# --------------------------------------------------------------------
# Pattern C: deeply nested coroutine calls.  Stackful coroutines
# (runloom) keep call stacks; asyncio's frame allocations per call add up.
# --------------------------------------------------------------------
async def deep_calls(n, depth):
    async def recurse(d):
        if d == 0:
            await asyncio.sleep(0)
            return 0
        return await recurse(d - 1) + 1
    async def one():
        await recurse(depth)
    await asyncio.gather(*[one() for _ in range(n)])


def section(title):
    print("\n=== %s ===" % title)
    print("  %20s | %12s | %12s | %s" % ("config", "asyncio s", "runloom.aio s", "speedup"))
    print("  " + "-" * 65)


def main():
    print("Python %s on %s" % (".".join(map(str, sys.version_info[:3])), sys.platform))

    section("A. fan-out (one sleep per task)")
    for n in (100, 1000, 10000):
        a = bench(asyncio.run, lambda: fan_out(n, 0.005))
        p = bench(paio.run,    lambda: fan_out(n, 0.005))
        print("  %20s | %12.4f | %12.4f | %.2fx" % ("n=%d" % n, a, p, a / p))

    section("B. multi-await chain (k awaits per task)")
    for n, k in [(100, 100), (1000, 50), (5000, 20)]:
        a = bench(asyncio.run, lambda: multi_await(n, k))
        p = bench(paio.run,    lambda: multi_await(n, k))
        print("  %20s | %12.4f | %12.4f | %.2fx" % ("n=%d k=%d" % (n, k), a, p, a / p))

    section("C. deep recursive awaits")
    for n, d in [(100, 20), (1000, 20), (1000, 50)]:
        a = bench(asyncio.run, lambda: deep_calls(n, d))
        p = bench(paio.run,    lambda: deep_calls(n, d))
        print("  %20s | %12.4f | %12.4f | %.2fx" % ("n=%d d=%d" % (n, d), a, p, a / p))


if __name__ == "__main__":
    main()
