"""Benchmark pygo.aio vs stock asyncio across three common patterns.

Each test runs the same async code under both loops and reports ns
per logical operation.  Patterns chosen to surface the dispatch-tier
overhead asyncio incurs per await:

  1. await chain        N awaits on already-done futures (microbench)
  2. sleep(0) loop      N tasks each does sleep(0) M times (gather)
  3. fan-out ping-pong  N tasks each does P round-trips through a
                        shared asyncio.Queue

Run:
    PYTHONPATH=src python3 examples/bench_aio.py
"""
import asyncio
import gc
import sys
import time

sys.path.insert(0, "src")
import pygo.aio as paio


# --------------------------------------------------------------------
# Pattern 1: chain of N awaits on already-resolved futures.  This is
# the most-direct measurement of per-await overhead.
# --------------------------------------------------------------------
async def chain_main(n):
    loop = asyncio.get_running_loop()
    futs = []
    for _ in range(n):
        f = loop.create_future()
        f.set_result(None)
        futs.append(f)
    for f in futs:
        await f


# --------------------------------------------------------------------
# Pattern 2: N tasks each does M `await asyncio.sleep(0)` -- exercises
# the "bare yield" path through gather.
# --------------------------------------------------------------------
async def sleep_zero(m):
    for _ in range(m):
        await asyncio.sleep(0)

async def sleep_main(n, m):
    await asyncio.gather(*[sleep_zero(m) for _ in range(n)])


# --------------------------------------------------------------------
# Pattern 3: N tasks ping-pong through one asyncio.Queue.  Hits the
# Future + lock + dispatch path heavily.
# --------------------------------------------------------------------
async def pingponger(q_in, q_out, p):
    for _ in range(p):
        v = await q_in.get()
        await q_out.put(v + 1)

async def pingpong_main(n, p):
    q1 = asyncio.Queue()
    q2 = asyncio.Queue()
    # Round-trip: each task pulls from q1, pushes to q2; another task
    # reverses that.  N pairs.
    tasks = []
    for _ in range(n):
        tasks.append(asyncio.create_task(pingponger(q1, q2, p)))
        tasks.append(asyncio.create_task(pingponger(q2, q1, p)))
    # Seed N values into q1 to start each chain.
    for _ in range(n):
        await q1.put(0)
    await asyncio.gather(*tasks)


# --------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------
def bench(name, coro_factory, ops, *, runner):
    gc.collect()
    t0 = time.perf_counter()
    runner(coro_factory())
    dt = time.perf_counter() - t0
    ns_per = dt * 1e9 / ops
    print("  %-20s  %.3f s  -> %7.1f ns/op   (%d ops)" % (name, dt, ns_per, ops))
    return ns_per


def run_asyncio(coro):
    asyncio.run(coro)


def run_pygo(coro):
    paio.run(coro)


def header(t):
    print("\n=== %s ===" % t)


def main():
    print("Python %s   on %s" % (".".join(map(str, sys.version_info[:3])), sys.platform))

    # 1. await-chain microbench
    header("1. await chain (per-await overhead)")
    N = 200_000
    a = bench("asyncio",   lambda: chain_main(N), N, runner=run_asyncio)
    p = bench("pygo.aio",  lambda: chain_main(N), N, runner=run_pygo)
    print("  pygo.aio is %.2fx asyncio" % (a / p))

    # 2. sleep(0) gather
    header("2. sleep(0) gather (dispatch + bare-yield)")
    N, M = 100, 200
    ops = N * M
    a = bench("asyncio",   lambda: sleep_main(N, M), ops, runner=run_asyncio)
    p = bench("pygo.aio",  lambda: sleep_main(N, M), ops, runner=run_pygo)
    print("  pygo.aio is %.2fx asyncio" % (a / p))

    # 3. queue ping-pong
    header("3. asyncio.Queue ping-pong")
    N, P = 50, 100
    ops = N * P * 2 * 2     # 2 sides * 2 ops/side per iter
    a = bench("asyncio",   lambda: pingpong_main(N, P), ops, runner=run_asyncio)
    p = bench("pygo.aio",  lambda: pingpong_main(N, P), ops, runner=run_pygo)
    print("  pygo.aio is %.2fx asyncio" % (a / p))


if __name__ == "__main__":
    main()
