"""Phase C M:N bench: pygo vs asyncio vs plain Python threading.

Workload: N goroutines / tasks each doing M SHA-256 iterations of
a 500-byte buffer (CPU-bound, no I/O).

On free-threaded Python 3.13t (GIL disabled) pygo's M:N hub pool
gets actual core-level parallelism.  asyncio is still single-OS-
thread (no GIL benefit) and threading hits the same per-thread
overhead but without our work-stealing.

Run as:
    ~/.pyenv/versions/3.13.13t/bin/python3.13t bench/bench_mn.py
"""
import asyncio
import hashlib
import os
import sys
import threading
import time

sys.path.insert(0, "src")
import pygo_core


N = 100
ITER = 5000


def work(n):
    x = b"x" * 500
    for _ in range(n):
        x = hashlib.sha256(x).digest()


# ----- pygo M:N -----
def bench_pygo(n_hubs):
    pygo_core.mn_init(n_hubs)
    t0 = time.perf_counter()
    for _ in range(N):
        pygo_core.mn_go(lambda: work(ITER))
    pygo_core.mn_run()
    t = time.perf_counter() - t0
    pygo_core.mn_fini()
    return t


# ----- asyncio -----
async def asyncio_worker():
    work(ITER)


async def asyncio_main():
    await asyncio.gather(*[asyncio_worker() for _ in range(N)])


def bench_asyncio():
    t0 = time.perf_counter()
    asyncio.run(asyncio_main())
    return time.perf_counter() - t0


# ----- plain threading -----
def bench_threading(n_threads):
    """N tasks distributed across n_threads via a queue."""
    from queue import Queue
    q = Queue()
    for _ in range(N):
        q.put(ITER)
    def worker():
        while True:
            try:
                iters = q.get_nowait()
            except Exception:
                return
            work(iters)
    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - t0


def fmt(t):
    return "{0:>7.0f} ms  {1:>7.2f} M ops/s".format(t * 1000, N * ITER / t / 1e6)


def main():
    print("python:", sys.version.split()[0])
    print("gil:", sys._is_gil_enabled())
    print("cpu:", os.cpu_count())
    print()
    total = N * ITER
    print("Workload: {0} tasks * {1} sha256 = {2:,} hashes".format(
        N, ITER, total))
    print()
    print("                              wall          throughput")
    for n in (1, 2, 4, 8):
        t = bench_pygo(n)
        print("pygo M:N {0:>2} hubs:           {1}".format(n, fmt(t)))
    print()
    t = bench_asyncio()
    print("asyncio (1 thread):          {0}".format(fmt(t)))
    print()
    for n in (1, 2, 4, 8):
        t = bench_threading(n)
        print("plain threading {0:>2}:           {1}".format(n, fmt(t)))


if __name__ == "__main__":
    main()
