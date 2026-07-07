"""M:N core-scaling benchmark -- runloom's headline value proposition.

CPU-bound workload (chained SHA-256, no I/O, nothing to preempt) spread
over a varying number of M:N hubs on free-threaded 3.13t.  With the GIL
off, runloom's hub pool gets real cores, so wall time should fall ~linearly
with hub count until memory bandwidth / NUMA / desktop noise caps it.

Reported per config: throughput (hashes/s), speedup vs 1 hub, and parallel
efficiency (speedup / hubs).  Baselines: a raw sequential call (no runloom),
plain threading (also GIL-free parallel), and asyncio (single OS thread, so
no parallel benefit -- the thing runloom beats).

Run:
    PYTHONPATH=src ~/.pyenv/versions/3.14.4t/bin/python -m bench.mn

Tunables: RUNLOOM_BENCH_N (goroutines), RUNLOOM_BENCH_ITER (sha256 chain length).
"""
import hashlib
import os
import threading

import runloom_c

from bench.gil import ensure_nogil
from bench.harness import Suite, default_pin_set

N = int(os.environ.get("RUNLOOM_BENCH_N", "128"))
ITER = int(os.environ.get("RUNLOOM_BENCH_ITER", "2000"))
HUB_COUNTS = [1, 2, 4, 8, 16]
TOTAL = N * ITER  # total sha256 ops per sample -> ops_per_s == hashes/s


def work(n):
    x = b"x" * 512
    for _ in range(n):
        x = hashlib.sha256(x).digest()
    return x


def make_sequential():
    def once():
        for _ in range(N):
            work(ITER)
    return once


def make_mn(hubs):
    """Returns (setup, once, teardown).

    The hub pool is created ONCE in setup and torn down in teardown, both
    untimed, so the per-sample number is pure work-dispatch + run -- not the
    ~12ms pool spin-up/teardown that would otherwise mask scaling at high
    hub counts.  mn_run is reusable after a single mn_init.
    """
    mn_init, mn_fiber, mn_run, mn_fini = (
        runloom_c.mn_init, runloom_c.mn_fiber, runloom_c.mn_run, runloom_c.mn_fini)

    def setup():
        mn_init(hubs)

    def once():
        for _ in range(N):
            mn_fiber(lambda: work(ITER))
        mn_run()

    def teardown():
        mn_fini()

    return setup, once, teardown


def make_threading(nthreads):
    """N tasks distributed across nthreads OS threads via a shared counter."""
    def once():
        from queue import SimpleQueue
        q = SimpleQueue()
        for _ in range(N):
            q.put(ITER)
        for _ in range(nthreads):
            q.put(None)

        def worker():
            while True:
                m = q.get()
                if m is None:
                    return
                work(m)

        ts = [threading.Thread(target=worker) for _ in range(nthreads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
    return once


def make_asyncio():
    import asyncio

    async def worker():
        work(ITER)

    async def main():
        await asyncio.gather(*[worker() for _ in range(N)])

    def once():
        asyncio.run(main())
    return once


def main():
    # Force the GIL off before measuring real M:N parallelism, which a
    # silently re-enabled GIL would invalidate.  In main(), not at import,
    # so pytest collection of this module stays side-effect-free.
    ensure_nogil()
    # Need at least max(HUB_COUNTS) cpus to show real scaling.
    cpus = default_pin_set(n=max(HUB_COUNTS), node=1)
    s = Suite("mn", pin_cpus=cpus, samples=10, warmup=2)
    s.banner()
    print("workload: %d goroutines x %d-deep SHA-256 chain = %d hashes/sample\n"
          % (N, ITER, TOTAL))

    seq = s.bench("sequential (no runloom)", make_sequential(), inner=TOTAL,
                  note="single-thread reference")
    base1 = None
    mn_results = {}
    for h in HUB_COUNTS:
        if h > len(cpus):
            break
        setup, once, teardown = make_mn(h)
        r = s.bench("mn %2d hub" % h, once, inner=TOTAL, note="%d M:N hubs" % h,
                    setup=setup, teardown=teardown)
        mn_results[h] = r
        if h == 1:
            base1 = r
    s.bench("threading x%d" % min(16, len(cpus)),
            make_threading(min(16, len(cpus))), inner=TOTAL,
            note="GIL-free OS threads")
    s.bench("asyncio (1 thread)", make_asyncio(), inner=TOTAL,
            note="no parallelism -- the baseline runloom beats")

    # Scaling table (uses the robust min(best) sample per config).
    print("\n  scaling (best-sample throughput):")
    print("  %-12s %14s %9s %11s" % ("config", "hashes/s", "speedup", "efficiency"))
    seq_tput = TOTAL / seq["min_s"]
    print("  %-12s %14s %9s %11s"
          % ("sequential", fmt_hashes(seq_tput), "1.00x", "-"))
    if base1:
        b = TOTAL / base1["min_s"]
        for h, r in mn_results.items():
            t = TOTAL / r["min_s"]
            print("  %-12s %14s %8.2fx %10.0f%%"
                  % ("mn %d hub" % h, fmt_hashes(t), t / b, 100.0 * (t / b) / h))
    s.write()


def fmt_hashes(x):
    return "%.2f M/s" % (x / 1e6)


if __name__ == "__main__":
    main()
