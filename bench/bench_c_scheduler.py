"""runloom C-scheduler vs Python-scheduler vs asyncio.

The C scheduler is the Phase A target: asm context switch + C-side
ready queue + single C-call yield.  At small fan-out it crushes asyncio
(~5M yields/s vs asyncio's ~0.6M).

NOTE: The C-scheduler currently crashes when many gs run in sequence
across multiple runloom_c.run() calls -- the CPython frame chain (tstate
cframe->current_frame) is not yet snapshotted per goroutine.  Phase B
work.  To get clean numbers we drive each cell in its own subprocess.
"""
import asyncio
import os
import subprocess
import sys
import time

sys.path.insert(0, "src")
import runloom
import runloom_c


def run_subprocess_bench(impl, n_coros, yields_per_coro):
    """Spawn a fresh python to run one cell of the matrix."""
    code = """
import sys; sys.path.insert(0, 'src')
import time
N = {0}; Y = {1}
if {2!r} == 'cgo':
    import runloom_c
    def w():
        for _ in range(Y): runloom_c.sched_yield()
    for _ in range(N): runloom_c.fiber(w)
    t0 = time.perf_counter()
    runloom_c.run()
    t = time.perf_counter() - t0
elif {2!r} == 'runloom':
    import runloom
    def w(n):
        for _ in range(n): runloom.yield_()
    for _ in range(N): runloom.fiber(w, Y)
    t0 = time.perf_counter()
    runloom.run()
    t = time.perf_counter() - t0
elif {2!r} == 'asyncio':
    import asyncio
    async def w(n):
        for _ in range(n): await asyncio.sleep(0)
    async def main():
        await asyncio.gather(*[w(Y) for _ in range(N)])
    t0 = time.perf_counter()
    asyncio.run(main())
    t = time.perf_counter() - t0
print(t)
""".format(n_coros, yields_per_coro, impl)
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    try:
        out = subprocess.run([sys.executable, "-c", code],
                              env=env, capture_output=True, timeout=60)
        if out.returncode != 0:
            return None
        return float(out.stdout.decode().strip())
    except (subprocess.TimeoutExpired, ValueError):
        return None


def fmt(t, ops):
    if t is None:
        return "    CRASH   "
    if t <= 0:
        return "       inf  "
    if ops / t >= 1e6:
        return "{0:>8.2f} M/s".format(ops / t / 1e6)
    return "{0:>8.0f} K/s".format(ops / t / 1e3)


def main():
    print("backend:", runloom_c.backend())
    print()
    print("Each cell is a fresh subprocess.")
    print()
    cases = [
        (10,    10),
        (10,   100),
        (50,   100),
        (100,  100),
        (150,  100),
        (300,  100),
        (1000, 100),
    ]
    print("{0:>6}  {1:>6}    {2:>13}    {3:>13}    {4:>13}".format(
        "coros", "yields", "runloom (C)", "runloom (Py)", "asyncio"))
    for n, y in cases:
        total = n * y
        c_t  = run_subprocess_bench("cgo", n, y)
        py_t = run_subprocess_bench("runloom", n, y)
        as_t = run_subprocess_bench("asyncio", n, y)
        print("{0:>6d}  {1:>6d}    {2:>13}    {3:>13}    {4:>13}".format(
            n, y, fmt(c_t, total), fmt(py_t, total), fmt(as_t, total)))


if __name__ == "__main__":
    main()
