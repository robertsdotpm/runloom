"""HONEST_BENCH workload mix (see docs/dev/HONEST_BENCH.md).

Per request, pick a tier:
  fast 60%   -> 5 ms backend I/O   (1 DB call)
  medium 30% -> 10 ms              (2 DB calls)
  slow 8%    -> 15 ms              (3 DB calls)
  pathological 2% -> ~100 ms of pure-PYTHON CPU (cold codec / regex / batch).

The pathological tier is the whole point: 100 ms of un-awaitable CPU in 2% of
requests is head-of-line blocking that a single-threaded event loop cannot
hide -- every request queued behind it waits. pygo (preempted + multi-hub on
3.13t) keeps serving; asyncio/uvloop/gevent freeze for the duration.
"""
import time

# Calibrate a pure-Python busy loop to milliseconds, once, at import.
def _burn_iters_per_ms():
    n = 200_000
    t0 = time.perf_counter()
    x = 0
    for i in range(n):
        x += i * i
    dt = time.perf_counter() - t0
    return max(1, int(n / (dt * 1000.0)))

ITERS_PER_MS = _burn_iters_per_ms()


def burn_cpu(ms):
    """Spin ~ms of pure-Python CPU (no I/O, nothing to await/yield)."""
    n = ITERS_PER_MS * ms
    x = 0
    for i in range(n):
        x += i * i
    return x


def tier(r):
    """r in [0,1) -> (kind, seconds). kind 'io' sleeps; 'cpu' burns."""
    if r < 0.60:
        return ("io", 0.005)
    if r < 0.90:
        return ("io", 0.010)
    if r < 0.98:
        return ("io", 0.015)
    return ("cpu", 0.100)
