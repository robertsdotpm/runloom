# HONEST_BENCH results — production-shaped workload with a pathological tier

Implements docs/dev/HONEST_BENCH.md. Per-request tier mix: fast 60% (5 ms
I/O) / medium 30% (10 ms) / slow 8% (15 ms) / **pathological 2% (~100 ms of
pure-Python CPU)**. The pathological tier is head-of-line blocking that a
single-threaded event loop cannot hide. Go loadgen, N=200 conns, 8 s window.
Servers: bench/io_compare/honest/.

## Multi-core, free-threaded 3.13t (runloom's native runtime)

| runtime | rps | p50 | p99 | max | overflow |
| --- | ---: | ---: | ---: | ---: | ---: |
| asyncio | 496 | 404 ms | 409 ms | 1.3 s | 1948 |
| uvloop | 353 | 409 ms | 409 ms | 11.2 s | 1501 |
| **runloom (8 hub)** | **3496** | **11 ms** | 271 ms | 526 ms | 29 |

The 2% pathological tier **freezes the single-threaded loops**: p50 jumps to
~405 ms and throughput collapses to ~350–500 rps (every request queued behind
a 100 ms CPU block waits). runloom holds **p50 = 11 ms (37× better) and 7–10×
the throughput** — its 8 hubs absorb the CPU blocks on other cores.

## Single core, GIL'd 3.13 (incl. gevent; runloom = 1 hub)

| runtime | rps | p50 | max | overflow |
| --- | ---: | ---: | ---: | ---: |
| asyncio | 739 | 275 ms | 788 ms | 869 |
| gevent | 469 | 403 ms | 1.3 s | 1777 |
| uvloop | 491 | 403 ms | 1.2 s | 1696 |
| runloom (1 hub) | 708 | 281 ms | 677 ms | 1047 |

**On one core, everyone collapses** — the pathological tier is CPU-bound, and
no scheduler can make one core do more work. runloom-1hub ≈ asyncio (preemption
buys a little fairness, not throughput). 

## Conclusion (honest)

runloom's advantage on the pathological tier is **multi-core (free-threading)**,
not preemption alone: it is the only one of the four that can run the 100 ms
CPU blocks on other cores while continuing to serve. asyncio / uvloop / gevent
are single-loop, single-core by design and have no escape from a CPU-heavy
request — exactly the dishonesty in echo-only benchmarks that HONEST_BENCH
was built to expose.
