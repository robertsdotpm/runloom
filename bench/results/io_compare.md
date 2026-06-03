# TCP echo I/O comparison results

Go loadgen, keepalive 10 B req / 1029 B resp, N=1000 conns, io=1 ms backend,
6 s measure window. Shared box; ratios are the result. Two representative
runs each; numbers below are representative.

## Single core, GIL'd 3.13.13 (all four comparable, incl. gevent)

| runtime | rps | p50 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| asyncio | 13.8K | 71ms | 108ms | 114ms | 114ms |
| gevent (greenlet+libev) | 13.2K | 74ms | 135ms | 170ms | 183ms |
| uvloop (libuv) | 24.2K | 32ms | 54ms | 410ms | 8.0s |
| **pygo (1 hub)** | **21.4K** | 45ms | 67ms | 81ms | 84ms |

- pygo ≈ **1.6× gevent** (its direct blocking-style analog) and **1.55×
  asyncio**, with the **tightest tail** of the four.
- uvloop's peak is ~12% above pygo, but its tail blew out (p99.9 410ms, max
  ~8s, hundreds of requests in the overflow bucket) under this connection
  load; pygo stayed bounded (max 84ms).

## Multi-core, free-threaded 3.13t (pygo's native runtime)

| runtime | rps | p50 | p99.9 | max | cores |
| --- | ---: | ---: | ---: | ---: | --- |
| asyncio | 11.3K | 87ms | 114ms | 116ms | 1 |
| uvloop | 19.9K | 41ms | 409ms | 8.2s | 1 |
| pygo (1 hub) | 19.6K | 49ms | 86ms | 88ms | 1 |
| **pygo (8 hub)** | **121K** | **7.9ms** | 16ms | 42ms | 8 |

pygo **scales ~6.2× to 8 hubs** — multi-core that gevent/uvloop/asyncio
cannot do (single loop, single core by design). gevent can't run here at all.
