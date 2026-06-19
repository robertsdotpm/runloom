## Benchmarks

Measured on Intel Xeon CPU E5-2696 v3 @ 2.30GHz, 64 vCPU, 78.6 GiB (free-threaded CPython 3.13t, GIL off) under a two-netns veth topology with disjoint CPU pinning. Full tables, per-connection curves, assumed constraints and every benchmark program are in the detailed report.[^bench]

### Echo throughput (1 KiB requests)

Requests/second, raw and normalised to a single core (multi-core servers divided by their core count).[^bench]

| Server | Cores | req/s | req/s per core | CPU-bound side |
|---|--:|--:|--:|---|
| uvloop (GIL, 1 core) | 1 | 58,015 | 58,015 | server |
| asyncio Protocol (GIL, 1 core) | 1 | 40,970 | 40,970 | server |
| gevent StreamServer (GIL, 1 core) | 1 | 19,881 | 19,881 | server |
| Runloom io_uring + Cython + optimize(throughput) | 44 | 642,200 | 14,595 | client |
| Runloom io_uring + Cython C handler | 44 | 631,954 | 14,363 | client |
| Runloom io_uring + Cython cdef handler (tstate-free c_entry) | 44 | 627,051 | 14,251 | client |
| Runloom C scaffold (py handler, C TCPConn) | 44 | 617,554 | 14,035 | client |
| Go net (GOMAXPROCS=44) | 44 | 602,706 | 13,698 | client |
| Runloom sync wrappers (epoll, py handler) | 44 | 593,730 | 13,494 | server |
| Runloom io_uring loop (py handler) | 44 | 589,112 | 13,389 | server |
| Runloom C scaffold + Cython C handler (epoll) | 44 | 423,458 | 9,624 | server |

> The 16-core Go loadgen saturates before the fastest servers (`client`-bound rows); the report gives a server-ceiling estimate from server CPU utilisation.[^bench]

> **io_uring:** driven through the Stage-2 proactor (`loop_recv`), the io_uring loop backend is a major win &mdash; the Cython handler on io_uring reaches a **1.16M req/s server ceiling (+2.17× over epoll)**, the fastest runloom config measured. "io_uring loses on loopback" was an artifact of driving it through the readiness path; see the findings writeup.[^bench]

### Memory per idle fiber

Used resident memory (RSS, not virtual) for 1,000,000 live parked fibers/goroutines.[^bench]

| Config | total RSS | bytes / fiber |
|---|--:|--:|
| go | 2.50 GiB | 2,685 |
| runloom_py | 8.24 GiB | 8,848 |
| runloom_c | 8.24 GiB | 8,848 |

### Scheduler micro-benchmarks

| Runtime | spawn (tasks/s) | ctx-switch (ns) |
|---|--:|--:|
| runloom | 51,815 | 25,392 |
| go | 1,348,486 | 676 |
| asyncio | 67,738 | 2,236 |
| uvloop | 74,129 | 1,324 |
| greenlet | 36,806 | 465 |

> Runloom fibers carry real C stacks (heavier to spawn than goroutines); its loaded-yield context-switch hits the free-threaded refcount wall at high hub counts. Strength is parallel I/O throughput, not single-stream latency.[^bench]

[^bench]: Full data, methodology, per-connection ladder curves, the assumed constraints, every benchmark program's source, and the zero-PyObject Cython disassembly proof: [`benchmark/report.html`](benchmark/report.html). Cross-platform backend syscall profiles (Linux/macOS/Windows) are linked from there.
