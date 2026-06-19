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

### Handler work curve (what compiling the handler buys)

Echo ties every handler optimisation because it does no CPU work in the handler. This is the one experiment that gives the handler something to do: **one server, one knob** (`--work N` = an FNV-1a byte hash over the 1024 B payload, repeated N times), **two builds of the identical algorithm** &mdash; interpreted Python vs Cython-compiled &mdash; on the same runtime and I/O path. `--work 0` **is** the echo (lowest point), so it consolidates the echo load and reproduces it as a cross-check.[^bench]

| --work (FNV passes) | Python handler req/s | Cython handler req/s | Cython / Python |
|--:|--:|--:|--:|
| 0 (echo) | 615,403 | 613,316 | 1.00× |
| 1 | 82,137 | 584,879 | 7.12× |
| 4 | 25,332 | 495,938 | 19.58× |
| 16 | 6,931 | 478,025 | 68.97× |
| 64 | 1,740 | 273,827 | 157.34× |

> As the knob grows the interpreted handler goes server-bound and collapses while the compiled handler holds (up to **157.3×** here). The work is pure inline arithmetic, never offloaded to a worker thread, so per-core accounting stays valid. **Honest framing:** if the handler delegated to a C library (`hashlib`/`json`/`struct`) Python and Cython would converge &mdash; the gap is specific to *handler-level* Python work.[^bench]

### Real-work handler curve across runtimes (per core)

The same `--work` FNV hash in every runtime's natural handler language, reported per core (peak req/s ÷ pinned cores). It shows the result is honest: under real CPU work the **handler language** sets the tier, not the runtime.[^bench]

| Runtime | handler | cores | req/s per core @ echo | req/s per core @ work=64 |
|---|:--|--:|--:|--:|
| Go net (GOMAXPROCS=44) | compiled | 44 | 13,522 | 7,010 |
| Runloom (M:N) — Cython handler | compiled | 44 | 13,971 | 5,951 |
| Runloom (M:N) — Python handler | interpreted | 44 | 14,089 | 39 |
| asyncio Protocol (1 core) | interpreted | 1 | 38,122 | 31 |
| uvloop (1 core) | interpreted | 1 | 53,073 | 31 |
| gevent StreamServer (1 core) | interpreted | 1 | 20,737 | 29 |

> Two bands by handler language: compiled (runloom-Cython, Go) sit together per core under load, interpreted (runloom-py, asyncio, uvloop, gevent) sit together. At echo the single-core event loops lead per core (pure I/O pays no free-threading/M:N tax) — that inverts the instant the handler does work. runloom's edge: it reaches the compiled band while keeping M:N across all cores automatically; one asyncio process serialises the same work onto one core. Delegate to a C lib and all runtimes re-converge.[^bench]

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
