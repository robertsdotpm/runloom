# Runloom

Go-style stackful coroutines for Python. Write **blocking** code — `fiber(fn)`,
plain `recv`/`send`, no `async`/`await` — and run a million of them across every
core in one process. Hand-rolled asm context switch + C work-stealing scheduler +
netpoll, built for **free-threaded Python 3.14t** (GIL off).

```python
import threading, runloom
from urllib.request import urlopen
runloom.monkey.patch()

def crawl(url):
    # urlopen() looks blocking -- but monkey.patch() parks the goroutine on the
    # socket instead of the OS thread, so all 64 fetches overlap on real cores.
    body = urlopen(url, timeout=10).read()
    print(threading.get_native_id(), len(body))

def main():
    for _ in range(64):
        runloom.fiber(crawl, "http://example.com")

runloom.run(8, main)   # 8 hub threads -> real cores on 3.14t (GIL off)
```

## Runloom vs Go

Same box (64c, free-threaded CPython 3.13t), 8 hubs / `GOMAXPROCS=8`, warm
steady-state. Go ≈ 2.1 M spawn/s here.

| metric | runloom | Go | verdict |
| --- | ---: | ---: | --- |
| **spawn** — pure C (`c_entry`) | **2.29 M/s** | 2.10 M/s | **beats Go** |
| **spawn** — Python (`runloom.fiber`) | 1.35 M/s | 2.10 M/s | 0.65× |
| **context switch** | ~75 ns yield · ~560 ns chan RT | ~50 ns `Gosched` | ~parity |
| **conn/s** — churn (new conn per req) | ~75–78 k/s | ~75–78 k/s | **parity** |
| **req/s** — keep-alive echo, Python handler | 596 k/s | 603 k/s | **0.99× — parity** (C handler beats Go) |
| **memory** — empty parked fiber | 8.8 KB | 2.7 KB | 3.3× (the one real gap) |

The short story: on **spawn, scheduling, and throughput, runloom trades blows
with Go and beats it on raw spawn** — a stackful coroutine runtime on CPython
matching a compiled language even with a Python handler (596 k vs 603 k req/s at
saturation; a C handler beats Go). The one honest gap left is **memory**: a
suspended fiber carries a CPython eval frame, ~3.3× Go's per-fiber RSS.
Full cross-runtime numbers + cold spawn-vs-N curves: **[benchmark report](https://github.com/robertsdotpm/runloom/blob/main/benchmark/report.html)**
· [perf summary](https://github.com/robertsdotpm/runloom/blob/main/docs/dev/PERF_SUMMARY.md).

```python
runloom.optimize("throughput")   # runloom.fiber -> max spawn rate (fiber_fast)
runloom.optimize("memory")       # runloom.fiber -> small right-sized stacks (default)
```

## Install

```bash
pip install runloom
```

```python
import runloom      # scheduler + channels, plus monkey/time/context/sync/aio
```

Prebuilt **wheels** (no compiler needed) for CPython 3.11–3.14 on Linux
(x86_64/aarch64), macOS (arm64/x86_64), Windows (AMD64); source build elsewhere.
**No runtime dependencies.**

## What it is

- **Hand-rolled asm context switch** (x86_64 SysV, aarch64) — ~80 ns/swap, no
  syscall; Windows Fibers / POSIX `ucontext` fallback.
- **M:N work-stealing scheduler** (3.13t) — Chase-Lev deque per hub, per-hub MPSC
  submission, woken goroutines routed back to their origin hub.
- **Per-goroutine `PyThreadState` snapshot** — cframe, datastack, exc_info,
  contextvars, recursion; a million yielded goroutines share their hub threads
  with no frame-chain cliff.
- **netpoll** — epoll / kqueue / IOCP / WSAPoll / select; goroutines park
  transparently on fd readiness, lost-wake-free 3-state park-commit.
- **Go-style channels** — `Chan(capacity)`, `select`, `for v in ch`.
- **Stall isolation + recovery** — one unanticipated blocking call stalls only
  its hub, and the runtime detects + recovers it (default on, 3.13t).
- **`monkey.patch()`** makes blocking stdlib (`socket`, `time`, `threading`, …)
  cooperative, so existing blocking code runs unchanged.

Already have `async def` code? The **`runloom.aio`** bridge runs it on the
single-threaded scheduler (`runloom.aio.run(main())` ≈ `asyncio.run`) — a
zero-rewrite port path, not a multi-core speedup (use the sync API with
`run(n>1, main)` for that).

## Honest limitations

- **The multi-core win needs free-threaded CPython 3.13t** (3.11+ for the frame
  snapshot at all). On a GIL build runloom still runs — cheap spawn, the
  goroutine model, netpoll — but single-core like asyncio.
- **runloom doesn't make Python faster per core.** CPython's ~80 k pure-Python
  ops/s/core is a constant it can't raise; it lets one process hit that on every
  core at once with a blocking model. The scheduler itself is Go-class.
- **Higher memory per goroutine than Go** (~3.3× for an empty fiber — the CPython
  eval frame; a C handler closes most of it).
- **Preemption fires only at Python bytecode boundaries** — a goroutine inside a
  tight pure-C call (e.g. `numpy`) holds its hub until it returns (same as Go +
  cgo).
- **Linux x86_64 / 3.13t is the primary, heavily-validated target** (2 M-conn
  runs, fuzzing, sanitizers, formal models); other backends are maintained
  in-step but less deeply exercised.

## Platform support

| OS / arch | switch | netpoll | tested |
| --- | --- | --- | --- |
| Linux x86_64 | fcontext-asm | epoll | **yes — hw, 3.11 / 3.12 / 3.13t / 3.14t (primary)** |
| Linux aarch64 | fcontext-asm | epoll | qemu |
| macOS x86_64 / arm64 | fcontext-asm | kqueue | hw, 3.14t |
| FreeBSD / GhostBSD | fcontext-asm | kqueue | hw, 3.12 |
| Windows 10/11 / Server 2022 | Fibers | IOCP→WSAPoll→select | hw, 3.14t |
| Solaris / Android / other BSD | ucontext / asm | select / epoll / kqueue | review |

## Docs & layout

Full guide in [docs/](https://github.com/robertsdotpm/runloom/tree/main/docs/):
[Quickstart](https://github.com/robertsdotpm/runloom/blob/main/docs/quickstart.md) ·
[Asyncio bridge](https://github.com/robertsdotpm/runloom/blob/main/docs/asyncio.md) ·
[Sync API](https://github.com/robertsdotpm/runloom/blob/main/docs/sync-api.md) ·
[Channels](https://github.com/robertsdotpm/runloom/blob/main/docs/channels.md) ·
[M:N parallelism](https://github.com/robertsdotpm/runloom/blob/main/docs/parallelism.md) ·
[Cookbook](https://github.com/robertsdotpm/runloom/blob/main/docs/cookbook.md) ·
[API reference](https://github.com/robertsdotpm/runloom/blob/main/docs/api-reference.md)

| Dir | Contents |
| --- | --- |
| `src/runloom_c/` | C extension: scheduler, channels, netpoll, asm backends, M:N hubs, stall recovery |
| `src/runloom/` | Python layers: `aio`, `sync`, `monkey`, `time`, `runtime` |
| `tests/` · `examples/` · `benchmark/` · `docs/` | tests · runnable examples · benchmarks + perf harness · docs |

Build from source (contributors): `pip install -e .` from a clone (needs a C
compiler; `scripts/install.sh` / `scripts\install.bat` bootstrap one). To hack on
runloom against free-threaded CPython, use a 3.13t interpreter.
