# runloom

> **Disclaimer:** all code written by Claude Opus 4.8.

Go-style stackful coroutines for Python. Write **blocking** code -- `go(fn)`,
plain `recv`/`send`, no `async`/`await` -- and run a million of them across
every core in one process. Hand-rolled asm context switch + C work-stealing
scheduler + netpoll, built for **free-threaded Python 3.13t** (GIL off).

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
        runloom.go(crawl, "http://example.com")

runloom.run(8, main) # 8 hub threads -> real cores on 3.13t (GIL off)
```

Already have `async def` code? Run it unchanged on runloom's scheduler with the
`runloom.aio` bridge (`runloom.aio.run(main())` ≈ `asyncio.run`). See [docs/](https://github.com/robertsdotpm/runloom/tree/main/docs/).

**The trade-off:** the bridge isn't a guaranteed speed-up -- it has more
per-task overhead than asyncio's own loop, so whether it comes out faster
depends on how many `await`s each task does before finishing.[^bridge] Think of
it as a way to run your existing async code on runloom's scheduler: point the
extension at your code and measure.

---

## Features

- **Hand-rolled asm context switch** (x86_64 SysV, aarch64) -- ~80 ns/swap, no
  syscall. Windows Fibers / POSIX `ucontext` fallback.
- **Per-goroutine `PyThreadState` snapshot** (cframe, datastack chunks,
  exc_info, contextvars, recursion) -- 50 000 yielded goroutines share one OS
  thread with no frame-chain cliff.
- **M:N work-stealing scheduler** (3.13t) -- Chase-Lev deque per hub, per-hub
  MPSC submission, goroutines routed back to their origin hub on wake.
- **netpoll** -- epoll / kqueue / IOCP / WSAPoll / select; goroutines park
  transparently on fd readiness. Lost-wake-free 3-state park-commit on all
  backends.
- **Go-style channels** -- `Chan(capacity)`, `select`, `for v in ch`; unbuffered
  ping-pong ~560 ns/round-trip (within 7% of Go 1.22 on the same box).

## How it compares -- the short answers

### 1. Memory vs Go

runloom's *own* per-coroutine bookkeeping is **< 0.3 KB** -- lighter than Go's `g`.
What you actually pay is the **CPython object tax**, not the scheduler:

| state | Go goroutine | runloom (Python handler) | runloom (C handler) |
| --- | ---: | ---: | ---: |
| empty / just spawned | ~2.5 KB | -- (running Python needs a datastack chunk) | -- |
| parked, holding a socket + read buffer | ~10–13 KB | **~26 KB** (~38 KB active) | **~7–8 KB** |

Straight answer: **in Python you will not beat Go on memory** (PyObject headers
make every `socket`/`bytes`/frame fatter than Go's structs) -- but a C handler
hits Go parity, and at scale the kernel's own socket buffers dominate in any
runtime anyway. The goroutine *model* is what makes a million feasible at all;
no runtime makes the connections themselves free.[^memory]

### 2. Speed

All measured; figures label whether the handler is Python or C.

- **Scheduler / context switch** (Python, 3.12): fast-path yield **47 ns**
  (75 ns on 3.13t) -- matching Go's `runtime.Gosched()` (~50 ns) and **~25–40×
  faster than asyncio** (~1800 ns/step). Sustains **2.4–3.1 M context
  switches/s** vs asyncio's ~0.4–0.5 M/s (**~5–8×**). A real cross-goroutine
  switch (2+ runnable, 3.13t) is ~360 ns -- **~1.6× faster than greenlet's
  raw `switch()`** (575 ns): runloom swaps registers via fcontext on separate
  stacks, greenlet copies stack on every switch.
- **Multi-core compute** (Python, 3.13t, 100 goroutines × SHA-256): scales
  **2.5× from 1→8 hubs to 2.12 M ops/s**, ≈ `threading`×8 (2.24 M) -- but with
  the goroutine model (cheap spawn, no thread-per-task explosion). asyncio
  gets **0× here -- it cannot use a second core**.
- **Network throughput vs Go** -- *same* in-process loopback echo bench (256
  concurrent conns × 8-byte round-trips, C `TCPConn` handler), Go and runloom at
  matched core/hub counts:

  | cores / hubs | Go (`net`) | runloom C `TCPConn` |
  | ---: | ---: | ---: |
  | 1  | 27 K/s · 37 µs/RT | 22 K/s · 46 µs/RT |
  | 4  | 94 K/s | 51 K/s |
  | 8  | 193 K/s | 100 K/s |
  | 16 | 324 K/s · 3.1 µs/RT | 143 K/s · 7.0 µs/RT |

  Go is **~1.2× per core** and **~2.3× at 16** -- it starts ~20% faster *and*
  scales better (75% vs runloom's 41% efficiency over 16 cores, as runloom hits
  CPython's refcount-contention ceiling). A **Python** handler is another
  ~2.2× slower than the C path. See *Current limitations* for why.

- **vs the async ecosystem** -- same TCP echo (N=1000 keepalive conns, 1 ms
  simulated backend I/O), Python handler, one core, on **GIL'd 3.13** so every
  runtime is single-core and comparable (gevent/uvloop don't run on
  free-threaded 3.13t -- gevent's cffi dep won't build):

  | runtime | rps | p50 | p99.9 | max |
  | --- | ---: | ---: | ---: | ---: |
  | asyncio | 13.8 K | 71 ms | 114 ms | 114 ms |
  | **gevent** (greenlet + libev) | 13.2 K | 74 ms | 170 ms | 183 ms |
  | uvloop (libuv) | 24.2 K | 32 ms | 410 ms | **8.0 s** |
  | **runloom** (1 core) | **21.4 K** | 45 ms | **81 ms** | **84 ms** |

  runloom is **~1.6× gevent** -- its closest analog (blocking-style code, one
  task per connection) -- and **~1.55× asyncio**, with the **tightest tail of
  the four**. uvloop's peak is ~12% higher, but its tail blew out (max ~8 s,
  hundreds of requests overflowing) under this connection load while runloom
  stayed bounded. And on free-threaded 3.13t runloom additionally **scales ~6× to
  8 cores (121 K rps, p50 7.9 ms)** -- multi-core that none of the others can
  do. Harness + numbers: [bench/io_compare/](https://github.com/robertsdotpm/runloom/tree/main/bench/io_compare/).

The honest ceiling: runloom does **not** make Python faster per core (~80 K
pure-Python ops/s/core is a CPython constant). It lets one process **hit that
on every core at once** with a blocking programming model -- which asyncio
(single loop, single core) structurally cannot.

### 3. vs asyncio / threads / processes

| | asyncio | OS threads | multiprocessing | **runloom** |
| --- | --- | --- | --- | --- |
| code style | `async`/`await` (colored) | blocking | blocking + IPC | **blocking, no async/await** |
| cores used | 1 (single loop) | N, but GIL-serialised (1 on GIL builds) | N | **N (3.13t, GIL off)** |
| practical max tasks | ~10⁵–10⁶ | ~10³–10⁴ (MB stacks) | ~10² | **10⁶+ (~26 KB/g)** |
| spawn cost | cheap | expensive (kernel thread) | very expensive | **cheap (asm swap)** |
| one unanticipated blocking call… | **stalls every task** | stalls just that thread | stalls just that process | **stalls just that hub -- and the runtime detects + recovers it** (see below) |

- **vs asyncio** -- runloom's structural wins: real multi-core parallelism,
  blocking-style code, and stall isolation+recovery. asyncio's wins: stdlib,
  mature, runs on any Python, huge ecosystem. runloom needs 3.13t for the
  multi-core win and is young.
- **vs threads** -- runloom scales to millions where threads cap at thousands (MB
  stacks + kernel scheduler overhead).
- **vs processes** -- runloom is one process (shared memory, cheap spawn);
  processes give hard isolation runloom doesn't.

---

## Proven at scale

**2,000,000 concurrent TCP connections in a single process** (both ends
in-process, ~4 M coexisting goroutines, C echo handlers), clean exit:

| connections | goroutines | peak RSS | VMAs | wall |
| ---: | ---: | ---: | ---: | ---: |
| 1,048,576 | ~2.1 M | 8.32 GB | 3.96 M | 174.7 s |
| **2,000,000** | **~4 M** | **14.27 GB** | **7.10 M** | **274.3 s** |

(The wall time is connection-setup-bound at 1 round-trip/conn, not a
throughput number -- see Speed above for req/s.) These are **C handlers**; a
Python handler is ~26 KB/g, so 2 M-with-Python is RAM-bound, not proven.
Large-N needs raised kernel limits -- see [docs](https://github.com/robertsdotpm/runloom/tree/main/docs/) (`vm.max_map_count`
is the one that bites first).

## Stall recovery (default ON, free-threaded 3.13t)

asyncio's fatal flaw is that one blocking task freezes the whole loop. runloom's
M:N scheduler isolates a stall to one hub, and a watchdog actively recovers it
(both default-on; opt out with `RUNLOOM_HANDOFF=0` / `RUNLOOM_PREEMPT=0`):

- **Blocking-IO wedge** (a goroutine in an unanticipated `Py_BEGIN_ALLOW_THREADS`
  call): a standby rescue thread adopts the stalled hub's thread-state and
  drains its stranded goroutines -- Go's `entersyscallblock` P-handoff. A pool
  recovers several simultaneously-wedged hubs in parallel.
- **CPU-bound wedge** (a goroutine monopolising a hub in a Python loop): the
  runtime preempts it at its next bytecode boundary so the hub round-robins --
  Go pre-1.14 cooperative preemption.

A >50 ms threshold keeps both dormant under normal load, so steady-state
scheduling is unchanged.

On a production-shaped workload (60/30/8% I/O tiers + a **2% tier doing
100 ms of pure-Python CPU** -- the head-of-line blocker echo benchmarks omit),
this is decisive: asyncio and uvloop freeze (p50 ~405 ms, ~400 rps as every
request queues behind a CPU block), while runloom holds **p50 11 ms at 7–10× the
throughput** by running the CPU blocks on other hubs. Numbers:
[bench/results/honest_bench.md](https://github.com/robertsdotpm/runloom/blob/main/bench/results/honest_bench.md).

## Stack safety (default ON)

Goroutine stacks are small (that's what makes a million of them affordable) but
safe: deep recursion raises `RecursionError` (not a crash), stacks grow on
demand, every stack has a guard page so an overflow faults cleanly instead of
corrupting a neighbour, and CPython's stack-hungry error paths can't blow a
goroutine. For a native call that needs a big stack in one shot, pass
`stack_size=`.[^bridgestack] And under M:N (`run(n>1)`) runloom **learns each
function's real stack need and reserves only that, on by default** -- the
function-bound *grow-down* (512 KB cold start → ~16 KB for a trivial handler,
in-memory only); turn it off with `runloom.set_grow_down(False)` or
`RUNLOOM_GROW_DOWN=0`, or pin a `stack_size=` to opt one function out. To find
which goroutine kinds are over- or under-reserving, `runloom.inspect.enable_stack_advice()`
then `print_stack_advice()` measures real per-kind stack use and suggests sizes.
Details: [docs/stack-sizing.md](https://github.com/robertsdotpm/runloom/blob/main/docs/stack-sizing.md#automatic-grow-down-on-by-default-mn).

## Ways to use it

runloom is one scheduler with several front-ends -- pick whichever fits your code;
they share the same goroutines and can be mixed.

- **`runloom.sync`** -- Go-style straight-line code: `go(fn)`, `Chan`, `select`,
  cooperative `sleep` and sockets. No `async`/`await`, no event-loop ceremony.
- **Stdlib monkey-patch** -- `runloom.monkey.patch()` makes blocking stdlib
  cooperative across ~20 categories, so `requests`, `pymysql`, plain `urllib`
  and friends run unchanged.[^monkey]
- **`runloom.aio`** -- run existing `async`/`await` code on the scheduler;
  high-fidelity enough to run **aiohttp, uvicorn, starlette, hypercorn,
  websockets and anyio** unchanged.[^aio]
- **`runloom.blocking(fn, …)` / `runloom.monkey.offload(fn, …)`** -- offload a
  genuinely-blocking or CPU-bound call to a worker pool so it never wedges a hub
  (runs inline when not on a goroutine).

## Correctness, verification & security

A lock-free M:N scheduler is exactly where bugs hide in rare interleavings, so
the concurrency core is checked from several independent angles. Every
machine-checked proof ships with a **negative control** that *must* fail, so the
checks are known to have teeth. One driver runs the lot: `scripts/check_all.sh
all`; the full writeup is in [docs/dev/VALIDATION.md](https://github.com/robertsdotpm/runloom/blob/main/docs/dev/VALIDATION.md) and [verify/](https://github.com/robertsdotpm/runloom/tree/main/verify/).

**Formal verification** ([verify/](https://github.com/robertsdotpm/runloom/tree/main/verify/), `verify/run_verify.sh`):

| engine | what it proves |
| --- | --- |
| **Spin** | the algorithms (Chase-Lev deque, `wake_state`, park/wake) over *all* interleavings -- safety **and** liveness (LTL + fairness) |
| **CBMC** | the **unmodified C source** of the deque, with its real `__atomic_*` memory orderings, over a bounded schedule |
| **herd7 / GenMC** | the C11/RC11 fence placement on the netpoll commit + wake paths, on a *weak* (RC11) memory model |
| **TLA+ / Alloy** | the *composed* scheduler + stall-recovery handoff (no lost/stranded goroutine); the `self_check` parker-graph invariant |
| **Coq / Iris / iRC11** | unbounded, machine-checked: `wake_state` & deque conservation over every reachable state, and the commit-publish release/acquire under RC11 |

**Testing:** the pytest suite plus a C deque stress harness (real threads,
millions of ops); a randomized M:N scheduler fuzzer; channel
**linearizability** via Porcupine + stateful Hypothesis; **deterministic
simulation** (seed → byte-identical repro) with PCT interleaving search; gcov
coverage that itemizes uncovered error/cleanup paths; and an `LD_PRELOAD`
**fault injector** that fails the Nth malloc/mmap/epoll_ctl so cleanup branches
run (0 cleanup-path bugs on the bundled workload). Backend conformance is
swept across epoll/kqueue/IOCP/WSAPoll/select.

**Security:** the C core runs clean under **ASan / TSan / UBSan**; `gcc
-fanalyzer` is a gating static-analysis phase (it found + fixed a real
NULL-deref); and optimized builds are hardened (`-fstack-protector-strong`,
`-D_FORTIFY_SOURCE=2`, `-Wformat-security`).

## Current limitations & downsides

Read this before betting on runloom -- it's where the project actually is.

- **The multi-core win needs free-threaded CPython 3.13t** (and 3.11+ at all --
  the frame snapshot depends on the 3.11+ tstate layout). On a normal GIL build
  runloom still runs -- cheap spawn, the goroutine model, netpoll -- but it's
  **single-core** like asyncio; the headline parallelism numbers don't apply.
- **runloom doesn't make Python faster per core.** It saturates every core from one
  process, but CPython's ~80 K pure-Python ops/s/core is a constant it can't
  raise, and Go stays faster on raw network I/O. None of that is the scheduler
  (~47 ns/yield, Go-class) -- it's the interpreter.[^percore]
- **Higher memory per goroutine than Go** (~26 KB with a Python handler vs Go's
  ~2.5–13 KB) -- the CPython object tax, only avoidable by dropping to C handlers.
- **Preemption only fires at Python bytecode boundaries.** A goroutine inside a
  tight pure-C call or third-party C extension (e.g. `numpy`) is **not**
  preemptible and holds its hub until it returns -- the same limitation Go has
  with cgo.[^preempt]
- **Linux x86_64 / 3.13t is the primary, heavily-validated target.**
  macOS/BSD/Windows and aarch64 backends are code-complete and maintained
  in-step, but the deep validation (2 M-conn runs, fuzzing, sanitizers) is on
  Linux; aarch64 is exercised via `qemu` + review, not yet on real ARM hardware.

## Install

```bash
pip install runloom
```

One import is all you need:

```python
import runloom      # scheduler + channels, plus monkey/time/context/sync/aio
```

When a prebuilt **wheel** exists for your platform + Python, `pip` just
downloads it — **no compiler, no build step**, like installing `numpy`.
Wheels are published for CPython 3.11–3.14 on Linux (x86_64/aarch64),
macOS (arm64/x86_64) and Windows (AMD64). On anything else, `pip` falls back
to the **source distribution** and compiles the C extension locally (you need
a C compiler then — see *Building*). runloom has **no runtime dependencies**.

> Maintainers: this project uses no hosted CI, so wheels are built by hand per
> platform — see [RELEASING.md](https://github.com/robertsdotpm/runloom/blob/main/RELEASING.md).

## Building from source (contributors)

Only needed to hack on runloom itself — normal use is `pip install runloom`
above. From a clone:

```bash
pip install -e .                                    # editable; compiles the C ext
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .   # free-threaded
```

Needs a C compiler; `scripts/install.sh` (POSIX) / `scripts\install.bat`
(Windows) bootstrap one if absent. Build knobs (`RUNLOOM_BACKEND`,
`RUNLOOM_NO_IOCP`, `RUNLOOM_DEBUG`, `RUNLOOM_EXTRA_CFLAGS`, …) and the
`cibuildwheel` matrix are documented in
[docs/](https://github.com/robertsdotpm/runloom/tree/main/docs/).

## Platform & Python support

| OS / arch | switch | netpoll | tested |
| --- | --- | --- | --- |
| Linux x86_64 | fcontext-asm | epoll | **yes -- hw, 3.11 / 3.12 / 3.13t (primary)** |
| Linux aarch64 | fcontext-asm | epoll | qemu |
| macOS x86_64 / arm64 | fcontext-asm | kqueue | hw (x86_64, 3.12) / review |
| FreeBSD / GhostBSD | fcontext-asm | kqueue | hw, 3.12 |
| Windows 10/11 / Server 2022 | Fibers | IOCP→WSAPoll→select | hw, 3.12 |
| Solaris / Android / other BSD | ucontext / asm | select / epoll / kqueue | code review |

One Windows binary spans Vista→11 (backend probed at runtime). Compilers: GCC
4.7+, Clang 3.5+, MSVC 19.20+, MinGW-w64, ICC 17+.

## Documentation

Full guide in [docs/](https://github.com/robertsdotpm/runloom/tree/main/docs/) (also on Read the Docs):
[Quickstart](https://github.com/robertsdotpm/runloom/blob/main/docs/quickstart.md) ·
[Asyncio bridge](https://github.com/robertsdotpm/runloom/blob/main/docs/asyncio.md) ·
[Sync API](https://github.com/robertsdotpm/runloom/blob/main/docs/sync-api.md) ·
[Channels](https://github.com/robertsdotpm/runloom/blob/main/docs/channels.md) ·
[Stack sizing](https://github.com/robertsdotpm/runloom/blob/main/docs/stack-sizing.md) ·
[Monkey-patching](https://github.com/robertsdotpm/runloom/blob/main/docs/monkey-patching.md) ·
[Preemption](https://github.com/robertsdotpm/runloom/blob/main/docs/preemption.md) ·
[M:N parallelism](https://github.com/robertsdotpm/runloom/blob/main/docs/parallelism.md) ·
[Cookbook](https://github.com/robertsdotpm/runloom/blob/main/docs/cookbook.md) ·
[API reference](https://github.com/robertsdotpm/runloom/blob/main/docs/api-reference.md)

## Layout

| Dir | Contents |
| --- | --- |
| `src/runloom_c/` | C extension: scheduler, channels, netpoll, asm backends, M:N hubs, stall recovery |
| `src/runloom/` | Python layers: `aio`, `sync`, `monkey`, `time`, `runtime` |
| `tests/` · `examples/` · `bench/` · `docs/` · `scripts/` | tests · runnable examples · benchmarks + perf harness · docs · bootstrap installers |

## Notes

[^bridge]: Tasks that do one `await` and finish (lots of tiny tasks, little work
    each) run about **5× slower** than plain asyncio; tasks that do many
    `await`s before finishing are **~1.7–1.9× faster**, as runloom's fast context
    switch amortises over the awaits.

[^memory]: A Python `socket` + `bytes` buffers + frame objects all carry
    PyObject headers and are simply fatter than Go structs and `[]byte`. Run the
    *same* handler in C (`runloom_mn_go_c`, no Python frames) and you're at Go
    parity. At scale the kernel's own socket buffers (~8 KB+/socket) dominate in
    any runtime, so a million live connections is ~tens of GB everywhere -- and
    vs ~1 MB-stack OS threads (where a million is impossible), the goroutine
    model is the only reason a million is feasible at all.

[^monkey]: Patched categories: `socket`/`ssl` (incl. `sendfile` + fd passing),
    files & `os` I/O, `select`/`selectors`, `subprocess` + child reaping (pidfd
    on Linux), `threading`/`queue`/`concurrent.futures`, `multiprocessing`,
    `fcntl` locks, `signal` waits, async DNS, and size-gated auto-offload of
    CPU-bound C calls (`hashlib`, `zlib`/`gzip`/`bz2`/`lzma`, KDFs above
    `RUNLOOM_OFFLOAD_BYTES`, default 256 KiB). Full reference:
    [Monkey-patching](https://github.com/robertsdotpm/runloom/blob/main/docs/monkey-patching.md).

[^aio]: Supported surface includes streams (`open_connection` / `start_server`),
    transports + protocols, UDP datagram endpoints, SSL client **and** server,
    `loop.add_reader` / `add_writer`, and `run_in_executor`. Full reference:
    [Asyncio bridge](https://github.com/robertsdotpm/runloom/blob/main/docs/asyncio.md). The running list of name-brand asyncio
    projects whose own test suites pass green under the bridge (driven through
    `RunloomEventLoop` on free-threaded 3.13t) is in [top_100.txt](https://github.com/robertsdotpm/runloom/blob/main/top_100.txt).

[^percore]: On the identical in-process loopback echo bench, Go is **~1.2×
    faster per core** and **~2.3× across 16 cores** (runloom hits CPython's
    refcount-contention ceiling), plus another **~2.2× if the handler is
    Python** rather than C. `RUNLOOM_TCPCONN_IOURING=1` narrows the syscall side at
    high fan-out on Linux. See *How it compares → Speed* above for the full
    table.

[^preempt]: The monkey layer's `heavy` category auto-offloads the common stdlib
    offenders (`hashlib`, `zlib`/`gzip`/`bz2`/`lzma`, KDFs) above a size gate,
    and `runloom.blocking(fn)` / `runloom.monkey.offload(fn)` are the manual escape
    hatch; the residual gap is a long non-stdlib C call you don't offload.
    (Blocking-IO is covered by the rescue handoff, pure-Python CPU by
    preemption.)

[^bridgestack]: The `runloom.aio` bridge does this for you: goroutines that run
    user protocol callbacks (`data_received`, `connection_made`, …) are spawned
    with a **512 KB** stack (`RUNLOOM_AIO_IO_STACK` / `RUNLOOM_AIO_TASK_STACK`),
    larger than the small default the core scheduler uses. Those callbacks can
    recurse deep into *C* — a TLS/SSH handshake runs an OpenSSL key exchange +
    cipher synchronously inside the callback — and C-level recursion (unlike
    Python recursion, whose frames live on the heap) burns the goroutine's
    real C stack, overflowing the small default into a clean guard-page fault.
    The 512 KB is virtual and pooled, so only the handful of pages a callback
    actually touches cost any RSS; only the bridge pays it — the core M:N paths
    keep the small default.
