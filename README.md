# pygo

Go-style stackful coroutines for Python.  Hand-rolled asm context
switch + C scheduler + netpoll + socket monkey-patch.  Runs on
free-threaded Python 3.13t with the GIL disabled.

```python
import socket, sys; sys.path.insert(0, "src")
import pygo, pygo.monkey, pygo_core
pygo.monkey.patch()

def handle(conn, addr):
    while True:
        data = conn.recv(4096)
        if not data: break
        conn.sendall(data)
    conn.close()

def accept_loop():
    s = socket.socket(); s.bind(("127.0.0.1", 9000)); s.listen(128)
    while True:
        conn, addr = s.accept()
        pygo_core.go(lambda c=conn,a=addr: handle(c, a))

pygo_core.go(accept_loop)
pygo_core.run()
```

No `async`.  No `await`.  Just `go(fn)` and blocking-style I/O.

Already have `async def` code?  Run it on pygo's scheduler with
`pygo.aio`:

```python
import asyncio
import pygo.aio as paio

async def handler(reader, writer):
    line = await reader.readline()
    writer.write(b"echo: " + line)
    await writer.drain()
    writer.close()

async def main():
    server = await paio.start_server(handler, "127.0.0.1", 9000)
    async with server:
        await server.serve_forever()

paio.run(main())          # ~equivalent to asyncio.run, on pygo
```

See [Documentation](#documentation) below for the full guide.

## Features

- **Hand-rolled asm context switch** on x86_64 SystemV and aarch64.
  No `sigprocmask` — about 80 ns per swap on x86_64.  Falls back to
  Windows Fibers on Windows and POSIX `ucontext` elsewhere.
- **Full per-goroutine `PyThreadState` snapshot** (cframe /
  current_frame, datastack chunks, exc_info, contextvars, recursion
  counters).  Each goroutine gets an isolated slice of CPython thread
  state, so 50 000 simultaneously-yielded goroutines run clean on one
  OS thread.  No frame-chain cliff.
- **C scheduler** with a ring-buffer ready queue and min-heap sleepers.
  Single C call from Python to yield.
- **netpoll** — epoll on Linux, kqueue on BSD/macOS, `select`
  fallback.  Goroutines park transparently on fd readiness.
- **Socket monkey-patch** replaces blocking socket methods with
  non-blocking + `wait_fd` retry loops.
- **Free-threaded Python 3.13t** support, declared safe via
  `Py_MOD_GIL_NOT_USED`.  Runs with the GIL disabled.
- **M:N scheduler** for 3.13t.  N OS threads, one Chase-Lev
  work-stealing deque per hub, per-hub MPSC submission list so
  external producers don't race the deque owner.  Goroutines are
  routed back to the originating hub on yield, sleep, or I/O wake.
- **Time-sliced preemption** (3.13t) via `Py_AddPendingCall` +
  `eval_breaker`.  Goroutines without explicit `sched_yield` calls
  cooperate automatically; zero hot-path overhead.
- **Sustained spawn rate**: ~1.7M scheduler-only spawn-and-runs
  per second at 100-deep peak concurrency on Linux 3.12, single
  thread.  At 1 µs of actual work per request (≈ a real handler),
  pygo sustains ~280 K req/s on one OS thread -- ~3× the 100 K req/s
  target.  M:N on 3.13t scales this further with 8 hubs.
- **Go-style channels** via `pygo_core.Chan(capacity=0)` --
  send / recv / try_send / try_recv / close, blocking + buffered,
  Go's `v, ok := <-ch` returned as a tuple.  Unbuffered ping-pong
  costs ~560 ns/round-trip on Linux 3.12 -- within 7% of Go 1.22
  `BenchmarkPingPong` on the same hardware.

## Performance

CPython 3.12, Linux x86_64.  Workload: N goroutines × 100 cooperative
yields each (full per-g `PyThreadState` snap on every yield).

| coros × yields | pygo | asyncio | speedup |
| ---: | ---: | ---: | ---: |
| 10 × 100   | **3.14 M/s** | 392 K/s | **8.0×** |
| 50 × 100   | **3.00 M/s** | 496 K/s | **6.0×** |
| 100 × 100  | **2.76 M/s** | 514 K/s | **5.4×** |
| 1000 × 100 | **2.42 M/s** | 534 K/s | **4.5×** |

**Per-yield latency** (single-coro tight loop, snap path isolated):

| path | 3.12 | 3.13t |
| ---: | ---: | ---: |
| Fast path — nobody else ready, snap skipped (Go's `runtime.Gosched`) | **47 ns**  | **75 ns** |
| Slow path — 2 coros ping-pong, full snap + asm yield + load         | **182 ns** | **228 ns** |

For comparison: Go's `runtime.Gosched()` is ~50 ns, asyncio ~1800 ns.
pygo's fast path on 3.12 matches Go to within 1 ns.  The 3.13t gap is
free-threading's interpreter overhead, not pygo's.

`sched_yield` is a vectorcall singleton, so caching the bound name in
a local — `y = pygo_core.sched_yield; y()` — is meaningfully faster
than the module-attribute form on 3.13t, where `LOAD_ATTR` costs ~100
ns/call on its own.

**Concurrent yielded goroutines:**

| coros | yields/coro | wall | throughput |
| ---: | ---: | ---: | ---: |
| 2 000  | 50  | 67 ms   | 1.48 M y/s |
| 10 000 | 10  | 284 ms  | 0.35 M y/s |
| 50 000 | 10  | 1362 ms | 0.37 M y/s |

**M:N multi-core (3.13t, GIL disabled, 100 goroutines × 5 000 SHA-256
iterations):**

| hubs | wall | throughput | speedup |
| ---: | ---: | ---: | ---: |
| 1 hub  | 586 ms | 0.85 M ops/s | 1.0×  |
| 2 hubs | 397 ms | 1.26 M ops/s | 1.48× |
| 4 hubs | 268 ms | 1.87 M ops/s | 2.19× |
| 8 hubs | 236 ms | 2.12 M ops/s | **2.50×** |

For reference: plain Python `threading` × 8 on 3.13t hits 2.24 M ops/s.
pygo matches that within ~5% (Chase-Lev + per-coro state overhead)
while exposing the goroutine model (cheap spawn, no thread-per-task
explosion).

**TCP echo server** (external client, sequential round-trips):
**8.6 K req/s, 116 µs/RT**.

## Stack sizing & memory

Each goroutine owns a private C stack.  pygo manages stack size with
three mechanisms working together:

1. **Auto-calibration** — every fresh stack is painted with a sentinel;
   on completion pygo scans the high-water mark.  After 1 000
   completions the scheduler-wide default is locked to
   `next_pow2(max_hwm × 4)`, clamped to `[16 KB, 8 MB]`, and painting
   shuts off.  Typical pure-Python workloads converge to **16 KB**;
   C-recursion-heavy workloads (`json.dumps` of deeply nested objects)
   converge to **64 KB**.

2. **`MADV_DONTNEED` on pool release** — when a finished goroutine's
   stack returns to the pool, all but the first page are released back
   to the kernel.  A burst of 5 000 goroutines × 1 MB stacks lands at
   **~21 MB resident**, not 5 GB.  Pages refault on reuse — no
   correctness impact.

3. **Per-call override** for the outlier goroutine:

```python
import pygo_core

# Lock in a known-good size before any spawn (skips calibration):
pygo_core.set_stack_size(64 * 1024)

# Single goroutine that's known to recurse deeply:
pygo_core.go(deep_handler, stack_size=512 * 1024)

# Inspect what calibration measured:
print(pygo_core.stats())
# {'stack_size_default': 16384, 'stack_hwm': 768,
#  'stack_calibrated': 1, 'stack_painting': 0, ...}
```

See [the stack sizing guide](docs/stack-sizing.md) for the full
mechanism, including the safety margin and when to override.

## Memory per coroutine — and how it compares to Go

pygo figures below are **measured** (the floor-decompose bench, default
scheduler); the Go figures are the standard reference value for an empty
goroutine plus an approximate add for a live connection's machinery.

| state | Go goroutine | pygo (Python coroutine) |
| --- | ---: | ---: |
| empty / just spawned | ~2.5 KB | n/a — running Python needs a datastack chunk |
| parked, holding a socket + read buffer | ~10–13 KB | **~26 KB** (~38 KB while active) |

pygo's own per-coroutine structs are **< 0.3 KB** — lighter than Go's
`g`.  The gap above is the **CPython object tax**: a Python `socket`,
`bytes` buffers, and frame objects all carry PyObject headers and are
simply fatter than Go structs and `[]byte`.  Run the *identical* handler
in C (`pygo_mn_go_c`, no Python frames) and pygo drops to ~7–8 KB/conn —
right next to Go.

Two caveats that matter more than the table:

- **"2 KB" is the *empty* goroutine.**  Give it a socket, a buffer, and
  a grown stack and Go is ~10 KB+ too — the famous gap is mostly
  empty-vs-loaded, not runtime-vs-runtime.
- **Kernel socket buffers (~8 KB+/socket, autotuned) are
  runtime-independent** and often dominate at scale.  So a million live
  connections is never cheap in *any* runtime — order ~15–25 GB in Go,
  ~30 GB+ for pygo-in-Python, ~10–15 GB for pygo-in-C.  The goroutine
  *model* (vs 1 MB-stack OS threads, where a million threads is simply
  impossible) is what makes it feasible at all — not any runtime making
  connections free.

## Kernel limits for high goroutine counts

Each goroutine owns a private mmap'd C stack, and with the guard page
that copy-grow installs that is **~2 virtual memory areas (VMAs) per
goroutine**.  The Linux defaults cap a process at ~1 M VMAs and ~1 M
open files, so past roughly **half a million live goroutines** the
*kernel* — not pygo — starts refusing `mmap`/`socket` with `ENOMEM`
("Cannot allocate memory").  The tell-tale is a spawn failure at a few
GB resident with most of RAM still free: it is a VMA/fd cap, not real
memory pressure.

Raise these before a high-N run:

| sysctl / limit | typical default | high-N value | why |
| --- | --- | --- | --- |
| `vm.max_map_count` | 65530–1048576 | `16777216` | ~2 VMAs/goroutine — the binding ceiling (N=2 M needs ~7 M maps) |
| `fs.nr_open` | 1048576 | `8388608` | raises the per-process open-fd hard ceiling for socket servers |
| `net.core.somaxconn` | 128–4096 | `1048576` | accept-queue depth for large connection bursts |
| `net.ipv4.tcp_max_syn_backlog` | 1024–4096 | `1048576` | SYN-queue depth, same reason |

```bash
# one-off (these reset on reboot):
sudo sysctl -w vm.max_map_count=16777216 fs.nr_open=8388608 \
     net.core.somaxconn=1048576 net.ipv4.tcp_max_syn_backlog=1048576

# persistent — re-applied at boot by systemd-sysctl:
sudo tee /etc/sysctl.d/99-pygo.conf >/dev/null <<'EOF'
vm.max_map_count = 16777216
fs.nr_open = 8388608
net.core.somaxconn = 1048576
net.ipv4.tcp_max_syn_backlog = 1048576
EOF
sudo sysctl --system
```

A socket server also needs its **open-file soft limit** lifted in the
process itself — `fs.nr_open` only raises the ceiling: `ulimit -n
8388608`, or `setrlimit(RLIMIT_NOFILE, …)` at startup.  `vm.max_map_count`
is the one that bites first and most silently.

With these in place, **2,000,000 concurrent connections** (~4 M
goroutines, both ends in-process) complete in a single process at
~14 GB RSS; at the defaults the same run dies spawning at ~500 K
goroutines.  macOS/BSD have analogous knobs (`kern.maxfiles`,
`kern.ipc.somaxconn`); there is no `max_map_count` equivalent, the
per-process fd limit is the practical ceiling there.

## Time-sliced preemption (3.13t)

A goroutine that never calls `sched_yield()` can still cooperate
through quantum-driven preemption:

```python
import pygo_core
pygo_core.preempt_init(quantum_us=10_000)   # 10 ms slices

def cpu_bound():
    total = 0
    for i in range(10_000_000):
        total += i * i

pygo_core.go(cpu_bound)
pygo_core.go(other_goroutine)
pygo_core.run()
```

A timer thread posts a `Py_AddPendingCall` every quantum; CPython's
`eval_breaker` check (already done between bytecodes) invokes our
callback, which calls `pygo_sched_yield()` on the running goroutine.

Cost: ~0 ns on the hot path.  Per quantum ~300 ns dispatch + yield —
at 100 Hz that's 30 µs/sec ≈ 0.003% overhead.

Caveats: preemption only fires at Python bytecode boundaries.  A
goroutine sitting inside a long C call (`numpy`, `hashlib`, etc.) is
non-preemptible until it returns — same limitation Go has with cgo.
`preempt_init` is 3.13t only; GIL builds raise `RuntimeError`.

## Building

Plain `pip` works if you already have a C compiler:

```bash
pip install -e .
```

On free-threaded Python 3.13t:

```bash
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .
```

### No compiler? Use the bootstrap helpers

For one-shot installs on a fresh box, the `scripts/` directory has
detect-and-install wrappers:

```bash
# POSIX (Linux, macOS, BSDs, Solaris, Haiku)
./scripts/install.sh                # detects distro, installs gcc/clang
./scripts/install.sh --editable     # passes -e through to pip

# Windows -- works from cmd or PowerShell
scripts\install.bat                 # auto-detects MSVC, falls back to MinGW
scripts\install.bat --editable
```

The orchestrator scripts check for a compiler on PATH and run
`bootstrap_compiler.{sh,ps1,bat}` if missing.  The bootstrap installer
knows: `apt-get`, `dnf/yum`, `pacman`, `zypper`, `apk`, `xbps`,
`pkg(BSD)`, `pkgin`, `pkgman(Haiku)`, the macOS Command Line Tools,
the MSVC Build Tools (via `vs_BuildTools.exe`), and MinGW-w64 (via
the WinLibs zip).

### Build-time environment knobs

| Variable | Effect |
| --- | --- |
| `PYGO_BACKEND=ucontext` | force the ucontext stack-swap backend even on x86_64/aarch64 |
| `PYGO_NO_ASM=1` | drop the `.S` source from the build (same effect as above) |
| `PYGO_NO_IOCP=1` | omit the Windows IOCP-AFD backend (falls back to WSAPoll/select) |
| `PYGO_DEBUG=1` | `-O0 -g` (POSIX) or `/Od /Zi` (MSVC) |
| `PYGO_EXTRA_CFLAGS` | appended to the compile command line |
| `PYGO_EXTRA_LDFLAGS` | appended to the link command line |
| `CC` | usual setuptools override; controls compiler selection on Windows too |

### Prebuilt wheels

`pyproject.toml` ships a `[tool.cibuildwheel]` matrix covering CPython
3.11–3.14 on:

- Linux x86_64 + aarch64 (manylinux\_2\_28)
- macOS universal2 (arm64 + x86_64)
- Windows AMD64

Run `cibuildwheel --output-dir wheels` from a CI runner (or locally
with Docker) to populate `wheels/` for upload to PyPI.

## Platform support

| OS / arch | stack switch | netpoll | atomics | tested |
| --- | --- | --- | --- | --- |
| Linux x86_64 (Debian 13, Fedora 39) | fcontext-asm | epoll  | GCC builtins | yes (hw, 3.11 + 3.12 + 3.13t) |
| Linux aarch64       | fcontext-asm | epoll  | GCC builtins | qemu-aarch64 |
| macOS Big Sur x86_64 | fcontext-asm | kqueue | GCC builtins | yes (hw, 3.12) |
| macOS arm64 (Apple Silicon) | fcontext-asm | kqueue | GCC builtins | code review |
| FreeBSD 14.3 x86_64 | fcontext-asm | kqueue | GCC builtins | yes (hw, 3.12) |
| GhostBSD (FreeBSD 14.1 base) | fcontext-asm | kqueue | GCC builtins | yes (hw, 3.12) |
| OpenBSD / NetBSD / DragonFly | fcontext-asm | kqueue | GCC builtins | code review |
| Solaris / illumos   | ucontext     | select | GCC builtins | code review |
| Android (Termux)    | fcontext-asm | epoll  | GCC builtins | code review (phone offline at test time) |
| Windows 11 Pro x64 (MSVC 2022) | Fibers | WSAPoll | _Interlocked\* shim | yes (hw, 3.12) |
| Windows 10 22H2 x64 (MSVC 2022) | Fibers | WSAPoll + select | _Interlocked\* shim | yes (hw, 3.12) |
| Windows Server 2022 x64 (MSVC 2022) | Fibers | WSAPoll + select | _Interlocked\* shim | yes (hw, 3.12) |
| Windows 8.1 x64 (MinGW-w64 13.2.0 ucrt) | Fibers | select | GCC builtins | yes (hw, 3.12) |
| Windows x64 (clang-cl) | Fibers | WSAPoll + select | GCC builtins | code review |
| Windows 7 / XP / Vista | Fibers | select (XP/2003) | _Interlocked\* shim | code review only: Python 3.11+ won't launch on Win 7-, and pygo's frame snap requires Python 3.11 fields |

The Linux 3.12 + 3.13t numbers, macOS 11.7, FreeBSD 14.3, GhostBSD,
and Windows 11 rows above were validated end-to-end on real hardware:
the C extension compiles, `tests/run_tests.py` is 5/5 green, and
`tests/test_monkey.py` is 17/17 (14 on Windows -- the 3 POSIX-pipe-
specific tests skip cleanly).

**Windows backend selection** is at runtime: `WSAPoll` is probed via
`GetProcAddress` at first netpoll init, falling through to `select()`
on hosts where it's missing (XP / Server 2003).  One binary works
across Windows Vista through Windows 11 / Server 2022.

**Compilers**: GCC 4.7+, Clang 3.5+ (including clang-cl), MSVC 19.20+
(Visual Studio 2019 16.0+), MinGW-w64, ICC 17+.  MSVC needs C11
`_Generic` (default in `/std:c11` mode; setup.py sets it).  When
building on Windows with MinGW-w64 (the practical path for Win 8.1,
since VS 2022 refuses to install on Windows < 10), setup.py adds
`-static-libgcc -Wl,-Bstatic -lwinpthread` automatically -- the
resulting .pyd has zero non-system DLL dependencies and runs without
a MinGW install on the target host.

**Python**: 3.11 or newer.  The Phase B per-goroutine PyThreadState
snapshot relies on 3.11+ tstate fields (`cframe`, `datastack_chunk`,
`exc_state`); 3.12 split the recursion counter into
`py_recursion_remaining` + `c_recursion_remaining`, 3.11 had a single
`recursion_remaining`, and the snap handles both.  A compile-time
`#error` in module.c catches the wrong version with a clear message.
Free-threaded 3.13t is also supported (M:N work-stealing scheduler +
time-sliced preemption).  Pre-3.11 Python used a fundamentally
different frame model (PyFrameObject linked list) that isn't
covered.

## Layout

| Directory | What's in it |
| --- | --- |
| `src/pygo_core/` | The C extension: scheduler, channels, netpoll, asm context-switch backends, M:N hubs. |
| `src/pygo/` | Pure-Python layers: `aio` (asyncio bridge), `sync` (no-async-await facade), `monkey` (stdlib patches), `time` (Go-style Timer/Ticker), `runtime` (legacy Python scheduler kept for tests). |
| `tests/` | Unit tests, stress/chaos/concurrency/edge/workload suites, monkey-patch behaviour tests. |
| `examples/` | Microbenchmarks (`bench_*.py`) and small servers (`echo_server.py`, `echo_client.py`). |
| `docs/` | Full user documentation — see below. |
| `scripts/` | Bootstrap compiler + install helpers for fresh boxes. |

## Documentation

Full guide aimed at people building things on top of pygo lives in
[docs/](docs/) and is also published via Read the Docs.  Highlights:

- [Quickstart](docs/quickstart.md) — your first goroutine, channels, sleep
- [Asyncio bridge (`pygo.aio`)](docs/asyncio.md) — run `async def` code on the pygo scheduler
- [Sync API (`pygo.sync`)](docs/sync-api.md) — same scheduler, no `async`/`await`
- [Channels](docs/channels.md) — buffered, unbuffered, `select`, `for v in ch`
- [Stack sizing](docs/stack-sizing.md) — calibration, MADV_DONTNEED, per-call overrides
- [Monkey-patching the stdlib](docs/monkey-patching.md) — drop-in cooperative `socket`, `time`, `ssl`
- [Time-sliced preemption](docs/preemption.md) — 3.13t auto-yield
- [M:N parallelism](docs/parallelism.md) — work-stealing across OS threads
- [Cookbook](docs/cookbook.md) — worker pools, pipelines, fan-in/out, cancellation
- [API reference](docs/api-reference.md) — every public symbol

To build the docs locally: `pip install mkdocs mkdocs-material && mkdocs serve`.

## Known gaps

- **aarch64 on hardware**: cross-compiles clean with
  `aarch64-linux-gnu-gcc` and runs end-to-end under `qemu-aarch64-static`
  (see `tests/test_arm64.sh`); not yet validated on real Apple Silicon
  or Linux ARM hardware.
- **Windows on hardware**: the C extension was added with the runtime
  shim + WSAPoll backend; not yet validated on a Windows box.  All
  POSIX-isms are gated behind `PYGO_OS_WINDOWS` and the atomic / mutex /
  thread / clock / sleep primitives go through `plat_compat.h`.
- **Preemption is 3.13t only.**  GIL builds raise `RuntimeError` on
  `preempt_init`.
- **IOCP**: Windows uses WSAPoll (or select).  IOCP would be the more
  efficient choice for many-socket workloads but adds substantial
  code; deferred until WSAPoll proves a real bottleneck.
