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

```bash
pip install -e .
```

On free-threaded Python 3.13t:

```bash
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .
```

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
| Windows x64 (MinGW-w64 / clang) | Fibers | WSAPoll + select | GCC builtins | code review |
| Windows 8.1 / 7 / XP / Vista | Fibers | select (XP/2003) or WSAPoll | _Interlocked\* shim | not testable: VS 2022 rejects Win < 10, Python > 3.12 needs Win 10+ |

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
`_Generic` (default in `/std:c11` mode; setup.py sets it).

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

```
src/pygo_core/
  arch/
    swap_x86_64.S       SysV x86_64 inline asm (~80 ns/switch)
    swap_aarch64.S      AAPCS64 inline asm (verified under qemu)
  plat.h                OS/arch/compiler detection
  plat_compat.h         mutex/thread/clock/sleep/cpu-count shim
  plat_atomic.h         __atomic_* shim for MSVC (no-op on GCC/Clang)
  compat.h              stdint/stdbool shims for old MSVC
  coro.{h,c}            stackful coro primitive (asm/fibers/ucontext)
  fcontext.{h,c}        asm trampoline + per-arch make_ctx
  pygo_sched.{h,c}      C scheduler + per-g PyThreadState snap/load
  netpoll.{h,c}         epoll/kqueue/WSAPoll/select backend (M:N-aware)
  mn_sched.{h,c}        M:N work-stealing scheduler (3.13t)
  cldeque.{h,c}         Chase-Lev work-stealing deque
  module.c              Python type + module init + free-thread declaration
src/pygo/
  monkey.py             stdlib monkey-patch (socket / time / select /
                        stdio / ssl / subprocess / threading / queue /
                        file / syscalls / dns) -- Windows-aware
  runtime.py            legacy Python scheduler (kept for tests)
tests/
  run_tests.py          unit tests
  test_arm64.{c,sh}     aarch64 cross-compile + qemu run
  test_monkey.py        monkey-patch behaviour + cross-OS shims
examples/
  bench_c_scheduler.py  pygo vs asyncio yields/s
  bench_snap.py         snap-path microbench (fast + slow path)
  bench_spawn.py        steady-state spawn cost
  bench_spawn_yield.py  raw spawn/yield throughput
  bench_concurrent_yield.py   N concurrent yielded coros stress
  bench_mn.py           M:N parallel sha256
  bench_mn_yield.py     M:N yield-in-hub
  bench_mn_sleep.py     M:N sleep-in-hub
  bench_mn_netpoll.py   M:N echo server across hubs
  bench_preempt.py      time-sliced preemption demo
  echo_server.py        TCP echo (Go-style demo)
  echo_client.py        parallel-goroutine client
```

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
