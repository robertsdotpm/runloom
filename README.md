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

C99 (`-std=gnu99 -D_GNU_SOURCE`), no build-isolation needed.  Compiler
matrix: GCC 3+, Clang 3+, MSVC 2008+ (with shims), ICC, MinGW.

On free-threaded Python 3.13t:

```bash
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .
```

## Layout

```
src/pygo_core/
  arch/
    swap_x86_64.S       SysV x86_64 inline asm (~80 ns/switch)
    swap_aarch64.S      AAPCS64 inline asm (verified under qemu)
  plat.h                OS/arch/compiler detection
  compat.h              stdint/stdbool shims for old MSVC
  coro.{h,c}            stackful coro primitive (asm/fibers/ucontext)
  fcontext.{h,c}        asm trampoline + per-arch make_ctx
  pygo_sched.{h,c}      C scheduler + per-g PyThreadState snap/load
  netpoll.{h,c}         epoll/kqueue/select backend (M:N-aware)
  mn_sched.{h,c}        M:N work-stealing scheduler (3.13t)
  cldeque.{h,c}         Chase-Lev work-stealing deque
  module.c              Python type + module init + free-thread declaration
src/pygo/
  monkey.py             socket monkey-patch
  runtime.py            legacy Python scheduler (kept for tests)
tests/
  run_tests.py          unit tests
  test_arm64.{c,sh}     aarch64 cross-compile + qemu run
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

- **aarch64 not validated on real hardware.**  Cross-compiles clean
  with `aarch64-linux-gnu-gcc`, asm + `make_ctx` follow AAPCS64, and
  `tests/test_arm64.sh` runs the full coro + yield + trampoline path
  under `qemu-aarch64-static`.  An Apple Silicon Mac or Linux ARM
  box would close it.
- **Preemption is 3.13t only.**  GIL builds raise `RuntimeError` on
  `preempt_init`.
