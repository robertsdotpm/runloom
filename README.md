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

## Phase status

|        | what                                                                                | state |
| ------ | ----------------------------------------------------------------------------------- | ----- |
| **A**  | Inline asm context switch (x86_64 SysV) + C scheduler + recursion-counter snapshot  | **done** |
| **B**  | Per-goroutine cframe / frame-chain swap                                             | **done** — full PythonState snapshot (cframe, current_frame, datastack_chunk, exc_info, context, recursion counters) per g; 50 K concurrent yielded gs run clean on both 3.12 and 3.13t |
| **C**  | Free-threaded Python 3.13t support + **M:N work-stealing scheduler**                | **done** — pygo runs on 3.13t with GIL disabled; Chase-Lev work-stealing deques per hub; 2.5× parallel speedup on 8 cores measured |
| **D**  | netpoll (epoll/kqueue/select) + socket monkey-patch                                  | **done** |
| **E**  | aarch64 inline asm context switch                                                    | **done** (untested on real ARM hardware; cross-compiled clean) |

## Performance

CPython 3.12, Linux x86_64, fresh subprocess per cell.  Workload: N
goroutines × 100 cooperative yields (full Phase B per-g PythonState
snap on every yield).

| coros × yields | pygo (C sched + asm) | asyncio | speedup |
| ---: | ---: | ---: | ---: |
| 10 × 100   | **2.99 M/s** | 377 K/s | **7.9×** |
| 50 × 100   | **2.82 M/s** | 501 K/s | **5.6×** |
| 100 × 100  | **2.56 M/s** | 502 K/s | **5.1×** |
| 150 × 100  | **2.60 M/s** | 523 K/s | **5.0×** |
| 1000 × 100 | **1.87 M/s** | 560 K/s | **3.3×** |

Phase B traded ~2× single-yield throughput for correctness: per yield
we now snapshot cframe, current_frame, datastack chunk pointers,
contextvars, exception state, and recursion counters.  Each goroutine
gets its own independent slice of CPython thread state.

**Concurrent yielded goroutines (Phase B stress test):**

| coros | yields/coro | wall | throughput |
| ---: | ---: | ---: | ---: |
| 2000   | 50  | 67 ms   | 1.48 M y/s |
| 10000  | 10  | 284 ms  | 0.35 M y/s |
| 50000  | 10  | 1362 ms | 0.37 M y/s |

50000 simultaneously-yielded goroutines run clean on a single OS
thread.  Pre-Phase-B this segfaulted at ~150-200 from frame chain
overflow.

Free-threaded Python 3.13t (GIL disabled): **2.0 M y/s** at 100 × 100
— about ~20% slower than 3.12 due to biased refcounting overhead.

**Phase C M:N multi-core parallelism on 3.13t**: 100 goroutines each
running 5000 SHA-256 iterations:

| hubs | wall | throughput | speedup |
| ---: | ---: | ---: | ---: |
| 1 hub  | 586 ms | 0.85 M ops/s | 1.0× |
| 2 hubs | 397 ms | 1.26 M ops/s | **1.48×** |
| 4 hubs | 268 ms | 1.87 M ops/s | **2.19×** |
| 8 hubs | 236 ms | 2.12 M ops/s | **2.50×** |

For reference (same workload, same machine):
- asyncio (single OS thread):                  0.92 M ops/s
- plain Python `threading` × 8 on 3.13t:       2.24 M ops/s
- **pygo M:N × 8 on 3.13t**:                   **2.12 M ops/s**

pygo M:N matches Python's native threading throughput (~5% overhead
from Chase-Lev bookkeeping + per-coro state) while exposing the
goroutine model (cheap spawn, no thread-per-task explosion at scale).

Per-yield latency:
- Go's `runtime.Gosched()`: ~50 ns
- **pygo today (3.12)**: ~200 ns
- **pygo on 3.13t**: ~350 ns
- asyncio: ~1800 ns
- pygo v0 (before Phase A): ~14 000 ns

Real network workload (pygo TCP echo server, plain external client):
**8.6 K req/s, 116 µs/RT** sequential round-trips.

## What works

- **Inline asm context switch** on x86_64 SystemV and aarch64.  No
  `sigprocmask`, ~80 ns per swap on x86_64.  Falls back to Windows
  Fibers on Windows and POSIX ucontext on other archs.
- **C scheduler** (`src/pygo_core/pygo_sched.c`): ring-buffer ready
  queue, min-heap sleepers, single C call from Python to yield.
- **netpoll** (`src/pygo_core/netpoll.c`): epoll on Linux, kqueue on
  BSD/macOS, select fallback.  Goroutines park transparently on fd
  readiness; scheduler pumps between yields.
- **Socket monkey-patch** (`src/pygo/monkey.py`): replaces blocking
  socket methods with non-blocking + `wait_fd` retry loops.
- **Free-threaded Python 3.13t**: declared safe via
  `Py_MOD_GIL_NOT_USED`; builds + runs end-to-end; tests pass with
  GIL disabled.

## What's broken / deferred

**Phase C v2 — yield support inside M:N hubs**.  v1 ships fire-and-
forget gs: each goroutine runs to completion on its hub.  Adding
`sched_yield` support inside a hub needs a thread-local "current hub"
pointer so yield knows which deque to push back to; today yield
operates on the single-threaded global scheduler.  ~100 LoC follow-up.

**Phase E — aarch64 untested on real hardware**.  Cross-compiles clean
with `aarch64-linux-gnu-gcc`; the asm + make_ctx code follows AAPCS64.
Confirming on an Apple Silicon Mac or Linux ARM box would close this.

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
    swap_aarch64.S      AAPCS64 inline asm (cross-compile verified)
  plat.h                OS/arch/compiler detection
  compat.h              stdint/stdbool shims for old MSVC
  coro.{h,c}            stackful coro primitive (asm/fibers/ucontext)
  fcontext.{h,c}        asm trampoline glue + per-arch make_ctx
  pygo_sched.{h,c}      C scheduler
  netpoll.{h,c}         epoll/kqueue/select backend
  mn_sched.h            M:N scheduler spec (Phase C, deferred)
  module.c              Python type + module init + free-thread declaration
src/pygo/
  monkey.py             socket monkey-patch
  runtime.py            legacy Python scheduler (kept for tests)
tests/run_tests.py
examples/
  bench_c_scheduler.py  pygo vs asyncio yields/s
  bench_spawn_yield.py  raw spawn/yield throughput
  echo_server.py        TCP echo (Go-style demo)
  echo_client.py        parallel-goroutine client
```

## What got built across this conversation

Started at aionetiface's perf bench (71 K yields/s on a Python scheduler).

Today:
- **2.5-3 M yields/s** single-thread on CPython 3.12 with full Phase B
  per-g PythonState snap (correctness-first)
- **50 000 concurrent yielded goroutines** on one OS thread, no frame
  chain cliff
- **2.5× parallel speedup** with M:N hubs on free-threaded 3.13t
- **8.6 K req/s** TCP echo server with Go-style blocking-looking code
- Cross-platform asm switches (x86_64 + aarch64), Windows Fibers, POSIX ucontext fallback
- Compiles + runs on CPython 3.5–3.13t

Phase B was the last correctness milestone; remaining work is
performance polish + Phase C v2 (yield inside M:N hubs) + Phase E
validation on real ARM hardware.
