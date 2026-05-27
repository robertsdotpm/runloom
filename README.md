# pygo

Go-style stackful coroutines for Python.  Inline asm context switch +
C scheduler + epoll/kqueue netpoll + socket monkey-patch.

```python
# Phase D (working today): write blocking code, get goroutine concurrency.
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
    listener = socket.socket(); listener.bind(("127.0.0.1", 9000)); listener.listen(128)
    while True:
        conn, addr = listener.accept()
        pygo_core.go(lambda c=conn,a=addr: handle(c, a))

pygo_core.go(accept_loop)
pygo_core.run()
```

No `async`.  No `await`.  Just `pygo_core.go(fn)` and blocking-style
socket calls.

## Phase status

|        | what                              | state |
| ------ | --------------------------------- | ----- |
| **A**  | Inline asm context switch + C scheduler + tstate recursion-counter snapshot | **done** |
| **B**  | Per-coroutine cframe + exc_info snapshot                                    | **partial** — recursion counters done; cframe / frame chain not. ~200 concurrent yielded goroutines is the soft cap. |
| **C**  | Free-threaded Python 3.13t + M:N work-stealing scheduler                    | future turn |
| **D**  | netpoll (epoll/kqueue/select) + socket monkey-patch (transparent blocking I/O) | **done** |

## Performance (Phase A + D, no Phase B fix yet)

Goroutine yield throughput:

| coros × yields | pygo (C sched + asm) | asyncio | speedup |
| ---: | ---: | ---: | ---: |
| 10 × 100  | **5.82 M/s** | 370 K/s | **15.7×** |
| 50 × 100  | **5.93 M/s** | 516 K/s | **11.5×** |
| 100 × 100 | **4.84 M/s** | 542 K/s | **8.9×** |

Real network workload (pygo TCP echo server, plain CPython client):

```
100 sequential round-trips: 11.6 ms total, 8593 req/s, 116 µs/RT.
```

Per-yield latency:
- Go's `runtime.Gosched()`: ~50 ns
- **pygo today**: ~200 ns
- asyncio: ~1800 ns
- pygo v0 (before Phase A): ~14 000 ns

## What works

- **Stackful coroutines** via hand-rolled x86_64 SystemV asm
  (`src/pygo_core/arch/swap_x86_64.S`).  ~80 ns per switch.
- **C scheduler**: ring-buffer ready queue, min-heap sleep timer,
  netpoll integration.  Single C call from Python to yield.
- **netpoll** with epoll (Linux), kqueue (BSD/macOS), select fallback
  (`src/pygo_core/netpoll.c`).
- **Socket monkey-patch** in `src/pygo/monkey.py`: replaces
  `socket.recv / send / sendall / accept / connect / recvfrom / sendto`
  with versions that park the goroutine on the fd transparently.

## What's still broken (Phase B)

The CPython frame chain (`tstate->cframe->current_frame` on 3.12,
moved/renamed in 3.13+) is **shared across all goroutines**.  When
goroutine N pushes a Python frame, it links backwards into goroutine
N-1's chain.  When N-1 finishes, dangling links remain.  Hundreds of
suspended-but-still-alive goroutines cause traceback walkers /
recursion checks to traverse arbitrarily deep chains and the C stack
overflows.

Greenlet handles this with ~200 lines of version-specific CPython C
that snapshots and restores cframe-derived state per greenlet on every
switch.  We've done the recursion counters (`py_recursion_remaining` /
`c_recursion_remaining`) but not the cframe pointer.

**Symptoms today**:
- Up to ~150 concurrent goroutines with many yields: works.
- ~200 concurrent goroutines yielding: crashes.
- TCP echo server with plain external clients: works (each handler is a
  short-lived goroutine that completes before others stack up).
- TCP echo client with many parallel goroutines: crashes early because
  monkey-patched socket adds more Python frames per RT.

## Layout

```
src/pygo_core/
  arch/swap_x86_64.S    inline asm context switch
  plat.h                OS/arch/compiler detection
  compat.h              stdint/stdbool shims for old MSVC
  coro.{h,c}            stackful coro primitive (asm/fibers/ucontext)
  fcontext.{h,c}        asm trampoline glue
  pygo_sched.{h,c}      C scheduler (ring queue, sleep heap)
  netpoll.{h,c}         epoll/kqueue/select backend
  module.c              Python type + module init
src/pygo/
  monkey.py             socket monkey-patch
  runtime.py            legacy Python scheduler (kept for testing)
tests/run_tests.py      plain-script test driver
examples/
  bench_c_scheduler.py  pygo vs asyncio yields/s
  echo_server.py        TCP echo demo
  echo_client.py        parallel-goroutine client
```

## Building

```bash
pip install -e .
```

C99 (`-std=gnu99` for `cpu_set_t` visibility), `-D_GNU_SOURCE`,
no build-isolation requirements.  Compiler matrix: GCC 3+, Clang 3+,
MSVC 2008+ (shims in `compat.h`), ICC, MinGW.

Asm fast path activates automatically on x86_64 + Linux/macOS/BSD.
Other archs fall back to ucontext (POSIX) or Fibers (Windows).

## Roadmap

- ~~Phase A — asm switch + C scheduler + recursion-counter swap~~  ✓
- ~~Phase D — netpoll + socket monkey-patch~~  ✓
- **Phase B (next)** — cframe / frame chain swap per goroutine.
  Fixes the ~200-concurrent-yielded-goroutine cliff.  Version-specific
  CPython C.  ~200 LoC.
- Phase C — free-threaded Python 3.13t, M:N scheduler, work-stealing.
  Real multi-core parallelism for pure-Python workloads.
- Phase E — aarch64 / arm / riscv asm context switches.
  Today's asm fast path is x86_64 SystemV only; everything else falls
  through to ucontext (still works, just ~20× slower per switch).
- Phase F — Windows IOCP integration (currently select() fallback).
