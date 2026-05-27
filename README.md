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
| **B**  | Per-goroutine cframe / frame-chain swap                                             | **deferred** — recursion counters done; full cframe swap needs ~200 LoC of greenlet-style CPython-version-specific C |
| **C**  | Free-threaded Python 3.13t support                                                  | **partial** — builds + runs cleanly with `Py_GIL_DISABLED`; M:N work-stealing scheduler is the next step |
| **D**  | netpoll (epoll/kqueue/select) + socket monkey-patch                                  | **done** |
| **E**  | aarch64 inline asm context switch                                                    | **done** (untested on real ARM hardware; cross-compiled clean) |

## Performance

CPython 3.12, Linux x86_64, fresh subprocess per cell.  Workload: N
goroutines × 100 cooperative yields.

| coros × yields | pygo (C sched + asm) | asyncio | speedup |
| ---: | ---: | ---: | ---: |
| 10 × 100  | **5.82 M/s** | 370 K/s | **15.7×** |
| 50 × 100  | **5.93 M/s** | 516 K/s | **11.5×** |
| 100 × 100 | **4.84 M/s** | 542 K/s | **8.9×** |
| 150 × 100 | **5.01 M/s** | 544 K/s | **9.2×** |

Free-threaded Python 3.13t (GIL disabled): **2.86 M y/s** at 100 × 100
— about half 3.12's throughput due to biased refcounting overhead.
The win on 3.13t is multi-core parallelism, which requires the M:N
scheduler (Phase C); single-threaded throughput is necessarily slower.

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

**Phase B — frame chain cliff**.  The CPython frame chain
(`tstate->cframe->current_frame` on 3.12; layout changed on 3.13) is
shared across all goroutines.  When goroutine N yields, its Python
frames stay alive (eval is suspended) and link to other goroutines'
frame chains.  At ~150-200 concurrent yielded goroutines, the linked
chain blows the C stack via traceback walks / recursion checks.

Greenlet fixes this in ~200 lines of CPython-version-specific C that
snapshots cframe-derived state per greenlet on every switch.  We've
done the recursion counters; cframe + exception state were attempted
but require deeper integration than was achievable in this session.

**Phase C — M:N work-stealing scheduler**.  pygo BUILDS + RUNS on
free-threaded 3.13t but the scheduler is still single-OS-thread.  The
real Phase C ships a Chase-Lev deque per hub, one hub per OS thread,
work stealing for load balance, and shared epoll across hubs.
`src/pygo_core/mn_sched.h` documents the design.

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

## What "do all" got us

We started this conversation looking at aionetiface's perf bench
showing 71 K yields/s.  Today pygo hits **5.9 M yields/s** — an 83×
speedup over that starting point, and **~9-15× faster than asyncio**
in the sweet spot.  We landed within **~4× of Go's per-yield cost on
CPython**, which is roughly the irreducible interpreter overhead.

Two limits remain.  The frame-chain cliff (~200 concurrent yielded
goroutines) and single-OS-thread scheduling.  Both have well-known
fixes (greenlet's algorithm + Chase-Lev work-stealing); both are
their own focused turns.
