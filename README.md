# pygo

Go-style stackful coroutines for Python, via a portable C99 extension
+ hand-rolled inline assembly context switch + C scheduler.

```python
import pygo_core

def worker():
    for i in range(3):
        print("step", i)
        pygo_core.sched_yield()

pygo_core.go(worker)
pygo_core.go(worker)
pygo_core.go(worker)
pygo_core.run()
```

No `async`.  No `await`.  Just `go(fn)` and `sched_yield()`.

## Performance

Workload: N goroutines × 100 cooperative yields, fresh subprocess per cell.

| coros | total yields | pygo (C sched + asm) | pygo (Python sched) | asyncio | pygo vs asyncio |
| ----: | -----------: | -------------------: | ------------------: | ------: | --------------: |
|   10  |       1 000  |  **5.82 M/s**        |        1.63 M/s     |  370 K/s | **15.7×**       |
|   50  |       5 000  |  **5.93 M/s**        |        91  K/s      |  516 K/s | **11.5×**       |
|  100  |      10 000  |  **4.84 M/s**        |        90  K/s      |  542 K/s | **8.9×**        |
|  150  |      15 000  |  **5.01 M/s**        |        90  K/s      |  544 K/s | **9.2×**        |
|  300+ |              |   crash (Phase B)    |        87  K/s      |  527 K/s |                 |

Linux x86_64, CPython 3.12.  Numbers are yields per second across N
concurrent goroutines.  Higher is better.

Per-yield latency:
- **pygo (C sched + asm)**: ~200 ns
- raw asm switch alone: ~80 ns
- asyncio: ~1800 ns
- pygo (Python sched): ~11 µs
- Go's `runtime.Gosched()`: ~50 ns

We are now within ~4× of Go's per-yield cost on CPython.  The remaining
gap is interpreter-shaped, not scheduler-shaped.

## What landed in Phase A

1. **Hand-rolled inline asm for x86_64 SystemV** (`src/pygo_core/arch/swap_x86_64.S`).
   30 instructions, no `sigprocmask` syscalls, no signal-mask save/restore.
   Replaces ucontext's `swapcontext` and is ~11× faster on the raw
   switch alone.

2. **C-level scheduler** (`src/pygo_core/pygo_sched.c`).  Ring queue of
   `pygo_g_t` structs, min-heap for sleepers, single C call from Python
   to yield.  Replaces the Python `pygo.runtime.Scheduler` for the
   `pygo_core.go / sched_yield / sched_sleep / run` fast path.

3. **Per-coroutine PyThreadState recursion-counter snapshot**.  CPython
   tracks `py_recursion_remaining` and `c_recursion_remaining` in
   thread-state; without per-coro save/restore they leak across our
   stack switch and a long run() hits `RecursionError`.  Snapshotting
   into each `pygo_g_t` makes recursion budget per-goroutine.

4. **Stack pool**.  Coroutine stacks are mmap'd once and recycled --
   never `munmap`'d -- because libc's `swapcontext` keeps internal
   bookkeeping tied to past stack memory and would corrupt fresh
   allocations on adjacent pages.

5. **Refcounted `pygo_g_t`**.  Two parties hold refs: the scheduler
   (while in ready/sleep queue) and the `PygoG` Python wrapper.  Frees
   when both drop -- so `pygo_core.go(fn)` without saving the handle
   doesn't use-after-free.

## What's broken / deferred to Phase B

- **300+ goroutines × many yields crash the C scheduler.**  Root cause
  is the CPython frame chain (`tstate->cframe->current_frame` in 3.12)
  being shared across all goroutines.  Each `go(worker)` pushes a
  Python frame; goroutines link backwards across other goroutines'
  frames; when a goroutine finishes its frames are reaped but other
  goroutines still link to them.
  - **Fix**: snapshot `tstate->cframe->current_frame` per g on yield,
    restore on resume.  Greenlet does this in ~200 lines of
    version-specific C.  Phase B.

- **Netpoll**: `sched_sleep()` blocks the OS thread instead of
  parking on an fd via epoll/kqueue/IOCP.

- **Socket monkey-patch**: the "transparent blocking I/O" pitch is not
  yet wired -- raw socket calls still block.

- **M:N scheduling**: still single OS thread.  Phase C is free-threaded
  Python with one scheduler per OS thread + work-stealing.

## Building

```bash
pip install -e .
```

C99 (`-std=gnu99` for `cpu_set_t` visibility), `-D_GNU_SOURCE`, no
build-isolation requirements.  Supports GCC 3+, Clang 3+, MSVC 2008+
(via shims in `compat.h`), ICC, MinGW.

Asm fast path activates automatically when the host is x86_64 +
Linux/macOS/BSD.  Other archs fall back to `ucontext` (POSIX) or
`Fibers` (Windows).

## Layout

```
src/pygo_core/
  arch/swap_x86_64.S   inline asm context switch
  plat.h               OS/arch/compiler detection
  compat.h             stdint/stdbool shims for old MSVC
  coro.h / coro.c      portable coro primitive (asm / fibers / ucontext)
  fcontext.h / .c      asm-path glue (trampoline, make_ctx)
  pygo_sched.h / .c    C scheduler (ring queue, sleep heap)
  module.c             Python type + module init
src/pygo/              Python runtime (legacy path; thin wrappers)
tests/run_tests.py     plain-script test driver
examples/              benchmarks vs asyncio
```

## Roadmap

- Phase A — **DONE**: asm switch + C scheduler + tstate counter swap
- Phase B — **next**: per-g frame chain swap, version-specific cframe handling
- Phase C — free-threaded Python 3.13t, M:N scheduler, work-stealing
- Phase D — netpoll, socket monkey-patch ("Go feel")
