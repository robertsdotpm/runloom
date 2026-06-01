# pygo

Go-style stackful coroutines for Python. Write **blocking** code -- `go(fn)`,
plain `recv`/`send`, no `async`/`await` -- and run a million of them across
every core in one process. Hand-rolled asm context switch + C work-stealing
scheduler + netpoll, built for **free-threaded Python 3.13t** (GIL off).

```python
import socket, sys; sys.path.insert(0, "src")
import pygo, pygo.monkey, pygo_core
pygo.monkey.patch()

def handle(conn, addr):
    while True:
        data = conn.recv(4096)
        if not data: break
        conn.sendall(data)          # looks blocking; parks the goroutine
    conn.close()

def accept_loop():
    s = socket.socket(); s.bind(("127.0.0.1", 9000)); s.listen(128)
    while True:
        conn, addr = s.accept()
        pygo_core.go(lambda c=conn, a=addr: handle(c, a))

pygo_core.go(accept_loop)
pygo_core.run()
```

Already have `async def` code? Run it unchanged on pygo's scheduler with the
`pygo.aio` bridge (`paio.run(main())` ≈ `asyncio.run`). See [docs/](docs/).

**The trade-off:** the bridge has more per-task overhead than asyncio's own
loop. If your tasks each do just one `await` and finish (lots of tiny tasks,
little work each), it's about **5× slower than plain asyncio**. If your tasks do
a lot of `await`s before finishing, pygo's fast context switch pays off and it's
**~1.7–1.9× faster**. So the bridge isn't a guaranteed speed-up -- think of it as
a way to run your existing async code on pygo's scheduler. The easy way to find
out is to just point the extension at your code with the bridge and see how it
does.

---

## How it compares -- the short answers

### 1. Memory vs Go

pygo's *own* per-coroutine bookkeeping is **< 0.3 KB** -- lighter than Go's `g`.
What you actually pay is the **CPython object tax**, not the scheduler:

| state | Go goroutine | pygo (Python handler) | pygo (C handler) |
| --- | ---: | ---: | ---: |
| empty / just spawned | ~2.5 KB | -- (running Python needs a datastack chunk) | -- |
| parked, holding a socket + read buffer | ~10–13 KB | **~26 KB** (~38 KB active) | **~7–8 KB** |

Straight answer: **in Python you will not beat Go on memory** -- a Python
`socket` + `bytes` buffers + frame objects all carry PyObject headers and are
simply fatter than Go structs and `[]byte`. Run the *same* handler in C
(`pygo_mn_go_c`, no Python frames) and you're at Go parity. At scale the
kernel's own socket buffers (~8 KB+/socket) dominate in **any** runtime, so a
million live connections is ~tens of GB everywhere. The goroutine *model* is
what makes a million feasible at all (vs ~1 MB-stack OS threads, where a
million is impossible) -- not any runtime making connections free.

### 2. Speed

All measured; figures label whether the handler is Python or C.

- **Scheduler / context switch** (Python, 3.12): fast-path yield **47 ns**
  (75 ns on 3.13t) -- matching Go's `runtime.Gosched()` (~50 ns) and **~25–40×
  faster than asyncio** (~1800 ns/step). Sustains **2.4–3.1 M context
  switches/s** vs asyncio's ~0.4–0.5 M/s (**~5–8×**).
- **Multi-core compute** (Python, 3.13t, 100 goroutines × SHA-256): scales
  **2.5× from 1→8 hubs to 2.12 M ops/s**, ≈ `threading`×8 (2.24 M) -- but with
  the goroutine model (cheap spawn, no thread-per-task explosion). asyncio
  gets **0× here -- it cannot use a second core**.
- **Network throughput vs Go** -- *same* in-process loopback echo bench (256
  concurrent conns × 8-byte round-trips, C `TCPConn` handler), Go and pygo at
  matched core/hub counts:

  | cores / hubs | Go (`net`) | pygo C `TCPConn` |
  | ---: | ---: | ---: |
  | 1  | 27 K/s · 37 µs/RT | 22 K/s · 46 µs/RT |
  | 4  | 94 K/s | 51 K/s |
  | 8  | 193 K/s | 100 K/s |
  | 16 | 324 K/s · 3.1 µs/RT | 143 K/s · 7.0 µs/RT |

  Go is **~1.2× per core** and **~2.3× at 16** -- it starts ~20% faster *and*
  scales better (75% vs pygo's 41% efficiency over 16 cores, as pygo hits
  CPython's refcount-contention ceiling). A **Python** handler is another
  ~2.2× slower than the C path. See *Current limitations* for why.

The honest ceiling: pygo does **not** make Python faster per core (~80 K
pure-Python ops/s/core is a CPython constant). It lets one process **hit that
on every core at once** with a blocking programming model -- which asyncio
(single loop, single core) structurally cannot.

### 3. vs asyncio / threads / processes

| | asyncio | OS threads | multiprocessing | **pygo** |
| --- | --- | --- | --- | --- |
| code style | `async`/`await` (colored) | blocking | blocking + IPC | **blocking, no async/await** |
| cores used | 1 (single loop) | N, but GIL-serialised (1 on GIL builds) | N | **N (3.13t, GIL off)** |
| practical max tasks | ~10⁵–10⁶ | ~10³–10⁴ (MB stacks) | ~10² | **10⁶+ (~26 KB/g)** |
| spawn cost | cheap | expensive (kernel thread) | very expensive | **cheap (asm swap)** |
| one unanticipated blocking call… | **stalls every task** | stalls just that thread | stalls just that process | **stalls just that hub -- and the runtime detects + recovers it** (see below) |

- **vs asyncio** -- pygo's structural wins: real multi-core parallelism,
  blocking-style code, and stall isolation+recovery. asyncio's wins: stdlib,
  mature, runs on any Python, huge ecosystem. pygo needs 3.13t for the
  multi-core win and is young.
- **vs threads** -- pygo scales to millions where threads cap at thousands (MB
  stacks + kernel scheduler overhead).
- **vs processes** -- pygo is one process (shared memory, cheap spawn);
  processes give hard isolation pygo doesn't.

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
Large-N needs raised kernel limits -- see [docs](docs/) (`vm.max_map_count`
is the one that bites first).

## Stall recovery (default ON, free-threaded 3.13t)

asyncio's fatal flaw is that one blocking task freezes the whole loop. pygo's
M:N scheduler isolates a stall to one hub, and a watchdog actively recovers it
(both default-on; opt out with `PYGO_HANDOFF=0` / `PYGO_PREEMPT=0`):

- **Blocking-IO wedge** (a goroutine in an unanticipated `Py_BEGIN_ALLOW_THREADS`
  call): a standby rescue thread adopts the stalled hub's thread-state and
  drains its stranded goroutines -- Go's `entersyscallblock` P-handoff. A pool
  recovers several simultaneously-wedged hubs in parallel.
- **CPU-bound wedge** (a goroutine monopolising a hub in a Python loop): the
  runtime preempts it at its next bytecode boundary so the hub round-robins --
  Go pre-1.14 cooperative preemption.

A >50 ms threshold keeps both dormant under normal load, so steady-state
scheduling is unchanged.

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
- **Socket monkey-patch** -- drop-in cooperative `socket`, `time`, `ssl`.
- **`pygo.aio`** -- run existing `async`/`await` code on the scheduler.
- **Go-style channels** -- `Chan(capacity)`, `select`, `for v in ch`; unbuffered
  ping-pong ~560 ns/round-trip (within 7% of Go 1.22 on the same box).
- **`pygo.blocking(fn, …)`** -- offload a genuinely-blocking call to a worker
  pool so it never wedges a hub in the first place.

## Current limitations & downsides

Read this before betting on pygo -- it's where the project actually is.

- **The multi-core win requires free-threaded CPython 3.13t.** On a normal
  (GIL) build pygo still runs -- cheap spawn, the goroutine model, netpoll -- but
  it's **single-core**, like asyncio. The headline parallelism numbers do not
  apply there.
- **Higher memory per goroutine than Go in Python** (~26 KB vs ~2.5–13 KB) --
  the CPython object tax; unwinnable without dropping to C handlers.
- **No per-core speedup.** pygo saturates all cores from one process but can't
  raise CPython's ~80 K ops/s/core. CPU-bound pure-Python work is still
  CPython-slow per thread.
- **Go is faster on raw network I/O -- ~1.2× per core, ~2.3× across 16, and
  another ~2.2× if the handler is Python.** Measured on the identical loopback
  echo bench (see *Performance*): single core **Go ~37 µs/RT vs pygo C
  `TCPConn` ~46 µs**; at 16 cores **324 K vs 143 K req/s** (Go scales better --
  pygo hits CPython's refcount-contention ceiling); a **Python** handler adds
  ~2.2× on top (~82 µs/RT) from interpreter overhead per `recv`/`send`. None of
  this is the scheduler -- that's ~47 ns/yield, Go-class. An opt-in io_uring
  path (`PYGO_TCPCONN_IOURING=1`) narrows the syscall side at high fan-out on
  Linux; matching Go per-operation with a Python handler isn't achievable.
- **Preemption only fires at Python bytecode boundaries.** A goroutine inside a
  long C call (`numpy`, `hashlib`) or a tight pure-C loop is **not**
  preemptible and will hold its hub until it returns -- same limitation Go has
  with cgo. (Blocking-IO is covered by the rescue handoff; call-bearing Python
  CPU by preemption; the remaining gap is tight C loops.)
- **Linux is the primary, heavily-validated target.** macOS/BSD/Windows
  backends are code-complete and maintained in-step, but the deep
  validation (2 M-conn runs, fuzzing, ASan) is on Linux x86_64 / 3.13t.
- **Young project.** APIs are stabilising; expect rough edges outside the
  documented happy paths.
- **`PYGO_PER_G_TSTATE` mode is experimental / default-OFF** (it SEGVs with
  Python handlers under load). The default snapshot-based scheduler is the
  supported path; per-g-tstate is not.
- **Python 3.11+ only** -- the frame snapshot depends on 3.11+ tstate layout;
  the pre-3.11 frame model isn't supported.
- **Two `aio`-compat tests are skipped** pending a fix to an executor-wake race
  in `pygo.aio` (a flake, not corruption).
- **aarch64 / Apple Silicon** validated via `qemu` + code review, not yet on
  real ARM hardware.

## Building

```bash
pip install -e .                                    # needs a C compiler
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .   # free-threaded
```

No compiler? `scripts/install.sh` (POSIX) / `scripts\install.bat` (Windows)
detect-and-install one, then build. Build knobs (`PYGO_BACKEND=ucontext`,
`PYGO_NO_IOCP=1`, `PYGO_DEBUG=1`, `PYGO_EXTRA_CFLAGS`, …) and the
`cibuildwheel` matrix (CPython 3.11–3.14, Linux/macOS/Windows) are documented
in [docs/](docs/).

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

Full guide in [docs/](docs/) (also on Read the Docs):
[Quickstart](docs/quickstart.md) ·
[Asyncio bridge](docs/asyncio.md) ·
[Sync API](docs/sync-api.md) ·
[Channels](docs/channels.md) ·
[Stack sizing](docs/stack-sizing.md) ·
[Monkey-patching](docs/monkey-patching.md) ·
[Preemption](docs/preemption.md) ·
[M:N parallelism](docs/parallelism.md) ·
[Cookbook](docs/cookbook.md) ·
[API reference](docs/api-reference.md)

## Layout

| Dir | Contents |
| --- | --- |
| `src/pygo_core/` | C extension: scheduler, channels, netpoll, asm backends, M:N hubs, stall recovery |
| `src/pygo/` | Python layers: `aio`, `sync`, `monkey`, `time`, `runtime` |
| `tests/` · `examples/` · `docs/` · `scripts/` | tests · benches + sample servers · docs · bootstrap installers |
