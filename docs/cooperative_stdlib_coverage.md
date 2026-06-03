# Cooperative stdlib coverage under the M:N scheduler

What of the Python standard library cooperates (parks the goroutine) vs blocks
an OS hub, under `runloom.monkey.patch()` + the M:N scheduler (`mn_init`/
`mn_go`/`mn_run`, free-threaded 3.13t, GIL off).  Built by scanning CPython
`Lib/` for the leaf blocking primitives and empirically probing each under a
single-hub canary (a 5 ms ticker goroutine: if the op parks the canary keeps
ticking → COOP; if it blocks the hub the canary freezes → STALL).

## Principle

The blocking surface is a small set of **leaf primitives**, not the thousands
of high-level functions.  Patch the leaf cooperatively and every module that
routes through it cooperates transparently.  e.g. `urllib`/`http.client`/
`ftplib` read via `socket.makefile()` → `SocketIO.readinto` → `socket.recv_into`
(patched), so they cooperate with no per-module work.

## Coverage (empirically validated)

| area | status | notes |
| --- | --- | --- |
| sockets (recv/send/accept/connect/sendfile) | **COOP** | netpoll `wait_fd` |
| TLS (`ssl`) incl. handshake | **COOP** | `wrap_socket` does a cooperative client handshake |
| DNS (`getaddrinfo`/`gethostbyname`) | **COOP** | async resolver; honors `AI_NUMERICHOST` |
| `select.epoll`/`select.kqueue` + `selectors` | **COOP** | park on the backing fd via `wait_fd` |
| `select.select` / `select.poll` (multi-fd) | **COOP** | offloaded to the pool so the wait parks instead of busy-polling the hub |
| pollable-fd file objects (pipes: `Popen.stdout`, `os.popen`) | **COOP** | `open`/`io.open` route pollable fds through `_pyio` on cooperative `os.read` |
| `subprocess` run/communicate/wait | **COOP** | pidfd + cooperative selectors/os |
| `os.waitpid`/`wait*`, `os.system` | **COOP** | pidfd / offload |
| `time.sleep`, `time.After`/`Tick`/`Timer`/`Ticker` | **COOP** | timers spawn on the active scheduler |
| `signal.sigwait`/`sigtimedwait`/`pause` | **COOP** | |
| `threading` Lock/RLock/Event/Condition/Semaphore/Barrier | **COOP** | M:N-safe (Lock backed by `runloom_c.Mutex`) |
| `queue.Queue`/`SimpleQueue`/Lifo/Priority | **COOP** | built on the cooperative Condition |
| `context.WithCancel`/`WithTimeout`/`WithDeadline` | **COOP** | deadline timer on the active scheduler |
| regular-file buffered reads | **FAST** locally | block only on slow media (NFS/FUSE); `open()` syscall is offloaded |
| GIL-releasing C blockers (`sqlite3`, `ctypes` I/O, `getrandom`) | **COOP*** | rescued by the sysmon handoff after ~50 ms; use `offload()` to avoid the latency |
| GIL-holding CPU (pure-Python loops, CPython-C aggregations) | **STALL** | fundamental — relocate via `offload()`/the `heavy` pattern |
| `multiprocessing` fork start-method | **deadlock** | use `spawn`/`forkserver` |
| `concurrent.futures.ProcessPoolExecutor` | **unsupported** | use the goroutine-backed `ThreadPoolExecutor` |

`*` Handoff makes GIL-releasing C calls cooperative without an explicit patch:
the hub goes DETACHED, a rescue thread adopts it (~50 ms), other goroutines run.

## Resolved: select.select() SIGSEGV was a goroutine stack overflow

An earlier note here described a "GIL-release + timer-park SIGSEGV" thought to
be a deep M:N tstate-corruption hazard.  **That diagnosis was wrong** — it was
a plain goroutine **C-stack overflow**, and it had nothing to do with the GIL,
timers, or the number of hubs.

CPython's `select_select_impl` declares three `pylist[FD_SETSIZE + 1]` arrays
and uses **50.9 KB of C stack in a single frame** (measured with
`runloom_coro_scan_hwm`).  Every other stdlib leaf is <10 KB — `ssl` handshake
8 KB, `json` 6 KB, `getaddrinfo`/`re` ~3 KB — so `select` is the worst case by
6×.  The old default goroutine stack was **32 KB**, smaller than that frame, so
the *very first* `select.select()` in a goroutine ran its
`-fstack-clash-protection` probe page-by-page straight into the coro's
PROT_NONE guard page → deterministic SIGSEGV, on **every** scheduler (M:1 `go`,
M:N `mn_go`, any hub count).  The "GIL-release / timer-park / ≥2 hubs" framing
was correlation: `select.select` happened to be the test's GIL-releasing call,
but its real relevance was the fat frame.  Park-only and `wait_fd` loops were
clean only because they never call a fat-frame C function.

**Fix (scheduler-level, not a monkey patch):** the default goroutine C-stack is
now **128 KB** — it clears `select`'s 50.9 KB with a ~2.5× margin for Python /
user frames above it.  The cost is VM address space only, not RSS (coro stacks
are demand-paged and `MADV_DONTNEED`'d on recycle; VMA count is size-
independent).  Calibration still adapts UP for stack-hungry programs but now
**never shrinks below the floor** (a g that overflows crashes before its HWM is
sampled, so a program whose first 1000 gs happen not to call `select` must not
be allowed to shrink the floor and re-arm the crash).  Tune the floor with
`RUNLOOM_STACK_SIZE=<bytes>` or `runloom_c.set_stack_size()`.  Regression guard:
`tests/test_stack_size.py`.

The `select.select`/`select.poll` **multi-fd offload** is retained, but for
cooperation, not crash-safety: it lets a multi-fd wait *park* on a pool worker
instead of busy-polling the hub.  Empty/single-fd `select` still runs inline on
the goroutine stack and is safe purely because of the 128 KB floor.

Reproducer (now exits cleanly; SEGV'd before the fix at the 32 KB default):

```python
import select, runloom, runloom_c
runloom.monkey.patch(); GO = runloom_c.mn_go
def worker():
    select.select([], [], [], 0)       # ~51 KB C frame; overflowed 32 KB
runloom.mn_init(2); GO(worker); runloom.mn_run(); runloom.mn_fini()
```
