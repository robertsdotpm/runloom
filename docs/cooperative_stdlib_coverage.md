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
| `select.select` / `select.poll` (multi-fd) | **COOP** | offloaded to the pool (see crash note) |
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

## Known issue: GIL-release + timer-park SIGSEGV (scheduler-level)

A goroutine that makes a **GIL-releasing C call and then parks on the timer
(`sched_sleep`)**, in a loop, under **≥2 hubs**, gets its suspended eval-frame
tstate corrupted → deterministic SIGSEGV.  Parks on `wait_fd` (netpoll) are
**not** affected, and a park-only loop is clean — so the trigger is
specifically *GIL-release then timer-park* on a hub.

This is a deep M:N/free-threaded tstate hazard (the eval loop bakes a tstate
into the stackful-coro frame); it needs a scheduler fix validated with the
TSan/FV tooling, **not** a monkey patch.  The monkey busy-poll paths that used
this shape on a hot loop — `select.select`/`select.poll` (→ `subprocess.
communicate`, `socketserver`, `http.server`) — were changed to **offload** the
blocking call instead (the goroutine merely parks, the crash-free pattern), so
no first-party stdlib path triggers it.  Lower-frequency busy-poll sites
(`fcntl.flock`, `sigtimedwait`, mp `SemLock`) retain the residual risk; they
use a growing backoff (few cycles) and can't be safely offloaded (per-thread
signal masks / lock ownership).

Minimal reproducer (deterministic, ≥2 hubs):

```python
import select, runloom, runloom_c
runloom.monkey.patch(); GO = runloom_c.mn_go
def worker():
    for _ in range(300):
        select.select([], [], [], 0)   # releases + reacquires the GIL
        runloom.sleep(0.001)           # timer park
runloom.mn_init(2); GO(worker); runloom.mn_run(); runloom.mn_fini()
```
