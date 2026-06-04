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
| `select.select` | **COOP** | reimplemented on a transient epoll: register the fds, park on the epoll's own fd via `wait_fd`, map back (no fat frame, no pool thread); offload fallback on a non-epollable fd / no-epoll platform |
| `select.poll` (object) | **COOP** | no backing fd to park on; offloaded to the pool so it parks instead of busy-polling |
| pollable-fd file objects (pipes: `Popen.stdout`, `os.popen`) | **COOP** | `open`/`io.open` route pollable fds through `_pyio` on cooperative `os.read` |
| `subprocess` run/communicate/wait | **COOP** | pidfd + cooperative selectors/os |
| `os.waitpid`/`wait*`, `os.system` | **COOP** | pidfd / offload |
| `sys.stdin` / `input()` | **COOP** | park on stdin's fd, then read |
| `getpass.getpass()` | **COOP** | offloaded to a worker (its `/dev/tty` read is opened internally) |
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

## Fat C frames vs the goroutine stack (select, ssl)

A goroutine runs on a small fixed C stack (default **32 KB**, with a PROT_NONE
guard page).  CPython's own C code assumes the main thread's ~8 MB stack, and a
few functions allocate **a single fat C frame** that overflows 32 KB — the
`-fstack-clash-protection` probe then walks straight into the guard page →
deterministic SIGSEGV (a clean crash, not corruption, thanks to the guard).
This is *not* a depth problem: Python→Python recursion lives on runloom's
growable datastack (proven safe to 1M deep), and C↔Python recursion is bounded
by CPython's `c_recursion_remaining` counter (clean `RecursionError`, proven to
100K deep).  Only a single oversized frame is unguarded — and the whole stdlib
has just two:

* **`select.select` — 50.9 KB** (three `pylist[FD_SETSIZE + 1]` arrays; measured
  with `runloom_coro_scan_hwm`).  *(An earlier note here misdiagnosed this as a
  "GIL-release + timer-park M:N tstate-corruption SIGSEGV needing TSan/FV." It
  is neither deep nor M:N-specific — it reproduces on `go`/`run` too. The
  GIL-release framing was pure correlation.)*
* **first `ssl` use — ~40 KB** (OpenSSL's one-time library init, on first
  `import _ssl` / first context).

**Fix — keep the 32 KB stack; remove the fat frame from the goroutine path:**

* `select.select` is **reimplemented cooperatively** (`monkey/polling.py`):
  register the fds on a transient epoll, park on the epoll's *own* fd via
  `wait_fd`, drain with a non-blocking `poll(0)`, map back to the (r, w, x)
  lists.  CPython's `select_select_impl` is never called from a goroutine, so
  the 50.9 KB frame never exists there — and the goroutine parks on netpoll
  like any other socket (no pool thread, scales to a million waiters), instead
  of the heavier offload.  Falls back to a pool-thread offload only on a
  non-epollable fd (regular file) or a no-epoll platform (Windows; `*BSD`/macOS
  could grow a kqueue path).  `select.poll` (no backing fd) stays on offload.
* first `ssl` use is **warmed on the main thread**: `runloom.monkey` imports
  `ssl` on the main thread and `_patch_ssl` forces OpenSSL init there (8 MB
  stack), so the fat init is pre-paid off any goroutine.

No global stack raise is needed.  Regression guards: `tests/test_stack_frames.py`
— cooperative-select correctness + a sibling-runs-while-one-parks cooperation
check, a measured **C-frame-footprint guard** (every non-allowlisted stdlib
leaf must fit the default stack, so a *new* fat frame is caught), and the
ssl-warmed-so-goroutine-is-safe check.  A goroutine that calls into arbitrary
third-party C with a single >32 KB frame remains the one residual: it fails as
a clean guard-page crash and is fixed by sizing that goroutine
(`runloom_c.go(fn, stack_size=…)`) or offloading it.

Reproducer (now exits cleanly; SEGV'd before the fix):

```python
import select, runloom, runloom_c
runloom.monkey.patch(); GO = runloom_c.mn_go
def worker():
    select.select([], [], [], 0)       # cooperative epoll path; no 51 KB frame
runloom.mn_init(2); GO(worker); runloom.mn_run(); runloom.mn_fini()
```

### Deep C-recursion residual (`ast` / `compile`)

A second, narrower stack class is *depth*, not a single frame.  A goroutine's
C-recursion guard is CPython 3.13's fixed `c_recursion_remaining` counter, which
is calibrated for the main thread's 8 MB stack and is NOT lowered per goroutine
(it has no stack-pointer check; that arrives in 3.14).  So whether deeply-nested
input gives a clean `RecursionError` or a SEGV on a 32 KB goroutine depends on
how much C stack each recursion level costs:

| op | C stack / level | result at depth in a 32 KB g |
| --- | --- | --- |
| `json` / `pickle` / `marshal` / `copy.deepcopy` / `pprint` | ~60–80 B | **clean RecursionError** (counter fires ~150 levels ≈ 12 KB, well under 32 KB) |
| `ast.parse` / `compile()` | ~1.5 KB | **SEGV at ~20 levels** — the stack is gone before the counter fires |

So the common, DoS-relevant cases (parsing untrusted nested JSON/pickle in a
goroutine) are **safe** — they degrade to `RecursionError` exactly like on the
main thread.  `ast`/`compile` would be the exception (they SEGV past ~18-deep,
before the counter fires), so they are **auto-offloaded**: the `compile` patch
(`monkey/heavy.py`) routes `builtins.compile` to the backend pool's full-size
thread stack when called inside a goroutine, where the recursion fits and
degrades to a clean `RecursionError` like the main thread.  This covers
`compile()`, `ast.parse()` (it calls `builtins.compile`), and source imports.
It's a no-op off-goroutine (where compiles normally happen), so import-time
compilation is untouched.

There is no clean shared-counter fix that would make this general (lowering the
counter to make `ast` safe would force `json`/`pickle` to `RecursionError` at
~14, breaking ordinary nested data); the general fix is CPython 3.14's
stack-pointer-based recursion check, at which point runloom can set each
goroutine's `c_stack_*` bounds and drop the offload.

**Residual:** `eval(str)`/`exec(str)` compile *internally in C* (not via
`builtins.compile`) and need the caller's namespace, so they are not offloaded.
A goroutine that `eval`/`exec`s deeply-nested untrusted source should use
`runloom.monkey.offload()` / `runloom_c.blocking()` for the compile, or a
roomier g-stack (`runloom_c.go(fn, stack_size=…)`).

### Re-scan summary (stdlib C-frame sweep)

A measured sweep of ~40 stdlib leaf operations confirms the fat-frame surface
is exactly **two single frames** — `select.select` (50.9 KB) and first-`ssl`
use (40 KB), both handled above — plus the `ast`/`compile` deep-recursion case
(now auto-offloaded; `eval`/`exec`-of-string the documented residual).
Everything else fits the 32 KB default with margin (next-largest: `email`
parse 21 KB, `json` nested 15 KB, `sqlite3` 13 KB, hashlib/socket/subprocess
~10 KB).  The blocking surface (sockets, TLS handshake, DNS, selectors,
buffered pipes, subprocess, files, signals, sync primitives) is cooperative or
offloaded per the table above; the earlier TLS-handshake / buffered-pipe gaps
are closed.
