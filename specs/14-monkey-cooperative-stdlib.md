# 14 — Monkey-patched cooperative stdlib

Ground truth: `runloom/monkey/` — `__init__.py` (categories + patch/unpatch),
`_base.py` (foundation: fiber detection, the Parker, the backend),
`sockets.py`, `osio.py`, `files.py`, `polling.py`, `tls.py`, `subproc.py`,
`signals.py`, `locks.py`, `events.py`, `queues.py`, `executors.py`, `dns.py`,
`dns_proto.py`, `heavy.py`; `docs/monkey-patching.md`,
`docs/cooperative_stdlib_coverage.md`; and the `Cooperative primitives must be
FOREIGN-OS-THREAD-safe` invariant in `CLAUDE.md`.

## The idea

`runloom.monkey.patch()` replaces blocking stdlib calls with cooperative ones that
**park the fiber instead of blocking the OS thread**. After it, ordinary
`socket.recv`, `time.sleep`, `ssl.read`, file I/O, `subprocess` waits, DNS, and
several `threading` primitives all yield — so any synchronous library (`requests`,
`pymysql`, `urllib`, `psycopg2`) becomes cooperative *unchanged*. It is the
front-end for "I have blocking code I don't want to rewrite" (vs `aio` for `async
def` code, vs `sync` for new Go-style code).

## The leaf-primitive principle (why this is tractable at all)

> The blocking surface of the stdlib is a **small set of leaf primitives**, not
> the thousands of high-level functions. Patch the leaf cooperatively and every
> module that routes through it cooperates transparently.

`urllib`/`http.client`/`ftplib` all read via `socket.makefile()` →
`SocketIO.readinto` → `socket.recv_into`. Patch `recv_into` once and they all
cooperate with zero per-module work. This is the single insight that makes
"cooperative stdlib" a few hundred lines instead of an impossible surface. The
empirically-validated coverage table is in `docs/cooperative_stdlib_coverage.md`.

## The categories (`__init__.py`)

`patch(**flags)` is idempotent and category-toggled (all default True):
`socket`, `time`, `os`, `select`, `selectors`, `stdio`, `getpass`, `ssl`,
`subprocess`, `process`, `threading`, `queue`, `futures`, `multiprocessing`,
`file`, `syscalls`, `fcntl`, `signal`, `heavy`, `compile`, `dns`. Order matters
(`socket` before `dns` because dns wraps socket fns). Each category is a
`(_patch_*, _unpatch_*)` pair in a section module; `__init__` is just the registry
+ ordering + the `_applied` set.

Representative mechanisms:

- **socket** — `recv`/`send`/`accept`/`connect`/`sendfile`/`recvmsg`… catch
  `BlockingIOError` and park on `wait_fd` (spec 06). `sendfile` reimplements the
  zero-copy `os.sendfile` fast path parking on `wait_fd`.
- **selectors** — patching `select.poll`/`epoll`/`kqueue` cooperative
  transparently makes the high-level `selectors` module cooperative, which is what
  `subprocess.communicate()`, `socketserver`, `http.server`, `wsgiref` actually
  block on. `epoll`/`kqueue` park on their *own backing fd* via `wait_fd`
  (event-driven); `poll` has no backing fd so it's offloaded.
- **select.select** — reimplemented cooperatively (register fds on a transient
  epoll, park on the epoll's own fd) so CPython's 50.9 KB `select_select_impl`
  frame is never reached from a fiber (spec 06/10).
- **subprocess/process** — park on a **pidfd** (Linux 5.3+) until the child exits,
  then reap; busy-poll fallback otherwise.
- **threading** — `Lock`/`RLock`/`Event`/`Condition`/`Semaphore`/`Barrier` made
  cooperative (Lock backed by `runloom_c.Mutex`); `queue` builds on them.
- **futures** — `ThreadPoolExecutor` is **fiber-backed** (work runs as
  fibers so `Future.result`/`as_completed` resolve in-domain; a real-threaded
  executor would notify a cooperative Condition cross-thread and deadlock the
  waiter). `ProcessPoolExecutor` is unsupported (its manager thread + forkserver
  machinery is nondeterministic under the cooperative scheduler).
- **heavy / compile** — size-gated auto-offload of CPU-bound C calls and
  `builtins.compile` (spec 08, 10).
- **dns** — a pure-async UDP resolver; see below.

## The foundation (`_base.py`) — three load-bearing pieces

### 1. Goroutine-context detection (`_in_fiber`)

`runloom_c` doesn't expose a "current fiber" accessor cheaply, so `monkey`
wraps `runloom_c.go`/`mn_go` to bump a **thread-local counter** for the duration of
each user callable; `_in_fiber()` is `count > 0` (plus the Python-scheduler's
`runloom.current()`). Every cooperative patch branches on this: in a fiber →
park cooperatively; outside one → fall back to real blocking. This is why the same
patched code is safe whether or not you're on a fiber.

### 2. The self-pipe Parker (`_Parker`)

A fiber that has no readiness fd to wait on (a Condition, a pool-completion
wake) parks on a **self-pipe**: POSIX `os.pipe()` (kernel fd ints all backends can
poll) or, on Windows, `socket.socketpair()` (Win `select` only polls SOCKET
handles, not pipe fds). `park()` waits on the read end; `unpark()` writes a byte.
Pooled (cap 64), with a real lock for the pop/append (under M:N several hubs build
parkers concurrently; `if _pool: _pool.pop()` is a TOCTOU race).

### 3. The blocking backend (`_ThreadPoolBackend`)

The non-pollable I/O patches (files, disk syscalls, `os.read`/`write` on regular
files, DNS-via-libc) dispatch through `_get_backend().submit(fn, args, kwargs)` —
a pre-started worker pool that runs the blocking call and writes a self-pipe byte
when done. The submit box carries a `done` flag (not just a result): a pooled
Parker can carry a stale wake byte and `wait_fd` can wake spuriously, so submit
**loops until `done`** to be edge-insensitive. The backend interface is one method
(`submit`), so a Linux **io_uring** backend (spec 08) slots in with no caller
change. `offload(fn, …)` is the public form. `_blocking_call` runs inline when not
on a fiber (zero dispatch overhead).

## The FOREIGN-OS-THREAD-safety invariant (the one that crashed under M:N)

`patch()` replaces `threading`/`select`/… **globally**, so a cooperative primitive
can be invoked from a thread that is **not a fiber and not a hub** — most often
a stdlib-internal daemon thread (a `multiprocessing.Queue._feed` thread, a
`concurrent.futures` worker) that takes a patched `Lock`/`Condition`. Such a thread
has no fiber, no hub, no per-thread scheduler. Any primitive it can reach must
detect this (`_in_fiber()` is False / a TLS peek is NULL) and **fall back to
real OS blocking**, never:

- **(a)** park a fiber that doesn't exist — the `_Parker` on a foreign thread
  blocks the *thread* on its wake fd with a **raw `select`** (`_raw_select`,
  captured before the cooperative patch), not `runloom_c.wait_fd` (which parks a
  fiber on a hub's netpoll — undefined on a thread with no fiber/hub →
  SIGSEGV under M:N);
- **(b)** lazily allocate scheduler state — `current_g()` must
  `runloom_sched_peek_current()`, never `runloom_sched_get()` (which mallocs a sched
  + arms the wake-pump). Same "peek, never get on a foreign thread" rule as spec 04.

Violating this raced into SIGSEGV/UAF under M:N (the free-threaded `mp.Queue`
case). The regression net is a synthetic multiprocessing corpus under `run(8)` +
`patch()`.

## The async DNS resolver (`dns.py` / `dns_proto.py`) — a notable design

A pure-async, Go-`netgo`-style resolver: parse `/etc/resolv.conf` + `/etc/hosts`,
send A/AAAA queries in parallel over the **cooperatively-patched UDP sockets**
(so the queries themselves park, not block), with a 60 s result cache — **no
threads**. (On Windows, which has no `/etc/resolv.conf`, it falls back to libc
`getaddrinfo` via the backend pool.) This matters because `getaddrinfo` is the
canonical non-preemptible blocking C call; doing DNS in pure Python over
cooperative sockets removes the most common reason to need the offload pool.

## Caveats that bound the design (from the docstring)

- **Patch early** — a library that does `from socket import socket` and caches the
  class at import time keeps the original if you patch after; patch-then-import.
- **`threading.Thread` is NOT replaced** — turning it into a fiber would break
  too many assumptions; use `runloom.go`.
- **Buffered file `.read()/.write()` can't be patched** — `io.FileIO`/`io.Buffered*`
  are immutable C types; use `os.read`/`os.write` on the raw fd, or `offload()`.
- **`mp` *fork* start-method deadlocks** (inherits runloom's threads) — use
  spawn/forkserver.

## The `__getattr__` PEP-562 subtlety (shared with `aio`)

Both `monkey` and `aio` were once flat modules; tools/tests read their internals
directly. To preserve that read surface after the split, `__init__.py` uses a
**PEP 562 module-level `__getattr__` function** that resolves a name against the
section modules — *not* a `ModuleType` subclass with `__getattr__`. The reason is a
real crash (memory: "Module `__getattr__` fiber SEGV"): a `__class__`-swapped
module subclass's `__getattr__` read **inside a fiber** segfaults; the plain
PEP-562 function form works. To *write* a section internal, assign to the section
module directly.

## Invariants

1. **Patch leaves cooperative; everything routing through them cooperates** — the
   leaf-primitive principle keeps the surface small.
2. **Every cooperative path branches on `_in_fiber()`** — park in a fiber,
   real-block otherwise.
3. **A foreign OS thread reaching a cooperative primitive falls back to real
   blocking** — raw `select`, never `wait_fd`; peek the sched, never allocate one.
   (M:N-fatal if violated.)
4. **The blocking backend's submit loops until `done`** (edge-insensitive to
   spurious/stale Parker wakes).
5. **Use a PEP-562 function `__getattr__`, never a module-subclass `__getattr__`**
   (the latter SEGVs inside a fiber).
