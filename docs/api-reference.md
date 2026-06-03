# API reference

Every public symbol exported by runloom, organised by module.  This is
a reference -- start with the [guides](index.md) if you're learning
your way around.

## `runloom_c` (C extension)

The low-level scheduler API.  Most user code calls `runloom_c.go` and
`runloom_c.run`; everything else is for advanced use.

### Scheduler control

#### `go(fn, *, stack_size=None) → G`

Spawn a goroutine running `fn`.  Returns a [`G`](#g) handle.

- `fn` -- a zero-arg callable.  Bind arguments with `lambda` or
  `functools.partial`.
- `stack_size` -- optional per-call override (bytes).  Bypasses the
  scheduler's calibrated default.  See [stack sizing](stack-sizing.md).

#### `go_noyield(fn) → G`

Like `go(fn)` but with a contract: `fn` promises not to yield, sleep,
park, or do monkey-patched I/O.  The scheduler skips per-g datastack
setup, saving 150–400 ns per spawn.  **Undefined behaviour if `fn`
yields.**  Use only for pure-compute callables.

#### `run() → int`

Drive the scheduler until every goroutine has finished.  Returns the
count of completed goroutines.

#### `sched_yield()` / `sched_yield_classic()`

Cooperatively yield.  `sched_yield` is a vectorcall fastpath
singleton; `sched_yield_classic` is the equivalent PyCFunction (kept
for benchmarking, otherwise identical).

#### `sched_sleep(seconds)`

Park the current goroutine until at least `seconds` have elapsed.
Other goroutines run in the meantime.

#### `sched_stop()`

Signal the scheduler to exit its drain loop at the next safe point.
Used internally by `runloom.aio` for early termination.

#### `sched_reset() → (int, int, int)`

Drop everything queued in the scheduler (ready FIFO, sleep heap,
netpoll-parked).  Returns `(n_ready, n_sleep, n_parked)`.  Used by
`paio.run` for cleanup between runs.

#### `park_self()`

Park the current goroutine until `g.wake()` is called on its handle.
Race-safe -- a wake that arrives before the park is consumed and the
park returns immediately.  Use with `current_g()` to capture the
handle before parking.

#### `current_g() → G | None`

Return a handle to the currently-running goroutine, or `None` if
called from outside any goroutine.

### Stack sizing

#### `get_stack_size() → int`

Current per-goroutine default stack size, in bytes.

#### `set_stack_size(bytes)`

Override the default and freeze calibration.  Clamped to
`[16 KB, 8 MB]`.  Disables stack painting.

### Channels

#### `Chan(capacity)`

Construct a channel of the given buffer capacity.  `Chan(0)` is
unbuffered (rendezvous).  See [Channels](channels.md).

Methods:

- `send(value)` -- block until the value fits, then enqueue.  Raises
  `ValueError` on closed channel.
- `recv() → (value, ok)` -- block for a value.  Returns
  `(None, False)` after the channel is closed and drained.
- `try_send(value) → bool` -- non-blocking send; `False` if the buffer
  is full.
- `try_recv() → (value, ok) | None` -- non-blocking; `None` if buffer
  empty.
- `close()` -- wake every parked sender (they raise) and receiver
  (they get `(None, False)`).
- `__iter__()` -- yields values until the channel closes.

#### `select(cases, default=False)`

Multi-way wait.  Each case is `("recv", chan)` or `("send", chan, value)`.

- Returns `(idx, payload)` for a fired case where `payload` is
  `(value, ok)` for recv or `None` for send.
- Returns `-1` (bare integer) if `default=True` and no case is ready.

### Networking primitives

#### `wait_fd(fd, events, timeout_ms=-1) → int`

Park the current goroutine until `fd` is ready.  `events` is a
bitmask: `1 = read`, `2 = write`.  Returns the ready bitmask.
`timeout_ms=-1` for no timeout; 0 to poll without parking.

#### `fd_read(fd, n) → bytes`, `fd_write(fd, data) → int`

Cooperative read/write on an fd.  Park on `wait_fd` when EAGAIN.

#### `tcp_recv(sock, n) → bytes`, `tcp_send(sock, data) → int`

TCP-specific fastpaths.  `sock` is a Python `socket.socket` (or its
fileno).

#### `file_read(fd, n, offset=-1) → bytes`, `file_write(fd, data, offset=-1) → int`

File I/O.  On Linux 5.1+ with `iouring_available()`, dispatched
through io_uring.  Elsewhere dispatched through a worker thread.

#### `iouring_available() → bool`

True if the kernel supports io_uring (Linux 5.1+).

### M:N parallelism (3.13t only)

See [Parallelism](parallelism.md).

- `mn_init(n=0)` -- start `n` hub threads (defaults to `cpu_count`).
- `mn_go(fn) → G` -- spawn on a round-robin hub.
- `mn_run() → int` -- wait for all hubs to drain.
- `mn_fini()` -- tear down the pool.

### Preemption (3.13t only)

See [Preemption](preemption.md).

- `preempt_init(quantum_us=10000)` -- start the per-thread quantum timer.
- `preempt_fini()` -- stop the timer.

### Pre-warming

#### `warmup(n, stack_size=None)`

Pre-allocate `n` goroutine stacks so the first `n` spawns skip mmap.

### Diagnostics

#### `backend() → str`

Active context-switch backend: `"fcontext-asm"`, `"fibers"`, or
`"ucontext"`.

#### `netpoll_backend() → str`

Active netpoll: `"epoll"`, `"kqueue"`, `"wsapoll"`, `"iocp"`, or
`"select"`.

#### `stats() → dict`

Snapshot of scheduler counters.  Keys: `ready`, `sleeping`,
`netpoll_parked`, `completed`, `running`, `stack_size_default`,
`stack_hwm`, `stack_completed`, `stack_calibrated`, `stack_painting`,
`backend`, `netpoll`.  Cheap; safe to poll periodically.

### Goroutine introspection

A Go-style goroutine dump -- which goroutines exist, what each is blocked
on, and where in your code.  See the [Debugging guide](debugging.md) for
the full picture (and the friendlier `runloom.inspect` wrappers).

#### `goroutines() → list[dict]`

One dict per live goroutine: `id`, `state` (`running` / `runnable` /
`io-wait` / `sleep` / `chan-wait` / `park` / ...), `blocked_on`, `fd` +
`events` (when `io-wait`), `wake_in` (when `sleep`), `age`, `refcount`,
`noyield`, `owner`.  Cheap; safe from a watchdog.

#### `goroutine_count() → int`

Number of live goroutines.

#### `goroutine_stack(id) → (callable_repr, [(file, line, func), ...])`

Best-effort reconstructed Python stack of one goroutine (deepest first).
Full stack under the single-thread scheduler (`runloom.aio`) and per-g-tstate
M:N; withheld under default M:N (no safe way to freeze a hub-resumable
goroutine).

#### `dump_goroutines(fd=2) → None`

Write an async-signal-safe structural dump (state histogram + per-goroutine
line, no Python objects) to `fd`.  The SIGQUIT path -- usable from a signal
handler and when the interpreter is wedged.

#### `set_introspect_timestamps(bool)`

Track each goroutine's park time so `goroutines()`/dumps report `age`.  Off
by default (one clock read per park); also via `RUNLOOM_INTROSPECT_TIME=1`.

#### `install_traceback_signal(signum=SIGQUIT) → int`

Install a raw-C signal handler that dumps all goroutines to stderr -- Go's
`GOTRACEBACK` / `kill -QUIT`.  Also via `RUNLOOM_TRACEBACK=1`.  POSIX only.

#### `reset_after_fork() → None`

Reset the runtime to a clean single-process state in a forked child
(abandons the dead hub/offload threads, re-inits inherited locks, gives the
child its own netpoll fd).  Registered automatically as an
`os.register_at_fork(after_in_child=...)` handler; see the [Debugging
guide](debugging.md#fork-safety).

#### `set_deadlock_mode(0|1|2)` / `get_deadlock_mode() → int` / `count_deadlocked() → int`

Deadlock detection: when the single-thread scheduler quiesces with
goroutines still blocked on channels/parks, mode 0=off, 1=warn (print the
dump, default), 2=raise `RuntimeError`.  Also `RUNLOOM_DEADLOCK=off|warn|raise`.
`count_deadlocked()` is the current chan/park-blocked count.

#### `set_max_goroutines(n)` / `get_max_goroutines() → int` / `live_goroutines() → int`

Backpressure: cap the live-goroutine count (0 = unlimited).  Over the cap,
spawn raises `RuntimeError`.  Also `RUNLOOM_MAX_GOROUTINES`.  Zero hot-path cost
when unset.

#### `set_introspect_timestamps(bool)` / `get_introspect_timestamps() → bool`

Park-age tracking (enables the `age` field + `runloom.inspect.leaked()`).

### Thread setup

#### `thread_init()` / `thread_fini()`

Per-OS-thread setup/teardown.  Called automatically; only invoke
manually if you're embedding runloom in a non-main thread.

### Types

#### `G`

Goroutine handle.  Attributes:

- `done` -- `True` once the goroutine has returned.
- `result` -- return value (or `None` until done).
- `error` -- exception object if the goroutine raised, else `None`.
- `wake()` -- re-queue a parked goroutine; race-safe with
  `park_self()`.
- `stack(limit=None)` -- return a list of `(filename, lineno, name)`
  frames for the goroutine's current Python stack.

#### `Coro`

Lower-level coroutine handle.  Most users won't construct these
directly; `G` wraps a `Coro` plus scheduler metadata.

---

## `runloom`

Top-level package.  Re-exports a Go-style API from `runloom.runtime`
(the original Python-only scheduler, kept for backward compatibility).

```python
import runloom

runloom.go(fn)            # spawn (uses the C scheduler under the hood)
runloom.yield_()          # cooperative yield
runloom.sleep(seconds)    # cooperative sleep
runloom.run(main_fn=None) # drive until idle
runloom.current() → Goroutine
runloom.backend() → str
```

For new code, prefer `runloom_c` (faster) or `runloom.sync` (richer API).

---

## `runloom.inspect`

Runtime introspection -- the friendly wrappers over the goroutine
registry.  `goroutines(stacks=)`, `count()`, `stack(id)`, `format(stacks=)`
(a human dump as a string), `dump(file=, stacks=)`, `enable_timestamps()`,
`install_dump_signal()`, `leaked(min_age, states)` / `watch_leaks(...)`
(leak detection), `set_deadlock_mode("off"/"warn"/"raise")`,
`set_max_goroutines(n)` / `live_goroutines()` (backpressure).  See the
[Debugging guide](debugging.md).

```python
import runloom.inspect as gi
print(gi.format(stacks=True))   # which goroutines, and where they're stuck
gi.install_dump_signal()        # kill -QUIT <pid> -> dump
```

---

## `runloom.aio`

Asyncio bridge.  See [runloom.aio](asyncio.md).

```python
runloom.aio.run(coro)                     # equivalent of asyncio.run
runloom.aio.install()                     # set RunloomEventLoopPolicy globally
runloom.aio.open_connection(host, port)   # async (reader, writer)
runloom.aio.start_server(cb, host, port)  # async server with serve_forever()
```

Classes:

- `RunloomEventLoop` -- drop-in `asyncio.AbstractEventLoop` backed by
  runloom's scheduler.
- `RunloomEventLoopPolicy` -- sets `RunloomEventLoop` as the default loop.
- `RunloomFuture` -- duck-typed Future with synchronous done-callback
  dispatch.
- `RunloomTask` -- `asyncio.Task` replacement that drives the coroutine
  inside a goroutine.
- `StreamReader` / `StreamWriter` -- asyncio-compatible stream
  interface, backed by `wait_fd`.
- `DatagramTransport` -- UDP transport for
  `loop.create_datagram_endpoint`.

---

## `runloom.sync`

No-`async`/`await` facade.  See [Sync API](sync-api.md).

```python
runloom.sync.go(fn, *args, **kwargs)        # spawn with args
runloom.sync.run(main_fn=None)              # drive scheduler
runloom.sync.sleep(seconds)
runloom.sync.yield_now()
runloom.sync.current() → G

runloom.sync.Chan                            # re-export of runloom_c.Chan
runloom.sync.select                          # re-export of runloom_c.select
runloom.sync.park / wake                     # park_self + wake helpers

runloom.sync.tcp_connect(host, port) → Socket
runloom.sync.tcp_listen(host, port, *, backlog=128) → Socket
runloom.sync.udp_endpoint(local_addr=None, remote_addr=None) → Socket
```

Synchronisation primitives matching `asyncio.*`:

- `runloom.sync.Lock` -- cooperative mutex.
- `runloom.sync.Event` -- set/clear/wait.
- `runloom.sync.Condition` -- waiter + notifier on a lock.
- `runloom.sync.Semaphore` -- bounded counting semaphore.

#### `Socket`

Wrapper around `socket.socket` whose blocking methods (`connect`,
`accept`, `recv`, `send`, `sendall`, `recv_into`, `recvfrom`, `sendto`)
park cooperatively on `wait_fd`.  Standard `socket.socket` attributes
(`setsockopt`, `fileno`, `getsockname`, `close`, etc.) pass through.

---

## `runloom.time`

Go-style timers and tickers.

#### `Sleep(seconds)`

Cooperative sleep.  Alias for `runloom_c.sched_sleep`.

#### `After(seconds) → Chan`

Returns a channel that will receive the current time after `seconds`.
Equivalent of Go's `time.After`.

```python
import runloom.time as t

after = t.After(1.0)
# ... do work ...
after.recv()           # blocks until the timer fires
```

#### `NewTimer(seconds) → Timer`

Single-shot timer.  Methods:

- `Timer.C` -- channel that fires once.
- `Timer.Stop()` -- cancel; returns `True` if cancelled before firing.
- `Timer.Reset(seconds)` -- rearm.

#### `NewTicker(seconds) → Ticker`

Recurring ticker.  Methods:

- `Ticker.C` -- channel that fires every `seconds`.
- `Ticker.Stop()` -- stop emitting.

#### `Tick(seconds) → Chan`

Shorthand for `NewTicker(seconds).C` when you don't need to stop it
(the ticker leaks -- only use for program-lifetime tickers).

---

## `runloom.monkey`

Stdlib monkey-patching.  See [Monkey-patching](monkey-patching.md).

#### `patch(**flags)`

Apply patches.  Default: all categories enabled.  Opt out:

```python
runloom.monkey.patch(threading=False, dns=False)
```

Categories: `socket`, `time`, `os`, `select`, `stdio`, `ssl`,
`subprocess`, `threading`, `queue`, `dns`.

#### `unpatch(**flags)`

Reverse patches.  Without args, reverses everything applied.

#### Co-aware synchronisation primitives

Available even without `patch()`:

- `CoLock`, `CoRLock` -- cooperative mutexes.
- `CoEvent` -- set/wait.
- `CoCondition` -- `wait` releases the lock cooperatively.
- `CoSemaphore`, `CoBoundedSemaphore` -- counting semaphores.

These implement the `threading.*` interface but park goroutines
instead of OS threads.  Useful when you want sync primitives but
don't want to install the full monkey patch.
