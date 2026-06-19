# runloom

**Go-style stackful coroutines for Python.**

runloom gives you the *cooperative concurrency* model from Go -- `go(fn)`,
channels, cheap fibers, blocking-style I/O -- running on top of
CPython with a hand-rolled assembly context switch and a C scheduler.

```python
import socket, runloom
runloom.monkey.patch()

def handle(conn):
    while True:
        data = conn.recv(4096)
        if not data:
            break
        conn.sendall(data)
    conn.close()

def accept_loop():
    s = socket.socket(); s.bind(("127.0.0.1", 9000)); s.listen(128)
    while True:
        conn, _ = s.accept()
        runloom.fiber(lambda c=conn: handle(c))

runloom.fiber(accept_loop)
runloom.run(1)
```

No `async`, no `await`, no callback chains -- `recv` and `accept`
suspend the fiber cooperatively while the OS thread runs other
fibers.

## What you get

- **Cheap fibers.**  A fiber is ~16 KB of C stack + ~150 B
  metadata after [calibration](stack-sizing.md).  50 000 idle
  fibers on one OS thread is normal; 200 000 has been tested.
- **Two programming styles.**  Use `runloom.fiber(fn)` for plain
  Go-style code, or `runloom.aio.run(coro)` to drive existing `async def`
  code on the same scheduler.  See the [asyncio bridge](asyncio.md).
- **Channels.**  `runloom.Chan(capacity)` with send/recv/close, plus
  `runloom.select([...])` for multi-channel waits.  Buffered and
  unbuffered.  See [Channels](channels.md).
- **Monkey-patched stdlib.**  After `runloom.monkey.patch()`, ordinary
  `socket.recv`, `time.sleep`, `select.select`, `ssl`, `subprocess`,
  `threading.Event`, file I/O, and DNS all yield cooperatively.  See
  [Monkey-patching](monkey-patching.md).
- **Multi-core (3.13t).**  An M:N work-stealing scheduler distributes
  fibers across N OS threads when the GIL is disabled.  See
  [Parallelism](parallelism.md).

## When to use runloom

**Good fit**

- You like `fiber + channel` and don't want to write `async`/`await`
  everywhere.
- You're porting Go code or designing a Go-style service.
- You have existing `async def` code but want sub-microsecond switch
  cost or `select`-style multi-wait without monkey-patching every
  library.
- You want one process running 10 000+ concurrent network connections
  on a single OS thread without callback spaghetti.

**Not a fit**

- You need to interoperate with libraries that already drive an
  asyncio loop and aren't willing to switch (Trio, custom event loops).
- You're CPU-bound on a single fiber -- that's threads, not
  coroutines.  runloom can't preempt inside a long C call (same limitation
  Go has with cgo).
- You need Python 3.10 or older -- runloom requires 3.11+ for the
  per-fiber `PyThreadState` snapshot.

## How it works in 60 seconds

When you call `runloom.fiber(fn)`, the scheduler allocates a new
fiber (a C struct + a private C stack) and puts it on the ready
queue.  `runloom.run(1)` starts the scheduler loop.  Each iteration:

1. Pop the next fiber from the ready FIFO.
2. Switch to its private C stack (one `swap` instruction, ~80 ns on
   x86_64).
3. Run user code until it blocks on I/O, channel, sleep, or explicit
   yield.
4. When the fiber suspends, save its `PyThreadState` (frame chain,
   exception state, datastack chunks, contextvars, recursion counters)
   into the fiber struct, swap back to the scheduler.
5. Loop.

I/O parks the fiber in the netpoll backend (epoll/kqueue/IOCP);
when the fd becomes ready, the fiber returns to the ready FIFO.

The result is that *every concurrent connection costs one stack + one
metadata struct*, and switching between them costs the same as a
function call -- no callbacks, no chains of `await`, no thread context
switches.

## What's next

- New to runloom?  Start with the [Quickstart](quickstart.md).
- Already async?  Read [runloom.aio](asyncio.md).
- Working on a server?  See the [Cookbook](cookbook.md) for worker
  pools, pipelines, and graceful shutdown.
- Memory matters?  See [Stack sizing](stack-sizing.md).
- Curious how deep "real native stacks" goes?  [Research: executing native
  machine code from a fiber](research-native-code.md) (experimental).
