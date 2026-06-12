# Asyncio bridge (`runloom.aio`)

`runloom.aio` lets you run existing `async def` code on the runloom
scheduler.  It implements the asyncio event-loop protocol on top of
fibers: every `asyncio.Task` becomes a runloom fiber driving the
coroutine, and `await fut` parks the fiber on a per-task wake
primitive.

## When to use this

**Use `runloom.aio` when:**

- You already have `async def` code and don't want to rewrite it
  Go-style.
- You want sub-microsecond context switches (~80 ns vs. asyncio's
  ~1800 ns).
- You're chaining many `await` operations per task (the runloom win
  amortises over awaits).
- You want to mix monkey-patched sync code (cooperative `socket.recv`,
  `time.sleep`) with `async def` in the same process -- runloom's
  scheduler drives both.

**Stick with vanilla asyncio when:**

- Your workload is "one `await` per task and dispatch" -- asyncio's
  tight C-deque dispatcher beats runloom's RunloomTask setup cost in that
  shape (a 5× slowdown was measured on a fan-out microbench).
- You depend on asyncio internals beyond what runloom implements
  (debug hooks, custom selectors, low-level transport flags).

Measured performance on Python 3.12 (Linux):

| Workload | Result |
| --- | --- |
| Multi-await chains (100 tasks × 100 awaits each) | **~1.9× faster** than asyncio |
| Deep recursive awaits (n=100, d=20) | **~1.7× faster** |
| Simple fan-out (10 000 tasks, one sleep each) | **~5× slower** |

## Hello world

```python
import asyncio
import runloom

async def main():
    print("hello from", asyncio.current_task())
    await asyncio.sleep(0.01)
    print("woke up")

runloom.aio.run(main())
```

`runloom.aio.run(coro)` is the equivalent of `asyncio.run(coro)`:

1. Installs the runloom event-loop policy (`RunloomEventLoopPolicy`).
2. Creates a `RunloomEventLoop`.
3. Runs your top-level coroutine to completion.
4. Cancels any pending tasks and drains the scheduler before returning.

## Concurrency: `gather`, `create_task`

Standard asyncio idioms work:

```python
import asyncio, runloom

async def worker(i):
    await asyncio.sleep(0.001)
    return i * 2

async def main():
    results = await asyncio.gather(*(worker(i) for i in range(10)))
    return results

print(runloom.aio.run(main()))   # [0, 2, 4, ..., 18]
```

`asyncio.create_task(coro)` schedules a task and returns immediately;
the task runs concurrently with the awaiter:

```python
async def main():
    t = asyncio.create_task(worker(7))
    # ... do other work ...
    return await t
```

## TCP server with streams

`runloom.aio` provides `open_connection` and `start_server` that return
`StreamReader`/`StreamWriter` objects compatible with `asyncio.streams`:

```python
import asyncio, runloom

async def handler(reader, writer):
    line = await reader.readline()
    print("client said:", line)
    writer.write(b"echo: " + line)
    await writer.drain()
    writer.close()

async def main():
    server = await runloom.aio.start_server(handler, "127.0.0.1", 9000)
    async with server:
        await server.serve_forever()

runloom.aio.run(main())
```

The server runs at full speed using runloom's netpoll (epoll on Linux,
kqueue on BSD/macOS, WSAPoll/IOCP on Windows).  Per-connection
overhead is one fiber -- by default 16 KB of stack after
[calibration](stack-sizing.md).

### Client

```python
import asyncio, runloom

async def main():
    reader, writer = await runloom.aio.open_connection("127.0.0.1", 9000)
    writer.write(b"hi\n")
    await writer.drain()
    response = await reader.readline()
    print(response)
    writer.close()

runloom.aio.run(main())
```

## Concurrent clients (the fan-out asyncio is supposed to be good at)

```python
import asyncio, runloom

async def fetch_one(host, port):
    r, w = await runloom.aio.open_connection(host, port)
    w.write(b"GET / HTTP/1.0\r\n\r\n")
    await w.drain()
    body = await r.read()
    w.close()
    return len(body)

async def main():
    targets = [("example.com", 80)] * 100
    sizes = await asyncio.gather(*(fetch_one(h, p) for h, p in targets))
    return sum(sizes)

print(runloom.aio.run(main()))
```

100 concurrent TCP connections, each parking on netpoll while the
others run.  No threads, no callbacks.

## Locks, Events, Queues, Conditions

The asyncio synchronisation primitives work as-is -- `runloom.aio` doesn't
reimplement them; they're driven via `Future` and `call_soon`, which
runloom's loop implements.

```python
import asyncio, runloom

async def waiter(lock, name):
    async with lock:
        print(name, "holds the lock")
        await asyncio.sleep(0.01)

async def main():
    lock = asyncio.Lock()
    await asyncio.gather(
        waiter(lock, "A"),
        waiter(lock, "B"),
        waiter(lock, "C"),
    )

runloom.aio.run(main())
```

Output (always sequential):

```
A holds the lock
B holds the lock
C holds the lock
```

`asyncio.Event`, `asyncio.Queue`, `asyncio.Condition`, `asyncio.Semaphore`
all behave the same way.

## `wait_for`, `shield`, cancellation

```python
import asyncio, runloom

async def slow():
    await asyncio.sleep(60.0)

async def main():
    try:
        await asyncio.wait_for(slow(), timeout=0.05)
    except asyncio.TimeoutError:
        print("timed out")

runloom.aio.run(main())
```

`shield(coro)` works as in stdlib asyncio -- a cancellation on the
shielded awaitable doesn't propagate to the underlying coroutine.

## `loop.add_reader` / `add_writer`

Low-level fd readiness callbacks work, driven by runloom's netpoll:

```python
import asyncio, os, runloom

async def main():
    loop = asyncio.get_running_loop()
    r, w = os.pipe()

    def on_read():
        data = os.read(r, 1024)
        print("got:", data)
        loop.remove_reader(r)

    loop.add_reader(r, on_read)
    os.write(w, b"hello\n")
    await asyncio.sleep(0.01)
    os.close(r); os.close(w)

runloom.aio.run(main())
```

These are level-triggered (like asyncio's default selector loop) just
driven by runloom's netpoll.

## Datagram (UDP) endpoints

`loop.create_datagram_endpoint(...)` returns a `(transport, protocol)`
pair with the standard asyncio protocol callbacks:

```python
import asyncio, runloom

class EchoProto(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
    def datagram_received(self, data, addr):
        self.transport.sendto(data, addr)

async def main():
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        EchoProto, local_addr=("127.0.0.1", 9999)
    )
    await asyncio.sleep(10)
    transport.close()

runloom.aio.run(main())
```

## Compatibility status

| Feature | Status |
| --- | --- |
| `asyncio.run`, `gather`, `create_task`, `wait_for`, `shield` | works |
| `asyncio.sleep`, `Lock`, `Event`, `Queue`, `Semaphore`, `Condition` | works |
| `asyncio.Future`, `add_done_callback`, `remove_done_callback` | works |
| `loop.add_reader`/`add_writer`, `sock_*` methods | works |
| `loop.run_in_executor` | works |
| `open_connection`/`start_server` (StreamReader/Writer) | works |
| `loop.create_connection`/`create_server` (Transport+Protocol) | works |
| `loop.create_datagram_endpoint` (UDP) | works |
| SSL (`ssl=` keyword on `create_connection`/`create_server`) | works -- cooperative `SSLSocket` (client + server, ALPN, cert fingerprint) |
| `loop.subprocess_*` | not implemented |
| `signal.set_wakeup_fd` integration | not implemented |

If a missing feature is blocking you, file an issue.  Most asyncio
extension points are mechanical to add given the existing scheduler
primitives.

## Performance tips

### Use `gather` over many `await`s

```python
# Slow -- 1000 sequential awaits = 1000 context switches
total = 0
for i in range(1000):
    total += await worker(i)

# Fast -- 1000 concurrent fibers
results = await asyncio.gather(*(worker(i) for i in range(1000)))
total = sum(results)
```

### Avoid making a new task for trivial work

A `RunloomTask` allocates a 16 KB fiber stack.  For something that's
basically "return a value", just call the function:

```python
# Wasteful
result = await asyncio.create_task(trivial())

# Better
result = await trivial()
```

### Mix in monkey-patched blocking I/O

```python
import runloom
runloom.monkey.patch()    # makes socket / time / ssl cooperative

async def main():
    # This blocks the fiber, not the OS thread:
    response = requests.get("http://example.com").content
    return len(response)

runloom.aio.run(main())
```

This lets you use libraries that don't support `async` -- `requests`,
`pymysql`, plain stdlib `urllib` -- without spawning threads.

## How it compares to vanilla asyncio internally

| | vanilla asyncio | `runloom.aio` |
| --- | --- | --- |
| Task storage | callback chains in `_callbacks` lists | per-task fiber + 1-call-deep stack |
| Context switch | `loop._run_once` + `selector.select` | C `swap` instruction |
| `await fut` | adds callback, returns control to loop | parks fiber on per-task wake |
| Per-task memory | ~5 KB (interpreter frame + Task object) | ~16 KB (stack) + ~250 B (G + Task) |
| Switch cost | ~1800 ns | ~80 ns |

The trade is: runloom costs more memory per task but switches between
tasks ~22× faster.  Workloads with many switches per task amortise that;
workloads with one switch per task pay the memory cost without
collecting the speed benefit.

## Known semantic differences from asyncio

`runloom.aio` is a high-fidelity bridge for real-world async code -- it runs
aiohttp, uvicorn, starlette, hypercorn, websockets, anyio and friends -- but it
is **not** a bit-exact emulator of asyncio's *scheduler semantics*.  Because a
task is a stackful fiber ordered by runloom's M:N scheduler (not a callback on
a single FIFO ready-queue driven by `loop._run_once`), a thin slice of code that
depends on asyncio's exact callback/timer ordering can observe a difference.

For the overwhelming majority of projects there is **no practical difference**
(the frameworks above all pass).  The differences below only surface in code
that depends on *when* a callback fires relative to other work in the same loop
iteration, or that drives timers by mocking the clock, or that runs several
loops on one OS thread.  When they do bite, they bite *loudly* (a failing test
or a hang), never as silent data corruption.

### 1. Two internal done-callbacks fire synchronously; everything else defers

> **Note (corrected):** an earlier version of this doc said runloom "fires most
> callbacks inline."  That is **no longer true** — the bridge was changed to
> defer (to fix the falcon / uvicorn / aiojobs ordering bugs), so the live
> behaviour matches asyncio's ordering far more closely than the old text
> implied.  See `_fire_callbacks` in `src/runloom/aio/futures.py`.

asyncio schedules every future done-callback through `loop.call_soon`, so the
code that completed the future finishes its synchronous run *before* any callback
fires.  runloom now does the same — `_fire_callbacks` **defers every done-callback
through `loop.call_soon`** to preserve asyncio's order — with exactly two
synchronous exceptions: (a) `RunloomTask._wake_unpark`, runloom's own await-wake
primitive (firing it inline only readies the fiber, which stays FIFO-after an
already-readied waiter, so ordering holds; deferring it would spawn a fiber
per `await`); and (b) callbacks tagged `_runloom_fire_sync` (the run loop's own
`_stop_on_done` control hook).  Stock `asyncio.Task` wakeups are **deferred**
through a trampoline.  An `add_done_callback` on an already-done future is always
scheduled via `call_soon`, never inline.

So the residual difference from asyncio is narrow: only those two runloom-internal
callbacks fire in the same turn rather than the next tick.  It can affect you only
if their effect is ordering-sensitive relative to the completer's continued
synchronous code *with no `await` in between* — and normal code `await`s, so the
distinction washes out.

### 2. Timers are real wall-clock, on a per-OS-thread scheduler

asyncio keeps a per-loop timer heap and fires timers by comparing `loop.time()`
inside `_run_once`.  runloom schedules `call_later`/`call_at`/`asyncio.sleep` as
real wall-clock fiber sleeps on the scheduler shared by every loop on that
OS thread.  Two consequences:

- **`loop.time()` is not consulted for firing.**  Mocking `loop.time()` to
  fast-forward a timer (a common asyncio *test* trick) does not advance runloom's
  timers -- they fire on real elapsed time.  Drive such tests with a real (short)
  duration instead of a mocked clock.
- **Timers are not isolated per loop on the same thread.**  If you run two event
  loops on one OS thread, a timer scheduled under loop A keeps counting while
  loop B is being driven, and can fire during B's run.  asyncio would never fire
  A's timer while only B is running.  (`call_at(when, ...)` *does* store `when`
  verbatim on the returned handle, matching asyncio; only the *firing* mechanism
  differs.)

### 3. Wake / callback ordering is the scheduler's, not a single FIFO

asyncio runs coroutine-steps, `call_soon` callbacks and task wakeups as entries
in one strict-FIFO ready deque, one batch per iteration.  runloom runs them as
fibers ordered by the M:N scheduler (ready-ring + work-stealing deque +
wake-state machine).  The *set* of work that runs is the same; the exact
interleaving between a just-woken task and a freshly scheduled callback can
differ.  Again: only code that pins on that sub-iteration ordering notices.

### Is a failing test a real problem or an over-specified test?

The useful question when a library's test fails under runloom: **does it assert an
observable behavioral guarantee, or does it assume an implementation
mechanism?**

- *Assumes a mechanism* -- mocks `loop.time()`, relies on an exact `sleep`
  duration, or runs multiple loops per thread.  Usually adaptable with light
  effort (use real time; close throwaway coroutines instead of awaiting them);
  arguably the test over-specifies asyncio internals.
- *Asserts observable behavior* -- e.g. "this callback must not see state X
  before `close()` runs."  Not adaptable without changing intent; it's flagging
  a genuine (if narrow) fidelity gap worth taking seriously.

If asyncio-exact scheduler semantics ever become a hard requirement for your use
case, that's a deeper change to the bridge's ready-queue/timer model (and a
trade against the performance the fiber model buys) -- open an issue.
