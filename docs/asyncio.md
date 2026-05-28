# Asyncio bridge (`pygo.aio`)

`pygo.aio` lets you run existing `async def` code on the pygo
scheduler.  It implements the asyncio event-loop protocol on top of
goroutines: every `asyncio.Task` becomes a pygo goroutine driving the
coroutine, and `await fut` parks the goroutine on a per-task wake
primitive.

## When to use this

**Use `pygo.aio` when:**

- You already have `async def` code and don't want to rewrite it
  Go-style.
- You want sub-microsecond context switches (~80 ns vs. asyncio's
  ~1800 ns).
- You're chaining many `await` operations per task (the pygo win
  amortises over awaits).
- You want to mix monkey-patched sync code (cooperative `socket.recv`,
  `time.sleep`) with `async def` in the same process — pygo's
  scheduler drives both.

**Stick with vanilla asyncio when:**

- Your workload is "one `await` per task and dispatch" — asyncio's
  tight C-deque dispatcher beats pygo's PygoTask setup cost in that
  shape (a 5× slowdown was measured on a fan-out microbench).
- You depend on asyncio internals beyond what pygo implements
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
import pygo.aio as paio

async def main():
    print("hello from", asyncio.current_task())
    await asyncio.sleep(0.01)
    print("woke up")

paio.run(main())
```

`paio.run(coro)` is the equivalent of `asyncio.run(coro)`:

1. Installs the pygo event-loop policy (`PygoEventLoopPolicy`).
2. Creates a `PygoEventLoop`.
3. Runs your top-level coroutine to completion.
4. Cancels any pending tasks and drains the scheduler before returning.

## Concurrency: `gather`, `create_task`

Standard asyncio idioms work:

```python
import asyncio, pygo.aio as paio

async def worker(i):
    await asyncio.sleep(0.001)
    return i * 2

async def main():
    results = await asyncio.gather(*(worker(i) for i in range(10)))
    return results

print(paio.run(main()))   # [0, 2, 4, ..., 18]
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

`pygo.aio` provides `open_connection` and `start_server` that return
`StreamReader`/`StreamWriter` objects compatible with `asyncio.streams`:

```python
import asyncio, pygo.aio as paio

async def handler(reader, writer):
    line = await reader.readline()
    print("client said:", line)
    writer.write(b"echo: " + line)
    await writer.drain()
    writer.close()

async def main():
    server = await paio.start_server(handler, "127.0.0.1", 9000)
    async with server:
        await server.serve_forever()

paio.run(main())
```

The server runs at full speed using pygo's netpoll (epoll on Linux,
kqueue on BSD/macOS, WSAPoll/IOCP on Windows).  Per-connection
overhead is one goroutine — by default 16 KB of stack after
[calibration](stack-sizing.md).

### Client

```python
import asyncio, pygo.aio as paio

async def main():
    reader, writer = await paio.open_connection("127.0.0.1", 9000)
    writer.write(b"hi\n")
    await writer.drain()
    response = await reader.readline()
    print(response)
    writer.close()

paio.run(main())
```

## Concurrent clients (the fan-out asyncio is supposed to be good at)

```python
import asyncio, pygo.aio as paio

async def fetch_one(host, port):
    r, w = await paio.open_connection(host, port)
    w.write(b"GET / HTTP/1.0\r\n\r\n")
    await w.drain()
    body = await r.read()
    w.close()
    return len(body)

async def main():
    targets = [("example.com", 80)] * 100
    sizes = await asyncio.gather(*(fetch_one(h, p) for h, p in targets))
    return sum(sizes)

print(paio.run(main()))
```

100 concurrent TCP connections, each parking on netpoll while the
others run.  No threads, no callbacks.

## Locks, Events, Queues, Conditions

The asyncio synchronisation primitives work as-is — `pygo.aio` doesn't
reimplement them; they're driven via `Future` and `call_soon`, which
pygo's loop implements.

```python
import asyncio, pygo.aio as paio

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

paio.run(main())
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
import asyncio, pygo.aio as paio

async def slow():
    await asyncio.sleep(60.0)

async def main():
    try:
        await asyncio.wait_for(slow(), timeout=0.05)
    except asyncio.TimeoutError:
        print("timed out")

paio.run(main())
```

`shield(coro)` works as in stdlib asyncio — a cancellation on the
shielded awaitable doesn't propagate to the underlying coroutine.

## `loop.add_reader` / `add_writer`

Low-level fd readiness callbacks work, driven by pygo's netpoll:

```python
import asyncio, os, pygo.aio as paio

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

paio.run(main())
```

These are level-triggered (like asyncio's default selector loop) just
driven by pygo's netpoll.

## Datagram (UDP) endpoints

`loop.create_datagram_endpoint(...)` returns a `(transport, protocol)`
pair with the standard asyncio protocol callbacks:

```python
import asyncio, pygo.aio as paio

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

paio.run(main())
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
| SSL (`ssl=` keyword) | not implemented — use blocking-style SSL via monkey-patch |
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

# Fast -- 1000 concurrent goroutines
results = await asyncio.gather(*(worker(i) for i in range(1000)))
total = sum(results)
```

### Avoid making a new task for trivial work

A `PygoTask` allocates a 16 KB goroutine stack.  For something that's
basically "return a value", just call the function:

```python
# Wasteful
result = await asyncio.create_task(trivial())

# Better
result = await trivial()
```

### Mix in monkey-patched blocking I/O

```python
import pygo.monkey, pygo.aio as paio
pygo.monkey.patch()    # makes socket / time / ssl cooperative

async def main():
    # This blocks the goroutine, not the OS thread:
    response = requests.get("http://example.com").content
    return len(response)

paio.run(main())
```

This lets you use libraries that don't support `async` — `requests`,
`pymysql`, plain stdlib `urllib` — without spawning threads.

## How it compares to vanilla asyncio internally

| | vanilla asyncio | `pygo.aio` |
| --- | --- | --- |
| Task storage | callback chains in `_callbacks` lists | per-task goroutine + 1-call-deep stack |
| Context switch | `loop._run_once` + `selector.select` | C `swap` instruction |
| `await fut` | adds callback, returns control to loop | parks goroutine on per-task wake |
| Per-task memory | ~5 KB (interpreter frame + Task object) | ~16 KB (stack) + ~250 B (G + Task) |
| Switch cost | ~1800 ns | ~80 ns |

The trade is: pygo costs more memory per task but switches between
tasks ~22× faster.  Workloads with many switches per task amortise that;
workloads with one switch per task pay the memory cost without
collecting the speed benefit.
