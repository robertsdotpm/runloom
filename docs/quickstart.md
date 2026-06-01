# Quickstart

This page gets you from zero to a running goroutine, then through the
core primitives: channels, sleep, fan-out, and a tiny TCP echo server.

## Your first goroutine

```python
import pygo_core

def hello():
    print("hello from a goroutine!")

pygo_core.go(hello)        # spawn -- doesn't run yet
pygo_core.run()            # drive the scheduler until everyone finishes
```

Output:

```
hello from a goroutine!
```

`go(fn)` queues `fn` for execution on the pygo scheduler.  Nothing
runs until you call `pygo_core.run()` -- that's the scheduler's main
loop, equivalent of Go's program-startup runtime.

## Many goroutines

```python
import pygo_core

def worker(i):
    print("worker", i)

for i in range(10):
    pygo_core.go(lambda i=i: worker(i))   # bind i per-spawn
pygo_core.run()
```

Output (order is scheduler-dependent):

```
worker 0
worker 1
worker 2
...
worker 9
```

The `lambda i=i:` is the standard Python late-binding workaround for
closures over a loop variable.  Each goroutine captures its own `i`.

## Cooperative sleep

`pygo_core.sched_sleep(seconds)` suspends the current goroutine for at
least `seconds`, letting other goroutines run in the meantime.  This is
not `time.sleep` -- `time.sleep` would block the whole OS thread.

```python
import pygo_core, time

def slow():
    print("start", time.time())
    pygo_core.sched_sleep(0.5)
    print("end  ", time.time())

# Spawn three sleeps concurrently; they all wake at ~the same time.
for _ in range(3):
    pygo_core.go(slow)
pygo_core.run()
```

Output:

```
start 1716901234.001
start 1716901234.001
start 1716901234.001
end   1716901234.503
end   1716901234.503
end   1716901234.503
```

All three sleeps ran in parallel because the scheduler parked each
goroutine on a min-heap of wake times.

## Channels

A channel is a typed FIFO that producers `send()` into and consumers
`recv()` from.  Buffered channels hold a fixed number of values;
unbuffered channels rendezvous (the sender blocks until a receiver is
ready, and vice-versa).

```python
import pygo_core

ch = pygo_core.Chan(10)            # buffered, capacity 10

def producer():
    for i in range(5):
        ch.send(i)
    ch.close()

def consumer():
    for v in ch:                   # iterates until ch closes
        print("got", v)

pygo_core.go(producer)
pygo_core.go(consumer)
pygo_core.run()
```

Output:

```
got 0
got 1
got 2
got 3
got 4
```

See [Channels](channels.md) for the full API -- `try_send`/`try_recv`,
`select`, error semantics on closed channels.

## TCP echo server

The simplest realistic program -- a TCP server that echoes whatever
clients send.

```python
import socket
import pygo, pygo.monkey, pygo_core

pygo.monkey.patch()                 # makes socket cooperative

def handle(conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                return
            conn.sendall(data)
    finally:
        conn.close()

def accept_loop():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 9000))
    srv.listen(128)
    while True:
        conn, _ = srv.accept()
        pygo_core.go(lambda c=conn: handle(c))

pygo_core.go(accept_loop)
pygo_core.run()
```

Test it:

```bash
echo "hi" | nc 127.0.0.1 9000
```

The `pygo.monkey.patch()` call swaps `socket.socket`'s methods for
cooperative versions that park on `wait_fd` instead of blocking the OS
thread.  See [Monkey-patching](monkey-patching.md) for the full list.

## Already have `async def` code?

Run it on the pygo scheduler with `pygo.aio`:

```python
import asyncio
import pygo.aio as paio

async def handler(reader, writer):
    line = await reader.readline()
    writer.write(b"echo: " + line)
    await writer.drain()
    writer.close()

async def main():
    server = await paio.start_server(handler, "127.0.0.1", 9000)
    async with server:
        await server.serve_forever()

paio.run(main())
```

`paio.run(coro)` is the equivalent of `asyncio.run(coro)`.  It
installs the pygo event-loop policy, creates a `PygoEventLoop`, and
drives your top-level coroutine.  See [Asyncio bridge](asyncio.md) for
when this wins vs. losing against vanilla asyncio.

## Where to go next

- [Channels](channels.md) -- the deep dive on send/recv/select
- [Monkey-patching](monkey-patching.md) -- making the stdlib cooperative
- [Cookbook](cookbook.md) -- patterns: worker pool, pipeline, fan-in/out
- [Stack sizing](stack-sizing.md) -- keep memory low under load
