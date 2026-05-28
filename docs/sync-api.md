# Sync API (`pygo.sync`)

`pygo.sync` is the *no-`async`/`await`* facade.  Same scheduler, same
performance, but the user code is plain straight-line Python — no
coroutines, no event loop ceremony.

This exists because many libraries (and many users) don't want their
public API to be `async def`-coloured.  With `pygo.sync` you get
cooperative concurrency that *looks* like threaded code but actually
runs as a single OS thread of goroutines.

## Hello world

```python
import pygo.sync as ps

def main():
    print("hello from a goroutine")
    ps.sleep(0.1)
    print("woke up")

ps.run(main)
```

`ps.run(main)` spawns `main` as a goroutine, drives the scheduler
until everything's done, and returns.

## Spawning goroutines

```python
import pygo.sync as ps

def worker(i):
    ps.sleep(0.01)
    print("worker", i, "done")

def main():
    for i in range(5):
        ps.go(worker, i)         # args + kwargs supported

ps.run(main)
```

`ps.go(fn, *args, **kwargs)` is like `threading.Thread(target=...).start()`
except the "thread" is a goroutine — cheap to create, cooperatively
scheduled.

## Cooperative sleep

```python
ps.sleep(0.5)            # this goroutine sleeps; others keep running
```

Outside any goroutine (e.g. at module top-level before `run()`),
`ps.sleep` falls back to `time.sleep`.

## Channels

Channels are re-exported as `ps.Chan` and `ps.select`:

```python
import pygo.sync as ps

def producer(ch):
    for i in range(10):
        ch.send(i)
    ch.close()

def consumer(ch):
    for v in ch:
        print("got", v)

def main():
    ch = ps.Chan(5)
    ps.go(producer, ch)
    ps.go(consumer, ch)

ps.run(main)
```

See [Channels](channels.md) for the full API.

## TCP server (straight-line style)

`pygo.sync` ships helper wrappers `tcp_connect` and `tcp_listen` that
return cooperative sockets:

```python
import pygo.sync as ps

def handle(conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                return
            conn.sendall(data)
    finally:
        conn.close()

def main():
    listener = ps.tcp_listen("127.0.0.1", 9000)
    while True:
        conn, _ = listener.accept()
        ps.go(handle, conn)

ps.run(main)
```

The socket returned by `tcp_listen` is a `ps.Socket` wrapper — same
interface as `socket.socket`, but `recv`/`accept`/`sendall` park the
goroutine on `wait_fd` instead of blocking the OS thread.

### Outbound TCP

```python
def main():
    sock = ps.tcp_connect("example.com", 80)
    sock.sendall(b"GET / HTTP/1.0\r\n\r\n")
    body = sock.recv(65536)
    print(body)
    sock.close()

ps.run(main)
```

### UDP endpoint

```python
def main():
    sock = ps.udp_endpoint(local_addr=("127.0.0.1", 0))
    sock.sendto(b"ping", ("127.0.0.1", 9999))
    data, addr = sock.recvfrom(4096)
    print("from", addr, ":", data)
    sock.close()

ps.run(main)
```

## Park / wake primitive

For library authors building custom synchronisation, `pygo.sync.wake`
+ `pygo_core.park_self()` form a lightweight per-task wake:

```python
import pygo.sync as ps
import pygo_core

def waiter():
    g = pygo_core.current_g()
    # ... arrange for someone else to call g.wake() ...
    pygo_core.park_self()       # blocks until wake arrives
    print("woken")

def main():
    ps.go(waiter)
    # Later, from another goroutine: ps.wake(g)  -- or g.wake()
```

This is what `pygo.aio` uses internally as the per-task wake mechanism
in place of a `Chan(1)` per task.  Same idea is available to user code.

## When to use `pygo.sync` vs. `pygo.aio`

**Choose `pygo.sync` when:**

- You're writing new code and want it to *look* synchronous — easier
  to read, easier to debug, no callback colour.
- You're porting Go code (each `goroutine` in Go is a `pygo.sync.go` here).
- You want a library API that doesn't require its callers to be in an
  `async def`.

**Choose `pygo.aio` when:**

- You already have `async def` code and don't want to rewrite it.
- You need a specific asyncio primitive (`asyncio.Queue`, `gather`,
  `wait_for` semantics, etc).
- You want compatibility with an `await`-based codebase you can't
  control.

The two APIs share the same scheduler — you can mix them, though
each adds a bit of overhead in its own layer.

## A complete example: parallel HTTP fetcher

```python
import pygo.sync as ps

def fetch_one(host, port, ch):
    try:
        sock = ps.tcp_connect(host, port)
        sock.sendall(b"GET / HTTP/1.0\r\nHost: " + host.encode() + b"\r\n\r\n")
        data = b""
        while True:
            chunk = sock.recv(8192)
            if not chunk:
                break
            data += chunk
        sock.close()
        ch.send((host, len(data)))
    except Exception as e:
        ch.send((host, "error: %s" % e))

def main():
    targets = ["example.com", "example.org", "example.net"]
    ch = ps.Chan(len(targets))
    for h in targets:
        ps.go(fetch_one, h, 80, ch)
    for _ in targets:
        host, result = ch.recv()[0]
        print(host, "->", result)

ps.run(main)
```

Straight-line code, no `async`, fully concurrent (each fetch runs
independently — `tcp_connect` and `recv` park the goroutine while
others make progress).
