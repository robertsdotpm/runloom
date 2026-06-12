# Sync API (`runloom.sync`)

`runloom.sync` is the *no-`async`/`await`* facade.  Same scheduler, same
performance, but the user code is plain straight-line Python -- no
coroutines, no event loop ceremony.

This exists because many libraries (and many users) don't want their
public API to be `async def`-coloured.  With `runloom.sync` you get
cooperative concurrency that *looks* like threaded code but actually
runs as a single OS thread of fibers.

## Hello world

```python
import runloom

def main():
    print("hello from a fiber")
    ps.sleep(0.1)
    print("woke up")

ps.run(main)
```

`ps.run(main)` spawns `main` as a fiber, drives the scheduler
until everything's done, and returns.

## Spawning fibers

```python
import runloom

def worker(i):
    ps.sleep(0.01)
    print("worker", i, "done")

def main():
    for i in range(5):
        ps.go(worker, i)         # args + kwargs supported

ps.run(main)
```

`ps.go(fn, *args, **kwargs)` is like `threading.Thread(target=...).start()`
except the "thread" is a fiber -- cheap to create, cooperatively
scheduled.

## Cooperative sleep

```python
ps.sleep(0.5)            # this fiber sleeps; others keep running
```

Outside any fiber (e.g. at module top-level before `run()`),
`ps.sleep` falls back to `time.sleep`.

## Channels

Channels are re-exported as `ps.Chan` and `ps.select`:

```python
import runloom

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

## Concurrency primitives

### Future: one-shot value passing

`Future` is a single-use slot for passing a value between fibers:

```python
import runloom

def main():
    fut = runloom.sync.Future()
    
    def sender():
        runloom.sleep(0.1)
        fut.set_result("hello")          # fiber-only
    
    def receiver():
        result = fut.result(timeout=1.0)  # blocks until result arrives or timeout
        print("got:", result)
    
    runloom.go(sender)
    runloom.go(receiver)

runloom.run(main)
```

Receivers can optionally specify a timeout; `result()` raises `TimeoutError` if
the value doesn't arrive in time.

### JoinSet: structured concurrency

`JoinSet` is a collection of spawned fibers that are all joined before
proceeding (like Go's `sync.WaitGroup` but with built-in result collection):

```python
def main():
    js = runloom.sync.JoinSet()
    
    for i in range(5):
        js.spawn(lambda i=i: i * 10)
    
    results = js.join_all()  # wait for all, return results in spawn order
    print(results)           # [0, 10, 20, 30, 40]

runloom.run(main)
```

Also works as a context manager (auto-joins on exit):

```python
def main():
    with runloom.sync.JoinSet() as js:
        for i in range(5):
            js.spawn(lambda i=i: i * 10)
        # auto-joins on __exit__

runloom.run(main)
```

If any spawned fiber raises an exception, `join_all()` raises the *first*
exception (by spawn order).

### gather: async-style concurrent result collection

`gather(*futures_or_values)` waits for all futures to complete and returns
their results in order (like `asyncio.gather`):

```python
def main():
    f1 = runloom.sync.Future()
    f2 = runloom.sync.Future()
    
    runloom.go(lambda: f1.set_result(10))
    runloom.go(lambda: f2.set_result(20))
    
    results = runloom.sync.gather(f1, f2)
    print(results)  # [10, 20]

runloom.run(main)
```

Non-future values are passed through as-is.

### WaitGroup: fiber barrier

`WaitGroup` waits for a set of fibers to complete:

```python
def main():
    wg = runloom.sync.WaitGroup()
    
    def worker(i):
        runloom.sleep(0.01 * i)
        print("worker", i, "done")
    
    for i in range(5):
        wg.add(1)
        runloom.go(lambda i=i: (worker(i), wg.done()))
    
    wg.wait()  # blocks until all Done() calls
    print("all done")

runloom.run(main)
```

Call `wg.add(N)` to increment the count, `wg.done()` to decrement, and
`wg.wait()` to block until the count reaches zero.

### RWMutex: reader-writer lock

`RWMutex` allows multiple concurrent readers OR a single exclusive writer:

```python
def main():
    mu = runloom.sync.RWMutex()
    data = [0]
    
    def reader(i):
        with mu.rlock():  # shared lock
            print("reader", i, "sees", data[0])
            runloom.sleep(0.01)
    
    def writer(i):
        with mu.lock():   # exclusive lock
            data[0] += 1
            print("writer", i, "set to", data[0])
            runloom.sleep(0.01)
    
    for i in range(3):
        runloom.go(reader, i)
        runloom.go(writer, i)
    
    runloom.sleep(0.2)

runloom.run(main)
```

- `mu.rlock()` / `runlock()` — acquire/release a read lock (shared, multiple allowed)
- `mu.lock()` / `unlock()` — acquire/release a write lock (exclusive)
- Context manager support: `with mu.rlock():` / `with mu.lock():`

### Semaphore: weighted concurrency limit

`Semaphore` limits the number of fibers executing a critical section:

```python
def main():
    sem = runloom.sync.Semaphore(2)  # max 2 concurrent
    
    def worker(i):
        sem.acquire()
        try:
            print("worker", i, "running")
            runloom.sleep(0.1)
        finally:
            sem.release()
    
    for i in range(6):
        runloom.go(worker, i)
    
    runloom.sleep(0.4)

runloom.run(main)
```

Semaphores support weighted permits (default 1):

```python
sem = runloom.sync.Semaphore(10)
sem.acquire(3)   # acquire 3 permits
sem.release(3)
```

Optional timeout on `acquire()`:

```python
ok = sem.acquire(timeout=1.0)  # raises TimeoutError if not acquired
try_ok = sem.try_acquire()     # returns True/False without blocking
```

### Once: run-once initialization

`Once` ensures a function runs exactly once, even under concurrent calls:

```python
def main():
    once = runloom.sync.Once()
    init_called = [0]
    
    def init():
        init_called[0] += 1
        print("initializing...")
        runloom.sleep(0.05)
    
    def worker():
        once.do(init)  # only one fiber runs init, others wait
        print("using initialized state")
    
    for i in range(5):
        runloom.go(worker)
    
    runloom.sleep(0.2)
    print("init was called", init_called[0], "times")  # 1

runloom.run(main)
```

Use `once_value(fn)` to get a result that's computed once and cached:

```python
expensive_result = runloom.sync.once_value(lambda: compute_something())
# First call computes; subsequent calls return the cached result
```

Use `once_func(fn)` to decorate a function for one-time execution:

```python
@runloom.sync.once_func
def setup():
    print("setup")

setup()  # prints "setup"
setup()  # no-op
setup()  # no-op
```

### Group (singleflight): deduplication

`Group` deduplicates concurrent calls to the same function, ensuring only one
call runs and all callers share the result:

```python
def main():
    group = runloom.sync.Group()
    call_count = [0]
    
    def expensive(key):
        call_count[0] += 1
        runloom.sleep(0.05)
        return "result for " + key
    
    def caller(key):
        result = group.do(key, expensive, key)
        print(result)
    
    # All 5 calls with the same key share one execution
    for i in range(5):
        runloom.go(caller, "x")
    
    runloom.sleep(0.2)
    print("expensive was called", call_count[0], "times")  # 1

runloom.run(main)
```

`group.do(key, fn, *args, **kwargs)` runs `fn(*args, **kwargs)` if it's the
first call for `key`; subsequent concurrent calls wait for the result.
Different keys execute independently. Call `group.forget(key)` to allow the
next call to `key` to re-execute.

### Watch: broadcast notifications

`Watch` lets multiple fibers wait for a value to change and be notified:

```python
def main():
    watch = runloom.sync.Watch()
    
    def setter(i):
        runloom.sleep(0.01 * (i + 1))
        watch.notify(i)
        print("notified with", i)
    
    def waiter(name):
        for expected in [0, 1, 2]:
            value = watch.wait_changed(timeout=1.0)  # blocks until value changes
            print(name, "got", value)
    
    runloom.go(setter, 0)
    runloom.go(setter, 1)
    runloom.go(setter, 2)
    for name in ["w1", "w2"]:
        runloom.go(waiter, name)
    
    runloom.sleep(0.2)

runloom.run(main)
```

- `watch.notify(value)` — broadcast a new value to all waiters
- `watch.wait_changed(timeout=None)` — block until the value changes

### Thread safety

All primitives in `runloom.sync` are **fiber-only** (meant for
fiber-to-fiber synchronization). For synchronizing real OS threads
with fibers, use the `runloom.monkey` patched versions (`threading.Lock`,
`threading.Event`, etc.) which detect whether they're called from a fiber
or a foreign thread and adapt accordingly.

## TCP server (straight-line style)

`runloom.sync` ships helper wrappers `tcp_connect` and `tcp_listen` that
return cooperative sockets:

```python
import runloom

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

The socket returned by `tcp_listen` is a `ps.Socket` wrapper -- same
interface as `socket.socket`, but `recv`/`accept`/`sendall` park the
fiber on `wait_fd` instead of blocking the OS thread.

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

For library authors building custom synchronisation, `runloom.sync.wake`
+ `runloom.park_self()` form a lightweight per-task wake:

```python
import runloom

def waiter():
    g = runloom.current_g()
    # ... arrange for someone else to call g.wake() ...
    runloom.park_self()       # blocks until wake arrives
    print("woken")

def main():
    ps.go(waiter)
    # Later, from another fiber: ps.wake(g)  -- or g.wake()
```

This is what `runloom.aio` uses internally as the per-task wake mechanism
in place of a `Chan(1)` per task.  Same idea is available to user code.

## When to use `runloom.sync` vs. `runloom.aio`

**Choose `runloom.sync` when:**

- You're writing new code and want it to *look* synchronous -- easier
  to read, easier to debug, no callback colour.
- You're porting Go code (each `fiber` in Go is a `runloom.sync.go` here).
- You want a library API that doesn't require its callers to be in an
  `async def`.

**Choose `runloom.aio` when:**

- You already have `async def` code and don't want to rewrite it.
- You need a specific asyncio primitive (`asyncio.Queue`, `gather`,
  `wait_for` semantics, etc).
- You want compatibility with an `await`-based codebase you can't
  control.

The two APIs share the same scheduler -- you can mix them, though
each adds a bit of overhead in its own layer.

## A complete example: parallel HTTP fetcher

```python
import runloom

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
independently -- `tcp_connect` and `recv` park the fiber while
others make progress).
