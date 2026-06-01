# Cookbook

Patterns and complete recipes for common pygo use cases.

## Worker pool

A fixed pool of workers pulling jobs from a channel:

```python
import pygo_core

def worker(jobs, results):
    while True:
        job, ok = jobs.recv()
        if not ok:                  # channel closed -> drain done
            return
        results.send(job * 2)       # whatever the work is

def main():
    N_WORKERS = 8
    N_JOBS = 200

    jobs = pygo_core.Chan(N_JOBS)
    results = pygo_core.Chan(N_JOBS)

    for _ in range(N_WORKERS):
        pygo_core.go(lambda: worker(jobs, results))

    # Feed jobs
    for i in range(N_JOBS):
        jobs.send(i)
    jobs.close()                    # signals workers to stop

    # Collect
    total = 0
    for _ in range(N_JOBS):
        v, _ = results.recv()
        total += v
    print("sum:", total)

pygo_core.go(main)
pygo_core.run()
```

## Pipeline (3-stage)

Stage 1 emits values; stage 2 transforms; stage 3 aggregates.  Each
stage is its own goroutine connected by channels.

```python
import pygo_core

def stage1(out):
    for i in range(100):
        out.send(i)
    out.close()

def stage2(in_, out):
    for v in in_:
        out.send(v * 2)
    out.close()

def stage3(in_, result):
    total = 0
    for v in in_:
        total += v
    result.send(total)

def main():
    a, b = pygo_core.Chan(10), pygo_core.Chan(10)
    result = pygo_core.Chan(1)

    pygo_core.go(lambda: stage1(a))
    pygo_core.go(lambda: stage2(a, b))
    pygo_core.go(lambda: stage3(b, result))

    print(result.recv()[0])

pygo_core.go(main)
pygo_core.run()
```

## Fan-in (many producers, one consumer)

```python
import pygo_core

def producer(id, out):
    for i in range(10):
        out.send((id, i))

def main():
    ch = pygo_core.Chan(100)
    PRODUCERS = 5
    for p in range(PRODUCERS):
        pygo_core.go(lambda p=p: producer(p, ch))

    # Drain everything (sender count × items per sender)
    for _ in range(PRODUCERS * 10):
        prod_id, value = ch.recv()[0]
        print(prod_id, value)

pygo_core.go(main)
pygo_core.run()
```

If producers might close the channel, use `for v in ch`.  If they
finish without closing, count yourself.

## Fan-out (one producer, many consumers)

Multiple consumers pull from the same channel; the runtime picks one
for each value.

```python
import pygo_core

def producer(out):
    for i in range(100):
        out.send(i)
    out.close()

def consumer(id, in_):
    for v in in_:
        print("consumer", id, "got", v)

def main():
    ch = pygo_core.Chan(10)
    pygo_core.go(lambda: producer(ch))
    for c in range(4):
        pygo_core.go(lambda c=c: consumer(c, ch))

pygo_core.go(main)
pygo_core.run()
```

## Cancellation via a "done" channel

Go's idiomatic pattern: pass a `done` channel that callers close to
signal cancellation.

```python
import pygo_core

def worker(done):
    while True:
        idx, _ = pygo_core.select([
            ("recv", done),         # case 0: cancellation
            ("send", out, "work"),  # case 1: emit a value
        ])
        if idx == 0:
            print("cancelled")
            return

def main():
    done = pygo_core.Chan(0)        # unbuffered; close to broadcast
    out = pygo_core.Chan(10)
    pygo_core.go(lambda: worker(done))

    # ... do stuff with out ...
    pygo_core.sched_sleep(0.05)
    done.close()                    # wakes every recv on done

pygo_core.go(main)
pygo_core.run()
```

`select` on a closed `done` channel returns immediately -- `recv` from
a closed channel never blocks.

## Timeouts via `select`

```python
import pygo_core
import threading

def with_timeout(ch, seconds):
    timer = pygo_core.Chan(1)
    def fire():
        pygo_core.sched_sleep(seconds)
        timer.send(None)
    pygo_core.go(fire)

    idx, payload = pygo_core.select([
        ("recv", ch),
        ("recv", timer),
    ])
    if idx == 1:
        return None                 # timed out
    return payload[0]               # got the real value

def main():
    data = pygo_core.Chan(1)
    # Don't send anything to data; the timeout will fire
    print(with_timeout(data, 0.1))  # None

pygo_core.go(main)
pygo_core.run()
```

For asyncio code, just use `asyncio.wait_for(coro, timeout=N)` --
`pygo.aio` handles it natively.

## Graceful shutdown

Pattern: a main goroutine kicks off workers, then waits for a `done`
event (e.g. SIGINT, an admin endpoint, a finite job list).  On
shutdown signal, close the input channel and wait for workers to
finish.

```python
import pygo_core, signal, threading

def worker(jobs, finished):
    for job in jobs:
        # do work
        pass
    finished.send(None)

def main():
    jobs = pygo_core.Chan(100)
    finished = pygo_core.Chan(4)        # one slot per worker

    for _ in range(4):
        pygo_core.go(lambda: worker(jobs, finished))

    shutdown = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())

    try:
        idx = 0
        while not shutdown.is_set():
            jobs.send(idx)
            idx += 1
    finally:
        jobs.close()
        for _ in range(4):
            finished.recv()
        print("clean shutdown")

pygo_core.go(main)
pygo_core.run()
```

## Mixing pygo with `threading`

You can spawn an OS thread that drives its own pygo scheduler:

```python
import threading, pygo_core

def worker_thread():
    def task():
        # ... cooperative work ...
        pass
    for _ in range(100):
        pygo_core.go(task)
    pygo_core.run()

threads = [threading.Thread(target=worker_thread) for _ in range(4)]
for t in threads: t.start()
for t in threads: t.join()
```

Each thread runs its own scheduler with its own goroutines.  Channels
are not shared across thread schedulers in this mode -- that's what
M:N is for.

## Throttling via a semaphore

A buffered channel makes a great semaphore:

```python
import pygo_core

# Allow at most 4 concurrent slow operations
sem = pygo_core.Chan(4)
for _ in range(4):
    sem.try_send(None)              # fill it; tokens

def slow_op():
    sem.recv()                      # acquire (blocks if no token)
    try:
        # ... slow thing ...
        pygo_core.sched_sleep(0.5)
    finally:
        sem.send(None)              # release

for _ in range(20):
    pygo_core.go(slow_op)
pygo_core.run()
```

20 goroutines compete for 4 tokens; at most 4 ever run simultaneously.

## Channel-of-channels

A useful pattern for routing: producers send *channels* through a
"router" channel; consumers receive a channel and read from it.

```python
import pygo_core

def consumer(work):
    chan = work.recv()[0]
    for v in chan:
        print("processed", v)

def producer(work):
    for batch in range(3):
        ch = pygo_core.Chan(10)
        work.send(ch)
        for i in range(10):
            ch.send((batch, i))
        ch.close()
    work.close()

def main():
    work = pygo_core.Chan(1)
    pygo_core.go(lambda: producer(work))
    pygo_core.go(lambda: consumer(work))

pygo_core.go(main)
pygo_core.run()
```

## Echo server with per-connection cancellation

A complete server: each connection gets a goroutine; closing the
listener cancels every active connection cleanly.

```python
import socket, pygo, pygo.monkey, pygo_core

pygo.monkey.patch()

def handle(conn, done):
    try:
        conn.settimeout(0.05)        # so we can poll `done`
        while not done.is_set():
            try:
                data = conn.recv(4096)
            except socket.timeout:
                continue
            if not data:
                return
            conn.sendall(data)
    finally:
        conn.close()

def serve(addr):
    done = threading.Event()
    srv = socket.socket()
    srv.bind(addr); srv.listen(128)
    try:
        while not done.is_set():
            conn, _ = srv.accept()
            pygo_core.go(lambda c=conn: handle(c, done))
    except KeyboardInterrupt:
        done.set()
        srv.close()

import threading
pygo_core.go(lambda: serve(("127.0.0.1", 9000)))
pygo_core.run()
```

## Replacing `threading.Thread` for I/O-bound work

If your code is `Thread(target=fn).start()` and `fn` does I/O,
swapping the thread for a goroutine is a one-line change:

```python
# Before
import threading
t = threading.Thread(target=worker)
t.start()

# After
import pygo_core
g = pygo_core.go(worker)             # plus pygo_core.run() at top level
```

You go from 8 MB per thread (Linux default) to ~16 KB per goroutine.
Spawn rate goes from ~10k/sec to ~1.7M/sec.

## Bridging pygo with `asyncio` libraries

You can call `pygo_core.go(fn)` from inside an async coroutine -- the
goroutine runs concurrently with the awaiting code:

```python
import asyncio, pygo, pygo.aio as paio, pygo_core

def background_worker():
    while True:
        # ... cooperative work ...
        pygo_core.sched_sleep(1.0)

async def main():
    pygo_core.go(background_worker)
    # ... your async code runs in parallel with the background goroutine ...
    await asyncio.sleep(5)

paio.run(main())
```

The async coroutine and the raw goroutine share the same scheduler;
both yield cooperatively.
