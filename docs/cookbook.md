# Cookbook

Patterns and complete recipes for common runloom use cases.

## Worker pool

A fixed pool of workers pulling jobs from a channel:

```python
import runloom_c

def worker(jobs, results):
    while True:
        job, ok = jobs.recv()
        if not ok:                  # channel closed -> drain done
            return
        results.send(job * 2)       # whatever the work is

def main():
    N_WORKERS = 8
    N_JOBS = 200

    jobs = runloom_c.Chan(N_JOBS)
    results = runloom_c.Chan(N_JOBS)

    for _ in range(N_WORKERS):
        runloom_c.go(lambda: worker(jobs, results))

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

runloom_c.go(main)
runloom_c.run()
```

## Pipeline (3-stage)

Stage 1 emits values; stage 2 transforms; stage 3 aggregates.  Each
stage is its own goroutine connected by channels.

```python
import runloom_c

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
    a, b = runloom_c.Chan(10), runloom_c.Chan(10)
    result = runloom_c.Chan(1)

    runloom_c.go(lambda: stage1(a))
    runloom_c.go(lambda: stage2(a, b))
    runloom_c.go(lambda: stage3(b, result))

    print(result.recv()[0])

runloom_c.go(main)
runloom_c.run()
```

## Fan-in (many producers, one consumer)

```python
import runloom_c

def producer(id, out):
    for i in range(10):
        out.send((id, i))

def main():
    ch = runloom_c.Chan(100)
    PRODUCERS = 5
    for p in range(PRODUCERS):
        runloom_c.go(lambda p=p: producer(p, ch))

    # Drain everything (sender count × items per sender)
    for _ in range(PRODUCERS * 10):
        prod_id, value = ch.recv()[0]
        print(prod_id, value)

runloom_c.go(main)
runloom_c.run()
```

If producers might close the channel, use `for v in ch`.  If they
finish without closing, count yourself.

## Fan-out (one producer, many consumers)

Multiple consumers pull from the same channel; the runtime picks one
for each value.

```python
import runloom_c

def producer(out):
    for i in range(100):
        out.send(i)
    out.close()

def consumer(id, in_):
    for v in in_:
        print("consumer", id, "got", v)

def main():
    ch = runloom_c.Chan(10)
    runloom_c.go(lambda: producer(ch))
    for c in range(4):
        runloom_c.go(lambda c=c: consumer(c, ch))

runloom_c.go(main)
runloom_c.run()
```

## Cancellation via a "done" channel

Go's idiomatic pattern: pass a `done` channel that callers close to
signal cancellation.

```python
import runloom_c

def worker(done):
    while True:
        idx, _ = runloom_c.select([
            ("recv", done),         # case 0: cancellation
            ("send", out, "work"),  # case 1: emit a value
        ])
        if idx == 0:
            print("cancelled")
            return

def main():
    done = runloom_c.Chan(0)        # unbuffered; close to broadcast
    out = runloom_c.Chan(10)
    runloom_c.go(lambda: worker(done))

    # ... do stuff with out ...
    runloom_c.sched_sleep(0.05)
    done.close()                    # wakes every recv on done

runloom_c.go(main)
runloom_c.run()
```

`select` on a closed `done` channel returns immediately -- `recv` from
a closed channel never blocks.

## Timeouts via `select`

```python
import runloom_c
import threading

def with_timeout(ch, seconds):
    timer = runloom_c.Chan(1)
    def fire():
        runloom_c.sched_sleep(seconds)
        timer.send(None)
    runloom_c.go(fire)

    idx, payload = runloom_c.select([
        ("recv", ch),
        ("recv", timer),
    ])
    if idx == 1:
        return None                 # timed out
    return payload[0]               # got the real value

def main():
    data = runloom_c.Chan(1)
    # Don't send anything to data; the timeout will fire
    print(with_timeout(data, 0.1))  # None

runloom_c.go(main)
runloom_c.run()
```

For asyncio code, just use `asyncio.wait_for(coro, timeout=N)` --
`runloom.aio` handles it natively.

## Graceful shutdown

Pattern: a main goroutine kicks off workers, then waits for a `done`
event (e.g. SIGINT, an admin endpoint, a finite job list).  On
shutdown signal, close the input channel and wait for workers to
finish.

```python
import runloom_c, signal, threading

def worker(jobs, finished):
    for job in jobs:
        # do work
        pass
    finished.send(None)

def main():
    jobs = runloom_c.Chan(100)
    finished = runloom_c.Chan(4)        # one slot per worker

    for _ in range(4):
        runloom_c.go(lambda: worker(jobs, finished))

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

runloom_c.go(main)
runloom_c.run()
```

## Mixing runloom with `threading`

You can spawn an OS thread that drives its own runloom scheduler:

```python
import threading, runloom_c

def worker_thread():
    def task():
        # ... cooperative work ...
        pass
    for _ in range(100):
        runloom_c.go(task)
    runloom_c.run()

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
import runloom_c

# Allow at most 4 concurrent slow operations
sem = runloom_c.Chan(4)
for _ in range(4):
    sem.try_send(None)              # fill it; tokens

def slow_op():
    sem.recv()                      # acquire (blocks if no token)
    try:
        # ... slow thing ...
        runloom_c.sched_sleep(0.5)
    finally:
        sem.send(None)              # release

for _ in range(20):
    runloom_c.go(slow_op)
runloom_c.run()
```

20 goroutines compete for 4 tokens; at most 4 ever run simultaneously.

## Channel-of-channels

A useful pattern for routing: producers send *channels* through a
"router" channel; consumers receive a channel and read from it.

```python
import runloom_c

def consumer(work):
    chan = work.recv()[0]
    for v in chan:
        print("processed", v)

def producer(work):
    for batch in range(3):
        ch = runloom_c.Chan(10)
        work.send(ch)
        for i in range(10):
            ch.send((batch, i))
        ch.close()
    work.close()

def main():
    work = runloom_c.Chan(1)
    runloom_c.go(lambda: producer(work))
    runloom_c.go(lambda: consumer(work))

runloom_c.go(main)
runloom_c.run()
```

## Echo server with per-connection cancellation

A complete server: each connection gets a goroutine; closing the
listener cancels every active connection cleanly.

```python
import socket, runloom, runloom.monkey, runloom_c

runloom.monkey.patch()

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
            runloom_c.go(lambda c=conn: handle(c, done))
    except KeyboardInterrupt:
        done.set()
        srv.close()

import threading
runloom_c.go(lambda: serve(("127.0.0.1", 9000)))
runloom_c.run()
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
import runloom_c
g = runloom_c.go(worker)             # plus runloom_c.run() at top level
```

You go from 8 MB per thread (Linux default) to ~16 KB per goroutine.
Spawn rate goes from ~10k/sec to ~1.7M/sec.

## Bridging runloom with `asyncio` libraries

You can call `runloom_c.go(fn)` from inside an async coroutine -- the
goroutine runs concurrently with the awaiting code:

```python
import asyncio, runloom, runloom.aio as paio, runloom_c

def background_worker():
    while True:
        # ... cooperative work ...
        runloom_c.sched_sleep(1.0)

async def main():
    runloom_c.go(background_worker)
    # ... your async code runs in parallel with the background goroutine ...
    await asyncio.sleep(5)

paio.run(main())
```

The async coroutine and the raw goroutine share the same scheduler;
both yield cooperatively.
