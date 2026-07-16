# M:N parallelism

Runloom has two different scheduler models. One is single-threaded.
It's intended purely for running legacy asyncio code on. The other
is multi-threaded. You don't need to do anything special to choose
the scheduler. When you call runloom.run the first parameter is the
thread number.

runloom.run(1, ...) -- one thread, single-threaded scheduler.

runloom.run(2, ..) -- multi-threaded, work-stealing scheduler.

When its set to >= 2 it gives you the speed up of multiple threads.
That's what this entire project is about. Leveraging multiple threads
in a dynamic, resilient model, to over-come the limitations of blocking
haulting the entire program in a single thread.

## When to use M:N

**Use it when:**

- You want fiber-cheap parallelism without thread-pool ceremony.
- You're running CPU-bound fibers (hashing, parsing, computation)
  and have a free-threaded 3.13t build.

**Skip it when:**

- You're on a GIL build -- the GIL serialises Python execution across
  threads anyway, so M:N gives no speedup.
- All your work is I/O-bound -- a single OS thread with netpoll
  saturates an NIC easily; M:N adds overhead without benefit.

## Performance

Measured on 3.13t (GIL disabled, Linux x86_64, 8 cores):

| Hubs | Wall time | Throughput | Speedup |
| --- | --- | --- | --- |
| 1 | 586 ms | 0.85 M ops/s | 1.00× |
| 2 | 397 ms | 1.26 M ops/s | 1.48× |
| 4 | 268 ms | 1.87 M ops/s | 2.19× |
| 8 | 236 ms | 2.12 M ops/s | **2.50×** |

For comparison: `threading.Thread` × 8 on the same hardware hits
2.24 M ops/s.  runloom matches that within ~5% while keeping the
fiber model (cheap spawn, no per-thread overhead).

## How it works

Each hub thread:

- Owns a Chase-Lev work-stealing deque (`cldeque.c`).
- Pushes new fibers locally; other hubs **steal** from the
  bottom when their own deque is empty.
- Has a per-hub MPSC submission queue for external producers
  (so `mn_fiber` from outside any hub doesn't race the deque owner).
- Routes fibers back to the originating hub on yield/sleep/I/O
  wake -- this preserves locality (the fiber's per-thread cache
  warms one hub, not all of them).

When a hub has no work and no other hub does either, the hub
blocks on a condition variable.  Wakes happen when:

- New `mn_fiber` lands work in the submission queue.
- A wait_fd / sleep / channel op completes.
- Another hub completes a steal that gives them headroom.

## Channels across hubs

Channels work across hubs.  A `Chan` is a synchronised primitive --
producers on hub A and consumers on hub B exchange via the same
channel object:

```python
import runloom

runloom.mn_init(n=4)

ch = runloom.Chan(100)

def producer():
    for i in range(1000):
        ch.send(i)

def consumer():
    total = 0
    for v in ch:
        total += v
    print("consumed:", total)

runloom.mn_fiber(producer)
runloom.mn_fiber(consumer)
runloom.mn_run()
runloom.mn_fini()
```

## Network I/O on M:N

netpoll uses a **single shared** epoll/kqueue/IOCP handle (created once); what is
per-hub is the parker bookkeeping (the per-hub parker pool) and the per-hub
io_uring ring.  Goroutines parked on I/O wake on the hub that submitted the
parking call -- the parker records its origin hub and the pump routes the wake
back there.  This means your accept loop and connection handlers stay on the same
hub by default, which is good for cache locality:

```python
import socket, runloom

runloom.monkey.patch()
runloom.mn_init(n=4)

def handle(conn):
    while True:
        data = conn.recv(4096)
        if not data:
            return
        conn.sendall(data)
    conn.close()

def accept_loop():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 9000))
    srv.listen(128)
    while True:
        conn, _ = srv.accept()
        runloom.mn_fiber(lambda c=conn: handle(c))

runloom.mn_fiber(accept_loop)
runloom.mn_run()
runloom.mn_fini()
```

On a 4-core machine, four concurrent client requests get processed
by four different hub threads simultaneously (subject to scheduling).

## Performance characteristics

- **Spawn**: `mn_fiber` is ~250 ns on 3.13t -- submission to the per-hub
  MPSC queue + work-steal-eligible push.  Comparable to single-thread
  `go`.
- **Yield**: per-hub yield is the same ~80 ns swap.  No cross-thread
  synchronisation on yield since fibers stay on their origin hub.
- **Steal**: ~1 µs to steal from another hub's deque (atomic CAS on
  the deque bottom).  Happens only when the local deque is empty.
- **Wake**: ~3 µs to wake a hub blocked on its CV.

For workloads with strong locality (a fiber that does
all-the-things on one connection), most of the cost stays per-hub
and steals are rare.  For workloads that fan out to many small tasks
(microservice-style), steals are more frequent but the cost is still
dominated by the actual work.

## Pairing with preemption

[Time-sliced preemption](preemption.md) works with M:N -- each hub has
its own preemption timer.  If you've got a fiber that doesn't
yield naturally, preemption applies on whichever hub it's running on
without affecting the others.

```python
runloom.mn_init(n=8)
runloom.preempt_init(quantum_us=10_000)
```

## Caveats

### Free-threaded 3.13t only

`mn_init` raises on GIL builds.  The M:N scheduler relies on
`Py_MOD_GIL_NOT_USED` and CPython's free-threading guarantees about
atomic refcount + GC; on a GIL build you'd get serialisation through
the lock with no concurrency benefit and a small overhead loss.

### Channel + lock contention

A channel shared by all hubs becomes a contention point at very high
throughput.  If your workload has a single channel that every hub
sends to, you'll see scaling fall off.  Mitigations:

- One channel per hub, fan into a final aggregator.
- Use atomic counters or per-hub thread-local accumulators when the
  data doesn't need ordering.

### Goroutine routing back to origin hub

If fiber A on hub 1 parks for I/O, and the I/O wake fires while
hub 1 is busy, A waits for hub 1 to be free -- even if hub 2 is idle.
This preserves locality at the cost of some load balance.  In
practice this evens out under steady load.

## Inspecting hub state

```python
runloom.mn_stats()
# {'hubs': 8,
#  'ready_per_hub': [3, 0, 2, 1, 0, 0, 4, 0], 
#  'completed_per_hub': [12431, 9854, ...],
#  'steals': 47,
#  ...}
```

Useful for tuning hub count or diagnosing load imbalance.
