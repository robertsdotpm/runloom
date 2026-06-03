# Time-sliced preemption

By default, runloom goroutines are **cooperative** -- they yield only when
they explicitly call `sched_yield`, sleep, or block on I/O.  If you
write a tight CPU loop with no yield, that goroutine monopolises the
scheduler until it returns.

This works for Go-style code (which conventionally has yields
sprinkled through it via channel operations and I/O) but is brittle
when you mix in libraries that don't expect to be cooperative -- a
long `numpy` matmul or a 10-million-iteration arithmetic loop will
starve every other goroutine.

`runloom.preempt_init(quantum_us=10_000)` solves this on
**free-threaded Python 3.13t** (the GIL-disabled build).  A timer
thread posts a `Py_AddPendingCall` every quantum; CPython's
`eval_breaker` check -- already done between bytecodes -- invokes our
callback, which calls `runloom_sched_yield()` on the running goroutine.

## Hello, preempted goroutine

```python
import runloom

runloom.preempt_init(quantum_us=10_000)    # 10 ms slices

def hog():
    total = 0
    for i in range(100_000_000):
        total += i * i
    print("hog done", total)

def chatty():
    for i in range(50):
        print("chatty tick", i)
        runloom.sched_sleep(0.01)

runloom.go(hog)
runloom.go(chatty)
runloom.run(1)
```

Without `preempt_init`, `chatty` wouldn't get any time until `hog`
finishes.  With it, `chatty` interleaves smoothly because the timer
forces `hog` to yield every 10 ms.

## What's the cost?

The hot path (between yields) pays nothing -- preemption only adds
work when the quantum fires:

- ~300 ns per quantum to dispatch the pending call.
- One runloom yield (~80 ns asm + snap/load).

At 100 Hz (the default 10 ms quantum), that's ~30 µs of overhead per
real-time second.  ≈ 0.003%.

## How CPython makes this possible

Every bytecode dispatch in CPython's eval loop checks `eval_breaker`
(an atomic flag that signals pending work like signals or pending
calls).  `Py_AddPendingCall` sets the flag; on the very next bytecode
boundary, CPython runs the queued function.

We exploit this by:

1. Starting a timer thread on `preempt_init`.
2. Every `quantum_us` microseconds, the timer thread calls
   `Py_AddPendingCall(yield_cb)`.
3. `yield_cb` checks if any goroutine is currently running on this
   thread and, if so, calls `runloom_sched_yield()` to swap it out.

The goroutine resumes the next time it's at the head of the ready
queue -- typically immediately after every other ready goroutine has
had a slice.

## Caveats

### Bytecode boundaries only

The `eval_breaker` check happens between Python bytecodes.  If a
goroutine is sitting inside a long **C call** (e.g. `numpy.dot` on a
huge matrix, `hashlib.sha256` on a multi-MB blob, a blocking system
call), the check doesn't fire -- Python isn't running.  Preemption
will hit as soon as the C call returns.

This is the same limitation Go has with cgo: while you're in C, the
scheduler can't preempt you.  Most stdlib functions release frequently
enough that this isn't noticeable in practice.

### 3.13t only

`preempt_init` raises `RuntimeError` on GIL builds.  The preemption
path relies on the M:N hub model and `Py_AddPendingCall` having a
fast path that's safe across hubs -- both of which are part of the
3.13t support that doesn't exist on earlier or non-free-threaded
Pythons.

If you really want time-slicing on a GIL build, the workaround is to
sprinkle `runloom.sched_yield_classic()` calls into your hot loops.
Crude but works.

### Per-thread, not per-process

`preempt_init` configures preemption for the calling OS thread's
scheduler.  Under the M:N hub model, each hub thread runs its own
scheduler; preemption needs to be initialised on each.  The
`mn_init`/`mn_go` path handles this automatically.

## Stopping preemption

```python
runloom.preempt_fini()
```

Idempotent.  Joins the timer thread.  Use this if you're toggling
preemption on/off for benchmarks -- most production code will just
leave it on after `preempt_init`.

## Choosing a quantum

- **10 ms (10 000 µs)** -- fair scheduling for typical mixed workloads,
  ~0.003% overhead.  This is the default.
- **1 ms (1 000 µs)** -- much finer-grained interleaving, ~0.03%
  overhead.  Use if you've got tight latency requirements (e.g. a
  game-loop-style update with strict frame timing).
- **100 ms** -- coarser, less responsive but lighter on the timer
  thread.  Use if you're CPU-bound and don't have latency-sensitive
  goroutines.

```python
runloom.preempt_init(quantum_us=1_000)
```

## When to use preemption

**Use it when:**

- You have mixed workloads (CPU-bound + I/O-bound) on the same
  scheduler.
- You can't audit every code path for yield points.
- You're running third-party code that might be greedy.

**Skip it when:**

- All your goroutines have natural yield points (channels, I/O,
  sleeps) and you're confident none monopolise the CPU.
- You're benchmarking the cooperative baseline and don't want the
  jitter from quantum-driven yields.
- You're on a GIL build (it'll raise).

The default for runloom is *no preemption*, which matches Go's behaviour
pre-1.14.  Opt into preemption when you actually need it.

## Combining with M:N

If you've called `mn_init(8)` to run 8 hub threads, preemption
applies per-hub.  Each hub's currently-running goroutine gets
preempted independently.  Two CPU-bound goroutines on different hubs
will both make progress without needing to yield to each other
(they're on different OS threads); preemption keeps any single hub
from being monopolised by one greedy goroutine.

See [Parallelism](parallelism.md) for the M:N model.
