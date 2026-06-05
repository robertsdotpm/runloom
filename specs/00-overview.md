# 00 — Overview: what runloom is and the shape of its design

## The problem

CPython gives you three ways to do concurrency and none of them is what Go has:

- **asyncio** — cheap tasks, but *colored* (`async`/`await` everywhere) and
  single-core (one event loop, one thread). One unanticipated blocking call
  freezes every task.
- **threads** — blocking-style code, but a real OS thread per task (MB stacks,
  kernel scheduling) caps you at thousands, and the GIL serializes them anyway.
- **processes** — true parallelism, but heavyweight and isolated.

Go's goroutines are the missing fourth quadrant: **blocking-style code, a
million of them, across every core, cheap to spawn.** runloom builds that for
Python — specifically for **free-threaded CPython 3.13t** (the GIL-off build),
where multi-core Python is finally possible.

The pitch: write ordinary blocking code — `go(fn)`, `conn.recv()`, `ch.send()`,
no `async`/`await` — and run a million of them in one process across all cores.

## What it is, in one sentence

> A hand-rolled **M:N work-stealing scheduler** (M goroutines on N OS-thread
> hubs) written in C, where each goroutine is a **stackful coroutine** (its own
> C stack, switched by an assembly `swap`) carrying a **snapshot of CPython's
> per-thread execution state**, with a **netpoll** layer that parks goroutines
> on fd readiness — plus Python front-ends (`sync`, `monkey`, `aio`) that make
> existing code run on it unchanged.

## The M:N:G model (borrowed from Go's runtime)

- **G — goroutine.** A unit of work: a C stack + a CPython tstate snapshot +
  scheduling bookkeeping (`struct runloom_g`). Refcounted.
- **Hub (Go's M+P fused).** One OS thread running one scheduler instance: a
  work-stealing deque of fresh goroutines, a local FIFO of yielded ones, a sleep
  heap, and a netpoll. On a free-threaded build, N hubs run Python *in parallel*.
- **The single-thread scheduler is the degenerate case N=1** — same data
  structures, one thread, no stealing. This is the GIL-build / compatibility
  path and what the asyncio bridge uses.

A goroutine is **created on a hub and pinned to it once it has run**, because a
suspended coroutine's C stack holds absolute pointers tied to one OS thread (and
its CPython eval frame caches that thread's tstate). Work-stealing therefore
steals only **fresh, never-run** goroutines (no live stack yet); woken/yielded
goroutines route **back to their origin hub**. This trades some load balance for
the ability to exist at all — see spec 05.

## The layer cake

```
  user code
  ┌─────────────┬──────────────┬───────────────────────────┐
  │ runloom.sync │ runloom.aio  │ runloom.monkey (patch())  │   Python front-ends
  │ go/Chan/sock │ async/await  │ cooperative stdlib        │
  └─────────────┴──────────────┴───────────────────────────┘
  ┌──────────────────────────────────────────────────────────┐
  │ runloom (runtime.py): go() / run() / sleep() / blocking() │   thin public API
  └──────────────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────────────┐
  │ runloom_c  (the C extension)                              │
  │   scheduler (single + M:N) · channels · netpoll · TCPConn │   the engine
  │   stackful coro + asm swap · blockpool · crash · introspect│
  └──────────────────────────────────────────────────────────┘
```

Everything above the C line is *optional sugar over the same goroutines*. The
three front-ends share one scheduler and can be mixed in one process:

- **`runloom.sync`** — Go-style straight-line code: `go(fn)`, `Chan`, `select`,
  cooperative sockets. No async coloring. (spec 12)
- **`runloom.monkey`** — `patch()` replaces blocking stdlib leaf calls with
  cooperative ones, so `requests`/`pymysql`/`urllib` run unchanged. (spec 14)
- **`runloom.aio`** — an asyncio event-loop implementation on top of goroutines,
  so existing `async def` code runs on runloom's scheduler. (spec 13)

## What you actually get, and what you don't

The honest trade-offs (these belong in the spec because they shaped the design):

- **runloom does not make Python faster per core.** ~80 K pure-Python ops/s/core
  is a CPython constant. What runloom buys is *hitting that on every core at once
  from one process, with blocking-style code* — which asyncio structurally
  cannot do. The scheduler itself is Go-class (~47–80 ns/yield); the ceiling is
  the interpreter.
- **The multi-core win needs 3.13t** (GIL off). On a normal GIL build runloom
  still runs — cheap spawn, the goroutine model, netpoll — but single-core like
  asyncio. The frame-snapshot also needs the 3.11+ tstate layout.
- **Memory per goroutine is higher than Go** (~26 KB with a Python handler vs
  Go's ~2.5–13 KB) — the CPython object tax (every `socket`/`bytes`/frame carries
  a PyObject header). A pure-C handler (`TCPConn`, `mn_go_c`) hits Go parity.
- **Preemption only fires at Python bytecode boundaries.** A goroutine inside a
  tight pure-C call (numpy, a third-party extension) is not preemptible until it
  returns — the same limitation Go has with cgo. The `heavy` monkey category and
  `offload()` are the escape hatches (spec 08).

## Why this design and not the alternatives

- **Stackful, not stackless.** A stackless design (like asyncio) keeps state in
  heap frame objects and needs `async`/`await` coloring. Stackful keeps a real C
  stack per goroutine, so user code is ordinary blocking Python and a switch is
  one `swap` (~22× cheaper than an asyncio loop step). The cost is per-goroutine
  memory, which spec 10 is entirely about minimizing. (spec 01)
- **Custom asm switch, not greenlet.** greenlet copies the stack on every switch;
  runloom keeps each goroutine on its own mmap'd stack and swaps the stack
  pointer — ~1.6× faster real switches, and the separate stacks are what make
  guard-page overflow detection and copy-grow possible. (spec 01)
- **C scheduler, not Python.** An earlier version implemented the scheduler in
  Python over raw coroutines. It worked for one goroutine but tangled CPython's
  `tstate.cframe` chain across stacks and crashed. Multiplexing Python frames
  across C stacks *requires* the per-g tstate snapshot, which must be C. (spec 03)
- **Free-threaded target, not GIL tricks.** The whole point is real parallelism.
  That forces the project onto 3.13t's internal contracts — which is where all
  the difficulty (and spec 09) lives.

## The correctness story (why there's a whole verification spec)

A lock-free M:N scheduler hides bugs in rare interleavings, and runloom drives OS
threads into CPython-internal state machines at moments those machines have
preconditions. So the concurrency core is checked from several independent
angles — model checkers (Spin, CBMC, GenMC, TLA+), machine-checked proofs (Coq,
Iris/iRC11 under the RC11 weak-memory model), linearizability (Porcupine),
deterministic replay, fault injection, and sanitizers — **each shipping with a
negative control that must fail**, so the checks are known to have teeth. The
design itself was partly *derived from* these checks (e.g. the park/wake fence in
spec 04 was proven necessary by GenMC). See spec 15.
