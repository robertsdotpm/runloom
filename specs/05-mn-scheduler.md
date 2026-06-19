# 05 — The M:N scheduler (hubs, work-stealing, routing)

Ground truth: `mn_sched.{h,c}`, `mn_sched_hub_main.c.inc`,
`mn_sched_runq.c.inc`, `mn_sched_init_fini.c.inc`, `mn_sched_mn_api.c.inc`,
`cldeque.{h,c}` (the deque), and spec 02 (the per-hub scheduler it replicates).

## The problem

The single-thread scheduler (spec 02) saturates one core. To use N cores you need
N OS threads each running Python in parallel — which only works with the **GIL
off** (free-threaded 3.13t). The challenge is doing that without (a) a global run
queue that every thread contends on, and (b) migrating live coroutine stacks
between threads (which is unsafe — spec 01, 03).

## The simple model: N hubs, each a spec-02 scheduler + a steal queue

```
  Hub 0            Hub 1            Hub 2     ...     (N OS threads)
  ┌────────────┐   ┌────────────┐   ┌────────────┐
  │ deque      │◀──┤ steal      │   │ deque      │   Chase-Lev: owner push/pop
  │  (fresh g) │   │            │   │  (fresh g) │   bottom; thieves CAS top
  ├────────────┤   ├────────────┤   ├────────────┤
  │ local FIFO │   │ local FIFO │   │ local FIFO │   yielded/woken gs (hub-pinned)
  │ sleep heap │   │ sleep heap │   │ sleep heap │
  │ parkerpool │   │ parkerpool │   │ parkerpool │   per-hub parker pool + io_uring
  │ io_uring   │   │ io_uring   │   │ io_uring   │   ring; per-hub PyThreadState
  │ tstate     │   │ tstate     │   │ tstate     │
  └────────────┘   └────────────┘   └────────────┘
         ▲ MPSC submission queue (sub_head) per hub: external mn_fiber / cross-hub wakes
         ▼ ONE shared netpoll (epoll/kqueue/IOCP) created once; the pump runs on
           whichever hub is idle and routes each wake back to the parker's origin hub
```

> **Correction (verified in code):** the *kernel poller* is a **single shared**
> `epoll`/`kqueue`/`IOCP` handle, created exactly once under the pool lock
> ([netpoll_init.c.inc:184,209](../src/runloom_c/netpoll_init.c.inc#L184)). What is
> *per-hub* is the **parker pool** (`by_fd`/head/deadline-heap, for lock locality)
> and the **io_uring ring** (`SINGLE_ISSUER`). Goroutines parked on I/O are routed
> back to their **origin hub** on wake via the parker's recorded `hub`, *not* via a
> per-hub poller. (`docs/parallelism.md` and an older mn_sched.c comment say "each
> hub has its own netpoll" — that's the stale view; spec 06 states it correctly.)

`runloom_mn_init(n)` starts n hub threads; `mn_fiber(fn)` spawns onto a hub;
`mn_run()` waits for all queues to drain; `mn_fini()` tears down. `runloom.run(n,
main)` wraps this whole envelope (spec 12).

### The hub main loop (`runloom_hub_main`)

Each hub thread, per iteration:

1. **Create its `PyThreadState` on its own thread** (see "the tstate ownership
   rule" below).
2. Drain its **MPSC submission queue** (external `mn_fiber`, cross-hub wakes) into
   either the deque (fresh g) or the local FIFO (a woken/yielded g with a saved
   snap).
3. Pop work, in priority order: **local FIFO** (yielded gs) → **own deque**
   (fresh gs) → **steal** from a neighbor's deque.
4. Save the hub's tstate into a local `hub_snap`; load the g's snap (or install a
   fresh datastack for a first run); `resume` it.
5. Restore `hub_snap`. If the g is alive and didn't self-queue (a raw
   `coro_yield`), push it back to the local FIFO so it keeps progressing.

This is the spec-02 drain, per hub, plus stealing. The idle policy: when all hubs
report `pending == 0`, keep polling (more `mn_fiber` can arrive any time); stop on
`h->stopping`.

## Work-stealing deque (Chase-Lev) — `cldeque.c`

A single-owner / multi-thief lock-free deque (Chase & Lev, SPAA 2005). The owner
`push`/`pop`s the **bottom** with no synchronization in the common path; thieves
CAS the **top**. Capacity is fixed (4096; overridable so the bounded model checker
can verify a small instance).

The memory orderings are load-bearing and proven (spec 15):
- `push`: relaxed load of bottom, acquire load of top, store item, **release**
  store of bottom (publishes the item).
- `pop`: **seq_cst** store of bottom then **seq_cst** load of top (the StoreLoad
  that resolves the last-element race with thieves), CAS top on the last element.
- `steal`: acquire top, **seq_cst fence**, acquire bottom, CAS top.

> The `take()` seq_cst StoreLoad and the `push()` release on `bottom` are both
> *necessary*: weakening either reproduces a duplication that `tests_c/test_cldeque`
> catches on real ARM hardware (x86-TSO masks the push case). The deque ships
> "ghost" instrumentation hooks (`RUNLOOM_CL_*`, zero-cost off) that CBMC/GenMC use
> to check disjointness + taken-once. This is the single most heavily verified
> piece of the system.

### Why steal only *fresh* fibers

A fiber that has **run** has a live C stack with absolute pointers bound to
one OS thread, and its CPython eval frame caches that hub's tstate (spec 03).
Resuming it on another thread is a cross-hub migration that is unsound in
free-threaded mode (and crashes on macOS/arm64 with a stale-tstate SIGSEGV). So:

- The **deque holds only fresh, never-run fibers** — no stack to migrate, so
  a thief can safely start one.
- **Yielded/woken fibers live in the hub's local FIFO**, which is *never
  stolen*, and wakes route them **back to their origin hub** (`runloom_mn_wake_g`).

This is the design's central trade: **locality and correctness over perfect load
balance.** A g parked for I/O wakes on its origin hub even if another hub is idle.
In practice this evens out under steady load; the alternative (migrating live
stacks) is not safely available.

### `mn_fiber`'s round-robin and the submission queue

`mn_fiber` from inside a hub places on that hub; from outside any hub it round-robins.
Either way it goes through the target hub's **MPSC submission queue** (`sub_head`
under `sub_lock`), guarded by the `in_sub_queue` CAS flag so a g can't be
submitted twice (a spurious double-wake becomes a no-op rather than a
double-resume of a freed coro). The hub drains submissions each iteration. A
cross-hub wake also kicks the hub's pump so an idle `epoll_wait` breaks.

## The `wake_state` machine (the per-g M:N wake invariant)

For the experimental global run-queue path (`RUNLOOM_PER_G_TSTATE`) and the idle
stack sweep, each g has a single atomic `wake_state` unifying *exactly-once-wake*
and *exclusive-resume* into one invariant:

```
  PARKED  --wake_g(any thread)-->  QUEUED   (winner enqueues + increfs)
  QUEUED  --hub pulls-->           RUNNING  (sole consumer of the entry)
  RUNNING --wake during run-->     RUNNING_WOKEN  (remember; do NOT enqueue)
  RUNNING --release, parked-->     PARKED, or RUNNING_WOKEN -> QUEUED (+enqueue)
  PARKED  --idle sweep claims-->   SWEEPING (own the stack for MADV_DONTNEED)
  SWEEPING--wake during madvise--> SWEEPING_WOKEN -> QUEUED at release
```

Why one field, not two: an exactly-once-wake dedup flag and an exclusive-resume
claim flag used to be *separate*, and they raced into a re-push livelock because
"one entry per park" and "one resumer" were distinct invariants that could
disagree. Folding them makes them the **same** invariant: a g holds at most one
run-queue entry exactly when QUEUED, and exactly one hub owns it exactly when
RUNNING. Being a single-location CAS, it is immune to the Dekker problem of spec
04. (Default per-hub-tstate mode doesn't use this; it's for the sweeper handshake
and the experimental global runq.)

## The tstate ownership rule (the link to spec 09)

**Each hub creates its `PyThreadState` on its own thread**, not on the main thread
in `mn_init`. `PyThreadState_New` binds the tstate's biased-refcount owner id and
mimalloc heap to the *calling* thread. If the main thread created it, every object
the hub creates would carry the main thread's `ob_tid`, and cross-thread DECREF
would merge the refcount as "owner exited" — the gc-churn use-after-free (contract
C1, spec 09). Creating it on the hub makes `brc->tid == ob_tid` for everything the
hub allocates. Creation is serialized across hubs under `runloom_hub_tstate_lock`
because CPython's `new_threadstate` races on the `gc.immortalize` check-then-act
outside its own HEAD_LOCK. The hub also **deletes its own tstate on its own
thread** at exit (contract C6).

## Caveats that shape usage (from `docs/parallelism.md`)

- **3.13t only.** `mn_init` raises on a GIL build — serializing through the GIL
  with no parallelism would be pure overhead.
- **A single channel/lock shared by all hubs is a contention point** at high
  throughput; the idiom is one channel per hub fanning into an aggregator.
- **Origin-hub routing costs some load balance** (a g waits for its busy origin
  hub even if another is idle) — the deliberate locality/correctness trade above.

## Invariants

1. **Only fresh, never-run fibers are stealable.** Live stacks never migrate
   in per-hub-tstate mode.
2. **Woken/yielded fibers route to their origin hub** via the MPSC
   submission queue; the local FIFO is never stolen.
3. **A hub creates *and deletes* its own tstate on its own thread**, serialized
   across hubs (contracts C1/C6, spec 09).
4. **`in_sub_queue` (CAS) prevents double-submit**; `wake_state` (single CAS) is
   the exactly-once-wake + exclusive-resume invariant where used.
5. **Chase-Lev's seq_cst orderings are necessary** — do not weaken to
   release/acquire (verified on real ARM).
