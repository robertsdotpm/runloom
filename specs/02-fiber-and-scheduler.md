# 02 — The fiber and the single-thread scheduler

Ground truth: `src/runloom_c/runloom_sched.{h,c}`,
`runloom_sched_core.c.inc`, `runloom_sched_drain.c.inc`,
`runloom_sched_parkwake.c.inc`, `runloom_gstate.{h,c}`.

This spec covers the **N=1 scheduler** — one OS thread driving many fibers
cooperatively. The M:N scheduler (spec 05) is this, replicated per hub, plus a
work-stealing deque. Understand this one first.

## The `G` — one fiber

`struct runloom_g` is the unit of work. The fields that matter conceptually:

- `coro` — its stackful coroutine (spec 01).
- `callable` (Python) **or** `c_entry`/`c_arg` (a pure-C fiber, no Python —
  used by the C test harness and `TCPConn` handlers for Go-parity memory).
- `snap` — the CPython thread-state snapshot, **valid only while suspended**
  (spec 03). This is the other half of "a fiber is a stack + a tstate snap."
- `result` / `error`, `done`, `refcount`.
- `owner` — the scheduler that owns it (the thread it was spawned on). A
  cross-thread wake must route the g back to *this* scheduler, not the waker's.
- `wake_at` + `sleep_seq` — sleep deadline and a FIFO tiebreak for equal
  deadlines (so equal-time timers fire in `(when, seq)` order like asyncio).
- a small set of **lock-free membership/lifecycle flags**: `in_sub_queue`,
  `wake_pending`/`parked_safe` (the park/wake handshake, spec 04), `wake_state`
  (the M:N woken-queue machine, spec 05), `netpoll_parker` (its active netpoll
  parker, spec 06), and an observational `state` byte (`runloom_gstate.h`).

**Refcounting:** two parties hold refs — the scheduler (while g is queued/sleeping)
and the Python `RunloomG` wrapper (while user code holds it). The g frees when
both drop. `try_incref` (CAS, fails at refcount 0) lets the dump pin a g found
via the registry without resurrecting one a final decref is mid-freeing.

### The observational state machine (`runloom_gstate.h`)

A single atomic byte records *where in its lifecycle* a g is: `INIT → SPAWNING →
RUNNABLE → RUNNING → PARKED_{NETPOLL,CHAN,SLEEP,SAFE} → WAKING → DONE → FREED`.
It is **deliberately a strict superset** of the load-bearing flags above — it
does not replace them, it *observes* them, so a debug build can assert illegal
edges (`submit must not see DONE`) and the diag ring can record the trajectory.
Production cost is one byte store per transition. The reason it's additive rather
than a rewrite: the existing flags are load-bearing in already-shipped concurrent
code; cleaning up the dual representation is a per-site follow-up, not a rewrite.

## The scheduler — `struct runloom_sched`, one per OS thread

Four data structures and a couple of cross-thread inboxes:

1. **Ready ring** — a power-of-2 array of `G*` (head/tail indices). It was a
   linked list through `g->next`, but every pop then dereffed a cache-cold g just
   to read `next`; the contiguous ring keeps the queue hot in L1 and saves a
   cache miss per push/pop. This is the runnable FIFO.
2. **Sleep heap** — a min-heap by `wake_at` (1-indexed array). Timers live here.
3. **`current`** — the g running right now (for `yield`).
4. **`completed`**, **`stopping`** — drain bookkeeping.
5. **Cross-thread wake list** — an MPSC list (`wake_list_head/tail` under
   `wake_list_lock`, threaded through `g->wake_next`). A foreign thread that wakes
   one of *our* fibers pushes here; our drain consumes it once per iteration.
   This keeps foreign wakers off the single-consumer ready ring (spec 04).
6. **Quiescence list** — fibers parked by `run_ready()` (the asyncio
   "one loop iteration" barrier; see below).

`netpoll_parked` is a *per-sched* count of this thread's netpoll parkers, so a
fiber parked on another (or dead) OS thread can't keep this thread's `run()`
alive forever.

## The core operations

### spawn

Allocate a g (per-thread **slab** with a LIFO freelist + cap; see the
field-ordering trick below), give it a coro sized by calibration/grow-down/an
explicit `stack_size`, mark it RUNNABLE, push to the ready ring. Returns a Python
`RunloomG` wrapper. Variants: `spawn_noyield` (caller asserts the callable never
yields → skip the per-g snapshot dance, save ~150–400 ns) and `spawn_sized`.

### yield (`runloom_sched_yield`) — the hottest path

```
if (inside an M:N hub) -> hand off to the hub yield (spec 05); return.
fold any pending cross-thread wakes into the ready ring   # see "sleep(0)" below
if (ready empty && no sleepers due && nothing parked && nothing offloaded):
    return            # Gosched fast path: nobody else to run, ~<10 ns
ready_push(self, g); snap(&g->snap); coro_yield()
# on resume: drain has restored g->snap; continue past coro_yield
```

Two design points worth lifting:

- **The Gosched fast path** (Go's `runtime.Gosched` shortcut): if no other work
  exists, yielding is pure bookkeeping that hands control right back, so skip the
  whole snap+swap+resume cycle. Cuts the tight-yield baseline from ~230 ns to
  <10 ns.
- **Draining the wake list *before* re-queuing self.** A fiber that calls
  `g.wake()` on a peer (via the aio bridge) and then `await sleep(0)` must let the
  woken g run first, like asyncio's `sleep(0)` = one loop iteration. So yield
  folds the cross-thread wake list into the ready ring before pushing itself,
  putting the woken g ahead in FIFO order. (A cheap NULL-hint keeps the
  tight-yield fast path intact.)

### sleep (`sleep_until`) — push to the heap, snap, yield

The g records `wake_at`, pushes onto the sleep heap, snaps, and yields without
re-queuing. The drain loop pops due sleepers back to ready. `sleep_until_real`
forces the *wall* clock even under the logical clock (the aio keepalive heartbeat
must not ride logical time — spec 13).

### park (`park_current`) — yield without re-queuing

Used by netpoll/channels: the parker takes ownership of the g and arranges its
own wake. The g snaps and yields; it is *not* on any ready queue. (Hub-aware: in
a hub it marks `tls_mark_parked` so the hub loop doesn't re-push it.)

### wake (`runloom_sched_wake`) — re-queue a parked g

Same-thread: push onto our ready ring. Cross-thread: enqueue onto the **owner's**
wake_list and kick its netpoll pump (level-triggered eventfd) so an idle
`epoll_wait` wakes to drain. Never push a foreign g onto our own ring — it's
single-consumer and would resume the g on the wrong thread.

### drain (`runloom_sched_drain`) — the loop that *is* `run()`

```
loop:
    drain cross-thread wake_list into ready
    while ready not empty:
        g = ready_pop(); current = g
        load g->snap into tstate (or install a fresh datastack for a first run)
        resume(g->coro)
        save the scheduler's own tstate back; restore prev current
        if g done:    free its datastack chain, decref g, count++
        # else g re-queued itself (yield) or a parker owns it (park)
    pop due sleepers -> ready
    flush the quiescence list -> ready        # asyncio one-iteration boundary
    if ready still empty:
        if nothing parked/sleeping/offloaded: break    # all work done
        compute the next timeout (earliest sleeper) and netpoll_pump(timeout)
        # pump wakes parked gs whose fds are ready, or returns on timeout/signal
```

The drain is what makes `runloom.run(1, main)` go: spawn `main`, drain to
quiescence, return the completed count. It also runs a **deadlock detector**: if
it would block with fibers still parked on channels/`park_safe` and nothing
can wake them, it reports (Go's "all fibers are asleep — deadlock!"); mode is
off/warn/raise (`RUNLOOM_DEADLOCK`).

### The quiescence barrier (`run_ready`)

`run_ready()` parks the caller on a separate FIFO that the drain flushes back to
ready **only at a quiescence point** — when the ready ring is empty, just before
it would block on netpoll/timers. Net effect: every fiber runnable *now*
(including freshly woken ones) runs to its next park before `run_ready` returns —
exactly asyncio's "drain this iteration's ready callbacks" semantics, iterated to
quiescence. This is the primitive the aio loop's `_run_once` is built on.

## The slab + registry field-ordering trick (a clever, load-bearing detail)

The introspection registry (spec 11) wants to walk every live g without taking a
lock on the hot spawn path. The trick: the registry links a g **once**, when its
struct is first OS-allocated, and unlinks only when returned to the OS. A recycled
(slab-cached) g **stays linked**. The slab reuse path memsets a g only up to
`offsetof(runloom_g_t, state)` ([runloom_sched_core.c.inc:329](../src/runloom_c/runloom_sched_core.c.inc#L329)),
then re-initializes `state` with an atomic store and resets `id`/`park_fd` at
spawn — so the `reg_prev`/`reg_next` links (placed *last* in the struct, after the
`state`/`id` introspection block) survive recycling untouched. (The header comment
says "up to `offsetof(id)`"; the code actually stops one block earlier, at
`state` — same effect, the links are after both.) Result: link/unlink happen
only on the cold slab-miss/overflow paths; the hot spawn/complete path takes no
registry lock and touches no shared atomic. **A re-implementer must keep the
registry links as the final struct members and stop the reuse memset before
them** — this field-ordering is a contract, commented at both sites.

## Invariants

1. **`g->snap` is valid iff the g is suspended.** Save on yield/park/sleep, load
   on resume; save and load must balance (spec 03).
2. **A g runs on exactly one thread; cross-thread wakes route to `g->owner`,
   never the waker's sched.** The ready ring is single-consumer.
3. **The per-sched `netpoll_parked` count gates this thread's drain exit** — not
   the global parked count — so foreign parkers can't keep us alive forever.
4. **Registry links survive slab recycling** (field-ordering contract above).
5. **Equal-`wake_at` timers fire in `sleep_seq` (FIFO) order** to match asyncio.
