# 04 — The lost-wake-free park / wake protocol

Ground truth: `runloom_sched_parkwake.c.inc`
(`runloom_sched_park_safe` / `runloom_sched_wake_safe`), the `wake_pending` /
`parked_safe` fields on `struct runloom_g`, and
`verify/genmc/sched_parkwake.c` (the machine-checked proof).

## The problem (the single hardest thing to get right)

Every blocking primitive — a future await, a channel op, a netpoll wait —
reduces to:

> The **parker** decides to go to sleep. The **waker** decides to wake it. These
> two decisions race. If the waker fires *just before* the parker commits to
> sleeping, and nobody notices, the goroutine sleeps **forever** (a lost wakeup).

The classic fix is "publish that I'm parked, then re-check whether a wake already
arrived; symmetrically, the waker bumps a flag, then checks whether I'm parked."
The trap: **the publish (a store) and the re-check (a load) are on different
memory locations, and release/acquire ordering does not prevent a store→load
reorder.** Under x86-TSO (store buffer) *and* weak models (ARM/RC11), both sides
can read stale values and miss each other.

## The simple model: `park_safe` / `wake_safe` on two words

Two fields per g: `wake_pending` (a counter) and `parked_safe` (a 0/1 flag).

```
  park_safe (the parker):                 wake_safe (the waker):
  ───────────────────────                 ──────────────────────
  if wake_pending > 0:                     atomic_add(wake_pending, 1)   # bump
      consume one, return (no sleep)       FENCE seq_cst                 # <-- (!)
  parked_safe = 1   (release)              if CAS(parked_safe, 1 -> 0):  # claim
  FENCE seq_cst         # <-- (!)              enqueue g to its owner    # we own
  if wake_pending > 0:                         the wake
      if CAS(parked_safe, 1 -> 0):         # else: parker is running or
          consume one, return (no sleep)   #       another waker claimed it;
      # else: waker claimed us; fall       #       our wake_pending bump is
      #       through, drain will pick      #       still observable
      #       up the enqueued g
  snap(); coro_yield()      # actually sleep
  # on resume: consume one wake_pending
```

The two `FENCE seq_cst` are the whole point. Without them the parker's recheck
can read a stale `wake_pending == 0` while the waker's CAS reads a stale
`parked_safe == 0` — and **both miss each other**: the g parks but is never
enqueued. This is the Dekker / StoreLoad pattern; release/acquire is *not* enough
because they don't order a store-then-load to *different* locations.

> **This was a real, found-and-fixed bug.** Verifying `park_safe`/`wake_safe`
> under GenMC's RC11 model surfaced a lost wakeup the SC Spin model could not see.
> The fix is the seq_cst fence on *both* sides; the `-DBUG_NO_SC_FENCE` negative
> control in `verify/genmc/sched_parkwake.c` reproduces the lost wake, and the
> fence makes GenMC clean. (The M:N single-location `wake_state` CAS — spec 05 —
> is immune because it's one location, not Dekker-shaped.) See memory of the
> park/wake StoreLoad bug; merged as `1052aba`.

### Why a counter, not a boolean, for `wake_pending`

A wake that arrives **before** the park makes the park a no-op (the parker eats
one count and continues). This is essential: a future can fire *synchronously*
inside `add_done_callback` if it was already done. The counter records "a wake
already happened" so park doesn't sleep through it. The aio bridge uses exactly
this to replace a per-task `Chan(1)` wake channel — saving ~5 µs per parked task.

### Why `parked_safe` and not "is s->current == g?"

The original predicate for "is g parked?" read `s->current`, which the *sched
owner's* drain updates. A **cross-thread** waker (a `run_in_executor` pool worker,
an io_uring CQE callback) reading `s->current` from a foreign thread races: it
could see `s->current == g` (drain hadn't restored `prev` yet) and skip the push —
losing the wake. The `parked_safe` CAS gives a deterministic "did *we* own the
wake?" answer independent of any cross-thread tstate observation.

## Cross-thread routing: never touch a foreign ready ring

When `wake_safe` wins the claim, it routes g to **`g->owner`** (the thread that
spawned it), because the waker may be a foreign thread whose own sched is never
drained:

- **Same-thread wake** (the common single-loop aio case): push straight onto the
  owner's cooperative **ready ring**, so the wake is **FIFO-ordered with the
  `call_soon`/`go` spawns issued after it**. (Routing a same-thread wake through
  the batch-drained wake_list instead made a woken task resume *after* a later
  `call_soon` — violating asyncio's future-completion-callback FIFO order; that
  was the asyncssh channel-open-vs-close crash. See spec 13.)
- **Cross-thread wake**: enqueue onto the owner's `wake_list` (MPSC, under
  `wake_list_lock`) and kick its netpoll pump. Our ready ring is single-consumer
  and not cross-thread-safe.

> Critical detail: to find "this thread's sched" the waker **peeks**
> `runloom_tls_sched`; it must **never** call `runloom_sched_get()`, which lazily
> *allocates* a sched and runs mimalloc. On a foreign waker thread with no usable
> Python heap (a blockpool worker / iouring CQE thread), that allocation crashes.
> `runloom_tls_sched == NULL` is exactly the cross-thread case. This is a recurring
> rule across the codebase: **on a possibly-foreign thread, peek, never get.**

The lock-free edge between `wake_safe` (producer) and the owner's drain
(consumer) is `wake_list_head`: written RELEASE, peeked ACQUIRE. `tail`/`wake_next`
are only touched under the lock, so only `head` needs the atomic.

## How the other blocking primitives use this

- **Futures / `park_self`** (the aio bridge, `runloom.sync.park`) — `park_safe`
  directly. A future's done-callback calls `wake_safe`.
- **Channels** (spec 07) — a parked sender/receiver waiter is woken via the same
  `runloom_sched_wake` / `runloom_mn_wake_g` machinery.
- **Netpoll** (spec 06) — has its *own* 3-state commit (`ARMED/PARKED/WOKEN`) for
  the parker↔pump race, then routes the wake through `runloom_sched_wake` /
  `runloom_mn_wake_g`. The netpoll commit and the park/wake handshake are
  siblings: both solve "the waker fired before I committed," netpoll for an fd
  event, park/wake for a future/channel.
- **Blockpool / io_uring completions** — a worker/CQE on a foreign thread calls
  `wake_safe` (single-thread) or `runloom_mn_wake_g` (hub); the same machinery.

## Invariants

1. **A seq_cst fence on *both* sides, between the publish-store and the
   recheck-load.** Release/acquire is insufficient (proven). This is the line
   between "works" and "hangs ~1 in 50k."
2. **`wake_pending` is a counter** so a pre-park wake is never lost; consume
   exactly one per delivered wake.
3. **Exactly one party owns each wake** — the `parked_safe` CAS decides. The
   loser does nothing (its bump stays observable).
4. **Route the wake to `g->owner`, never the waker's sched.** Same-thread →
   ready ring (FIFO with later spawns); cross-thread → owner's wake_list + pump
   kick.
5. **Peek the TLS sched, never lazily allocate one, on a possibly-foreign
   thread.**
