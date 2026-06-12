# 07 — Channels and select

Ground truth: `chan.{h,c}`, `chan_ops.c.inc`, `chan_waiters.c.inc`,
`chan_select_main.c.inc`, `chan_select_helpers.c.inc`, `docs/channels.md`,
and the linearizability check `tools/lincheck/` (Porcupine).

## The problem

Go channels are the project's headline synchronization primitive: typed,
blocking, with rendezvous (unbuffered) and buffered modes, `select` over many,
and close semantics. They must work **across hubs** (a producer on hub A, a
consumer on hub B) and be **linearizable to Go's FIFO semantics**.

## The simple model

A `runloom_chan` is: a capacity, a ring buffer (for `cap > 0`), a closed flag, two
waiter queues (parked **senders**, parked **receivers**), and **one mutex** — the
only synchronization. Park/wake piggybacks on the scheduler's wake path
(`runloom_sched_wake` / `runloom_mn_wake_g`), so each waiter records its `hub` + `g`
and the wake routes back to the right scheduler under M:N.

Semantics (match Go, modulo names):

| | unbuffered (`cap=0`) | buffered (`cap>0`) |
|---|---|---|
| send | blocks until a receiver is ready; **direct handoff** | blocks only when full |
| recv | blocks until a sender is ready | blocks only when empty *and* no parked sender |
| close | wakes all parked senders (they raise) + receivers (they get the close sentinel); buffered values still drain first; double-close raises |

`recv` returns `(value, ok)`; `ok=False` means closed-and-empty (Go's `v, ok :=
<-ch`). `try_send`/`try_recv` never park (the non-blocking forms). On the Python
side, `for v in ch` iterates until close.

## The send/recv logic (one locked critical section each)

The whole protocol is a short decision tree under the channel lock. **Send:**

```
if closed:            unlock; raise ValueError("send on closed channel")
if a receiver waits:  INCREF value -> hand directly to receiver, wake it     # rendezvous
if buffered & room:   INCREF value -> push to ring buffer
else if non-blocking: unlock; return WOULD-BLOCK
else:                 park as a sender holding our ref; on wake, our ref was
                      taken (delivered) or we were closed-while-parked (raise)
```

**Recv** is the mirror, with one ordering rule: **buffered values take priority
over closed status** (a closed channel still drains its buffer), and after popping
a buffered value, if a sender was parked (buffer was full) pull *its* value into
the freed slot and wake it — so a full buffer with parked senders stays full and
FIFO.

### Reference-counting discipline (easy to get wrong)

- `send` **INCREFs** the value into the channel (caller keeps its own ref).
- `recv` **transfers** that ref to the caller (no extra INCREF on the recv path).
- A parked sender holds its own ref on the value until a receiver/buffer takes it;
  on a closed-while-parked wake it DECREFs and raises.
- `close` DECREFs anything left in the buffer.

The direct-handoff path (`send` → waiting receiver, or `recv` → waiting sender)
moves the ref straight across with **no copy and no buffer touch** — that's what
makes unbuffered ping-pong ~560 ns (within 7% of Go 1.22 on the same box).

### The waiter as a stack local (plain ops) / heap array (select)

For a **plain** `send`/`recv`, the waiter is a small `runloom_chan_waiter_t` **on
the parking fiber's own stack** (`park_waiter` links it, releases the lock,
snaps, yields; the waker fills `value`/`ok`/`send_result` and wakes —
[chan_ops.c.inc:52-68](../src/runloom_c/chan_ops.c.inc#L52)). A **`select`**
instead heap-allocates an `n`-entry waiter array (`PyMem_Calloc`,
[chan_select_main.c.inc:37](../src/runloom_c/chan_select_main.c.inc#L37)) whose
lifetime is bounded by the select call, and pins every case's channel with an
incref across the park+eviction phase. The stack-local form is safe (unlike the
netpoll parker, spec 06) because the waiter is consumed before the fiber
resumes — the fiber is suspended exactly at the park, so its stack frame
holding the waiter is stable until wake.

## `select` — the multi-way wait

`runloom_chan_select(cases, n, default_ready)` waits on N send/recv cases:

1. **Poll pass.** Walk the cases; if any is immediately ready (a receiver/sender
   waiting, or buffer room/data), fire it and return its index. Ready cases are
   tried so a `select` over already-ready channels is allocation-free (~120 ns).
2. If none ready and `default_ready`, return -1 (Go's `default:` branch — the
   non-blocking select).
3. Otherwise **enqueue a waiter on every case's channel** and park once. When any
   channel fires for this g, exactly one case must win — the **claim CAS** ensures
   a g enqueued on N channels is consumed by at most one waker (the select-claim
   protocol). On wake, unlink from the *other* channels and return the winning
   index.

The select-claim CAS is the analog of netpoll's commit CAS and the channel
waiter's `waiter_pop_claimable`: a waiter present on multiple queues must be
claimed exactly once. This is precisely what the linearizability harness checks.

## Cross-hub correctness and the linearizability check

Channels are exercised under the real multi-hub M:N scheduler with overlapping
real-time producer/consumer intervals (GIL off), and **Porcupine** decides whether
some linearization consistent with those intervals satisfies the sequential
FIFO-channel spec. Real runs are LINEARIZABLE; the negative control (a phantom or
duplicated delivery) is reported NOT LINEARIZABLE — so the check has teeth. A
stateful Hypothesis machine generates random op sequences against a reference FIFO
model with shrinking. (spec 15)

## Higher-level uses

Channels are the substrate for several front-ends: `runloom.time.After/Tick/Timer/
Ticker` are channels driven by a sleeping fiber (spec 12); `runloom.context`'s
cancellation is a never-/eventually-closed channel (spec 12); the original aio
per-task wake was a `Chan(1)` before it was replaced by the cheaper `park_safe`
primitive (spec 04).

## Invariants

1. **One mutex per channel is the only synchronization**; park/wake routes via the
   scheduler wake path so it works across hubs.
2. **Ref discipline:** send INCREFs in, recv transfers out, parked sender holds
   its ref until taken, close DECREFs the buffer. Direct handoff moves the ref
   with no copy.
3. **Buffered values drain before close is observed**; a full buffer with parked
   senders refills FIFO on each recv.
4. **A waiter enqueued on multiple channels (select) is claimed exactly once** (the
   claim CAS) — no phantom or duplicate delivery (linearizability has teeth).
5. **Double-close raises; send-on-closed raises; recv-on-closed-empty returns
   `(None, False)`** — Go semantics exactly.
