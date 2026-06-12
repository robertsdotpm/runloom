# 06 — netpoll: parking fibers on fd readiness

Ground truth: `netpoll.{h,c}` and its includes — `netpoll_parkers.c.inc`
(parker pool), `netpoll_wait_fd.c.inc` (the park path + 3-state commit),
`netpoll_pump.c.inc` / `netpoll_pump_helpers.c.inc` (the pump),
`netpoll_register.c.inc`, `netpoll_parker_link.c.inc`, `netpoll_iocp.c`
(Windows), and `verify/genmc/netpoll_claim.c` (the proof).

## The problem

A fiber doing `recv` on a not-yet-ready socket must **park** (let other
fibers run) and resume when the fd is readable — with no busy-poll, scaling to
a million parked waiters, and **without losing a wake** when an event fires in the
narrow window while the fiber is committing to park. Under M:N a *different*
hub's pump may observe the event, so the protocol is cross-thread.

## The simple model

`runloom_netpoll_wait_fd(fd, events, timeout_ns)` parks the current fiber
until `fd` is ready (or timeout/signal/cancel) and returns the ready mask. One
shared kernel poller per backend (`epoll`/`kqueue`/`IOCP`/`WSAPoll`/`select`); a
**pump** drains ready events and routes each to the parked fiber's owner.

```
  wait_fd: register interest, park the g, yield.
  pump:    wait on the kernel poller, for each ready fd find its parker(s),
           record the ready mask, unlink, and wake the g to its owner.
```

## The lost-wake-free 3-state park-commit

The heart of correctness. A parker has a `commit` field with three states:
`ARMED` (created, not yet committed), `PARKED` (committed to sleeping), `WOKEN`
(claimed by a pump/timeout/cancel). The exact **ordering in `wait_fd`** is the
whole game — it was arrived at by fixing two distinct lost-wake bugs:

```
1. link the parker          # now any pump can FIND it in pool->by_fd[fd]
2. consume pending-wake bits # an event between our last unlink and this link
3. register fd with kernel   # epoll_ctl ADD / IOCP submit
4. re-check pending bits      # the ADD may synthesize an edge another hub ate
5. CAS commit ARMED -> PARKED:
      success -> snap, park_current, coro_yield   (truly asleep; pump wakes us)
      fail (==WOKEN) -> a pump already claimed us; it recorded the ready mask
                        and unlinked but did NOT re-queue (we weren't PARKED);
                        abort the park and return the mask directly
```

Why this exact order (both halves are war stories):

- **Link before register**, not after. The previous order (register, then link)
  lost wakes two ways: (a) the ADD synthesizes a readiness edge that another hub's
  pump processes *before the parker is visible*; (b) an edge fires in the gap
  between a previous parker being unlinked-on-wake and a new one being linked.
  Linking first makes the parker findable before any edge can be generated.
- **The `pending-wake` bitmap** captures "an event fired for this fd before any
  parker was visible." A pump that finds no parker for a ready fd sets the bit;
  `wait_fd` drains it at steps 2 and 4 and, if set, wakes itself immediately
  instead of parking. This closes the cross-hub window.
- **The commit CAS (step 5)** closes the *residual* window between the last
  pending re-check and the `coro_yield`. If a pump claims the parker (CAS it to
  WOKEN) after step 4 but before the fiber sleeps, the fiber's own
  `ARMED → PARKED` CAS fails, it sees WOKEN, and it returns the readiness directly
  without ever sleeping. **Exactly one of {pump, timeout, cancel, signal} wins the
  commit CAS** — that's the linearization point. (Verified in
  `verify/genmc/netpoll_claim.c`.)

This is Go's `netpollblockcommit` pattern, generalized to a shared cross-hub
poller.

## The parker pool (`netpoll_parkers.c.inc`)

A parker is **heap-allocated from a per-thread pool**, never stack-allocated.

> Why heap, not stack: a stack-allocated parker shares the fiber's coro stack
> address space, which is returned to a pool and reissued to the next g. A missed
> unlink would then leave a global/per-fd pointer aimed at a byte-identical
> address the new occupant just claimed — a use-after-free that resurrects a freed
> g via pump dispatch. Heap-pool parkers can't alias: in the freelist nothing
> references them; in flight they sit at a unique heap address.

Each parker pool groups its state under one lock + cache line:
- **global head list** (slot-pointer trick for O(1) unlink),
- **`by_fd` sparse array** (fd → bucket head; turns the pump from O(N·events) to
  O(events) — the difference between scanning every parker and a direct lookup at
  N=1024 conns),
- **deadline min-heap** (`dh_*`, O(log N) timeout management).

There is **one pool per hub** (plus one default for the single-thread sched), so a
fiber parking on hub H contends only on pool[H]'s lock — per-hub locality.
(Gated to the kernel by-fd backends epoll/kqueue/IOCP; WSAPoll/select fall back to
the single default pool because they rebuild fdsets by walking `head`.)

Defensive layers worth keeping: each g caches its active parker in
`g->netpoll_parker`, force-unlinked at g completion so a leaked parker can't
survive into stack-pool reuse; `g->park_fd`/`park_events` are plain data on the g
so the dump (spec 11) can say "fd=N R/W" without dereferencing a freeable parker.

## The pump

`runloom_netpoll_pump(timeout_ns)` is called by an idle scheduler/hub. It waits on
the kernel poller, and for each ready fd walks `by_fd[fd]`, claims each parker via
the same commit CAS, records the ready mask in the parker's `ready_out`, unlinks
it, and wakes the g via `runloom_mn_wake_g(hub, g)` (hub) or `runloom_sched_wake`
(single-thread). A woken g routes to **its origin hub's** local FIFO (which is
never stolen — spec 05), so the hub that runs the post-resume madvise is the same
one that resumes the g (the safety argument for park-time stack reclaim, spec 01).

The pump also services **io_uring** completion eventfds (per-hub and global rings,
spec 08) and a **cross-thread wake eventfd** (level-triggered, non-exclusive, so
every blocked pumper wakes and drains its own wake_list — spec 04).

## Three out-of-band wakes that ride the same commit CAS

All three claim the parker with the same CAS (so they can't double-wake a
committed g) and route to the owner:

1. **Timeout** — the deadline heap fires; `wait_fd` returns 0.
2. **Cancel** (`runloom_netpoll_cancel_g`) — `task.cancel()` targeting a g blocked
   in a C `wait_fd` where there is **no coro await-point to throw into**. Claims
   the parker, makes `wait_fd` return the `RUNLOOM_NETPOLL_CANCELLED` sentinel; the
   aio `_wait_fd` wrapper turns it into `CancelledError`. This is what lets a
   `task.cancel()` interrupt a fiber stuck in `sock_recv`/`accept`/`connect`
   (the root of the aiosmtpd/anyio teardown hangs — see memory).
3. **Signal delivery** (`runloom_netpoll_signal_wake` + `RUNLOOM_NETPOLL_SIGNALED`)
   — see below.

## Signal delivery: into the parked fiber, never via the scheduler

A core invariant (`CLAUDE.md`): a Python signal handler that **raises** during a
cooperative blocking call (`recv`/`select`/`accept`/…) must propagate **out of
that call into the caller's own `try/except`** — exactly as a signal interrupting
a real `recv()` does. So when the idle pump's wait returns `EINTR` and
`PyErr_CheckSignals` confirms a handler raised, the scheduler stashes the raised
exception on the g's owner sched (`->signal_exc`) and `signal_wake` re-queues
*one* parked g with the `SIGNALED` sentinel. On resume `wait_fd` restores the
exception into *that fiber's* tstate and returns -1 with it set. Only when
**nothing is parked** to receive it does the scheduler carry a `KeyboardInterrupt`
out of `run()` (the idle/sleep-only Ctrl-C case). Backend-independent.

## Backends and a fat-frame gotcha

`epoll` (Linux) / `kqueue` (BSD/macOS) / `IOCP`→`WSAPoll`→`select` (Windows,
probed at runtime) / `select` (everything else). On Windows IOCP-AFD the submit
happens in `wait_fd` and the pump just drains completions (a readiness shim over
a completion API).

One subtlety from `docs/cooperative_stdlib_coverage.md`: `select.select`'s CPython
C impl allocates a **50.9 KB single frame** (three `pylist[FD_SETSIZE+1]`), which
overflows a small fiber stack. The fix is *not* a bigger stack — it's to
**reimplement `select.select` cooperatively** in `monkey/polling.py` (register fds
on a transient epoll, park on the epoll's *own* fd via `wait_fd`) so CPython's fat
frame is never reached from a fiber (spec 14).

## Invariants

1. **Link before register; drain pending bits before and after register; commit
   via CAS before yield.** This exact order is lost-wake-free; reordering loses
   wakes (two proven ways).
2. **Exactly one of {pump, timeout, cancel, signal} wins a parker's commit CAS.**
   That CAS is the linearization point.
3. **Parkers are heap-pool, never stack** (no aliasing with recycled coro stacks).
4. **A woken g routes to its origin hub.** Blocked-on info lives as plain data on
   the g, not via the parker pointer.
5. **Signals deliver into the parked fiber's own stack**, never carried out of
   `run()` while something is parked to receive them.
