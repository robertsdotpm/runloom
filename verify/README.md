# pygo formal verification

Machine-checked correctness for pygo's lock-free concurrency primitives.
Two engines, used for what each is best at:

| engine | what it checks | how |
|--------|----------------|-----|
| **Spin** | the *algorithms*, exhaustively, over **all** thread interleavings (sequentially-consistent memory) | hand-written Promela models in [`spin/`](spin/) |
| **CBMC** | the **actual C source** of the deque, including the real `__atomic_*` memory orderings, over a bounded schedule | harness in [`cbmc/`](cbmc/) compiles `src/pygo_core/cldeque.c` *unmodified* |

They are complementary: Spin proves the algorithm has no bad interleaving
but abstracts the C; CBMC runs on the real code (real index arithmetic,
real acquire/release/seq_cst) but on a small bounded schedule. Together
with the runtime sanitizer stress in `tests_c/test_cldeque.c` (real
threads, millions of ops) that's three independent angles on the same
code.

## Run it

```sh
verify/run_verify.sh         # everything; ~3-4 min (CBMC dominates)
verify/run_verify.sh -q      # quieter
```

Needs `spin`, `cbmc`, and a C compiler:

```sh
sudo apt-get install spin cbmc
```

## What is proven

### 1. Chase-Lev work-stealing deque — `spin/cldeque.pml` + `cbmc/cldeque_cbmc.c`

The run-queue under each M:N hub (`src/pygo_core/cldeque.c`). One owner
pushes/pops the bottom lock-free; thieves CAS the top. Famous for being
*wrong* under weak memory in its original SPAA'05 form (see Lê, Pop,
Cohen, Nardelli, PPoPP'13); `cldeque.c` uses the corrected seq-cst
pop/steal + acquire/release push.

Proven over `owner + 2 thieves`, hitting the 1-element boundary where
the pop CAS races the steal CAS:

* **No duplication** — no work-item is ever returned to two consumers.
* **No loss** — at quiescence `consumed + deque_size == pushed`.
* **No phantom** — a returned item is always a real pushed tag.
* **No deadlock / size never negative.**

CBMC checks the same on the **unmodified `cldeque.c`** (compiled at
`-DPYGO_CLDEQUE_CAP=4` for a tractable SAT instance — the logic is
capacity-independent; production stays 4096). All 5 assertions:
`VERIFICATION SUCCESSFUL`.

### 2. Per-g `wake_state` machine — `spin/wake_state.pml`

The heart of the M:N scheduler: the 6-state CAS protocol documented on
`struct pygo_g.wake_state` (`pygo_sched.h`). Its predecessor — two
separate flags for "exactly-once wake" and "exclusive resume" — raced
into a re-push **livelock**; this model verifies the unified machine
against concurrent wakers (any thread), hubs (pull/resume/release), and
the idle-stack sweeper:

* **No duplicate / orphan run-queue entry** — `qentries == (state == QUEUED)` invariant holds at every reachable state.
* **No double resume** — at most one hub owns the g (`owners <= 1`).
* **No lost wake** — every issued wake is followed by a resume; the
  fully-drained terminal state always has `last_wake_unserved == 0`.

**Negative control:** compile with `-DBUGGY_DROP_WAKE` (a wake dropped
during `RUNNING`, the classic lost-wakeup) and Spin **finds it** —
assertion violated at depth 30 with a counterexample trail. The check
has teeth. `run_verify.sh` runs this and asserts it *does* fail.

### 3. `park_safe`/`wake_safe` handshake — `spin/parked_safe.pml`

The race-safe single-thread park used by `pygo.aio`'s `PygoTask` and the
blocking-offload pool, where a wake can arrive from another OS thread
mid-park (`pygo_sched_park_safe` / `pygo_sched_wake_safe`). Models the
`wake_pending` counter + `parked_safe` CAS handoff verbatim:

* **No lost wake** — the parker never blocks forever at the yield while a
  wake is outstanding (encoded as Spin's invalid-end-state check).
* **Balanced** — `wake_pending` nets to 0; the g is enqueued at most once
  (no double-schedule).

### 4. `select()` claim CAS — `spin/select_claim.pml`

The lock-free core of `select`: a goroutine parked on N channels shares
one `fired_case`; channels race to CAS it from −1 to their own index, and
only the winner does the handoff (`waiter_claim` in `chan.c`). Models N
channels racing to deliver:

* **Fires at most one case** — `wins <= 1` always.
* **Exactly-once wake** — `wake_count == wins`; losers leave a tombstone
  and never wake the goroutine.
* **Consistent result** — `fired_case` ends a valid index = the winner.

### 5. `select()` Phase-2 vs send/close — `spin/select_close.pml`

The full protocol where the 2026-05-31 select crash/loss arc lived (the
claim CAS alone is #4; this models install → abort-on-ready → park → wake
→ result, racing a concurrent send and close on the channel). A blocking
RECV select against a sender, a closer, and a spurious-wake source:

* **WELL-FORMED** — the result is exactly a sent value or *closed*; never
  NULL (the close-wake SIGSEGV) and never the no-case sentinel (a blocking
  select must never report "nothing ready").
* **CONSERVATION** — a value that was produced (claimed into our waiter or
  buffered) is the one returned; the abort / spurious-retry paths must not
  evict-and-drop a just-delivered value.
* **PROGRESS** — always terminates (bounded retries; no deadlock).

This model **found two additional real races** beyond the three the fuzzer
hit (close-wake-NULL, abort-bare-−1, abort-drops-value): (a) a value
buffered in the Phase-1→install window is orphaned if close claims the
waiter first — fixed by re-scanning on a close-wake (buffered drains
before closed); (b) a spurious-retry frees a waiter a racing delivery just
filled — fixed by evicting *before* re-reading `fired_case`. Five negative
controls (`-DBUG_CLOSE_NULL`, `-DBUG_ABORT_NOCASE`, `-DBUG_ABORT_DROP`,
`-DBUG_SPURIOUS`) each reintroduce a bug and make the model fail, so the
properties demonstrably have teeth.

### 6. Default M:N wake path — `spin/hub_submit.pml`

The wake path that actually runs by default on Linux free-threaded 3.13t:
`PYGO_PER_G_TSTATE` and `PYGO_STEAL_WOKEN` are both off, so `pygo_mn_wake_g`
routes through `pygo_mn_hub_submit` (the per-hub-tstate MPSC submission
list), **not** the global-runq `wake_state` machine of #2. A parker can be
`wake_g`'d more than once (a netpoll-pump unlink + a stale safety-unlink
wake); two defenses keep that safe and are modelled here:

* **No resume-after-done** — the hub never resumes a g that already ran to
  completion (the second resume would touch a coro freed by the
  post-completion decref — the segfault these defenses prevent). Guarded
  by the `in_sub_queue` CAS dedup **and** the done-check at pop.
* **Runs exactly once** — coalesced wakes resume the g exactly once (no
  lost wake, no double-resume).
* **At most one entry** — the dedup keeps g's submission count ≤ 1.

Negative control `-DBUG_NO_DEDUP` removes both defenses and the model
fails (resume-after-done).

### 7. Blocking-offload wake order — `spin/blockpool.pml`

The default `pygo.blocking` / DNS-offload path (`pygo_blockpool.c`): a
goroutine offloads to a worker thread and parks; the single-thread drain
blocks in `epoll_wait`, so an `inflight` counter keeps it alive while a job
is outstanding. The worker must **re-queue the goroutine before
decrementing `inflight`**, so the instant `inflight` hits 0 the drain
already sees the goroutine on its wake list.

* **No lost wake** — the offloaded goroutine is always resumed; the drain
  never exits (`inflight==0 && ready empty`) leaving it parked. Encoded as
  Spin's invalid-end-state check (a lost wake deadlocks the parked caller).
* **Resumed once** — the goroutine is resumed exactly once.

Negative control `-DBUG_DEC_BEFORE_REQUEUE` flips the order and the model
fails (the drain exits and strands the goroutine).

### 8. netpoll park/wake commit — `spin/netpoll_commit.pml`

The lost-wake guard for I/O parking (`netpoll.c`): the piece where the real
lost-wake bugs have lived (EPOLLET edge-drop, and the residual "missing atomic
park-commit"). It models Go's `netpollblockcommit`, adapted to pygo's re-queue
model — the `commit` field (`ARMED → {PARKED | WOKEN}`) shared between a
goroutine parking on an fd (`pygo_netpoll_wait_fd`) and the pump that delivers
readiness (`pygo_pump_dispatch_event` / `pygo_pump_claim`):

* the parking g CASes `ARMED → PARKED`; on success it yields, on failure
  (`WOKEN`) a pump beat it to the parker, so it aborts the park and returns the
  readiness it left;
* the pump CASes `commit → WOKEN` and re-queues the g **only** if it claimed
  from `PARKED` — claiming from `ARMED` means the g hasn't parked yet and will
  abort itself, so re-queueing would double-resume it; a second claimer that
  sees `WOKEN` skips entirely.

Proven over one parking g racing **two** pumps that both see the fd ready
(so the "second claimer sees `WOKEN`, touches nothing" path is exercised):

* **No lost wake** — the g always returns from `wait_fd` (re-queued if it
  parked, self-aborts if a pump claimed first). A lost wake leaves it blocked
  forever at the park = a Spin invalid end state.
* **At most once** — `resumes ≤ 1` / `requeues ≤ 1`: at most one pump claims
  from `PARKED`, and an aborting g is never re-queued.
* **Readiness delivered** — whenever the g returns, `ready_out` was written by
  the claiming pump first (the `pool->lock` ordering the abort path re-takes).
* **Mutually exclusive paths** — the g never both parks and aborts.

Negative control `-DBUG_NO_COMMIT` drops the commit CAS (the g always parks;
the pump re-queues only if it happens to observe a plain `parked` flag already
set) and Spin finds the classic lost wake: the pump checks the flag *before*
the g sets it, declines to wake, and the g parks forever.

### 9. netpoll LT+ONESHOT re-arm — `spin/netpoll_rearm.pml`

The *other* half of the netpoll lost-wake guard (#8 models the parker-claim
commit; this models the **arming discipline**). An fd can become ready while no
parker is linked (the g unlinked on its last wake and hasn't re-linked); a pump
processing that delivery finds no parker and stashes it in the per-fd
pending-wake bitmap. But the **bitmap alone does not close the window**: the
pump can be preempted between "found no parker" and the lock-free
`pygo_fd_pending_wake_set` (netpoll.c:2185-2195), letting the g link, consume
the still-empty bitmap twice, commit, and park *before* the bit is set.

What closes it is the documented T1.5 fix (`pygo_netpoll_register`,
netpoll.c:1158-1207): arm **LEVEL-triggered + `EPOLLONESHOT`, re-armed via
`EPOLL_CTL_MOD` on every park, strictly after linking the parker** (link 1803,
register 1845). `MOD` being level-triggered re-reports a still-ready fd,
queueing a *fresh* delivery — generated after the link, so it finds the linked
parker and wakes it. `EPOLLONESHOT` means the prior arm delivered once and
disarmed, so nothing is in flight before the re-arm.

* **No lost wake** — the g always becomes runnable. Under LT+ONESHOT the only
  delivery is the post-link re-arm one, which finds the linked parker; the
  bitmap is provably never even needed (the model never sets `pending`).

Negative control `-DBUG_EDGE_TRIGGERED` models the **old scheme** (EPOLLET,
registered once, never re-armed): `register` is a cached no-op and an
already-ready fd is *not* re-reported. Spin finds the lost wake — the pump
consumes the lone edge before the link, is preempted, the g links + double-
consumes the empty bitmap + parks, then the pump sets the bit too late and no
re-arm delivery ever comes. (Matches the recorded "EPOLLET+ONESHOT+re-arm hung
96/96; only LEVEL fixed it".) This is precisely *why* the bitmap needs the LT
re-arm.

### 10. netpoll multi-pool dispatch — `spin/netpoll_multipool.pml`

Per-hub parker pools: a g parked on hub H links into `pool[H]`. One epoll
delivery is processed by one pump (`EPOLLONESHOT`) that doesn't know the owning
hub, so `pygo_pump_dispatch_event` (netpoll.c:1977-2023) **walks every pool**,
dropping each pool lock before the next, and on a match claims + unlinks +
`wake_g(parker->hub)` — and `wake_g` takes the *home hub's* `sub_lock`
(`pygo_mn_hub_submit`, mn_sched.c:1273) **while still holding the pool lock**.
That is a two-level hierarchy with a documented order (netpoll.c:1972-1976):

```
pool->lock  <  hub->sub_lock        (always; never reversed)
at most ONE pool lock held at a time (dropped before walking the next pool)
```

Confirmed against the source: the only takers of *both* locks are
`dispatch_event` and `pygo_pump_drain_expired`, both `pool→sub`; every
`sub_lock` region (`hub_submit`, the hub-drain at mn_sched.c:651) takes the sub
lock alone. Proven over **two pumps racing one delivery** whose parker lives in
pool 1, plus a `sub_lock` contender (a hub draining its submission list):

* **No deadlock** — with pool-before-sub and one pool at a time there is no
  circular wait; every actor terminates (a deadlock is a Spin invalid end
  state).
* **Found anywhere** — the parker is found in whichever pool holds it (here the
  second pool walked), regardless of which pump reaches it.
* **Claimed once** — the pool lock + commit claim make exactly one pump wake
  the g though both find it (`wakes ≤ 1`); the loser sees it unlinked / WOKEN.

Negative control `-DBUG_LOCK_ORDER` makes the contender take its locks in the
**reverse** order (`sub_lock` then `pool_lock`) — the ABBA a future refactor
could introduce — and Spin finds the deadlock: a pump holds pool 1 waiting for
sub 1 while the contender holds sub 1 waiting for pool 1.

### 11. io_uring multishot handle lifetime — `spin/iouring_msclose.pml`

The one genuinely io_uring-specific lifetime question (an audit finding, not a
guessed property): `pygo_iouring_ms_recv` parks with the handle's `waiter_g`
set and, on wake, **re-locks the handle** (io_uring.c:999); `on_cqe` on the
closing CQE wakes that waiter and then frees the handle *outside* `h->lock`
(io_uring.c:878-891), and `ms_close`'s `!armed` branch frees immediately
(:1018-1032). `PygoTCPConn` holds no lock around `self->ms`/`self->closed`
(pygo_tcp.c), so `recv` and `close` are unsynchronised.

This is memory-safe **only under the single-owner convention**: a `TCPConn` is
driven by one goroutine, so `close()` runs after `recv()` returns and no
consumer is parked in `ms_recv` when the closing CQE frees the handle.
(`PygoTCPConn` is a standalone primitive — *not* used by `pygo.aio` — and its
benches/tests are one-goroutine-per-conn.) The model proves **no use-after-free
under that convention**: the consumer never re-locks the handle after it is
freed (`assert(freed == 0)` at the re-lock).

Negative control `-DBUG_CONCURRENT_CLOSE` lifts the convention (a second task
closes the conn while the first is parked in `recv` — a shared `TCPConn` under
`PYGO_TCPCONN_IOURING=1` on M:N free-threaded) and Spin finds the UAF: the
closing CQE wakes the parked consumer *and* frees the handle, and the woken
consumer re-locks freed memory. So the single-owner convention is load-bearing
for memory safety; making `TCPConn` shareable would require refcounting the
handle or freeing it under coordination with a parked `recv`.

### 12. Phase C per-thread-scheduler wake routing — `spin/cross_thread_wake.pml`

pygo now runs **one scheduler per OS thread** (commit 4bef422); pygo.aio drives
each event loop on its own thread, and a goroutine records its owner sched at
spawn (`g->owner`). When a **foreign thread** wakes it — a `run_in_executor`
pool worker, or an io_uring CQE resolving a future the owner awaits —
`pygo_sched_wake_safe` must enqueue the g onto the **owner sched's** wake_list
(the list the owner thread drains), not the waker thread's:

```c
pygo_sched_t *s = g->owner ? g->owner : pygo_sched_get();   /* route to owner */
```

This composes the verified `park_safe`/`wake_safe` handshake (§3, unchanged by
Phase C) with the new routing dimension. Proven over a goroutine owned by and
parked on the owner sched, woken by a foreign thread:

* **No lost wake** — the g is always resumed: it either consumed the pending
  wake at park (the waker beat it) or parked and the **owner's** drain pulled
  it off the owner wake_list. A lost wake leaves the g blocked at its park with
  the owner drain idle = a Spin invalid end state.

Negative control `-DBUG_ROUTE_TO_WAKER` enqueues the woken g onto the *waker*
thread's wake_list (the pre-Phase-C `pygo_sched_get()` behavior); the owner's
drain never sees it and the foreign waker runs no drain loop, so Spin finds the
lost wake — exactly the concurrent-loop deadlock Phase C fixes.

> **Phase 2 (done, merged):** routing **netpoll** fd completions to the
> parker's owner sched (the multi-loop *socket* case) landed — the pump may run
> on a different thread than the parker's owner, so `dispatch_event` /
> `drain_expired` wake via `p->g->owner` (`pygo_sched_wake` routes cross-thread),
> and `drain_parked` is scoped to the calling thread's gs. This model +
> `netpoll_commit.pml` cover the wake decision underneath it.

### 13. netpoll deadline sweep vs fd dispatch — `spin/netpoll_deadline.pml`

The *timeout* half of the netpoll. A parked goroutine has both an fd it waits on
and a finite deadline in the per-pool min-heap, so two **different** pump paths
can wake it, delivering **different values**:

* `pygo_pump_dispatch_event` — the fd became ready: `*ready_out = mask` (nonzero).
* `pygo_pump_drain_expired` — the deadline passed: `*ready_out = 0` (timeout).

Both serialise on `pool->lock` and both gate the `ready_out` write behind the
*same* `pygo_pump_claim` commit CAS (§8). The property here — beyond the
no-lost-wake / at-most-once of §8 — is **value correctness** under a simultaneous
fd-ready + deadline-expiry race: the g resumes **exactly once** and observes the
value of whichever claimer actually won the CAS, never a spurious timeout (`0`)
clobbering a delivered nonzero mask, and never the un-set initial. Proven over a
parking g racing one fd dispatch and one deadline drain:

* **No lost wake** — the g always returns from `wait_fd`.
* **At most once** — `resumes <= 1`, `requeues <= 1`: the loser of the claim CAS
  sees `WOKEN` and touches neither `ready_out` nor the run queue.
* **Value correctness** — on return, `ready_out` was written by exactly the
  claimer recorded in `winner` (fd-win ⇒ mask, timeout-win ⇒ 0), never `UNSET`.

Negative control `-DBUG_SWEEP_NO_COMMIT` models the naive sweep the commit CAS
replaced: `drain_expired` pops the heap top and **unconditionally** writes
`*ready_out = 0` and re-queues a parked g, without claiming. Spin finds the
spurious-timeout / double-resume — the fd dispatch delivers a nonzero mask and
re-queues, the sweep clobbers `ready_out` with `0` and re-queues *again*.

### 14. netpoll force_unlink release lifetime — `spin/netpoll_forceunlink.pml`

The **exactly-once `pool_release`** question. A parker `p` lives on the parking
g's coroutine stack and is tracked by `g->netpoll_parker` (the *token*).
`pygo_parker_unlink` clears that token under `pool->lock` whenever it removes p
(netpoll.c:605-606). Three sites touch p: the **pump** unlinks it (clearing the
token) and re-queues the g, but **never releases** — the woken g resumes in
`wait_fd` and releases p itself; **`wait_fd`** releases p on every exit *after*
clearing the token; and **`pygo_netpoll_force_unlink_g_parker`** (the
g-completion safety net) takes `pool->lock`, **re-reads the token under the
lock** (netpoll.c:1421-1424: *"in case `g->netpoll_parker` was cleared by a
concurrent unlink between the check above and the lock acquire"*), and
unlinks + releases **only if it still saw the token set**.

`wait_fd` and `force_unlink` run on the same thread in program order (the
coroutine, then `hub_main`'s completion), so they never race each other. The
genuine race is `force_unlink` (completion thread) vs the pump (a poller thread)
for a p that is about to go back to the pool and be re-issued. Proven:

* **Exactly-once release** — `released <= 1`: p is released by exactly one of
  {the resumed g riding the pump's wake, `force_unlink`}, never both. The
  under-lock token re-read is what makes the loser observe the cleared token and
  decline.
* **No use-after-free** — `assert(!freed)` guards every unlink/release: once p is
  unlinked under the lock a later pump pass cannot find it, and `force_unlink`
  cannot release a parker the resumed g already returned.

Negative control `-DBUG_NO_RECHECK` drops the under-lock re-read: `force_unlink`
trusts the stale cheap-path token it sampled *before* taking the lock and
releases unconditionally. Spin finds the double-free — the pump unlinks + wakes
the g (which resumes and releases p), and `force_unlink`, still holding the stale
"token set", frees the same parker again.

## Scope & honesty

* Spin models are **sequentially consistent**: they prove the algorithm
  has no bad *interleaving*, not the C11 fence placement. The fence
  placement on the deque is what **CBMC** covers (it carries the real
  `__atomic_*` orders). Where the three engines agree, that's strong
  evidence; none is a substitute for the others.
* Bounds are small by necessity (BMC / explicit-state both blow up). The
  deque proofs use 2 thieves and ≤3 items; the wake machine uses 2
  wakers / 2 hubs / 1 sweeper. These are the cardinalities at which the
  known bugs reproduced, so they are the right bounds — but they are
  bounds.
* The **channel send/recv/buffer** logic itself is not modelled
  end-to-end here: it is serialized under `ch->lock`, so its concurrency
  reduces to (verified `wake_state`/`park_safe` primitives) + (verified
  `select` claim) + straight-line locked code. The integrated channel is
  exercised by `tests/test_chan.py`, `tests/test_mn.py`, and
  `tools/mn_stress.py`.
* The netpoll models cover the two lost-wake cores — the parker-claim commit
  (`netpoll_commit.pml`) and the arming discipline (`netpoll_rearm.pml`) — but
  not all the surrounding machinery: the per-fd bucket / global-list link &
  unlink surgery, the deadline min-heap timeout sweep, and
  `pygo_netpoll_force_unlink_g_parker` (the g-completion safety unlink) are not
  modelled. The timeout sweep and cancel/drain use the *same* `pygo_pump_claim`
  for the wake decision, so the exactly-once guarantee of #8 carries to them;
  the list surgery and force-unlink are lock-protected straight-line code
  (`pool->lock` serialises them against the pump) exercised by the netpoll
  tests and `tools/mn_stress.py`. The multi-pool dispatch walk and its
  `pool→sub` lock hierarchy are covered by `netpoll_multipool.pml`, and the
  deadline min-heap timeout sweep (the fd-dispatch-vs-timeout-drain claim race)
  by `netpoll_deadline.pml` (§13), and the `force_unlink` release lifetime
  (exactly-once `pool_release`, no use-after-free vs the pump) by
  `netpoll_forceunlink.pml` (§14). What remains unmodelled is the min-heap
  *mechanics* (sift-up/down, arbitrary-remove via `heap_index`) — pure
  `pool->lock`-serialised straight-line code with no concurrency, exercised by
  the netpoll tests and `tools/mn_stress.py`.
* **io_uring** is not modelled directly, by design: its single-op path
  (`pygo_iouring_submit` / `pygo_iouring_drain`) is verified *by composition* —
  the goroutine parks via `pygo_sched_park_safe` (covered by `parked_safe.pml`)
  and the drain **wakes the goroutine before decrementing `inflight_count`**
  (io_uring.c:620-625 wake, :640 decrement), the exact ordering `blockpool.pml`
  proves keeps the single-thread drain from exiting early. The one genuinely
  io_uring-specific surface is **multishot** (`pygo_iouring_ms_*`): its handle
  lifetime (the `on_cqe`/`ms_close` free vs a parked `ms_recv`) is now modelled
  by `iouring_msclose.pml` (§11) — memory-safe under the single-owner
  convention, a use-after-free without it.

## Layout

```
verify/
  run_verify.sh            driver: runs all Spin + CBMC checks, reports PASS/FAIL
  spin/
    cldeque.pml            Chase-Lev deque (no loss / dup / phantom)
    wake_state.pml         per-g wake_state machine (+ BUGGY_DROP_WAKE control)
    parked_safe.pml        park_safe/wake_safe handshake
    select_claim.pml       select fired_case CAS
    select_close.pml       select Phase-2 vs send/close (+ 4 bug controls)
    hub_submit.pml         default M:N wake dedup (+ BUG_NO_DEDUP control)
    blockpool.pml          blocking-offload wake order (+ BUG_DEC_BEFORE_REQUEUE)
    netpoll_commit.pml     netpoll park/wake commit protocol (+ BUG_NO_COMMIT)
    netpoll_rearm.pml      netpoll LT+ONESHOT re-arm vs not-yet-linked window (+ BUG_EDGE_TRIGGERED)
    netpoll_multipool.pml  netpoll multi-pool dispatch pool->sub lock hierarchy (+ BUG_LOCK_ORDER)
    iouring_msclose.pml    io_uring multishot handle lifetime, recv vs close (+ BUG_CONCURRENT_CLOSE)
    netpoll_deadline.pml   netpoll deadline sweep vs fd dispatch, value correctness (+ BUG_SWEEP_NO_COMMIT)
    netpoll_forceunlink.pml netpoll force_unlink vs pump, exactly-once release / no UAF (+ BUG_NO_RECHECK)
    cross_thread_wake.pml  Phase C per-thread-sched owner-routed wake_safe (+ BUG_ROUTE_TO_WAKER)
  cbmc/
    cldeque_cbmc.c         harness over the real cldeque.c
    stubs/plat_compat.h    minimal stub so cldeque.c compiles standalone under CBMC
```
