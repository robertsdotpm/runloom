# 08 — Stall recovery: blocking, preemption, and offload

Ground truth: `runloom_blockpool.{h,c}`, `mn_sched_sysmon.c.inc`,
`mn_sched_handoff.c.inc`, `runloom_sched_preempt.c.inc`, `io_uring.{h,c}`
(+ `io_uring_l_*.c.inc`), `monkey/heavy.py`, `docs/preemption.md`,
`docs/parallelism.md`.

## The problem, stated precisely

A cooperative scheduler assumes fibers yield. Three things break that
assumption, and **they are three different problems with three different fixes**.
A spec that lumps them as "blocking" is wrong. The thing to internalize:

| Stall class | Example | Why it wedges a hub | The fix |
|---|---|---|---|
| **I/O wait** | `recv` on an empty socket | would block the OS thread | **park on netpoll** (spec 06) — not really a stall |
| **CPU-bound Python** | a 10M-iteration arithmetic loop | never reaches a yield point | **bytecode-boundary preemption** (sysmon) |
| **Opaque blocking C** | libc `getaddrinfo`, a GIL-releasing C ext | invisible to preemption *and* netpoll | **offload** (move it off the hub) **or** **P-handoff rescue** (adopt the hub) |

asyncio has none of these recoveries: one blocking call freezes the whole loop.
runloom isolates a stall to one hub and *recovers* it. Both recovery mechanisms
are **default-on under 3.13t** and dormant under ~50 ms (steady-state scheduling
is unchanged); opt out with `RUNLOOM_HANDOFF=0` / `RUNLOOM_PREEMPT=0`.

## Fix 1 — Preemption (CPU-bound Python loops)

There are **two distinct preemption mechanisms** — don't conflate them (the code
keeps them in separate files):

1. **Default-on sysmon wedge-preemption** (`RUNLOOM_PREEMPT`,
   `mn_sched_sysmon.c.inc`). Default **on** for free-threaded 3.13+ (the
   `runloom_flag_default_on` gate; the in-code "default OFF" comments are the stale
   *static-init* view — `runloom_sysmon_config` flips it on). It is **reactive**:
   the sysmon watchdog only acts after a hub has been wedged in one `coro_resume`
   for `> runloom_sysmon_wedge_ns` (**50 ms** default, `RUNLOOM_SYSMON_MS`) *and*
   classified `ATTACHED` (CPU-bound, not a DETACHED blocking call → that's the
   handoff's job). It then sets `h->preempt_requested`; a **chained eval-frame
   function** (installed only when `RUNLOOM_PREEMPT` is on, since it costs CPython's
   `_PyEval_EvalFrameDefault` fast path) reads the flag at the next Python *frame*
   boundary and `runloom_coro_yield()`s the running g — plus a **single-frame
   liveness backstop** (`_PyEval_AddPendingCall`) that rides backward-jump checks
   *inside* a frame so a tight `while: pass` (no calls → no frame entry) is still
   broken. So: dormant under normal load, fires only on a genuine CPU monopoly.
   ([mn_sched_sysmon.c.inc:124-155](../src/runloom_c/mn_sched_sysmon.c.inc#L124),
   [mn_sched_runq.c.inc:306-328](../src/runloom_c/mn_sched_runq.c.inc#L306))
2. **Explicit `preempt_init(quantum_us)` time-slicer** (default **off**,
   `runloom_sched_preempt.c.inc`; the `docs/preemption.md` API). A timer thread
   posts `Py_AddPendingCall(yield_cb)` every quantum; CPython's `eval_breaker`
   runs it at the next bytecode boundary and it `runloom_sched_yield()`s the running
   g — Go-1.14 round-robin slicing. Cost: ~300 ns/quantum + one yield (~0.003% at
   100 Hz); the **hot path between yields pays nothing**. This is opt-in fair
   interleaving, unconditional (no 50 ms wedge gate).

Mechanism summary: both ride CPython's between-bytecode/frame `eval_breaker`; (1)
is a default-on safety net that fires only on a >50 ms CPU wedge, (2) is an opt-in
quantum slicer. `docs/preemption.md` documents only (2) and says "the default is
no preemption" — true *for the quantum slicer*, but (1) is on by default on 3.13t.

The hard limitations (both real, both in the spec because they bound the design):

- **Bytecode boundaries only.** A fiber inside a tight pure-C call (numpy, a
  multi-MB `hashlib`, a third-party extension) isn't running Python, so the check
  never fires — preemption hits when the C call returns. Same as Go with cgo.
  This is what `heavy`/`offload` (Fix 3) exists to cover.
- **Must NOT yield mid-object-destruction.** The preempt eval-frame wrapper fires
  at arbitrary frame entries, which can be nested inside an in-flight `tp_dealloc`
  (a weakref callback/finalizer driven by the free-threaded biased-refcount merge
  or the trashcan unwind). Yielding there freezes a half-finished destructor on
  the coro stack while the hub reaches a GC-safe point; a concurrent
  stop-the-world GC / QSBR reclaim then runs against partially-destroyed objects →
  use-after-free. **Both yield sites gate on `runloom_tstate_in_destruction(ts)`
  and DEFER while it's true** (leaving the trigger armed, so the next frame entry
  after the destructor unwinds takes the yield — never lost). This is contract C5
  (spec 09); it crashed `test_weakref` before the guard. Cooperative yields are
  exempt (they only happen at Python call points, never nested in a C destructor).

## Fix 2 — Offload (opaque blocking C, the proactive form)

`runloom_blockpool.c`. Run the blocking call on a **small dedicated pool of OS
threads** and **park the calling fiber** until it finishes — turning a
hub-wedging blocking call into an ordinary cooperative park. The hub keeps
scheduling other fibers; only the pool threads block. Pool size bounds blocking
concurrency (extra callers park on the job queue), exactly like a resolver pool.
The wake travels the **same race-safe path as everything else** (`wake_safe` /
`runloom_mn_wake_g`), so a worker finishing before the caller has parked is handled
by the park/wake counter (spec 04).

The pool worker runs `fn(arg)` on a plain OS thread **with no GIL**; `fn` must not
touch Python objects (acquire the GIL itself if it must) and must not call
scheduler ops. If the caller isn't on a fiber, `fn` runs inline (correct — it
just blocks the caller as before).

Surfaced to users three ways:
- **`runloom.blocking(fn, …)` / `runloom.monkey.offload(fn, …)`** — the manual
  escape hatch.
- **The `heavy` monkey category** (`monkey/heavy.py`) — *automatic, size-gated*
  offload of the common non-preemptible stdlib offenders: `hashlib` SHA/MD5/blake2,
  `zlib`/`gzip`/`bz2`/`lzma` compress/decompress above `RUNLOOM_OFFLOAD_BYTES`
  (default 256 KiB), KDFs (pbkdf2/scrypt) always. The size gate keeps small calls
  inline (zero cost); only the genuinely-long C loops, which nothing can preempt,
  go to the pool.
- **The `compile` patch** — `builtins.compile` (and thus `ast.parse` + source
  imports) offloads when called inside a fiber, because `compile`'s
  ~1.5 KB/level C recursion overflows a *small* g-stack before the per-fiber
  C-recursion counter (reset to 200 frames at entry, spec 10) fires. The arithmetic:
  200 × 1.5 KB ≈ 300 KB, so on the **512 KB default** compile fits and degrades to
  a clean `RecursionError`; but a grown-down (16 KB, M:N) or raw-`Coro` (128 KB)
  stack overflows at ~10–85 frames — well under 200 — and SEGVs. The pool thread's
  full-size stack runs it safely (spec 10). (`docs/cooperative_stdlib_coverage.md`
  frames this against a "32 KB g-stack" — that was the default before it was raised
  to 512 KB; the offload now matters for the *small-stack* paths, not the default.)

### io_uring — the cooperative form of file/socket I/O on Linux

For file I/O (and optionally the TCP hot path), `io_uring.c` provides truly async
I/O instead of a thread-hop: submit an SQE referencing a per-op record on the
caller's stack, park via `park_safe`, and the kernel signals a registered
**eventfd** on each completion; the netpoll pump drains the CQ ring and wakes the
parked fiber. Under M:N each **hub owns a `SINGLE_ISSUER` ring** (no submission
lock) whose eventfd is in the shared pump; the global ring keeps the multishot +
provided-buffer-ring path. This is a drop-in faster backend behind the same "park
the fiber" contract — `monkey`'s `os.read`/`write` on regular files use the
pool by default and io_uring when available, with no caller change.

## Fix 3 — P-handoff rescue (opaque blocking C, the reactive form)

`mn_sched_handoff.c.inc` + `mn_sched_sysmon.c.inc`. The offload pool only helps
calls runloom *anticipated*. For an **unanticipated** blocking call (a third-party
C driver you didn't patch), the hub goes DETACHED (the call released its tstate via
`Py_BEGIN_ALLOW_THREADS`) and its queued fibers are stranded — work-stealing
can't reach a wedged hub's local FIFO. The fix is **Go's `entersyscallblock`
P-handoff**: a standby **rescue thread adopts the stalled hub's tstate and drains
its stranded fibers** while the original thread is stuck in the syscall.

How it works, and the safety gates that make it sound:

- **sysmon detects the wedge.** A watchdog thread (holds no GIL, no tstate — only
  reads per-hub atomics) flags a hub whose current `resume` has run longer than
  `runloom_sysmon_wedge_ns` (~50 ms). It classifies by attach-state: DETACHED ⇒ a
  GIL-releasing blocking call (rescuable); ATTACHED ⇒ a CPU wedge (preempt it
  instead).
- **It requires a *stable* DETACH streak before dispatching a rescue.** A genuine
  blocking call holds the tstate DETACHED for the whole wedge (the thread is parked
  in a syscall and won't touch the tstate until it returns). A *transient* detach
  (an idle/world-yield blip, or the instant before a stop-the-world flips the state
  to SUSPENDED) does not survive consecutive ticks — and adopting one races the
  owner re-attaching/suspending on the same tstate → use-after-free (contract C3,
  spec 09). Requiring `RUNLOOM_HANDOFF_DETACH_TICKS` consecutive DETACHED ticks
  keeps the rescue off transient detaches.
- **The rescue adopts, drains, hands back.** A pool of standby threads CASes a
  per-hub claim slot (so two threads never rescue the same hub),
  `PyEval_RestoreThread`s the hub's tstate, and runs **one drain pass that resumes
  ONLY fresh deque fibers** — never a resumed/woken g, because that g's coro
  stack is baked to the *owner* hub's OS thread and resuming it on the rescue thread
  is a cross-hub migration that crashes (spec 03/05). It drains to empty, restores
  the owner's saved tstate slice, `PyEval_SaveThread`s (so the owner reclaims the
  instant its block ends), re-verifies the wedge, and either re-adopts or releases.
- **Several wedged hubs recover in parallel** — a pool of `min(hub_count, 4)`
  rescue threads by default (`RUNLOOM_HANDOFF_POOL`), each owning a distinct hub via
  the FREE→PENDING→OWNED claim slot. Dispatch requires `RUNLOOM_HANDOFF_DETACH_TICKS`
  (= 2) consecutive DETACHED ticks first (the stable-detach gate, contract C3). The
  blocking-offload pool (Fix 2) defaults to 8 workers (`RUNLOOM_BLOCKPOOL_WORKERS`,
  max 64). ([mn_sched_sysmon.c.inc:205-222](../src/runloom_c/mn_sched_sysmon.c.inc#L205),
  [runloom_blockpool.c:42-43](../src/runloom_c/runloom_blockpool.c#L42))

### The monopoly world-yield (a sibling case)

`mn_sched_sysmon.c.inc`'s `world_yield_if_monopolizing`: a fiber that loops a
*stop-the-world* op (a tight `gc.collect()` loop) while it's the sole runnable g on
its hub holds the world stopped ~100% of the time, starving hub-pinned work on
*other* hubs (which can't re-attach to drain). The fix: when a hub is about to
re-run its sole runnable g AND a sibling has work but is SUSPENDED-or-DETACHED-with-
pending, briefly DETACH this thread and sleep ~100 µs (a detached thread counts as
already-stopped), letting the stalled sibling start-the-world and progress before
this hub stops it again. Precisely targeted — a busy multi-g hub short-circuits on
a couple of relaxed loads.

## How the three fixes compose (the decision a hub makes)

```
  fiber parks on an fd/timer/channel/future  -> netpoll/sleep/chan/park (no hub cost)
  fiber runs a long Python loop              -> sysmon flags, preempt at bytecode boundary
  fiber runs a known heavy C call            -> heavy/offload moves it to the pool (park)
  fiber runs an UNanticipated blocking C call-> hub goes DETACHED ~50ms,
                                                    rescue thread adopts + drains it
```

The `>50 ms` threshold on both recoveries keeps them dormant under normal load.
On a production-shaped workload (I/O tiers + a 2% tier doing 100 ms of pure-Python
CPU), asyncio/uvloop freeze (p50 ~405 ms, every request queues behind the CPU
block) while runloom holds p50 ~11 ms at 7–10× throughput by running the CPU
blocks on other hubs. This stall isolation+recovery is runloom's main *structural*
win over asyncio.

## Cooperative stdlib coverage (the residual map)

From `docs/cooperative_stdlib_coverage.md`: sockets/TLS/DNS/selectors/subprocess/
files/signals/sync-primitives are **COOP** (park) or **offloaded**; GIL-releasing C
blockers (sqlite3, ctypes I/O, getrandom) are **COOP\*** (rescued by handoff after
~50 ms; use `offload()` to avoid the latency); **GIL-holding pure-Python/CPython-C
aggregation is STALL** — fundamental, relocate via `offload()`/`heavy`; `mp` *fork*
start-method **deadlocks** (use spawn/forkserver); `ProcessPoolExecutor` is
**unsupported** (use the fiber-backed `ThreadPoolExecutor`).

## Invariants

1. **Three stall classes, three fixes** — netpoll (I/O), preemption (CPU Python),
   offload + handoff (opaque C). Don't conflate them.
2. **Never preempt-yield mid-`tp_dealloc`** (`runloom_tstate_in_destruction` gate,
   contract C5). Defer with the trigger armed.
3. **The handoff rescue resumes ONLY fresh deque fibers** — never a g with a
   baked coro stack (no cross-hub migration), and **only after a stable DETACHED
   streak** (contract C3).
4. **Offloaded `fn` runs GIL-less and must not touch Python or scheduler ops**;
   the wake uses the standard race-safe path.
5. **Recoveries are dormant below ~50 ms** so steady-state scheduling is unchanged.
