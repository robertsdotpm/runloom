# runloom — design specifications (reverse-engineered archive)

This folder is a **design spec reconstructed from the implementation**. It is not
generated docs and it is not a tutorial. It answers one question for every part
of runloom:

> *If the whole codebase were lost and you had only these specs, could you
> re-derive a simple, correct implementation of the same design — and would you
> understand why it has to be that way?*

Each document states the **problem**, the **simple model** a re-implementer
should build, the **decisions and the reason for each** ("why we ended up
there"), the **invariants that must hold** (the hard-won correctness
constraints — most were paid for with a real crash), and **pointers to the
ground-truth code**.

The guiding principle of the archive: runloom is ~38 K lines of C and Python,
but the *design* is small. Almost all of the line count is (a) portability
shims, (b) defensive instrumentation, and (c) the handful of concurrency
protocols written out very carefully. Strip those away and the conceptual core
fits in these pages. The hard part was never the code volume — it was the
**boundary between a custom M:N scheduler and free-threaded CPython's internal
state machines**, and that boundary is what the specs spend their words on.

## How to read this

Read in order for the full model; jump by subsystem if you know what you want.

| # | Spec | What it pins down |
|---|------|-------------------|
| 00 | [overview](00-overview.md) | The north-star, the M:N:G model, the layering, the trade-offs |
| 01 | [stackful coroutines](01-stackful-coroutines.md) | The asm context switch, stacks, guard pages, pooling, copy-grow |
| 02 | [goroutine + single-thread scheduler](02-goroutine-and-scheduler.md) | The `G`, the ready ring, sleep heap, yield, drain, quiescence |
| 03 | [CPython per-goroutine state](03-cpython-state-snapshot.md) | Why a stack swap isn't enough; the tstate snapshot/restore dance |
| 04 | [park / wake protocol](04-park-wake-protocol.md) | The lost-wake-free primitive (the Dekker fence), cross-thread wakes |
| 05 | [M:N scheduler](05-mn-scheduler.md) | Hubs, work-stealing, origin-hub routing, the `wake_state` machine |
| 06 | [netpoll](06-netpoll.md) | The 3-state park-commit, backends, parker pool, signals, cancel |
| 07 | [channels + select](07-channels-and-select.md) | Go channels, direct handoff, select claim protocol, close |
| 08 | [blocking, preemption, offload](08-blocking-preemption-offload.md) | The three stall classes and their three fixes |
| 09 | [the free-threaded CPython boundary](09-cpython-freethreaded-boundary.md) | The 5 internal state machines and the 6 contracts (every bug lived here) |
| 10 | [stack safety + sizing](10-stack-safety-and-sizing.md) | Guard page, RecursionError, grow-down, calibration, prescan |
| 11 | [crash + introspection](11-crash-and-introspection.md) | Fault classification, the goroutine registry/dump, hub snapshot |
| 12 | [public API + sync/time/context](12-public-api.md) | `go`/`run`, the Go-style facades |
| 13 | [the asyncio bridge](13-asyncio-bridge.md) | `RunloomTask`, the future protocol, the documented semantic diffs |
| 14 | [monkey-patched cooperative stdlib](14-monkey-cooperative-stdlib.md) | The leaf-primitive principle, the Parker, foreign-thread safety |
| 15 | [verification + testing](15-verification-and-testing.md) | How the invariants are actually proven/checked, with teeth |
| 16 | [portability](16-portability.md) | Platform/arch/compiler detection, backend selection, build knobs |

## The five things to understand first

If you read nothing else, these are the load-bearing ideas the rest hang off:

1. **A goroutine is a real C stack + a CPython thread-state snapshot.** The asm
   `swap` moves the C stack; a manual save/restore of ~12 `PyThreadState` fields
   moves the Python execution context. Neither alone is enough (spec 01, 03).

2. **Park/wake must be lost-wake-free under weak memory.** Every blocking
   primitive reduces to "publish that I'm parked, then re-check whether a wake
   already arrived" — and that store-then-load needs a *sequentially-consistent
   fence*, not just release/acquire (spec 04, 06). This was a real, verified bug.

3. **The default scheduler runs one OS thread; M:N is the same scheduler, N
   times, with a work-stealing deque and origin-hub routing** (spec 02, 05).
   Goroutines are *pinned* to their origin hub once started, because a live coro
   stack is bound to one OS thread. Only *fresh, never-run* goroutines are
   stealable.

4. **Every hard bug lived on the CPython free-threaded boundary, not in
   runloom's own logic** (spec 09). Six contracts (brc-owner, attach/detach,
   no-resurrect-suspended, detach-before-block, no-yield-mid-dealloc,
   gilstate-delete-on-owner) — violate one and you get a use-after-free 200 ms
   later. The specs name each contract and the bug it cost.

5. **Three things can stall a hub, and there are three distinct fixes**
   (spec 08): I/O → park on netpoll; CPU-bound Python loop → bytecode-boundary
   preemption; opaque blocking C call → a rescue thread adopts the hub
   (Go's P-handoff). A spec is incomplete if it treats "blocking" as one problem.

## A note on fidelity

These specs were written by reading the headers (which carry the contracts),
the implementation `.inc`/`.c`/`.py` files, the user docs under `docs/`,
`CLAUDE.md` (the live invariant list), and `QUALITY_CAMPAIGN.md`. Where a spec
states an invariant, it is because the code enforces it and usually because a
comment records the crash that motivated it. Code pointers use the real paths
under `src/runloom_c/` and `src/runloom/`. The project was originally named
`pygo`; the public name is **runloom** and the C symbols/prefix are `runloom_`.
