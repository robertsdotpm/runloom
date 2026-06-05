# 09 — The free-threaded CPython boundary (where every hard bug lived)

Ground truth: `docs/dev/cpython_boundary.md`, `runloom_iframe.{h,c}`, the hub
tstate code in `mn_sched_hub_main.c.inc`, `mn_sched_handoff.c.inc`,
`runloom_sched_preempt.c.inc`, and the TLA+ models `verify/tla/RunloomCPythonSTW.tla`
/ `RunloomGilstate.tla`.

## The thesis of this spec

> runloom's hard bugs do not live in its own logic. They live on the **boundary**
> between the M:N scheduler and free-threaded CPython 3.13t's runtime invariants.
> Every confirmed bug was a **contract violation** at this boundary — runloom drove
> an OS thread into a CPython-internal state machine at a moment that machine's
> protocol forbids.

If you re-derive runloom, the scheduler is the easy part. **This is the part that
will bite you**, and it only exists on 3.13t (GIL off). A GIL build serializes
everything and none of this applies — which is exactly why the multi-core target
is also the hard target.

## The surface is small

runloom touches only ~18 private CPython symbols; only ~7 are *concurrency*
boundaries. The rest (`_PyStackChunk`, `_PyInterpreterFrame`, `_PyBytes_Resize`,
…) is frame/stack plumbing for the coro swap (spec 03), not shared-state machines.
The concurrency surface, by call:

| entry point | drives |
|---|---|
| `PyEval_SaveThread` / `RestoreThread` | attach/detach (M1) |
| `PyThreadState_New` (`_NewBound`) | tstate lifecycle (M3) + gilstate (M4) + brc (M5) |
| `PyThreadState_Clear` / `Delete` | tstate lifecycle (M3) |
| `_PyEval_AddPendingCall` | eval-breaker (rides M2) |
| `_Py_brc_init_thread` (via `bind_tstate`) | biased refcounting (M5) |

## The five internal state machines

**M1 — tstate attach/detach** (`tstate->state`: ATTACHED/DETACHED/SUSPENDED, an
atomic, free-threaded only). `RestoreThread` = DETACHED→ATTACHED (CAS; **only the
owning thread attaches itself**, and only when no other tstate is active on this OS
thread — else `Py_FatalError`). `SaveThread` = ATTACHED→DETACHED (a thread detaches
only *itself*). A **stop-the-world requester** flips other threads DETACHED→SUSPENDED.

**M2 — stop-the-world handshake** (`stoptheworld`: requested / thread_countdown /
world_stopped). The requester sets `requested`, counts other threads, parks each
DETACHED→SUSPENDED and sets a please-stop bit on each ATTACHED one, waits until the
countdown hits 0, sets `world_stopped`. **Invariant: while `world_stopped`, every
thread but the requester is SUSPENDED** — nothing mutates objects/refcounts. A
thread that is ATTACHED but blocked **without reaching an eval-breaker check**
never clears its stop bit → STW hangs (this is why preemption can't be the *only*
cooperation mechanism, and why the world-yield in spec 08 detaches).

**M3 — tstate lifecycle + the thread list** (`interp->threads.head`, `HEAD_LOCK`).
`new_threadstate` links under HEAD_LOCK; delete unlinks then unbinds gilstate (M4),
unbinds tstate, qsbr-unregisters, frees. **Concurrent `PyThreadState_New` from
several hubs also races `gc.immortalize`'s check-then-act *outside* HEAD_LOCK** —
so runloom serializes hub-tstate creation with `runloom_hub_tstate_lock`.

**M4 — GILState TSS binding** (per-OS-thread slot). `_PyThreadState_NewBound` binds
the new tstate as **the calling thread's** gilstate tstate iff that thread's slot
is empty (always true for a fresh hub thread). The unbind asserts `tstate ==
gilstate_tss_get(...)`. **Contract: a gilstate-bound tstate must be Clear/Delete'd
on the thread it was bound to.**

**M5 — biased reference counting + dealloc** (`brc`, trashcan).
`_Py_brc_init_thread` binds the tstate's brc to **the calling thread**; cross-thread
decrefs route to `brc->tid`'s merge queue (asserts `brc->tid == _Py_ThreadId()` on
drain). **Contract: the brc owner must be the thread that runs the tstate** (so
`ob_tid` of objects it creates equals `brc->tid`). And mid-`tp_dealloc` (trashcan
`delete_later`, or the merge drain `local_objects_to_merge`) objects are
half-destroyed — **never suspend/yield there.**

## The six contracts, and the bug each one cost

This table *is* the spec. Memorize it; it is the difference between a runtime that
works and one that UAFs ~1 in 50k.

| # | Contract | Violated by | How it bit |
|---|---|---|---|
| **C1** | brc owner == the thread that runs the tstate | bug 1: hub tstate created on the **main** thread | cross-thread decref merges refcount as "owner exited" → release UAF (the gc-churn crash). **Fix: create each hub's tstate on its own thread** (spec 05). |
| **C2** | only the owning thread attaches/detaches *itself* | structural — honored by construction | `Py_FatalError` if violated. |
| **C3** | never re-attach a tstate another thread may have SUSPENDED mid-STW | bug 2: handoff rescue adopted a *transiently*-detached tstate | the owner re-attaching/STW-suspending on the same `_status` (which CPython only touches from the single owner) → UAF. **Fix: require a stable DETACHED streak before adopting** (spec 08). |
| **C4** | detach before any blocking wait, so STW can complete | honored — the controlled scheduler / world-yield `SaveThread` before blocking | else STW hangs. |
| **C5** | never suspend/yield a goroutine mid-`tp_dealloc` | would-be preempt bug | a concurrent STW/QSBR reclaim corrupts the half-destroyed objects → UAF (crashed `test_weakref`). **Fix: `runloom_tstate_in_destruction` gate on both yield sites** (spec 08). |
| **C6** | a gilstate-bound tstate is deleted on its owning thread | the gilstate bug: hub tstates deleted from **main** in `mn_fini` | `pystate.c:345` abort under `--with-pydebug`; release builds hid it. **Fix: the hub deletes its own tstate on its own thread** (spec 05). |

C1, C2, C3, C5 were learned from the gc-churn crashes + the preempt guard. **C6
was found by building a `--with-pydebug --disable-gil` oracle and running a bare
`mn_init/run/fini` under it** — release builds hid it entirely. This is the
methodological lesson: *the contracts are invisible in release builds; you need
the pydebug oracle (or a sanitizer, or a model) to surface them at the violation
point instead of as a UAF 200 ms later.*

## Two CPython-internal helpers runloom needs (the `Py_BUILD_CORE`-isolated TU)

`runloom_iframe.c` is compiled as its own translation unit so the `Py_BUILD_CORE`
blast radius (reaching internal frame/tstate layout) is contained:

- **`runloom_tstate_in_destruction(ts)`** — true while `ts` runs `tp_dealloc`
  machinery (`delete_later` trashcan, or `brc.local_objects_to_merge` merge drain).
  The C5 gate. (Note: you cannot fix C5 by rerouting preemption through the
  eval-breaker — the merge's dealloc→callback→eval re-enters `_Py_HandlePending`
  *nested*, so a pending-call preempt still fires inside the destructor.)
- **`runloom_critsec_suspend/restore`** — release/reinstate held per-object critical
  sections across a cooperative park (spec 03), since runloom parks *without*
  detaching the tstate and a held `ma_mutex` would deadlock every other hub.
- **`runloom_iframe_walk`** — walk a suspended frame chain for the dump (spec 11).

## How this boundary is validated (and why the spec trusts it)

- **`--with-pydebug` oracle** (`tools/run_pydebug.sh`): build against a
  `--with-pydebug --disable-gil` interpreter; CPython's own `assert()`s fire at the
  violation point. Already earned C6.
- **Targeted delay injection** (`RUNLOOM_DELAY`): widen the M1/M2/M5 windows
  (world-yield detach, handoff adopt, coro acquire/release) so TSan reliably hits a
  C3/C5-class race instead of ~1/56k.
- **TLA+ models with negative controls**: `RunloomCPythonSTW.tla` composes M1+M2 and
  checks "no non-requester hub is ATTACHED while the world is stopped"; the
  `Bypass=TRUE` control (re-attaching a SUSPENDED tstate) violates it — the formal
  counterpart of bug 2 / C3. `RunloomGilstate.tla` puts C6 under the same net.
- **Trace conformance**: the extension emits a trace of its real transitions and
  TLC replays it through the model's own actions, so the *actual run* is checked
  against the spec (not a re-transcription). Fully doable for runloom-side machines
  (gilstate, the controlled-scheduler baton); only partial for STW (that handshake
  lives inside CPython — the pydebug oracle exercises it instead).

## Invariants (the whole spec, condensed)

1. **C1** create each hub's tstate on its own thread (brc owner == runner).
2. **C2** a thread only attaches/detaches itself.
3. **C3** never adopt a transiently-detached tstate — require a stable streak.
4. **C4** detach before any blocking wait so STW can complete.
5. **C5** never yield/suspend mid-`tp_dealloc` — gate on `in_destruction`.
6. **C6** delete a gilstate-bound tstate on its owning thread.
7. Serialize concurrent hub tstate creation (`gc.immortalize` race, M3).
8. **Validate with a pydebug/sanitizer/model oracle** — these violations are
   invisible in release builds.
