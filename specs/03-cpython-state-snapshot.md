# 03 — The CPython per-fiber state snapshot

Ground truth: `src/runloom_c/runloom_sched.h` (`struct runloom_pystate_snap`),
`runloom_sched_pystate.c.inc`, `runloom_sched_datastack.c.inc`,
`runloom_iframe.{h,c}`. The algorithm is transcribed from **greenlet**
(`TPythonState.cpp`, MIT) into C99 with `PY_VERSION_HEX` gates.

## The problem an asm stack swap does NOT solve

Swapping the C stack pointer (spec 01) moves the *machine* execution context. But
CPython keeps a chunk of each running coroutine's state on the **`PyThreadState`**
(one per OS thread), not on the C stack — and runloom multiplexes many fibers
onto one tstate (one per hub). If you swap C stacks without saving/restoring those
tstate fields, fiber B resumes reading fiber A's interpreter state. The
result is not subtle: stale exception objects, corrupted frame chains, segfaults
in `_PyErr_SetObject` during the next exception cascade.

> This is *the* reason the scheduler is in C. An earlier Python-over-greenlet
> prototype could run one fiber but tangled the `tstate.cframe` chain across
> stacks and crashed. Correctly multiplexing Python frames across C stacks
> requires touching these private tstate fields, which is C-only.

## The simple model

Per fiber, keep a `runloom_pystate_snap`. On **yield/park/sleep**, *copy out*
the live tstate fields into the snap (and transfer ownership of the owned ones).
On **resume**, *copy them back* into the tstate. The scheduler's own tstate fields
are restored after the swap returns. Save and load must balance exactly.

```
  resume(g):  load g->snap  -> tstate ;  swap into g ;  (g runs)
  yield/park: snap tstate    <- tstate ;  swap out of g
```

## What's in the snapshot (and why each field is there)

The exact set is version-gated; conceptually:

- **contextvars `context`** (owned ref) — so each fiber has its own
  `contextvars.Context` (request-id/OTel/structlog middleware depends on this).
- **datastack arena pointers** (`datastack_chunk`, `datastack_top`,
  `datastack_limit`) — CPython stores Python *frames* in a separate "datastack"
  arena, not the C stack. Each fiber needs its own frame chain; these three
  pointers are the frame arena's cursor and must move with the fiber.
- **exception state** (`exc_info`, `exc_state`) — the active exception-handling
  chain.
- **`current_exception`** (`tstate->current_exception`) — the in-flight *unraised*
  exception (set mid-`PyErr_SetObject`). **Critical:** at high concurrency a
  fiber yields with this non-NULL, another overwrites it, and the first reads
  a freed object on resume → segfault in the next exception cascade (e.g. an async
  function's `StopIteration` on return). Forgetting this field is a real crash.
- **recursion counters** — 3.11 has one (`recursion_remaining`); 3.12+ split into
  `py_recursion_remaining` + `c_recursion_remaining`. They are **reset at
  fiber entry** (`py_recursion_remaining = Py_GetRecursionLimit()`,
  `c_recursion_remaining = 200`, [runloom_sched_core.c.inc:226-239](../src/runloom_c/runloom_sched_core.c.inc#L226))
  and then saved/restored per fiber here — together that makes deep recursion
  raise a catchable `RecursionError` *per fiber* instead of leaking the
  counter across the OS thread (spec 10).
- **the current frame** — 3.11/3.12 thread a `_PyCFrame` on the C stack;
  3.13 removed `cframe` and put `current_frame` directly on tstate. Gated
  accordingly. Also `delete_later` (the trashcan chain, owned) and
  `trash_delete_nesting` on the versions that have them.

The version gating is not incidental: **the frame-snapshot depends on the 3.11+
tstate layout, which is why runloom needs Python ≥ 3.11 even on a GIL build.**

## The datastack: pooling and idle reclaim

Because frames live in the datastack arena, a first-run fiber needs a root
chunk. `first_run_install_datastack` pulls one from a per-thread pool (or leaves
the fields NULL so PyEval arena-allocates — either is correct). At completion,
`drain_g_datastack` frees the g's chunk chain back to that pool **before** loading
any other snap (loading first would overwrite the pointers and leak the chunks) —
this matches greenlet's `did_finish`. A parked fiber's *idle* datastack tail
(above `datastack_top`, up to `datastack_limit`) can be `MADV_DONTNEED`'d under
the same owning-hub-while-suspended contract as the C-stack sweep (spec 01).

## Two free-threaded-only hazards the snapshot must handle

These only exist on 3.13t (GIL off) and are the bridge between this spec and
spec 09:

### Critical sections (`critical_section` field + `runloom_critsec_*`)

On 3.13t, CPython takes a *per-object critical section* (e.g. a dict's `ma_mutex`)
around operations like a dict key `__eq__`. If that `__eq__` **yields** (a
fiber parks mid-comparison), the held mutex would stay locked across the swap
— and because runloom does **not** detach the tstate on a cooperative park, every
*other* hub then deadlocks on that mutex, and the chain leaks across fibers.

Fix: on **snap**, `runloom_critsec_suspend(tstate)` releases all held critical
sections and returns the saved (deactivated) chain; on **load**,
`runloom_critsec_restore` reinstalls it and re-locks the top. This mirrors what
CPython itself does at a real detach. It lives in the `Py_BUILD_CORE`-isolated TU
(`runloom_iframe.c`) because `_PyCriticalSection_*` are internal.

### Cross-hub exception-chain re-rooting

When a g suspended inside active exception handling, the bottom per-g
`_PyErr_StackItem`'s `previous_item` points at the **origin hub's**
`&tstate->exc_state` — hub-bound. If that g is ever resumed on a *different* hub
(only under the experimental `RUNLOOM_STEAL_WOKEN` / per-g-tstate path), the snap
records `exc_chain_bottom` so load can re-root it onto the target hub's
`&exc_state`. (In the default per-hub-tstate mode this is NULL — see below.)

## The two tstate modes

- **Per-hub tstate (default).** One `PyThreadState` per hub; fibers share it
  via this snap/restore dance. A started fiber's suspended eval frame caches
  its origin hub's tstate, so it is **not** safely migratable to another hub —
  which is *why work-stealing only steals fresh fibers* (spec 05). `snap`/`load`
  do the real work.
- **Per-g tstate (`RUNLOOM_PER_G_TSTATE`, experimental, NON-default).** Each g
  owns a `PyThreadState`; `snap` no-ops and the M:N layer attaches/detaches the
  g's own tstate. This *could* allow cross-hub migration — but it is structurally
  unsound (spec 09): a g's tstate carries a thread-bound mimalloc heap and brc
  binding, so running it on another hub aborts under `--with-pydebug`. Left as a
  documented dead-end; the default mode is the supported one.

## Invariants

1. **Save and load balance.** Every `snap` is matched by exactly one `load`;
   the snap is *valid only while the g is suspended*.
2. **Save `current_exception` and the recursion counters.** Omitting either is a
   real crash (stale-exception segfault; cross-fiber recursion-limit leak).
3. **Free the datastack chain before loading any other snap** at completion.
4. **On 3.13t, suspend critical sections on snap and restore on load** — else a
   park inside a CPython critical section deadlocks every other hub.
5. **Do not migrate a started fiber across hubs in per-hub-tstate mode.** Its
   eval frame is bound to the origin tstate. (The `RUNLOOM_DIAG_MIGRATE`
   `origin_tstate` field exists to *detect* a violation, not to permit it.)
