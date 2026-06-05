# 10 — Stack safety and sizing

Ground truth: `docs/stack-sizing.md`, `docs/cooperative_stdlib_coverage.md`,
`coro.c` (guard page, copy-grow, mincore HWM — spec 01),
`runloom_stackadvice.{h,c}`, `runloom_sched.c` (calibration),
`runtime.py` (`grow_down_*`), `runloom_heavy_frames.h` + `tools/heavy_frames/`.

## The problem

Small per-goroutine stacks are what make a million goroutines affordable (spec 01
is the mechanism; this is the *policy*). But "small" fights "safe": a too-small
stack overflows. The design answer is **layered defense** so an underestimate is
always a *clean classified crash, never silent corruption*, plus several sizing
strategies so the common case is small without you thinking about it.

## The defense layers (each handles a different overflow cause)

A goroutine running low on stack is defended the way the main thread is, scaled
down:

1. **Deep recursion raises `RecursionError`, not a crash.** At goroutine entry
   the per-tstate recursion counters are *reset for this goroutine* —
   `py_recursion_remaining = Py_GetRecursionLimit()` and
   `c_recursion_remaining = 200` (the comment: "200 frames matches what a 128 KB
   stack can safely hold") — and then **saved/restored across yields** in the
   snapshot (spec 03). So unbounded Python *or* C recursion (`json`, `re`, nested
   calls) hits a catchable `RecursionError` well before the stack overflows.
   ([runloom_sched_core.c.inc:226-239](../src/runloom_c/runloom_sched_core.c.inc#L226))
   Note this contradicts `docs/cooperative_stdlib_coverage.md`, which says the C
   counter is "NOT lowered per goroutine" — that doc is stale; the code lowers it
   to 200 at entry.
2. **Stacks grow on demand** (copy-grow / Path A, spec 01). A goroutine that
   gradually deepens *across yields* is copied onto a 2× stack at the resume
   boundary. `RUNLOOM_STACK_GROW=0` disables.
3. **Every stack has a `PROT_NONE` guard page** (spec 01). An overflow faults
   immediately and cleanly at the guard instead of scribbling on a neighbor.
4. **The crash reporter classifies the guard fault** (spec 11) into "goroutine N
   overflowed its K-KiB stack" instead of a bare segfault.
5. **CPython's stack-hungry error paths are neutralized.** A missing-attribute
   lookup on a *module* makes CPython 3.13's `_PyModule_IsPossiblyShadowing`
   reserve **~32 KB of C stack** (two `wchar_t[MAXPATHLEN]` path buffers) just to
   build a "did you shadow a stdlib module?" hint — larger than a *small* g-stack
   (the grown-down/raw sizes below). runloom overrides `PyModule_Type.tp_getattro`
   to raise the plain `AttributeError` without that hint *only while on a
   goroutine* (off-goroutine the stock hint is byte-for-byte preserved), so
   `getattr`/`hasattr` misses on a module can't blow the stack.
   ([module_init.c.inc:334-419](../src/runloom_c/module_init.c.inc#L334)) (This
   ~32 KB is the *hint buffer*, not the goroutine stack default — see the next
   section; do not confuse them.)

The **residual**: a *single native/FFI C frame larger than the whole goroutine
stack* — a non-probing extension that jumps the guard in one allocation. That's
the one case the layers can't catch; the fix is to size that goroutine up front
(`stack_size=`) or offload it.

## The two oversized-frame cases the stdlib actually has

From a measured sweep of ~40 stdlib leaf ops, the fat-frame surface is **exactly
two single frames**, both handled by *removing the frame from the goroutine path*
rather than enlarging the stack:

- **`select.select` — 50.9 KB** (three `pylist[FD_SETSIZE+1]` arrays). Reimplemented
  cooperatively in `monkey/polling.py` (park on a transient epoll's own fd via
  `wait_fd`) so CPython's `select_select_impl` is never reached from a goroutine
  (spec 06/14).
- **first `ssl` use — ~40 KB** (OpenSSL's one-time library init). Warmed on the
  main thread: `runloom.monkey` imports `ssl` and forces OpenSSL init on the 8 MB
  main stack, pre-paying it off any goroutine.

And one *depth* (not single-frame) case: **`ast.parse`/`compile`** cost ~1.5 KB of
C stack per recursion level, so deeply-nested source SEGVs at ~20 levels — before
the recursion counter fires. Auto-offloaded (the `compile` patch routes it to the
pool's full-size thread stack, where it degrades to a clean `RecursionError`).
`json`/`pickle`/`marshal`/`deepcopy` cost ~60–80 B/level, so the counter fires
first → clean `RecursionError`; the common DoS-relevant case (nested untrusted
JSON in a goroutine) is **safe**. `eval`/`exec` of a string compile internally in
C and need the caller's namespace, so they're the documented residual (use
`offload()` or a roomier stack).

> **The one-line rule for writing stack-predictable goroutines**: keep stack depth
> bounded and *input-independent* — iterate or use a heap-allocated work stack
> instead of recursing through native code. Pure-Python recursion is already safe
> (it runs on the growable datastack, degrades to `RecursionError`). C-bouncing
> recursion on *input* depth is the dangerous case; flatten it or pin the stack.

## The sizing strategies (how "small by default" actually happens)

There are several, layered, with an explicit precedence:

**The numbers, from the code** (not the docs — `docs/cooperative_stdlib_coverage.md`
still says "32 KB default" everywhere, which is **stale by a prior default change**;
the true values are in `runloom_sched_datastack.c.inc:521-525`):

| constant | value | where |
|---|---|---|
| `RUNLOOM_DEFAULT_STACK_SIZE` (scheduler default) | **512 KB** | [datastack.inc:521](../src/runloom_c/runloom_sched_datastack.c.inc#L521) |
| `RUNLOOM_MIN_STACK_SIZE` (floor, explicit override only) | 16 KB | [datastack.inc:522](../src/runloom_c/runloom_sched_datastack.c.inc#L522) |
| `RUNLOOM_MAX_STACK_SIZE` (cap) | 8 MB | [datastack.inc:523](../src/runloom_c/runloom_sched_datastack.c.inc#L523) |
| calibration target / safety | 1000 completions / ×4 | [datastack.inc:524-525](../src/runloom_c/runloom_sched_datastack.c.inc#L524) |
| raw `runloom_c.Coro()` / `TCPConn` constructor default | 128 KB | [module_coro.inc:37](../src/runloom_c/module_coro.c.inc#L37), [module_tcp.inc:46](../src/runloom_c/module_tcp.c.inc#L46) |
| `runloom_c.go` (stack_size=-1) / `mn_go` (stack_size=0) | resolve to the 512 KB scheduler default (via `advice_size_for`) | [module_go.inc:56](../src/runloom_c/module_go.c.inc#L56), [mn_sched_init_fini.inc:385](../src/runloom_c/mn_sched_init_fini.c.inc#L385) |
| aio task / io goroutine (`_TASK_STACK`/`_IO_STACK`) | 512 KB | [aio/_base.py:56](../src/runloom/aio/_base.py#L56) |

So the default a normal `go()`/`mn_go()` goroutine gets is **512 KB** (or the
grow-down learned size under M:N), *not* 32 KB. The only real 32 KB in the system
is the CPython module-hint buffer above.

**Precedence (highest wins):** explicit `runloom.go(fn, stack_size=N)` > the opt-in
C auto-sizer (if you turned it on) > the default-on grow-down (M:N) > the
process-wide calibrated default.

1. **Calibration** ([runloom_sched_core.c.inc:17-47](../src/runloom_c/runloom_sched_core.c.inc#L17)).
   The first 1000 goroutines run on the 512 KB default; their HWMs are scanned
   (mincore, paint-free); then the scheduler locks the default to
   `next_pow2(max_hwm × 4)` and turns off measurement. **It only ever adapts
   *up*** — `if (chosen < RUNLOOM_DEFAULT_STACK_SIZE) chosen = ...DEFAULT` floors
   it at 512 KB so it never re-exposes the deep (`Decimal`/crypto) cases; the
   "trivial → 16 KB calibrated" rows in `docs/stack-sizing.md` are *grow-down*,
   not calibration. A process-wide single default.
2. **Function-bound grow-down** (`runtime.py`, **default-on under M:N**). "The
   function IS the database row": the first `go(fn)` runs on the safe cold-start
   default, measures the real HWM on return, and writes `next_pow2(hwm × 4)`
   (floored 16 KB) onto `fn.__dict__["runloom_stack"]`. Later spawns of `fn` reserve
   only that. The stored size is the **monotone max over the first 64 spawns**,
   then frozen (steady state = one dict lookup, zero measurement). A trivial handler
   settles at 16 KB instead of 512 KB (32× cut). Restricted to M:N because under
   single-thread `run(1)` the per-spawn learning is pure overhead and the
   scale-only memory win isn't on the table. **It only ever shrinks *from* a size
   the function already ran on**, so it never reserves *more* than known-safe.
3. **Opt-in C auto-sizer + prescan** (`runloom_stackadvice.c`,
   `RUNLOOM_STACK_AUTOSIZE`). "Start large, learn down": an unseen kind starts at a
   generous size (256 KiB) and shrinks to `next_pow2(peak × 4)` once measured.
   Over-sizing the first few is cheap (the idle pages are returned to the OS on
   park, so they cost address space, not RSS). `prescan=True` adds a cold-start
   optimizer: loosely scan an unseen kind's bytecode for symbols whose C impl has a
   **fat single frame** — chiefly `Decimal` (a single `_decimal` frame is 256 KiB,
   the fattest in 3.13's stdlib) — and start big enough to hold it; and treat
   **crypto** symbols (`encrypt`/`sign`/`Fernet`/`HKDF`/…) heuristically by
   starting at 1 MiB (their cost is cumulative call *depth* in third-party C, not
   one fat frame, so a name list — `tools/heavy_frames/` — is used, needing no
   third-party install). A prescan-matched kind never learns *down* below its
   cold-start floor (matching the symbol is itself the signal it *can* go deep).
4. **Advisory profiler** (`enable_stack_advice` / `print_stack_advice`). Measures
   per-kind real usage and *suggests* sizes; never changes anything. You read and
   apply.

### Why sizes are never persisted to disk

A remembered-small size is only ever a **lower bound** on what a *future input*
might need (recursion depth is data-dependent). Persisting it would be a foot-gun
across restarts/deploys — the run that finally gets the deep input would load a
too-small size. So all learning is **in-memory only**; the guard page, on-demand
growth, and crash reporter remain the safety net for any underestimate.

## The pool memory discipline that makes "reserve big, pay small" true

(Mechanism in spec 01.) A reserved stack is virtual, demand-paged; the unused tail
costs no RAM until touched. On goroutine completion the stack returns to a
per-thread pool with `MADV_DONTNEED` (Linux/POSIX) so the pool holds ~one page
each, not the full size. On a long park, `runloom_coro_park` drops the below-SP
idle pages. Net: 10k idle goroutines cost ~their actual usage, not their
reservation; a burst of 5000 × 1 MB stacks lands at ~21 MB RSS, not ~5 GB. On
Windows, `CreateFiberEx` (not `CreateFiber`) reserves big but commits only a small
floor, growing on demand — the same "reserve big, pay for what you touch."

## Invariants

1. **An underestimate is always a clean classified crash, never corruption** —
   guarded by the PROT_NONE page + per-g recursion counter + the module-hint skip.
2. **Learned sizes only shrink from a known-safe cold start; they are never
   persisted.** The guard page is the backstop for the residual deep-input case.
3. **Calibration/grow-down/auto-sizer have a strict precedence**; an explicit
   `stack_size=` always wins (and skips all learning).
4. **The two fat stdlib frames are removed from the goroutine path** (cooperative
   `select`, main-thread `ssl` warm), not papered over with a bigger default.
5. **Reserved stacks are virtual + pooled + madvise-reclaimed**, so reserving big
   costs address space, not RSS.
