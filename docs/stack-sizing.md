# Stack sizing & memory

Each goroutine in runloom owns a private C stack -- this is what enables
the "looks-like-a-thread, costs-like-a-callback" cooperative model.
This page explains how runloom manages that stack so you can run a lot of
goroutines at once without burning memory.

## The mechanisms in one paragraph

runloom defaults to **256 KB** per goroutine but auto-calibrates: every
fresh stack is painted with a sentinel pattern, every completed
goroutine's high-water mark is scanned, and after 1 000 completions
the default is locked to `next_pow2(max_hwm × 4)` clamped to
`[16 KB, 8 MB]`.  When a goroutine finishes, its stack returns to a
per-thread pool with `MADV_DONTNEED` applied -- the kernel reclaims the
physical pages while keeping the virtual mapping.  Net effect: 10 000
idle goroutines cost about as much RAM as their actual stack usage,
not their reservation.

## Why goroutines have stacks at all

Stackful coroutines (runloom, greenlet, gevent, Go) keep the C stack
*per coroutine*.  Switching between them is a single `swap` instruction
that saves callee-saved registers, swaps the stack pointer, and
restores the new context -- ~80 ns on x86_64.

Stackless coroutines (asyncio, Trio, vanilla Python `async def`) keep
state in heap-allocated frame objects and switch by returning to a
trampoline.  No per-coroutine C stack -- but every `await` requires
allocating frame state and walking back through the event loop.

Both models are valid; runloom picks stackful because the switch cost is
~22× lower and the user code can be ordinary blocking-style without
async/await colour.  The cost is *per-goroutine memory* -- which is
exactly what this page is about minimising.

## Auto-calibration

The first 1 000 goroutines run with the generous default (256 KB) and
get their stacks painted on creation, scanned on completion.  After
the calibration window:

```python
import runloom

s = runloom.stats()
print(s["stack_size_default"])    # the new default (post-calibration)
print(s["stack_hwm"])              # max bytes any goroutine actually used
print(s["stack_completed"])        # how many goroutines were measured
print(s["stack_calibrated"])       # 1 once frozen
print(s["stack_painting"])         # 0 once painting is disabled
```

Typical numbers:

| Workload | Calibrated default |
| --- | --- |
| Trivial Python (`count += 1`) | 16 KB |
| Stdlib socket I/O loops | 16 KB |
| `json.dumps` of 100-deep nested dict | 64 KB |
| `re.match` on big inputs | 32–64 KB |

The 4× safety factor means actual usage stays well under the
calibrated value; you'd need a 4× spike from one goroutine to the
next to risk overflow.

### What's measured

The scan walks the goroutine's stack memory in 8-byte chunks looking
for the deepest non-sentinel word.  This catches anything the
goroutine actually wrote -- including local C variables, frame
linkage, saved registers, deep Python recursion.

What it *misses*: a goroutine that allocates a 50 KB local C buffer,
writes through it briefly, and then returns before yielding.  The
peak is real but transient -- the sentinel scan only sees what was
still in memory at the moment we ran it.  In practice this rarely
matters because the safety factor (4×) covers reasonable transients.

## `MADV_DONTNEED` on pool release

When a goroutine finishes, its stack returns to a per-thread free
list capped at 4 096 entries.  Without `MADV_DONTNEED` that would
mean **4 096 × stack_size** resident memory -- at the default 256 KB
that's 1 GB just for idle pool entries.

The release path calls `madvise(addr, size, MADV_DONTNEED)` on
everything except the first 4 KB (which holds the pool's
linked-list pointer).  The kernel reclaims the page frames; the
mapping itself stays.  Next time the stack is reused, the goroutine
faults in fresh zero pages as it writes -- same correctness as a brand
new mmap, but no syscall.

Measured: after a burst of 5 000 goroutines × 1 MB stacks, RSS lands
at ~21 MB (one page per pool entry + the goroutines' actual usage,
mostly headers).  Without `MADV_DONTNEED` that workload would hold
~5 GB.

This is a Linux/POSIX optimisation.  On Windows (Fibers backend) the
OS manages stacks differently -- runloom lets it.

## Per-call override

```python
import runloom

# Goroutine known to recurse deeply or call into a heavy C extension:
runloom.go(deep_handler, stack_size=512 * 1024)

# Pure-compute callable that you've confirmed fits in 8 KB:
runloom.go(tight_loop,  stack_size=8 * 1024)
```

The `stack_size=N` kwarg overrides the calibrated default for that
single spawn.  The default is unaffected.

Use this for the rare outlier -- most goroutines should use whatever
the scheduler calibrated to.

## Locking a known-good size

If you don't want to spend the first 1 000 goroutines running at the
generous default, lock the size up-front:

```python
import runloom

# Before any runloom.go() call:
runloom.set_stack_size(32 * 1024)

# Subsequent goroutines use exactly 32 KB:
runloom.go(worker)
```

`set_stack_size` also **freezes** calibration (no further auto-tuning)
and disables painting (no per-spawn overhead).  Use this when:

- You've already calibrated on a representative workload and want the
  same size in production.
- You're memory-constrained and want a small fixed size from the start
  (and you're willing to take responsibility for sufficiency).
- You're running a benchmark and want the size to not drift.

```python
import runloom

print(runloom.get_stack_size())   # current default
```

Bounds: `[16 KB, 8 MB]`.  Below or above is silently clamped.

## What's a "safe" stack size?

A pure-Python goroutine doing socket I/O typically uses **< 1 KB** of
C stack -- Python's interpreter loop stores frames in the *datastack*
(a separate arena), not the C stack.  C extensions that recurse on
the C stack (`json.dumps`, `re`, nested function calls in extension
code) push the usage up.

Empirical rules of thumb:

- **8 KB**: only for trivial computational loops with no I/O and no
  deep Python recursion.  Below 16 KB you're flirting with `RuntimeError:
  maximum recursion depth exceeded`.
- **16 KB**: fine for typical server handlers (socket I/O, JSON
  parsing of normal payloads, simple state machines).
- **64 KB**: safe for most code including moderately deep call graphs
  through stdlib code.
- **256 KB+**: deep recursion, heavy C extensions (XML parsers, ORMs
  with deep query trees).

When in doubt, run with calibration on, look at the measured
`stack_hwm`, and lock a value that gives you ≥ 4× headroom.

## Inspecting current usage

```python
import runloom

# Snapshot of calibration state
print(runloom.stats())
# {
#   'ready': 0, 'sleeping': 0, 'netpoll_parked': 0,
#   'completed': 1042, 'running': 0,
#   'stack_size_default': 16384,
#   'stack_hwm': 768,
#   'stack_completed': 1000,
#   'stack_calibrated': 1,
#   'stack_painting': 0,
#   ...
# }
```

Useful for production observability -- log the calibrated size on
startup and the high-water mark periodically.

## Defending against overflow

A goroutine running low on stack is defended in layers -- the same kind of
protection the main thread gets, scaled to the goroutine's smaller stack:

- **Deep recursion raises `RecursionError`, not a crash.** CPython's
  C-recursion counter is tracked per goroutine (saved and restored across
  yields), so unbounded Python *or* C recursion (`json`, `re`, deeply nested
  calls) hits a catchable `RecursionError` well before the stack overflows.
- **Stacks grow on demand.** At each resume boundary a goroutine whose headroom
  has dropped below a quarter of its stack is copied onto a stack twice as big
  (`RUNLOOM_STACK_GROW`, default on; `RUNLOOM_STACK_GROW=0` disables). A goroutine
  that gradually deepens grows with it.
- **Every stack has a guard page.** A `PROT_NONE` page sits just below each
  goroutine stack (the OS provides one on the Windows Fibers backend). An
  overflow faults *immediately and cleanly* at the guard rather than silently
  scribbling over a neighbouring stack. runloom does not yet turn that fault into a
  friendly message -- it currently surfaces as a `faulthandler` segfault
  traceback (a diagnostic handler is a possible follow-up).
- **CPython's stack-hungry error paths are neutralised.** A missing-attribute
  lookup on a module makes CPython 3.13 reserve a 32 KB path buffer just to
  build a "did you shadow a stdlib module?" hint -- on its own larger than a
  default goroutine stack. runloom skips that hint while on a goroutine (the
  `AttributeError` is otherwise unchanged), so `getattr`/`hasattr` misses on a
  module can't blow the stack, by any lookup path.

Between them those cover everything that's actually come up in practice. The
residual is a **single native/FFI C frame larger than the whole goroutine
stack** -- a deeply-nested C-extension call that bypasses CPython's recursion
counter and is big enough to jump the guard page in one allocation. With
`-fstack-clash`-compiled code (CPython itself) that still faults cleanly at the
guard; a non-probing extension could corrupt. If you have such a goroutine,
give it a bigger stack up front:

```python
runloom.set_stack_size(128 * 1024)        # process-wide default floor
# or just the suspicious goroutine:
runloom.go(work, stack_size=512 * 1024)
```

So the 16 KB minimum is a *floor for the calibrator*, not a blanket "safe for
anything" size: it works because recursion is bounded, stacks grow, and the one
oversized CPython frame is handled -- not because 16 KB fits every possible C
call.

## Putting it all together

For a production service:

```python
import runloom

# Optional: pre-calibrate during a dry-run, then lock for production
runloom.set_stack_size(32 * 1024)        # whatever your dry-run found

# Spawn workers
for i in range(10000):
    runloom.go(worker)

runloom.run_single()

# Inspect after the burst
print("peak resident usage:", runloom.stats())
```

For exploratory work or benchmarks, just let calibration run and
inspect `stats()` afterwards.

## Roadmap

- Guard page (`mprotect PROT_NONE` on the lowest page) + `SIGSEGV` handler
  that raises a clean `StackOverflowError` instead of crashing.
- Per-thread calibration so M:N hubs converge independently.
- Adaptive shrink (currently only grows during calibration).
