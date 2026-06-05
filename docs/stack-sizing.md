# Stack sizing & memory

Each goroutine in runloom owns a private C stack -- this is what enables
the "looks-like-a-thread, costs-like-a-callback" cooperative model.
This page explains how runloom manages that stack so you can run a lot of
goroutines at once without burning memory.

## The mechanisms in one paragraph

runloom defaults to **512 KB** per goroutine -- enough to cover everything a
goroutine realistically does (the deepest stdlib frame, `_decimal` at 256 KB;
full TLS/SSH crypto in a callback; nested parsers) without hitting the guard
page.  It's cheap because stacks are demand-paged (virtual, not resident -- the
unused tail costs no RAM until touched).  Calibration scans completed
goroutines' high-water marks and after 1 000 completions can adapt the default
**up** to `next_pow2(max_hwm × 4)` (clamped at 8 MB) for stack-hungry programs --
but it is **floored at 512 KB** and never auto-shrinks below it.  (Reclaiming
the tail per function is the **grow-down**'s job -- on by default under M:N, see
the next section; an explicit `set_stack_size()` can still go down to 16 KB.)
When a goroutine finishes, its stack returns to a
per-thread pool with `MADV_DONTNEED` applied -- the kernel reclaims the
physical pages while keeping the virtual mapping.  Net effect: 10 000
idle goroutines cost about as much RAM as their actual stack usage,
not their reservation.

## Automatic grow-down (on by default, M:N)

Under M:N scheduling (`run(n)` with `n > 1`) runloom **learns each function's
real stack need and reserves only that** -- automatically, no setup. This is the
function-bound *grow-down*, and it is **on by default**.

The first time you `runloom.go(fn)` a function, its goroutine starts at the safe
default stack (a "cold start" -- a size the function is known to complete on),
measures its real C-stack high-water mark on return, and writes a derived size
back onto the function itself (`fn.__dict__["runloom_stack"]` -- the function
*is* the lookup row). The next spawn of that same function reserves only
`next_pow2(measured_hwm × 4)`, floored at 16 KB. The stored size is the running
**max** over the first 64 spawns, then frozen -- after that, spawning is a single
dict lookup with no measurement overhead. A trivial handler settles at 16 KB
instead of 512 KB (a 32× cut); a `json.dumps`-heavy one at ~64 KB.

It only ever shrinks *from* the cold start -- a size the function already ran on
-- so it never reserves **more** than a known-safe amount. The one residual
risk, an input that drives the function deeper than any of the first 64 sampled
runs, lands on the guard page as a clean classified crash (never corruption) and
re-learns next process. The learned size is **in-memory only, never persisted**:
a remembered-small size would be a foot-gun across restarts -- the run that
finally gets the deep input would load a too-small size.

### Turning it off

```python
import runloom

runloom.set_grow_down(False)     # reserve the fixed default for every goroutine
runloom.grow_down_enabled()      # -> current state
```

or set `RUNLOOM_GROW_DOWN=0` in the environment before `import runloom`. A
per-call `runloom.go(fn, stack_size=N)` pin always wins regardless -- use it to
opt a single function out and choose its exact size. The grow-down also steps
aside automatically when you explicitly enable the opt-in C auto-sizer
([below](#letting-runloom-size-them-for-you)) -- the sizer you turned on by hand
wins, since it may *deliberately* over-reserve (the crypto prescan's 1 MiB
margin) where grow-down would measure-and-shrink.

### Scope

- **M:N only.** Single-thread `run(1)` (the GIL/compat path) keeps the fixed
  default -- there the per-spawn learning is pure overhead (a tight spawn loop
  runs nothing until it finishes, so the sampler can't amortise) and the memory
  win, which only pays off at scale, isn't on the table.
- **Per `runloom.go()`-spawned function.** Goroutines spawned through the raw C
  entry points (`runloom_c.mn_go`) use the plain default; the learning lives in
  the friendly `runloom.go()` wrapper. Arg-bearing `runloom.go(fn, x)` binds the
  size to `fn` (shared across all its arg variants), not the per-call wrapper.

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

The first 1 000 goroutines run with the generous default (**512 KB** --
`RUNLOOM_DEFAULT_STACK_SIZE`, `src/runloom_c/runloom_sched_datastack.c.inc`) and
get their stacks measured (resident-page high-water mark) on completion.  After
the calibration window the default is locked to `next_pow2(max_hwm × 4)` but
**floored at 512 KB** -- calibration only ever adapts *up*, never below the safe
default.  After:

```python
import runloom

s = runloom.stats()
print(s["stack_size_default"])    # the new default (post-calibration)
print(s["stack_hwm"])              # max bytes any goroutine actually used
print(s["stack_completed"])        # how many goroutines were measured
print(s["stack_calibrated"])       # 1 once frozen
print(s["stack_painting"])         # 0 once painting is disabled
```

Typical numbers (these are the **per-function grow-down** sizes -- the default-on
M:N auto-sizer that shrinks each function to its measured need; *calibration*
itself never goes below the 512 KB floor, see above):

| Workload | Grown-down size |
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

Running **millions** of shallow goroutines and want to reclaim the 512 KB
default's virtual footprint? Lock a smaller size up-front (an explicit size
overrides the default and its floor, down to the 16 KB hard minimum):

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
  scribbling over a neighbouring stack. With the crash reporter installed
  (`runloom.inspect.install_crash_handler()` or `RUNLOOM_CRASH=on`) that fault
  is turned into a classified message that *names the overflowing goroutine and
  its stack size* instead of a bare segfault -- see
  [Crash reporting](debugging.md#crash-reporting-sigsegv--sigbus).
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

## Right-sizing with the advisory profiler

The calibrator picks one global default from the deepest goroutine it sees, and
the layers above stop overflow turning into corruption. But to know whether a
*particular* goroutine kind is over-reserving (wasting address space at high
goroutine counts) or running close to its limit (a candidate for an explicit
bigger `stack_size`), measure it directly:

```python
import runloom

runloom.inspect.enable_stack_advice()      # opt-in; keeps stack painting on
... run your real workload ...
runloom.inspect.print_stack_advice()
```

```
=== runloom stack advice (3 kinds) ===
samples  max_use  reserved  suggested  kind
   4012      41K       32K        16K   app.handle_request (server.py:88)  (tight -- consider a bigger stack)
  12030       1K       32K        16K   app.heartbeat (server.py:204)  (over-reserved)
    980      11K       32K        16K   app.parse_json (codec.py:55)
```

Each row is one **goroutine kind** (its entry callable), with the deepest C
stack any goroutine of that kind actually used (`max_use`) versus what it
reserved, plus a `suggested` `stack_size` that covers the observed peak with
margin. `runloom.inspect.stack_advice()` returns the same data as a list of
dicts (`kind`, `samples`, `max_hwm`, `reserved`, `suggested`).

It is **purely advisory**: runloom never changes or persists a stack size from
this -- a remembered-small size is only ever a lower bound on what a future
input might need (recursion depth is data-dependent), so the guard page and
crash reporter stay the safety net. You read the advice and apply it yourself,
e.g. give the `tight` kind a roomier stack:

```python
runloom.go(handle_request, stack_size=128 * 1024)
```

Enabling the profiler keeps stack painting on (a small per-spawn cost) for the
session; it is off by default and costs nothing until you turn it on.

### Letting runloom size them for you

If you'd rather not read the table and apply sizes by hand, turn on the
**adaptive auto-sizer**, which does it automatically:

```python
runloom.inspect.enable_stack_autosize()    # or RUNLOOM_STACK_AUTOSIZE=1
```

It works by **starting large and learning down**: the first time a goroutine
kind is seen its goroutines start at a generous size (256 KiB by default,
`RUNLOOM_STACK_AUTOSIZE_START`); once runloom has measured how much C stack that
kind really uses, its later goroutines start at the learned size
(`next_pow2(peak * 4)`). A kind that turns out shallow shrinks toward the floor;
a deep one settles at a roomy size. Because over-sizing the first few is cheap
(the idle pages are returned to the OS on park -- the auto-sizer turns park-time
reclaim on -- so they cost address space, not RSS) and under-sizing is the only
dangerous direction, "start large, learn down" is the safe polarity.

It is **in-memory only and never persisted to disk.** A remembered-small size
is only a lower bound on what a *future* input might need (recursion depth is
data-dependent), so writing it out would be a foot-gun across restarts and
deploys -- the run that finally gets the deep input would load a too-small size.
The guard page, on-demand growth, and the crash reporter remain the safety net
for any underestimate. An explicit `runloom.go(fn, stack_size=...)` always wins
over the auto-sizer. Off by default (it changes per-kind stack sizes); enable it
before the runtime starts so kinds are sized from their first spawn.

#### Cold-start scan (`prescan=True`)

The auto-sizer's one weak moment is a kind's *first* goroutine: it starts at the
generic large default before anything has been measured, so a kind that needs
more than that on its very first run can overflow before learn-down ever sees
it. The standout offender is `Decimal` arithmetic -- a single
`_decimal` frame (`squaretrans_pow2`, big-integer multiply/pow) is **256 KiB**,
the fattest single frame in the whole 3.13 stdlib.

```python
runloom.inspect.enable_stack_autosize(prescan=True)   # or RUNLOOM_STACK_AUTOSIZE=prescan
```

With `prescan` on, an unseen kind's bytecode is loosely scanned for symbols whose
C implementation has a known fat single frame (from a DWARF `.eh_frame` profile
of the stdlib -- see `tools/heavy_frames/`). If it references one, the kind
starts big enough to hold that frame (the **largest** matched frame, never a sum
-- only the deepest single frame constrains the stack), so it survives its first
run; learn-down then measures the real usage and right-sizes from there. Only a
handful of stdlib symbols qualify (chiefly `Decimal`); everything else cold-
starts at the normal default. The scan is one-level and name-based, so it is a
loose heuristic, not a guarantee -- the guard page and crash reporter still
backstop anything it misses.

**Cryptography** is the other cold-start class, handled the same way but for a
different reason. Signing, verification, AEAD encryption and KDFs route through
deep, *cumulative* native call chains in third-party C (`cryptography`/OpenSSL,
PyNaCl/libsodium, PyCryptodome) -- enough to overflow the small default on the
first call. Those libraries aren't in the stdlib profile (their binaries vary by
version/platform/build, are usually stripped, and the cost is call *depth*, not
one fat frame), so prescan treats them **heuristically**: a kind whose bytecode
references a crypto symbol (`encrypt`, `decrypt`, `sign`, `verify`, `Cipher`,
`Fernet`, `Ed25519PrivateKey`, `HKDF`, ...) cold-starts at **1 MiB**. The symbol
list is in `tools/heavy_frames/gen_heavy_frames.py` (a *name* list, not a
measured-size table -- it needs no third-party installs and covers libraries
runloom has never seen); keep additions crypto-specific so a false match only
over-provisions virtual stack.

**Heuristic floor.** Unlike ordinary kinds, a prescan-matched kind (crypto,
`Decimal`) never learns *down* below its cold-start size, even if its measured
runs are shallow. Matching the symbol is itself the signal that the kind *can*
go deep, so a run of small inputs must not shrink it under the size that
protects the deep path it didn't happen to exercise this time (a small RSA sign
now, a 4096-bit one later). Learn-down still right-sizes everything else; the
floor only pins the classes whose depth is most likely to surprise you. To
reclaim that memory anyway, pin the kind explicitly with
`runloom.go(fn, stack_size=...)` -- an explicit size always wins, floor and all.

> Arg-bearing spawns are keyed correctly too: `runloom.go(fn, x)` wraps `fn` in
> a binding lambda, but the auto-sizer follows `__wrapped__` to `fn`, so the
> per-kind size and the prescan scan apply to *your* function, not the wrapper.
> (Decorated functions with `functools.wraps` get the same treatment.)

#### Writing stack-predictable goroutines

The auto-sizer is sound exactly when a kind's **stack depth is a function of the
code, not the input**: it measures the high-water mark of the runs it sees and
reserves `next_pow2(worst_seen * 4)`, so a future run that goes more than ~4x
deeper than anything observed can still overflow (it fails safe on the guard
page — a clean classified crash, never corruption — but it is a crash). The
one-line rule that keeps you out of that case:

> **Keep stack depth bounded and input-independent — iterate or heap-stack
> instead of recursing through native code.**

Concretely:

* **Recursion that stays in pure Python is already safe** — it runs on runloom's
  *growable* datastack (proven to ~1M deep) and degrades to a clean
  `RecursionError`, never a stack-overflow SEGV. Depth here is not the
  auto-sizer's concern.
* **Recursion that bounces through C at each level** (a native parser, `ast`/
  `compile`, crypto, regex) consumes the small *C* stack the auto-sizer manages,
  and its depth is usually a function of the *input* (how nested the document
  is). That is the case the headroom cannot guarantee.
* The fix is algorithmic, not stylistic: convert call-stack recursion into an
  explicit **heap-allocated stack/queue** (`while work: node = work.pop(); ...`).
  The depth then lives in heap memory and the C stack stays flat — the
  auto-sizer and the guard page never see it, regardless of how adversarial the
  input is.

For a kind whose native-recursion depth is genuinely input-driven and you can't
flatten it (a third-party recursive parser on untrusted data), don't let it
learn down — pin it with an explicit `stack_size=` (which always wins over the
auto-sizer) or offload the deep call.

## Putting it all together

For a production service:

```python
import runloom

# Optional: pre-calibrate during a dry-run, then lock for production
runloom.set_stack_size(32 * 1024)        # whatever your dry-run found

# Spawn workers
for i in range(10000):
    runloom.go(worker)

runloom.run(1)

# Inspect after the burst
print("peak resident usage:", runloom.stats())
```

For exploratory work or benchmarks, just let calibration run and
inspect `stats()` afterwards.

## Roadmap

- Guard page (`mprotect PROT_NONE` on the lowest page) + `SIGSEGV` handler
  that raises a clean `StackOverflowError` instead of crashing.
- Per-thread calibration so M:N hubs converge independently.
- ~~Adaptive shrink~~ -- shipped: the default-on [grow-down](#automatic-grow-down-on-by-default-mn)
  shrinks each function to its measured need under M:N.
