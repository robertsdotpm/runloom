# 01 — Stackful coroutines: the context switch and the stack

Ground truth: `src/runloom_c/coro.{h,c}`, `fcontext.{h,c}`,
`arch/swap_*.S`, `plat.h`.

## The problem

A goroutine must be able to *suspend mid-function* (inside `recv`, inside a deep
call chain) and resume later exactly where it left off, with no syscall and no
`async`/`await` coloring. That means each goroutine needs **its own C stack** and
a way to **swap between stacks** cheaply.

## The simple model

A `runloom_coro` is: a heap/mmap'd C stack + a saved stack pointer + an entry
function. Two operations:

- `resume(c)` — save the *caller's* registers, switch to `c`'s stack, restore
  `c`'s registers, continue where `c` last yielded. Called by the scheduler.
- `yield()` — the inverse: save the coroutine's registers, switch back to the
  caller. Called from inside the coroutine.

A thread-local pointer `runloom_tls_current` names the coroutine running on this
OS thread, so `yield()` knows whose caller to return to. **That TLS pointer is
why N OS threads can each run their own scheduler independently** — there is no
shared "current coroutine" global.

```
  scheduler stack            coroutine stack
       │  resume(c) ───────────▶ │  (runs user code)
       │                         │  ... recv() would block ...
       │  ◀─────────── yield()   │  (suspended, SP saved in c)
       │  (pick next g)          │
       │  resume(c) ───────────▶ │  (continues just past yield)
```

## Three backends, one active per build (`plat.h` selects)

| Backend | Where | How |
|---|---|---|
| **fcontext** (asm) | Linux/macOS/BSD on x86-64 / aarch64 | hand-rolled `swap_*.S`: save callee-saved regs to the current stack, store SP into `*from`, load SP from `*to`, restore regs, `ret`. ~80 ns, no syscall. **The fast path.** |
| **Fibers** | Windows | `ConvertThreadToFiber` + `CreateFiberEx` + `SwitchToFiber`. Available since Win95. |
| **ucontext** | POSIX fallback | `getcontext`/`makecontext`/`swapcontext`. ~20× slower (it `sigprocmask`s every switch); correctness fallback only. |

The public API (`runloom_coro_new/resume/yield/destroy`) is identical across all
three. Selection is compile-time; `RUNLOOM_FORCE_UCONTEXT` / `RUNLOOM_BACKEND` can
force the fallback so it gets exercised on asm-capable hosts.

### Why a custom asm switch instead of greenlet/ucontext

- **vs ucontext**: `swapcontext` saves/restores the signal mask via a syscall on
  every switch. The asm `swap` touches only callee-saved registers + SP — no
  kernel. ~20×.
- **vs greenlet**: greenlet copies stack bytes on every switch (one shared
  stack). runloom keeps each goroutine on its **own** stack and only swaps the SP
  — faster *and* it enables guard pages and copy-grow, which a copying design
  can't have.

The asm trampoline is the subtle part: `make_ctx` pre-writes a fresh stack so the
first `swap` into it lands in `runloom_asm_trampoline` → `runloom_asm_entry(coro)`
→ `coro->entry(coro->user)`. When the entry returns, `runloom_asm_entry` sets
`done=1` and swaps back to the caller forever (so a mistaken resume of a finished
coro just yields back, never runs into garbage).

## The stack, and why its lifecycle is most of `coro.c`

A goroutine stack is the thing you pay for a million times, so its memory
discipline *is* the feasibility argument (spec 10 covers sizing policy; here is
the mechanism).

### Guard page — overflow faults cleanly, never corrupts

Every stack is `mmap`'d as `[ guard page PROT_NONE | usable RW ]`. The usable
base handed to the rest of the code is `region_base + guard`, so a stack that
grows down past its low end lands in the `PROT_NONE` page → immediate SIGSEGV,
instead of silently scribbling on a neighbor. The crash handler (spec 11) maps
that fault back to "goroutine N overflowed its K-KiB stack."

> Decision: **deliberately NOT `MAP_STACK`.** On FreeBSD/macOS `MAP_STACK` asks
> for a kernel grow-down stack whose lower pages are inaccessible until grown
> into — which faults when runloom eagerly touches the usable region. runloom
> installs its *own* guard page, so the kernel auto-grow is both unnecessary and
> harmful. `MAP_STACK` is a no-op on Linux, so dropping it is free there.

### Per-thread pools — spawn is allocation-free in steady state

Two pools, both thread-local (TLS), so a single-thread bench does O(1)
push/pop with zero allocator traffic:

- **Stack pool**: a freed stack's low 16 bytes hold the freelist `next`+`size`
  in-place (the stack grows down, so low bytes are unused while pooled) — no
  per-stack malloc node. On release, `MADV_DONTNEED` drops the page frames
  (keeping the VA mapping); the next reuse re-faults zero pages. Net: a pooled
  entry costs ~one resident page (4 KB) instead of its full `stack_size`. At a
  4096-entry cap that's ~16 MB instead of `4096 × stack_size`.
- **Coro-struct pool**: recycle the whole `runloom_coro` (stack still attached)
  so `new()` is a pop + re-`make_ctx` (writing ~6 registers), saving ~150–250 ns
  a spawn. A copy-grown (oversized) coro is *not* pooled — it would park a big
  stack at the head and defeat reuse for every later default spawn.

### Park-time reclaim and the high-water-mark scan (no painting)

- `runloom_coro_park(c)` / `madvise_idle(c)` drop the idle pages *below the saved
  SP* of a suspended coro (`MADV_DONTNEED`), so a long-parked goroutine holding a
  big stack costs only the pages it actually touched. Strict M:N safety contract:
  only the **owning hub** runs this, only while the coro is **suspended** (saved
  SP valid). Off by default (madvise+refault hurts short-park churn); the
  auto-sizer turns it on so "start large, learn down" stays RSS-free.
- HWM measurement is **paint-free**: it counts the run of resident pages
  (`mincore`) down from the top of the stack. A goroutine faults pages as it
  deepens; the deepest resident page is the high-water mark.

> War story baked into the design: an earlier version *painted* the stack with a
> sentinel **word** so the HWM scan could find the deepest overwritten slot. That
> sentinel was a fake pointer, and in a rare timing it landed on the interpreter
> value stack where a live object pointer belongs — `FOR_ITER` then *called* it
> as `tp_iternext` → jump to garbage → SIGSEGV. The fix was to stop writing any
> marker into stack memory and measure residency instead. **Lesson encoded:
> never put a non-NULL sentinel where the interpreter might dereference it.**

### Copy-on-grow (Path A) — small default stacks that still survive deepening

At every `resume`, if the suspended coro is using >3/4 of its stack, its live
region `[SP, top)` is copied onto a 2× stack (page-rounded, capped 8 MB),
interior stack-pointers are relocated by the fixed delta, and the saved SP is
patched. Safe because at a swap boundary the coro's entire live state is the
fcontext frame at SP plus the chain above it — no volatile registers to fix
(unlike a signal handler). This is what lets runloom **ship a small default
stack**: a goroutine that legitimately deepens *across yields* grows with it. It
**cannot** rescue a deep *non-yielding* burst between two yields — that overflows
into the guard page (clean SIGSEGV); such code sets `stack_size=` explicitly.

## Invariants a re-implementer must keep

1. **`resume`/`yield`/`new`/`destroy` for a coro all happen on one OS thread.**
   The saved SP and the asm frame are thread-bound. (This is why goroutines pin
   to their origin hub — spec 05.)
2. **A guard page sits below every stack** (where the backend allows). Overflow
   must fault, never corrupt a neighbor.
3. **Never recycle/destroy a coro a thread is still executing on.** A debug
   build asserts `dbg_running == 0` at release/reacquire; violating it is the
   use-after-recycle class behind the gc-churn crashes. Under ASan the pooled
   stack is poisoned so any access-while-free is caught.
4. **Park/grow/madvise touch only memory strictly below the saved SP**, and only
   while suspended. Touching at/above SP corrupts the live frame.
5. **Don't write sentinels into stack memory** that the interpreter could read as
   a live object (see the war story).

## What a minimal re-derivation looks like

You can build a working v0 with just: one mmap'd stack per coro, a `swap_x86_64.S`
with `make_ctx` + `swap`, a TLS current-pointer, and `resume`/`yield`. Everything
else here (pools, guard page, copy-grow, mincore HWM, park reclaim) is
*optimization and safety* layered on without changing that core — which is
exactly how to stage a re-implementation.
