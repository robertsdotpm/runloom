# CPython patches for runloom

Cross-hub fiber migration needs **both** halves below. They fix independent
halves of the same root cause — free-threaded CPython ties a running frame to
one OS thread in two separate ways — and either alone still corrupts:

| half | flag | what it decouples |
|---|---|---|
| **allocation** | `Py_TSTATE_ALLOC_HOME` | which heap a migrated fiber allocates on |
| **execution** | `Py_TSTATE_EXEC_HOME` | whether the compiler may cache *which OS thread we are* |

Each half ships as a **version-specific** patch, and they are **not**
interchangeable — take the pair matching your interpreter:

| target | allocation | execution |
|---|---|---|
| **CPython 3.14.4t** | `cpython314t-tstate-alloc-home.patch` | `cpython314t-tstate-exec-home.patch` |
| **CPython 3.13.13t** | `cpython313t-tstate-alloc-home.patch` | `cpython313t-tstate-exec-home.patch` |

All four apply at **zero fuzz** (`patch -p1 -F0`) to their pinned release, and
`tools/ci/check_patches.sh` enforces that in seconds. The cross-version deltas
are not cosmetic: 3.14 reordered `_PyThreadStateImpl` and rewrote the mimalloc
page-reclaim path, so the 3.14 alloc-home patch **drops** two hunks upstream has
since superseded and **adds** one relaxing an assert that alloc-home makes false.
See each patch's `WHAT CHANGED` header. Applying the other series' patch with
`-F3` can fuzz those superseded hunks back in and yield a silently wrong
interpreter — so don't.

`runloom.migration_available()` is True only with both; `runloom.migration_status()`
reports which half is missing. With either absent, migration stays behind the
`RUNLOOM_ALLOW_UNSAFE_MIGRATION` dev override and the scheduler falls back to the
default non-migrating mode with a warning naming the gap.

## `cpython313t-tstate-alloc-home.patch` — per-tstate allocation home

Optional CPython 3.13t feature (`-DPy_TSTATE_ALLOC_HOME`, **off by default**) that
lets a thread state borrow another tstate's allocator. It is the minimal,
upstream-shaped enabler for **transparent cross-hub migration of real stackful
fibers** — the thing that is otherwise blocked because one `_PyThreadState_GET()`
ties execution to allocation (see `docs/dev/HUB_MIGRATION_VERDICT.md`).

- **Default build:** zero change — the redirect macro-expands to the original
  `tstate->mimalloc`; no field, no ABI change, no perf cost.
- **Enabled build:** a migratable fiber runs under a thin execution-only tstate
  whose `_alloc_home` is repointed (one store) to the current hub on each resume.
  Allocation lands on the running hub's heap; old objects remote-free (supported).
  No per-fiber heap (no GC wall), no heap migration (no `_mi_page_retire` crash).

**runloom wiring (when enabled):** call `_PyThreadState_SetAllocHome(g->tstate,
hub->tstate)` at the per-g-tstate attach point (`mn_sched_hub_main.c.inc`); give
the per-g tstate no live heap. That turns the gated `RUNLOOM_PER_G_TSTATE` mode
from "heavy + crashing" into "lightweight + sound".

**Validation status: VALIDATED end-to-end.** Built CPython 3.14.4t with the flag
(593 stdlib tests pass, zero regression). Wired `runloom_iframe_borrow_alloc_home`
into the per-g-tstate attach (`mn_sched_hub_main.c.inc`). The previously-crashing
`RUNLOOM_PER_G_TSTATE` channel-churn repro now passes **24/24** with the borrow vs
**8/8 abort** without it (`RUNLOOM_NO_ALLOC_HOME=1`); default mode unaffected.
Refined scope: only the alloc-heap + mimalloc page_list redirect to home; the QSBR
reader stays the running tstate's (`_Py_qsbr_poll` asserts that). A direct migration
proof (`tests/experiments/resume_rebuild/migration_crosshub_proof.py`) shows **50/60 fibers
wake on a different hub than they parked on, zero crash** — real cross-hub migration
of transparent stackful fibers.

**Scope limit:** this patch moves the *heap*. It does nothing about the compiler
caching *which OS thread is running* — see the exec-home patch below, which is
required alongside it.

## `cpython314t-tstate-exec-home.patch` — non-cacheable execution-context reads

Optional CPython feature (`-DPy_TSTATE_EXEC_HOME`, **off by default**) that stops
the optimizer from caching the two reads identifying the OS thread a frame runs
on: `_PyThreadState_GET()` (via `pycore_pystate.h`, now routed through the
out-of-line `_PyThreadState_GetCurrent()`, which gains `Py_NO_INLINE` so LTO can't
undo it) and `_Py_ThreadId()` (via `object.h`, whose per-arch reads become
volatile asm).

**Why it's needed.** Both reads are pure expressions, so the compiler hoists them
out of loops, CSEs repeats into one, and sinks them to function entry. That is
sound under C's assumption that a live frame stays on one OS thread — an
assumption a stackful fiber scheduler breaks by swapping C stacks. The result is
a use-after-free with no incorrect C anywhere in the source:

- a stale `_PyThreadState_GET()` drives another thread's exception state /
  recursion limits / delayed-free queue, or one already freed by
  `PyThreadState_Delete()` if the origin hub has exited;
- a stale `_Py_ThreadId()` makes `_Py_IsOwnedByCurrentThread()` answer "yes" on a
  thread that doesn't own the object, routing decrefs down the **non-atomic**
  `ob_ref_local` path. Racing threads there lose or duplicate decrements; a
  duplicated one frees an object still referenced elsewhere.

**Not a Darwin problem.** The manifestation is platform-specific; the bug is not.
Compiling a read of the thread-local twice around an opaque call that can migrate
the fiber (clang `-O2`) — the second read is the one that must see the new thread:

| target | what the compiler does to the second read |
|---|---|
| Darwin/AArch64 | caches the **address**: one `tlv_get_addr` per function, spilled to the stack frame and reused |
| Linux/x86-64 | caches the **value**: `movq %fs:tss_tstate@TPOFF, %rbx` before the call, `movq %rbx, %rax` after — the second read is deleted |
| Linux/AArch64 | same: `mrs x8, TPIDR_EL0; ldr x19, [x8]` before, `mov x0, x19` after |

Identical under `-ftls-model=` `local-exec`, `initial-exec` and `global-dynamic`.
The Linux form is arguably worse — no address is involved at all, so there is
nothing to "resolve correctly"; the value simply predates the migration. It is a
legal optimization: the slot's address never escapes, so LLVM concludes no other
thread can write it — true only while a frame stays on one thread.

`_Py_ThreadId()` is worse still, because its per-arch reads are plain
(non-`volatile`) asm. On **all three** targets clang deletes *both* reads around
the call and folds `tid_before == tid_after` to a compile-time `mov w0, #1` —
"same thread", decided at compile time. Patched, each emits two real reads.

*(Linux numbers here are cross-compiled codegen only; the runtime soak below was
run on arm64 Darwin.)*

**The crash, in situ.** In `_PyEval_EvalFrameDefault` (3.14.4t, `-O2`, clang 21,
Darwin/arm64) the faulting sequence is:

```
ldr x9,  [sp, #0xf8]     ; cached &_Py_tss_tstate  <-- ORIGIN hub's slot
ldr x20, [x9]            ; tstate
ldr x9,  [x20, #0x350]   ; tstate->c_stack_soft_limit   <-- SIGSEGV, x20 == 0
```

The frame is on the *fiber's* stack, so it travels to whichever hub resumes the
fiber and keeps reading the origin hub's slot. Caught in the act: the crashing
thread's own `&_Py_tss_tstate` was `0x100cd84b8`, while the address cached in its
eval frame was `0x100cd6ff8` — a **different** hub thread's slot, and that thread
was parked idle in `cvwait`, so the slot held NULL. Had it instead been running
another fiber, the read would have returned a valid-but-wrong tstate and corrupted
state silently. The same fault reproduces as `_PyCriticalSection_BeginMutex`
(offset `0xb0`) and `_PyErr_Occurred` (offset `0x70`) depending on which inlined
`_PyThreadState_GET()` the optimizer happened to reuse.

**Default build:** zero change — `_Py_TID_ASM` expands to plain `__asm__` and
`_PyThreadState_GET()` keeps its inline thread-local read, both behind `#ifdef`.
No ABI change. Enabled, it costs exactly the hoist/CSE the inline reads exist to
enable.

**Validation status: VALIDATED end-to-end** (3.14.4t, arm64, release `-O2`, with
alloc-home applied). `benchmark/bench_migration.py` **before**: `--hubs 1` clean,
`--hubs 2/4/8` SIGSEGV every run. **After**: `--hubs 1/2/4/8` all clean, 10/10 soak
runs at 8 hubs × 64 fibers × 60 rounds × 32 allocs, with **77–80% of wakes landing
on a different hub than the fiber parked on**. `mpmc_pergt_repro.py` and
`migration_crosshub_proof.py` both pass; runloom's own suite is unchanged (223
passed / 1 skipped, and the two residual failures — `test_mn_sim_bytes` late-parker,
`test_monkey_leak` subprocess — reproduce identically on the *unpatched*
interpreter, so they are pre-existing). **Not** run against the CPython test suite.

alloc-home is genuinely required alongside exec-home: with `RUNLOOM_NO_ALLOC_HOME=1`
(exec-home on, alloc-home off) `mpmc_pergt_repro.py` still crashed **3/8** runs vs
**0/8** with both.

### Which exec-home half fixes what

exec-home has two halves, and they are *not* equally evidenced. Three interpreters,
identical apart from the patch, running `exec_home_min.py --hubs 4`, 10 runs each:

| build | halves present | result |
|---|---|---|
| `.venv-orig` | neither | **SIGSEGV 10/10** |
| `.venv-halfa` | `_PyThreadState_GET()` only | clean 10/10 |
| `.venv-mig` | both | clean 10/10 |

**The crash is entirely attributable to the `_PyThreadState_GET()` half.** The
`volatile` `_Py_ThreadId()` half contributes nothing to fixing it.

That half is kept on different grounds. Its mechanism is concrete and readable in
`refcount.h` — a stale thread id mis-answers `_Py_IsOwnedByCurrentThread()`, sending
a refcount update down the **non-atomic** `ob_ref_local` path from a thread that does
not own the object, which loses updates when the real owner does the same thing
concurrently. But **no failure has been pinned on it**. A canary hammering
main-thread-owned objects from migrating fibers came back clean on all three builds
(its first version reported drift on all three, including the fixed one — a
self-inflicted false positive from `for o in SHARED` leaving `o` bound). A lost
refcount update is silent, so a clean run cannot distinguish "unnecessary" from "not
yet observed". It is kept because it measured **free** and covers the failure class
testing cannot rule out. Settling it properly needs ThreadSanitizer on `ob_ref_local`
under migration; that has not been run.

**Cost.** Measured against the **alloc-home-only** build — i.e. this is the marginal
cost of adding exec-home to the original patch, not the cost of migration support
as a whole. Three interpreters built identically apart from the patch;
interpreter-bound microbenchmark, 2M iterations, min of 5, 3.14.4t arm64:

| | alloc-home only | + exec-home | Δ |
|---|---|---|---|
| dict alloc churn | 0.152 s | 0.176 s | +16% |
| function call | 0.042 s | 0.045 s | +7% |
| list alloc churn | 0.112 s | 0.124 s | +11% |

All of it is the `_PyThreadState_GET()` out-of-lining: the half-only build measures
0.175 / 0.044 / 0.125 s, indistinguishable from both halves. The `volatile`
`_Py_ThreadId()` is free. Anyone making migration cheaper should target the tstate
lookup, not the thread id.

> An earlier revision of this file quoted +38/+15/+52%. That was wrong: it compared
> against a separately-built interpreter rather than a controlled baseline, so build
> differences were being counted as patch cost. The table above compares three
> interpreters built from the same source with the same configure line.

**⚠ Do not build with `--with-lto` / `--enable-optimizations`.** exec-home works by
making `_PyThreadState_GetCurrent()` a genuine cross-TU call that cannot be CSE'd;
LTO can inline it back into its callers and silently reintroduce the bug.
`Py_NO_INLINE` covers only the same-TU case.

**⚠ Rebuild everything.** `_Py_ThreadId()` is inlined into `Py_INCREF`/`Py_DECREF`
through the *public* `refcount.h`, so the fix only reaches code compiled against
the patched headers. Every extension module in a migrating process must be rebuilt
— a prebuilt wheel keeps its cacheable reads. `runloom_c.exec_home_available` can
only speak for runloom's own extension.

**Known gaps:** on MSVC the thread-id reads are intrinsics (`__readgsqword`,
`__getReg`), not asm, and are left untouched — free-threaded MSVC isn't a migration
target today. Two other thread-locals are in the same class but not covered because
no fiber path reads them across a park: `pkgcontext` (`Python/import.c`) and
mimalloc's `_mi_heap_default` (object allocation reaches the heap through the
tstate — see alloc-home — not through it).

## Using it (production, behind flags)

Migration is **off by default**. To enable it you need two things: build CPython with
**both** patches, and set the flag.

1. **Build CPython with both patches.** `tools/ci/build_patched_cpython.sh` does
   the whole thing — fetch the pinned release, verify its sha256, apply the pair
   at zero fuzz, configure, build, install, and assert both halves report present:
   ```sh
   tools/ci/build_patched_cpython.sh 314      # or 313; pins live in tools/ci/versions.env
   ```
   By hand, for 3.14 (use the `313` files on 3.13 — they are not interchangeable):
   ```sh
   cd cpython
   patch -p1 -F0 < .../patches/cpython314t-tstate-alloc-home.patch
   patch -p1 -F0 < .../patches/cpython314t-tstate-exec-home.patch
   ./configure --disable-gil CPPFLAGS="-DPy_TSTATE_ALLOC_HOME -DPy_TSTATE_EXEC_HOME"
   # ALSO put both #defines in pyconfig.h -- CPPFLAGS covers only the CPython
   # build, while extension modules include the INSTALLED pyconfig.h.  alloc-home
   # adds a field to _PyThreadStateImpl, so a mismatch shifts struct offsets
   # silently rather than failing to link.
   printf '#define Py_TSTATE_ALLOC_HOME 1\n#define Py_TSTATE_EXEC_HOME 1\n' >> pyconfig.h
   make && make install
   ```
   Use `-F0` and treat any reject as a hard stop. Both flags are armed in
   `pyconfig.h` independently of whether the hunks landed, so a half-patched tree
   **advertises** the features while lacking them. Verify with the compile-time
   witnesses before building — this is what the CI's `rl_verify_witnesses` checks:
   ```sh
   grep -q _Py_TID_ASM Include/object.h                            # exec-home
   grep -q _PyThreadStateImpl_AllocHome Include/internal/pycore_tstate.h  # alloc-home
   ```

   Then build runloom against that interpreter (`python setup.py build_ext --inplace`),
   **and rebuild every other extension module you load** — `_Py_ThreadId()` inlines
   into `Py_INCREF`/`Py_DECREF` via the public `refcount.h`, so a wheel built against
   unpatched headers keeps the cacheable reads.

   The same runloom source builds against **stock** CPython too — both features
   compile out to no-ops, so nothing about the default build changes.

2. **Opt in at runtime** (before the runtime starts — the flag is read once at init):
   ```python
   import runloom
   if runloom.migration_available():        # True only with BOTH patches
       runloom.enable_migration()           # or set RUNLOOM_MIGRATION=1 in the env
   else:
       print(runloom.migration_status())    # which half is missing
   runloom.run(n_hubs, main)
   ```

**Flags / API:**

| flag / call | effect |
|---|---|
| `RUNLOOM_MIGRATION=1` | production master switch — enables cross-hub migration |
| `runloom.migration_available()` | `True` iff built against **both** patches (safe to enable) |
| `runloom.migration_status()` | `{"alloc_home":…, "exec_home":…, "available":…}` — which half is missing |
| `runloom.enable_migration()` | set the flag; **raises** (naming the missing patch) on an under-patched build |
| `runloom.migration_enabled()` | whether migration was requested for the next run |
| `runloom_c.alloc_home_available` | raw C-level capability bit for the allocation half (`0`/`1`) |
| `runloom_c.exec_home_available` | raw C-level capability bit for the execution half (`0`/`1`) |
| `RUNLOOM_NO_ALLOC_HOME=1` | disable the heap-borrow (A/B baseline; reproduces the crash) |
| `RUNLOOM_ALLOW_UNSAFE_MIGRATION=1` | **dev/fuzz only** — force migration on an under-patched CPython (can crash / UAF under churn) |

**Safety contract (validated):** on a build missing **either** patch,
`RUNLOOM_MIGRATION=1` prints a warning naming the missing half and **falls back to
the default non-migrating scheduler — no crash**, and `enable_migration()` raises
rather than risk a segfault. The unsafe override exists only for fuzzing the
under-patched path. `RUNLOOM_PER_G_TSTATE` and `RUNLOOM_STEAL_WOKEN` remain as
internal aliases of `RUNLOOM_MIGRATION`.
