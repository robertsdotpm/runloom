# CPython patches for runloom

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

**Validation status: VALIDATED end-to-end.** Built CPython 3.13.13t with the flag
(593 stdlib tests pass, zero regression). Wired `runloom_iframe_borrow_alloc_home`
into the per-g-tstate attach (`mn_sched_hub_main.c.inc`). The previously-crashing
`RUNLOOM_PER_G_TSTATE` channel-churn repro now passes **24/24** with the borrow vs
**8/8 abort** without it (`RUNLOOM_NO_ALLOC_HOME=1`); default mode unaffected.
Refined scope: only the alloc-heap + mimalloc page_list redirect to home; the QSBR
reader stays the running tstate's (`_Py_qsbr_poll` asserts that). A direct migration
proof (`experiments/resume_rebuild/migration_crosshub_proof.py`) shows **50/60 fibers
wake on a different hub than they parked on, zero crash** — real cross-hub migration
of transparent stackful fibers.

## Using it (production, behind flags)

Migration is **off by default**. To enable it you need two things: build CPython with
this patch, and set the flag.

1. **Build CPython with the patch** (one-time):
   ```sh
   cd cpython && patch -p1 < .../patches/cpython313t-tstate-alloc-home.patch
   # turn the feature ON for this interpreter:
   echo '#define Py_TSTATE_ALLOC_HOME 1' >> pyconfig.h    # or: ./configure CPPFLAGS=-DPy_TSTATE_ALLOC_HOME
   make && make install
   ```
   Then build runloom against that interpreter (`python setup.py build_ext --inplace`).
   The same runloom source builds against **stock** CPython too — the borrow compiles
   out to a no-op, so nothing about the default build changes.

2. **Opt in at runtime** (before the runtime starts — the flag is read once at init):
   ```python
   import runloom
   if runloom.migration_available():        # True only on a patched interpreter
       runloom.enable_migration()           # or set RUNLOOM_MIGRATION=1 in the env
   runloom.run(n_hubs, main)
   ```

**Flags / API:**

| flag / call | effect |
|---|---|
| `RUNLOOM_MIGRATION=1` | production master switch — enables cross-hub migration |
| `runloom.migration_available()` | `True` iff built against the patch (safe to enable) |
| `runloom.enable_migration()` | set the flag; **raises** on an unpatched build |
| `runloom.migration_enabled()` | whether migration was requested for the next run |
| `runloom_c.alloc_home_available` | the raw C-level capability bit (`0`/`1`) |
| `RUNLOOM_NO_ALLOC_HOME=1` | disable the heap-borrow (A/B baseline; reproduces the crash) |
| `RUNLOOM_ALLOW_UNSAFE_MIGRATION=1` | **dev/fuzz only** — force migration on *stock* CPython (can crash under churn) |

**Safety contract (validated):** on a build *without* the patch, `RUNLOOM_MIGRATION=1`
prints a one-line warning and **falls back to the default non-migrating scheduler — no
crash**, and `enable_migration()` raises rather than risk a segfault. The unsafe
override exists only for fuzzing the unpatched path. `RUNLOOM_PER_G_TSTATE` and
`RUNLOOM_STEAL_WOKEN` remain as internal aliases of `RUNLOOM_MIGRATION`.
