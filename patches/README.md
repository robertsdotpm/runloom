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

**Validation status:** applies cleanly to CPython 3.13.13; default-off is
identical by construction; the *enabled* path still needs a build + the runloom
wiring to validate end to end.
