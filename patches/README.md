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
reader stays the running tstate's (`_Py_qsbr_poll` asserts that).
