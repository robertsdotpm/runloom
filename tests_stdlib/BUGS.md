# runloom × CPython stdlib test-suite — bug triage

Findings from running the **vendored CPython 3.13t stdlib test corpus** through
runloom's M:N scheduler, one module per goroutine (no monkey-patching). See
[README.md](README.md) for the harness; raw per-module logs land under
`results/<STATUS>/<module>.log` (gitignored).

**Status:** triage only — bugs are catalogued here and revisited/root-caused at
the end. Do **not** treat counts as final until the sweep + `triage.py` clustering
has run; the per-cluster section below is regenerated from `results/`.

Environment: free-threaded CPython 3.13.13t, `PYTHON_GIL=0`, x86-64 Linux,
runloom @ branch `stdlib-test-port` (origin/main tip `8a82f6a`).

---

## Headline

- **689 modules** run, one per goroutine on the M:N scheduler (128 KB stacks):
  **346 PASS · 297 CRASH (all SIGSEGV) · 34 LOADERR · 11 FAIL · 1 HANG**.
- **All 297 crashes are ONE bug** — the 128 KB goroutine stack overflowing on
  deep CPython C-stack bursts ([BUG-001](#bug-001--the-small-default-goroutine-stack-128-kb-overflows-on-real-cpython-c-stack-depth--sigsegv)).
  Re-running those 297 under an 8 MB stack: **246 PASS · 36 FAIL · 9 ERROR ·
  5 LOADERR · 0 CRASH**.

  | metric | baseline (128 KB) | crashed-set re-run @ 8 MB |
  |--------|------------------:|--------------------------:|
  | SIGSEGV | 297 | **0** |
  | newly PASS | — | 246 |
  | residual FAIL/ERROR (2nd-layer bugs) | — | 45 |

- So a single stack-policy fix would take the suite from **346 → ~592 PASS**, and
  surface ~45 genuinely-distinct issues currently masked by the crash.
- LOADERR/FAIL are largely **environment** (SkipTest: missing `_dbm`, Windows/BSD
  only, GIL-disabled; plus `-j` collisions), not runloom bugs.

---

## Re-triage at an 8 MB per-spawn stack — the REAL second layer

The original per-cluster table below clusters the **32 KB-crash** logs, so the
single stack bug (BUG-001) got smeared into ~10 fake "distinct" clusters
(`test_pulldom` etc. are **PASS at 8 MB** — the scary lines were verbose test
*docstrings* + harmless `[RUNLOOM_SYSMON] … 0 stranded` noise). A full 689-module
re-sweep at an 8 MB per-spawn stack (`sweep_mn.py --stack 8388608`, which uses
`mn_go(stack_size)`) is the honest baseline:

| | 32 KB baseline | **8 MB re-sweep** |
|---|---:|---:|
| PASS | 346 | **576** |
| CRASH | 297 | **7** |
| HANG | 1 | 5 |
| FAIL | 11 | 54 |
| LOADERR | 34 | 47 |

**290 of 297 crashes were BUG-001.** The genuine non-stack surface is tiny — and
classified by whether it fits a decision already made (✅ fix-ready), needs a
policy/design call (⏸ discuss), or is environment (▫ not a bug):

| ⏸/▫ | cluster | modules | read |
|---|---|---|---|
| ⏸ | **BUG-001 — goroutine stack policy** | (290 modules) | THE one decision. Default M:N g-stack (32 KB) vs CPython's 8 MB C-stack assumption. Resolvable per-spawn (`mn_go(stack_size)` / aio `_IO_STACK`) with zero runtime change; the *default UX* (raise default? auto-warmup? clean RecursionError at the guard? tune grow-on-fault?) is the deferred call. |
| ⏸ | **signal / KeyboardInterrupt** (1 root cause) | `test_signal`, `test_unittest.test_break`, `test_generators`, `test_multiprocessing_fork`, `test_multiprocessing_forkserver` — all crash ending in `KeyboardInterrupt` | tests self-`SIGINT`; SIGINT→KeyboardInterrupt delivery through a **bare (unpatched) M:N goroutine**. Verify against the documented signal invariant; partly *expected* for self-interrupting tests. NOT five bugs. |
| ▫ | **`test_threading` → SIGABRT** → **[BUG-003]** | `run_in_subinterp` | ROOT-CAUSED: `Py_EndInterpreter: not the last thread` — **sub-interpreters**, not a bug. Reclassified to unsupported-config; recommend a fail-loud guard. |
| ✅ | **`test_weakref` → SIGSEGV** → **[BUG-002]** | `test_threaded_weak_key_dict_copy` | **FIXED (80b5956).** runloom **preemption** froze a goroutine mid `tp_dealloc` (weakref callback / finalizer, via the BRC cross-thread merge or trashcan) at a GC-safe point → a concurrent `gc.collect()` STW reclaim corrupted the half-destroyed objects (UAF). Fix: defer the preempt/liveness yield while `runloom_tstate_in_destruction(ts)`. Repro 85/85 clean, ext-TSan clean. |
| ⏸ | **asyncio.run / pending-calls HANG** | `test_asyncio.test_runners` (`run_without_uncancel`), `test_capi.test_misc` (`Py_AddPendingCall`) | real-ish lost-wake / scheduler-interaction candidates. |
| ▫ | **multiprocessing under M:N** | `mp_spawn` (hang), the two mp crashes (signal cluster) | the documented "fork inherits hub threads → unsupported; use spawn/forkserver" family; spawn/forkserver also struggling is worth a note but is a known-fraught area, not new. |
| ▫ | **env / slow, not bugs** | `test_datetime` (slow ZoneInfo, hit 120 s), `test_zipfile64` (largefile resource), **LOADERR 47** (mostly `SkipTest`: no `_dbm`/`_gdbm`, Windows/BSD-only, GIL-disabled), **most FAIL 54** (`-j` collisions + semantic) | re-run single-process before trusting any FAIL. |

**Bottom line:** "297 crashes + 45 maybes" → after root-causing the golds:
**one policy decision (BUG-001 stack) + ONE real runloom bug (BUG-002 —
preemption × free-threaded destruction, `test_weakref`) ✅ now FIXED (80b5956) +
one unsupported-config (BUG-003 — sub-interpreters, `test_threading`) + one
signal cluster + two real hangs**, the rest stack/env. The single genuine
runloom bug (BUG-002) is fixed and ext-TSan-validated; everything else is a
policy call, a fail-loud-guard candidate, or environment.

8 MB re-sweep artifacts: `results_retriage8m/` (results.csv + per-module logs).

---

## How to reproduce any single bug

```bash
# one module, in a goroutine on the M:N scheduler, verbose (names the test that dies):
PYTHON_GIL=0 PYTHONPATH=tests_stdlib:src \
  python tests_stdlib/run_one_mn.py test.<module> 4
```

The child runs `unittest` at verbosity=2, so the **last line on stderr before a
SIGSEGV names the test/doctest that was executing**.

---

## Confirmed bugs (root-caused enough to file)

### BUG-001 — the small default goroutine stack (128 KB) overflows on real CPython C-stack depth → SIGSEGV

**This one bug accounts for essentially all of the 297 crashes** (import-time +
doctest + most mid-run). Stock CPython runs on the ~8 MB main-thread stack;
runloom's M:N goroutines run on a 128 KB swapped stack (`go()` defaults the
same), and a lot of perfectly normal stdlib code recurses deeper than that in C.

- **Minimal repro:**
  ```python
  import runloom_c as rc
  rc.mn_init(1); rc.mn_go(lambda: __import__("test.test_argparse")); rc.mn_run(); rc.mn_fini()
  # -> Segmentation fault (exit 139)
  ```
- **Confirmed it is the stack** (the `go()` 1:1 path accepts a stack arg; `mn_go`
  does not):
  ```
  go(import test.test_argparse)            # 128 KB default -> SIGSEGV
  go(import test.test_argparse, 8<<20)     # 8 MB stack     -> imports fine
  ```
  Same for `import readline` (see backtrace below): SEGV at 128 KB, clean at 8 MB.
- **Not runloom-agnostic / not a parallelism bug:** plain 3.13t imports these
  fine on the main thread *and* a secondary `threading.Thread`. The crash needs a
  goroutine, but is **independent of hub count (1≡4) and GIL (on≡off)** — purely
  the swapped stack size.
- **Grow-on-fault does not rescue it:** auto-grow (`RUNLOOM_STACK_GROW`, doubles
  the stack on a guard-page fault up to 8 MB) is on by default and the imports
  still SEGV — consistent with the code's own note (`coro.c` / `runtime.py`) that
  a *deep, non-yielding C-stack burst* (a cold import chain, terminfo/readline
  init, OpenSSL, a doctest example) blows past the guard faster than the fault
  handler can grow it. The documented mitigation is `runloom.warmup(...)` /
  pre-warming or a larger default stack.
- **readline sub-case — gdb backtrace (the `~1s` "crash at the first `Doctest:`
  line" cluster, e.g. `test_heapq`, `test_collections`):** several modules run
  `doctest` examples; that path imports `readline`, whose terminal init uses
  large stack buffers and tips the goroutine over:
  ```
  #0 libtinfo.so.6   _nc_read_entry2          <-- SIGSEGV
  #3 libtinfo.so.6   _nc_setupterm
  #5 libreadline.so  _rl_init_terminal_io
  #6 libreadline.so  rl_initialize
  #7 setup_readline  (Modules/readline.c:1358)
  #8 PyInit_readline (Modules/readline.c:1588)   <-- import readline
  ```
- **Blast-radius control:** re-running all 297 crashed modules under `go()` at an
  8 MB stack (`RUNLOOM_RUN_MODE=go --stack 8388608`) turns them from CRASH into
  PASS/FAIL/ERROR with **~zero remaining SIGSEGV** — see `results_gobig/` and the
  table below. The residual FAIL/ERROR are the *second layer* of genuine issues
  that were hidden behind the crash, to be triaged next.
- **Design question for processing (not yet decided):** raise the M:N default
  goroutine stack? auto-`warmup` the known-deep paths? turn the overflow into a
  `RecursionError` at the guard page instead of a raw SEGV? This is a runloom
  policy call, deferred to the fix phase.

### BUG-002 — preemption froze a goroutine mid object-destruction → free-threaded UAF (`test_weakref`)  ✅ FIXED (80b5956)

The one genuine **non-stack** runloom bug from the sweep.
`test_weakref.MappingTestCase.test_threaded_weak_key_dict_copy` SIGSEGV'd under
M:N **at an 8 MB stack** (~85% of runs). **Fixed** — root-caused first-hand with
gdb and corrected from the earlier speculation below.

> NB the crash only surfaces at a roomy stack. At the default goroutine stack
> the same module crashes earlier on the deep cold-import C burst (BUG-001),
> which masked this one; the original "RUNLOOM_PREEMPT=0 → 10/10" reading was at
> 8 MB. At 8 MB: preempt on → ~85% crash; `RUNLOOM_PREEMPT=0` → clean.
> Independent of handoff (`RUNLOOM_HANDOFF=0` still crashes) and hub count
> (`hubs=1` still crashes — see the real mechanism below).

- **Workload.** Builds a 70 000-entry `WeakKeyDictionary` **in the goroutine**
  (so the hub tstate *owns* those objects under biased refcounting), then spawns
  two **native** threads — one `d.copy()`, one popping the last key references so
  keys are GC'd (clearing weakrefs, mutating `d`) and periodically calling
  `gc.collect()`.
- **Crash (gdb, reproduced identically across runs).** A hub thread draining its
  cross-thread refcount-merge queue, deeply recursive because each dealloc runs a
  Python weakref callback that re-enters the eval loop and the merge:
  ```
  clear_weakref_lock_held            <- SIGSEGV (callback ptr = a C-stack garbage addr)
  PyObject_ClearWeakRefs
  subtype_dealloc / _PyTrash_thread_destroy_chain
  merge_queued_objects     Python/brc.c
  _Py_brc_merge_refcounts
  _Py_HandlePending        ceval_gil.c:1297
  _PyEval_EvalFrameDefault   (... repeats: callback eval -> HandlePending -> merge ...)
  ```
- **Root cause (real mechanism).** runloom's **preemption** (the eval-frame
  wrapper / single-frame liveness backstop) yields the goroutine at a Python
  frame entry that can be **nested inside an in-flight `tp_dealloc`** (a weakref
  callback or finalizer, driven by the BRC cross-thread merge or the trashcan
  unwind). Suspending there returns the hub thread to hub_main — a **GC-safe
  point** — with a *half-finished destructor frozen on the goroutine's coro
  stack*. A concurrent native thread's `gc.collect()` stop-the-world (or QSBR
  reclaim) then runs against the partially-destroyed objects → use-after-free.
  This is why `hubs=1` still crashes (the native `gc.collect()` is the second
  actor, not a second goroutine) and why cooperative yields are safe (they only
  happen at Python-level call points, never nested in a C destructor).
- **Dead ends (confirmed by reading 3.13t source).** `ts->critical_section==0`
  is useless — `PyObject_ClearWeakRefs` runs callbacks *after* `UNLOCK_WEAKREFS`,
  lock-free. The trashcan has **no nesting counter** in 3.13 (just
  `delete_later` + `c_recursion_remaining`). And **rerouting preemption through
  the eval-breaker / pending-call boundary does NOT help** (the earlier proposed
  fix): the merge's dealloc→callback→eval *re-enters* `_Py_HandlePending`
  nested, so a pending-call preempt still fires inside the destructor.
- **The fix (`80b5956`).** A `Py_BUILD_CORE`-isolated helper
  `runloom_tstate_in_destruction(ts)` reports whether CPython is mid `tp_dealloc`
  on this tstate — `tstate->delete_later` (trashcan unwind) or
  `brc.local_objects_to_merge` (BRC merge drain). The preempt wrapper and the
  liveness backstop **defer** the yield while it is true, leaving the trigger
  armed so the *next* frame entry after the destructor unwinds takes the yield —
  preemption is delayed a few frames, never lost. Reads only the owning thread's
  own tstate fields (`local_objects_to_merge` is, per `pycore_brc.h`, "not
  accessed by other threads"), so it is race-free by construction; zero hot-path
  cost (consulted only when a preempt is already pending).
- **Validation.** Repro 85/85 clean across hubs=4 / hubs=1 / the deterministic
  frame-count preemptor (was ~85% crash); full `test_weakref` 137/137; native
  suite 81 files green; `mn_stress` confirms preemption still breaks wedged hubs;
  **ext-TSan clean** (`tools/run_sanitizers_ext.sh`, 0 runloom race reports).

### BUG-003 — sub-interpreters are incompatible with M:N → fatal abort (`test_threading`)  [reclassify: unsupported config]

`test_threading` aborts with `Fatal Python error: Py_EndInterpreter: not the
last thread`, from `_testcapi.run_in_subinterp` (test
`test_interrupt_main_with_signal_handler`). **Not a runloom bug** — it's
sub-interpreters: `Py_EndInterpreter` requires the caller be the *only* thread
in the interpreter, which can never hold while runloom owns N hub threads. This
is the **unsupported-config** class, not gold. Belongs with the asyncio/mp
clusters below, not the crash table.

**Recommendation — fail loud, not crash (narrow guards only):** add guards that
raise a clear `RuntimeError` at the *fundamental, cleanly-detectable* entry
points, rather than letting them SIGSEGV/abort:
- **sub-interpreters** (`Py_NewInterpreter` / `run_in_subinterp`): fundamentally
  impossible under M:N; intercept and raise.
- **multiprocessing `fork` start-method**: already deadlocks; detect under M:N
  and raise "use spawn/forkserver".

Do **not** guard fuzzy/sometimes-works cases (native `asyncio` in a goroutine,
bare signals) — a false `NotImplementedError` is worse than a documented rare
failure; document those and point at `runloom.aio` / `monkey.patch` instead.

### Distinct clusters that are NOT (only) the stack bug

- **`mid-unittest-run crash` (subset of 58):** some survive the 8 MB control as
  ERROR/FAIL rather than PASS — those have a second cause; triage individually.
- **`test_signal`, `test_unittest.test_break` → KeyboardInterrupt:** these tests
  deliberately raise `KeyboardInterrupt` via `SIGINT`/`alarm`; interaction with
  runloom's signal-into-parked-goroutine delivery — verify against the documented
  signal invariant, may be expected.
- **`test_file_eintr`, `test_pty`:** EINTR / pty master-fd handling under the
  cooperative I/O path — likely real, separate.
- **`HANG` `test_zipfile64`:** 64-bit large-file test; probably just slow/env
  (needs the `largefile` resource), confirm it is not a lost-wake.

> LOADERR (34) and FAIL (11) in the baseline are mostly **environment**, not
> runloom bugs: LOADERR is overwhelmingly `unittest.SkipTest` (no `_dbm`/`_gdbm`,
> Windows/BSD/Solaris-only, `GIL disabled`, missing source-build dirs); the FAILs
> are semantic and some are `-j` parallel collisions. Revisit case-by-case.

---

## Open clusters (auto-generated)

> Run `python tests_stdlib/triage.py` after a sweep to (re)generate the section
> below from `results/`. It clusters CRASH/HANG/ERROR by a signature derived from
> each child's final stderr + how long it survived (import-time vs. mid-run).

<!-- TRIAGE:BEGIN -->
**689 modules** | CRASH=297 FAIL=11 HANG=1 LOADERR=34 PASS=346

| status | signature | count | example modules |
|--------|-----------|-------|-----------------|
| CRASH | import/load-time crash (no unittest output) | 212 | `test.test__colorize`, `test.test_android`, `test.test_apple`, `test.test_argparse`, `test.test_ast.test_ast`, `test.test_asyncgen`, … (+206) |
| CRASH | mid-unittest-run crash | 58 | `test.test___all__`, `test.test__interpchannels`, `test.test__interpreters`, `test.test_atexit`, `test.test_audit`, `test.test_bdb`, … (+52) |
| CRASH | doctest execution crash (BUG-001 readline/terminfo family) | 22 | `test.test_code`, `test.test_collections`, `test.test_ctypes.test_objects`, `test.test_deque`, `test.test_descrtut`, `test.test_difflib`, … (+16) |
| LOADERR | other: unittest.case.SkipTest: test_gdb only works on source builds at the moment. | 5 | `test.test_gdb.test_backtrace`, `test.test_gdb.test_cfunction`, `test.test_gdb.test_cfunction_full`, `test.test_gdb.test_misc`, `test.test_gdb.test_pretty_print` |
| LOADERR | other: unittest.case.SkipTest: GIL disabled | 5 | `test.test_interpreters.test_api`, `test.test_interpreters.test_channels`, `test.test_interpreters.test_lifecycle`, `test.test_interpreters.test_queues`, `test.test_interpreters.test_stress` |
| LOADERR | other: unittest.case.SkipTest: peg_generator directory could not be found | 4 | `test.test_peg_generator.test_c_parser`, `test.test_peg_generator.test_first_sets`, `test.test_peg_generator.test_grammar_validator`, `test.test_peg_generator.test_pegen` |
| LOADERR | other: unittest.case.SkipTest: scripts directory could not be found | 3 | `test.test_tools.test_i18n`, `test.test_tools.test_reindent`, `test.test_tools.test_sundry` |
| CRASH | other: KeyboardInterrupt | 2 | `test.test_signal`, `test.test_unittest.test_break` |
| LOADERR | other: unittest.case.SkipTest: test irrelevant for an installed Python | 1 | `test.test_asdl_parser` |
| LOADERR | other: unittest.case.SkipTest: Windows only | 1 | `test.test_asyncio.test_windows_utils` |
| LOADERR | other: unittest.case.SkipTest: clinic directory could not be found | 1 | `test.test_clinic` |
| LOADERR | other: unittest.case.SkipTest: Windows-specific test | 1 | `test.test_ctypes.test_win32_com_foreign_func` |
| LOADERR | other: unittest.case.SkipTest: No module named '_gdbm' | 1 | `test.test_dbm_gnu` |
| LOADERR | other: unittest.case.SkipTest: No module named '_dbm' | 1 | `test.test_dbm_ndbm` |
| LOADERR | other: unittest.case.SkipTest: test works only on Solaris OS family | 1 | `test.test_devpoll` |
| CRASH | other: BufferedReader.read() must handle signals and not lose data. ... [RUNLOOM_SYSMON] hub 0 WE | 1 | `test.test_file_eintr` |
| LOADERR | other: unittest.case.SkipTest: cases_generator directory could not be found | 1 | `test.test_generated_cases` |
| LOADERR | other: unittest.case.SkipTest: No module named 'winreg' | 1 | `test.test_importlib.test_windows` |
| LOADERR | other: unittest.case.SkipTest: test works only on BSD | 1 | `test.test_kqueue` |
| LOADERR | other: unittest.case.SkipTest: test only applies to Windows | 1 | `test.test_launcher` |
| LOADERR | other: unittest.case.SkipTest: windows related tests | 1 | `test.test_msvcrt` |
| CRASH | other: Test the normal data case on both master_fd and stdin. ... | 1 | `test.test_pty` |
| CRASH | other: PullDOM does not receive "comment" events. ... | 1 | `test.test_pulldom` |
| LOADERR | other: unittest.case.SkipTest: test only relevant on win32 | 1 | `test.test_pyrepl.test_windows_console` |
| LOADERR | other: unittest.case.SkipTest: freeze directory could not be found | 1 | `test.test_tools.test_freeze` |
| LOADERR | other: unittest.case.SkipTest: i18n directory could not be found | 1 | `test.test_tools.test_msgfmt` |
| LOADERR | other: unittest.case.SkipTest: No module named '_wmi' | 1 | `test.test_wmi` |
| LOADERR | other: unittest.case.SkipTest: Unable to import big_o | 1 | `test.test_zipfile._path.test_complexity` |
| HANG | mid-unittest-run crash | 1 | `test.test_zipfile64` |

**FAIL (11)** — unittest failures/errors (semantic; some may be -j env collisions, revisit individually):

`test.test_cmd_line`, `test.test_importlib.frozen.test_finder`, `test.test_importlib.import_.test_path`, `test.test_importlib.source.test_file_loader`, `test.test_largefile`, `test.test_posix`, `test.test_threadsignals`, `test.test_time`, `test.test_tools.test_makefile`, `test.test_unittest.test_loader`, `test.test_unittest.test_program`

<!-- TRIAGE:END -->
