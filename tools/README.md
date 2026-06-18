# runloom dev tooling

Tools for exposing deadlocks, hangs, races, and crashes in the runloom runtime --
and the harnesses that drive them hard. This file is the **complete index of every
tool under `tools/`**. Most are wired into the CI lanes (`../scripts/check_all.sh`
and its `_fast` / `_extensive` variants); the machine-checked proofs live in
`../verify/`. Every tool's own file header explains it in full -- this index is the
map, and the few deepest-used ones are written up at the bottom.

## Index

### Hang / race / crash hunting
| tool | what | run |
|------|------|-----|
| [`watchdog.py`](watchdog.py) | turn a silent hang into a full state dump (thread stacks + scheduler self-check + stats + lifecycle event ring) | `python tools/watchdog.py` / import in tests |
| [`mn_stress.py`](mn_stress.py) | seeded fuzzer for the M:N scheduler: token-conservation over cross-hub channels + `select` under real parallelism | `python tools/mn_stress.py --iters 500 [--seed N]` |
| [`hang_hunter/`](hang_hunter/) | autonomous stress+fuzz daemon for the M:N scheduler with auto-triage + dedup of hangs/crashes | see [`hang_hunter/README.md`](hang_hunter/README.md) |
| [`lifefuzz/`](lifefuzz/) | generative, **replayable** life-cycle fuzzer: mass-produces diverse runloom programs under lifecycle oracles | see [`lifefuzz/README.md`](lifefuzz/README.md) |
| [`monkey_offload_stress.py`](monkey_offload_stress.py) | stress the monkey-offload cross-thread wake path (worker threads `unpark`-ing goroutines) | `python tools/monkey_offload_stress.py [ngor] [ops] [nhubs]` |
| [`wake_skew_test.sh`](wake_skew_test.sh) | wake-protocol **Layer 3**: run wake-sensitive tests under skew injection (`-DRUNLOOM_WAKE_SKEW`) to expose park/wake races | `tools/wake_skew_test.sh` |

### Sanitizers & instrumented interpreters (dynamic analysis)
| tool | what | run |
|------|------|-----|
| [`run_sanitizers.sh`](run_sanitizers.sh) | the standalone deque C harness (`test_cldeque`) under ASan / TSan / UBSan | `tools/run_sanitizers.sh [pushes thieves rounds]` |
| [`run_sanitizers_ext.sh`](run_sanitizers_ext.sh) | the **whole `runloom_c` ext** under TSan (preloaded libtsan) on free-threaded CPython -- real scheduler/chan/select/netpoll | `tools/run_sanitizers_ext.sh [intensity]` |
| [`run_pydebug.sh`](run_pydebug.sh) | runloom under a `--with-pydebug` CPython so the host's OWN internal asserts (tstate/STW/gilstate/mimalloc) are the oracle | `tools/run_pydebug.sh [iters]` -- see [`../docs/dev/cpython_boundary.md`](../docs/dev/cpython_boundary.md) |
| [`run_msan.sh`](run_msan.sh) | `runloom_c` under MemorySanitizer vs an MSan-instrumented CPython -- uninitialised reads | `tools/run_msan.sh` -- see [`../docs/dev/msan.md`](../docs/dev/msan.md) |
| [`build_msan_cpython.sh`](build_msan_cpython.sh) | build a free-threaded CPython under MSan (clang, `-fsanitize=memory`) -- prereq for `run_msan.sh` | `tools/build_msan_cpython.sh` |
| [`build_tsan_cpython.sh`](build_tsan_cpython.sh) | build a fully TSan-instrumented CPython (gold standard -- interpreter + ext both instrumented) | `tools/build_tsan_cpython.sh` |
| [`build_patched_rr.sh`](build_patched_rr.sh) | build + install `rr` with the vPMU min-period clamp so record/replay works on VMware vPMU | `tools/build_patched_rr.sh` -- see [`../docs/dev/rr_vpmu_status.md`](../docs/dev/rr_vpmu_status.md) |

### Static analysis
| tool | what | run |
|------|------|-----|
| [`static_analysis.sh`](static_analysis.sh) | seclint (banned funcs) + `gcc -fanalyzer` + clang analyzer + cppcheck | `tools/static_analysis.sh` |
| [`racerd.sh`](racerd.sh) | compositional static race detection (Infer **RacerD**) + memory-safety (**Pulse**) | `tools/racerd.sh` |

### Coverage
| tool | what | run |
|------|------|-----|
| [`coverage.sh`](coverage.sh) | C line/branch coverage (gcov) over the whole corpus (pytest + mn_stress + deque stress) | `tools/coverage.sh [phases]` |
| [`cov_measure.sh`](cov_measure.sh) | coverage via the **isolated** runner (`-j1`) -- avoids cross-file state leaks + the `.gcda` race | `tools/cov_measure.sh [args]` |
| [`cov_subsystem.py`](cov_subsystem.py) | aggregate gcov for a subsystem split across `.c` + `.c.inc` fragments (honors LCOV markers) | `cov_subsystem.py <covdir>` |
| [`cov_summary.py`](cov_summary.py) | per-file line coverage + heuristic uncovered-error-path report (ENOMEM/errno/`return -1`) | `cov_summary.py <covdir>` |
| [`kqueue_cov.sh`](kqueue_cov.sh) · [`kqueue_cov_run.sh`](kqueue_cov_run.sh) · [`kqueue_cov_parse.py`](kqueue_cov_parse.py) | scoped **branch** coverage of the macOS kqueue netpoll backend (one module/process) | `tools/kqueue_cov.sh` -- see [`../docs/dev/KQUEUE_AUDIT_2026-06.md`](../docs/dev/KQUEUE_AUDIT_2026-06.md) |

### Deterministic & controlled scheduling (a failure reduces to one seed)
| tool | what | run |
|------|------|-----|
| [`dst/`](dst/) | Deterministic Simulation Testing on the single hub: real chan/select, seeded yield oracle (`UniformYield` / `PCTBounded`) | see [`dst/README.md`](dst/README.md) |
| [`pct/`](pct/) | Probabilistic Concurrency Testing (single hub): random priorities + demotions, depth-bounded bug guarantee | see [`pct/README.md`](pct/README.md) |
| [`mn_controlled/`](mn_controlled/) | the M:N analogue: baton-gated hub resumption (`RUNLOOM_MN_SEED`) for reproducible multi-hub races | see [`mn_controlled/README.md`](mn_controlled/README.md) |

### Linearizability & model<->binary conformance
| tool | what | run |
|------|------|-----|
| [`lincheck/`](lincheck/) | channel histories checked LINEARIZABLE vs the FIFO spec (Porcupine) + a stateful Hypothesis model | see [`lincheck/README.md`](lincheck/README.md) |
| [`stw_trace_conform.py`](stw_trace_conform.py) + [`stw_trace_conform_demo.sh`](stw_trace_conform_demo.sh) | conform the REAL CPython stop-the-world (M2) handshake against `verify/tla/RunloomCPythonSTW.tla` under TLC (needs the instrumented pydebug; run via [`run_pydebug.sh`](run_pydebug.sh)) | `tools/stw_trace_conform_demo.sh` |
| [`tla_trace_conform.py`](tla_trace_conform.py) + [`trace_conform_demo.sh`](trace_conform_demo.sh) | conform the real gilstate-TSS lifecycle (M4) hub-tstate create/delete against `RunloomGilstate.tla` | `tools/trace_conform_demo.sh` (gated in `check_all` via `verify/tla/run_trace_conform.sh`) |
| [`mn_trace_conform.py`](mn_trace_conform.py) + [`mn_trace_conform_demo.sh`](mn_trace_conform_demo.sh) | conform the real controlled-M:N baton events against `RunloomMNControl.tla` | `tools/mn_trace_conform_demo.sh` (also gated in `check_all`) |

### Fault injection & robustness
| tool | what | run |
|------|------|-----|
| [`fault_sweep.py`](fault_sweep.py) | single-fault sweep: fail each Nth alloc/syscall in turn, classify OK / GRACEFUL / CRASH / HANG | `tools/fault_sweep.py [targets] [maxN]` |
| [`faultinj/`](faultinj/) | the `LD_PRELOAD` fault shim + cleanup-path workload that `fault_sweep.py` drives | see [`faultinj/README.md`](faultinj/README.md) |
| [`leak_check.py`](leak_check.py) | resource-balance harness: run a workload N times, assert live objects + open fds return to baseline | `python tools/leak_check.py` / import `check_leak()` |

### Mutation, combinatorial, security
| tool | what | run |
|------|------|-----|
| [`mutate/`](mutate/) | mutation testing: inject one compilable fault, rebuild, run a slice -- surviving mutants name untested lines | see [`mutate/README.md`](mutate/README.md) |
| [`combinatorial/`](combinatorial/) | t-way covering-array config-matrix testing -- find interaction faults cheaply (pairwise/3-way) | see [`combinatorial/README.md`](combinatorial/README.md) |
| [`security/`](security/) | the security verification suite (stack scrub, tainted-pointer bounds, frame overflow, C-API / TLS-bridge fuzzers, hardening lint) | see [`security/README.md`](security/README.md) |

### Benchmarking & misc
| tool | what | run |
|------|------|-----|
| [`bench/`](bench/) | rigorous microbench harness defending against autocorrelation + layout bias (Kalibera & Jones) -- driven by `../scripts/bench.sh` | see [`bench/README.md`](bench/README.md) |
| [`heavy_frames/`](heavy_frames/) | the stdlib fat-frame profile + generator for `runloom_heavy_frames.h` (goroutine stack cold-start sizing) | see [`heavy_frames/README.md`](heavy_frames/README.md) |

**Related, outside `tools/`:** `../scripts/check_all*.sh` (the CI lanes that drive
most of the above), `../scripts/check_wake_protocol.sh` (wake-protocol Layer 2
lint), `../verify/` (Spin / CBMC / GenMC / herd7 / TLA+ / Coq / Iris proofs, see
[`../verify/README.md`](../verify/README.md)), and `../tests_c/test_cldeque.c`
(deque stress). Several tools have a dedicated deep-dive doc under `../docs/dev/`
(linked inline above).

---

The few deepest-used tools are written up in full below; everything else is
self-documenting (open the file -- its header explains it and how to run it).

## watchdog.py -- hang / deadlock detector

A goroutine deadlock or a scheduler lost-wake looks like a process that
just stops: `run()` / `mn_run()` never returns, no exception. This makes
it loud and debuggable.

```python
from tools.watchdog import run_guarded, watchdog, hang_dump

# test-friendly: runs fn() in a worker thread, raises TimeoutError (after
# dumping full state) if it overruns -- works even when the scheduler is
# wedged, because the wedge is on the worker, not the caller.
run_guarded(lambda: runloom_c.run(), seconds=5.0, label="my workload")

# context-manager form (good when the block DOES return, just slowly;
# pass abort=True to os.abort() for a core dump on a true wedge):
with watchdog(5.0, label="ping-pong", abort=False):
    runloom_c.run()

# or dump state on demand from anywhere:
hang_dump(label="manual")
```

On a breach it dumps: every OS thread's stack (`faulthandler`), the
scheduler/netpoll `_self_check`, `stats()`, and the per-thread lifecycle
event ring (`_diag_dump`). For the ring to contain anything, start with
`RUNLOOM_DEBUG=ring,gstate` (read once at import).

Self-demo (catches a deliberate non-terminating scheduler):

```sh
RUNLOOM_DEBUG=ring,gstate python tools/watchdog.py
```

## mn_stress.py -- M:N scheduler fuzzer

The rest of `tests/` runs single-threaded; this hammers the multi-hub
path. Each iteration is a seeded "token conservation" experiment:
producers push a known multiset of tokens into a channel pool, a
coordinator closes them, consumers (some `recv`-range, some `select`)
drain them -- and **every token must be received exactly once**.
`_self_check()` must stay clean between iterations, all under the
watchdog so a hang prints its reproducing seed.

```sh
python tools/mn_stress.py --iters 500              # random seed
python tools/mn_stress.py --iters 1 --seed 12346   # deterministic repro
```

Exit 0 = clean; non-zero = conservation mismatch, self-check violation,
or hang (with the offending seed).

## run_sanitizers.sh -- C sanitizer harnesses

```sh
tools/run_sanitizers.sh                 # quick (seconds)
tools/run_sanitizers.sh 500000 8 10     # soak
```

Builds and runs `tests_c/test_cldeque` under ASan/TSan/UBSan. TSan runs
are auto-wrapped in `setarch -R` (high-entropy ASLR on 6.x kernels makes
TSan abort otherwise).

## run_sanitizers_ext.sh -- the whole runtime under TSan

`run_sanitizers.sh` only covers the standalone deque. This builds the
**entire `runloom_c` extension** with `-fsanitize=thread` and runs it
under a stock free-threaded CPython (force-loading `libtsan`), driven by
`mn_stress` + the lincheck recorder (plain **and** select consumers) + a
chan/sched pytest subset -- so TSan watches the real scheduler, channel,
select, and netpoll code under genuine GIL-off parallelism.

```sh
tools/run_sanitizers_ext.sh            # ~30s
tools/run_sanitizers_ext.sh 1000       # heavier mn_stress soak
```

TSan instruments only the ext (exactly runloom's C, incl. inlined `Py_INCREF`);
the few races inside the uninstrumented interpreter are filtered by
[`tsan_suppressions.txt`](tsan_suppressions.txt) (CPython-only -- never
suppress a `src/runloom_c/*` frame). The fully-instrumented interpreter
([`build_tsan_cpython.sh`](build_tsan_cpython.sh)) is the gold standard but
is currently blocked upstream; this preload path needs no patched CPython.

This harness found and fixed five real scheduler/chan/netpoll data races on
its first runs (see Findings C below).

---

## Findings (what the tooling has already turned up)

> These are **open** issues this tooling reproduces deterministically.
> They are surfaced here, not fixed.

### A. M:N: select() under contention crashed / lost values -- FIXED

Under real free-threaded parallelism, `select()` over channels with a
concurrent `close()` could SIGSEGV, hang, or silently drop values
(`tools/mn_stress.py` full mode mismatched ~1/16 iterations).  Root-caused
to **four** distinct bugs in `chan.c`'s select path, all now fixed:

1. **close-wake returned NULL** (`dcd1988`).  `close()` woke a goroutine
   parked in select Phase-2 with `value == NULL`; `m_select` put that NULL
   into the `(value, ok)` result tuple → SIGSEGV in the caller's
   `v, ok = ...` unpack.  Fixed: close-wake returns a fresh `Py_None`,
   matching every other closed-recv path.
2. **abort path returned a bare -1** (`ae2df38`).  Phase-2 install's
   "channel went ready" abort returned `select_try_each()` directly, which
   is -1 when the ready channel raced away.  For a *blocking* select a bare
   -1 became `PyLong(-1)` → the caller's unpack raised `TypeError`, killing
   the goroutine and dropping every value it had received.  Fixed: retry
   the scan-then-park instead of returning -1.
3. **abort dropped an already-delivered value** (`ae2df38`).  The abort's
   `CAS(fired_case, -1→i)` result was ignored; if a delivery had already
   fired the select on an earlier case (CAS won, value in that waiter), the
   abort evicted/freed the waiter holding it → value vanished.  Fixed: a
   lost CAS means "already fired" → stop installing and park, returning the
   delivered value.
4. **spurious wake returned -2-without-exception** (`ae2df38`).  A resume
   with `fired_case < 0` returned -2 → `m_select` returned NULL with no
   exception set → CPython `SystemError` → dead goroutine.  Fixed: retry
   (Go's spurious-wakeup behaviour), with a 10M-retry guard.

The earlier framing of this finding ("`select(default=True)` busy-poll")
was a mis-minimisation: that repro also had a *usage* bug (unpacking
select's `-1` default-sentinel as a tuple).  The real defects are the four
above and are independent of `default=`.  The deque, `wake_state`,
`park_safe`, and `select`-claim *algorithms* remain machine-proven in
`verify/`; these were integration bugs in the select → park/wake path,
surfaced by the fuzzer + watchdog under M:N.

Verified: `mn_stress` full (select consumers) CLEAN over 3000 iterations
across 6 seeds; single/multi-channel blocking-select+close clean 40/40;
guarded by `tests/test_mn.py::test_select_close_conservation`.

### B. `getaddrinfo` codec import overflowed the goroutine stack -- FIXED

`tests/test_sync.py` used to SIGSEGV on the first network call: the first
`socket.getaddrinfo` triggers a deep C-level codec import
(`encodings.idna` → `stringprep` → `unicodedata`) that overflowed the
32 KB default coroutine stack -- caught cleanly by the PROT_NONE guard
page (a clean fault, not silent corruption).

`runloom.runtime` already had `prewarm_stdlib()` (resolves that import on the
main thread's big stack before any goroutine runs) and `runloom.runtime.run`
/ the aio loop called it -- but `runloom.sync.run`/`runloom.sync.go` did not.
Fixed by calling `prewarm_stdlib()` from the `runloom.sync` entry points
too, guarded so it only warms on the main thread (never on a goroutine's
small stack). `tests/test_sync.py` now passes 7/7.

### C. Whole-runtime TSan: five scheduler/chan/netpoll data races -- FIXED

`run_sanitizers_ext.sh` (the ext under ThreadSanitizer, driven by mn_stress +
lincheck + a pytest subset) flagged five data races in runloom's own C on its
first runs.  All were real C11 races -- benign on x86 (aligned word
read/write) but UB, and several stale-prone on weak memory models.  Each was
the lone plain access among siblings that already used the correct atomic, and
each is now fixed to match:

1. **`chan.c` select** -- `park.fired_case` read plain after wake while
   `waiter_claim` claims it with an ACQ_REL CAS (the dominant report, 77/80
   under mn_stress).  Acquire-load both reads; pairs with the claimer's release
   so the captured value is visible.
2. **`mn_sched.c` preempt hook** -- `runloom_preempt_prev_eval` (function pointer
   on the per-frame eval hot path) read plain vs plain install/uninstall writes.
   Release-store on install/uninstall, acquire-load on the eval path.
3. **`mn_sched.c` sysmon** -- `h->resume_g` read plain by the watchdog vs the
   hub's per-resume write.  Relaxed-atomic both sides (its two siblings already
   were).
4. **`mn_sched.c` starve_bound** -- lazy-static env flag read plain vs an atomic
   first-touch store.  Loaded once into a loop-invariant local via
   `__atomic_load_n`, matching all seven sibling flags.
5. **`netpoll.c` pump** -- `runloom_pump_wake_fd` lazy-init eventfd checked/set
   plain (also double-created on concurrent first-arm).  Double-checked locking
   under `runloom_pool.lock` + release-store; readers acquire-load.

The select/deque/park-wake *algorithms* remain machine-proven in `verify/`;
these were missing-atomic-qualifier bugs in the shipped C that only a sanitizer
on the real binary (not a model of an extracted fragment) can see.

### D. Deadlock-quiescence census reads hub fields cross-thread -- 3 FIXED, 1 deferred

`tools/lifefuzz` (the generative life-cycle fuzzer) run under the gold-standard
TSan ext surfaced a race cluster `mn_stress`'s fixed workload never reached: the
deadlock-quiescence census `runloom_mn_has_wakeable_work` (main thread) reads
several **owner-only per-hub fields lock-free**, but only some were atomic-
qualified.  The lone-plain-among-atomic-siblings pattern of Finding C, recurring:

1. **`ready_head` / `ready_tail`** (`runloom_sched_ready_empty`) read plain vs the
   owning hub's `ready_push`/`ready_pop`/`grow` writes (`datastack:267/403/255`).
   **FIXED** -- relaxed-atomic both sides (single-writer per ring).
2. **`sleep_size`** read plain (census) vs `sleep_push`/`sleep_pop` writes
   (`datastack:454/482`).  **FIXED** -- relaxed-atomic.
3. **`timer_size`** read plain (census) vs `timer_push`/`timer_pop` writes
   (`datastack:541/566`).  **FIXED** -- relaxed-atomic.
4. **`sub_head`** -- the census ACQUIRE-reads it lock-free; the 6
   producer/handoff/drain WRITES (`mn_api:108`, `hub_main:273`, `hub_resume:618`,
   `init:92/197/694`) were PLAIN under `sub_lock` (which the lock-free census does
   not take).  **FIXED** -- a publish (store g) is now `__atomic_store_n(...,
   RELEASE)` so the census sees a non-NULL head with g's fields visible; a clear
   (store NULL) is `RELAXED`.  A multi-writer pointer, so RELEASE (not relaxed) on
   the publishes.

All are benign on x86/TSO (aligned word access) but UB in C11 and stale-prone on
weak memory; the census is best-effort (its deadlock streak + re-kick absorb a
stale read), so severity is low.  Verified: after all four fixes, **no** TSan race
remains across `lifefuzz` seeds 1-12; the default suite stays green.  Repro:
`tools/lifefuzz/lifefuzz.py repro 3 --mn-seed 900002` under the TSan ext (see
`tools/lifefuzz/README.md`).
