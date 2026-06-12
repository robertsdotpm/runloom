# 15 — Verification, testing, and the whole QA apparatus

Ground truth: `scripts/check_all.sh` (the one driver), `verify/` + `verify/README.md`
+ `verify/run_verify.sh` (formal proofs), `tools/` + `tools/README.md` (the
harnesses), `bench/` + `tools/bench/` (profiling), `QUALITY_CAMPAIGN.md` (the
research-grade frontier), `docs/dev/VALIDATION.md`, `CLAUDE.md` (No hosted CI +
the local CI runner), and the conformance tests under `tests/`.

This spec is part of the *design*. Several invariants in the preceding specs were
**derived from or proven by** these tools (the park/wake seq_cst fence by GenMC,
the Chase-Lev orderings by `test_cldeque` on real ARM, the netpoll claim
linearization by Spin+GenMC, contract C6 by the pydebug oracle, the four select
bugs by the fuzzer + a Spin model). A re-derivation that omits the apparatus will
re-introduce the exact bugs the specs warn about, because they live in rare
interleavings and weak-memory reorderings ordinary tests miss ~50000 runs of
50001.

## The governing rules (read these first)

- **No hosted CI, ever.** GitHub Actions is deliberately absent (macOS minutes
  especially are not free; asked for repeatedly). "CI" is **local**:
  `scripts/check_all.sh`. A separate **cron-driven shell runner** on the dev box
  does post-merge matrix validation (Linux 3.13t + Windows 3.12/3.13t + macOS
  arm64 3.13t) — **post-merge, never a merge gate**; you periodically *check* its
  `PASS`/`REGRESSION` result, you don't block on it.
- **Every machine-checked proof ships a negative control that *must* fail.** A
  check that can't fail proves nothing. This is the project's signature: each
  model/harness has a `-DBUG_*` / `_bug` / teeth variant the driver confirms is
  rejected. If you trust one thing here, trust that.
- **Advisory unless proven; skip-if-absent.** A new analysis starts informational
  and only becomes a hard gate once trusted not to false-positive; a phase whose
  tool isn't installed **skips clean** (prints how to install it), never fails the
  driver.

## The one driver — `scripts/check_all.sh`

Twelve phases, fastest first. Default run = `tests mn replay lincheck dst ctest`;
`all` adds `static sanitizers exttsan verify`; `bench`/`combo` are opt-in.

| phase | what it runs |
|---|---|
| `static` | `gcc -fanalyzer` (**gate** — found+fixed a real NULL-deref) + `cppcheck` (advisory) on the C core |
| `tests` | the pytest suite via `tests/run_isolated.py` (one file per subprocess — in-process `pytest tests/` flakes/hangs on cross-file state leaks; an 11-hour hang was observed) |
| `mn` | the M:N scheduler fuzzer `tools/mn_stress.py` (token-conservation, `--stable` gate form) |
| `replay` | controlled-M:N **deterministic replay** probes (`tools/mn_controlled/repro_{probe,select,timer}.py` — same seed must reproduce one signature) |
| `lincheck` | channel **linearizability** (Porcupine + a stateful select model) |
| `dst` | **deterministic simulation** seed sweep (`tools/dst/dst.py`) |
| `ctest` | the C deque concurrency stress (`tests_c/test_cldeque`, real threads × millions of ops) |
| `sanitizers` | the deque harness under ASan/TSan/UBSan (`setarch -R` for TSan vs 6.x ASLR) |
| `exttsan` | the **whole `runloom_c` ext** under ThreadSanitizer, driven by mn_stress + lincheck + a pytest subset under a free-threaded interpreter |
| `verify` | the formal proofs (`verify/run_verify.sh`: Spin + CBMC + herd7 + GenMC, plus Coq/Iris/Alloy/TLC when present) |
| `bench` | the rigorous microbench sweep (informational, machine-dependent — not in `all`) |
| `combo` | pairwise config-matrix interaction sweep |

## Formal verification — `verify/` (16 machine-checked checks, every one with teeth)

The threat model: a lock-free M:N scheduler driving OS threads into CPython's
internal state machines hides bugs in (a) rare interleavings and (b) weak-memory
reorderings x86-TSO masks but ARM/RC11 expose. No single engine covers both, so
the core is attacked from complementary directions.

**Engines** (`verify/README.md` is the authoritative per-check writeup):

| engine | what it proves | negative control that bites |
|---|---|---|
| **Spin** (14 Promela models) | the *algorithms* over **all** interleavings (SC), safety + liveness (LTL + weak fairness) | each model ships a `-DBUG_*`: `wake_state` (`BUGGY_DROP_WAKE`), `select_close` (4 controls), `hub_submit` (`BUG_NO_DEDUP`), `blockpool` (`BUG_DEC_BEFORE_REQUEUE`), `netpoll_commit` (`BUG_NO_COMMIT`), `netpoll_rearm` (`BUG_EDGE_TRIGGERED`), `netpoll_multipool` (`BUG_LOCK_ORDER` ABBA), `netpoll_deadline`/`forceunlink`, `cross_thread_wake` (`BUG_ROUTE_TO_WAKER`), `iouring_msclose` (`BUG_CONCURRENT_CLOSE`) |
| **CBMC** | the **unmodified `cldeque.c`** with its real `__atomic_*` orders, bounded (`CAP=4`) — 5 assertions `VERIFICATION SUCCESSFUL` | — |
| **herd7 litmus** | the C11/RC11 **fence placement** on netpoll-commit + wake_list, weak model | `commit_cas_then_publish` reachable (CAS-acquire alone insufficient) vs `commit_lock_publish` forbidden (the `pool->lock` round-trip is load-bearing) |
| **GenMC** | the **whole claim protocol as real C** (pthreads + C11) under RC11, every execution | `-DBUG_NO_LOCK` → a non-atomic race on `ready_out`; `sched_parkwake.c` `-DBUG_NO_SC_FENCE` → the lost wake (the park/wake fence, spec 04) |
| **TLA+ / TLC** | the *composed* scheduler (`RunloomSched`, no lost g), stall-recovery handoff (`RunloomHandoff`, no stranded work), and the **CPython STW + gilstate contracts** (`RunloomCPythonSTW`/`RunloomGilstate`, M1/M2/M4 + C3/C6, spec 09) | `Buggy=TRUE` / `Bypass=TRUE` / `DeleteOnOwner=FALSE` each violate |
| **Alloy** | the `runloom_self_check` parker-graph invariant | `BucketsAlwaysOnGlobal` SAT exhibits the dangling-bucket shape |
| **Coq** (unbounded) | `WakeState` INV1/INV2, deque conservation, select-claim, blockpool wake-order — over *every* reachable state by induction | each lemma rejects a buggy transition |
| **Iris / iRC11** (concurrent program logic) | exactly-once wake, the 3-state protocol under racing wakers, and the commit-publish release/acquire under **RC11** (machine-checked, unbounded) | two CAS winners ⇒ contradiction |

The 14 Spin models are not generic — they target the exact places real bugs lived:
the deque, the `wake_state` machine, `park_safe`/`wake_safe`, the **two select
protocols** (claim CAS + the full Phase-2-vs-send/close, which itself *found two
new races* beyond the fuzzer's three), the default hub-submit dedup, blockpool wake
order, the netpoll commit + LT-ONESHOT re-arm + multipool lock hierarchy + the
3-way deadline/cancel claim race + the force-unlink release lifetime, the io_uring
multishot handle lifetime, and Phase-C owner-routed wakes. (Scaffolded engines that
skip-clean until installed: **Dartagnan** — `.cat`+C bounded SMT bridging litmus
and CBMC; **Nidhugg** — a second weak-memory SMC; **rr-chaos**.)

## Testing harnesses — `tools/` (the dynamic side)

| tool | what it does |
|---|---|
| `mn_stress.py` | seeded M:N fuzzer: producers push a known multiset into a channel pool, consumers (`recv` + `select`) drain — **every token received exactly once**, `_self_check` clean between iters, under the watchdog so a hang prints its repro seed |
| `watchdog.py` | turns a silent hang into a full dump (every thread's stack via faulthandler + scheduler/netpoll `_self_check` + `stats()` + the lifecycle event ring) — `run_guarded` / `watchdog()` ctx-mgr / on-demand `hang_dump` |
| `hang_hunter/` | targeted workloads that reproduce specific hang classes (found the sentinel-pointer gc-churn SEGV) |
| `lincheck/` | **linearizability**: real producers/consumers as fibers on the M:N scheduler (overlapping real-time intervals, GIL off) → **Porcupine** (Go) decides whether some linearization satisfies the FIFO-channel spec; a stateful Hypothesis `RuleBasedStateMachine` generates op sequences with shrinking |
| `dst/` | **deterministic simulation**: a seeded oracle chooses yield points → a failing run reduces to one integer seed; modes `determinism`/`sweep`/`pct` (PCT bounded, Burckhardt ASPLOS'10); `selftest` confirms a broken invariant is caught + reproduces |
| `pct/` | the single-hub PCT controlled scheduler (runtime hook in `ready_pop`, env-gated, zero-cost off) + explorer |
| `mn_controlled/` | the **controlled M:N scheduler + deterministic replay** (the 6 levers: barrier census, startup gate, no-steal, census-idle guard, deterministic preemption, logical clock) + `repro_*` probes |
| `faultinj/` + `fault_sweep.py` | a dependency-free `LD_PRELOAD` shim fails the Nth malloc/calloc/realloc/mmap/epoll_ctl/eventfd/timerfd; the sweep classifies OK/GRACEFUL/CRASH/HANG (result: **0 cleanup-path bugs** on the bundled workload) |
| `run_sanitizers.sh` / `run_sanitizers_ext.sh` | the deque under ASan/TSan/UBSan; and the **whole ext** under TSan (preloaded libtsan + a CPython-only suppressions file) — *found+fixed five real scheduler/chan/netpoll data races* on first run |
| `build_tsan_cpython.sh` | recipe for a fully-TSan-instrumented free-threaded CPython (gold standard; currently blocked upstream) |
| `run_pydebug.sh` | the **`--with-pydebug --disable-gil` oracle** — CPython's own asserts fire at a boundary violation point instead of a release UAF later (earned contract C6, spec 09) |
| `mutate/` | mutation testing — a test that passes against a mutant is weak (found+fixed 2 weak blocking-shim tests) |
| `coverage.sh` + `cov_summary.py` | gcov, reported worst-first with the uncovered **error/cleanup** lines itemized (the fault-injection targets) |
| `leak_check.py` | FD / fiber / parked-parker leak balance across cycles |
| `combinatorial/` | covering arrays over the `RUNLOOM_*` config matrix (backend × GIL × handoff × preempt × …) — all t-way interactions in few runs (isolated the `STEAL_WOKEN=1` SIGSEGV on day one) |
| `racerd.sh` / `static_analysis.sh` | Infer/RacerD compositional static race detection + `gcc -fanalyzer` gate + cppcheck (advisory/skip-clean) |
| `security/` | the bridge fuzzer + a refcount-race probe + a signal-storm probe + the stack-scrub helper + a valgrind smoke (see `FINDINGS.md`, e.g. the S1 stack-leak that motivated the aio stack-scrub) |
| `heavy_frames/` | a DWARF `.eh_frame` profiler of the stdlib that generates `runloom_heavy_frames.h` (the fat-frame symbol table the stack auto-sizer's prescan reads, spec 10) |
| `tla_trace_conform.py` / `mn_trace_conform.py` | **trace conformance** — the ext emits its real transitions and TLC replays them through the model's own actions (gilstate + the MN baton fully conformed; STW only partially, spec 09) |
| `monkey_offload_stress.py` | the synthetic multiprocessing/offload corpus under `run(8)` + `monkey.patch()` (the foreign-thread-safety net, spec 14) |

## Determinism toolkit (how a 1-in-50k bug becomes reproducible)

Three layers, because *find it once* isn't enough — you must *replay* it:

1. **Deterministic simulation** (`dst`): the single-thread cooperative scheduler is
   deterministic, so a seed → byte-identical run; a failure reduces to one seed.
2. **Seeded delay injection** (`RUNLOOM_DELAY`): sleep a deterministic-per-(seed,
   site, count) interval at instrumented transition sites (STW, calibration-freeze,
   coro-reuse, handoff-adopt, snap) to *widen* the ~1/56k race windows so TSan
   reliably hits them.
3. **Controlled M:N replay** (`RUNLOOM_MN_SEED` + `RUNLOOM_MN_BARRIER`): a seeded
   execution baton serializes real hub scheduling so the parallel races become
   reproducible and seed-explorable — same seed ⇒ byte-identical run (6 levers,
   TLA-modeled, off by default, off-path regression-free).

## Profiling tier — `tools/bench/` + `bench/` (the "where does time go" category)

Was the empty category (`QUALITY_CAMPAIGN.md` P1); now: a **rigorous bench
harness** (`rigor.py` — Kalibera-Jones ISMM'13: repetitions + warmup + bootstrap
CIs + CV-instability flag + A/B significance), a **USL fit** (`usl.py` — Gunther's
Universal Scalability Law: extract contention α and coherence β from
throughput-vs-hubs; fit here α≈0.028, β≈0.0002), and scaffolded profilers
(`profile/coz_*` causal profiling, `offcpu.sh` eBPF park→wake latency,
`scalene_profile.sh` native-vs-Python split). `bench/io_compare/` is the
cross-runtime HTTP bench (vs asyncio/gevent/uvloop); results in
`bench/results/honest_bench.md` + `io_compare.md`.

## Conformance harnesses — `tests/`

- **asyncio conformance**: `test_asyncio_conformance.py` runs **CPython's own
  `Lib/test/test_asyncio` bodies verbatim** against `RunloomEventLoop`
  (`BaseSockTestsMixin` 13/13 after fixing 3 real gaps); companions cover
  `BaseTestBufferedProtocol` and the server side. (The third-party-suite side — 222
  name-brand projects — is spec 17.)
- **monkey / cooperative-stdlib conformance**: `test_{socket,threading,queue,
  selectors,signal,os_io,process,multiprocessing,fcntl,futures,heavy}_compat.py`
  plus the empirical single-hub-canary coverage sweep (spec 14).
- **backend conformance**: `test_netpoll_conformance.py` sweeps epoll/kqueue/IOCP/
  WSAPoll/select; `test_iouring*`, `test_kqueue_faultinject`, the arm64 C test.

## What the apparatus actually caught (provenance, not theater)

- the four `select` integration bugs (close-wake NULL, abort bare -1, abort drops
  value, spurious -2) — `mn_stress` + the `select_close` Spin model (which found 2
  *more*);
- five scheduler/chan/netpoll **data races** — whole-ext TSan, first run;
- the `getaddrinfo` codec-import stack overflow — `test_sync` under the guard page;
- the **park/wake lost wake** — GenMC (SC Spin couldn't see it);
- the **gilstate contract C6** — the pydebug oracle on a bare `mn_init/run/fini`;
- the `STEAL_WOKEN=1` SIGSEGV — the combinatorial sweep, day one;
- the one aio bug in the 222-project sweep — the done-callback ordering fix (spec 17).

## Invariants (about the process)

1. **No hosted CI** — local `check_all.sh`; the matrix runner is post-merge, never
   a gate.
2. **Every proof ships a negative control that must fail** — teeth, always.
3. **New analyses are advisory + skip-if-absent** until trusted.
4. **A rare interleaving bug must reduce to a seed and replay** (dst / delay
   injection / controlled M:N) before it's "understood."
5. **The model is checked against the code via trace conformance**, not assumed to
   match.
6. **Conformance = CPython's own test_asyncio verbatim + the cooperative-stdlib
   compat suites**, on top of the third-party corpus (spec 17).
