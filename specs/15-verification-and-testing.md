# 15 — Verification and testing (how the invariants are actually proven)

Ground truth: `verify/` (the formal models), `tools/` (the testing harnesses),
`scripts/check_all.sh` (the one driver), `docs/dev/VALIDATION.md`,
`QUALITY_CAMPAIGN.md`, `CLAUDE.md` (the "No hosted CI" + tooling notes).

This spec is part of the *design*, not an afterthought: several invariants in the
preceding specs (the park/wake fence, the deque orderings, the netpoll claim) were
**derived from or proven by** these checks. A re-implementer who skips them will
re-introduce the exact bugs the specs warn about, because the bugs live in rare
interleavings that ordinary tests miss ~50000 runs out of 50001.

## The governing rules

- **No hosted CI, ever.** GitHub Actions is not free (macOS minutes especially)
  and is deliberately absent (`CLAUDE.md`, repeated). The "CI" is **local**:
  `scripts/check_all.sh [phases]` (`tests mn lincheck dst ctest sanitizers
  exttsan verify`). A separate cron-driven shell runner on the dev box does
  post-merge matrix validation (Linux 3.13t + Windows 3.12/3.13t + macOS arm64) —
  **post-merge, not a gate**; you never block a merge on it.
- **Every machine-checked proof ships a negative control that *must* fail.** A
  check that can't fail proves nothing. This is the project's signature
  methodology: each model/harness has a `_bug` / `-DBUG_*` / teeth variant that the
  driver confirms is rejected. If you trust one number from this spec, trust that.
- **Advisory unless proven; skip-if-absent.** A new analysis starts informational
  and only becomes a hard gate once trusted not to false-positive; a phase whose
  tool isn't installed *skips clean* (prints how to install it), never fails the
  driver. (`QUALITY_CAMPAIGN.md`.)

## Why so many angles (the threat model)

A lock-free M:N scheduler driving OS threads into CPython's internal state machines
hides bugs in (a) rare thread interleavings and (b) weak-memory reorderings that
x86-TSO masks but ARM exposes. No single tool covers both. So the core is checked
from independent directions, each with teeth:

| Tool | What it proves / finds | The teeth (negative control) |
|---|---|---|
| **Spin** (Promela) | the algorithms (Chase-Lev, `wake_state`, park/wake) over *all* interleavings — safety **and** liveness (LTL + weak fairness) | `live_wake -a` fails without `-f` (fairness provably necessary); `live_deque -DBUG_BLOCKING` |
| **CBMC** | the **unmodified C source** of the deque + sched ring/pystate, with its real `__atomic_*` orderings, bounded | a buggy transition rejected |
| **herd7 / GenMC** | the C11/RC11 **fence placement** on netpoll-commit + park/wake, on a *weak* (RC11) memory model, on real C | `-DBUG_NO_SC_FENCE` reproduces the park/wake lost wake; `-DBUG_NO_LOCK` the commit race |
| **TLA+ / Alloy** | the *composed* scheduler + stall-recovery handoff (no lost/stranded g); the STW/gilstate contracts; the `self_check` parker-graph invariant | `Buggy=TRUE` / `Bypass=TRUE` / `BucketsAlwaysOnGlobal` each violate |
| **Coq / Iris / iRC11** | unbounded, machine-checked: `wake_state` INV1/INV2, deque conservation, select-claim, blockpool wake-order; and the commit-publish release/acquire under **RC11** | each lemma has a buggy-transition rejection |

And on the testing side:

| Harness | What it does |
|---|---|
| **pytest suite** (+ `run_isolated.py`, one file per subprocess) | the functional suite; in-process `pytest tests/` flakes on cross-file state leaks |
| **`mn_stress`** | a randomized M:N scheduler fuzzer (token conservation across hubs) |
| **lincheck** (Porcupine + stateful Hypothesis) | channel **linearizability** to Go-FIFO under real multi-hub runs (spec 07) |
| **dst** (deterministic simulation) | seed → byte-identical repro of the cooperative scheduler + channel/select; a failing run reduces to **one integer seed** |
| **fault injection** (`tools/faultinj` + `fault_sweep.py`) | an `LD_PRELOAD` shim fails the Nth malloc/mmap/epoll_ctl/eventfd so cleanup branches run; classifies OK/GRACEFUL/CRASH/HANG (result: **0 cleanup-path bugs** on the bundled workload) |
| **gcov coverage** | itemizes uncovered error/cleanup lines (the priority targets for fault injection) |
| **mutation testing** (`tools/mutate`) | found + fixed weak tests (a test that passes against a mutant is weak) |
| **sanitizers** | the C core runs clean under **ASan / TSan / UBSan**, plus whole-ext TSan under free-threaded CPython (preloaded libtsan + `setarch -R`); `gcc -fanalyzer` is a *gating* static phase (found + fixed a real NULL-deref) |
| **`--with-pydebug` oracle** | builds against `--with-pydebug --disable-gil` so CPython's own asserts fire at a boundary violation (spec 09) instead of a release UAF later — earned contract C6 |

## The determinism toolkit (how a rare bug becomes reproducible)

Three layers, because "find it once" isn't enough — you need to *replay* it:

1. **Deterministic simulation (dst).** The single-thread cooperative scheduler is
   deterministic, so a seeded oracle choosing yield points makes the whole run
   reproducible — a failing run reduces to one seed. (PCT mode = Burckhardt et al.
   ASPLOS'10: few, well-placed preemptions with a probabilistic lower bound on
   finding depth-d bugs.)
2. **Seeded delay injection (`RUNLOOM_DELAY`).** At each instrumented scheduler
   transition site, sleep a deterministic-per-(seed, site, call-count) interval to
   *widen* the narrow race windows (STW, calibration-freeze, coro-reuse,
   handoff-adopt, snap) that the fuzzer otherwise hits ~1/56k — turning
   "statistically findable" into "reliably reproducible," so TSan hits the race.
3. **Controlled M:N scheduler + deterministic replay** (`RUNLOOM_MN_SEED` +
   `RUNLOOM_MN_BARRIER`). A seeded execution baton serializes the *real* hub
   scheduling so the parallel races where the hard bugs live become reproducible
   and seed-explorable. Same seed ⇒ byte-identical run, achieved via six levers
   (barrier-rendezvous census, startup gate, no-steal, census-idle wake-guard,
   deterministic frame-count preemption, a logical clock for timers). All off by
   default, zero cost when unset, off-path regression-free. (`QUALITY_CAMPAIGN.md`
   P4.4 has the full story.)

## Trace conformance — checking the *code* against the *model*

A model checked by TLC proves things about *itself*; nothing guarantees the C
implements it. **Trace conformance** closes that gap: the extension emits a trace
of its real transitions (`RUNLOOM_GILSTATE_TRACE`, `RUNLOOM_MN_EVENTS`) and TLC
replays that trace through the model's *own* actions — so the actual run is checked
against the actual spec, no re-transcription. Fully doable when a model's
transitions are all runloom-side events (gilstate create/delete, the controlled
baton); only partial when the machine lives inside CPython (the STW handshake),
where the pydebug oracle exercises it instead (spec 09).

## How this connects back to the specs

- The **park/wake seq_cst fence** (spec 04) is in the code *because* GenMC's RC11
  model found the lost wake the SC Spin model couldn't see.
- The **Chase-Lev seq_cst orderings** (spec 05) are non-weakenable *because*
  `test_cldeque` reproduces a duplication on real ARM when you weaken them.
- The **netpoll commit CAS** (spec 06) linearization point is checked in
  `verify/genmc/netpoll_claim.c`.
- The **six CPython boundary contracts** (spec 09) are under TLA+ models with
  `Bypass=TRUE` controls and the pydebug oracle.

That is why this is a *spec*, not a test log: the verification is load-bearing on
the design's correctness, and a re-derivation that omits it is not the same system.

## Invariants (about the process itself)

1. **No hosted CI.** Local `scripts/check_all.sh`; the matrix runner is post-merge,
   never a gate.
2. **Every proof ships a negative control that must fail** — checks must have teeth.
3. **New analyses are advisory + skip-if-absent** until trusted.
4. **A rare interleaving bug must be reduced to a seed and replayed** before it's
   considered understood (dst / delay injection / controlled M:N).
5. **The model is checked against the code via trace conformance**, not assumed to
   match.
