# lifefuzz — generative, replayable life-cycle fuzzer

`lifefuzz` mass-produces structurally-diverse runloom programs that exercise the
**object life-cycle** operations and runs each under the **life-cycle oracles** —
the dynamic counterpart to the `verify/` life-cycle models. Where those models
*prove* "every object is allocated, owned by the right thread, and freed exactly
once," this *tries to violate it at scale* on the real extension.

It is the dynamic realization of the bug-hunt plan: **new bugs come from
executions the code has never run, crossed with an oracle that notices when they
go wrong.** lifefuzz generates the executions; the oracles are the net.

## Why this isn't a duplicate of the existing tools

The `tools/` suite is mature, but every existing fuzzer holds the *workload*
roughly fixed and varies one other axis:

| tool | varies | oracle | gap lifefuzz fills |
|---|---|---|---|
| `mn_stress.py` | seeds, one token-conservation workload | conservation + self-check + hang | no life-cycle stressors (stacks/nesting/timers/scratch), no life-cycle oracles |
| `dst/`, `pct/`, `mn_controlled/` | the **schedule** (yield points / PCT priorities) | conservation | fixed workload; single-hub or controlled-baton only |
| `combinatorial/` | the **config** (4-knob covering array) | runs `mn_stress` | one fixed workload per config |
| `hang_hunter/` | random/Hypothesis programs | **hangs / crashes only** | no life-cycle oracles (freed-state, migration, refcount-ledger), not life-cycle-targeted |
| `faultinj/` | injected errno | crash on cleanup | fixed workload |
| `security/fuzz_bridge.py` | network chaos | crash/hang/leak | the aio transport, not the core life-cycle |

lifefuzz is the missing axis: it varies the **program structure itself** over the
life-cycle operations, and it **composes** the others rather than duplicating
them — each run is a point in *workload × schedule (`RUNLOOM_MN_SEED`) × config
(the 4 knobs)* space, and it reuses the proven conservation kernel from
`mn_stress.py` and the watchdog from `watchdog.py`.

## What each knob targets (the model-map)

Every generator knob is aimed at a `verify/` life-cycle model, so a finding maps
back to a proven (or gated) invariant:

| generator knob | exercises | model |
|---|---|---|
| `prod_stacks` / `cons_stacks` (16 KB … 512 KB pins) | cross-hub stack-depot push/pop/flush/size-mismatch reuse | `spin/stack_depot.pml` (#1) |
| `nest` (nested goroutine spawn under M:N) | snap / per-g migration across hubs | `tla/RunloomTstateMigration.tla` (#2), `genmc/mimalloc_page_free.c` |
| `scratch` (buffered chan filled then dropped undrained) | `Chan` dealloc must release buffered PyObject refs | `cbmc/chan_refflow_cbmc.c` (#8), `genmc/chan_refcount.c` (#10) |
| `cons_select` + closer (select racing close) | the select + close life-cycle | tools/README Finding A, `cbmc/fiber_admit` |
| `timer_us` (`sched_sleep` between sends) | deadline heap + park/wake + freed-state timer entry | timer-entry oracle (`RUNLOOM_DBG_GSTATE`) |
| `nprod`/`ncons`/spawn counts | fiber-admission slot conservation | `cbmc/fiber_admit_cbmc.c` (#7) |
| `scale` (×8–20 counts, up to 8 hubs) | stack-depot recycle + admission *at scale* | `spin/stack_depot.pml` (#1), `cbmc/fiber_admit` (#7) |

### Program kinds

Two kinds are generated (the seed picks; `kind` in `gen`):

- **core** (default, ~75%) — `runloom_c` goroutines + channels + select + timers,
  the table above.
- **aio** (~25%) — a small asyncio program under `runloom.aio` (`paio.run`): a
  known token multiset over an `asyncio.Queue`, `create_task` + **task cancel**
  mid-flight, **`call_later` timers cancelled** before firing, and optional
  **`run_in_executor`**. Reaches the seams the core path can't:

| aio knob | exercises | model / invariant |
|---|---|---|
| `aio_timers` (call_later + cancel) | a cancelled timer's goroutine holds no ref to its callback graph | aio `timer_leak.py` invariant |
| `aio_decoys` (task cancel mid-flight) | task teardown + cancel of a parked wait | `project_pygo_cancel_wait_fd` |
| `aio_executor` (run_in_executor) | the blockpool stack-`job` cross-thread lifetime | `genmc/blockpool_job.c` (#3) |

## The oracles (the net)

Each run is checked against all of:

1. **token conservation** — sent multiset == received multiset (every value once).
2. **goroutine completion** — `mn_run()`'s completed count == goroutines spawned.
3. **parked-leak** — `sleeping + netpoll_parked + running == 0` after the run.
4. **scheduler self-check** — `runloom_c._self_check()` reports 0 violations.
5. **runtime DBG oracles** — `RUNLOOM_DBG_GSTATE` (freed-state), `RUNLOOM_DBG_MIGRATE`
   (per-g tstate cross-thread use); their stderr warnings are captured by the parent.
6. **hang watchdog** — a lost wakeup becomes a `TimeoutError`, not a wedge
   (programs are always-terminating by construction, so a hang is a real bug).
7. **ASan / TSan** — if the ext was built with a sanitizer, its report is captured.

## Replayability

A finding reduces to a one-liner. The program is a pure function of its seed and
the schedule is pinned by `RUNLOOM_MN_SEED`, so:

```sh
tools/lifefuzz/lifefuzz.py repro <seed> --mn-seed <S>     # re-run the exact execution
tools/lifefuzz/lifefuzz.py gen   <seed>                   # inspect the generated spec
tools/lifefuzz/lifefuzz.py shrink <seed> --mn-seed <S>    # delta-debug to a minimal spec
```

Findings are written to `tools/lifefuzz/corpus/seed_<N>.json` (signal + stderr tail).

## Usage

```sh
# default-path hunt: 8000 config-diverse programs, 56 parallel workers
tools/lifefuzz/lifefuzz.py sweep 8000 --workers 56 --mn-seed 200000

# single run / inspect / repro
PYTHON_GIL=0 PYTHONPATH=src tools/lifefuzz/lifefuzz.py run 42
tools/lifefuzz/lifefuzz.py repro 42 --mn-seed 1
```

### Teeth check (the negative control)

A fuzzer that has only ever found zero bugs is worthless until it is shown to
catch a *planted* one. `--unsafe-migrate` flips on the gated per-g-tstate
migration (`RUNLOOM_PER_G_TSTATE=1 RUNLOOM_ALLOW_UNSAFE_MIGRATION=1`) — the known
mimalloc abandon/adopt hazard — and the migration oracle must then fire:

```sh
tools/lifefuzz/lifefuzz.py sweep 120 --unsafe-migrate --mn-seed 5000
# -> findings: "[RUNLOOM_DBG_MIGRATE] ... _mi_page_retire corruption is imminent"
```

If this stops producing findings, the oracle-capture pipeline has regressed —
treat it as a tooling failure, exactly like a `verify/` negative control that
stops failing.

## Composing with the rest of the suite

- Build the ext under **ASan** first (`RUNLOOM_EXTRA_CFLAGS=-fsanitize=address …`,
  see `security/fuzz_bridge.py` header) and the sweep gains a memory-error oracle.
- Run under the **gold-standard TSan** interpreter (`build_tsan_cpython.sh`) and
  it gains a data-race oracle on every generated interleaving.
- Feed a finding's seed into `dst/` or `pct/` to drive a *targeted* schedule
  search around the failing workload.
