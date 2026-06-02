# tools/bench — rigorous microbenchmarking

Performance numbers are easy to get and easy to get *wrong*. This harness
exists so a pygo speed claim survives scrutiny.

## The two failure modes it defends against

1. **Autocorrelated samples → fake confidence.** Repetitions inside one
   process share caches, branch predictors, allocator state and a fixed
   memory layout, so they're correlated; a CI computed from them is far too
   tight. The honest CI comes from *independent process executions*.
   — Kalibera & Jones, *Rigorous Benchmarking in Reasonable Time*, ISMM 2013.

2. **Layout bias → lucky wins.** Code/stack/heap layout — perturbed by
   something as trivial as the size of an environment variable or the link
   order — can move a measurement by more than the change under test. A
   single pinned layout can hand one build a win that vanishes on another.
   — Mytkowicz et al, *Producing Wrong Data Without Doing Anything Obviously
   Wrong!*, ASPLOS 2009.

## How it answers them

- **Two-level sampling.** `--runs` independent OS processes (outer) each run
  `--inner` timed repetitions (inner, first `--warmup` discarded). Each
  process contributes one point (its inner median); the reported 95% interval
  is a **nonparametric bootstrap** over those independent points — no
  normality assumption, pure stdlib.
- **Layout-bias guard (on by default).** Every child gets a random-length
  `PYGO_BENCH_PAD` env var, so layout varies run-to-run and the interval
  *absorbs* layout sensitivity instead of hiding it. `--pin` turns it off for
  a tightly controlled single-state A/B.
- **A noise gate.** Reports the coefficient of variation and warns when it's
  high enough (>5%) that you shouldn't trust small deltas.
- **A real A/B test.** `ab base.json new.json` bootstraps the *difference of
  medians*; it only calls a change real when the difference's CI excludes 0.

## Usage

```sh
PY=~/.pyenv/versions/3.13.13t/bin/python3

$PY tools/bench/rigor.py list
$PY tools/bench/rigor.py run spawn                      # full run, real CI
$PY tools/bench/rigor.py run chan_pingpong --runs 20 --inner 5

# regression check across a change:
$PY tools/bench/rigor.py run spawn --json before.json   # on the base commit
# ... make the change, rebuild ...
$PY tools/bench/rigor.py run spawn --json after.json
$PY tools/bench/rigor.py ab before.json after.json       # significant?
```

Or the whole sweep (also the `bench` phase of `scripts/check_all.sh`):

```sh
PYTHON=$PY bash tools/bench/bench.sh        # all workloads
BENCH_RUNS=4 BENCH_INNER=2 PYTHON=$PY bash tools/bench/bench.sh   # quicker
```

`bench` is **informational and opt-in** — deliberately *not* in the gating
`check_all.sh all` set, because absolute throughput is machine-dependent. It
fails only if a workload crashes, never on a number.

## Workloads (`workloads.py`)

| name | what it stresses | op |
|------|------------------|----|
| `spawn` | goroutine create + drain (alloc + stack-pool + handle) | spawn+run |
| `chan_pingpong` | unbuffered send/recv park-wake roundtrip | roundtrip |
| `chan_buffered` | buffered-channel fast path (mostly no parking) | item |
| `yield_storm` | bare scheduler context-switch throughput | ctxsw |

Add one: write a `() -> (ops, seconds)` function and register it in
`WORKLOADS`. The harness handles everything else.
