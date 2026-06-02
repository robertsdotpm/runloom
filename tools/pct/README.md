# tools/pct — Probabilistic Concurrency Testing (single-hub)

A controlled scheduler for pygo's single-hub `run()` path, enabled by
`PYGO_PCT_SEED`. Instead of FIFO, the ready-pop runs the highest-priority ready
goroutine, with random priorities + `d-1` random priority **change points** that
demote a goroutine mid-run — the PCT algorithm, with a probabilistic lower
bound on finding any depth-`d` bug.

> Burckhardt, Kothari, Musuvathi, Nagarakatte, *A Randomized Scheduler with
> Probabilistic Guarantees of Finding Bugs*, ASPLOS 2010.

Runtime hook: `src/pygo_core/pygo_sched.c` (`pygo_pct_*`, in `pygo_sched_ready_pop`).
Off by default — one predicted-not-taken branch, zero cost when `PYGO_PCT_SEED`
is unset. Env: `PYGO_PCT_SEED`, `PYGO_PCT_DEPTH` (d, 3), `PYGO_PCT_STEPS` (k,
2000), `PYGO_PCT_DEBUG`.

```sh
PY=~/.pyenv/versions/3.13.13t/bin/python3
$PY tools/pct/pct_explore.py demo                 # schedule diversity + conservation
$PY tools/pct/pct_explore.py sweep tests/test_chan.py --seeds 30 --depth 3
```

Demo result here: FIFO explores **1** schedule; 40 PCT seeds explore **33
distinct** channel-wake schedules, all conservation-clean.

## Scope — read before relying on it

PCT controls **only the single-hub cooperative `run()` path**, where the
ready-pop order *is* the whole schedule. It deliberately does **not**:

- **reach the M:N hubs** — those are real parallel OS threads; PCT can't
  serialize them, and that's where pygo's hardest concurrency bugs live. (For
  the M:N path: `tools/mn_stress.py`, `exttsan`, `verify/`.)
- **reach the aio `call_soon` ordering** — that FIFO is a Python-level queue
  inside one loop goroutine, so the scheduler ready-ring PCT permutes is only
  0–1 deep there. (Guarded separately: `pygo_compat/call_soon_fifo.py`.)
- **permute pure-compute `yield_`** — single-hub `yield_` has a
  run-to-completion fast path when nothing else is parked.

Its real, demonstrated surface is: several goroutines parked on
channels/select/sleep in single-hub mode, whose **wake order** it permutes —
a genuine complement to `lincheck` for ordering robustness.

## Keep / drop

This is **experimental**. It is correct, regression-free, and free when off,
and it adds a real (if narrow) capability plus a foundation for the
higher-value follow-up — a **seedable, controlled M:N scheduler** (deterministic
replay across hubs), which is where pygo's concurrency risk actually
concentrates. See `QUALITY_CAMPAIGN.md` (P4.1) for the full evaluation.
