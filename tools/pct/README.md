# tools/pct — Probabilistic Concurrency Testing (single-hub)

A controlled scheduler for runloom's single-hub `run()` path, enabled by
`RUNLOOM_PCT_SEED`. Instead of FIFO, the ready-pop runs the highest-priority ready
goroutine, with random priorities + `d-1` random priority **change points** that
demote a goroutine mid-run — the PCT algorithm, with a probabilistic lower
bound on finding any depth-`d` bug.

> Burckhardt, Kothari, Musuvathi, Nagarakatte, *A Randomized Scheduler with
> Probabilistic Guarantees of Finding Bugs*, ASPLOS 2010.

Runtime hook: `src/runloom_c/runloom_sched.c` (`runloom_pct_*`, in `runloom_sched_ready_pop`).
Off by default — one predicted-not-taken branch, zero cost when `RUNLOOM_PCT_SEED`
is unset. Env: `RUNLOOM_PCT_SEED`, `RUNLOOM_PCT_DEPTH` (d, 3), `RUNLOOM_PCT_STEPS` (k,
2000), `RUNLOOM_PCT_DEBUG`.

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
  serialize them, and that's where runloom's hardest concurrency bugs live. (For
  the M:N path: `tools/mn_stress.py`, `exttsan`, `tools/verify/`, and the controlled
  M:N scheduler `RUNLOOM_MN_SEED` / `tools/mn_controlled/`.)
- **permute pure-compute `yield_`** — single-hub `yield_` has a
  run-to-completion fast path when nothing else is parked.

**asyncio-aware (call_soon-FIFO preserved).** Regular `loop.call_soon` spawns
one goroutine per callback (`_go_io`, for the roomy stack protocol callbacks
need), and task steps are scheduled the same way, so those callbacks ARE
ready-ring goroutines. asyncio *guarantees* them call_soon-FIFO — permuting them
is exploring an interleaving the contract FORBIDS (a false positive, not a bug).
So the aio bridge marks every such goroutine `fifo` (`runloom_c.fiber(fn,
fifo=True)` → `g->pct_fifo`), and PCT keeps `fifo` goroutines in spawn order:
only the OLDEST ready `fifo` g is a pick candidate, younger ones wait their
turn. PCT may still freely interleave *raw* (un-marked) goroutines/channels —
even between two call_soon callbacks — it just can't run a later callback before
an earlier one. Result: PCT is safe to run on `runloom.aio` (asyncio test surface
clean across seeds — `test_edge_cases`, `test_aio*`, `test_asyncio_conformance`),
while still exploring raw concurrency in mixed programs. Note that asyncio
scheduling is essentially FIFO-deterministic, so PCT finds little in *pure*
asyncio code (no legal reordering freedom); its bug-finding value is raw
goroutine/channel/select code and the raw parts of mixed programs.
(`call_soon_threadsafe` is a separate Python-level FIFO queue drained in order by
the keepalive, never on the ready ring; regression-guarded by
`runloom_compat/call_soon_fifo.py`.)

Its real, demonstrated surface is: several (raw) goroutines parked on
channels/select/sleep in single-hub mode, whose **wake order** it permutes —
a genuine complement to `lincheck` for ordering robustness. (Tests that ASSERT a
specific raw wake order — `test_stress.test_sleeper_storm`, the
`TestParkWakeRace` yield-coordination tests — are by design not PCT-robust and
will "fail" under a seed; that is PCT exploring, not a runtime bug.)

## Logical clock for timers (`RUNLOOM_LOGICAL_CLOCK`)

To make a TIMER schedule replay deterministically (and make `loop.time()` exact),
`RUNLOOM_LOGICAL_CLOCK=1` measures `sched_sleep` deadlines + the aio `loop.time()`
against a logical clock the single-thread drain advances — at quiescence only —
straight to the earliest pending deadline (the single-thread analogue of the M:N
logical clock, `tools/mn_controlled/` lever 6). So `loop.time()` advances by exact
logical amounts (a two-`sleep` `0.05 + 0.03` measures **exactly** `0.08` every run,
not `0.0807…`), and timer firing order is a function of the schedule, not wall
time. Combine with `RUNLOOM_PCT_SEED` for full order+time deterministic replay.

**Closed workloads only.** Real network I/O still rides the wall clock (the
netpoll pump), and the logical clock advances *only* when no fd is parked, so a
workload with live fds is not made deterministic — and a real-I/O *timeout* can't
fire under it (its logical deadline never advances while the I/O is parked), so
forcing it on a real-I/O test will hang. Off by default; the aio keepalive's
real-time heartbeat uses `sched_sleep_real` so it is never advanced by it.

## Keep / drop

This is **experimental**. It is correct, regression-free, and free when off,
and it adds a real (if narrow) capability plus a foundation for the
higher-value follow-up — a **seedable, controlled M:N scheduler** (deterministic
replay across hubs), which is where runloom's concurrency risk actually
concentrates. See `QUALITY_CAMPAIGN.md` (P4.1) for the full evaluation.
