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
  the M:N path: `tools/mn_stress.py`, `exttsan`, `verify/`, and the controlled
  M:N scheduler `PYGO_MN_SEED` / `tools/mn_controlled/`.)
- **permute pure-compute `yield_`** — single-hub `yield_` has a
  run-to-completion fast path when nothing else is parked.

**Do NOT run PCT against the asyncio bridge (`pygo.aio`) tests.** Regular
`loop.call_soon` spawns one goroutine per callback (`_go_io`, for the roomy
stack protocol callbacks need — see the aio invariants in CLAUDE.md), so the
call_soon callbacks ARE ready-ring goroutines that PCT permutes. But asyncio
*guarantees* call_soon FIFO (a future's awaiter resumes before a later-scheduled
callback; `await sleep(0)` drains all ready callbacks before resuming). pygo.aio
satisfies that in production because the default scheduler is FIFO — so permuting
it is exploring an interleaving the asyncio contract FORBIDS, i.e. a FALSE
POSITIVE, not a bug. (Confirmed 2026-06-03: under PCT seeds, `test_edge_cases.
test_done_callback_after_done_fires_immediately` drops the callback,
`test_stress.test_sleeper_storm` exceeds its wake-order budget, etc. — all the
same call_soon/wake-FIFO reordering; the runtime primitives themselves (park/
wake, channels) stay correct under PCT.) An earlier version of this note wrongly
claimed PCT "does not reach call_soon ordering" — it does; the right conclusion
is simply that PCT's valid surface is raw goroutine/channel/select code, not the
FIFO-ordered asyncio callback layer. (`call_soon_threadsafe` IS a real
Python-level FIFO queue drained in order by the keepalive, untouched by PCT;
regression-guarded by `pygo_compat/call_soon_fifo.py`.)

Its real, demonstrated surface is: several goroutines parked on
channels/select/sleep in single-hub mode, whose **wake order** it permutes —
a genuine complement to `lincheck` for ordering robustness.

## Keep / drop

This is **experimental**. It is correct, regression-free, and free when off,
and it adds a real (if narrow) capability plus a foundation for the
higher-value follow-up — a **seedable, controlled M:N scheduler** (deterministic
replay across hubs), which is where pygo's concurrency risk actually
concentrates. See `QUALITY_CAMPAIGN.md` (P4.1) for the full evaluation.
