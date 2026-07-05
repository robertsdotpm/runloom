# Soak harness — fail on a slope, not a crash

_docs/dev/RELIABILITY_PROGRAM.md R1._  The standard long-uptime methodology,
made runnable: drive realistic workloads for hours while sampling the R0 gauge
surface, and **fail on a rising trend line, not on a crash**.  A leak of 100
bytes per connection never crashes a 48-hour run — but it shows up as a visible
upward slope within the first hour.  Slopes catch at hour 1 what would crash at
hour 700.

## Run it

```sh
# the R1 acceptance run — a 2-hour mixed soak, 4 workers:
python3 tools/soak/soak.py --workload mixed --hours 2 --workers 4

# the negative control — MUST report FAIL (proves the oracle has teeth):
python3 tools/soak/soak.py --workload leak_control --minutes 5

# accelerated-life + mode knobs:
python3 tools/soak/soak.py --workload mixed --hours 1 --compress \
      --env RUNLOOM_PERHUB_EPOLL=1 --env RUNLOOM_IOURING_LOOP=1
```

Each run writes `docs/dev/soak/soak_<workload>_<NNN>/` containing one
`worker<i>.csv` per worker (the raw samples) and a one-page `REPORT.md` verdict.
Only the REPORT.md files are committed as ledger entries; the CSVs are
gitignored (they are large and reproducible).

Re-run the oracle on an archived CSV without re-soaking:

```sh
python3 tools/soak/oracle.py docs/dev/soak/soak_mixed_003/worker0.csv --warmup 600
```

## Pieces

| file | role |
|---|---|
| `soak.py` | orchestrator: launch N workers, watch heartbeats for hangs/crashes, run the oracle, write REPORT.md |
| `worker.py` | one worker process: run a workload continuously, self-sample `/proc/self` + `runloom.stats()` every interval to CSV + a heartbeat |
| `workloads.py` | the workload shapes (below) + the `leak_control` negative control |
| `oracle.py` | the slope oracle: least-squares fit + 95% CI + per-metric epsilon + absolute-change floor |

## Workloads

Each maximizes object-lifecycle turnover (spawn/join, connect/close, park/wake)
— long-uptime bugs are about how many times the create/destroy cycle ran, not
wall-clock:

- `spawn_churn` — goroutine create/die (ages the g slab + coro-stack depot)
- `chan_select` — producer→consumer chan pipeline, joined (chan park/unpark)
- `timer` — sleep-heap + timed-parker storm
- `tcp_churn` — connect/serve-one-echo/close storm (netpoll parkers + fd arm cache)
- `keepalive` — many idle conns + periodic ping (the N=1M long-dwell shape)
- `offload` — blocking-pool offload churn
- `mixed` — all of the above interleaved (**default**, highest signal)
- `leak_control` — **deliberately leaks** 4 KB/unit forever; the oracle MUST fail it

## The oracle (why a slope, and how it stays honest across timescales)

After a warmup (which absorbs one-time setup: lazy imports, pool priming,
reaching peak concurrency), `oracle.py` fits an ordinary-least-squares line to
each metric over time and **passes iff the slope is indistinguishable from
flat** — any one of:

1. the slope's **95% confidence interval includes 0** (statistically flat), or
2. `|slope|` is below a **per-metric epsilon** (a real but harmless drift —
   e.g. RSS creeping < 4 MB/h is allocator noise, not a leak), or
3. the **total fitted change across the window** is below an absolute floor
   (e.g. < 8 MB of RSS movement).

Guard 3 is what makes the harness usable at *any* duration.  On a short run,
pool settling produces a small absolute change that OLS extrapolates to a
scary-looking per-hour slope; the floor absorbs it.  On a real multi-hour soak
a genuine leak's total change dwarfs the floor, so it is still caught — the
negative control leaks gigabytes and fails unmistakably.

Cumulative **odometers** (`mn_completed_total`, `stale_arm_heals`, `progress`,
…) are excluded — a soak *wants* those climbing; they confirm work is actually
happening.

## `--churn-compress` — accelerated life

Long-uptime reliability is almost never about *time*; it is about how many
create/destroy cycles ran.  `--compress` drops the inter-unit cooperative
yields so a worker spends 100% of its wall-clock in the lifecycle churn.  One
day of compressed churn ages the runtime's object lifecycles like months of
steady real traffic, so a per-cycle leak (a struct, an fd, a parker not
released on some path) reaches a detectable slope far sooner.  It does **not**
compress wall-clock-bound waits (a real `sleep` still sleeps); it removes only
the artificial idle between units.

## Hang / crash detection

Each worker's sampler rewrites a heartbeat file `<elapsed> <progress> <alive>`
every interval.  The orchestrator watches it: a worker whose **progress counter
freezes** while it should still be running is a scheduler wedge — the
orchestrator captures a `tools/hang_hunter/triage.py` gdb dump (all-thread
backtraces + stop-the-world / hub state) **before** killing it, so the hang is
diagnosable.  A worker that exits non-zero is a crash.  Either fails the run.
(Live gdb attach needs `kernel.yama.ptrace_scope=0` or root; it degrades to a
note otherwise.)

## Modes matrix

Any `--env KEY=VAL` is passed to the workers, so the same workload can be soaked
under each scheduler mode: `RUNLOOM_PERHUB_EPOLL`, `RUNLOOM_IOURING_LOOP`,
`RUNLOOM_STACK_PARK_SWEEP`, hub count via the workload's own env.  R2
(`tools/soak/matrix.sh`) drives these presets across durations and sanitizers.
