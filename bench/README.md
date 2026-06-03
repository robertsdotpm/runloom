# runloom performance-benchmark campaign

A reproducible, statistically-honest measurement layer for runloom, plus the
profiling drivers used to explain the numbers.  This is **measurement
infrastructure**, not optimization work: the goal is that any number is
diffable, has its full environment recorded, and a regression is visible.

Primary target runtime is free-threaded CPython **3.13t** (runloom's M:N hub
pool only gets real core-level parallelism with the GIL off).  GIL'd 3.13,
asyncio, and the Go loadgen are comparison baselines.

## Why not just the existing `bench/bench_*.py`?

Those print one wall-clock number from one run -- no warmup, no repetition,
no dispersion, no environment record.  Useful as a smoke check, useless as a
measurement you can compare across days or use as a regression gate.

## Layout

| Path | What |
| --- | --- |
| `harness.py` | env capture + CPU pinning + warmup/samples + median/MAD/min + bootstrap-CI median + JSON writer |
| `micro.py` | single-hub scheduler microbenchmarks (spawn, yield, chan ping-pong, buffered chan) |
| `mn.py` | M:N CPU-bound core-scaling (1..N hubs on 3.13t) |
| `results/*.json` | committed baselines; the regression gate diffs against these |
| `profile/` | profiling drivers (cProfile, perf stat/record, perf c2c, bpftrace, memory) |
| `../scripts/bench.sh` | one-shot driver: cleanest-env run of the whole suite + report |

## Methodology

- **Build**: production-representative `-O2` (the default), plus `-g` so
  `perf --call-graph dwarf` gets accurate stacks.  Never ASan/`-O0` for a
  perf number.
- **Pinning**: this is a 64-vCPU / 2-NUMA-node VM *shared with a desktop
  session*.  We pin to a contiguous CPU set on **one NUMA node** (default
  node1, cpus 32+) to dodge cross-NUMA latency and OS/desktop preemption on
  the low cpus.  Frequency governor + turbo are not exposed (virtualized),
  so variance control is affinity + ASLR-off + statistics, not pstate.
- **Stats**: median + MAD + min(best) + %RSD + bootstrap 95% CI of the
  median.  Median/MAD are robust to the occasional preemption spike a mean
  would smear; `min_s` is the cleanest lower bound.
- **GC**: collected untimed before each sample, frozen during it.

## Run

```sh
# whole suite, cleanest env (ASLR off, pinned, one NUMA node)
scripts/bench.sh

# a single suite by hand
PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python -m bench.micro
```

## Campaign phases

0. **Foundation** -- worktree off origin/main, `-O2 -g` build, harness. ✅
1. **Common tools** -- harness micro/macro suites, pytest-benchmark, cProfile
   hotspots, `/usr/bin/time` + `perf stat` macro counters.
2. **Research-grade** -- `perf record/report` HW counters (IPC, cache/branch
   miss) + flame graphs, `perf c2c` false-sharing on the lock-free
   structures, `bpftrace` latency histograms (park->wake, runqueue, futex),
   memory (stack HWM distribution, RSS, tracemalloc).
3. **Reporting + regression gate** -- dated reports, baseline diffing.
