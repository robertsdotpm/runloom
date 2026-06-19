# Runloom benchmark suite

Throughput / speed / memory benchmarks comparing runloom against Go, asyncio,
uvloop, gevent and raw greenlet. Produces a single consolidated
[`../report.html`](../report.html) and curated README sections
([`../README_SECTIONS.md`](../README_SECTIONS.md)).

The original spec and every scoping decision are archived verbatim in
[`../prompt/original_spec.md`](../prompt/original_spec.md).

## What it measures

- **Performance** (`run_perf.py`) — req/s (1 KiB) and bandwidth (1.5 MiB) for 9
  server configs: 5 runloom tiers (sync wrappers / C scaffold / io_uring /
  io_uring+Cython / +optimize(throughput)) plus asyncio, uvloop, gevent and Go.
  A Go closed-loop loadgen walks a connection ladder until req/s plateaus.
- **Speed** (`run_speed.py`) — spawn 1M tasks, context switch, HTTP req/s vs a Go
  server, and TCP round-trip latency, for [runloom, go, asyncio, greenlet, uvloop].
- **Memory** (`run_mem.py`) — used RSS (not virtual) per idle fiber and at 1M
  fibers, for [go, runloom py handler, runloom py+optimize(memory), runloom c handler].

## Prerequisites

- Free-threaded CPython 3.13t with the runloom C extension built
  (`python setup.py build_ext --inplace` from the repo root) and Cython 3.x.
- The GIL build of 3.13 with `uvloop` + `gevent` (the single-threaded baselines
  run there — their best case).
- `go`, passwordless `sudo` (for `ip netns` + `prlimit`), `taskset`, `liburing`.
- Build the native pieces once:

      python servers/build_cy.py build_ext --inplace      # Cython handler (+ disasm_check.sh proves zero-PyObject)
      (cd clients && go build -o loadgen loadgen.go)
      (cd servers && go build -o srv_go srv_go.go)
      (cd speed   && go build -o speed_go speed_go.go)
      (cd memory  && go build -o mem_go mem_go.go)

## Run

    PYTHONPATH=../../src python harness/...   # not needed; the orchestrators set it
    python run_all.py            # full suite -> results/{perf,speed,mem,env}.json
    python run_all.py --quick    # fast smoke (reduced N / ladders)

Then build the outputs:

    python gen_report.py         # -> ../report.html
    python gen_readme.py         # -> ../README_SECTIONS.md

Individual phases: `run_perf.py`, `run_speed.py`, `run_mem.py` (each `--quick`,
`--only`, `--metric`).

## Why the methodology looks the way it does

- **veth across two netns, disjoint NUMA pinning** — a fresh netns has an empty
  firewall ruleset (no ~14% loopback nft tax) and keeps the loadgen from stealing
  server cores; the veth path doesn't hide io_uring the way `lo` does.
- **Per-core = saturated M:N throughput / hub count**, never `run(1)` — that is the
  M:1 cooperative scheduler, a different runtime than the M:N work-stealer.
- **The 16-core client can't saturate the fastest servers** for a symmetric echo,
  so each peak records the CPU-bound side and a server-ceiling estimate.
- **Zero-PyObject Cython handler** calls runloom's cooperative recv/send as plain C
  via the `runloom_c.__tcp_capi__` capsule; `servers/disasm_check.sh` objdumps the
  hot loop to prove it.

See `harness/config.py` for the exact numbers and `gen_report.py` for the full
"assumed constraints" section.
