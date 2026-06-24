#!/usr/bin/env python3
"""Spawn rate vs N -- spawn/s for every runtime as N climbs 1k -> 1M.

No bounding, no tricks: spawn N tasks, drain, measure N/seconds.  All runtimes
front-load the same way (Go's bench does too).  Each runtime runs at its native
config: runloom + go on HUBS cores (GIL off), asyncio/uvloop/greenlet on 1 core
(GIL on) -- same as the cross-runtime report.  Writes results/spawn_curve.json
for the report curve.  RUNLOOM_SYSMON/PREEMPT off (microbenchmark watchdog noise).

WARM steady-state, boot excluded for ALL runtimes.  The runloom programs are
invoked with --warm WARM: they run WARM extra in-process passes and report the
best timed pass, so the one-time runloom.run() scheduler boot is NOT in the timed
window.  Go and the GIL loops are already warm at main() (their runtime/loop boots
before they start their own timer), so no flag is needed for them -- this is the
SAME basis, not a runloom-specific trick.  (Without --warm, runloom would carry a
~39 ms scheduler boot inside the timed window that Go never pays in its window --
a startup race, not a per-spawn comparison.)

This is NAKED spawn -- the WORST case: create+run+destroy with no I/O to amortize
over.  Warm on this box at 1M, c_entry and Go are AT PARITY (~2.2M each, 8-run
medians 2.23M vs 2.24M, ranges overlapping, ranking flips run-to-run); fiber_fast
~1.9M (~0.85x Go).  The rate still climbs with N for every runtime -- a per-run
fixed cost (front-load loop + drain) amortizing; runloom's residual (~19 ms) is
larger than Go's (~5 ms), so its small-N rates sag more.  The per-spawn SLOPE is
what matters: warm, runloom's marginal cost per fiber (~440 ns) is within noise of
Go's (~410 ns).

Two runloom spawn entries are reported:
  runloom_py -- runloom.fiber_fast: the fair apples-to-apples vs Go's `go f()`
                (a thin Python spawn, no per-spawn work).
  runloom_c  -- the pure-C c_entry path (no Python frame): the scheduler ceiling.
NOTE: the DEFAULT runloom.fiber adds the grow-down auto-sizer (small right-sized
stacks, an RSS feature Go lacks).  Its learned size spawns down the DEFERRED
stack-alloc path, so the default is ~1.34M/s warm (small-stacks AND fast), not the
old ~7x-slower eager-alloc number; optimize("throughput")/("memory") swaps
runloom.fiber between fiber_fast and grow-down.
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
import config

HERE = os.path.dirname(os.path.abspath(__file__))
FT = config.FT_PYTHON
GIL = config.GIL_PYTHON
GO = os.path.join(HERE, "speed_go")
HUBS = 8
MANY = "16-%d" % (16 + HUBS - 1)   # the 8 server cores
ONE = "16"                          # single-core runtimes
NS = [1000, 3000, 10000, 30000, 100000, 300000, 1000000]
REPS = 2
WARM = 4   # extra in-process passes for the runloom programs (scheduler boot excluded)

# label + how to invoke spawn for each runtime
RTS = [
    ("runloom_py", "Runloom (M:N) — Python fiber"),
    ("runloom_c",  "Runloom (M:N) — pure C fiber (c_entry)"),
    ("go",         "Go (GOMAXPROCS=%d)" % HUBS),
    ("asyncio",    "asyncio (1 core)"),
    ("uvloop",     "uvloop (1 core)"),
    ("greenlet",   "greenlet (1 core)"),
]


def spec(rt, n):
    """-> (argv, cpus, gil_off)"""
    if rt == "go":
        return [GO, "-metric", "spawn", "-n", str(n), "-gomaxprocs", str(HUBS)], MANY, True
    if rt == "runloom_py":
        return [FT, os.path.join(HERE, "runloom_epoll_py_fiber.py"), "--metric", "spawn",
                "--n", str(n), "--hubs", str(HUBS), "--warm", str(WARM)], MANY, True
    if rt == "runloom_c":
        return [FT, os.path.join(HERE, "run_centry.py"), "--metric", "spawn",
                "--n", str(n), "--hubs", str(HUBS), "--warm", str(WARM)], MANY, True
    if rt in ("asyncio", "uvloop"):
        return [GIL, os.path.join(HERE, "speed_asyncio.py"), "--metric", "spawn",
                "--loop", rt, "--n", str(n)], ONE, False
    if rt == "greenlet":
        return [GIL, os.path.join(HERE, "greenlet_native_py_coro.py"), "--metric", "spawn",
                "--n", str(n)], ONE, False
    raise ValueError(rt)


def _run(argv, cpus, gil_off):
    env = dict(os.environ, PYTHONPATH=os.path.join(config.REPO, "src"),
               PYTHON_GIL="0" if gil_off else "1",
               RUNLOOM_SYSMON="0", RUNLOOM_PREEMPT="0")
    out = subprocess.run(["taskset", "-c", cpus] + argv,
                         capture_output=True, text=True, env=env, timeout=300).stdout
    for line in reversed(out.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("no json")


def rate(rt, n):
    argv, cpus, gil = spec(rt, n)
    best = 0.0
    for _ in range(REPS):
        best = max(best, n / _run(argv, cpus, gil)["seconds"])
    return best


def main():
    res = {rt: {} for rt, _ in RTS}
    labels = {rt: lab for rt, lab in RTS}
    print("hubs=%d  reps=%d (best)  raw spawn/s = N / whole-run-seconds\n" % (HUBS, REPS))
    print("%-10s" % "N" + "".join("%14s" % rt for rt, _ in RTS))
    for n in NS:
        row = "%-10d" % n
        for rt, _ in RTS:
            try:
                r = rate(rt, n)
                res[rt][n] = r
                row += "%14.0f" % r
            except Exception:
                res[rt][n] = None
                row += "%14s" % "ERR"
        print(row, flush=True)
    out = os.path.join(config.RESULTS_DIR, "spawn_curve.json")
    json.dump({"hubs": HUBS, "NS": NS, "reps": REPS, "labels": labels, "rates": res},
              open(out, "w"), indent=2)
    print("\nwrote", out)


if __name__ == "__main__":
    main()
