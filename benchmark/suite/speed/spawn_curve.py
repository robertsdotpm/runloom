#!/usr/bin/env python3
"""Spawn rate vs N -- spawn/s for every runtime as N climbs 1k -> 1M.

No bounding, no tricks: spawn N tasks, drain, measure raw N/seconds.  All
runtimes front-load the same way (Go's bench does too).  Each runtime runs at its
native config: runloom + go on HUBS cores (GIL off), asyncio/uvloop/greenlet on 1
core (GIL on) -- same as the cross-runtime report.  Writes results/spawn_curve.json
for the report curve.  RUNLOOM_SYSMON/PREEMPT off (microbenchmark watchdog noise).

This is NAKED spawn -- the WORST case: create+run+destroy with no I/O to amortize
over.  runloom MATCHES Go here: the user-facing Python spawn (runloom.fiber_fast)
hits ~2.0M/s vs Go's ~2.1M, and the pure-C c_entry path beats Go warm
(~2.2-2.46M).

Two runloom spawn entries are reported:
  runloom_py -- runloom.fiber_fast: the fair apples-to-apples vs Go's `go f()`
                (a thin Python spawn, no per-spawn work).  ~Go.
  runloom_c  -- the pure-C c_entry path (no Python frame): the scheduler ceiling.
NOTE: the DEFAULT runloom.fiber adds the grow-down auto-sizer (a per-spawn
stack-HWM learn that trades ~7x spawn speed for small resident stacks -- an RSS
feature Go lacks), so it is a separate RSS-vs-speed number, not the
spawn-speed-vs-Go comparison measured here.  Each rep is a fresh process, so this
is the COLD first-burst; warm steady-state is higher.
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
                "--n", str(n), "--hubs", str(HUBS)], MANY, True
    if rt == "runloom_c":
        return [FT, os.path.join(HERE, "run_centry.py"), "--metric", "spawn",
                "--n", str(n), "--hubs", str(HUBS)], MANY, True
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
