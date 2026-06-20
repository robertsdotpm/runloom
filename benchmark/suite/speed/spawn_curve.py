#!/usr/bin/env python3
"""Spawn rate vs N -- spawn/s for every runtime as N climbs 1k -> 1M.

No bounding, no tricks: spawn N tasks, drain, measure raw N/seconds.  All
runtimes front-load the same way (Go's bench does too).  Each runtime runs at its
native config: runloom + go on HUBS cores (GIL off), asyncio/uvloop/greenlet on 1
core (GIL on) -- same as the cross-runtime report.  Writes results/spawn_curve.json
for the report curve.  RUNLOOM_SYSMON/PREEMPT off (microbenchmark watchdog noise).

TRADE-OFF / TARGET (full diagnosis: docs/dev/spawn_cost.md).  This is NAKED spawn
-- the WORST case: create+run+destroy with no I/O to amortize over.  runloom is
~48x behind Go HERE and only here; req/s does NOT have this gap (it spawns one
handler per *connection* at setup, then loops -- spawn is ~0% of the timed
window), and conn/s matches Go's conn/s (at ~4x CPU; TCP syscalls dominate).  The
gap is per-fiber stack mmap+mprotect during the spawn burst (runloom_coro_new ->
runloom_stack_map_guarded), because RUNLOOM_STACK_ARENA holds ONE stack-size class
and falls back to per-stack mmap on any other size.  TARGET: productionize the
arena -- per-size-class arenas + unguarded pure-Python slots + a bounded resident
pool (no per-completion madvise).  Do NOT read this number as "runloom is 48x
slower"; read the metrics legend at the top of the report.
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
