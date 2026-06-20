#!/usr/bin/env python3
"""Capstone sweep -> results/centry_capstone.json.

ctxswitch ns/switch vs hub count for the tstate-free c_entry fiber (pure
scheduler, no Python eval; run_centry.py) vs the Python fiber (speed_runloom.py),
each pinned to `hubs` cores, n=0-subtracted, median of REPS. Proves runloom's
scheduler yield is ~20 ns and flat while the Python-fiber path explodes -> the
ctxswitch wall is CPython, not runloom. Rendered in report.html's speed section.
"""
import json
import os
import statistics
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "harness"))
import config

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = config.REPO
PY = config.FT_PYTHON
RC = os.path.join(HERE, "run_centry.py")
SR = os.path.join(HERE, "speed_runloom.py")
HUBS = [1, 8, 16, 44]
N = 500_000
REPS = 2
SRV0 = config.SERVER_CPUS[0]


def _run(script, hubs, n, extra=()):
    cpus = "%d-%d" % (SRV0, SRV0 + hubs - 1)
    # RUNLOOM_PREEMPT=0: the ATTACHED/CPU-preemption watchdog (default ON) fires
    # on a pure-CPU yield-loop microbenchmark (the hub stays in g-context past
    # the 50ms wedge budget) and force-enables sysmon -- pure noise here, and it
    # hits Python fibers (tstate-ATTACHED) but NOT nogil c_entry, which would
    # make the python-vs-c_entry comparison unfair.  A real I/O-bound server
    # returns to hub_main on every park and never trips it, so disabling it
    # measures the representative cooperative-yield cost.  RUNLOOM_SYSMON=0 alone
    # can't do this (preempt re-enables sysmon).
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH=os.path.join(REPO, "src"),
               RUNLOOM_SYSMON="0", RUNLOOM_PREEMPT="0")
    cmd = ["taskset", "-c", cpus, PY, script, "--metric", "ctxswitch",
           "--n", str(n), "--hubs", str(hubs), *extra]
    out = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=180).stdout
    for line in reversed(out.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("no JSON from %s @ %d hubs:\n%s" % (script, hubs, out[-400:]))


def ns(script, hubs, extra=()):
    vals = []
    for _ in range(REPS):
        b = _run(script, hubs, 0, extra)
        f = _run(script, hubs, N, extra)
        vals.append(max(f["seconds"] - b["seconds"], 1e-9) * 1e9 / f["switches"])
    return statistics.median(vals)


def main():
    # Three lines:
    #   c_entry_ns         -- the C handler (tstate-free c_entry): the ceiling.
    #   python_ns          -- Python handler, ONE shared handler fn for every
    #                         fiber (the naive server): yield object is now the
    #                         immortal singleton, but the SHARED code object's
    #                         adaptive-cache writes still contend at high hubs.
    #   python_distinct_ns -- Python handler, per-fiber code object (the fixed
    #                         path): both contention layers gone -> should scale.
    res = {"hubs": HUBS, "n": N, "reps": REPS,
           "c_entry_ns": [], "python_ns": [], "python_distinct_ns": []}
    for h in HUBS:
        ce = ns(RC, h)
        py = ns(SR, h)
        pyd = ns(SR, h, extra=("--distinct",))
        res["c_entry_ns"].append(ce)
        res["python_ns"].append(py)
        res["python_distinct_ns"].append(pyd)
        print("  hubs=%-3d  c_entry=%8.0f ns   python(shared)=%10.0f ns   "
              "python(distinct)=%10.0f ns" % (h, ce, py, pyd), flush=True)
    out = os.path.join(config.RESULTS_DIR, "centry_capstone.json")
    with open(out, "w") as f:
        json.dump(res, f, indent=2)
    print("wrote", out)


if __name__ == "__main__":
    main()
