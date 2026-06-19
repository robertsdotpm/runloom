#!/usr/bin/env python3
"""Run the whole benchmark suite (perf + speed + memory), full mode, in order,
and capture an environment snapshot.  Each sub-orchestrator writes its own JSON
into benchmark/results/; gen_report.py turns them into the consolidated HTML.

Usage: python3 run_all.py [--quick]
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import env

HERE = os.path.dirname(os.path.abspath(__file__))
PY = config.FT_PYTHON


def phase(name, argv):
    print("\n" + "=" * 70, flush=True)
    print("== PHASE: %s ==" % name, flush=True)
    print("=" * 70, flush=True)
    t0 = time.time()
    rc = subprocess.run([PY, os.path.join(HERE, argv[0])] + argv[1:]).returncode
    print("== %s done in %.0fs (rc=%d) ==" % (name, time.time() - t0, rc), flush=True)
    return rc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()
    q = ["--quick"] if args.quick else []

    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    # environment snapshot up front
    info = env.capture()
    with open(os.path.join(config.RESULTS_DIR, "env.json"), "w") as f:
        json.dump(info, f, indent=2)
    print("\n".join(env.header_lines(info)), flush=True)

    phase("performance (req/s + bandwidth)", ["run_perf.py"] + q)
    phase("speed (spawn/ctxswitch/rtt/http)", ["run_speed.py"] + q)
    phase("memory (RSS per fiber + 1M)", ["run_mem.py"] + q)
    print("\nALL PHASES COMPLETE. results in", config.RESULTS_DIR, flush=True)


if __name__ == "__main__":
    main()
