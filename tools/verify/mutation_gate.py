#!/usr/bin/env python3
"""mutation_gate.py -- ratcheting mutation-score floor for the formal models (item 14).

model_mutate.py already exits 1 on ANY survivor (a 100% floor).  That is too
strict to gate on when a model legitimately has a known, audited survivor (an
element the property provably need not constrain).  This gate makes the floor a
RATCHET instead:

  * a committed baseline (mutation_baseline.json) records the accepted survivor
    set + min score per target;
  * the gate FAILS only on a REGRESSION -- a NEW survivor, or a score below the
    baseline floor -- so teeth can never silently erode;
  * --accept ratchets the baseline UP to the current (better-or-equal) result,
    never down (a regression must be fixed or explicitly re-accepted).

Run periodically (weekly lane), not per-commit -- the sweep is minutes.

Usage:
  mutation_gate.py [--full] [--target T]     # gate against the baseline
  mutation_gate.py --accept [--full]         # ratchet the baseline to current

House style: %/.format, prints kept.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
BASELINE = os.path.join(HERE, "mutation_baseline.json")
MUTATE = os.path.join(HERE, "model_mutate.py")


def run_mutate(full, target, timeout):
    fd, path = tempfile.mkstemp(suffix=".json"); os.close(fd)
    argv = [sys.executable, MUTATE, "--json", path]
    if target:
        argv += ["--target", target]
    elif full:
        argv.append("--full")
    if timeout:
        argv += ["--timeout", str(timeout)]
    # model_mutate exits 1 on survivors -- expected; we read the JSON regardless.
    subprocess.run(argv, cwd=os.path.dirname(os.path.dirname(HERE)))
    try:
        with open(path) as f:
            return json.load(f)
    finally:
        os.unlink(path)


def load_baseline():
    if os.path.exists(BASELINE):
        return json.load(open(BASELINE))
    return {}


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--target", default=None)
    ap.add_argument("--timeout", type=int, default=None)
    ap.add_argument("--accept", action="store_true",
                    help="ratchet the baseline UP to the current result")
    a = ap.parse_args(argv)

    cur = run_mutate(a.full, a.target, a.timeout)
    base = load_baseline()

    if a.accept:
        merged = dict(base)
        for name, r in cur.items():
            prev = base.get(name)
            # ratchet: accept only if score >= previous floor (never regress the floor)
            floor = max(r["score"], prev["floor"]) if prev else r["score"]
            merged[name] = {"floor": floor, "survivors": sorted(r["survivors"])}
        json.dump(merged, open(BASELINE, "w"), indent=1, sort_keys=True)
        print("ratcheted baseline for %d target(s) -> %s" % (len(cur), BASELINE))
        return 0

    regressions = []
    for name, r in cur.items():
        prev = base.get(name)
        cur_surv = set(r["survivors"])
        if prev is None:
            # a target with no baseline yet: any survivor is a regression, and a
            # clean target should be recorded (run --accept once to seed).
            if cur_surv:
                regressions.append("%s: NEW target with %d survivor(s): %s"
                                   % (name, len(cur_surv), ", ".join(sorted(cur_surv))))
            continue
        new_surv = cur_surv - set(prev.get("survivors", []))
        if new_surv:
            regressions.append("%s: NEW survivor(s) vs baseline: %s"
                               % (name, ", ".join(sorted(new_surv))))
        if r["score"] + 1e-9 < prev["floor"]:
            regressions.append("%s: score %.0f%% below floor %.0f%%"
                               % (name, r["score"], prev["floor"]))

    if regressions:
        print("\nmutation_gate FAIL -- teeth regressed:")
        for x in regressions:
            print("  " + x)
        print("fix the model/property, or re-run with --accept if the new "
              "survivor is audited and legitimate.")
        return 1
    print("\nmutation_gate OK: no target regressed below its baseline floor.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
