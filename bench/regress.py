"""Regression gate: compare a benchmark run against a committed baseline.

Reads two harness JSON files (bench/results/<suite>.json) and, per bench
(matched by name), flags any that got slower by more than a tolerance. Uses
``min_s`` (best sample) by default -- it's the most noise-robust lower bound
on a shared box, so a flagged regression is much more likely real than noise.

Exit status is non-zero if any bench regressed past the tolerance, so this
can gate a local pre-merge check (we have no hosted CI -- see CLAUDE.md).

Usage:
    # compare a fresh run against the committed baseline
    PYTHONPATH=src python3 -m bench.regress \
        bench/results/micro.json bench/results/micro.new.json
    # custom metric / tolerance
    ... --metric median_s --tol 0.15
"""
import argparse
import json
import sys


def load(path):
    with open(path) as f:
        doc = json.load(f)
    return {r["name"]: r for r in doc.get("results", [])}, doc.get("env", {})


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline")
    p.add_argument("current")
    p.add_argument("--metric", default="min_s",
                   help="stat to compare (min_s best, or median_s)")
    p.add_argument("--tol", type=float, default=0.10,
                   help="fractional slowdown tolerated before failing (0.10=10%%)")
    args = p.parse_args(argv)

    base, base_env = load(args.baseline)
    cur, cur_env = load(args.current)

    # A different build/runtime invalidates the comparison -- warn loudly.
    for k in ("runloom_backend", "python", "gil_enabled"):
        if base_env.get(k) != cur_env.get(k):
            print("WARNING: env mismatch %s: baseline=%r current=%r"
                  % (k, base_env.get(k), cur_env.get(k)))

    print("%-34s %12s %12s %9s" % ("bench", "baseline", "current", "delta"))
    regressed = []
    improved = []
    for name, c in cur.items():
        b = base.get(name)
        if b is None:
            print("%-34s %12s %12s %9s" % (name, "-", "(new)", ""))
            continue
        bv, cv = b[args.metric], c[args.metric]
        delta = (cv - bv) / bv if bv else 0.0
        flag = ""
        if delta > args.tol:
            flag = "  REGRESS"
            regressed.append((name, delta))
        elif delta < -args.tol:
            flag = "  faster"
            improved.append((name, delta))
        print("%-34s %12.6f %12.6f %+8.1f%%%s"
              % (name, bv, cv, delta * 100, flag))

    print()
    for name, d in improved:
        print("  improved: %s %+.1f%%" % (name, d * 100))
    if regressed:
        print("\nFAIL: %d bench(es) regressed > %.0f%% on %s:"
              % (len(regressed), args.tol * 100, args.metric))
        for name, d in regressed:
            print("  %s  %+.1f%%" % (name, d * 100))
        return 1
    print("\nOK: no regression > %.0f%% on %s" % (args.tol * 100, args.metric))
    return 0


if __name__ == "__main__":
    sys.exit(main())
