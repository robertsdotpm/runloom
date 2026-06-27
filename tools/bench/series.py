#!/usr/bin/env python3
"""series.py -- longitudinal benchmark series + change-point regression detection.

rigor.py (Kalibera&Jones) gives a rigorous TWO-run A/B comparison, but nothing
tracks a workload across COMMITS, so a slow regression that creeps in over many
small commits -- each individually within noise -- is invisible.  This appends
every rigor.py result to a committed series and flags the commit where a
SUSTAINED shift began (a change point), satisfying the no-hosted-CI rule (the
series is just a JSON-lines file in the repo).

  series.py add --workload spawn_per_s --sha $(git rev-parse --short HEAD) \
      --commit-time 1719500000 --values "812000,805000,818000,809000"
  series.py detect --workload spawn_per_s          # flag the regressing commit
  series.py list --workload spawn_per_s

Detection: a single-change-point scan -- for every split of the per-commit medians
into a prefix/suffix, compute a Welch t-statistic of the two means; the split with
the largest |t| that ALSO exceeds --min-effect (relative shift) and --t-thresh is
reported as the change point, attributed to the first commit of the suffix.
Direction is reported (regression vs improvement) per --higher-is-better.  Pure
stdlib (no numpy) to match the house style.

Exit (detect): 0 = no regression change-point; 1 = a regression change-point
found; 2 = not enough data / error.
"""
import argparse
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STORE = os.path.join(HERE, "series.jsonl")


def load(workload=None):
    rows = []
    if os.path.isfile(STORE):
        with open(STORE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if workload is None or r.get("workload") == workload:
                    rows.append(r)
    rows.sort(key=lambda r: (r.get("commit_time", 0), r.get("sha", "")))
    return rows


def median(xs):
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def mean_var(xs):
    n = len(xs)
    if n == 0:
        return 0.0, 0.0
    m = sum(xs) / n
    v = sum((x - m) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
    return m, v


def cmd_add(args):
    vals = [float(x) for x in args.values.replace(" ", "").split(",") if x]
    if not vals:
        print("series add: no --values"); return 2
    rec = {"workload": args.workload, "sha": args.sha,
           "commit_time": args.commit_time, "points": vals,
           "median": median(vals)}
    with open(STORE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
    print("series add: {0} {1} median={2:.4g} (n={3})".format(
        args.workload, args.sha, rec["median"], len(vals)))
    return 0


def cmd_list(args):
    rows = load(args.workload)
    for r in rows:
        print("  {0:<10} {1:<14} median={2:.6g} n={3}".format(
            r.get("sha", "?"), r.get("workload", "?"), r.get("median", 0),
            len(r.get("points", []))))
    print("{0} records".format(len(rows)))
    return 0


def detect(rows, t_thresh, min_effect):
    """Single best change-point by max |Welch-t| over prefix/suffix of medians.
    Returns (idx, t, rel_shift, pre_mean, post_mean) or None."""
    meds = [r["median"] for r in rows]
    n = len(meds)
    if n < 6:
        return None
    best = None
    for k in range(3, n - 2):          # need >=3 each side
        a, b = meds[:k], meds[k:]
        ma, va = mean_var(a)
        mb, vb = mean_var(b)
        se = math.sqrt(va / len(a) + vb / len(b))
        if se == 0:
            continue
        t = (mb - ma) / se
        rel = (mb - ma) / ma if ma else 0.0
        if abs(t) >= t_thresh and abs(rel) >= min_effect:
            if best is None or abs(t) > abs(best[1]):
                best = (k, t, rel, ma, mb)
    return best


def cmd_detect(args):
    rows = load(args.workload)
    if len(rows) < 6:
        print("series detect: need >=6 commits for {0} (have {1})".format(
            args.workload, len(rows)))
        return 2
    cp = detect(rows, args.t_thresh, args.min_effect)
    if not cp:
        print("series detect [{0}]: no change-point (stable across {1} commits)".format(
            args.workload, len(rows)))
        return 0
    k, t, rel, pre, post = cp
    culprit = rows[k]
    higher_better = args.higher_is_better
    improved = (rel > 0) == higher_better
    kind = "IMPROVEMENT" if improved else "REGRESSION"
    print("series detect [{0}]: {1} at commit {2} (t={3:.2f}, shift {4:+.1%})"
          .format(args.workload, kind, culprit.get("sha", "?"), t, rel))
    print("  before mean={0:.6g}  after mean={1:.6g}  (higher_is_better={2})"
          .format(pre, post, higher_better))
    return 1 if (kind == "REGRESSION") else 0


def main(argv):
    ap = argparse.ArgumentParser(description="benchmark series + change-point detect")
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add")
    a.add_argument("--workload", required=True)
    a.add_argument("--sha", required=True)
    a.add_argument("--commit-time", type=int, default=0)
    a.add_argument("--values", required=True, help="comma list of measured points")
    a.set_defaults(fn=cmd_add)
    l = sub.add_parser("list")
    l.add_argument("--workload", default=None)
    l.set_defaults(fn=cmd_list)
    d = sub.add_parser("detect")
    d.add_argument("--workload", required=True)
    d.add_argument("--t-thresh", type=float, default=3.0)
    d.add_argument("--min-effect", type=float, default=0.05, help="min relative shift")
    d.add_argument("--higher-is-better", action="store_true",
                   help="set for throughput (spawn/s, ops/s); leave off for latency")
    d.set_defaults(fn=cmd_detect)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
