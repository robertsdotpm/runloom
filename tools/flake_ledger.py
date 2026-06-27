#!/usr/bin/env python3
"""flake_ledger.py -- turn "is this a real race or sampling variance?" into DATA.

runloom is M:N (not asyncio-deterministic), so an order-/scheduling-dependent test
result is a recurring judgment call ("round-1's failure did NOT recur -- sampling
variance").  This makes it measurable: run each test FILE K times via the
isolated runner, classify {stable-pass, stable-fail, FLAKY}, and persist a
per-file flip-rate ledger.  A flip-rate INCREASE on a previously-stable test --
or a brand-new flake -- is itself a regression signal (a new rare race), surfaced
the next run instead of being argued about.

  flake_ledger.py test_smoke.py test_chan.py --runs 7
  flake_ledger.py --all --runs 5            # every tests/test_*.py (slow)
  flake_ledger.py --shuffle test_a.py test_b.py --runs 5   # also probe cross-file order

Ledger: tools/flake_ledger.json (commit it -- the history is the point).
Exit: 0 = nothing flaky and no regression vs the ledger; 1 = a flaky/newly-flaky
test; 2 = runner error.  (DeFlaker overlay -- correlate a new failure with the
gcov-diff of changed C lines to separate flake from real regression -- is a
follow-up; this is the flip-rate substrate it builds on.)
"""
import argparse
import json
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RUN_ISOLATED = os.path.join(ROOT, "tests", "run_isolated.py")
PYBIN = os.environ.get("RUNLOOM_PYTHON",
                       os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"))
LEDGER = os.path.join(HERE, "flake_ledger.json")


def run_once(files, timeout):
    """Run the isolated runner over `files`; return True iff exit 0 (all passed)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [PYBIN, RUN_ISOLATED] + list(files)
    try:
        p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True,
                           text=True, timeout=timeout)
        return p.returncode == 0
    except subprocess.TimeoutExpired:
        return False


def classify(passes, runs):
    if passes == runs:
        return "stable-pass"
    if passes == 0:
        return "stable-fail"
    return "FLAKY"


def load_ledger():
    if os.path.isfile(LEDGER):
        try:
            return json.load(open(LEDGER))
        except ValueError:
            pass
    return {}


def main(argv):
    ap = argparse.ArgumentParser(description="flaky-test classifier + flip-rate ledger")
    ap.add_argument("files", nargs="*", help="test_*.py files (under tests/)")
    ap.add_argument("--all", action="store_true", help="every tests/test_*.py")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--shuffle", action="store_true",
                    help="run the files together in randomized order each pass "
                         "(probes cross-file order dependence, not just per-file flakiness)")
    args = ap.parse_args(argv)

    tests_dir = os.path.join(ROOT, "tests")
    if args.all:
        files = sorted(f for f in os.listdir(tests_dir)
                       if f.startswith("test_") and f.endswith(".py"))
    else:
        files = [os.path.basename(f) for f in args.files]
    if not files:
        print("flake_ledger: no test files (give some or --all)"); return 2

    ledger = load_ledger()
    results = {f: 0 for f in files}     # pass counts

    if args.shuffle:
        # cross-file: run ALL files together, randomized order, K times; a file's
        # pass/fail is the whole-batch verdict (coarse but catches order coupling).
        rng = random.Random(1234)
        for k in range(args.runs):
            order = files[:]
            rng.shuffle(order)
            ok = run_once(order, args.timeout * 2)
            for f in files:
                results[f] += 1 if ok else 0
            print("  shuffle pass {0}/{1}: {2}".format(k + 1, args.runs, "OK" if ok else "FAIL"))
    else:
        for f in files:
            for k in range(args.runs):
                ok = run_once([f], args.timeout)
                results[f] += 1 if ok else 0
            cls = classify(results[f], args.runs)
            print("  {0:<44} {1}/{2}  {3}".format(f, results[f], args.runs, cls))

    any_flaky = False
    regressions = []
    for f in files:
        passes = results[f]
        cls = classify(passes, args.runs)
        flip = min(passes, args.runs - passes) / float(args.runs)   # 0 = stable
        prev = ledger.get(f, {})
        prev_cls = prev.get("classification")
        # accumulate history
        ent = {
            "classification": cls,
            "last_passes": passes, "last_runs": args.runs,
            "last_flip_rate": round(flip, 3),
            "runs_total": prev.get("runs_total", 0) + args.runs,
            "passes_total": prev.get("passes_total", 0) + passes,
            "prev_classification": prev_cls,
        }
        ledger[f] = ent
        if cls == "FLAKY":
            any_flaky = True
        if prev_cls == "stable-pass" and cls != "stable-pass":
            regressions.append((f, prev_cls, cls))

    json.dump(ledger, open(LEDGER, "w"), indent=1, sort_keys=True)

    print("\nflake_ledger: {0} files, {1} flaky".format(
        len(files), sum(1 for f in files if ledger[f]["classification"] == "FLAKY")))
    if regressions:
        print("REGRESSIONS vs ledger (was stable-pass, now not -- likely a NEW rare race):")
        for f, a, b in regressions:
            print("  {0}: {1} -> {2}".format(f, a, b))
    print("ledger: {0}".format(LEDGER))
    return 1 if (any_flaky or regressions) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
