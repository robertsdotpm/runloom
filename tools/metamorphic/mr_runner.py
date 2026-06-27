#!/usr/bin/env python3
"""mr_runner.py -- metamorphic relation runner for the big_100 / synthetic corpus.

A metamorphic relation (MR) is a property that ties the outputs of *related*
runs without needing a hand-specified expected output -- the killer feature for
a nondeterministic M:N runtime where you cannot write down "the" answer.

  * MR3 -- HUB-COUNT INVARIANCE (the primary, implemented here).
    The same program, same --seed / --funcs / --rounds, run at different --hubs
    counts, must (a) all PASS (a program that passes at --hubs 2 but hangs or
    fails at --hubs 16 is a scheduler-shape-dependent bug -- the single best fit
    for runloom's documented M:N nondeterminism), and (b) AGREE on any
    deterministic metric it emits (a conservation checksum is invariant to how
    many hubs drained it).

This catches scheduler-shape-dependent bugs that a fixed-oracle, a
differential-vs-Go, and lincheck structurally cannot. The conservation programs
already print stable lines (e.g. "produced=N consumed=N"); point --metric at one
to get value-agreement on top of the pass/fail-agreement.

Usage:
    tools/metamorphic/mr_runner.py tests/big_100/p213_select_timer_conservation.py \
        --hubs 2,4,8,16 --seed 12345 --funcs 800 --rounds 2 --duration 30 \
        --metric 'produced=(\\d+) consumed=(\\d+)'

    # sweep a whole range as a check_all phase:
    for p in tests/big_100/p2[01]*_*conservation*.py; do
        tools/metamorphic/mr_runner.py "$p" --hubs 2,8 --seed 7 --funcs 500 --rounds 1 || exit 1
    done

Exit: 0 = invariance holds (all hub counts agree); 1 = a hub count diverged
(different verdict or different metric); 2 = runner/setup error.
Wire MR3 over a deterministic subset into scripts/check_all.sh; the fuller MR set
(MR1 extra-spawn-layer, MR2 redundant-yield injection) is future work.
"""
import argparse
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PYBIN = os.environ.get(
    "RUNLOOM_PYTHON",
    os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"),
)

VERDICTS = {0: "PASS", 1: "INVARIANT", 2: "ERROR", 3: "HANG", 4: "BOXLIMIT"}


def run_once(prog, hubs, args):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [
        PYBIN, prog,
        "--hubs", str(hubs),
        "--seed", str(args.seed),
        "--funcs", str(args.funcs),
        "--rounds", str(args.rounds),
        "--duration", str(args.duration),
    ]
    # generous wall-clock cap so a real HANG (watchdog exit 3) still returns;
    # the program's own --hang-timeout fires first on a genuine wedge.
    try:
        p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True,
                           text=True, timeout=args.duration + 120)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired as e:
        return 3, "TIMEOUT (runner wall-clock): " + (e.stdout or "")[-400:]


def extract_metric(text, pattern):
    if not pattern:
        return None
    m = re.search(pattern, text)
    if not m:
        return "<no-match>"
    return tuple(m.groups()) if m.groups() else m.group(0)


def main(argv):
    ap = argparse.ArgumentParser(description="MR3 hub-count invariance runner")
    ap.add_argument("program")
    ap.add_argument("--hubs", default="2,4,8", help="comma list of hub counts")
    ap.add_argument("--seed", type=int, default=12345)
    ap.add_argument("--funcs", type=int, default=500)
    ap.add_argument("--rounds", type=int, default=1)
    ap.add_argument("--duration", type=int, default=20)
    ap.add_argument("--metric", default=None,
                    help="regex; its groups (or whole match) must agree across hub counts")
    args = ap.parse_args(argv)

    prog = args.program
    if not os.path.isfile(prog):
        cand = os.path.join(ROOT, prog)
        if os.path.isfile(cand):
            prog = cand
        else:
            print("mr_runner: no such program: {0}".format(args.program))
            return 2

    hub_counts = [int(h) for h in args.hubs.split(",") if h.strip()]
    name = os.path.basename(prog)
    print("MR3 hub-count invariance: {0}  hubs={1} seed={2} funcs={3} rounds={4}"
          .format(name, hub_counts, args.seed, args.funcs, args.rounds))

    verdicts = {}
    metrics = {}
    for h in hub_counts:
        rc, out = run_once(prog, h, args)
        verdicts[h] = rc
        metrics[h] = extract_metric(out, args.metric)
        v = VERDICTS.get(rc, "rc={0}".format(rc))
        mtxt = "" if args.metric is None else "  metric={0}".format(metrics[h])
        print("  hubs={0:<3} -> {1}{2}".format(h, v, mtxt))

    ok = True
    # (a) verdict agreement: every hub count must PASS (rc 0) or all BOXLIMIT (4).
    rcs = set(verdicts.values())
    passing = all(rc in (0, 4) for rc in verdicts.values())
    if not passing:
        ok = False
        bad = {h: VERDICTS.get(rc, rc) for h, rc in verdicts.items() if rc not in (0, 4)}
        print("\nFAIL: hub-count-dependent verdict -- {0} diverged from PASS".format(bad))
    elif len(rcs) > 1:
        print("\nNOTE: mixed PASS/BOXLIMIT across hubs (benign scale, not a fault).")

    # (b) metric agreement (only meaningful where the program emits one).
    if args.metric is not None and passing:
        vals = set(metrics.values())
        if len(vals) > 1:
            ok = False
            print("\nFAIL: metric diverged across hub counts (broken invariance): {0}"
                  .format(metrics))
        else:
            print("\nmetric agrees across all hub counts: {0}".format(next(iter(vals))))

    print("\nMR3 {0}".format("HOLDS (exit 0)" if ok else "VIOLATED (exit 1)"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
