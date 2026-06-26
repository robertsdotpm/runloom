#!/usr/bin/env python3
"""chess_compose.py -- INPUT x SCHEDULE composition over the systematic explorer.

A concurrency bug needs the right INPUT and the right INTERLEAVING.  Fuzzers vary
the input and run one (random) schedule; the schedule explorer (chess_explore.py)
fixes the input and exhausts the <=c-preemption schedule space.  Composing them
turns bug-finding into  input-coverage x schedule-coverage, both measured: for
each input, run the exhaustive context-bounded schedule enumeration, and report
the matrix -- which (input, found-at-c) cells produced a bug, and the exact
replayable (input, schedule) for each.

This is the principled version of the seed-hunt: instead of "spray random
inputs x one random schedule and hope", you state "for every input in this set,
the <=c-preemption schedule space is exhausted (or PCT-quantified beyond)".

SCOPE: same v1 closed-world fence as chess_explore (the schedule must be a pure
function of the prefix -- CPU + chan/lock/sync + logical-clock sched_sleep).  An
OFFLOADING workload (e.g. fuzz_capi) is OUT: its schedule isn't replayable, so
the NON_REPLAYABLE guard fires and that input's coverage claim is withdrawn
rather than silently wrong.

House style: .format(), no f-strings.
Usage:
  chess_compose.py --inputs VAR=a,b,c [--cmax C] [--workload P] [other ENV=V ...]
"""
import argparse
import itertools
import os
import sys

# reuse the explorer's run/enumerate machinery
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chess_explore as ce

ROOT = ce.ROOT


def explore_one(workload, cmax, timeout, env):
    """Run the full context-bounded enumeration for ONE input (env). Returns
    (found_c, bug_prefix, n_schedules, exhausted_at_cmax, replay_ok)."""
    results = {}
    cov = {"points": {}, "covered": set()}
    found_c = None
    bug = None
    final_pruned = 0
    for c in range(0, cmax + 1):
        runs = {"n": 0}
        pruned, _md, _mf = ce.explore(workload, c, timeout, env, results, runs, cov)
        final_pruned = pruned
        if found_c is None:
            for hubseq, (outcome, last, prefix) in results.items():
                if outcome == "BUG":
                    found_c, bug = c, (prefix, last, hubseq)
                    break
    replay_ok = True
    if bug is not None:
        stable, _outs, _n = ce.replay_check(workload, bug[0], timeout, env, 6)
        replay_ok = stable
    return found_c, bug, len(results), (final_pruned == 0), replay_ok


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--workload", default=ce.DEFAULT_WORKLOAD)
    p.add_argument("--cmax", type=int, default=2)
    p.add_argument("--timeout", type=float, default=25.0)
    p.add_argument("--inputs", action="append", default=[], metavar="VAR=a,b,c",
                   help="an input axis: the workload env VAR ranges over the "
                        "comma-separated values. Repeat for a multi-axis product.")
    p.add_argument("env", nargs="*", help="fixed extra ENV=VALUE for every run")
    a = p.parse_args(argv)

    fixed = {}
    for kv in a.env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            fixed[k] = v

    # build the input product (input-coverage axis)
    axes = []
    for spec in a.inputs:
        var, vals = spec.split("=", 1)
        axes.append([(var, v) for v in vals.split(",")])
    if not axes:
        axes = [[("_", "_")]]            # single trivial input
    combos = list(itertools.product(*axes))

    print("CHESS input x schedule composition")
    print("  workload: {}".format(os.path.relpath(a.workload, ROOT)))
    print("  inputs: {} ({} combinations)  x  schedules: exhaustive c<=0..{}".format(
        a.inputs or "(none)", len(combos), a.cmax))
    print("-" * 76)
    print("  {:<26}  {:>9}  {:>9}  {:>9}  {}".format(
        "input", "schedules", "found@c", "exhausted", "result"))

    bugs = []
    for combo in combos:
        env = dict(fixed)
        label_parts = []
        for var, val in combo:
            if var != "_":
                env[var] = val
                label_parts.append("{}={}".format(var, val))
        label = ",".join(label_parts) if label_parts else "(default)"
        found_c, bug, nsched, exhausted, replay_ok = explore_one(
            a.workload, a.cmax, a.timeout, env)
        if bug is not None and not replay_ok:
            res = "NON_REPLAYABLE (input out of closed-world scope)"
        elif bug is not None:
            res = "BUG  {}".format(bug[1])
            bugs.append((label, found_c, bug[0], list(bug[2])))
        else:
            res = "clean"
        print("  {:<26}  {:>9}  {:>9}  {:>9}  {}".format(
            label, nsched, found_c if found_c is not None else "-",
            "yes" if exhausted else "no", res))

    print("-" * 76)
    print("{} of {} inputs produced a bug.".format(len(bugs), len(combos)))
    for label, c, prefix, hubseq in bugs:
        print("  input [{}] -> bug at c={}: schedule={} hubs={}".format(
            label, c, prefix, hubseq))
    return 1 if bugs else 0


if __name__ == "__main__":
    sys.exit(main())
