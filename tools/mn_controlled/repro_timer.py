#!/usr/bin/env python3
"""repro_timer.py -- deterministic-replay probe for TIMERS (sched_sleep).

The hardest closed-workload case for replay: same-delay sleeps, whose wake order
is decided purely by scheduling, not by the delay values.  Without the logical
clock each hub's sleep heap fires on its own wall-clock poll, so the cross-hub
wake order varies run-to-run; with it the controller advances a logical clock to
the earliest pending deadline only at a quiescent census, so the order is a
function of the seed.

Workload: N workers all sleep the SAME delay then record their id; plus a few
with mixed delays to exercise multiple distinct deadlines.  Signature is the
record order.  Same RUNLOOM_MN_SEED must reproduce one signature.  House style:
.format().

Usage: repro_timer.py [seeds] [reps]   (defaults 8 seeds, 6 reps)
"""
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

WORKLOAD = r"""
import sys; sys.path.insert(0, 'src'); import runloom_c
runloom_c.mn_init(3)
got = []
def worker(wid, delay):
    runloom_c.sched_sleep(delay)
    got.append(wid)
# 6 same-delay workers (pure scheduling order) + 3 staggered (distinct deadlines)
for i in range(6):
    runloom_c.mn_go(lambda i=i: worker(i, 0.002))
for j in range(3):
    runloom_c.mn_go(lambda j=j: worker(100 + j, 0.001 * (j + 1)))
runloom_c.mn_run(); runloom_c.mn_fini()
# conservation: every worker recorded exactly once
ok = sorted(got) == sorted([0,1,2,3,4,5,100,101,102])
print(",".join(str(x) for x in got) + ("|OK" if ok else "|LOST"))
"""


def run_once(seed):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    env["RUNLOOM_MN_SEED"] = str(seed)
    try:
        out = subprocess.run([sys.executable, "-c", WORKLOAD], env=env, cwd=ROOT,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    line = out.stdout.decode(errors="replace").strip().splitlines()
    last = line[-1] if line else "ERR"
    sig, sep, status = last.partition("|")
    if sep == "" or status != "OK":
        return "ERR(" + last + ")"
    return sig


def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    print("repro_timer: {} seeds x {} reps (same-delay + staggered sched_sleep)".format(seeds, reps))
    stable = 0
    for s in range(1, seeds + 1):
        sigs = [run_once(s) for _ in range(reps)]
        uniq = sorted(set(sigs))
        ok = len(uniq) == 1 and not uniq[0].startswith(("ERR", "TIMEOUT"))
        stable += ok
        if ok:
            print("  seed {:>3}: STABLE  {}".format(s, uniq[0]))
        else:
            print("  seed {:>3}: VARIES  {} distinct: {}".format(s, len(uniq), uniq))
    print("-" * 60)
    print("  {}/{} seeds reproduce identically across {} reps".format(stable, seeds, reps))
    return 0 if stable == seeds else 1


if __name__ == "__main__":
    sys.exit(main())
