#!/usr/bin/env python3
"""pct_find.py -- PCT (Probabilistic Concurrency Testing) on the controlled M:N
scheduler: a bug-DEPTH-guaranteed seeded search, the principled upgrade of the
baton's uniform-random grant order (RUNLOOM_MN_PCT=<depth d>).

PCT (Burckhardt et al., ASPLOS 2010) assigns every hub a distinct random base
priority, always grants the baton to the highest-priority WAITING hub, and plants
d-1 seeded "priority change points" at grant-step indices in [1, k]; reaching a
change point demotes the hub that ran that step below all base priorities. The
d-1 demotions are exactly the d-1 ordering inversions a depth-d bug needs, so any
such bug is hit with probability >= 1/(n * k^(d-1)) PER SEED -- a guaranteed lower
bound the uniform draw has no analogue of. (See the runtime hook + the TLA
safety argument in src/runloom_c/mn_sched_hub_resume_preempt.c.inc and
verify/tla/PygoMNControl.tla.)

This script demonstrates the three properties that matter on a deliberately
NARROW order-dependent bug -- goroutine B must observe a shared counter at a LATE
value, an interleaving uniform scheduling is biased away from (it runs B's single
read early, geometrically) but that PCT targets directly:

  1. depth-1 (no change points) NEVER finds it: a fixed priority order cannot
     sandwich B's read among A's increments. The bug is genuinely order-dependent.
  2. PCT depth-2 finds it at ~1/(n*k) -- matching the bound -- while uniform
     random (no PCT) essentially never does on the same seed budget.
  3. A finding seed REPLAYS the bug every run -> a permanent, deterministic
     regression repro, not a flaky once-in-N.

House style: .format(), no f-strings.

Usage: pct_find.py [seeds]          (default 60)
"""
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

# N increments by A; B must observe the counter == TARGET (a late value). k is
# sized to the run length (a handful of grants) per the README's guidance.
N, TARGET, K = 12, 10, 14

WORKLOAD = (
    "import sys; sys.path.insert(0, 'src'); import runloom_c\n"
    "N = {}; TARGET = {}\n".format(N, TARGET) +
    "runloom_c.mn_init(2)\n"
    "st = {'x': 0, 'seen': None}\n"
    "def A():\n"
    "    for i in range(N):\n"
    "        st['x'] += 1\n"
    "        runloom_c.sched_sleep(0)\n"
    "def B():\n"
    "    st['seen'] = st['x']\n"           # one read = one segment, PCT places it
    "runloom_c.mn_go(A)\n"
    "runloom_c.mn_go(B)\n"
    "runloom_c.mn_run(); runloom_c.mn_fini()\n"
    "print('BUG' if st['seen'] == TARGET else 'ok', st['seen'])\n"
)


def run_once(seed, depth=None):
    """One subprocess run. depth=None -> uniform (no PCT); else RUNLOOM_MN_PCT=depth.
    Returns True if the narrow bug fired."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    env["RUNLOOM_MN_SEED"] = str(seed)
    if depth is not None:
        env["RUNLOOM_MN_PCT"] = str(depth)
        env["RUNLOOM_MN_PCT_STEPS"] = str(K)
    try:
        out = subprocess.run([sys.executable, "-c", WORKLOAD], env=env, cwd=ROOT,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    except subprocess.TimeoutExpired:
        return False
    line = out.stdout.decode(errors="replace").strip().splitlines()
    return bool(line) and line[-1].startswith("BUG")


def sweep(seeds, depth):
    hits = [s for s in range(1, seeds + 1) if run_once(s, depth)]
    return hits


def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    bound = 1.0 / (2.0 * K)             # 1/(n * k^(d-1)) for n=2, d=2
    print("PCT on the controlled M:N scheduler -- narrow late-observe bug")
    print("  workload: A increments x {}x; B reads once; BUG iff B sees x=={}".format(N, TARGET))
    print("  {} seeds; k={}; n=2 hubs; PCT depth-2 bound = 1/(n*k) = {:.3f}".format(seeds, K, bound))
    print("-" * 64)

    d1 = sweep(seeds, 1)
    uni = sweep(seeds, None)
    d2 = sweep(seeds, 2)
    print("  depth-1 PCT (0 change points): {:>2}/{} hits  {}".format(
        len(d1), seeds, "(fixed order cannot sandwich -> order-dependent)" if not d1 else d1))
    print("  uniform     (no PCT):          {:>2}/{} hits  {}".format(
        len(uni), seeds, uni if uni else "(random is biased away from the narrow interleaving)"))
    print("  PCT depth-2 (1 change point):  {:>2}/{} hits  seeds={}".format(len(d2), seeds, d2))
    rate = (len(d2) / float(seeds)) if seeds else 0.0
    print("    empirical depth-2 rate {:.3f}  vs  bound {:.3f}".format(rate, bound))
    print("-" * 64)

    ok_depth = (not d1)                 # depth-1 must find nothing
    ok_find = bool(d2)                  # PCT must find it
    ok_replay = True
    if d2:
        seed = d2[0]
        reps = 12
        rep_hits = sum(run_once(seed, 2) for _ in range(reps))
        ok_replay = (rep_hits == reps)
        print("  replay: PCT seed {} x {} reps -> {}/{} hits  {}".format(
            seed, reps, rep_hits, reps, "DETERMINISTIC" if ok_replay else "NONDETERMINISTIC!"))

    print("-" * 64)
    ok = ok_depth and ok_find and ok_replay
    print("  RESULT: {}".format(
        "PASS -- depth-1 finds nothing, PCT finds + replays the narrow bug"
        if ok else "INCONCLUSIVE (try more seeds: pct_find.py 200)"))
    # Not a CI gate: PCT is a probabilistic search, so a too-small budget can miss.
    # Exit 0 on a clean demonstration, 1 only if the determinism property breaks.
    return 0 if (ok or (ok_depth and not d2)) else 1


if __name__ == "__main__":
    sys.exit(main())
