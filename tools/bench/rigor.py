#!/usr/bin/env python3
"""rigor.py -- statistically rigorous microbenchmark harness for pygo.

Most "is it faster?" numbers are wrong for two well-documented reasons, and
this harness defends against both:

  1. Within-process repetitions are autocorrelated, so a confidence interval
     computed from them is far too tight -- you conclude a difference is real
     when it's noise.  The only honest CI comes from *independent process
     executions*.  (Kalibera & Jones, "Rigorous Benchmarking in Reasonable
     Time", ISMM 2013.)

  2. Code/stack/heap layout -- which shifts with something as silly as the
     size of an environment variable or the link order -- can move a
     measurement by more than the change you're trying to measure.  A single
     pinned layout can hand one build a lucky win.  (Mytkowicz, Diwan,
     Hauswirth, Sweeney, "Producing Wrong Data Without Doing Anything
     Obviously Wrong!", ASPLOS 2009.)

Design:
  * Two-level sampling: ``--runs`` independent OS processes (outer) each do
    ``--inner`` timed repetitions (inner, with ``--warmup`` discarded).  Each
    process contributes ONE point (its inner median); the reported CI is the
    nonparametric bootstrap over those independent points.
  * Layout-bias guard: every child gets a random-length ``PYGO_BENCH_PAD``
    env var, so layout varies run-to-run and the CI *absorbs* layout
    sensitivity instead of hiding it.  ``--pin`` disables it (for a tightly
    controlled A/B on one machine state).

Pure stdlib -- no numpy/scipy.  House style: ``.format()``, no f-strings.

Usage:
  tools/bench/rigor.py list
  tools/bench/rigor.py run spawn [--runs 10 --inner 5 --warmup 2] [--json out.json]
  tools/bench/rigor.py ab base.json new.json     # significance of a change
  tools/bench/rigor.py child <workload> ...      # internal (one subprocess)
"""
import argparse
import json
import os
import random
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SRC = os.path.join(ROOT, "src")


# --------------------------------------------------------------------------
# statistics (pure stdlib, nonparametric -- no normality assumption)
# --------------------------------------------------------------------------
def percentile(sorted_xs, pct):
    """Linear-interpolated percentile of an already-sorted list."""
    if not sorted_xs:
        return float("nan")
    if len(sorted_xs) == 1:
        return sorted_xs[0]
    rank = (pct / 100.0) * (len(sorted_xs) - 1)
    lo = int(rank)
    frac = rank - lo
    if lo + 1 >= len(sorted_xs):
        return sorted_xs[lo]
    return sorted_xs[lo] + frac * (sorted_xs[lo + 1] - sorted_xs[lo])


def bootstrap_ci(points, statfn=statistics.median, conf=0.95, resamples=4000,
                 rng=None):
    """Nonparametric bootstrap CI of ``statfn`` over ``points``."""
    rng = rng or random.Random(0xC0FFEE)
    n = len(points)
    if n < 2:
        v = statfn(points) if points else float("nan")
        return v, v
    boots = []
    for _ in range(resamples):
        sample = [points[rng.randrange(n)] for _ in range(n)]
        boots.append(statfn(sample))
    boots.sort()
    half = (1.0 - conf) / 2.0
    return percentile(boots, half * 100.0), percentile(boots, (1.0 - half) * 100.0)


def cv(points):
    """Coefficient of variation (stdev/mean) -- a unitless noise measure."""
    if len(points) < 2:
        return 0.0
    m = statistics.mean(points)
    return statistics.pstdev(points) / m if m else float("inf")


# --------------------------------------------------------------------------
# child: run ONE workload in this process, print a JSON line
# --------------------------------------------------------------------------
def run_child(name, inner, warmup, scale):
    if SRC not in sys.path:
        sys.path.insert(0, SRC)
    import pygo_core
    import workloads

    fn, unit = workloads.WORKLOADS[name]
    # Pre-warm the stack pool so we measure steady state, not first mmap.
    if hasattr(pygo_core, "warmup"):
        pygo_core.warmup(200000)

    kw = {} if scale is None else {"scale": scale}
    for _ in range(warmup):
        try:
            fn(**kw)
        except TypeError:
            fn()

    rates = []
    for _ in range(inner):
        try:
            ops, secs = fn(**kw)
        except TypeError:
            ops, secs = fn()
        rates.append(ops / secs if secs > 0 else float("inf"))

    print(json.dumps({
        "workload": name,
        "unit": unit,
        "backend": pygo_core.backend() if hasattr(pygo_core, "backend") else "?",
        "inner_rates": rates,        # ops/sec per inner repetition
        "inner_median": statistics.median(rates),
    }))


# --------------------------------------------------------------------------
# parent: spawn independent processes, collect one point each, report
# --------------------------------------------------------------------------
def child_env(pin):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = SRC + os.pathsep + HERE + os.pathsep + env.get("PYTHONPATH", "")
    if not pin:
        # Mytkowicz layout-bias guard: perturb the environment size so the
        # stack/heap/code layout differs between independent runs.
        env["PYGO_BENCH_PAD"] = "x" * random.randint(0, 4096)
    return env


def run_process(name, inner, warmup, scale, pin):
    cmd = [sys.executable, os.path.join(HERE, "rigor.py"), "child", name,
           "--inner", str(inner), "--warmup", str(warmup)]
    if scale is not None:
        cmd += ["--scale", str(scale)]
    out = subprocess.run(cmd, env=child_env(pin), cwd=ROOT,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    line = out.stdout.decode().strip().splitlines()
    if out.returncode != 0 or not line:
        sys.stderr.write(out.stderr.decode())
        raise RuntimeError("child failed for workload " + name)
    return json.loads(line[-1])


def fmt_rate(r):
    if r >= 1e6:
        return "{:>8.2f} M/s".format(r / 1e6)
    if r >= 1e3:
        return "{:>8.2f} K/s".format(r / 1e3)
    return "{:>8.2f}  /s".format(r)


def cmd_run(args):
    points = []
    backend = unit = "?"
    for i in range(args.runs):
        res = run_process(args.workload, args.inner, args.warmup, args.scale,
                          args.pin)
        points.append(res["inner_median"])
        backend, unit = res["backend"], res["unit"]
        sys.stderr.write("  run {:>3}/{}  {}\r".format(
            i + 1, args.runs, fmt_rate(res["inner_median"])))
    sys.stderr.write("\n")

    med = statistics.median(points)
    lo, hi = bootstrap_ci(points)
    coeff = cv(points)
    ns = 1e9 / med if med else float("nan")
    width = (hi - lo) / med * 100.0 if med else float("nan")

    print("")
    print("workload : {}   ({} per op)".format(args.workload, unit))
    print("backend  : {}".format(backend))
    print("design   : {} processes x {} inner ({} warmup), layout-guard {}".format(
        args.runs, args.inner, args.warmup, "OFF (--pin)" if args.pin else "ON"))
    print("-" * 60)
    print("throughput median : {}   ({:.1f} ns/op)".format(fmt_rate(med), ns))
    print("95% CI (bootstrap): [{}, {}]   (+/-{:.1f}% of median)".format(
        fmt_rate(lo).strip(), fmt_rate(hi).strip(), width / 2.0))
    print("noise (CV)        : {:.2%}{}".format(
        coeff, "   <-- UNSTABLE: distrust deltas below this" if coeff > 0.05 else ""))
    print("-" * 60)

    if args.json:
        with open(args.json, "w") as f:
            json.dump({"workload": args.workload, "unit": unit,
                       "backend": backend, "points": points}, f, indent=2)
        print("saved points -> {}".format(args.json))
    return 0


def cmd_ab(args):
    with open(args.base) as f:
        a = json.load(f)
    with open(args.new) as f:
        b = json.load(f)
    if a["workload"] != b["workload"]:
        sys.stderr.write("warning: comparing different workloads ({} vs {})\n"
                         .format(a["workload"], b["workload"]))
    pa, pb = a["points"], b["points"]
    ma, mb = statistics.median(pa), statistics.median(pb)
    change = (mb - ma) / ma * 100.0 if ma else float("nan")

    # Bootstrap the difference of medians; CI excluding 0 => significant.
    rng = random.Random(0x5EED)
    diffs = []
    for _ in range(8000):
        sa = statistics.median([pa[rng.randrange(len(pa))] for _ in pa])
        sb = statistics.median([pb[rng.randrange(len(pb))] for _ in pb])
        diffs.append(sb - sa)
    diffs.sort()
    lo, hi = percentile(diffs, 2.5), percentile(diffs, 97.5)
    significant = (lo > 0) or (hi < 0)

    print("A/B: {}".format(a["workload"]))
    print("  base : {}".format(fmt_rate(ma)))
    print("  new  : {}".format(fmt_rate(mb)))
    print("  change: {:+.1f}%   ({})".format(
        change, "FASTER" if change > 0 else "SLOWER"))
    print("  95% CI of difference: [{:+.0f}, {:+.0f}] ops/s".format(lo, hi))
    if significant:
        print("  -> SIGNIFICANT (CI excludes 0)")
    else:
        print("  -> NOT significant (CI straddles 0 -- difference is noise)")
    return 0 if not (significant and change < 0) else 1


def cmd_list(args):
    import importlib
    sys.path.insert(0, HERE)
    workloads = importlib.import_module("workloads")
    print("workloads:")
    for name, (_, unit) in workloads.WORKLOADS.items():
        print("  {:<16} ({} per op)".format(name, unit))
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")

    pr = sub.add_parser("run", help="benchmark one workload with a real CI")
    pr.add_argument("workload")
    pr.add_argument("--runs", type=int, default=10, help="independent processes")
    pr.add_argument("--inner", type=int, default=5, help="reps per process")
    pr.add_argument("--warmup", type=int, default=2, help="discarded reps")
    pr.add_argument("--scale", type=int, default=None, help="override workload size")
    pr.add_argument("--pin", action="store_true", help="disable layout-bias guard")
    pr.add_argument("--json", default=None, help="save points for later A/B")
    pr.set_defaults(func=cmd_run)

    pa = sub.add_parser("ab", help="significance test between two saved runs")
    pa.add_argument("base")
    pa.add_argument("new")
    pa.set_defaults(func=cmd_ab)

    pc = sub.add_parser("child", help=argparse.SUPPRESS)
    pc.add_argument("workload")
    pc.add_argument("--inner", type=int, default=5)
    pc.add_argument("--warmup", type=int, default=2)
    pc.add_argument("--scale", type=int, default=None)
    pc.set_defaults(func=lambda a: run_child(a.workload, a.inner, a.warmup, a.scale))

    pl = sub.add_parser("list", help="list workloads")
    pl.set_defaults(func=cmd_list)

    args = p.parse_args()
    if not getattr(args, "func", None):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
