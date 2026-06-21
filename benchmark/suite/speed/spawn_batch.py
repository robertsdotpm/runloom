"""Active (batch) spawn measurement -- the fiber_n fleet-launch path, measured
IN-SUITE on this box so the report cites a committed number instead of prose.

For each N it times the end-to-end create+run+destroy of N no-op fibers two ways:
  - naked : for _ in range(N): runloom.fiber(noop)   (one at a time)
  - batch : runloom_c.fiber_n(noop, N)               (one bulk C call)
under default config and under runloom.optimize("throughput") (warm-stack arena +
bulk + FRESH + parallel create). An n=0 empty-run baseline is subtracted to remove
scheduler startup/teardown. rate = N / (wall - baseline), median of R reps.

HONEST FRAMING: there is NO Go batch-spawn equivalent (Go has no bulk-spawn API),
so this is a runloom *capability* measurement, NOT a Go comparison. The only
like-for-like spawn comparison vs Go is naked single-spawn (spawn_curve.json),
which Go wins. Run pinned to a single NUMA node (the cross-NUMA g-arena traffic
otherwise dominates and is a pinning artifact, not a spawn cost).
"""
import argparse
import json
import os
import statistics
import time

import runloom
import runloom_c


def noop():
    pass


def _empty():
    pass


def _baseline(hubs, reps=3):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        runloom.run(hubs, _empty)
        ts.append(time.perf_counter() - t0)
    return min(ts)   # min = least startup noise


def measure(mode, n, hubs, base, reps):
    rates, walls = [], []
    for _ in range(reps):
        if mode == "naked":
            def root():
                for _ in range(n):
                    runloom.fiber(noop)
        else:
            def root():
                runloom_c.fiber_n(noop, n)
        t0 = time.perf_counter()
        runloom.run(hubs, root)
        wall = time.perf_counter() - t0
        walls.append(wall)
        rates.append(n / max(wall - base, 1e-9))
    return {"rate_per_s": statistics.median(rates),
            "wall_median_s": statistics.median(walls), "reps": reps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--ns", default="100000,300000,1000000")
    ap.add_argument("--optimize", default="none", choices=["none", "throughput"])
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    ns = [int(x) for x in args.ns.split(",")]

    eff = runloom.optimize("throughput") if args.optimize == "throughput" else None
    base = _baseline(args.hubs)

    rates = {}
    for n in ns:
        rates[n] = {"naked": measure("naked", n, args.hubs, base, args.reps),
                    "batch": measure("batch", n, args.hubs, base, args.reps)}
        nk = rates[n]["naked"]["rate_per_s"]
        bt = rates[n]["batch"]["rate_per_s"]
        print("N=%-8d naked=%10.0f/s  batch=%10.0f/s  (batch/naked %.2fx)"
              % (n, nk, bt, bt / max(nk, 1)))

    out = {"meta": {"hubs": args.hubs, "reps": args.reps, "baseline_s": base,
                    "optimize": args.optimize, "optimize_eff": eff,
                    "cpu_affinity": sorted(os.sched_getaffinity(0)),
                    "ncores_pinned": len(os.sched_getaffinity(0)),
                    "note": "rate = N/(wall - empty-run baseline), median of reps; "
                            "NO Go batch equivalent exists -- capability, not comparison"},
           "rates": rates}
    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=1)
        print("wrote", args.out)


if __name__ == "__main__":
    main()
