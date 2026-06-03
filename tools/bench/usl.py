#!/usr/bin/env python3
"""usl.py -- Universal Scalability Law fit for runloom's M:N scheduler.

Throughput rarely scales linearly with hubs.  Gunther's Universal Scalability
Law explains why with two coefficients:

    C(p) = p / (1 + alpha*(p-1) + beta*p*(p-1))

  * alpha (contention)  -- serialization: shared work that can't go parallel
                           (locks, a single run-queue, the GIL if it leaked).
                           Pure-alpha scaling saturates at a ceiling.
  * beta  (coherence)   -- crosstalk: cost that grows with *pairs* of workers
                           (cache-line ping-pong, work-stealing chatter, false
                           sharing).  Pure-beta scaling has a PEAK, then gets
                           *slower* as you add hubs.

Fitting (alpha, beta) to measured throughput-vs-hubs tells you which wall runloom
is hitting and predicts the optimal hub count:

    p* = sqrt((1 - alpha) / beta)        (the throughput peak, if beta > 0)

Reference: N. Gunther, "Guerrilla Capacity Planning" (the USL).

Pure stdlib: the 2-parameter fit is a coarse-to-fine grid search (bulletproof,
no scipy).  House style: .format(), no f-strings.

Usage:
  tools/bench/usl.py [--tasks 200] [--iter 4000] [--reps 5] [--hubs 1,2,4,8,...]
"""
import argparse
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
import runloom_c


def work(n):
    """CPU-bound, GIL-off-parallelisable, and *cooperatively preemptible*.

    A pure-Python LCG loop on purpose: it runs as interpreter bytecode, so
    runloom's eval-hook preemption keeps hubs fair (no sysmon "WEDGED" flood),
    and it is NOT auto-offloaded the way hashlib/zlib are -- so the curve
    measures real M:N + interpreter parallel scaling, not the offload path.
    """
    s = 1
    for _ in range(n):
        s = (s * 1103515245 + 12345) & 0x7FFFFFFF
    return s


def throughput(hubs, tasks, iters):
    """ops/sec for `tasks` sha-chains of length `iters` across `hubs` hubs."""
    runloom_c.mn_init(hubs)
    t0 = time.perf_counter()
    for _ in range(tasks):
        runloom_c.mn_go(lambda: work(iters))
    runloom_c.mn_run()
    dt = time.perf_counter() - t0
    runloom_c.mn_fini()
    return (tasks * iters) / dt if dt > 0 else float("inf")


def measure(hub_list, tasks, iters, reps):
    pts = []
    for p in hub_list:
        samples = [throughput(p, tasks, iters) for _ in range(reps)]
        x = statistics.median(samples)
        pts.append((p, x))
        sys.stderr.write("  hubs {:>3}: {:>8.2f} M ops/s\n".format(p, x / 1e6))
    return pts


def usl(p, alpha, beta):
    return p / (1.0 + alpha * (p - 1) + beta * p * (p - 1))


def fit_usl(pts):
    """Fit (alpha, beta) to normalised speedup C(p)=X(p)/X(1) by grid search."""
    x1 = pts[0][1]
    ps = [p for p, _ in pts]
    cs = [x / x1 for _, x in pts]
    a_lo, a_hi, b_lo, b_hi = 0.0, 1.0, 0.0, 0.2
    best = (float("inf"), 0.0, 0.0)
    for _ in range(5):
        ga = [a_lo + (a_hi - a_lo) * i / 40.0 for i in range(41)]
        gb = [b_lo + (b_hi - b_lo) * i / 40.0 for i in range(41)]
        best = (float("inf"), 0.0, 0.0)
        for a in ga:
            for b in gb:
                err = sum((usl(p, a, b) - c) ** 2 for p, c in zip(ps, cs))
                if err < best[0]:
                    best = (err, a, b)
        _, a, b = best
        da = (a_hi - a_lo) / 20.0
        db = (b_hi - b_lo) / 20.0
        a_lo, a_hi = max(0.0, a - da), min(1.0, a + da)
        b_lo, b_hi = max(0.0, b - db), b + db
    return best[1], best[2], x1


def report(pts, alpha, beta, x1):
    print("")
    print("backend : {}".format(runloom_c.backend()))
    print("cpus    : {}".format(os.cpu_count()))
    print("-" * 64)
    print("  hubs     measured        USL model      speedup   (model)")
    for p, x in pts:
        c = usl(p, alpha, beta)
        print("  {:>4}   {:>9.2f} M/s   {:>9.2f} M/s    {:>6.2f}x  ({:>6.2f}x)".format(
            p, x / 1e6, c * x1 / 1e6, x / x1, c))
    print("-" * 64)
    print("alpha (contention) : {:.4f}".format(alpha))
    print("beta  (coherence)  : {:.5f}".format(beta))
    if beta > 1e-6 and alpha < 1.0:
        pstar = ((1.0 - alpha) / beta) ** 0.5
        peak = usl(pstar, alpha, beta) * x1
        print("predicted peak     : {:.0f} hubs  ->  {:.2f} M ops/s".format(
            pstar, peak / 1e6))
        print("interpretation     : coherence-bound -- adding hubs past ~{:.0f} "
              "makes it SLOWER.".format(pstar))
    elif alpha > 1e-3:
        ceiling = (1.0 / alpha) * x1
        print("predicted ceiling  : {:.2f} M ops/s (1/alpha) -- contention-bound,"
              " no peak.".format(ceiling / 1e6))
    else:
        print("interpretation     : near-linear in this range (alpha~0, beta~0).")
    print("-" * 64)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tasks", type=int, default=256)
    ap.add_argument("--iter", type=int, default=40000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--hubs", default=None,
                    help="comma list (default: powers of 2 up to cpu_count)")
    args = ap.parse_args()

    if args.hubs:
        hub_list = [int(h) for h in args.hubs.split(",")]
    else:
        n = os.cpu_count() or 8
        hub_list = []
        p = 1
        while p <= n:
            hub_list.append(p)
            p *= 2
        if hub_list[-1] != n:
            hub_list.append(n)

    if hasattr(runloom_c, "warmup"):
        runloom_c.warmup(args.tasks * 2)
    pts = measure(hub_list, args.tasks, args.iter, args.reps)
    alpha, beta, x1 = fit_usl(pts)
    report(pts, alpha, beta, x1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
