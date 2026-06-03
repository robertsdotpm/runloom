"""cProfile a Python-level runloom workload -> top tables (+ optional .pstats).

Scope/caveats (important so the output isn't misread):
  * cProfile only sees PYTHON frames. runloom's scheduler / channels / fcontext
    swap are C and invisible here -- use perfrecord.sh for the C hot path
    (finding F6). cProfile is for the Python-visible call breakdown + counts.
  * cProfile only profiles the CALLING OS thread, so use it on single-hub
    workloads (spawn/yield/pingpong/buffered), never mn (its hubs run on
    other threads and would be missed).

Usage:
    PYTHONPATH=src python3 -m bench.profile.cprofile_driver pingpong --n 50000
    ... --sort cumtime --out bench/results/profiles/pingpong.pstats
"""
import argparse
import cProfile
import pstats

from bench.gil import ensure_nogil


def make(workload, n, it):
    from bench.micro import (
        make_spawn, make_yield, make_pingpong, make_buffered)
    if workload == "spawn":
        return make_spawn(n or 10_000)
    if workload == "yield":
        return make_yield(n or 100, it or 1_000)
    if workload == "pingpong":
        return make_pingpong(n or 50_000)
    if workload == "buffered":
        return make_buffered(n or 500_000, 64)
    raise SystemExit("unknown/unsupported (single-hub only) workload %r" % workload)


def main(argv=None):
    ensure_nogil()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("workload",
                   choices=["spawn", "yield", "pingpong", "buffered"])
    p.add_argument("--n", type=int, default=0)
    p.add_argument("--iter", type=int, default=0)
    p.add_argument("--sort", default="tottime",
                   choices=["tottime", "cumtime", "ncalls"])
    p.add_argument("--rows", type=int, default=20)
    p.add_argument("--out", default="")
    args = p.parse_args(argv)

    from bench.harness import default_pin_set, pin
    pin(default_pin_set(n=8, node=1))

    run = make(args.workload, args.n, args.iter)
    run()  # warm import / type caches so they don't dominate the profile

    pr = cProfile.Profile()
    pr.enable()
    run()
    pr.disable()

    st = pstats.Stats(pr).strip_dirs().sort_stats(args.sort)
    print("# cProfile %s (Python frames only; sort=%s)" % (args.workload, args.sort))
    st.print_stats(args.rows)
    if args.out:
        pr.dump_stats(args.out)
        print("wrote %s" % args.out)


if __name__ == "__main__":
    main()
