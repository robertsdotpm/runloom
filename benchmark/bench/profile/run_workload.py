"""Run ONE runloom workload as a clean process for an external profiler to
observe (perf / bpftrace / strace / valgrind / cProfile-via-driver).

This deliberately does no timing or statistics -- the attached tool is the
instrument.  It forces the GIL off and pins exactly like bench.harness so the
profiled process matches benchmarked conditions, then runs the chosen
workload `reps` times and exits.

Usage:
    PYTHONPATH=src python3 -m bench.profile.run_workload <workload> [opts]

    workloads: spawn yield pingpong buffered mn
    opts: --n N --iter M --hubs H --reps R --quiet

Examples:
    # spawn 10k goroutines x5 -- for `perf stat -e page-faults`
    ... run_workload spawn --n 10000 --reps 5
    # M:N sha256 on 8 hubs -- for `perf record -e task-clock`
    ... run_workload mn --n 128 --iter 2000 --hubs 8 --reps 3
"""
import argparse
import sys

from bench.gil import ensure_nogil


def build(workload, args):
    """Return a zero-arg callable performing one unit of the workload."""
    import runloom_c

    if workload == "mn":
        from bench.mn import make_mn, N as MN_N
        import bench.mn as mn
        mn.N = args.n or MN_N
        mn.ITER = args.iter or mn.ITER
        setup, once, teardown = make_mn(args.hubs)
        setup()  # leave the pool up for the whole profiled run

        def run():
            once()
        run.teardown = teardown
        return run

    from bench.micro import (
        make_spawn, make_yield, make_pingpong, make_buffered)
    if workload == "spawn":
        return make_spawn(args.n or 10_000)
    if workload == "yield":
        return make_yield(args.n or 100, args.iter or 1_000)
    if workload == "pingpong":
        return make_pingpong(args.n or 100_000)
    if workload == "buffered":
        return make_buffered(args.n or 500_000, 64)
    raise SystemExit("unknown workload %r" % workload)


def main(argv=None):
    ensure_nogil()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("workload",
                   choices=["spawn", "yield", "pingpong", "buffered", "mn"])
    p.add_argument("--n", type=int, default=0, help="goroutines / round-trips")
    p.add_argument("--iter", type=int, default=0, help="inner iterations")
    p.add_argument("--hubs", type=int, default=8, help="M:N hub count")
    p.add_argument("--reps", type=int, default=1, help="times to run the unit")
    p.add_argument("--pin", type=int, default=8, help="cpus to pin (node1)")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args(argv)

    # Pin like the harness (import here so ensure_nogil's re-exec is cheap).
    from bench.harness import default_pin_set, pin
    pinned = pin(default_pin_set(n=max(args.pin, args.hubs), node=1))

    run = build(args.workload, args)
    if not args.quiet:
        import sys as _s
        gil = getattr(sys, "_is_gil_enabled", lambda: True)()
        print("run_workload %s n=%s iter=%s hubs=%s reps=%d gil=%s pinned=%s"
              % (args.workload, args.n or "default", args.iter or "default",
                 args.hubs, args.reps, gil,
                 ("%d cpus" % len(pinned)) if pinned else "no"),
              file=_s.stderr)
    for _ in range(args.reps):
        run()
    teardown = getattr(run, "teardown", None)
    if teardown is not None:
        teardown()


if __name__ == "__main__":
    main()
