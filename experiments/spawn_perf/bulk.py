#!/usr/bin/env python3
"""Bulk "launch a fleet" harness (Exp B).  Unlike scaling.py (issuer fibers each
calling runloom.fiber in a loop), this drives the BULK path directly:
runloom_c.fiber_n(noop, N) builds the g/coro/stack arenas in ONE locked op, then
mn_run() drains all N on the hubs.  This is the idiom for spawning a large fixed
fleet at once, and the place MAP_POPULATE / pre-fault / huge pages can act on a
single contiguous block.

Reports best-of-reps spawn/s (create), run/s (drain), total/s.  Toggle the path
with env: RUNLOOM_GON_BULK=1 (else fiber_n loops = per-g spawn), RUNLOOM_GON_FRESH,
RUNLOOM_STACK_ARENA[_HUGE], RUNLOOM_GON_POPULATE (Exp B pre-fault)."""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import runloom_c


def noop():
    pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--n", type=int, default=300000)
    ap.add_argument("--stack-size", type=int, default=0)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--label", default="bulk")
    args = ap.parse_args()

    runloom_c.mn_init(args.hubs)
    b_spawn = b_run = b_total = 1e18
    prev_done = 0  # mn_run() returns the CUMULATIVE completed count; check the delta
    for _ in range(args.reps):
        t0 = time.perf_counter()
        if args.stack_size > 0:
            runloom_c.fiber_n(noop, args.n, stack_size=args.stack_size)
        else:
            runloom_c.fiber_n(noop, args.n)
        t1 = time.perf_counter()
        done = runloom_c.mn_run()
        t2 = time.perf_counter()
        assert done - prev_done == args.n, "delta=%d != n=%d" % (done - prev_done, args.n)
        prev_done = done
        b_spawn = min(b_spawn, t1 - t0)
        b_run = min(b_run, t2 - t1)
        b_total = min(b_total, t2 - t0)
    runloom_c.mn_fini()

    rec = {"label": args.label, "hubs": args.hubs, "n": args.n,
           "stack_size": args.stack_size, "reps": args.reps,
           "spawn_s": b_spawn, "run_s": b_run, "total_s": b_total,
           "spawn_per_s": args.n / b_spawn, "run_per_s": args.n / b_run,
           "total_per_s": args.n / b_total,
           "bulk": os.environ.get("RUNLOOM_GON_BULK", ""),
           "fresh": os.environ.get("RUNLOOM_GON_FRESH", ""),
           "arena": os.environ.get("RUNLOOM_STACK_ARENA", ""),
           "huge": os.environ.get("RUNLOOM_STACK_ARENA_HUGE", ""),
           "populate": os.environ.get("RUNLOOM_GON_POPULATE", ""),
           "ld_preload": "keep_resident" if "keep_resident" in os.environ.get("LD_PRELOAD", "") else ""}
    print("%-20s create=%8.0f/s  run=%8.0f/s  total=%8.0f/s  (ss=%d)" %
          (args.label, rec["spawn_per_s"], rec["run_per_s"], rec["total_per_s"],
           args.stack_size), file=sys.stderr)
    print(json.dumps(rec))


if __name__ == "__main__":
    main()
