#!/usr/bin/env python3
"""Spawn-scaling harness (committed; the per-magnitude experiments measure against
this).  Spawns `issuers` concurrent producer fibers across `hubs` hubs; each
producer creates n/issuers no-op worker fibers.  run() returns only after every
fiber (producers + workers) has run AND completed -- so this measures the full
spawn -> run -> destroy cycle, which is where the per-fiber stack mmap/mprotect
(creation) AND the CPython mimalloc QSBR purge madvise (completion) both land.

The experiments toggle behaviour purely through env (RUNLOOM_STACK_ARENA,
RUNLOOM_STACK_ARENA_HUGE, RUNLOOM_STACK_POPULATE, ...) + LD_PRELOAD; this harness
sets none of them, so a sweep is honest A/B.  Pin cores + GIL-off in the launcher
(see run_baseline.sh) -- NOT here.

Reports best-of-reps aggregate spawn/s = n / whole-run-seconds (Go's bench front-
loads the same way).  Prints one JSON line per invocation (last line)."""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import runloom


def noop():
    pass


def run_once(hubs, issuers, n, stack_size):
    base, rem = divmod(n, issuers)

    def producer(count):
        if stack_size > 0:
            for _ in range(count):
                runloom.fiber(noop, stack_size=stack_size)
        else:
            for _ in range(count):
                runloom.fiber(noop)

    def root():
        for i in range(issuers):
            runloom.fiber(producer, base + (1 if i < rem else 0))

    t0 = time.perf_counter()
    runloom.run(hubs, root)
    return time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--issuers", type=int, default=8)
    ap.add_argument("--n", type=int, default=500000)
    ap.add_argument("--stack-size", type=int, default=0)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    best = 1e18
    for _ in range(args.reps):
        best = min(best, run_once(args.hubs, args.issuers, args.n, args.stack_size))
    rate = args.n / best
    rec = {"label": args.label, "hubs": args.hubs, "issuers": args.issuers,
           "n": args.n, "stack_size": args.stack_size, "reps": args.reps,
           "seconds": best, "spawn_per_s": rate,
           "arena": os.environ.get("RUNLOOM_STACK_ARENA", ""),
           "arena_huge": os.environ.get("RUNLOOM_STACK_ARENA_HUGE", ""),
           "populate": os.environ.get("RUNLOOM_STACK_POPULATE", ""),
           "ld_preload": "keep_resident" if "keep_resident" in os.environ.get("LD_PRELOAD", "") else ""}
    print("%-22s issuers=%d  %8.0f spawn/s  (%.3fs / %d)" %
          (args.label or "run", args.issuers, rate, best, args.n), file=sys.stderr)
    print(json.dumps(rec))


if __name__ == "__main__":
    main()
