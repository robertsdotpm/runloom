#!/usr/bin/env python3
"""Exp E: the free-threaded cross-core refcount tax on spawn.

Each runloom.fiber(noop) increfs the shared callable `noop` when it stows it in the
g, and decrefs it at completion -- on whichever hub ran the fiber.  Under the GIL
those are cheap biased-refcount ops; free-threaded they are cross-core atomic RMWs
on ONE cache line (noop's refcount), bouncing between all the hub cores.  Same for
noop.__code__ / __globals__ and the producer closures.  `immortalize`-ing those
shared objects turns every incref/decref on them into a no-op (FT immortal refcount)
-> the cache line stops bouncing.  This measures how much of the spawn wall is that
tax.  --immortalize toggles it; everything else matches scaling.py."""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import runloom
import runloom_c


def noop():
    pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--issuers", type=int, default=8)
    ap.add_argument("--n", type=int, default=300000)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--immortalize", action="store_true")
    ap.add_argument("--label", default="refcount")
    args = ap.parse_args()

    base, rem = divmod(args.n, args.issuers)

    def producer(count):
        for _ in range(count):
            runloom.fiber(noop)

    def root():
        for i in range(args.issuers):
            runloom.fiber(producer, base + (1 if i < rem else 0))

    if args.immortalize:
        # Freeze every object the per-fiber spawn touches the refcount of: the
        # callable graph + the producer closure graph + the spawn entrypoint.
        for obj in (noop, noop.__code__, noop.__globals__,
                    producer, producer.__code__, root, root.__code__,
                    runloom.fiber):
            runloom_c.immortalize(obj)

    best = 1e18
    for _ in range(args.reps):
        t0 = time.perf_counter()
        runloom.run(args.hubs, root)
        best = min(best, time.perf_counter() - t0)
    rate = args.n / best
    rec = {"label": args.label, "hubs": args.hubs, "issuers": args.issuers,
           "n": args.n, "reps": args.reps, "immortalize": args.immortalize,
           "seconds": best, "spawn_per_s": rate}
    print("%-22s issuers=%d immort=%-5s %9.0f spawn/s" %
          (args.label, args.issuers, args.immortalize, rate), file=sys.stderr)
    print(json.dumps(rec))


if __name__ == "__main__":
    main()
