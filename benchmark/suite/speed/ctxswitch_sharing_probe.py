#!/usr/bin/env python3
"""WHY do Python fibers cost so much per yield? -- the shared-object contention
probe. Run pinned at a hub count, e.g.:

    taskset -c 16-59 env PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON=0 \
        python3.13t benchmark/suite/speed/ctxswitch_sharing_probe.py --hubs 44

It de-shares the loaded-yield benchmark one layer at a time and prints aggregate
switches/sec, isolating each contention source:

  shared worker + shared yield     -- the original benchmark (704 IDENTICAL
                                      worker fns + one sched_yield object).
  distinct worker + shared yield   -- per-fiber worker fn (own code obj+globals);
                                      removes the shared-function contention.
  distinct worker + partial yield  -- ALSO per-fiber yield callable
                                      (functools.partial); removes the shared
                                      sched_yield-object contention.
  distinct worker + closure yield  -- control: distinct closure that RE-SHARES
                                      sched_yield internally (should NOT help).

Reference points: c_entry capstone ~= 278M switches/s @44 hubs (no Python),
1 hub ~= 4.3M/s, asyncio ~= 447k/s (1 core). See SCHEDULER_SCALING_FINDINGS.md
"## ACTIVE / RESUME" for the full readout and the proposed fix.
"""
import argparse
import functools
import statistics
import time

import runloom
import runloom_c

SYC = runloom_c.sched_yield


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=44)
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--reps", type=int, default=2)
    a = ap.parse_args()
    HUBS, G = a.hubs, a.hubs * 16
    K = max(1, a.n // G)
    SW = G * K

    def worker_with(yobj):
        g = {"sy": yobj, "K": K, "__builtins__": __builtins__}
        exec(compile("def w():\n for _ in range(K):\n  sy()", "<w>", "exec"), g)
        return g["w"]

    def run_shared_worker():
        sy = SYC
        def worker():
            for _ in range(K):
                sy()
        def root():
            for _ in range(G):
                runloom.fiber(worker)
        t0 = time.perf_counter(); runloom.run(HUBS, root); return time.perf_counter() - t0

    def run_distinct(make_yield):
        workers = [worker_with(make_yield()) for _ in range(G)]
        def root():
            for w in workers:
                runloom.fiber(w)
        t0 = time.perf_counter(); runloom.run(HUBS, root); return time.perf_counter() - t0

    cases = [
        ("shared worker + shared yield", run_shared_worker),
        ("distinct worker + shared yield", lambda: run_distinct(lambda: SYC)),
        ("distinct worker + partial yield", lambda: run_distinct(lambda: functools.partial(SYC))),
        ("distinct worker + closure yield", lambda: run_distinct(lambda: (lambda yc=SYC: (lambda: yc()))())),
    ]
    print("hubs=%d  G=%d fibers  %d switches  (median of %d reps)" % (HUBS, G, SW, a.reps), flush=True)
    for name, fn in cases:
        ts = [fn() for _ in range(a.reps)]
        t = statistics.median(ts)
        print("  %-34s %8.0f ns/switch  %12.0f switches/s" % (name, t * 1e9 / SW, SW / t), flush=True)


if __name__ == "__main__":
    main()
