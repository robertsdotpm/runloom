#!/usr/bin/env python3
"""Driver for the c_entry scheduler capstone. Mirrors runloom_epoll_py_fiber.py's
ctxswitch/spawn but spawns tstate-free c_entry fibers (no Python eval, no shared
closure cells) via centry_probe. Resolves runloom_c's exported scheduler symbols
by promoting runloom_c.so to RTLD_GLOBAL before importing the probe.

The orchestrator subtracts an n=0 baseline (same as run_speed.py).
"""
import argparse
import ctypes
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import runloom_c
ctypes.CDLL(runloom_c.__file__, mode=ctypes.RTLD_GLOBAL)   # promote exported symbols
import centry_probe                                          # noqa: E402
import runloom                                               # noqa: E402


def m_ctxswitch(n, hubs):
    G = max(2, hubs * 16)
    K = max(1, n // G)
    def root():
        centry_probe.spawn_yielders_c(G, K)
    t0 = time.perf_counter()
    runloom.run(hubs, root)
    return {"seconds": time.perf_counter() - t0, "switches": G * K, "n": n,
            "hubs": hubs, "fibers": G}


def m_spawn(n, hubs, warm=0):
    # warm>0: run the spawn `warm` extra times in-process first and report the
    # BEST timed pass, so the one-time runloom.run() scheduler boot is excluded --
    # the same basis Go is measured on (its runtime is already up at main()).
    # warm=0: single raw pass (boot included), the cold first-burst.
    def root():
        centry_probe.spawn_c(n)
    best = None
    for _ in range(warm + 1):
        t0 = time.perf_counter()
        runloom.run(hubs, root)
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return {"seconds": best, "n": n, "hubs": hubs, "warm": warm}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metric", choices=["ctxswitch", "spawn"], required=True)
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--warm", type=int, default=0,
                    help="extra in-process passes before timing (boot-excluded warm rate)")
    a = ap.parse_args()
    res = m_ctxswitch(a.n, a.hubs) if a.metric == "ctxswitch" else m_spawn(a.n, a.hubs, a.warm)
    res["metric"] = a.metric
    print(json.dumps(res))


if __name__ == "__main__":
    main()
