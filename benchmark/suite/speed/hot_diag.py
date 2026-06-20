#!/usr/bin/env python3
"""Diagnostic: what actually contends in the 44-hub ctxswitch microbenchmark?
One variant per process, pinned, preempt off.  Pinpoints whether the cost is the
shared CODE object, the shared GLOBALS dict, or a shared CLOSURE (shared cells).
"""
import argparse
import time
import types

import runloom
import runloom_c

SYC = runloom_c.sched_yield
_K = 0


def _module_tmpl():
    for _ in range(_K):            # _K + SYC via THIS module's globals (shared dict)
        SYC()


def _make_closure(K, yobj):
    def worker():
        for _ in range(K):         # K + yobj via CLOSURE CELLS
            yobj()
    return worker


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", required=True)
    ap.add_argument("--hubs", type=int, default=44)
    ap.add_argument("--n", type=int, default=1_000_000)
    a = ap.parse_args()
    G = a.hubs * 16
    K = max(1, a.n // G)
    SW = G * K
    global _K
    _K = K

    if a.variant == "module_shared":                 # module fn, 1 code, shared globals
        workers = [_module_tmpl] * G
    elif a.variant == "module_code_clone":           # module fn, G CODE clones, shared globals
        workers = [types.FunctionType(_module_tmpl.__code__.replace(),
                                      _module_tmpl.__globals__, "m") for _ in range(G)]
    elif a.variant == "closure_shared":              # ONE closure shared by all (shared cells)
        w = _make_closure(K, SYC)
        workers = [w] * G
    elif a.variant == "closure_distinct":            # G distinct closures (own cells each)
        workers = [_make_closure(K, SYC) for _ in range(G)]
    elif a.variant == "closure_code_clone":          # 1 closure, G CODE clones, SAME cells (== @hot on a closure)
        w = _make_closure(K, SYC)
        workers = [types.FunctionType(w.__code__.replace(), w.__globals__, "w",
                                      w.__defaults__, w.__closure__) for _ in range(G)]
    elif a.variant == "closure_perhub":              # 1 closure, HUBS code clones, same cells
        w = _make_closure(K, SYC)
        base = [types.FunctionType(w.__code__.replace(), w.__globals__, "w",
                                   w.__defaults__, w.__closure__) for _ in range(a.hubs)]
        workers = [base[i % a.hubs] for i in range(G)]
    elif a.variant == "closure_perhub_cells":        # HUBS clones, FRESH cells (copy contents) -- the REAL fix
        w = _make_closure(K, SYC)
        base = [types.FunctionType(
                    w.__code__, w.__globals__, "w", w.__defaults__,
                    tuple(types.CellType(c.cell_contents) for c in w.__closure__))
                for _ in range(a.hubs)]
        workers = [base[i % a.hubs] for i in range(G)]
    else:
        raise SystemExit("bad variant")

    def root():
        for w in workers:
            runloom.fiber(w)

    t0 = time.perf_counter()
    runloom.run(a.hubs, root)
    dt = time.perf_counter() - t0
    print("%-18s %12.0f switches/s" % (a.variant, SW / dt), flush=True)


if __name__ == "__main__":
    main()
