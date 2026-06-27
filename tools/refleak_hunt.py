#!/usr/bin/env python3
"""refleak_hunt.py -- per-iteration refcount/alloc drift hunter (run under pydebug).

A --with-pydebug CPython exposes sys.gettotalrefcount() and getallocatedblocks().
CPython's own `python -m test -R 3:3` uses them to catch a steady +1/iteration
leak that a release build hides. runloom uses them today ONLY to assert the build
IS pydebug (run_pydebug.sh) -- never to measure per-iteration drift. This does
that for runloom's hot ops, and -- because runloom's stated focus is the
free-threaded BIASED-REFCOUNT cross-thread merge -- it treats a NEGATIVE drift as
a hard fail too (an over-release / merge-accounting bug), not just a positive leak.

Method (CPython's huntrleaks shape): warm WARMUP iterations (let one-time caches
settle), then measure ITERS iterations with gc.collect() between each, recording
the gettotalrefcount + getallocatedblocks delta per iteration. A steady same-sign
nonzero delta across the measured iterations is a leak; a lone first-iteration
blip is tolerated (residual warm-up).

The `mn_cycle` op is the biased-refcount stressor: goroutines on real hubs
incref/decref a SHARED object, merged at the thread boundary -- the exact path
where a brc accounting bug would net a nonzero per-cycle delta.

Run:  tools/run_refleak.sh        (builds the pydebug-ABI ext, then this)
      RUNLOOM_PYDEBUG_PYTHON=/path/python tools/refleak_hunt.py
Exit: 0 = no steady drift; 1 = a leak (or over-release) in some op; 2 = not pydebug.
"""
import gc
import os
import sys

if not hasattr(sys, "gettotalrefcount"):
    sys.stderr.write("refleak_hunt: needs a --with-pydebug interpreter "
                     "(no sys.gettotalrefcount). Run via tools/run_refleak.sh. SKIP.\n")
    sys.exit(0)

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
try:
    import runloom_c
except ImportError as e:
    sys.stderr.write("refleak_hunt: runloom_c not built for this (pydebug) ABI: "
                     "{0}\n  build: PYTHON_GIL=0 <pydebug-python> setup.py build_ext "
                     "--inplace\n".format(e))
    sys.exit(2)

WARMUP = 3
ITERS = 6


# ---- hot ops: each does ONE self-contained unit that must net zero ----------
def op_chan_construct():
    c = runloom_c.Chan(8)
    del c


def op_coro_construct():
    c = runloom_c.Coro(lambda: None, 0)
    del c


def op_backend_read():
    runloom_c.backend()
    runloom_c.netpoll_backend()


def op_mn_cycle():
    # biased-refcount stressor: 2 goroutines hammer incref/decref on a SHARED
    # object across hubs; the cross-thread merge happens at mn_fini.
    runloom_c.mn_init(2)
    shared = ["payload"] * 4
    done = runloom_c.Chan(2)

    def worker():
        acc = 0
        for _ in range(2000):
            ref = shared            # incref/decref the shared list repeatedly
            acc += len(ref)
        done.send(acc)

    runloom_c.mn_fiber(worker)
    runloom_c.mn_fiber(worker)
    runloom_c.mn_run()
    done.recv()
    done.recv()
    runloom_c.mn_fini()


OPS = [
    ("chan_construct", op_chan_construct),
    ("coro_construct", op_coro_construct),
    ("backend_read", op_backend_read),
    ("mn_cycle", op_mn_cycle),
]


def measure(fn):
    for _ in range(WARMUP):
        fn()
        gc.collect()
    r0 = sys.gettotalrefcount()
    b0 = sys.getallocatedblocks()
    dr, db = [], []
    for _ in range(ITERS):
        fn()
        gc.collect()
        r1 = sys.gettotalrefcount()
        b1 = sys.getallocatedblocks()
        dr.append(r1 - r0)
        db.append(b1 - b0)
        r0, b0 = r1, b1
    return dr, db


def steady_nonzero(deltas):
    """A leak: every measured delta after the first is the same nonzero sign."""
    tail = deltas[1:]
    if not tail:
        return False
    if all(d > 0 for d in tail):
        return True
    if all(d < 0 for d in tail):
        return True   # over-release (brc accounting) is a hard fail too
    return False


def main():
    print("refleak_hunt: {0} ({1})".format(sys.executable, sys.version.split()[0]))
    bad = []
    for name, fn in OPS:
        dr, db = measure(fn)
        leak_r = steady_nonzero(dr)
        leak_b = steady_nonzero(db)
        flag = "LEAK" if (leak_r or leak_b) else "ok"
        print("  {0:<16} refcount Δ/iter={1}  blocks Δ/iter={2}  {3}"
              .format(name, dr, db, flag))
        if leak_r or leak_b:
            bad.append(name)
    if bad:
        print("\nrefleak_hunt: steady refcount/alloc drift in: {0} "
              "(positive = leak; negative = over-release / brc-merge accounting bug)"
              .format(", ".join(bad)))
        return 1
    print("\nrefleak_hunt: no steady drift in any hot op")
    return 0


if __name__ == "__main__":
    sys.exit(main())
