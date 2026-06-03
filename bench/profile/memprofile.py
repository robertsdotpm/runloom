"""Memory + stack-footprint profiling for pygo goroutines.

Three lenses, all from stdlib + pygo_core's own introspection -- no external
tools, so it runs anywhere the ext builds:

  1. stack HWM  -- pygo *paints* goroutine stacks, so pygo_core.stats()
     ['stack_hwm'] is the high-water mark actually touched. Tells us whether
     the 32 KB default is over- or under-provisioned for a given Python call
     depth (ties to F4 and the stack-sizing strategy in docs/).
  2. RSS sweep  -- resource maxrss across a spawn-count sweep: the resident
     cost of N live goroutine stacks (F4).
  3. tracemalloc -- Python-object allocation on a hot path (e.g. the per-recv
     (v, ok) tuple in F6).

Usage:
    PYTHONPATH=src python3 -m bench.profile.memprofile [all|hwm|rss|alloc]
"""
import resource
import sys
import tracemalloc

import pygo_core

from bench.gil import ensure_nogil
from bench.harness import default_pin_set, pin


def rss_mb():
    # ru_maxrss is KiB on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def recurse_then_yield(depth):
    """Recurse to `depth` nested Python frames, then yield AT THE BOTTOM.

    pygo's stack paint-sweep runs at swap-out, so the goroutine must be
    suspended while its stack is deep for the high-water mark to capture the
    real usage -- a synchronous recurse-and-return is back to a shallow
    frame before any sweep sees it (that is why an un-yielded recursion
    reports a flat ~1 KB).
    """
    if depth > 0:
        return recurse_then_yield(depth - 1)
    pygo_core.sched_yield()
    return 0


def stack_hwm():
    """How much of the goroutine stack does real Python recursion touch?

    Spawn several goroutines that recurse to increasing depth and park at the
    bottom; the round-robin swap-out paints each deep stack so stats()
    ['stack_hwm'] reflects the deepest usage.  Reports HWM vs the 32 KB
    default budget and the implied C-stack cost per nested Python frame.
    """
    print("== stack high-water mark vs Python recursion depth (yield-at-depth) ==")
    size = pygo_core.get_stack_size()
    print("default goroutine stack: %d bytes (%d KB)" % (size, size // 1024))
    print("  %6s  %10s  %8s  %12s" % ("depth", "stack_hwm", "% of 32K", "headroom"))
    points = {}
    for depth in (0, 10, 50, 100, 200, 400, 800):
        pygo_core.sched_reset()

        def worker(d=depth):
            recurse_then_yield(d)
        # >=2 goroutines so yields actually swap (and paint) rather than
        # idle the run loop.
        for _ in range(8):
            pygo_core.go(worker)
        pygo_core.run()
        hwm = pygo_core.stats()["stack_hwm"]
        points[depth] = hwm
        pct = 100.0 * hwm / size if size else 0.0
        print("  %6d  %10d  %7.1f%%  %9d B" % (depth, hwm, pct, size - hwm))
    print("  NB: Python depth barely moves the C-stack HWM -- on CPython 3.11+")
    print("      Python frames live on the heap data-stack, not the C stack.")
    print("      The goroutine C-stack budget is spent by C-level recursion:")
    c_recursion(size)


def c_recursion(size):
    """Force recursion that DOES grow the C stack: json.dumps of a list
    nested to depth d recurses in the C encoder.  Run it then park, so the
    overwritten paint is swept and stack_hwm captures the C depth -- the
    real consumer of a goroutine's stack (cf. the aio bridge's 512 KB
    _IO_STACK for protocols that recurse into OpenSSL/asyncssh)."""
    import json
    print("  -- C-level recursion (json.dumps of depth-d nested list) --")
    print("  %6s  %10s  %8s  %14s" % ("c-depth", "stack_hwm", "% of 32K", "B/level"))
    # Stay well below the overflow boundary: ~188 B/level => the 32 KB stack
    # SEGVs (guard page, no Python traceback) around depth ~180. Do NOT raise
    # these past ~120 or the driver itself crashes -- which is the finding.
    base = None
    for depth in (0, 25, 50, 75, 100, 120):
        nested = []
        cur = nested
        for _ in range(depth):
            nxt = []
            cur.append(nxt)
            cur = nxt
        pygo_core.sched_reset()

        def worker(d=nested):
            json.dumps(d)
            pygo_core.sched_yield()
        for _ in range(8):
            pygo_core.go(worker)
        pygo_core.run()
        hwm = pygo_core.stats()["stack_hwm"]
        if base is None:
            base = hwm
        per = (hwm - base) / depth if depth else 0.0
        print("  %6d  %10d  %7.1f%%  %12.0f" % (depth, hwm, 100.0 * hwm / size, per))
    print("  => C recursion ~%.0f B/level; 32 KB overflows (SEGV) near depth"
          " ~%d." % (per, int(size / per) if per else 0))
    print("     This is why the aio bridge uses a 512 KB _IO_STACK for")
    print("     callbacks that recurse into C (OpenSSL/asyncssh).")


def rss_sweep():
    print("== resident memory vs live goroutine count ==")
    print("  %8s  %10s  %14s" % ("N", "RSS (MB)", "KB/goroutine"))
    base = rss_mb()
    for n in (1000, 5000, 10000, 20000):
        pygo_core.sched_reset()
        def noop():
            pass
        for _ in range(n):
            pygo_core.go(noop)
        pygo_core.run()
        rss = rss_mb()
        print("  %8d  %10.1f  %14.2f" % (n, rss, (rss - base) * 1024.0 / n))


def alloc():
    print("== tracemalloc: Python allocation on the chan ping-pong path ==")
    from bench.micro import make_pingpong
    run = make_pingpong(20_000)
    run()  # warm
    tracemalloc.start()
    snap0 = tracemalloc.take_snapshot()
    run()
    snap1 = tracemalloc.take_snapshot()
    tracemalloc.stop()
    stats = snap1.compare_to(snap0, "lineno")
    for s in stats[:8]:
        print("  %s" % s)


def main(argv=None):
    ensure_nogil()
    which = (argv or sys.argv[1:] or ["all"])[0]
    pin(default_pin_set(n=8, node=1))
    print("pygo %s/%s  gil=%s\n"
          % (pygo_core.backend(), pygo_core.netpoll_backend(),
             getattr(sys, "_is_gil_enabled", lambda: True)()))
    if which in ("all", "hwm"):
        stack_hwm(); print()
    if which in ("all", "rss"):
        rss_sweep(); print()
    if which in ("all", "alloc"):
        alloc()


if __name__ == "__main__":
    main()
