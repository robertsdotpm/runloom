"""Pure 1M-coroutine STARTUP cost (no run).

Times runloom_c.fiber_n(noop, N) with RUNLOOM_GON_NOSUBMIT=1: the bulk path builds
the g-arena + coro-arena (+ stacks) but never publishes to the hubs, so nothing
runs.  This isolates creation cost.  Compare RUNLOOM_GON_FRESH=0 (eager
asm_make_ctx -> 1M stack-top faults on this thread) vs =1 (deferred to first
resume on the hubs -> those faults skipped entirely here, since we never run).

Run with: RUNLOOM_GON_BULK=1 RUNLOOM_GON_NOSUBMIT=1 [RUNLOOM_GON_FRESH=0|1]
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import runloom_c

N = int(os.environ.get("SPAWN_N", "1000000"))
REPS = int(os.environ.get("SPAWN_REPS", "3"))


def noop():
    pass


def main():
    runloom_c.mn_init(8)
    fresh = os.environ.get("RUNLOOM_GON_FRESH", "0")
    nosub = os.environ.get("RUNLOOM_GON_NOSUBMIT", "0")
    bulk = os.environ.get("RUNLOOM_GON_BULK", "0")
    print("N={0} bulk={1} nosubmit={2} fresh={3} hubs=8".format(N, bulk, nosub, fresh))
    best = 1e9
    for r in range(REPS):
        t0 = time.monotonic()
        runloom_c.fiber_n(noop, N)
        dt = time.monotonic() - t0
        best = min(best, dt)
        print("  rep {0}: {1:.3f}s  ({2:.0f}k spawn/s)".format(r, dt, N / dt / 1000))
    print("  BEST: {0:.3f}s  ({1:.0f}k spawn/s)".format(best, N / best / 1000))
    runloom_c.mn_fini()


if __name__ == "__main__":
    main()
