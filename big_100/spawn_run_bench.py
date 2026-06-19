"""1M-coroutine spawn + RUN to completion: correctness + where the moved cost lands.

go_n(noop, N) then mn_run() (drains all N on the hubs).  mn_run returns the
completed count -- assert == N proves the deferred-stack path actually runs.
Compare RUNLOOM_GON_FRESH=0 (frames written at spawn) vs =1 (frames written
lazily on the owning hub at first resume, in parallel across 8 hubs).

Run with: RUNLOOM_GON_BULK=1 [RUNLOOM_GON_FRESH=0|1]   (NO nosubmit here)
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import runloom_c

N = int(os.environ.get("SPAWN_N", "1000000"))


def noop():
    pass


def main():
    runloom_c.mn_init(8)
    fresh = os.environ.get("RUNLOOM_GON_FRESH", "0")
    bulk = os.environ.get("RUNLOOM_GON_BULK", "0")
    print("N={0} bulk={1} fresh={2} hubs=8  (spawn + run to completion)".format(N, bulk, fresh))

    t0 = time.monotonic()
    runloom_c.fiber_n(noop, N)
    t_spawn = time.monotonic() - t0

    t1 = time.monotonic()
    done = runloom_c.mn_run()
    t_run = time.monotonic() - t1

    total = time.monotonic() - t0
    ok = "OK" if done == N else "!!! MISMATCH"
    print("  spawn : {0:.3f}s".format(t_spawn))
    print("  run   : {0:.3f}s".format(t_run))
    print("  TOTAL : {0:.3f}s   ({1:.0f}k g/s)".format(total, N / total / 1000))
    print("  completed: {0}/{1}  {2}".format(done, N, ok))
    runloom_c.mn_fini()


if __name__ == "__main__":
    main()
