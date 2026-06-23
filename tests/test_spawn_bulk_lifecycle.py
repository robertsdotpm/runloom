"""Bulk-spawn batch lifecycle at scale (docs/dev/spawn_experiments.md, Exp B).

Exercises the warm-stack arena + bulk + FRESH fast path that optimize("throughput")
wires: fiber_n(fn, N) builds one big g/coro/stack batch, mn_run() drains all N, and
the batch teardown returns the slots.  Repeated to stress batch reset/reuse.  The
gate runs this under ASan/TSan, so a lifecycle UAF or a lost fiber surfaces here.

Own file = own subprocess (run_isolated), so the low-level mn_init/mn_fini here
never shares runtime state with the high-level runloom.run tests.
"""
import os

# Fast path must be in env before runloom_c caches the gates on first spawn.
os.environ.setdefault("RUNLOOM_STACK_ARENA", "1")
os.environ.setdefault("RUNLOOM_GON_BULK", "1")
os.environ.setdefault("RUNLOOM_GON_FRESH", "1")
os.environ.setdefault("PYTHON_GIL", "0")
# Deterministic (the batch lifecycle is scheduler-independent for this count check).
os.environ.setdefault("RUNLOOM_PREEMPT", "0")
os.environ.setdefault("RUNLOOM_SYSMON", "0")

import runloom_c  # noqa: E402


def test_bulk_large_n_lifecycle_correct():
    N = 20000
    runloom_c.mn_init(8)
    try:
        def worker():
            pass

        prev = 0
        for _ in range(3):                      # stress batch reset/reuse
            runloom_c.fiber_n(worker, N)
            done = runloom_c.mn_run()           # cumulative completed count
            assert done - prev == N, (done - prev, N)
            prev = done
    finally:
        runloom_c.mn_fini()


def test_bulk_small_n_still_correct():
    # Small batches must also drain fully (the arena lazy-inits its size class here).
    runloom_c.mn_init(4)
    try:
        runloom_c.fiber_n(lambda: None, 100)
        assert runloom_c.mn_run() == 100
    finally:
        runloom_c.mn_fini()
