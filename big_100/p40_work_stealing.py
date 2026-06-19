"""big_100 / 40 -- work stealing pressure.

A recursive divide-and-conquer computation with deliberately UNEVEN splits:
each task either does a chunk of work or splits into two unequal sub-tasks
spawned as goroutines.  The skew forces the M:N work-stealing deques to
rebalance.  We verify the parallel result equals the known sequential answer,
so any deque corruption or lost task shows up as a wrong total.

Stresses: M:N scheduling, work stealing, deque correctness.
"""
import harness
import runloom


def seq_sum(lo, hi):
    # sum of i*i for i in [lo, hi)  (closed form not used -- keep it a real loop)
    return sum(i * i for i in range(lo, hi))


def parallel_sum(H, lo, hi, result):
    """Compute sum of i*i over [lo,hi), pushing the partial onto `result`."""
    n = hi - lo
    if n <= 256:
        result.send(seq_sum(lo, hi))
        return
    # Uneven split (1/4 vs 3/4) to stress load balancing.
    mid = lo + max(1, n // 4)
    sub = runloom.Chan(2)
    H.fiber(parallel_sum, H, lo, mid, sub)
    H.fiber(parallel_sum, H, mid, hi, sub)
    total = 0
    for _ in range(2):
        total += sub.recv()[0]
    result.send(total)


# Only a bounded subset of workers exercise the recursive divide-and-conquer
# tree.  If all 1M workers did, each would spawn a tree of sub-goroutines and the
# multiplicative blow-up is tens of millions of live goroutines -> instant OOM,
# and even bounded it would leave millions live at drain.  A few thousand trees
# is plenty to stress the work-stealing deques + verify correctness; the rest of
# the 1M pool stays as real cooperative load (each computes the sum directly).
PARALLEL_WORKERS = 4096


def worker(H, wid, rng, state):
    # No startup stagger: parking 1M goroutines on a timer just to wake them
    # again is itself slow at this scale, and with only PARALLEL_WORKERS trees
    # there is no t=0 spawn storm to stagger away from.
    do_parallel = wid < PARALLEL_WORKERS
    while H.running():
        # Modest range: still always > 256 so the parallel path always splits
        # (exercising work-stealing), but light enough that 1M CPU-oversubscribed
        # goroutines actually complete iterations (a heavier sum starves them all
        # to ops=0 -- a hollow "survival" pass).
        hi = rng.randint(300, 1024)
        expected = seq_sum(0, hi)
        if do_parallel:
            result = runloom.Chan(1)
            H.fiber(parallel_sum, H, 0, hi, result)
            got = result.recv()[0]
        else:
            got = expected               # 1M-goroutine load, no sub-tree
        if not H.check(got == expected,
                       "work-stealing wrong sum wid={0}: {1} != {2}".format(
                           wid, got, expected)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p40_work_stealing", body, default_funcs=500,
                 describe="uneven divide-and-conquer; parallel sum must match")
