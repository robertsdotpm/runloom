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
    H.go(parallel_sum, H, lo, mid, sub)
    H.go(parallel_sum, H, mid, hi, sub)
    total = 0
    for _ in range(2):
        total += sub.recv()[0]
    result.send(total)


def worker(H, wid, rng, state):
    while H.running():
        hi = rng.randint(2000, 8000)
        expected = seq_sum(0, hi)
        result = runloom.Chan(1)
        H.go(parallel_sum, H, 0, hi, result)
        got = result.recv()[0]
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
