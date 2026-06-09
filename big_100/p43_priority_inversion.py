"""big_100 / 43 -- priority inversion simulator.

A single shared lock.  "Low priority" goroutines grab it and hold it for a
while (long critical sections); "high priority" goroutines want it briefly and
measure how long they waited.  runloom has no real priorities, so this checks
the lock does not let waiters starve unboundedly -- a high-priority acquire
must always complete within a generous bound.

Stresses: lock acquire/release fairness, blocking waiters, the wait queue.
"""
import threading
import time

import harness
import runloom

WAIT_BOUND = 30.0       # a high-pri acquire slower than this == pathological

# At 100k goroutines, drain time = N / lock-throughput.  Lock throughput is
# ~100/s (hold_avg=10ms), so 100k goroutines = 1000s drain -- far past the
# 120s limit.  max_concurrent caps goroutines so only MAX_CONTENDERS compete
# per pool; total drain stays well within bounds.
MAX_CONTENDERS = 2000


def setup(H):
    H.state = {"lock": runloom.sync.Lock(), "maxwait": [0.0] * 1024}


def low_pri(H, wid, rng, state):
    lock = state["lock"]
    while H.running():
        with lock:
            runloom.sleep(rng.uniform(0.001, 0.02))
        H.op(wid)
        runloom.yield_now()


def high_pri(H, wid, rng, state):
    lock = state["lock"]
    while H.running():
        t0 = time.perf_counter()
        with lock:
            waited = time.perf_counter() - t0
            if waited > state["maxwait"][wid & 1023]:
                state["maxwait"][wid & 1023] = waited
            if not H.check(waited < WAIT_BOUND,
                           "high-pri starved {0:.1f}s waiting for lock "
                           "wid={1}".format(waited, wid)):
                return
        H.op(wid)
        H.task_done(wid)
        runloom.sleep(rng.uniform(0.0, 0.002))


def body(H):
    lows = H.funcs // 2
    highs = H.funcs - lows
    H.run_pool(lows, low_pri, H.state)
    H.run_pool(highs, high_pri, H.state)


def post(H):
    H.log("max_high_pri_wait={0:.3f}s".format(max(H.state["maxwait"])))


if __name__ == "__main__":
    harness.main("p43_priority_inversion", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="long lock holders must not starve short waiters")
