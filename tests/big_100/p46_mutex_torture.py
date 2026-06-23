"""big_100 / 46 -- mutex torture.

Thousands of goroutines hammer a single shared counter, each increment guarded
by one lock.  At the end the locked counter must exactly equal the total number
of increments performed (tracked independently per goroutine) -- a single lost
update means the lock dropped a critical section.

Stresses: lock acquire/release, blocking waiters, exactness under contention.
"""
import threading

import harness


def setup(H):
    H.state = {"counter": [0], "lock": threading.Lock()}


def worker(H, wid, rng, state):
    counter = state["counter"]
    lock = state["lock"]
    while H.running():
        with lock:
            counter[0] += 1
        H.op(wid)               # independent per-goroutine increment tally


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    locked = H.state["counter"][0]
    tally = H.total_ops()
    H.check(locked == tally,
            "lost updates: locked counter {0} != tally {1} (lost {2})".format(
                locked, tally, tally - locked))
    H.log("locked_counter={0} independent_tally={1}".format(locked, tally))


if __name__ == "__main__":
    harness.main("p46_mutex_torture", body, setup=setup, post=post,
                 default_funcs=5000,
                 describe="locked counter must equal total increments exactly")
