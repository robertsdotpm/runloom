"""big_100 / 48 -- semaphore limiter.

A semaphore with limit K guards a "resource pool".  Thousands of goroutines
acquire it, spend a little time inside, and release.  An observer counter
(guarded by its own lock so it is accurate) tracks how many are inside at once;
it must NEVER exceed K.

Stresses: semaphore correctness under heavy contention.
"""
import threading

import harness
import runloom

LIMIT = 32


def setup(H):
    sem = runloom.sync.Semaphore(LIMIT)

    def _cancel_watcher(r=H.running, s=sem):
        while r():
            runloom.sleep(0.05)
        s.cancel_all()

    H.go(_cancel_watcher)
    H.state = {"sem": sem,
               "active": [0], "lock": threading.Lock(), "maxactive": [0]}


def worker(H, wid, rng, state):
    sem = state["sem"]
    lock = state["lock"]
    active = state["active"]
    while H.running():
        # Explicit acquire so we can detect cancel_all() returning False and
        # exit without touching the active counter (avoids spurious LIMIT breach).
        if not sem.acquire():
            break  # drain started, cancel_all() fired
        if not H.running():
            sem.release()
            break
        try:
            with lock:
                active[0] += 1
                if active[0] > state["maxactive"][0]:
                    state["maxactive"][0] = active[0]
                cur = active[0]
            if not H.check(cur <= LIMIT,
                           "semaphore breached: {0} active > limit {1} "
                           "wid={2}".format(cur, LIMIT, wid)):
                with lock:
                    active[0] -= 1
                return
            runloom.sleep(rng.uniform(0.0, 0.002))
            with lock:
                active[0] -= 1
        finally:
            sem.release()
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.state["active"][0] == 0,
            "active count not back to 0: {0}".format(H.state["active"][0]))
    H.log("max_active={0} (limit {1})".format(H.state["maxactive"][0], LIMIT))


if __name__ == "__main__":
    harness.main("p48_semaphore_limiter", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="active-in-pool count never exceeds the semaphore limit")
