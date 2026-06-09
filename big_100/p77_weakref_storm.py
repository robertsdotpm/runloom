"""big_100 / 77 -- weakref callback storm.

Goroutines create objects with weakref callbacks that "schedule work" (push a
token onto a shared queue), then drop the objects so the callbacks fire during
collection, with periodic gc.collect().  Worker drains the scheduled tokens.
The callbacks must fire, be safe to run during dealloc under M:N, and the
scheduled count must track the fired count.

Stresses: weakref callback reentrancy, callbacks running during GC under M:N.
"""
import gc
import threading
import weakref

import harness
import runloom


class Thing(object):
    __slots__ = ("__weakref__", "n")

    def __init__(self, n):
        self.n = n


# At 100k goroutines, all calling del things simultaneously, 100k weakref
# callbacks try to acquire the same CoLock => drain takes hours.
# max_concurrent=MAX_WORKERS spawns only MAX_WORKERS goroutines, each looping
# -- no CoSemaphore needed (which would create one pipe-pair per waiting
# goroutine and blow the FD limit at 1M funcs).
# Note: _thread.allocate_lock is patched to CoLock by monkey.patch(), so
# we use threading.Lock() directly (also a CoLock after patching).
MAX_WORKERS = 2000


def setup(H):
    H.state = {"lock": threading.Lock(), "fired": [0], "created": [0],
               "queue": []}


def worker(H, wid, rng, state):
    lock = state["lock"]
    queue = state["queue"]

    def callback(ref):
        with lock:
            state["fired"][0] += 1
            queue.append(1)             # "schedule work"

    while H.running():
        things = []
        refs = []
        k = rng.randint(4, 20)
        for i in range(k):
            t = Thing(i)
            refs.append(weakref.ref(t, callback))
            things.append(t)
        with lock:
            state["created"][0] += k
        del things                  # drop strong refs -> callbacks fire
        if rng.random() < 0.05:
            gc.collect()
        with lock:
            drained = len(queue)
            queue.clear()
        H.op(wid, max(1, drained))
        H.task_done(wid)


def body(H):
    def gc_driver():
        while H.running():
            H.sleep(0.1)
            gc.collect()

    H.go(gc_driver)
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_WORKERS)


def post(H):
    gc.collect()
    created = H.state["created"][0]
    fired = H.state["fired"][0]
    H.check(fired > 0, "no weakref callbacks fired")
    H.check(fired <= created,
            "more callbacks ({0}) than objects ({1})".format(fired, created))
    H.log("created={0} callbacks_fired={1}".format(created, fired))


if __name__ == "__main__":
    harness.main("p77_weakref_storm", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="weakref callbacks schedule work; survive GC reentrancy")
