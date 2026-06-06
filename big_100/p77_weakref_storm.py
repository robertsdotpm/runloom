"""big_100 / 77 -- weakref callback storm.

Goroutines create objects with weakref callbacks that "schedule work" (push a
token onto a shared queue), then drop the objects so the callbacks fire during
collection, with periodic gc.collect().  Worker drains the scheduled tokens.
The callbacks must fire, be safe to run during dealloc under M:N, and the
scheduled count must track the fired count.

Stresses: weakref callback reentrancy, callbacks running during GC under M:N.
"""
import _thread as _realthread
import gc
import weakref

import harness
import runloom


class Thing(object):
    __slots__ = ("__weakref__", "n")

    def __init__(self, n):
        self.n = n


def setup(H):
    # Use a real OS lock: weakref callbacks fire inside tp_dealloc (destructor
    # chain) when goroutines call `del things`.  With 100k goroutines all
    # deleting a batch on their final iteration simultaneously, a CoLock here
    # serialises 100k * k ≈ 1.2M acquisitions at cooperative speed (~276/s)
    # → many minutes to drain.  A real OS mutex takes <1µs per acquire and
    # never parks a goroutine or blocks a hub thread.
    H.state = {"lock": _realthread.allocate_lock(), "fired": [0], "created": [0],
               "queue": []}


def worker(H, wid, rng, state):
    lock = state["lock"]
    queue = state["queue"]

    def callback(ref):
        with lock:
            state["fired"][0] += 1
            queue.append(1)             # "schedule work"

    while H.running():
        refs = []
        things = []
        k = rng.randint(4, 20)
        for i in range(k):
            t = Thing(i)
            refs.append(weakref.ref(t, callback))
            things.append(t)
        with lock:
            state["created"][0] += k
        del things                      # drop strong refs -> callbacks fire
        if rng.random() < 0.05:
            gc.collect()
        # Drain a few scheduled tokens (the "work").
        with lock:
            drained = len(queue)
            queue.clear()
        H.op(wid, max(1, drained))
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def gc_driver():
        while H.running():
            H.sleep(0.1)
            gc.collect()

    H.go(gc_driver)


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
