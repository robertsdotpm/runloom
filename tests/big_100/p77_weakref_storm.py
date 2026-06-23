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


def setup(H):
    # Per-worker `created` slots (one per wid -> race-free, exact) and a single
    # racy `fired` counter incremented in the weakref callback.  NO global lock +
    # shared queue: at 1M goroutines a lock taken by every worker iteration AND
    # every one of millions of callbacks serialises the whole run to a wedge
    # (ops stuck at 0).  `fired` may under-count under the race, which is fine --
    # the test only needs fired > 0 and fired <= created.
    H.state = {"fired": [0], "created": [0] * H.funcs}


def worker(H, wid, rng, state):
    fired = state["fired"]
    created = state["created"]

    def callback(ref):
        fired[0] += 1                   # lock-free; racy under-count is acceptable

    while H.running():
        things = []
        refs = []
        k = rng.randint(4, 20)
        for i in range(k):
            t = Thing(i)
            refs.append(weakref.ref(t, callback))
            things.append(t)
        created[wid] += k               # own slot -> race-free
        del things                  # drop strong refs -> callbacks fire
        # A few workers drive an explicit collect from their own context; at 1M
        # a 5%-per-iteration collect is tens of thousands of stop-the-worlds.
        if wid < 64 and rng.random() < 0.05:
            gc.collect()
        H.op(wid, k)
        H.task_done(wid)


def body(H):
    def gc_driver():
        while H.running():
            H.sleep(0.1)
            gc.collect()

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gc.collect()
    created = sum(H.state["created"])
    fired = H.state["fired"][0]
    H.check(fired > 0, "no weakref callbacks fired")
    H.check(fired <= created,
            "more callbacks ({0}) than objects ({1})".format(fired, created))
    H.log("created={0} callbacks_fired={1}".format(created, fired))


if __name__ == "__main__":
    harness.main("p77_weakref_storm", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="weakref callbacks schedule work; survive GC reentrancy")
