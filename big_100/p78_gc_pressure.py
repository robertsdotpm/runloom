"""big_100 / 78 -- GC under scheduler pressure.

Tens of thousands of goroutines churn out reference cycles (only the cyclic GC
can reclaim them) while a driver goroutine calls gc.collect() aggressively and
other goroutines keep running.  The collector runs concurrently with allocation
and with the scheduler swapping goroutine stacks; it must not crash or corrupt
object state.

Stresses: object lifetime, PyThreadState safety, stop-the-world GC under M:N.
"""
import gc
import threading

import harness
import runloom


class Node(object):
    __slots__ = ("nxt", "payload")

    def __init__(self, payload):
        self.nxt = None
        self.payload = payload


def setup(H):
    H.state = {"lock": threading.Lock(), "collections": [0]}


def worker(H, wid, rng, state):
    while H.running():
        # Build a ring of nodes (a reference cycle), touch it, then drop it.
        k = rng.randint(4, 24)
        nodes = [Node(i) for i in range(k)]
        for i in range(k):
            nodes[i].nxt = nodes[(i + 1) % k]
        total = 0
        cur = nodes[0]
        for _ in range(k):
            total += cur.payload
            cur = cur.nxt
        if not H.check(total == k * (k - 1) // 2,
                       "cycle walk wrong wid={0}".format(wid)):
            return
        del nodes, cur
        if rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def gc_driver():
        while H.running():
            n = gc.collect()
            with H.state["lock"]:
                H.state["collections"][0] += 1
            H.sleep(0.05)
        H.log("gc_collect_calls={0}".format(H.state["collections"][0]))

    H.go(gc_driver)


if __name__ == "__main__":
    harness.main("p78_gc_pressure", body, setup=setup, default_funcs=4000,
                 describe="cyclic garbage + aggressive gc.collect() under M:N")
