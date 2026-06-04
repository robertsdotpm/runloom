"""big_100 / 47 -- condition variable storm.

A shared work list guarded by a Condition.  Producers append items and notify;
consumers wait on the condition until work is available.  Notifiers randomly
use notify() (one) or notify_all().  A lost wakeup would leave a consumer
asleep while work sits in the list -- caught as items produced but never
consumed.

Stresses: Condition wait/notify, lost-wakeup correctness.
"""
import threading

import harness
import runloom


def setup(H):
    H.state = {"cond": threading.Condition(), "work": [],
               "produced": [0], "consumed": [0] * 1024, "done": [False]}


def producer(H, wid, rng, state):
    cond = state["cond"]
    work = state["work"]
    p = 0
    while H.running():
        with cond:
            work.append((wid, p))
            if rng.random() < 0.5:
                cond.notify()
            else:
                cond.notify_all()
        p += 1
        H.op(wid)
        if rng.random() < 0.1:
            runloom.yield_now()
    with cond:
        state["produced"][0] += p


def consumer(H, wid, rng, state):
    cond = state["cond"]
    work = state["work"]
    got = 0
    while True:
        with cond:
            while not work and not state["done"][0]:
                cond.wait()
            if work:
                work.pop()
                got += 1
            elif state["done"][0]:
                break
        H.op(wid)
    state["consumed"][wid & 1023] += got


def body(H):
    half = H.funcs // 2
    H.run_pool(half, producer, H.state)
    H.run_pool(H.funcs - half, consumer, H.state)

    def stopper():
        # When the run ends, flip done and wake every waiter so consumers can
        # drain the remaining work and exit (no lost wakeup at teardown).
        while H.running():
            H.sleep(0.1)
        cond = H.state["cond"]
        with cond:
            state_done = H.state["done"]
            state_done[0] = True
            cond.notify_all()
        # Keep nudging in case a late producer added work after the flip.
        for _ in range(20):
            with cond:
                cond.notify_all()
            H.sleep(0.02)

    H.go(stopper)


def post(H):
    produced = H.state["produced"][0]
    consumed = sum(H.state["consumed"])
    leftover = len(H.state["work"])
    H.check(produced == consumed + leftover,
            "condition bookkeeping off: produced={0} consumed={1} "
            "leftover={2}".format(produced, consumed, leftover))
    # A correct teardown drains everything: leftover should be 0.
    H.check(leftover == 0,
            "lost wakeup: {0} items left unconsumed".format(leftover))
    H.log("produced={0} consumed={1} leftover={2}".format(
        produced, consumed, leftover))


if __name__ == "__main__":
    harness.main("p47_condition_storm", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="condition wait/notify storm; no lost wakeups")
