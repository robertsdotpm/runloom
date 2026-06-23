"""big_100 / 52 -- bounded queue backpressure.

A tiny channel (capacity 2) with many producers and a few SLOW consumers.  The
producers spend most of their time blocked in send waiting for room; they must
resume correctly each time a consumer makes space, and every produced item must
eventually be consumed.

Stresses: blocking put/get, backpressure, producer wake-ups.
"""
import threading

import harness
import runloom


def setup(H):
    # produced/consumed: one slot per goroutine (indexed by wid) — no two
    # goroutines share a slot, eliminating the data race under GIL=0.
    H.state = {"ch": runloom.Chan(2), "lock": threading.Lock(),
               "prod_done": [0], "produced": [0] * H.funcs,
               "consumed": [0] * H.funcs, "nproducers": [0]}


def producer(H, wid, rng, state):
    ch = state["ch"]
    n = 0
    while H.running():
        try:
            ch.send(1)          # blocks while the tiny buffer is full
        except ValueError as e:
            # The teardown closer closes the channel once producers SHOULD have
            # stopped, but at an extreme producer:consumer ratio (e.g. 1M:8 at
            # --funcs 1000000) its bounded drain window can elapse while this
            # producer is still BLOCKED in send.  Closing a channel with blocked
            # senders raises "send on closed channel" (Go semantics) -- the
            # correct stop signal from the sender side, not an error: the unsent
            # item was never enqueued, so conservation (produced == consumed)
            # still holds.  Stop cleanly.
            if "closed" not in str(e):
                raise
            break
        n += 1
        H.op(wid)
    state["produced"][wid] += n
    with state["lock"]:
        state["prod_done"][0] += 1


def consumer(H, wid, rng, state):
    ch = state["ch"]
    n = 0
    while True:
        val, ok = ch.recv()
        if not ok:
            break
        n += 1
        runloom.sleep(0.001)        # slow consumer
    state["consumed"][wid] += n


def body(H):
    consumers = max(2, H.hubs)
    producers = H.funcs - consumers
    state = H.state
    state["nproducers"][0] = producers
    H.run_pool(producers, producer, state)
    H.run_pool(consumers, consumer, state)

    def closer():
        # Close the channel once every producer has stopped, so the slow
        # consumers drain the last items and exit cleanly (conservation exact).
        while H.running():
            H.sleep(0.1)
        import time
        until = time.monotonic() + 30
        while state["prod_done"][0] < producers and time.monotonic() < until:
            H.sleep(0.02)
        try:
            state["ch"].close()
        except Exception:
            pass

    H.fiber(closer)


def post(H):
    produced = sum(H.state["produced"])
    consumed = sum(H.state["consumed"])
    H.check(produced == consumed,
            "backpressure lost items: produced={0} consumed={1}".format(
                produced, consumed))
    H.log("produced={0} consumed={1}".format(produced, consumed))


if __name__ == "__main__":
    harness.main("p52_bounded_backpressure", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="tiny buffer, many producers, slow consumers; no loss")
