"""big_100 / 41 -- single hot producer, many consumers.

One producer goroutine pushes a stream of numbered items onto a bounded
channel; a large pool of consumers drain it.  We verify conservation: the sum
of everything consumed equals the sum of everything produced (nothing dropped,
nothing duplicated).

Stresses: channel fairness, consumer wake-ups, producer/consumer contention.
"""
import threading

import harness
import runloom


def setup(H):
    H.state = {"ch": runloom.Chan(8192),
               "produced_sum": [0], "produced_n": [0],
               "consumed_sum": [0] * 1024, "consumed_n": [0] * 1024,
               "plock": threading.Lock()}


def producer(H, state):
    ch = state["ch"]
    val = 1
    s = 0
    n = 0
    while H.running():
        ch.send(val)
        s += val
        n += 1
        val = val + 1 if val < 1000 else 1
    state["produced_sum"][0] = s
    state["produced_n"][0] = n
    # Close AFTER the last send: consumers then drain every buffered item and
    # see ok=False, so the conservation count is exact (no teardown race).
    ch.close()
    H.log("producer done: n={0} sum={1}".format(n, s))


def consumer(H, wid, rng, state):
    ch = state["ch"]
    s = 0
    n = 0
    while True:
        val, ok = ch.recv()        # blocks; returns ok=False once closed+empty
        if not ok:
            break
        s += val
        n += 1
        H.op(wid)
    state["consumed_sum"][wid & 1023] += s
    state["consumed_n"][wid & 1023] += n


def body(H):
    H.go(producer, H, H.state)
    H.run_pool(H.funcs, consumer, H.state)


def finish_check(H):
    ps = H.state["produced_sum"][0]
    cs = sum(H.state["consumed_sum"])
    pn = H.state["produced_n"][0]
    cn = sum(H.state["consumed_n"])
    H.check(ps == cs and pn == cn,
            "conservation violated: produced (n={0},sum={1}) != consumed "
            "(n={2},sum={3})".format(pn, ps, cn, cs))
    H.log("produced n={0} sum={1} | consumed n={2} sum={3}".format(
        pn, ps, cn, cs))


if __name__ == "__main__":
    harness.main("p41_producer_consumers", body, setup=setup,
                 post=finish_check, default_funcs=2000,
                 describe="one producer, many consumers; conservation")
