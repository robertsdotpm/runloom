"""big_100 / 53 -- rendezvous channel.

An unbuffered (capacity 0) channel: every send must hand off directly to a
receiver, with no buffering in between.  Senders push unique values; receivers
collect them.  The sum of values successfully handed off must equal the sum
received -- exact pairing, no value lost or duplicated.

Stresses: unbuffered send/recv pairing correctness, wake handoff.
"""
import threading

import harness
import runloom


def setup(H):
    H.state = {"ch": runloom.Chan(0), "lock": threading.Lock(),
               "sent_sum": [0], "recv_sum": [0] * 1024,
               "sender_done": [0], "nsenders": [0]}


def sender(H, wid, rng, state):
    ch = state["ch"]
    s = 0
    base = wid * 1000003
    i = 0
    try:
        while H.running():
            v = base + i
            ch.send(v)              # blocks until a receiver takes it
            s += v
            i += 1
            H.op(wid)
    except Exception:
        pass                        # channel closed mid-send -> not delivered
    with state["lock"]:
        state["sent_sum"][0] += s
        state["sender_done"][0] += 1


def receiver(H, wid, rng, state):
    ch = state["ch"]
    s = 0
    while True:
        v, ok = ch.recv()
        if not ok:
            break
        s += v
    state["recv_sum"][wid & 1023] += s


def body(H):
    senders = H.funcs // 2
    receivers = H.funcs - senders
    state = H.state
    state["nsenders"][0] = senders
    H.run_pool(senders, sender, state)
    H.run_pool(receivers, receiver, state)

    def closer():
        while H.running():
            H.sleep(0.1)
        import time
        until = time.monotonic() + 30
        # Wait for senders to stop, then close so receivers (and any sender
        # parked on the unbuffered send) wake.
        while state["sender_done"][0] < senders and time.monotonic() < until:
            H.sleep(0.02)
        try:
            state["ch"].close()
        except Exception:
            pass

    H.fiber(closer)


def post(H):
    sent = H.state["sent_sum"][0]
    recv = sum(H.state["recv_sum"])
    H.check(sent == recv,
            "rendezvous mismatch: handed-off sum {0} != received {1}".format(
                sent, recv))
    H.log("handoff_sum={0} received_sum={1}".format(sent, recv))


if __name__ == "__main__":
    harness.main("p53_rendezvous", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="unbuffered channel exact send/recv value pairing")
