"""big_100 / 63 -- cancelled DNS lookup.

Goroutines start getaddrinfo lookups (offloaded to the worker pool) in a child
goroutine and race the result against a short cancel timer.  When the timer
wins, the parent abandons the lookup and discards whatever the child eventually
delivers.  The disposed results must not crash or leak.

Stresses: getaddrinfo offload integration and result disposal on cancel.
Fully offline (numeric + localhost + fast-failing names).
"""
import socket

import harness
import cancelutil
import runloom

NAMES = ["127.0.0.1", "::1", "localhost", "10.1.2.3", "192.0.2.7",
         "not-numeric-host", "8.8.8.8"]


def do_lookup(host, out):
    try:
        flags = 0 if host == "localhost" else socket.AI_NUMERICHOST
        res = socket.getaddrinfo(host, 80, socket.AF_UNSPEC,
                                 socket.SOCK_STREAM, 0, flags)
        out.send(("ok", len(res)))
    except Exception as exc:                # noqa: BLE001
        out.send(("err", type(exc).__name__))


def worker(H, wid, rng, state):
    while H.running():
        host = rng.choice(NAMES)
        out = runloom.Chan(1)               # buffered: child can always deliver
        H.go(do_lookup, host, out)
        # Race the result against a tiny cancel window.
        ctx, cancel = cancelutil.WithTimeout(cancelutil.Background(),
                                             rng.uniform(0.0001, 0.005))
        got = cancelutil.cancellable_recv(ctx, out)
        cancel()
        if got is None:
            state["cancelled"][wid & 1023] += 1
            # The child will still deliver into the buffered channel; that
            # result is simply discarded when `out` is dropped.
        else:
            state["completed"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"cancelled": [0] * 1024, "completed": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("completed={0} cancelled={1}".format(
        sum(H.state["completed"]), sum(H.state["cancelled"])))


if __name__ == "__main__":
    harness.main("p63_cancelled_dns", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="start DNS lookups, cancel some, discard results safely")
