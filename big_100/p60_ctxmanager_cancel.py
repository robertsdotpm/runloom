"""big_100 / 60 -- context manager cancellation.

A custom context manager acquires a pooled resource on __enter__ and releases
it on __exit__.  Goroutines do a cancellable wait inside the `with` block; when
cancelled, an exception propagates out of the block -- and __exit__ must still
run, returning the resource.  The live-resource count must never go negative
and must return to zero.

Stresses: __exit__ under exception/cancellation, resource accounting.
"""
import threading

import harness
import cancelutil


class Cancelled(Exception):
    pass


class Resource(object):
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        with self.state["lock"]:
            self.state["active"][0] += 1
            if self.state["active"][0] > self.state["peak"][0]:
                self.state["peak"][0] = self.state["active"][0]
        return self

    def __exit__(self, exc_type, exc, tb):
        with self.state["lock"]:
            self.state["active"][0] -= 1
            self.state["released"][0] += 1
        return False                # never swallow the exception


def setup(H):
    H.state = {"lock": threading.Lock(), "active": [0], "peak": [0],
               "acquired": [0], "released": [0]}


def worker(H, wid, rng, state):
    while H.running():
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        if rng.random() < 0.5:
            H.go(cancelutil.delayed_cancel, cancel, rng.uniform(0.0, 0.008))
        try:
            with Resource(state):
                with state["lock"]:
                    state["acquired"][0] += 1
                if not cancelutil.cancellable_sleep(ctx, rng.uniform(0.0, 0.02)):
                    raise Cancelled()       # cancel INSIDE the with block
        except Cancelled:
            pass
        finally:
            cancel()
        if not H.check(state["active"][0] >= 0,
                       "resource count went negative wid={0}".format(wid)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.state["active"][0] == 0,
            "resources leaked: {0} still active".format(H.state["active"][0]))
    H.check(H.state["acquired"][0] == H.state["released"][0],
            "acquire/release imbalance: {0} acquired, {1} released".format(
                H.state["acquired"][0], H.state["released"][0]))
    H.log("peak_active={0} acquired={1} released={2}".format(
        H.state["peak"][0], H.state["acquired"][0], H.state["released"][0]))


if __name__ == "__main__":
    harness.main("p60_ctxmanager_cancel", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="__exit__ always runs even when cancelled inside `with`")
