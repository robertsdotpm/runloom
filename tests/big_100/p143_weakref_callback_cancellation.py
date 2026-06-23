"""big_100 / 143 -- weakref callbacks doing scheduler-ish work.

Objects carry weakref.ref(obj, callback) callbacks that do scheduler-adjacent
work from inside the GC dealloc path: close an os.pipe fd, set a runloom Event,
and bump a per-worker counter.  Goroutines churn create/drop them with periodic
gc.collect() under load.  The reentrant scheduler-ish call (Event.set) from a
weakref callback running during collection must not crash or deadlock, and
forward progress must continue.

Stresses: weakref callback reentrancy, a runloom Event set + an fd close from a
GC callback under M:N.
"""
import gc
import os
import weakref

import harness
import runloom


class Thing(object):
    __slots__ = ("__weakref__", "n")

    def __init__(self, n):
        self.n = n


def setup(H):
    # Per-worker `fired` and `closed` slots (single-writer-per-slot -> race-free,
    # exact).  A shared runloom Event the callbacks set: setting an already-set
    # Event is the reentrant scheduler-touch we want to exercise from the dealloc
    # path; we re-create/clear it periodically so it actually transitions.
    H.state = {
        "fired": [0] * H.funcs,
        "closed": [0] * H.funcs,
        "events_set": [0],          # racy aggregate, only needs >0
        "ev": runloom.sync.Event(),
    }


def worker(H, wid, rng, state):
    fired = state["fired"]
    closed = state["closed"]
    events_set = state["events_set"]
    ev = state["ev"]

    def make_callback(rfd, wfd):
        # The weakref callback runs while the Thing is being collected.  It does
        # scheduler-ish work: set a runloom Event (a real scheduler primitive
        # reentered from the dealloc path) and close two real fds, then count.
        def callback(ref):
            try:
                ev.set()                     # reentrant scheduler-touch
                events_set[0] += 1           # racy aggregate, fine
            except Exception:
                pass
            for fd in (rfd, wfd):
                try:
                    os.close(fd)
                    closed[wid] += 1
                except OSError:
                    pass
            fired[wid] += 1                   # own slot -> race-free
        return callback

    for _ in H.round_range():
        things = []
        refs = []
        k = rng.randint(4, 16)
        for _i in range(k):
            t = Thing(_i)
            r, w = os.pipe()
            refs.append(weakref.ref(t, make_callback(r, w)))
            things.append(t)
        del things                          # drop strong refs -> callbacks fire
        if wid < 64 and rng.random() < 0.03:
            gc.collect()
        # Occasionally clear the Event so set() from the next batch's callbacks
        # is a real 0->1 transition (waking a parked waiter), not a no-op.
        if (wid & 63) == 0 and rng.random() < 0.1:
            ev.clear()
        H.op(wid, k)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def body(H):
    state = H.state

    # A waiter goroutine that repeatedly waits on the shared Event and clears it,
    # so the callbacks' set() actually has someone to wake (exercises the
    # set-from-dealloc -> wake path), and so we observe forward progress on the
    # scheduler primitive driven by GC callbacks.
    def waiter():
        woken = 0
        while H.running():
            ev = state["ev"]
            # TIMED wait so the waiter rechecks H.running() and EXITS at teardown.
            # A no-timeout ev.wait() strands this goroutine once the workers are
            # done and no setter remains: the harness teardown force-cancels
            # netpoll parkers but NOT a cooperative Event waiter (an in-memory
            # runloom_c.park), so the stranded waiter wedges mn_run's join.  macOS
            # exposes this 100% (the wait->clear->wait re-park lands stuck); Linux
            # only happened to fire a last set() from post()'s gc.collect().  The
            # timed poll still exercises the set-from-dealloc -> wake path.
            if ev.wait(timeout=0.05):
                woken += 1
                ev.clear()
            runloom.yield_now()
        H.log("event_wakes={0}".format(woken))

    H.fiber(waiter)

    def gc_driver():
        while H.running():
            H.sleep(0.05)
            gc.collect()

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, state)


def post(H):
    gc.collect()
    fired = sum(H.state["fired"])
    closed = sum(H.state["closed"])
    H.check(fired > 0, "no weakref callbacks fired")
    H.check(H.state["events_set"][0] > 0,
            "no Event.set() ran from a weakref callback")
    H.log("callbacks_fired={0} fds_closed={1} events_set={2}".format(
        fired, closed, H.state["events_set"][0]))


if __name__ == "__main__":
    harness.main("p143_weakref_callback_cancellation", body, setup=setup,
                 post=post, default_funcs=2000,
                 describe="weakref callbacks set a runloom Event + close fds from "
                          "the GC dealloc path; no crash/deadlock")
