"""big_100 / 217 -- GC stop-the-world under goroutine churn (regression guard).

A REAL OS thread (captured at module top, BEFORE monkey.patch()) repeatedly
calls gc.collect() -- a stop-the-world pause that, on free-threaded 3.13t, must
briefly halt every hub thread -- while the worker pool spawns/exits/migrates
goroutines that allocate and free objects, some forming reference cycles the GC
must trace.

This is the regression guard for the baselined win-3.13t GC-STW MONOPOLY
deadlock (a stop-the-world collection that monopolised the runtime so no hub made
progress; fixed in the scheduler 2026-06-03, and it reproduced on Linux too).
The invariant is purely forward progress: the harness watchdog (its own real OS
thread) fails the run if ops stop rising for hang-timeout seconds, which is
exactly what a STW deadlock would do.

Stresses: cross-thread stop-the-world gc.collect() vs M:N goroutine churn,
QSBR/biased-refcount reclaim under load, no STW monopoly deadlock.
"""
import gc
import _thread as _real_thread        # captured BEFORE monkey.patch()
import time as _time

import harness
import runloom

REAL_SLEEP = _time.sleep


def gc_thread(H, collects):
    """Real OS thread: hammer gc.collect() every few ms for the whole run.
    gc.collect() is a stop-the-world pause on 3.13t; under the fixed scheduler
    it must let the hubs resume promptly, never monopolise them."""
    n = 0
    while H.running():
        try:
            gc.collect()
        except Exception:
            pass
        n += 1
        REAL_SLEEP(0.003)
    collects[0] = n


class Node(object):
    __slots__ = ("peer", "buf", "idx")

    def __init__(self, idx):
        self.idx = idx
        self.peer = None
        self.buf = bytearray(idx & 0xFF)


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        # Allocate a small batch, some forming cycles the GC must trace,
        # then drop them so the next collect has real work.
        batch = []
        for k in range(rng.randint(6, 20)):
            a = Node(wid + k)
            b = Node(wid + k + 1)
            a.peer = b
            b.peer = a                     # cycle -> needs the GC, not refcount
            batch.append(a)
        # Touch them so the work isn't optimised away.
        checksum = 0
        for nd in batch:
            checksum = (checksum + nd.idx + len(nd.buf)) & 0xFFFFFFFF
        if not H.check(checksum >= 0, "impossible"):
            return
        del batch
        H.op(wid)
        H.task_done(wid)
        if (wid & 7) == 0 and rng.random() < 0.05:
            runloom.yield_now()            # migrate hubs mid-churn


def setup(H):
    H.state = {"collects": [0]}


def body(H):
    collects = H.state["collects"]
    _real_thread.start_new_thread(gc_thread, (H, collects))
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # The whole point is no-deadlock: if we got here, the watchdog never fired.
    H.check(H.total_ops() > 0, "no work done (the run wedged early)")
    H.check(H.state["collects"][0] > 0,
            "the gc thread never completed a single collect (STW deadlock?)")
    H.log("collects={0} ops={1}".format(
        H.state["collects"][0], H.total_ops()))


if __name__ == "__main__":
    harness.main("p217_gc_stw_under_churn", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="real-thread gc.collect() STW vs goroutine churn; "
                          "no STW-monopoly deadlock, ops keep rising")
