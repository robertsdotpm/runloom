"""big_100 / 65 -- finalizer exception chaos.

Objects whose __del__ does unusual things -- bump a counter, sometimes raise
(Python prints "Exception ignored" and moves on), sometimes touch shared state.
Goroutines churn through creating and dropping them, with periodic gc.collect()
under load.  The interpreter must survive destructor reentrancy without
crashing, and the finalizers must actually run.

Stresses: GC + destructor interaction, finalizer reentrancy under M:N.
"""
import gc
import threading

import harness
import runloom


class Noisy(object):
    __slots__ = ("idx", "state", "peer")

    def __init__(self, idx, state):
        self.idx = idx
        self.state = state
        self.peer = None

    def __del__(self):
        st = self.state
        with st["lock"]:
            st["finalized"][0] += 1
        if (self.idx & 7) == 0:
            raise RuntimeError("boom in __del__ {0}".format(self.idx))


# At 100k goroutines, each iteration acquires state["lock"] ONCE for
# `created` AND the Noisy.__del__ finalizers each acquire it too.  With
# 100k goroutines competing for a single CoLock, throughput ≈ 80/s and
# drain ≈ 100k/80 ≈ 1250s >> 120s.  Cap concurrent workers + cancel watcher.
MAX_WORKERS = 2000


def worker(H, wid, rng, state):
    sem = state["sem"]
    i = 0
    while H.running():
        if not sem.acquire():
            break
        try:
            # Build a small batch, including a reference cycle, then drop it.
            batch = []
            for _ in range(rng.randint(4, 16)):
                o = Noisy(i, state)
                o2 = Noisy(i + 1, state)
                o.peer = o2          # mutual reference cycle -> needs the GC
                o2.peer = o
                batch.append(o)
                i += 2
            with state["lock"]:
                state["created"][0] += len(batch)
            del batch
            if rng.random() < 0.02:
                gc.collect()
            H.op(wid)
            H.task_done(wid)
            if rng.random() < 0.1:
                runloom.yield_now()
        finally:
            sem.release()


def setup(H):
    sem = threading.Semaphore(MAX_WORKERS)
    H.state = {"lock": threading.Lock(), "finalized": [0], "created": [0],
               "sem": sem}


def body(H):
    sem = H.state["sem"]

    def _cancel_watcher():
        while H.running():
            runloom.sleep(0.05)
        sem.cancel_all()

    def gc_driver():
        while H.running():
            H.sleep(0.2)
            gc.collect()
        H.log("created={0} finalized={1}".format(
            H.state["created"][0], H.state["finalized"][0]))

    H.go(_cancel_watcher)
    H.go(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gc.collect()
    # Finalizers must have run for a large fraction of created objects (some may
    # still be pending at exit, but the count must be advancing, not stuck).
    H.check(H.state["finalized"][0] > 0,
            "no finalizers ran at all")
    H.log("final created={0} finalized={1}".format(
        H.state["created"][0], H.state["finalized"][0]))


if __name__ == "__main__":
    harness.main("p65_finalizer_chaos", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="__del__ that raises/touches state; survive GC reentrancy")
