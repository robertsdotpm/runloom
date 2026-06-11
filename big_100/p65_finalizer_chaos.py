"""big_100 / 65 -- finalizer exception chaos.

Objects whose __del__ does unusual things -- bump a counter, sometimes raise
(Python prints "Exception ignored" and moves on), sometimes touch shared state.
Goroutines churn through creating and dropping them, with periodic gc.collect()
under load.  The interpreter must survive destructor reentrancy without
crashing, and the finalizers must actually run.

Stresses: GC + destructor interaction, finalizer reentrancy under M:N.
"""
import gc
import sys
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


def worker(H, wid, rng, state):
    i = 0
    while H.running():
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
        # Only a few workers ever drive an explicit collect from their own
        # context: at 1M workers a 2%-per-iteration collect is tens of thousands
        # of stop-the-world collections, which serialises everything.  The
        # gc_driver goroutine supplies the realistic "GC under load" pressure.
        if wid < 64 and rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def setup(H):
    H.state = {"lock": threading.Lock(), "finalized": [0], "created": [0],
               "booms": [0]}
    # A raising __del__ becomes an "unraisable" exception.  The DEFAULT hook
    # prints the full traceback to stderr -- at 1M goroutines that is millions
    # of multi-line tracebacks, and the synchronous stderr I/O (not the GC or
    # the lock) is what dominates the run.  Install a hook that COUNTS the
    # ignored exception instead: the raising-__del__ path is still fully
    # exercised ("survive + move on"), and post() can verify it fired.
    booms = H.state["booms"]

    def count_unraisable(unraisable):
        booms[0] += 1

    sys.unraisablehook = count_unraisable


def body(H):
    def gc_driver():
        while H.running():
            H.sleep(0.2)
            gc.collect()
        H.log("created={0} finalized={1}".format(
            H.state["created"][0], H.state["finalized"][0]))

    H.go(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gc.collect()
    # Finalizers must have run for a large fraction of created objects (some may
    # still be pending at exit, but the count must be advancing, not stuck).
    H.check(H.state["finalized"][0] > 0,
            "no finalizers ran at all")
    # The raising-__del__ path must have actually fired (the whole point of the
    # test) -- the unraisable hook counts it instead of flooding stderr.
    H.check(H.state["booms"][0] > 0,
            "no raising __del__ was observed by the unraisable hook")
    H.log("final created={0} finalized={1} booms={2}".format(
        H.state["created"][0], H.state["finalized"][0], H.state["booms"][0]))


if __name__ == "__main__":
    harness.main("p65_finalizer_chaos", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="__del__ that raises/touches state; survive GC reentrancy")
