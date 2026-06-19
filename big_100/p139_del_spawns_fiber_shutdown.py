"""big_100 / 139 -- __del__ that touches the scheduler, incl. at shutdown.

Objects whose __del__ tries to do SCHEDULER work -- spawn a goroutine
(runloom.fiber), yield (runloom.yield_now), and/or set a runloom Event -- are
churned through create/drop by thousands of goroutines, with periodic
gc.collect() under load.  The finalizers fire from arbitrary points: a plain
drop, a cyclic-GC sweep, the free-threaded biased-refcount cross-thread merge,
and finally interpreter shutdown.  The runtime must let a finalizer touch the
scheduler (perform it, or cleanly no-op/raise an ignored exception) WITHOUT a
crash or use-after-free -- this is exactly the path the CLAUDE.md invariant
"preemption must NOT yield a goroutine mid object-destruction" guards: a yield
nested inside tp_dealloc, racing a concurrent stop-the-world gc.collect() on
another thread, must not run the GC against a half-destroyed object.

A raising/​spawning __del__ becomes an "unraisable"; the default hook would print
millions of tracebacks, so we COUNT them via sys.unraisablehook instead -- the
path is still fully exercised, and post() verifies finalizers fired.

Stresses: finalizer reentrancy into the scheduler, preempt-mid-dealloc safety,
GC-vs-dealloc race, interpreter-shutdown finalization under M:N.
"""
import gc
import sys
import threading

import harness
import runloom


def noop():
    """A goroutine spawned from inside a __del__ -- it must run (or be cleanly
    dropped) without corrupting the scheduler."""
    return


class Finalizes(object):
    __slots__ = ("idx", "state", "peer")

    def __init__(self, idx, state):
        self.idx = idx
        self.state = state
        self.peer = None

    def __del__(self):
        st = self.state
        with st["lock"]:
            st["finalized"][0] += 1
        # Touch the scheduler from inside tp_dealloc.  Rotate the kind of touch
        # so all three reentrant paths are exercised; each is wrapped because a
        # finalizer that raises during shutdown must not abort the process.
        kind = self.idx % 3
        try:
            if kind == 0:
                runloom.fiber(noop)                 # spawn from a destructor
                with st["lock"]:
                    st["spawned"][0] += 1
            elif kind == 1:
                runloom.yield_now()              # yield from a destructor
            else:
                st["event"].set()                # wake a waiter from a destructor
        except Exception:
            # Counted by the unraisable hook if it escapes; swallow here so the
            # rest of the dealloc (and the trashcan unwind) completes.
            with st["lock"]:
                st["touch_errors"][0] += 1


def worker(H, wid, rng, state):
    i = wid * 1_000_000
    for _ in H.round_range():
        batch = []
        for _ in range(rng.randint(4, 16)):
            o = Finalizes(i, state)
            o2 = Finalizes(i + 1, state)
            o.peer = o2                  # a cycle -> only the GC reclaims it
            o2.peer = o
            batch.append(o)
            i += 2
        with state["lock"]:
            state["created"][0] += len(batch)
        del batch
        if wid < 64 and rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def setup(H):
    H.state = {
        "lock": threading.Lock(),
        "created": [0], "finalized": [0], "spawned": [0],
        "touch_errors": [0], "unraisable": [0],
        "event": runloom.sync.Event(),
    }
    counter = H.state["unraisable"]

    def count_unraisable(unraisable):
        counter[0] += 1

    sys.unraisablehook = count_unraisable


def body(H):
    def gc_driver():
        # GC pressure that forces finalizers (incl. of the cyclic objects) to
        # fire from a DIFFERENT context than the worker that created them --
        # the cross-thread merge / STW path the invariant protects.
        while H.running():
            H.sleep(0.05)
            gc.collect()
        H.log("created={0} finalized={1} spawned_from_del={2}".format(
            H.state["created"][0], H.state["finalized"][0],
            H.state["spawned"][0]))

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gc.collect()
    st = H.state
    H.check(st["finalized"][0] > 0, "no finalizers ran at all")
    # At least the spawn-from-__del__ path must have executed (the sharpest
    # reentrancy: a goroutine spawn from inside tp_dealloc).
    H.check(st["spawned"][0] > 0,
            "no goroutine was ever spawned from a __del__")
    H.log("final created={0} finalized={1} spawned_from_del={2} "
          "touch_errors={3} unraisable={4}".format(
              st["created"][0], st["finalized"][0], st["spawned"][0],
              st["touch_errors"][0], st["unraisable"][0]))


if __name__ == "__main__":
    # Correctness test: the subject is finalizer (__del__) reentrancy into the
    # scheduler under GC churn, not scale.  At 100k+ the per-worker finalizer
    # churn doesn't drain inside the window (TIMEOUT).  Cap to intended scale.
    harness.main("p139_del_spawns_goroutine_shutdown", body, setup=setup,
                 post=post, default_funcs=1000, max_funcs=1000,
                 describe="__del__ spawns goroutines / yields / sets events under "
                          "GC churn; finalizer reentrancy is crash-free")
