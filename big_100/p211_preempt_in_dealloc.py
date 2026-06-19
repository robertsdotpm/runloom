"""big_100 / 211 -- preemption must not yield mid-dealloc.

Mass-create and drop objects with a HEAVY __del__: deep container teardown (a
few hundred elements), a touched peer object, and reference cycles the trashcan
must unwind.  Run under DEFAULT sysmon preemption (on by default on 3.13t -- we
do NOT disable it).  The preempt eval-frame wrapper / single-frame liveness
backstop fire at arbitrary Python-frame entries, which can land nested inside an
in-flight tp_dealloc; the runtime must DEFER the yield while a destructor is in
flight (CLAUDE.md: "Preemption must NOT yield a goroutine mid object-
destruction").  A concurrent stop-the-world gc.collect() from a real OS thread
AND a driver goroutine races the dealloc -- yielding mid-dealloc here would let
the STW reclaim against a half-destroyed object -> UAF/crash.

Stresses: sysmon preemption vs tp_dealloc, cross-thread STW vs in-flight
destructor under M:N.
"""
import gc

import harness
import runloom

import _thread as _real_thread
import time as _time
_REAL_SLEEP = _time.sleep


class Heavy(object):
    """A __del__ that does real work: tear down a deep container, touch a peer,
    and run a Python loop -- a long-ish destructor that gives the preempt
    machinery many frame-entry points to (wrongly) try to yield at."""
    __slots__ = ("idx", "bucket", "peer", "state")

    def __init__(self, idx, state):
        self.idx = idx
        # A few hundred elements -> a non-trivial teardown when the object dies.
        self.bucket = [bytearray(8) for _ in range(rng_count())]
        self.peer = None
        self.state = state

    def __del__(self):
        # Heavy destructor body.  This runs inside tp_dealloc; the preempt
        # wrapper must NOT swap this goroutine out half-way through (which would
        # freeze a half-finished destructor while a concurrent STW collect runs).
        s = 0
        b = self.bucket
        for ba in b:
            s += len(ba)                    # touch every element
        # Touch the peer object (another live object reached from the dtor).
        p = self.peer
        if p is not None:
            try:
                p.idx                       # attribute access -> a frame entry
            except Exception:
                pass
        # Drop the deep container explicitly (more dealloc nested in this dtor).
        self.bucket = None
        st = self.state
        with st["lock"]:
            st["finalized"][0] += 1


_DEEP = 256


def rng_count():
    return _DEEP


def setup(H):
    H.state = {
        "lock": runloom.sync.Lock(),
        "finalized": [0],
        "thread_collects": [0],
        "stop": [False],
    }


def worker(H, wid, rng, state):
    for _ in H.round_range():
        batch = []
        for j in range(rng.randint(2, 6)):
            o = Heavy(wid * 1000 + j, state)
            o2 = Heavy(wid * 1000 + j + 500, state)
            o.peer = o2                     # cross-reference
            o2.peer = o                     # cycle -> trashcan unwinds it
            batch.append(o)
        # Drop the batch: the heavy __del__s (and the cycle teardown) run now,
        # possibly while a concurrent gc.collect() STW is in progress.
        del batch
        if wid < 32 and rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def body(H):
    state = H.state

    # Real OS thread: stop-the-world gc.collect() racing the goroutines' heavy
    # destructors.  This is the cross-thread STW-vs-dealloc race the invariant
    # guards.
    def gc_thread():
        while not state["stop"][0] and H.running():
            try:
                gc.collect()
                state["thread_collects"][0] += 1
            except Exception:
                pass
            _REAL_SLEEP(0.002)

    _real_thread.start_new_thread(gc_thread, ())

    def gc_driver():
        while H.running():
            H.sleep(0.03)
            gc.collect()
        state["stop"][0] = True
        H.log("finalized={0} thread_collects={1}".format(
            state["finalized"][0], state["thread_collects"][0]))

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, state)


def post(H):
    H.state["stop"][0] = True
    gc.collect()
    H.check(H.state["finalized"][0] > 0, "no heavy destructors ran")
    H.log("finalized={0} thread_collects={1} ops={2}".format(
        H.state["finalized"][0], H.state["thread_collects"][0], H.total_ops()))


if __name__ == "__main__":
    harness.main("p211_preempt_in_dealloc", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="heavy __del__ + cross-thread STW; preemption must not "
                          "yield mid-dealloc (no UAF/crash)")
