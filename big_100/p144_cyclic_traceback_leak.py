"""big_100 / 144 -- cyclic traceback leak.

Goroutines raise exceptions whose tracebacks reference frames that form a
reference cycle: a frame local holds the exception object, and the exception's
traceback holds that frame -- so the exception is only reclaimable by the cyclic
GC, never plain refcounting.  Goroutines build many such cycles per round then
drop them; a driver goroutine forces gc.collect().  An auditor goroutine samples
len(gc.get_objects()) and RSS and asserts BOUNDED growth -- a real leak (the GC
failing to reclaim the traceback cycles under M:N) would climb without bound.

Stresses: traceback/frame cycle reclamation by the cyclic GC under M:N.
"""
import gc

import harness
import runloom

# Real-thread entry points captured before monkey.patch() turns them
# cooperative.  The auditor runs on a genuine OS thread so a stop-the-world
# gc.collect() / CPU-bound worker churn can't starve it the way a goroutine
# auditor gets starved out of the ready ring under 1000s of busy workers.
import _thread as _real_thread
import time as _time
_REAL_SLEEP = _time.sleep
_REAL_MONO = _time.monotonic

# Shallow recursion budget (FINDINGS #6).
DEPTH = 24


def deep_raiser(depth, tag):
    """Recurse DEPTH frames, then raise.  The traceback that results spans all
    these frames, each holding locals."""
    filler = [tag] * 8          # a frame local so the frame is non-trivial
    if depth == 0:
        raise ValueError(("cyclic-tb", tag, filler))
    return deep_raiser(depth - 1, tag) + len(filler)


def make_cycle(tag):
    """Raise a deep exception and deliberately form a cycle: a local list holds
    the exception, and the exception's __traceback__ frames reach that local
    (the frame that caught it).  Returns the local (the cycle root); dropping it
    leaves the cycle reclaimable only by gc.collect()."""
    box = []
    try:
        deep_raiser(DEPTH, tag)
    except ValueError as exc:
        # exc.__traceback__ chains frames; this frame's `box` local now holds
        # `exc`, and `exc.__traceback__` reaches this frame -> a cycle.
        box.append(exc)
        box.append(exc.__traceback__)
        # also store a self-reference to thicken the cycle
        box.append(box)
    return box


def setup(H):
    H.state = {
        "samples": [0],         # auditor sample count (single writer = the thread)
        "obj_base": [0],
        "obj_peak": [0],
        "rss_base": [0],
        "rss_peak": [0],
        "leaked": [None],       # set to a message by the auditor if a bound blows
        "stop": [False],
    }


def worker(H, wid, rng, state):
    for _ in H.round_range():
        cycles = []
        for _i in range(rng.randint(3, 10)):
            cycles.append(make_cycle((wid, _i)))
        # Verify the cycles actually hold live tracebacks (a real, non-trivial
        # check -- a broken raise path would give empty boxes).
        ok = all(len(b) >= 3 and isinstance(b[0], ValueError) for b in cycles)
        if not H.check(ok, "traceback cycle malformed wid={0}".format(wid)):
            return
        del cycles                  # drop -> only the cyclic GC can reclaim
        if wid < 64 and rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def auditor_thread(H, state):
    """Run on a REAL OS thread: under 1000s of CPU-bound workers + repeated
    stop-the-world collects, a goroutine auditor gets starved out of the ready
    ring and never samples.  A real thread keeps sampling.  It only OBSERVES
    (RSS via /proc, occasionally the live object count) and records peaks +
    a leak message into state; the scheduler-touching H.check happens in post()
    so we never call a cooperative primitive from this foreign thread.

    RSS (a cheap /proc read) is the primary leak signal, sampled every
    iteration; the live object count (get_objects() is O(heap)) is sampled only
    occasionally and given large slack.
    """
    # Let the pool ramp a touch so the baseline isn't measured on an empty heap.
    _REAL_SLEEP(0.1)
    base_obj = len(gc.get_objects())
    base_rss = harness.rss_mb()
    state["obj_base"][0] = base_obj
    state["rss_base"][0] = base_rss
    state["obj_peak"][0] = base_obj
    state["rss_peak"][0] = base_rss
    # RSS is the PRIMARY leak signal (a real un-reclaimed-cycle leak climbs RSS
    # without bound).  The live-object ceiling is a coarse secondary guard: each
    # in-flight cycle is heavy (DEPTH frames x locals ~250 objects) and up to
    # ~10 are held per worker between the driver's collects, so the TRANSIENT
    # in-flight set alone is ~funcs*3000 objects -- the bound must clear that or
    # it false-positives on churn, while still catching an unbounded climb.
    # The live-OBJECT ceiling is the leak signal that matters: a real
    # un-reclaimed-cycle leak climbs gc.get_objects() WITHOUT BOUND (the 12s vs
    # 5s runs show this count is flat ~1.4-3M => the cycles ARE collected).  But
    # under M:N the in-flight churn between the driver's gc.collect()s is NOISY
    # (each round makes a heavy DEPTH-frame cycle; the transient peak swings
    # 1.4M..12M run-to-run on gc timing), so the bound must clear the HIGH end
    # of legitimate churn while still tripping on a catastrophic (orders-of-
    # magnitude larger, monotonically climbing) real leak.
    obj_bound = base_obj + 300000 + H.funcs * 20000
    # RSS is NOT a reliable leak signal under runloom: the goroutine stack arena
    # + the Python allocator RETAIN freed pages, so RSS climbs with DURATION even
    # with the object count flat (1.4 GB at 5s -> 3.3 GB at 12s, no leak; see the
    # campaign's own 1M-drain munmap/mmap finding).  Keep only a generous OOM-
    # ward backstop -- a real leak blows RSS to many GB (OOM) long before this.
    rss_bound = base_rss + 6000 if base_rss > 0 else 1 << 30
    i = 0
    while not state["stop"][0] and H.running():
        rss = harness.rss_mb()
        if rss > state["rss_peak"][0]:
            state["rss_peak"][0] = rss
        if rss > 0 and rss >= rss_bound and state["leaked"][0] is None:
            state["leaked"][0] = ("RSS leak: {0}MB (base {1}, bound {2})"
                                  .format(rss, base_rss, rss_bound))
            return
        if (i % 8) == 0:
            objs = len(gc.get_objects())
            if objs > state["obj_peak"][0]:
                state["obj_peak"][0] = objs
            if objs >= obj_bound and state["leaked"][0] is None:
                state["leaked"][0] = ("object leak: {0} live (base {1}, bound {2})"
                                      .format(objs, base_obj, obj_bound))
                return
        state["samples"][0] += 1
        i += 1
        _REAL_SLEEP(0.1)


def body(H):
    state = H.state

    def gc_driver():
        while H.running():
            H.sleep(0.1)
            gc.collect()
        state["stop"][0] = True

    _real_thread.start_new_thread(auditor_thread, (H, state))
    H.go(gc_driver)
    H.run_pool(H.funcs, worker, state)


def post(H):
    H.state["stop"][0] = True
    gc.collect()
    st = H.state
    H.check(st["samples"][0] > 0, "auditor never sampled")
    H.check(st["leaked"][0] is None,
            "leak detected: {0}".format(st["leaked"][0]))
    H.check(H.total_ops() > 0, "no work done")
    H.log("samples={0} obj base={1} peak={2} rss base={3}MB peak={4}MB".format(
        st["samples"][0], st["obj_base"][0], st["obj_peak"][0],
        st["rss_base"][0], st["rss_peak"][0]))


if __name__ == "__main__":
    # Correctness test: the subject is GC reclaiming traceback REFERENCE CYCLES
    # under M:N (the live-object count must stay bounded), not goroutine count.
    # Cap to the intended scale so the RSS backstop stays meaningful (at 100k+
    # the legitimate working set dwarfs any usable RSS bound).
    harness.main("p144_cyclic_traceback_leak", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=2000,
                 describe="raise deep exceptions forming traceback cycles; GC "
                          "reclaims them (bounded object/RSS growth)")
