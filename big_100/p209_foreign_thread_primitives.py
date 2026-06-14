"""big_100 / 209 -- patched primitives used from a FOREIGN OS thread.

`monkey.patch()` replaces threading / queue globally, so a cooperative
`threading.Lock` / `Condition` / `queue.Queue` can be reached from a thread that
is NOT a goroutine and NOT a hub -- a real OS thread.  The CLAUDE.md
"FOREIGN-OS-THREAD-safe" invariant requires the patched primitive to DETECT the
foreign thread (no goroutine / TLS peek NULL) and fall back to REAL OS blocking,
never park a non-existent goroutine nor lazily allocate scheduler state.

Here a modest pool of real OS threads (spawned via `_thread.start_new_thread`,
captured at module top BEFORE patch) increments a shared counter under a patched
Lock, ALONGSIDE goroutines doing the same with the same Lock.  The counter must
equal the exact total of increments from BOTH worlds (no lost update, no crash /
UAF).

Stresses: foreign-OS-thread safety of cooperative Lock/Condition/Queue, mixed
goroutine + real-thread contention on one patched primitive.
"""
import _thread as _rt          # captured BEFORE monkey.patch()
import time as _time

import harness
import runloom

REAL_SLEEP = _time.sleep

NTHREADS = 16                  # modest real-OS-thread pool
THREAD_INCS = 2000            # increments each real thread performs
GO_INCS_PER_ROUND = 4        # increments each goroutine does per round


def setup(H):
    # The shared state is created INSIDE the scheduler (in body) so the Lock /
    # Queue are the cooperative patched versions.  setup only sizes per-slot
    # tallies.
    H.go_incs = [0] * H.funcs
    # one slot per real thread (single writer each)
    H.thread_incs = [0] * NTHREADS
    H.threads_done = [0] * NTHREADS
    H.state = {}


def real_thread_body(H, tid):
    """Runs on a REAL OS thread.  Takes the patched Lock; the patched primitive
    must detect this thread is foreign and block on a real OS lock rather than
    park a goroutine."""
    lock = H.state["lock"]
    q = H.state["queue"]
    counter = H.state["counter"]
    n = 0
    try:
        while H.running() and n < THREAD_INCS:
            with lock:
                counter[0] += 1
            n += 1
            # Also exercise the patched Queue from the foreign thread (its
            # internal Condition must fall back to real blocking here).
            if (n & 31) == 0:
                q.put((tid, n))
            if (n & 255) == 0:
                REAL_SLEEP(0)      # let the scheduler breathe
        H.thread_incs[tid] = n
    finally:
        H.threads_done[tid] = 1


def queue_drainer(H):
    """A goroutine that drains the Queue the foreign threads fill, so the Queue's
    Condition is contended from both a goroutine (get) and real threads (put)."""
    q = H.state["queue"]
    drained = [0]
    while H.running():
        try:
            item = q.get(timeout=0.05)
        except Exception:
            continue
        drained[0] += 1
    H.state["drained"] = drained[0]


def worker(H, wid, rng, state):
    lock = state["lock"]
    counter = state["counter"]
    go_incs = H.go_incs
    for _ in H.round_range():
        for _ in range(GO_INCS_PER_ROUND):
            with lock:
                counter[0] += 1
            go_incs[wid] += 1
        H.op(wid)
        H.task_done(wid)
        runloom.yield_now()


def body(H):
    import threading
    import queue
    # These are the PATCHED (cooperative) primitives now that monkey.patch() ran.
    H.state["lock"] = threading.Lock()
    H.state["queue"] = queue.Queue()
    H.state["counter"] = [0]
    H.state["drained"] = 0

    H.go(queue_drainer, H)
    # Spawn the real OS threads (foreign to the scheduler).  start_new_thread is
    # the pre-patch _thread entry point, so these are genuine OS threads.
    for tid in range(NTHREADS):
        _rt.start_new_thread(real_thread_body, (H, tid))

    H.run_pool(H.funcs, worker, H.state)

    # Wait for the real threads to finish their fixed increment budget so the
    # final counter accounting is exact (they exit on their own count or on
    # H.running() going false).
    deadline = harness.REAL_MONO() + 30.0
    while sum(H.threads_done) < NTHREADS and harness.REAL_MONO() < deadline:
        H.sleep(0.02)


def post(H):
    counter = H.state["counter"][0]
    go_total = sum(H.go_incs)
    thread_total = sum(H.thread_incs)
    expected = go_total + thread_total
    H.check(go_total > 0, "no goroutine increments")
    H.check(thread_total > 0, "no real-thread increments (foreign path untested)")
    # The patched Lock must serialise BOTH worlds: every increment counted.
    H.check(counter == expected,
            "counter {0} != goroutine {1} + thread {2} = {3} (lost update across "
            "goroutine/foreign-thread Lock)".format(
                counter, go_total, thread_total, expected))
    H.check(sum(H.threads_done) == NTHREADS,
            "only {0}/{1} foreign threads finished".format(
                sum(H.threads_done), NTHREADS))
    H.log("counter={0} go={1} thread={2} drained={3} threads_done={4}/{5}".format(
        counter, go_total, thread_total, H.state.get("drained", 0),
        sum(H.threads_done), NTHREADS))


if __name__ == "__main__":
    harness.main("p209_foreign_thread_primitives", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="patched Lock/Queue shared by goroutines and real OS "
                          "threads; counter conserved, no lost update")
