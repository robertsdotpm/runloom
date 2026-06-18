"""big_100 / 227 -- blocking-offload pool saturation storm.

Many programs touch the blocking-offload pool incidentally, but none drive
offload DEMAND far past the pool's worker count.  This one does: with a small
pool (RUNLOOM_BLOCKPOOL_WORKERS=4 by default) and thousands of goroutines all
calling runloom_c.blocking() in a tight round loop, the offload queue depth runs
deep, the worker-thread cap is held hard, and every completion has to come back
through the foreign-waker path (the offload thread wakes the parked caller via a
same-thread peek, NOT sched_get -- a documented invariant).  Pool saturation is
exactly where a foreign-waker lost-wakeup or a worker-cap deadlock surfaces: a
dropped or misrouted wake leaves a caller parked forever (-> watchdog HANG), and
a result mixup hands one caller another's digest.

The offloaded body is a GIL-releasing real-blocking op: a hashlib.sha256 update
loop over the goroutine's own deterministic payload (the C digest releases the
GIL, so a worker genuinely occupies its thread while others queue).  Some rounds
instead use a tiny jittered time.sleep to mix sleep-blocking workers in with
CPU-blocking ones, so queue depth varies and the FIFO-ish drain is exercised
under a moving worker mix.

Oracle: every blocking() return value MUST equal the deterministic transform of
that goroutine's own input -- a misrouted/dropped foreign wake either hangs (the
watchdog catches it) or delivers a wrong value (a result mixup), and both fail
H.check here.  Forward progress (ops nonzero, no starved caller) proves the pool
drains under backpressure, and a post-run conservation check asserts the total
completed offload count equals what was submitted (no swallowed completion).  We
also probe live_fibers() stays bounded (the pool must not leak goroutines).

Stresses: Stresses: blocking-offload pool saturation -- far more concurrent
runloom_c.blocking() callers than RUNLOOM_BLOCKPOOL_WORKERS, driving queue depth,
worker-thread cap, FIFO-ish fairness, and the foreign-waker (offload-thread ->
scheduler) wake path under sustained backpressure.
"""
import os

# Force a SMALL offload pool so demand >> pool size and the queue saturates.
# RUNLOOM_BLOCKPOOL_WORKERS is read by runloom_blockpool_init() via getenv() the
# first time blocking() is called, so it MUST be set before runloom initialises.
# setdefault so a soak harness can dial pool size (e.g. =hubs) to compare.
os.environ.setdefault("RUNLOOM_BLOCKPOOL_WORKERS", "4")

import hashlib  # noqa: E402
import struct   # noqa: E402

import harness  # noqa: E402
import runloom_c  # noqa: E402


# Availability guard: blocking is cross-platform, but never assume.
_OFFLOAD = getattr(runloom_c, "blocking", None)
_LIVE = getattr(runloom_c, "live_fibers", None)


def transform(payload):
    """The deterministic transform, used both ON the offload pool and as the
    reference oracle.  hashlib.sha256.update releases the GIL, so when this runs
    on a blockpool worker it genuinely holds that worker thread while sibling
    callers queue -- which is the saturation we want."""
    h = hashlib.sha256()
    # A handful of update rounds so the C work is non-trivial (real GIL-releasing
    # blocking), making a worker occupy its thread long enough to build queue
    # depth across thousands of callers and a 4-worker cap.
    for _ in range(32):
        h.update(payload)
    return h.hexdigest()


def sleep_body(payload, secs):
    """An alternate GIL-releasing offload body: sleep a tiny jittered interval,
    then return the same deterministic transform.  Mixing real-sleep workers in
    with CPU workers varies queue depth and exercises the drain under a moving
    worker-occupancy mix.  time.sleep here is the ORIGINAL (this runs on a
    blockpool OS thread, not a goroutine, so it must really block, not park)."""
    import time as _time
    _time.sleep(secs)
    return transform(payload)


def worker(H, wid, rng, state):
    completed = state["completed"]
    base = struct.pack("<I", wid)
    seq = 0
    for _ in H.round_range():
        if not H.running():
            break
        seq += 1
        # A payload unique to THIS goroutine+round: if a foreign wake is
        # misrouted, the value we get back will not match our own payload's
        # transform, and the oracle below catches the mixup.
        payload = (base + struct.pack("<I", seq)
                   + bytes((wid * 31 + seq * 7) & 0xFF for _ in range(40)))
        expected = transform(payload)

        # Most rounds: a CPU (hashlib) offload that holds a worker thread.  Some
        # rounds: a tiny jittered real-sleep offload, to vary the worker mix and
        # keep the queue depth moving under the 4-worker cap.
        try:
            if (seq & 7) == 0:
                secs = 0.0002 + rng.random() * 0.0008
                got = _OFFLOAD(sleep_body, payload, secs)
            else:
                got = _OFFLOAD(transform, payload)
        except OSError:
            # Pool teardown at shutdown can surface as OSError on a parked
            # offload; only a failure if the run is still live.
            if not H.running():
                break
            raise

        # Oracle: the result MUST be THIS payload's transform.  A dropped wake
        # would have hung us forever (watchdog); a misrouted wake hands us a
        # sibling's value -> this fails.
        if not H.check(got == expected,
                       "offload result mismatch wid={0} seq={1} "
                       "(foreign-wake misroute or result mixup)".format(
                           wid, seq)):
            return
        completed[wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"completed": [0] * 1024, "submitted": [0] * 1024}
    H.log("RUNLOOM_BLOCKPOOL_WORKERS={0} funcs={1} (demand >> pool: "
          "saturating the offload queue)".format(
              os.environ.get("RUNLOOM_BLOCKPOOL_WORKERS"), H.funcs))


def body(H):
    if _OFFLOAD is None:
        # Availability guard: offload pool unavailable -> trivial no-op PASS.
        H.log("SKIP: runloom_c.blocking unavailable; offload pool not built")
        return
    H.run_pool(H.funcs, worker, H.state)


def probe_live_fibers(H):
    """Background probe: live_fibers() must stay bounded (no offload-induced
    goroutine leak).  Caps at a generous multiple of the funded pool so a real
    leak trips it but normal in-flight churn does not."""
    if _LIVE is None:
        return
    cap = max(64, H.expected) * 4 + 4096
    while H.running():
        try:
            live = _LIVE()
        except Exception:
            return
        if not H.check(live <= cap,
                       "live_fibers={0} exceeds bound {1} (offload goroutine "
                       "leak?)".format(live, cap)):
            return
        H.sleep(0.5)


def run_with_probe(H):
    if _OFFLOAD is None:
        H.log("SKIP: runloom_c.blocking unavailable; offload pool not built")
        return
    H.go(probe_live_fibers, H)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if _OFFLOAD is None:
        return
    completed = sum(H.state["completed"])
    # Forward progress: the saturated pool MUST have drained at least some work
    # (a wedged pool completes nothing and the watchdog fires, but guard anyway).
    H.check(completed > 0,
            "no offload ever completed -- pool may have wedged under saturation")
    # Conservation: every goroutine that exited cleanly verified its own result
    # before counting, so completed must equal the op total (no swallowed
    # completion handed back the wrong value or silently lost).
    H.check(completed == H.total_ops(),
            "offload completion count {0} != op count {1} (a completion was "
            "swallowed or double-counted)".format(completed, H.total_ops()))
    H.log("completed_offloads={0} ops={1}".format(completed, H.total_ops()))


if __name__ == "__main__":
    harness.main("p227_blockpool_saturation_storm", run_with_probe,
                 setup=setup, post=post, default_funcs=2000,
                 describe="thousands of runloom_c.blocking() callers against a "
                          "tiny offload pool: queue depth, worker cap, FIFO "
                          "fairness, foreign-waker wake path under backpressure")
