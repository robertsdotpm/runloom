"""big_100 / 177 -- ctypes blocking call vs cooperative progress.

SOME goroutines call a libc blocking function (usleep / nanosleep) via ctypes.
That is a genuinely non-cooperative C call: it blocks the HUB OS THREAD it runs
on for the whole sleep (ctypes does not park the goroutine).  Meanwhile OTHER
goroutines do purely cooperative ops (yield + a counter).  With multiple hubs,
the cooperative goroutines MUST keep making progress even while some hubs are
stuck in the C sleep -- as long as the heavy fraction can't occupy ALL hubs at
once.

Invariant: the cooperative op count keeps rising throughout (sampled over the
run; it must never stall to zero for a sampling window); no crash.

Stresses: a hub-blocking ctypes C call alongside cooperative goroutines, hub
availability under non-cooperative work, sysmon/scheduler progress.
"""
import ctypes
import ctypes.util

import harness
import runloom

# usleep(useconds_t) is portable enough on Linux/macOS via libc.  Fall back to
# nanosleep if usleep is unavailable.
_LIBC = ctypes.CDLL(ctypes.util.find_library("c") or None, use_errno=True)


def _blocking_csleep(micros):
    """Block THIS OS thread `micros` microseconds inside libc (no goroutine
    park)."""
    try:
        _LIBC.usleep(ctypes.c_uint(micros))
    except AttributeError:
        # nanosleep(const struct timespec*, struct timespec*)
        class TS(ctypes.Structure):
            _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]
        ts = TS(micros // 1000000, (micros % 1000000) * 1000)
        _LIBC.nanosleep(ctypes.byref(ts), None)


# Keep the heavy fraction modest so it can NEVER occupy all hubs at once.
HEAVY_FRACTION = 4    # 1 in 4 goroutines is heavy


def setup(H):
    # Per-goroutine cooperative-op slots (race-free).  The "progress" metric is
    # the sum of these; the watchdog + an in-test sampler check it keeps rising.
    H.coop_ops = [0] * H.funcs
    H.heavy_calls = [0] * H.funcs
    H.state = {}


def cooperative(H, wid):
    coop = H.coop_ops
    for _ in H.round_range():
        coop[wid] += 1
        H.op(wid)
        H.task_done(wid)
        runloom.yield_now()
        runloom.sleep(0.0002)


def heavy(H, wid, rng):
    heavy_calls = H.heavy_calls
    for _ in H.round_range():
        # A C sleep long enough to genuinely tie up a hub thread, but short
        # enough that rounds cycle.  100-400us.
        _blocking_csleep(rng.randint(100, 400))
        heavy_calls[wid] += 1
        H.op(wid)
        H.task_done(wid)


def worker(H, wid, rng, state):
    if (wid % HEAVY_FRACTION) == 0:
        heavy(H, wid, rng)
    else:
        cooperative(H, wid)


def sampler(H):
    """Watch that cooperative progress never stalls to zero across a sampling
    window while heavy goroutines are running.  This is the real invariant: the
    blocking C calls must not be able to starve the cooperative world."""
    import harness as _h
    last = sum(H.coop_ops)
    stalls = 0
    # Allow a few empty windows at the very start/end (ramp-up / drain) but no
    # sustained stall in the middle.
    while H.running():
        H.sleep(0.25)
        cur = sum(H.coop_ops)
        if cur == last and H.running() and H.time_left() > 0.5:
            stalls += 1
            if stalls >= 6:    # ~1.5s with zero cooperative progress -> starved
                H.fail("cooperative progress stalled for ~1.5s while heavy "
                       "ctypes calls ran (hub starvation)")
                return
        else:
            stalls = 0
        last = cur


def body(H):
    H.go(sampler, H)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    coop = sum(H.coop_ops)
    heavy_n = sum(H.heavy_calls)
    H.check(coop > 0, "no cooperative ops completed")
    H.check(heavy_n > 0, "no heavy ctypes calls ran (test did nothing)")
    H.log("coop_ops={0} heavy_calls={1}".format(coop, heavy_n))


if __name__ == "__main__":
    harness.main("p177_ctypes_blocking_call", body, setup=setup, post=post,
                 default_funcs=500,
                 describe="hub-blocking ctypes usleep alongside cooperative "
                          "goroutines; cooperative progress never stalls")
