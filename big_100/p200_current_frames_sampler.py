"""big_100 / 200 -- sys._current_frames() sampling under churn.

A background sampler goroutine AND a real OS thread repeatedly call
`sys._current_frames()` while the worker pool spawns, migrates between hubs, and
exits.  `sys._current_frames()` returns a dict mapping each OS thread id to its
topmost frame; under M:N the hub threads are constantly swapping goroutine C
stacks in and out, so this call races the scheduler hard.  It must ALWAYS return
a real dict without crashing, and the thread/frame accounting must stay
internally consistent (keys are ints, values are real frames whose attributes
are readable).

Stresses: sys._current_frames() racing goroutine stack swaps across hubs and a
foreign OS thread; frame-table consistency under M:N churn.
"""
import sys
import time as _time
import _thread as _real_thread       # captured before monkey.patch()

import harness
import runloom

REAL_SLEEP = _time.sleep


def validate_frames(H, frames, who):
    """A returned mapping must be a dict {int thread_id: frame}; every frame's
    basic attributes must be readable without crashing."""
    if not H.check(isinstance(frames, dict),
                   "{0}: _current_frames did not return a dict: {1}".format(
                       who, type(frames).__name__)):
        return -1
    n = 0
    for tid, fr in frames.items():
        if not H.check(isinstance(tid, int),
                       "{0}: non-int thread id key {1!r}".format(who, tid)):
            return -1
        if fr is None:
            continue
        try:
            _ = fr.f_code.co_name
            _ = fr.f_lineno
        except Exception as exc:                       # noqa: BLE001
            H.fail("{0}: frame attr read crashed: {1!r}".format(who, exc))
            return -1
        n += 1
    return n


def foreign_sampler(H, state):
    """Runs on a REAL OS thread (NOT a goroutine): calls _current_frames from
    outside the scheduler entirely, the harshest racer."""
    while H.running():
        frames = sys._current_frames()
        n = validate_frames(H, frames, "foreign-thread")
        if n < 0:
            return
        if n > state["max_frames"][0]:
            state["max_frames"][0] = n          # single foreign writer
        state["foreign_samples"][0] += 1        # single foreign writer
        del frames
        REAL_SLEEP(0.001)


def goroutine_sampler(H, wid, rng, state):
    while H.running():
        frames = sys._current_frames()
        n = validate_frames(H, frames, "goroutine")
        if n < 0:
            return
        # per-sampler slot, single writer
        state["go_samples"][wid & 255] += 1
        if n > state["go_max_frames"][wid & 255]:
            state["go_max_frames"][wid & 255] = n
        del frames
        H.op(wid)
        runloom.sleep(0.001)


def churn_worker(H, wid, rng, state):
    """Ordinary cooperative churn: allocate, migrate, exit -- moving frames in
    and out of the per-thread frame table the sampler reads."""
    for _ in H.round_range():
        # A little nested call depth so there are real frames to sample.
        def lvl3():
            runloom.yield_now()
            return [bytearray(64) for _ in range(8)]

        def lvl2():
            runloom.sleep(0.0005)
            return lvl3()

        junk = lvl2()
        if not H.check(len(junk) == 8, "churn alloc wrong wid={0}".format(wid)):
            return
        del junk
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.3:
            runloom.yield_now()


def setup(H):
    H.state = {
        "max_frames": [0],
        "foreign_samples": [0],
        "go_samples": [0] * 256,
        "go_max_frames": [0] * 256,
    }


def body(H):
    state = H.state
    # A handful of goroutine samplers + ONE real OS thread sampler.
    nsamp = 4
    _real_thread.start_new_thread(foreign_sampler, (H, state))
    H.run_pool(nsamp, goroutine_sampler, state)
    # The bulk of the pool is cooperative churn.
    H.run_pool(max(1, H.funcs - nsamp), churn_worker, state)


def post(H):
    go_samples = sum(H.state["go_samples"])
    foreign = H.state["foreign_samples"][0]
    total = go_samples + foreign
    H.check(total > 0, "no _current_frames() sample ever completed")
    go_max = max(H.state["go_max_frames"]) if H.state["go_max_frames"] else 0
    max_frames = max(H.state["max_frames"][0], go_max)
    H.log("samples={0} (goroutine={1} foreign={2}) max_frames_seen={3}".format(
        total, go_samples, foreign, max_frames))


if __name__ == "__main__":
    harness.main("p200_current_frames_sampler", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="sys._current_frames() always returns a consistent "
                          "dict while goroutines churn/migrate, no crash")
