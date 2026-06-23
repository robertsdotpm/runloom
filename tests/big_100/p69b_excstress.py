"""big_100 / 69b -- exception-state preempt stress (p69 root-cause reproducer).

p69_traceback_integrity crashes ~8%/hour at 100k because the runtime snaps the
tstate exception state ONLY when a preemption fires INSIDE the `except` block
(cooperative yield_now happens before the raise; extract_tb's file reads are
linecache-cached after warmup).  p69's except block is tiny, so that window is
rarely hit -> the bug needs ~an hour of 100k runtime to surface.

This variant MAXIMIZES the window: each goroutine spends almost all its time
INSIDE the except block with the exception live, entering thousands of Python
frames (each frame entry is a preemption opportunity / eval-frame-wrapper point).
With aggressive preemption (RUNLOOM_SYSMON_MS small) and RUNLOOM_DBG_EXCSTATE=1
the snap/load exception-state validator should trip FAST if the bug is real --
turning a multi-hour repro into seconds.  No yield happens inside the except
block, so (like the real bug) ONLY preemption can interrupt it there.
"""
import traceback

import harness
import runloom


def level3(H):
    runloom.yield_now()
    raise ValueError("deep")


def level2(H):
    runloom.yield_now()
    level3(H)


def level1(H):
    runloom.yield_now()
    level2(H)


def _spin(x):
    # A real frame entry per call (the eval-frame preempt wrapper fires here).
    return x + 1


EXPECTED_LAST = "level3"


def worker(H, wid, rng, state):
    while H.running():
        try:
            level1(H)
        except ValueError as exc:
            # HEAVY except block: stay here with `exc` live, entering many frames
            # so a preemption lands mid-except with high probability -> the
            # exception state gets snapped/restored on the busy path.  NO yield
            # in here (matches the real bug: only preempt interrupts an except).
            acc = 0
            for _ in range(4000):
                acc = _spin(acc)
            # Touch the traceback + message AFTER the churn -- if the exception
            # state was corrupted by a mid-except preempt snap/restore, this is
            # where it shows (corrupt frames / freed exc_value / SIGSEGV).
            frames = [f.name for f in traceback.extract_tb(exc.__traceback__)]
            if not H.check(frames and frames[-1] == EXPECTED_LAST,
                           "tb corrupt wid={0}: {1}".format(wid, frames)):
                return
            if not H.check(str(exc) == "deep",
                           "msg corrupt wid={0}: {1!r}".format(wid, str(exc))):
                return
            if acc != 4000:
                H.check(False, "spin corrupt wid={0}: {1}".format(wid, acc))
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p69b_excstress", body, default_funcs=8000,
                 describe="preempt-mid-except exception-state stress")
