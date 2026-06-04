"""big_100 / 69 -- traceback integrity test.

A chain of nested functions, each yielding before calling the next, finally
raises at the bottom.  The goroutine catches it and walks the traceback,
verifying the frames are exactly the expected functions in the expected order
-- the stack switching must not corrupt the frame chain.

Stresses: frame chain / cframe state, traceback construction after yields.
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


EXPECTED = ["worker", "level1", "level2", "level3"]


def worker(H, wid, rng, state):
    while H.running():
        try:
            level1(H)
        except ValueError as exc:
            frames = [f.name for f in traceback.extract_tb(exc.__traceback__)]
            if not H.check(frames == EXPECTED,
                           "traceback corrupted wid={0}: {1}".format(
                               wid, frames)):
                return
            # The message must survive too.
            if not H.check(str(exc) == "deep",
                           "exception message corrupted wid={0}: {1!r}".format(
                               wid, str(exc))):
                return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p69_traceback_integrity", body, default_funcs=4000,
                 describe="traceback frame chain stays sane across yields")
