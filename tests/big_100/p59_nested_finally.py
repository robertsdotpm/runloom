"""big_100 / 59 -- nested try/finally cleanup.

A goroutine descends through deeply nested try/finally frames, doing a
cancellable wait at each level.  Cancellation can strike at any depth; however
it unwinds, every frame that was ENTERED must run its finally block exactly
once -- no missed or double cleanup.

Stresses: frame state, stack unwinding through the cooperative scheduler.
"""
import harness
import cancelutil

MAXDEPTH = 40


class Cancelled(Exception):
    pass


def descend(H, ctx, depth, counters):
    counters["entered"][0] += 1
    try:
        if depth > 0:
            # A cancellable pause; if cancelled, raise to unwind the whole nest.
            if not cancelutil.cancellable_sleep(ctx, 0.0005):
                raise Cancelled()
            descend(H, ctx, depth - 1, counters)
    finally:
        counters["finalized"][0] += 1


def worker(H, wid, rng, state):
    while H.running():
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        if rng.random() < 0.6:
            H.fiber(cancelutil.delayed_cancel, cancel,
                 rng.uniform(0.0, 0.01))
        counters = {"entered": [0], "finalized": [0]}
        try:
            descend(H, ctx, rng.randint(1, MAXDEPTH), counters)
        except Cancelled:
            pass
        finally:
            cancel()
        if not H.check(counters["entered"][0] == counters["finalized"][0],
                       "finally mismatch wid={0}: entered={1} finalized={2}"
                       .format(wid, counters["entered"][0],
                               counters["finalized"][0])):
            return
        H.op(wid, counters["entered"][0])
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p59_nested_finally", body, default_funcs=2000,
                 describe="cancellation at any depth; every finally runs once")
