"""big_100 / 147 -- generator try/finally on cancel/close.

Generators with a try/finally (the finally bumps a per-worker 'cleaned' slot)
that yield internally.  Goroutines create a generator, partially consume it,
then close()/drop it under cancellation + load, while a driver forces
gc.collect() to finalize the dropped ones.  The finally MUST run for every
generator that is created -- whether it is explicitly close()'d, dropped and
GC-finalized, or exhausted.

Stresses: generator GeneratorExit / finalization under M:N, finally-on-close
correctness across migrations.
"""
import gc

import harness
import runloom


def setup(H):
    # Per-worker race-free slots: created (how many generators we made) and
    # cleaned (how many finally blocks ran).  Conservation: cleaned == created.
    H.state = {"created": [0] * H.funcs, "cleaned": [0] * H.funcs}


def worker(H, wid, rng, state):
    created = state["created"]
    cleaned = state["cleaned"]

    def counting_gen(n):
        # The finally MUST run on close(), on GC finalization of a dropped
        # generator, and on normal exhaustion.  It yields internally (a plain
        # generator `yield`); the WORKER blocks (yield_now/sleep) between the
        # next() pulls so the suspended generator frame spans hub migrations.
        # NB: deliberately NO scheduler call inside the generator -- a
        # GeneratorExit thrown into the frame by close()/GC-finalize must unwind
        # to the finally without re-entering the scheduler from a dealloc path.
        try:
            acc = 0
            for i in range(n):
                acc += i
                yield acc                   # suspend point inside the try
        finally:
            cleaned[wid] += 1               # own slot -> race-free

    for _ in H.round_range():
        n = rng.randint(2, 20)
        g = counting_gen(n)
        # Start the generator (>=1 next) so the try block is ENTERED -- only then
        # is the finally guaranteed to run on close/drop/exhaust.  A generator
        # that never started has no suspended frame and (correctly, per Python
        # semantics) runs no finally on close/drop, so counting it as "created"
        # would make the conservation check spuriously fail.  We count it as
        # created only after it is started.
        next(g)
        created[wid] += 1
        # Partially consume the rest: pull a random further prefix, blocking
        # between pulls so the generator frame spans hub migrations.
        pull = rng.randint(0, n - 1)
        try:
            for _i in range(pull):
                next(g)
                if rng.random() < 0.3:
                    runloom.sleep(0.0002)
        except StopIteration:
            pass
        # Three fates, all of which MUST run the finally exactly once:
        fate = rng.random()
        if fate < 0.4:
            g.close()                       # explicit cancel -> GeneratorExit
        elif fate < 0.7:
            # Drain to exhaustion (finally runs on normal completion).
            try:
                for _v in g:
                    pass
            except StopIteration:
                pass
        else:
            del g                           # drop -> GC finalizes -> finally runs
        if wid < 64 and rng.random() < 0.05:
            gc.collect()
        H.op(wid)
        H.task_done(wid)


def body(H):
    def gc_driver():
        # Aggressively finalize dropped generators so their finally runs
        # promptly (not just at process exit).
        while H.running():
            H.sleep(0.03)
            gc.collect()

    H.go(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Force finalization of any generators still pending (dropped but not yet
    # collected) so the conservation check is exact.  Under free-threading,
    # finalization of dropped generators can lag behind the drop (deferred /
    # biased refcount merge), so collect repeatedly until `cleaned` stops
    # advancing -- every dropped generator's finally must eventually run.
    created = sum(H.state["created"])
    prev = -1
    cleaned = sum(H.state["cleaned"])
    for _ in range(40):
        if cleaned == created or cleaned == prev:
            break
        prev = cleaned
        gc.collect()
        harness.REAL_SLEEP(0.05)        # let QSBR/biased-refcount merge advance
        cleaned = sum(H.state["cleaned"])
    H.check(created > 0, "no generators created")
    H.check(cleaned == created,
            "finally ran {0} times for {1} generators (a close/drop skipped its "
            "finally)".format(cleaned, created))
    H.log("created={0} cleaned={1}".format(created, cleaned))


if __name__ == "__main__":
    harness.main("p147_generator_finally_cancel", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="generator try/finally runs on close/drop/exhaust under "
                          "cancellation + GC")
