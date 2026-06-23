"""big_100 / 74 -- regular generator migration.

A goroutine creates a generator and pulls values from it with next(), BLOCKING
(sleep / yield) between every next() call.  The generator's frame is suspended
on the goroutine's stack across those blocks, possibly resuming on a different
hub thread; its values and final StopIteration must come out exactly right.

Stresses: generator frame state and ownership across goroutine switches.
"""
import harness
import runloom


def squares(n):
    acc = 0
    for i in range(n):
        acc += i
        yield i * i, acc


def worker(H, wid, rng, state):
    while H.running():
        n = rng.randint(3, 40)
        g = squares(n)
        expect_acc = 0
        for i in range(n):
            val, acc = next(g)
            expect_acc += i
            if not H.check(val == i * i and acc == expect_acc,
                           "generator value wrong wid={0}: {1},{2} at {3}"
                           .format(wid, val, acc, i)):
                return
            # Block between next() calls.
            if rng.random() < 0.5:
                runloom.yield_now()
            else:
                runloom.sleep(0.0003)
        # Generator must be exhausted now.
        try:
            next(g)
            H.fail("generator not exhausted wid={0}".format(wid))
            return
        except StopIteration:
            pass
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p74_generator_migration", body, default_funcs=4000,
                 describe="generators driven across goroutine blocks")
