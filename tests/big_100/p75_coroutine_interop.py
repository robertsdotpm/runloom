"""big_100 / 75 -- coroutine object interop.

Native `async def` coroutines are created inside sync goroutines and driven
manually through a trivial adapter (no asyncio): the goroutine steps the
coroutine with .send(), feeding back values for each await, and yields to the
scheduler between steps.  The coroutine's frame state must survive being parked
on the goroutine's stack across those switches.

Stresses: coroutine object state + the goroutine scheduler, no event loop.
"""
import harness
import runloom


class Doubler(object):
    """An awaitable that yields its value and returns whatever is sent back."""
    def __init__(self, v):
        self.v = v

    def __await__(self):
        sent = yield self.v
        return sent


async def task(n):
    total = 0
    for i in range(n):
        r = await Doubler(i)
        total += r
    return total


def worker(H, wid, rng, state):
    while H.running():
        n = rng.randint(2, 24)
        expected = sum(i * 2 for i in range(n))      # driver sends back i*2
        coro = task(n)
        send = None
        got = None
        while True:
            try:
                yielded = coro.send(send)
            except StopIteration as e:
                got = e.value
                break
            send = yielded * 2          # "process" the awaited value
            runloom.yield_now()          # park the coroutine across a switch
        if not H.check(got == expected,
                       "coroutine result wrong wid={0}: {1} != {2}".format(
                           wid, got, expected)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p75_coroutine_interop", body, default_funcs=3000,
                 describe="manually-driven native coroutines on goroutines")
