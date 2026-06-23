"""big_100 / 73 -- async generator interaction.

Async generators are driven MANUALLY from sync goroutines (no asyncio, no aio
bridge): each __anext__() coroutine is stepped with .send(None), and the
goroutine yields between steps so the suspended async-gen frame is parked on the
goroutine's swapped stack.  We exercise normal iteration, an exception raised
inside the generator, and early aclose().

Stresses: async-generator frame finalization and state across goroutine
switches -- without the event loop.
"""
import harness
import runloom


async def agen(n, raise_at):
    for i in range(n):
        if i == raise_at:
            raise ValueError("agen-boom-{0}".format(i))
        yield i * i


def anext_value(ag):
    """Step the async gen once.  Returns ('val', x) | ('stop',) and may raise
    a real exception the generator body raised."""
    coro = ag.__anext__()
    try:
        coro.send(None)             # no real awaits -> resolves immediately
    except StopIteration as e:
        return ("val", e.value)
    except StopAsyncIteration:
        return ("stop",)


def drive_close(ag):
    coro = ag.aclose()
    try:
        coro.send(None)
    except StopIteration:
        pass


def worker(H, wid, rng, state):
    while H.running():
        # 1) full clean iteration
        n = rng.randint(2, 16)
        ag = agen(n, -1)
        out = []
        while True:
            runloom.yield_now()
            r = anext_value(ag)
            if r[0] == "stop":
                break
            out.append(r[1])
        if not H.check(out == [i * i for i in range(n)],
                       "async-gen values wrong wid={0}: {1}".format(wid, out)):
            return

        # 2) exception inside the generator propagates at the right point
        n2 = rng.randint(3, 16)
        ra = rng.randint(1, n2 - 1)
        ag2 = agen(n2, ra)
        seen = 0
        raised = False
        while True:
            try:
                r = anext_value(ag2)
            except ValueError as e:
                raised = ("agen-boom-{0}".format(ra) == str(e))
                break
            if r[0] == "stop":
                break
            seen += 1
        if not H.check(raised and seen == ra,
                       "async-gen raise wrong wid={0}: seen={1} ra={2} "
                       "raised={3}".format(wid, seen, ra, raised)):
            return

        # 3) early close
        ag3 = agen(10, -1)
        anext_value(ag3)
        drive_close(ag3)

        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p73_async_generator", body, default_funcs=3000,
                 describe="manually-driven async generators on goroutines")
