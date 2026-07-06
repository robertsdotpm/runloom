"""Differential conformance: runloom's asyncio bridge vs STOCK CPython asyncio.

Item 8 of the systematic-improvement program (docs/dev/SYSTEMATIC_IMPROVEMENTS.md).

The oracle here is not a model and not a hand-written expectation -- it is stock
CPython itself.  Each scenario is an ordinary async program that records an
observable trace (callback fire order, awaited results, exception types, timeout
outcomes).  We run the IDENTICAL program twice -- once under `asyncio.run`, once
under `runloom.aio.run` -- and assert the traces match.  A divergence is a
conformance bug in the bridge, surfaced deterministically instead of via some
downstream library (the historical path: aiocsv/falcon/websockets deadlocks and
BlockingIOErrors that took days to trace back to send()/call_soon/done-callback
ordering).

Directly covers the ordering/protocol class (bugs 99/100/102/107/117/118/119 in
the appendix): every scenario is deterministic under stock asyncio, so any
mismatch is a real semantic divergence, not scheduling noise.

Run standalone or via tests/run_isolated.py.  House style: %/.format, prints kept.
"""
import asyncio
import sys

sys.path.insert(0, "src")

import runloom.aio as paio


# --------------------------------------------------------------------------
# Scenarios.  Each is a zero-arg factory returning a FRESH coroutine whose
# return value IS the observable trace.  Fresh per run so the two runners never
# share state.  Everything here is deterministic under stock asyncio.
# --------------------------------------------------------------------------

def sc_call_soon_fifo():
    """call_soon callbacks fire in FIFO submission order (bug 118)."""
    async def run():
        ev = []
        loop = asyncio.get_running_loop()
        for i in range(6):
            loop.call_soon(lambda i=i: ev.append(i))
        # let the ready queue drain
        for _ in range(3):
            await asyncio.sleep(0)
        return ev
    return run()


def sc_done_callback_order():
    """Future done-callbacks fire in add order, after the result is set (bug 119)."""
    async def run():
        ev = []
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        for i in range(5):
            fut.add_done_callback(lambda f, i=i: ev.append(("cb", i)))
        loop.call_soon(lambda: (ev.append(("set",)), fut.set_result("R")))
        for _ in range(4):
            await asyncio.sleep(0)
        ev.append(("result", fut.result()))
        return ev
    return run()


def sc_gather_result_order():
    """gather preserves argument order in results regardless of completion
    order (bug 99): a later-listed task finishing first must still land last."""
    async def run():
        ev = []

        async def worker(name, hops):
            for _ in range(hops):
                await asyncio.sleep(0)
            ev.append(("done", name))
            return name

        # b finishes first (fewer hops) but must appear 2nd in results
        results = await asyncio.gather(worker("a", 3), worker("b", 1),
                                       worker("c", 2))
        ev.append(("results", tuple(results)))
        return ev
    return run()


def sc_wait_for_timeout():
    """wait_for on a never-completing awaitable raises TimeoutError and the
    inner task is cancelled (bug 107)."""
    async def run():
        ev = []

        async def forever():
            try:
                await asyncio.sleep(1000)
            except asyncio.CancelledError:
                ev.append(("inner_cancelled",))
                raise

        try:
            await asyncio.wait_for(forever(), timeout=0.01)
            ev.append(("no_timeout",))
        except asyncio.TimeoutError:
            ev.append(("timeout",))
        return ev
    return run()


def sc_cancel_propagation():
    """Cancelling a task delivers CancelledError at its await point and the
    finally block runs before the cancellation surfaces to the awaiter (bug 102)."""
    async def run():
        ev = []

        async def child():
            try:
                await asyncio.sleep(1000)
            finally:
                ev.append(("child_finally",))

        loop = asyncio.get_running_loop()
        t = loop.create_task(child())
        await asyncio.sleep(0)          # let child reach its await
        t.cancel()
        try:
            await t
            ev.append(("awaited_ok",))
        except asyncio.CancelledError:
            ev.append(("awaiter_saw_cancel",))
        return ev
    return run()


def sc_send_none_protocol():
    """The driver resumes a coroutine with send(None); a custom awaitable whose
    __await__ yields a future and inspects the resume value must see the future's
    result flow through normally, not a bogus send(result) (bug 117)."""
    async def run():
        ev = []
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(lambda: fut.set_result(777))

        class Awaitable:
            def __await__(self):
                v = yield from fut.__await__()
                ev.append(("await_delivered", v))
                return v * 2

        r = await Awaitable()
        ev.append(("await_ret", r))
        return ev
    return run()


def sc_call_soon_threadsafe_ordering():
    """call_soon after an await runs before a later-scheduled sleep resumes,
    preserving strict ready-queue ordering (bug 118 sibling)."""
    async def run():
        ev = []
        loop = asyncio.get_running_loop()
        loop.call_soon(lambda: ev.append("early"))
        await asyncio.sleep(0)
        loop.call_soon(lambda: ev.append("late1"))
        loop.call_soon(lambda: ev.append("late2"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return ev
    return run()


def sc_semaphore_fairness():
    """A bounded semaphore hands off to waiters in FIFO order (bug 100)."""
    async def run():
        ev = []
        sem = asyncio.Semaphore(1)

        async def worker(name):
            async with sem:
                ev.append(("enter", name))
                await asyncio.sleep(0)
                ev.append(("exit", name))

        await asyncio.gather(worker("w1"), worker("w2"), worker("w3"))
        return ev
    return run()


SCENARIOS = {
    "call_soon_fifo": sc_call_soon_fifo,
    "done_callback_order": sc_done_callback_order,
    "gather_result_order": sc_gather_result_order,
    "wait_for_timeout": sc_wait_for_timeout,
    "cancel_propagation": sc_cancel_propagation,
    "send_none_protocol": sc_send_none_protocol,
    "call_soon_threadsafe_ordering": sc_call_soon_threadsafe_ordering,
    "semaphore_fairness": sc_semaphore_fairness,
}


# --------------------------------------------------------------------------
# Runners
# --------------------------------------------------------------------------

def observe(runner, factory):
    """Run one scenario under `runner` (asyncio.run or paio.run); return a
    comparable (trace, exception-signature) pair.  A raised exception is folded
    into the signature so 'both raise TimeoutError' compares equal and 'one
    hangs/raises, the other returns' compares unequal."""
    try:
        trace = runner(factory())
        return (trace, None)
    except BaseException as e:                    # noqa: BLE001 - we compare it
        return (None, "%s: %s" % (type(e).__name__, e))


def run_scenario(name):
    factory = SCENARIOS[name]
    stock = observe(asyncio.run, factory)
    runloom = observe(paio.run, factory)
    return stock, runloom


def main():
    failures = []
    for name in SCENARIOS:
        stock, runloom = run_scenario(name)
        ok = stock == runloom
        print("  %-32s %s" % (name, "OK" if ok else "DIVERGES"))
        if not ok:
            print("      stock  : %r" % (stock,))
            print("      runloom: %r" % (runloom,))
            failures.append(name)
    if failures:
        print("DIFFERENTIAL FAIL: %d/%d scenarios diverge from stock asyncio: %s"
              % (len(failures), len(SCENARIOS), ", ".join(failures)))
        return 1
    print("all %d scenarios conform to stock asyncio" % len(SCENARIOS))
    return 0


# pytest entry points ------------------------------------------------------
def _mk(name):
    def test(_name=name):
        stock, runloom = run_scenario(_name)
        assert stock == runloom, (
            "scenario %r diverges from stock asyncio:\n  stock  : %r\n  runloom: %r"
            % (_name, stock, runloom))
    test.__name__ = "test_diff_" + name
    return test


for _n in SCENARIOS:
    globals()["test_diff_" + _n] = _mk(_n)


if __name__ == "__main__":
    sys.exit(main())
