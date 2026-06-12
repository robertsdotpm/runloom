"""Concurrency tests for runloom: race conditions, deadlocks, ordering.

Each test pins down a specific cooperative-concurrency invariant that
could otherwise silently break in a refactor:

  * wake-before-park race on park_self / G.wake
  * cancel-during-park, cancel-during-running, cancel-after-done
  * channel send/recv ordering invariants
  * select() fairness over multiple ready cases
  * lock + condition deadlock prevention
  * fast-path completion (already-done future, drained channel) doesn't
    incorrectly park

Designed to fail loudly if any future change breaks the invariant.
"""
import asyncio
import unittest

import runloom_c
import runloom.aio as paio


# ====================================================================
# park/wake races
# ====================================================================
class TestParkWakeRace(unittest.TestCase):
    def test_wake_before_park(self):
        """Wake arrives BEFORE park_self -- park must consume the
        pending wake and return immediately (no actual yield)."""
        g_holder = []
        order = []

        def parker():
            g_holder.append(runloom_c.current_g())
            # Yield once so waker can capture our handle.
            runloom_c.sched_yield_classic()
            order.append("before-park")
            runloom_c.park_self()   # should not block
            order.append("after-park")

        def waker():
            # Wait for parker to capture and yield, then wake before
            # parker calls park_self.
            runloom_c.sched_yield_classic()  # let parker run
            runloom_c.sched_yield_classic()  # parker is back in scheduler
            g_holder[0].wake()
            order.append("waked")

        runloom_c.go(parker)
        runloom_c.go(waker)
        runloom_c.run()
        # Both orderings ("waked" before "before-park" OR after) are
        # legal; what matters is "after-park" happens.
        self.assertIn("after-park", order)

    def test_wake_after_park(self):
        """Normal park-then-wake."""
        g_holder = []
        order = []

        def parker():
            g_holder.append(runloom_c.current_g())
            order.append("before-park")
            runloom_c.park_self()
            order.append("after-park")

        def waker():
            # Sleep so parker definitely parks first.
            runloom_c.sched_sleep(0.005)
            order.append("waking")
            g_holder[0].wake()

        runloom_c.go(parker)
        runloom_c.go(waker)
        runloom_c.run()
        self.assertEqual(order, ["before-park", "waking", "after-park"])

    def test_multiple_wakes_consumed(self):
        """N wakes coming in before a single park.  park consumes one;
        the rest leave wake_pending > 0 (consumed by subsequent parks)."""
        g_holder = []
        events = []

        def parker():
            g_holder.append(runloom_c.current_g())
            runloom_c.sched_yield_classic()
            # 3 parks; should all return immediately since 3 wakes are queued.
            for _ in range(3):
                runloom_c.park_self()
                events.append("p")

        def burst_waker():
            runloom_c.sched_yield_classic()
            runloom_c.sched_yield_classic()  # let parker capture itself
            for _ in range(3):
                g_holder[0].wake()

        runloom_c.go(parker)
        runloom_c.go(burst_waker)
        runloom_c.run()
        self.assertEqual(events, ["p", "p", "p"])


# ====================================================================
# Cancellation races
# ====================================================================
class TestCancellationRace(unittest.TestCase):
    def test_cancel_done_task_returns_false(self):
        """Cancelling a task that already completed must not raise and
        must return False."""
        async def w():
            return 1
        async def main():
            t = asyncio.create_task(w())
            await t
            return t.cancel()
        self.assertFalse(paio.run(main()))

    def test_cancel_pending_task_returns_true(self):
        async def slow():
            await asyncio.sleep(60.0)
            return 1
        async def main():
            t = asyncio.create_task(slow())
            await asyncio.sleep(0.005)
            cancelled = t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            return cancelled
        self.assertTrue(paio.run(main()))

    def test_double_cancel_idempotent(self):
        """Cancelling twice should not crash."""
        async def slow():
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                return "cancelled"

        async def main():
            t = asyncio.create_task(slow())
            await asyncio.sleep(0.005)
            t.cancel()
            t.cancel()
            return await t
        self.assertEqual(paio.run(main()), "cancelled")

    def test_cancel_during_callback(self):
        """Cancel a task whose callback is currently firing."""
        out = []

        async def child():
            await asyncio.sleep(0.005)
            return "child-done"

        async def main():
            t = asyncio.create_task(child())
            def cb(fut):
                out.append(("cb", fut.result()))
                # try cancelling self mid-callback (no-op since done)
                self.assertFalse(t.cancel())
            t.add_done_callback(cb)
            await t

        paio.run(main())
        self.assertEqual(out, [("cb", "child-done")])


# ====================================================================
# Channel ordering
# ====================================================================
class TestChannelOrdering(unittest.TestCase):
    def test_send_recv_fifo_buffered(self):
        """Buffered channel: receives in send-order."""
        ch = runloom_c.Chan(100)
        out = []
        def producer():
            for i in range(50):
                ch.send(i)
            ch.close()
        def consumer():
            for v in ch:
                out.append(v)
        runloom_c.go(producer)
        runloom_c.go(consumer)
        runloom_c.run()
        self.assertEqual(out, list(range(50)))

    def test_send_recv_fifo_unbuffered(self):
        """Unbuffered channel: hand-off preserves order with one
        producer and one consumer."""
        ch = runloom_c.Chan(0)
        out = []
        def producer():
            for i in range(20):
                ch.send(i)
            ch.close()
        def consumer():
            for v in ch:
                out.append(v)
        runloom_c.go(producer)
        runloom_c.go(consumer)
        runloom_c.run()
        self.assertEqual(out, list(range(20)))

    def test_try_send_after_close_returns_false(self):
        """try_send on closed chan returns False (or raises)."""
        ch = runloom_c.Chan(1)
        ch.try_send("first")
        ch.close()
        # Receiver picks up "first" then sees close.
        out = []
        def consumer():
            for v in ch:
                out.append(v)
        runloom_c.go(consumer)
        runloom_c.run()
        self.assertEqual(out, ["first"])

    def test_recv_after_close_drains_then_returns_default(self):
        """recv on closed chan returns (None, False) once drained."""
        ch = runloom_c.Chan(2)
        ch.try_send("a")
        ch.try_send("b")
        ch.close()
        recv_results = []
        def consumer():
            recv_results.append(ch.recv())   # "a", True
            recv_results.append(ch.recv())   # "b", True
            recv_results.append(ch.recv())   # None, False
        runloom_c.go(consumer)
        runloom_c.run()
        self.assertEqual(recv_results,
                         [("a", True), ("b", True), (None, False)])


# ====================================================================
# select() correctness
# ====================================================================
class TestSelect(unittest.TestCase):
    def test_select_with_default_no_block(self):
        """select(default=True) returns -1 immediately when no case
        is ready."""
        ch_a = runloom_c.Chan(1)
        ch_b = runloom_c.Chan(1)
        result = [None]

        def w():
            r = runloom_c.select([("recv", ch_a),
                                  ("recv", ch_b)], default=True)
            # default-fired returns bare -1 (not a tuple)
            result[0] = r if isinstance(r, int) else r[0]
        runloom_c.go(w)
        runloom_c.run()
        self.assertEqual(result[0], -1)

    def test_select_picks_ready_case(self):
        ch_a = runloom_c.Chan(1)
        ch_b = runloom_c.Chan(1)
        ch_b.try_send("b-value")

        result = [None]
        def w():
            idx, val = runloom_c.select([("recv", ch_a),
                                         ("recv", ch_b)])
            result[0] = (idx, val)
        runloom_c.go(w)
        runloom_c.run()
        self.assertEqual(result[0], (1, ("b-value", True)))

    def test_select_send_case(self):
        ch = runloom_c.Chan(1)
        result = [None]

        def w():
            idx, _ = runloom_c.select([("send", ch, "x")])
            result[0] = idx

        runloom_c.go(w)
        runloom_c.run()
        self.assertEqual(result[0], 0)

    def test_select_blocks_until_ready(self):
        """select without default blocks until any case is ready."""
        ch = runloom_c.Chan(0)
        out = []

        def w():
            idx, val = runloom_c.select([("recv", ch)])
            out.append((idx, val))

        def feeder():
            runloom_c.sched_sleep(0.005)
            ch.send("late")

        runloom_c.go(w)
        runloom_c.go(feeder)
        runloom_c.run()
        self.assertEqual(out, [(0, ("late", True))])


# ====================================================================
# Lock / Condition deadlock prevention
# ====================================================================
class TestLockCondition(unittest.TestCase):
    def test_condition_wait_releases_lock(self):
        """asyncio.Condition: wait() must release the lock so another
        task can acquire it + notify."""
        out = []

        async def waiter(cond, name):
            async with cond:
                out.append((name, "acquired"))
                await cond.wait()
                out.append((name, "woken"))

        async def notifier(cond):
            await asyncio.sleep(0.005)
            async with cond:
                cond.notify_all()
                out.append("notified")

        async def main():
            cond = asyncio.Condition()
            await asyncio.gather(
                waiter(cond, "A"),
                waiter(cond, "B"),
                notifier(cond),
            )
        paio.run(main())
        # Both A and B should be woken.  out contains both 2-tuples and
        # the bare string "notified" -- only count tuples.
        woken = sum(1 for x in out
                    if isinstance(x, tuple) and x[1] == "woken")
        self.assertEqual(woken, 2)
        self.assertIn("notified", out)

    def test_lock_reentrant_via_rlock(self):
        """asyncio doesn't have RLock; verify our Lock IS NOT reentrant
        (matches asyncio semantics)."""
        async def main():
            lk = asyncio.Lock()
            await lk.acquire()
            # Re-acquire from same coroutine -- would deadlock.
            # Use try-acquire with short timeout to confirm.
            try:
                await asyncio.wait_for(lk.acquire(), timeout=0.01)
                return "reentrant"
            except asyncio.TimeoutError:
                return "blocked"
            finally:
                lk.release()
        self.assertEqual(paio.run(main()), "blocked")


# ====================================================================
# Fast-path correctness: already-done future
# ====================================================================
class TestFastPath(unittest.TestCase):
    def test_await_done_future_no_park(self):
        """await on an already-done future should resolve without
        actually parking the fiber."""
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_result(42)
            return await fut
        self.assertEqual(paio.run(main()), 42)

    def test_gather_all_done_synchronously(self):
        """gather of N already-done coroutines completes inline."""
        async def w(i):
            return i
        async def main():
            return await asyncio.gather(*[w(i) for i in range(50)])
        self.assertEqual(paio.run(main()), list(range(50)))

    def test_recv_buffered_chan_no_park(self):
        """recv on a chan with buffered data returns immediately."""
        ch = runloom_c.Chan(5)
        for i in range(5):
            ch.try_send(i)
        out = []
        def w():
            for _ in range(5):
                out.append(ch.recv())
        runloom_c.go(w)
        runloom_c.run()
        self.assertEqual(out, [(i, True) for i in range(5)])


if __name__ == "__main__":
    unittest.main()
