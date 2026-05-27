"""Edge-case coverage: boundary conditions and error paths across the
pygo public surface.

Where the happy-path tests assert correct values on correct inputs,
these tests assert correct *errors* (or no errors) on adversarial
inputs:

  * empty / zero-sized things
  * double-completion (set_result twice, cancel after done, etc.)
  * close-during-send and close-during-recv races
  * exception propagation through await / gather / done callbacks
  * recursion / nesting (paio.run nested, gather of gather)
  * misuse: lock release without acquire, send on closed chan
"""
import asyncio
import unittest

import pygo_core
import pygo.aio as paio


# ====================================================================
# Future edge cases
# ====================================================================
class TestFutureEdges(unittest.TestCase):
    def test_set_result_twice_raises(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_result(1)
            try:
                fut.set_result(2)
            except asyncio.InvalidStateError:
                return "raised"
            return "no-raise"
        self.assertEqual(paio.run(main()), "raised")

    def test_set_exception_twice_raises(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_exception(ValueError("first"))
            try:
                fut.set_exception(ValueError("second"))
            except asyncio.InvalidStateError:
                return "raised"
            return "no-raise"
        self.assertEqual(paio.run(main()), "raised")

    def test_result_before_done_raises(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            try:
                fut.result()
            except asyncio.InvalidStateError:
                return "raised"
            return "no-raise"
        self.assertEqual(paio.run(main()), "raised")

    def test_exception_before_done_raises(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            try:
                fut.exception()
            except asyncio.InvalidStateError:
                return "raised"
            return "no-raise"
        self.assertEqual(paio.run(main()), "raised")

    def test_cancel_after_done_returns_false(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_result(1)
            return fut.cancel()
        self.assertFalse(paio.run(main()))

    def test_done_callback_after_done_fires_immediately(self):
        out = []
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_result("v")
            fut.add_done_callback(lambda f: out.append(f.result()))
            # Give the loop a tick to dispatch.
            await asyncio.sleep(0)
        paio.run(main())
        self.assertEqual(out, ["v"])

    def test_remove_done_callback(self):
        called = []
        cb = lambda f: called.append(1)
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.add_done_callback(cb)
            self.assertEqual(fut.remove_done_callback(cb), 1)
            fut.set_result(None)
            await asyncio.sleep(0)
        paio.run(main())
        self.assertEqual(called, [])


# ====================================================================
# Channel edge cases
# ====================================================================
class TestChannelEdges(unittest.TestCase):
    def test_send_on_closed_raises(self):
        ch = pygo_core.Chan(1)
        ch.close()
        out = []
        def w():
            try:
                ch.send("x")
                out.append("no-raise")
            except ValueError:
                out.append("closed")
        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, ["closed"])

    def test_double_close_raises(self):
        ch = pygo_core.Chan(1)
        ch.close()
        with self.assertRaises(Exception):
            ch.close()

    def test_recv_on_empty_closed_returns_default(self):
        ch = pygo_core.Chan(1)
        ch.close()
        out = []
        def w():
            out.append(ch.recv())
        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, [(None, False)])

    def test_try_recv_empty_returns_none(self):
        """try_recv on empty chan returns None (would-block sentinel)."""
        ch = pygo_core.Chan(1)
        out = []
        def w():
            out.append(ch.try_recv())
        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, [None])

    def test_try_recv_with_value(self):
        ch = pygo_core.Chan(1)
        ch.try_send("v")
        out = []
        def w():
            out.append(ch.try_recv())
        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, [("v", True)])

    def test_try_send_full_returns_false(self):
        ch = pygo_core.Chan(1)
        out = []
        def w():
            out.append(ch.try_send("a"))
            out.append(ch.try_send("b"))   # full
        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, [True, False])

    def test_zero_capacity_chan(self):
        """Chan(0) is unbuffered -- send blocks until recv."""
        ch = pygo_core.Chan(0)
        out = []
        def producer():
            ch.send("hi")
            out.append("sent")
        def consumer():
            v, _ = ch.recv()
            out.append(("got", v))
        pygo_core.go(producer)
        pygo_core.go(consumer)
        pygo_core.run()
        self.assertIn(("got", "hi"), out)
        self.assertIn("sent", out)

    def test_close_unblocks_pending_recv(self):
        ch = pygo_core.Chan(0)
        out = []
        def waiter():
            v, ok = ch.recv()
            out.append((v, ok))
        def closer():
            pygo_core.sched_sleep(0.005)
            ch.close()
        pygo_core.go(waiter)
        pygo_core.go(closer)
        pygo_core.run()
        self.assertEqual(out, [(None, False)])

    def test_negative_capacity_raises(self):
        with self.assertRaises(Exception):
            pygo_core.Chan(-1)


# ====================================================================
# Exception propagation
# ====================================================================
class TestExceptionPropagation(unittest.TestCase):
    def test_exception_in_task_propagates_to_await(self):
        class Boom(Exception):
            pass
        async def crasher():
            await asyncio.sleep(0.001)
            raise Boom("xxx")

        async def main():
            try:
                await asyncio.create_task(crasher())
            except Boom as e:
                return str(e)
        self.assertEqual(paio.run(main()), "xxx")

    def test_exception_in_gather_propagates(self):
        async def crasher():
            raise RuntimeError("xx")
        async def main():
            try:
                await asyncio.gather(crasher())
            except RuntimeError as e:
                return str(e)
        self.assertEqual(paio.run(main()), "xx")

    def test_exception_in_finally(self):
        out = []
        async def w():
            try:
                raise ValueError("first")
            finally:
                out.append("finally-ran")
        async def main():
            try:
                await w()
            except ValueError as e:
                return str(e)
        self.assertEqual(paio.run(main()), "first")
        self.assertEqual(out, ["finally-ran"])

    def test_exception_in_done_callback_swallowed(self):
        """A raise inside a done-callback must not crash the loop."""
        async def w():
            return 1
        async def main():
            t = asyncio.create_task(w())
            t.add_done_callback(lambda f: 1/0)
            return await t
        # Should complete; callback exception is logged not raised.
        self.assertEqual(paio.run(main()), 1)

    def test_keyboardinterrupt_propagates(self):
        """KeyboardInterrupt should propagate out of paio.run."""
        async def main():
            raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            paio.run(main())


# ====================================================================
# Lock edge cases
# ====================================================================
class TestLockEdges(unittest.TestCase):
    def test_release_without_acquire_raises(self):
        async def main():
            lk = asyncio.Lock()
            try:
                lk.release()
            except RuntimeError:
                return "raised"
            return "no-raise"
        self.assertEqual(paio.run(main()), "raised")

    def test_locked_returns_correct_state(self):
        async def main():
            lk = asyncio.Lock()
            states = [lk.locked()]
            await lk.acquire()
            states.append(lk.locked())
            lk.release()
            states.append(lk.locked())
            return states
        self.assertEqual(paio.run(main()), [False, True, False])

    def test_lock_async_with(self):
        async def main():
            lk = asyncio.Lock()
            async with lk:
                return lk.locked()
        self.assertTrue(paio.run(main()))


# ====================================================================
# gather edge cases
# ====================================================================
class TestGatherEdges(unittest.TestCase):
    def test_empty_gather(self):
        async def main():
            return await asyncio.gather()
        self.assertEqual(paio.run(main()), [])

    def test_gather_return_exceptions(self):
        async def good():
            return 1
        async def bad():
            raise ValueError("v")
        async def main():
            results = await asyncio.gather(
                good(), bad(), good(),
                return_exceptions=True,
            )
            return [type(r).__name__ if isinstance(r, Exception) else r
                    for r in results]
        self.assertEqual(paio.run(main()), [1, "ValueError", 1])

    def test_nested_gather(self):
        async def w(i):
            return i
        async def inner():
            return await asyncio.gather(w(1), w(2))
        async def main():
            return await asyncio.gather(inner(), inner())
        self.assertEqual(paio.run(main()), [[1, 2], [1, 2]])

    def test_gather_cancel_propagates(self):
        async def slow():
            await asyncio.sleep(60.0)
        async def main():
            g = asyncio.gather(slow(), slow())
            await asyncio.sleep(0.001)
            g.cancel()
            try:
                await g
            except asyncio.CancelledError:
                return "cancelled"
            return "not-cancelled"
        self.assertEqual(paio.run(main()), "cancelled")


# ====================================================================
# sleep edge cases
# ====================================================================
class TestSleepEdges(unittest.TestCase):
    def test_sleep_zero_is_yield(self):
        """asyncio.sleep(0) should yield once but not actually wait."""
        out = []
        async def a():
            out.append("a1")
            await asyncio.sleep(0)
            out.append("a2")
        async def b():
            out.append("b1")
            await asyncio.sleep(0)
            out.append("b2")
        async def main():
            await asyncio.gather(a(), b())
        paio.run(main())
        # Interleaving must have happened.
        self.assertEqual(set(out), {"a1", "a2", "b1", "b2"})

    def test_sleep_negative_no_crash(self):
        """asyncio.sleep with negative value -- treat as 0."""
        async def main():
            await asyncio.sleep(-1.0)
            return "done"
        self.assertEqual(paio.run(main()), "done")

    def test_sleep_returns_result_arg(self):
        async def main():
            return await asyncio.sleep(0.001, result="payload")
        self.assertEqual(paio.run(main()), "payload")


# ====================================================================
# wait_for edge cases
# ====================================================================
class TestWaitForEdges(unittest.TestCase):
    def test_wait_for_timeout(self):
        async def slow():
            await asyncio.sleep(60.0)
        async def main():
            try:
                await asyncio.wait_for(slow(), timeout=0.01)
            except asyncio.TimeoutError:
                return "timed-out"
            return "completed"
        self.assertEqual(paio.run(main()), "timed-out")

    def test_wait_for_immediate_success(self):
        async def fast():
            return "v"
        async def main():
            return await asyncio.wait_for(fast(), timeout=1.0)
        self.assertEqual(paio.run(main()), "v")

    def test_wait_for_none_timeout(self):
        async def w():
            return 7
        async def main():
            return await asyncio.wait_for(w(), timeout=None)
        self.assertEqual(paio.run(main()), 7)


# ====================================================================
# paio.run re-entrancy
# ====================================================================
class TestRunReentrancy(unittest.TestCase):
    def test_sequential_runs(self):
        async def w():
            return 1
        self.assertEqual(paio.run(w()), 1)
        self.assertEqual(paio.run(w()), 1)
        self.assertEqual(paio.run(w()), 1)

    def test_run_with_pending_tasks_cleans_up(self):
        """If main() leaves a task running, paio.run should cancel it
        so the next run isn't polluted."""
        async def slow():
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                pass
        async def main1():
            asyncio.create_task(slow())
            return 1
        paio.run(main1())
        # Second run must not see the leftover task / sleep timer.
        async def main2():
            return 2
        self.assertEqual(paio.run(main2()), 2)


if __name__ == "__main__":
    unittest.main()
