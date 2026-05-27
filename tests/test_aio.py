"""Tests for pygo.aio (the async/await bridge)."""
import asyncio
import time
import unittest

import pygo.aio as aio


class TestBasicRun(unittest.TestCase):
    def test_simple_async_returns_value(self):
        async def main():
            return 42
        self.assertEqual(aio.run(main()), 42)

    def test_async_returning_none_is_fine(self):
        async def main():
            pass
        self.assertIsNone(aio.run(main()))

    def test_exception_propagates(self):
        async def main():
            raise ValueError("nope")
        with self.assertRaises(ValueError):
            aio.run(main())


class TestSleep(unittest.TestCase):
    def test_asyncio_sleep_actually_sleeps(self):
        async def main():
            t0 = time.monotonic()
            await asyncio.sleep(0.02)
            return time.monotonic() - t0
        elapsed = aio.run(main())
        self.assertGreaterEqual(elapsed, 0.015)
        self.assertLess(elapsed, 0.5)

    def test_asyncio_sleep_zero(self):
        async def main():
            await asyncio.sleep(0)
            return "ok"
        self.assertEqual(aio.run(main()), "ok")


class TestGather(unittest.TestCase):
    def test_gather_parallel_sleeps(self):
        # Three 0.02s sleeps in parallel should take ~0.02s, not 0.06s.
        async def slow(i):
            await asyncio.sleep(0.02)
            return i

        async def main():
            t0 = time.monotonic()
            results = await asyncio.gather(slow(1), slow(2), slow(3))
            return results, time.monotonic() - t0

        results, elapsed = aio.run(main())
        self.assertEqual(results, [1, 2, 3])
        self.assertLess(elapsed, 0.06)

    def test_gather_one_raises(self):
        async def good():
            return "ok"
        async def bad():
            raise RuntimeError("boom")
        async def main():
            return await asyncio.gather(good(), bad(), return_exceptions=True)
        results = aio.run(main())
        self.assertEqual(results[0], "ok")
        self.assertIsInstance(results[1], RuntimeError)


class TestTasksAndFutures(unittest.TestCase):
    def test_create_task(self):
        async def child():
            await asyncio.sleep(0.005)
            return "child-done"
        async def main():
            t = asyncio.create_task(child())
            self.assertFalse(t.done())
            return await t
        self.assertEqual(aio.run(main()), "child-done")

    def test_future_set_result_in_callback(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            loop.call_later(0.005, fut.set_result, "later")
            return await fut
        self.assertEqual(aio.run(main()), "later")

    def test_wait_for_timeout(self):
        async def slow():
            await asyncio.sleep(0.5)
            return "never"

        async def main():
            return await asyncio.wait_for(slow(), timeout=0.02)

        with self.assertRaises(asyncio.TimeoutError):
            aio.run(main())


class TestCancellation(unittest.TestCase):
    def test_cancel_task(self):
        cancelled = []

        async def slow():
            try:
                await asyncio.sleep(1.0)
                return "never"
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        async def main():
            t = asyncio.create_task(slow())
            await asyncio.sleep(0.01)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                return "cancelled"
            return "not cancelled"

        self.assertEqual(aio.run(main()), "cancelled")
        self.assertEqual(cancelled, [True])


class TestPrimitives(unittest.TestCase):
    def test_event(self):
        async def main():
            ev = asyncio.Event()

            async def setter():
                await asyncio.sleep(0.005)
                ev.set()

            asyncio.create_task(setter())
            await ev.wait()
            return "woken"

        self.assertEqual(aio.run(main()), "woken")

    def test_lock(self):
        async def main():
            lk = asyncio.Lock()
            counter = [0]

            async def worker():
                async with lk:
                    v = counter[0]
                    await asyncio.sleep(0.001)
                    counter[0] = v + 1

            await asyncio.gather(*[worker() for _ in range(10)])
            return counter[0]

        self.assertEqual(aio.run(main()), 10)

    def test_queue(self):
        async def main():
            q = asyncio.Queue(maxsize=3)
            out = []

            async def producer():
                for i in range(5):
                    await q.put(i)
                await q.put(None)

            async def consumer():
                while True:
                    item = await q.get()
                    if item is None:
                        return
                    out.append(item)

            await asyncio.gather(producer(), consumer())
            return out

        self.assertEqual(aio.run(main()), [0, 1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
