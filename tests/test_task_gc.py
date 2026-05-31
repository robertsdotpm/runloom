"""Regression tests for goroutine/task lifetime under GC.

A pygo.aio task owns a goroutine whose callable is the task's own bound
`_driver` method, so the C-level pygo_g_t holds  g->callable -> _driver ->
task.  Combined with task._g (a PygoG wrapping that same pygo_g_t) this is a
reference cycle:

    task -> task._g (PygoG) -> pygo_g_t -> g->callable (_driver) -> task

pygo_g_t is a plain C struct, invisible to cyclic GC, and PygoG has no
tp_traverse, so the collector cannot see the g->callable edge.  Before the fix
a *completed* task therefore leaked forever.  The fix releases g->callable the
moment the goroutine finishes (it is never called again), cutting the cycle at
the source so the task collects by plain refcounting.
"""
import gc
import asyncio
import unittest

import pygo.aio as aio


def _count(typename):
    return sum(1 for o in gc.get_objects() if type(o).__name__ == typename)


class TestTaskGC(unittest.TestCase):
    def test_completed_tasks_do_not_accumulate(self):
        """Inside one running loop, spawning + awaiting + dropping many tasks
        must not grow the live PygoTask population.

        Measured as a delta from a baseline taken inside main(), so the count
        is unaffected by tasks other tests in the suite may have leaked (e.g.
        never-awaited background tasks force-killed at loop teardown -- a
        separate, frame-unwind issue, not the completed-task cycle this guards)."""
        async def child(n):
            return n * 2

        async def main():
            loop = asyncio.get_event_loop()
            gc.collect()
            base = _count("PygoTask")
            deltas = []
            for _ in range(4):
                for i in range(150):
                    t = loop.create_task(child(i))
                    await t
                    del t
                gc.collect()
                deltas.append(_count("PygoTask") - base)
            return deltas

        deltas = aio.run(main())
        # Each batch completes + drops 150 tasks; with the cycle broken they
        # all collect, so the live count returns to baseline every batch.  A
        # leak would show ~150 growth per batch (600 total).
        self.assertLessEqual(max(deltas), 5,
                             "completed tasks accumulating (delta/batch): %r" % (deltas,))

    def test_completed_task_is_collectable(self):
        """A completed task with no external refs is reclaimed."""
        import weakref
        box = {}

        async def child():
            return 123

        async def main():
            loop = asyncio.get_event_loop()
            t = loop.create_task(child())
            await t
            box["w"] = weakref.ref(t)
            del t

        aio.run(main())
        gc.collect()
        self.assertIsNone(box["w"](), "completed task was not collected (leak)")

    def test_callable_released_breaks_self_referential_closure(self):
        """A goroutine whose callable closes over its own handle (the same
        shape as the task<->driver cycle) is reclaimed once it completes,
        because the goroutine releases its callable at completion."""
        import weakref
        import pygo_core
        box = {}

        def run_once():
            cell = {}

            def body():
                # body's closure holds `cell`; cell holds the handle, whose
                # pygo_g_t holds body -> a cycle through the C struct.
                cell["g"] = pygo_core.current_g()
                return 7

            box["w"] = weakref.ref(body)
            pygo_core.go(body)
            pygo_core.run()
            del body

        run_once()
        gc.collect()
        self.assertIsNone(box["w"](), "goroutine callable leaked (cycle held)")


if __name__ == "__main__":
    unittest.main()
