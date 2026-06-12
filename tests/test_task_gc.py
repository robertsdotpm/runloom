"""Regression tests for fiber/task lifetime under GC.

A runloom.aio task owns a fiber whose callable is the task's own bound
`_driver` method, so the C-level runloom_g_t holds  g->callable -> _driver ->
task.  Combined with task._g (a RunloomG wrapping that same runloom_g_t) this is a
reference cycle:

    task -> task._g (RunloomG) -> runloom_g_t -> g->callable (_driver) -> task

runloom_g_t is a plain C struct, invisible to cyclic GC, and RunloomG has no
tp_traverse, so the collector cannot see the g->callable edge.  Before the fix
a *completed* task therefore leaked forever.  The fix releases g->callable the
moment the fiber finishes (it is never called again), cutting the cycle at
the source so the task collects by plain refcounting.
"""
import gc
import asyncio
import unittest

import runloom.aio as aio


def _count(typename):
    return sum(1 for o in gc.get_objects() if type(o).__name__ == typename)


class TestTaskGC(unittest.TestCase):
    def test_completed_tasks_do_not_accumulate(self):
        """Inside one running loop, spawning + awaiting + dropping many tasks
        must not grow the live RunloomTask population.

        Measured as a delta from a baseline taken inside main(), so the count
        is unaffected by tasks other tests in the suite may have leaked (e.g.
        never-awaited background tasks force-killed at loop teardown -- a
        separate, frame-unwind issue, not the completed-task cycle this guards)."""
        async def child(n):
            return n * 2

        async def main():
            loop = asyncio.get_event_loop()
            gc.collect()
            base = _count("RunloomTask")
            deltas = []
            for _ in range(4):
                for i in range(150):
                    t = loop.create_task(child(i))
                    await t
                    del t
                gc.collect()
                deltas.append(_count("RunloomTask") - base)
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
        """A fiber whose callable closes over its own handle (the same
        shape as the task<->driver cycle) is reclaimed once it completes,
        because the fiber releases its callable at completion."""
        import weakref
        import runloom_c
        box = {}

        def run_once():
            cell = {}

            def body():
                # body's closure holds `cell`; cell holds the handle, whose
                # runloom_g_t holds body -> a cycle through the C struct.
                cell["g"] = runloom_c.current_g()
                return 7

            box["w"] = weakref.ref(body)
            runloom_c.go(body)
            runloom_c.run()
            del body

        run_once()
        gc.collect()
        self.assertIsNone(box["w"](), "fiber callable leaked (cycle held)")


if __name__ == "__main__":
    unittest.main()
