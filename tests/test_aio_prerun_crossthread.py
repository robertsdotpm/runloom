"""R7 item 2: foreign-thread PRE-RUN scheduling on a runloom asyncio loop.

If a foreign thread calls call_soon / call_later / create_task on a loop BEFORE
it starts running, the work used to spawn onto the foreign thread's own
scheduler (never drained by the loop) and was silently lost.  The fix
(DESIGN_loop_run_prerun_scheduling.md): claim the driver thread at run entry
(_pg_driver_tid), route foreign pre-run work into the loop's _ts_queue, and give
RunloomTask a deferred-spawn mode so pre-run create_task is non-blocking.

These are the gated promotion of tests/bughunt_repros/r01 + the cases the adversarial
review of the design flagged: both callback orderings, the schedule-then-start-
driver pattern (no deadlock), the driver's own pre-run create_task, a custom
task_factory, cancel-before-spawn, and the refcycle break.
"""
import gc
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import asyncio
import runloom.aio as aio


def _run_on_worker(loop, timeout=10.0):
    """Drive `loop.run_until_complete(_stopper)` on a fresh worker thread, where
    _stopper just lets the loop spin briefly so any pre-scheduled work runs, then
    stops.  Returns after the worker finishes (mirrors the classic 'main thread
    schedules, worker drives' pattern)."""
    done = threading.Event()
    err = {}

    async def _stopper():
        # give pre-run-scheduled work a few loop turns to run
        for _ in range(20):
            await asyncio.sleep(0.005)

    def _drive():
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_stopper())
        except BaseException as e:  # noqa
            err["e"] = e
        finally:
            done.set()

    t = threading.Thread(target=_drive)
    t.start()
    done.wait(timeout)
    assert done.is_set(), "worker never finished (deadlock?)"
    if "e" in err:
        raise err["e"]


def test_prerun_foreign_call_soon_and_create_task_run_in_order():
    """The r01 bug: main thread schedules call_soon THEN create_task, worker
    thread runs the loop.  Both must run, in order ['cb', 'task']."""
    loop = aio.RunloomEventLoop()
    results = []

    def cb():
        results.append("cb")

    async def task_body():
        results.append("task")

    loop.call_soon(cb)                 # foreign (main) thread, loop not running
    loop.create_task(task_body())      # foreign (main) thread, loop not running
    _run_on_worker(loop)
    loop.close()
    assert results == ["cb", "task"], results


def test_prerun_reverse_order_create_task_then_call_soon():
    """The ordering the r01 repro does NOT test (must-fix #1): create_task THEN
    call_soon.  Stock asyncio runs the task's first step then the callback."""
    loop = aio.RunloomEventLoop()
    results = []

    async def task_body():
        results.append("task")

    def cb():
        results.append("cb")

    loop.create_task(task_body())
    loop.call_soon(cb)
    _run_on_worker(loop)
    loop.close()
    assert results == ["task", "cb"], results


def test_prerun_call_later_runs_not_lost():
    """A foreign pre-run call_later timer must fire, not strand on the foreign
    thread's sched."""
    loop = aio.RunloomEventLoop()
    fired = []
    loop.call_later(0.01, lambda: fired.append(1))
    _run_on_worker(loop)
    loop.close()
    assert fired == [1], fired


def test_prerun_create_task_is_nonblocking_schedule_then_start():
    """create_task from a foreign thread BEFORE the driver starts must NOT block
    (must-fix / Trap B) -- else 'schedule then start the worker' deadlocks.  We
    schedule on the main thread and only THEN start the worker; if create_task
    blocked, the worker would never start."""
    loop = aio.RunloomEventLoop()
    ran = []

    async def body():
        ran.append(1)

    # This call must return immediately (non-blocking) even though no thread is
    # driving the loop yet.
    task = loop.create_task(body())
    assert task is not None
    _run_on_worker(loop)               # worker starts AFTER the create_task
    loop.close()
    assert ran == [1], ran


def test_run_until_complete_own_pre_run_create_task_still_works():
    """Trap A: run_until_complete's OWN create_task(future) at entry (same
    thread, loop not yet running) must spawn directly, not deadlock."""
    async def main():
        return 7
    # aio.run -> run_until_complete(create_task(main())) on THIS thread.
    assert aio.run(main()) == 7


def test_prerun_create_task_under_task_factory():
    """must-fix #3: a foreign pre-run create_task with a custom task_factory
    (stock asyncio.Task) must run without an AttributeError at drain."""
    loop = aio.RunloomEventLoop()
    made = []

    def factory(lp, coro, **kw):
        made.append(1)
        return asyncio.Task(coro, loop=lp, **kw)
    loop.set_task_factory(factory)

    ran = []
    async def body():
        ran.append(1)
    loop.create_task(body())           # foreign, pre-run, factory installed
    _run_on_worker(loop)
    loop.close()
    # The factory is used for our body task AND the worker's own _stopper task,
    # so made has >=1 entry; the point is body RAN (no AttributeError at drain).
    assert len(made) >= 1 and ran == [1], (made, ran)


def test_prerun_task_cancelled_before_spawn_settles_clean():
    """must-fix: a task cancelled before its deferred spawn settles CANCELLED,
    never runs its coro, and doesn't wedge -- the loop still completes."""
    loop = aio.RunloomEventLoop()
    ran = []

    async def body():
        ran.append(1)                  # must NOT run
    task = loop.create_task(body())    # foreign, pre-run -> deferred spawn
    task.cancel()                      # cancel BEFORE the driver spawns it
    _run_on_worker(loop)
    loop.close()
    assert ran == [], "cancelled-before-spawn task should not have run"
    assert task.cancelled(), "task should be settled CANCELLED"


def test_prerun_deferred_task_no_refcycle():
    """must-fix #2: a completed pre-run task must be collectable without an
    explicit gc.collect() -- i.e. self._body must be cleared so no
    task->_body->self cycle survives refcounting."""
    import weakref
    loop = aio.RunloomEventLoop()
    ref = {}

    async def body():
        return 1
    t = loop.create_task(body())
    ref["w"] = weakref.ref(t)
    _run_on_worker(loop)
    loop.close()
    del t
    # No gc.collect(): if _body (a bound method whose __self__ is the task) were
    # retained, the task would survive refcounting and this weakref would stay
    # alive.
    assert ref["w"]() is None, "completed pre-run task not collected by refcount"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
