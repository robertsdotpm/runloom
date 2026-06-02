"""Handle/TimerHandle, future-state constants, task registries, and the
stock-task interop shims shared by futures, tasks and the loop."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# Stock asyncio.Task types whose wakeups must be DEFERRED (scheduled via
# call_soon), never run synchronously from inside a future's set_result/cancel.
# Two distinct stock implementations exist: the C `_asyncio.Task` (exposed as
# asyncio.Task) and the pure-Python `asyncio.tasks._PyTask` -- and the Python
# one is NOT a subclass of the C one, so an `isinstance(host, asyncio.Task)`
# check alone misses every _PyTask (CPython's own test_asyncio drives many).
# We list BOTH; PygoTask (our own) is excluded at the call site since it wants
# synchronous wakes.  See _fire_callbacks.
_STOCK_TASK_TYPES = (asyncio.Task,)
_PyTaskCls = getattr(asyncio.tasks, "_PyTask", None)
if _PyTaskCls is not None and _PyTaskCls is not asyncio.Task:
    _STOCK_TASK_TYPES = (asyncio.Task, _PyTaskCls)


def _pg_convert_future_exc(exc):
    """Convert a concurrent.futures exception to its asyncio twin when a
    concurrent.futures.Future result is marshalled into an asyncio Future
    (run_in_executor / wrap_future).  Reuses asyncio's own
    futures._convert_future_exc when present (handles CancelledError +
    InvalidStateError, version-correctly); falls back to a local mapping."""
    try:
        conv = asyncio.futures._convert_future_exc
    except AttributeError:
        conv = None
    if conv is not None:
        try:
            return conv(exc)
        except Exception:
            pass
    import concurrent.futures as _cf
    klass = type(exc)
    if klass is _cf.CancelledError:
        return asyncio.CancelledError(*exc.args).with_traceback(exc.__traceback__)
    ise = getattr(_cf, "InvalidStateError", None)
    if ise is not None and klass is ise:
        return asyncio.InvalidStateError(*exc.args).with_traceback(exc.__traceback__)
    return exc


def _run_stock_task_cb(loop, cb, fut):
    # Run a deferred stock-C-_asyncio.Task done-callback (its __wakeup) the way
    # asyncio's loop would: between task steps, with NO current task registered.
    #
    # The C Task's __wakeup -> task_step calls enter_task(loop, task), which
    # RAISES "Cannot enter into task X while another task Y is being executed"
    # if loop is already a key in _current_tasks.  Stock asyncio guarantees the
    # slot is empty when a call_soon callback runs.  pygo CANNOT: PygoTask._driver
    # keeps _current_tasks[loop] = self across the whole send/throw, and a task
    # that parks mid-step on a RAW scheduler primitive (pygo's transport I/O
    # does sock_recv/connect via pygo_core.wait_fd, not by yielding a future)
    # leaves its entry in place while the goroutine is switched out -- so this
    # deferred callback, scheduled onto another goroutine, would see a stale
    # "current" PygoTask and the stock Task.__wakeup would raise instead of
    # delivering the cancellation (the body-writer hangs forever).
    #
    # The parked PygoTask is suspended, not actually executing, so clearing its
    # slot for the duration of the (synchronous) stock-Task step is safe; we
    # restore it afterward so the PygoTask's own _driver finally still sees the
    # value it expects.  Single-thread sched per loop => no races on the swap.
    prev = _CURRENT_TASKS.pop(loop, None)
    try:
        cb(fut)
    finally:
        if prev is not None:
            _CURRENT_TASKS[loop] = prev


# Make our tasks visible to asyncio.all_tasks() (and debug tooling, anyio's
# get_running_tasks, etc.).  Use the register/unregister hooks rather than a
# specific set: 3.11 walked asyncio.tasks._all_tasks, but 3.12+ renamed it to
# _scheduled_tasks and all_tasks() enumerates THAT via _register_task -- so the
# old `_all_tasks` lookup AttributeError'd on 3.13 and our tasks never showed up.
try:
    _REGISTER_TASK = asyncio.tasks._register_task
    _UNREGISTER_TASK = asyncio.tasks._unregister_task
except AttributeError:
    _REGISTER_TASK = _UNREGISTER_TASK = None

# Default task names mirror stock asyncio's "Task-N" (some libraries -- e.g.
# aiojobs -- assert task.get_name().startswith("Task-")).
import itertools as _itertools
_TASK_NAME_COUNTER = _itertools.count(1)

# Every PygoTask, across ALL loops on this process.  The pygo scheduler is one
# per OS thread (shared by every PygoEventLoop on that thread), so loop.close()
# needs to know if a SIBLING loop still has live tasks before it drains the
# shared scheduler.  WeakSet so finished/collected tasks drop out on their own.
_PG_ALL_TASKS = _weakref.WeakSet()

# Every PygoEventLoop that has been constructed and not yet close()'d, across
# the process.  close()'s sched_reset() bulldozes the SHARED per-thread sleep
# heap + ready ring, which would drop another still-open loop's in-flight work
# -- and not just its tasks: a raw call_later timer goroutine (an asyncio.sleep
# that a server handler on a sibling loop is parked on) lives in that shared
# sleep heap too, invisible to the _PG_ALL_TASKS task guard.  So close() only
# resets when it is the LAST open loop (see _cancel_outstanding_tasks).  WeakSet
# so a loop that is GC'd without close() drops out on its own.
_PG_OPEN_LOOPS = _weakref.WeakSet()


# ====================================================================
# Handles -- minimal asyncio.Handle / asyncio.TimerHandle compat.
# ====================================================================
class _Handle(asyncio.Handle):
    """asyncio.Handle subclass, but created OUTSIDE the loop's call queue --
    pygo fires the callback from a goroutine after consulting `_cancelled`
    (which asyncio.Handle.cancel() sets).  Subclassing the real type so that
    `isinstance(h, asyncio.Handle)` holds -- libraries (e.g. aiocache) assert
    that loop.call_*() returns an asyncio.Handle."""
    def __init__(self, cb, args, loop, context=None):
        super().__init__(cb, args, loop, context)


class _TimerHandle(asyncio.TimerHandle):
    """asyncio.TimerHandle subclass (see _Handle).  `when` is informational --
    pygo schedules via a goroutine sched_sleep, not the loop's timer heap."""
    def __init__(self, cb, args, loop, when=0, context=None):
        super().__init__(when, cb, args, loop, context)


# ====================================================================
# PygoFuture -- pure-Python Future replacement with synchronous-fire
# callbacks.  Not a subclass of asyncio.Future (the C class blocks real
# method overrides); duck-types the future protocol asyncio uses.
#
# Why this exists: stock asyncio.Future.set_result schedules every
# done_callback through loop.call_soon -- one goroutine spawn per
# callback in our model.  At 10k concurrent tasks that's 30k+ goroutine
# spawns, more than asyncio's tight C-deque path can be beaten by.
#
# In a goroutine model the defer is unnecessary -- the callbacks we
# register are just "wake the parked goroutine" via try_send, which is
# reentrant-safe.  Firing inline turns the bridge from ~5x slower than
# asyncio (at high fan-out) into a real win.
#
# asyncio recognises us via the _asyncio_future_blocking duck-type
# protocol (used by ensure_future / isfuture / Task.__step).  No
# isinstance(asyncio.Future) checks rely on the class hierarchy in
# code paths we exercise.
# ====================================================================
_PENDING   = 0
_FINISHED  = 1
_CANCELLED = 2
