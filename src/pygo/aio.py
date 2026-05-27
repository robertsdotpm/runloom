"""pygo.aio -- async/await on the pygo scheduler.

Approach: each asyncio.Task gets its own pygo goroutine.  The goroutine
drives `coro.send()` itself; when the coro yields a pending Future,
the goroutine parks via a 1-buffered channel and resumes when the
Future's done_callback fires.  Cooperative switching between tasks is
a stack swap (~80 ns).

Measured perf characteristics (Python 3.12 on Linux, see
examples/bench_aio_io.py):
  * Multi-await chains (n=100 k=100 awaits each): ~1.9x faster
  * Deep recursive awaits (n=100 d=20): ~1.7x faster
  * Simple fan-out (10k tasks one sleep each): ~5x SLOWER

The wins come from amortizing PygoTask setup cost across many awaits.
The losses come from PygoTask creation + Chan alloc being heavier
than asyncio's tight C-deque dispatcher for one-await fan-outs.

For workloads dominated by per-task setup (asyncio-style microservice
request handlers), stick with vanilla asyncio.  For workloads with
significant per-task work (multi-await pipelines, recursive coroutine
trees, mixed monkey-patched sync I/O), the bridge wins.

The much-larger speedup our architecture allows (3-10x) requires
bypassing the asyncio.Future protocol entirely -- a separate project.

Compatibility:
  * asyncio.Future, asyncio.gather, asyncio.wait_for, asyncio.shield: work.
  * asyncio.sleep, asyncio.Lock, asyncio.Event, asyncio.Queue: work.
  * loop.add_reader / add_writer: work (level-triggered like asyncio's
    default selector loop, just driven by pygo's netpoll).
  * asyncio.start_server / open_connection (Transport+Protocol stack):
    NOT in this MVP -- for I/O, prefer `pygo.monkey.patch()` and write
    blocking-style socket code inside an `async def`.  Stack-switching
    means it just works.

Use:
    import pygo.aio as aio
    aio.install()                        # one-shot policy install
    asyncio.run(main())                  # routed through pygo

    # or directly:
    import pygo.aio as aio
    aio.run(main())                      # equivalent of asyncio.run

A user can also opt into the bridge per-call:
    loop = aio.PygoEventLoop()
    loop.run_until_complete(main())
"""
import asyncio
import sys
import time as _time

import pygo_core


# Python's per-thread C recursion counter is shared across all
# goroutines on the OS thread.  Phase B saves/restores it per-g, but
# the absolute limit is still global -- spawning thousands of tasks
# can hit RecursionError just from the depth of asyncio's frame chain
# (Task.__step -> coro.send -> awaitable.__await__ -> Future.__await__).
# Pygo's __init__.py bumps the limit when imported; pygo.aio is often
# imported standalone so we do the same here.
if sys.getrecursionlimit() < 1_000_000:
    sys.setrecursionlimit(1_000_000)


# asyncio's private "currently-running task per loop" registry.  This is
# what asyncio.current_task() reads, and several stdlib helpers
# (asyncio.timeouts, asyncio.shield, taskgroups) bail with
# "must be used inside a task" if the entry is missing.  We update it
# from PygoTask._driver around every send/throw.
try:
    _CURRENT_TASKS = asyncio.tasks._current_tasks
except AttributeError:
    # Very old Python -- fall back to a no-op dict; current_task() will
    # return None and asyncio.timeouts won't work, but the rest does.
    _CURRENT_TASKS = {}


# WeakSet that asyncio.all_tasks() walks.  Registering keeps debug
# tooling happy and lets external code see our tasks.
try:
    _ALL_TASKS = asyncio.tasks._all_tasks
except AttributeError:
    _ALL_TASKS = None


# ====================================================================
# Handles -- minimal asyncio.Handle / asyncio.TimerHandle compat.
# ====================================================================
class _Handle(object):
    """Stand-in for asyncio.Handle.  The backing goroutine consults
    _cancelled before firing the callback."""
    __slots__ = ("_cancelled", "_callback", "_args")
    def __init__(self, cb, args):
        self._cancelled = False
        self._callback  = cb
        self._args      = args
    def cancel(self):
        self._cancelled = True
    def cancelled(self):
        return self._cancelled


class _TimerHandle(_Handle):
    """asyncio.TimerHandle compat shim."""


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


class PygoFuture(object):
    """Duck-typed Future with synchronous-callback dispatch.

    Used by PygoEventLoop.create_future and as the base of PygoTask.
    Intentionally no __slots__ -- asyncio.gather and friends set extra
    attributes (_log_destroy_pending, _cancel_message, ...) on futures
    they adopt, and we need to accept whatever they throw at us."""

    def __init__(self, *, loop=None):
        self._state    = _PENDING
        self._result   = None
        self._exception = None
        self._callbacks = []
        self._loop     = loop
        # Required by asyncio's await protocol.  Task.__step sets this
        # to False when it adopts the future; we re-arm to True in
        # __await__ each time we suspend.
        self._asyncio_future_blocking = False

    # ---- query ----
    def done(self):       return self._state != _PENDING
    def cancelled(self):  return self._state == _CANCELLED
    def get_loop(self):   return self._loop

    def result(self):
        if self._state == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._state == _CANCELLED:
            raise asyncio.CancelledError()
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self):
        if self._state == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._state == _CANCELLED:
            raise asyncio.CancelledError()
        return self._exception

    # ---- mutation ----
    def set_result(self, result):
        if self._state != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        self._result = result
        self._state  = _FINISHED
        self._fire_callbacks()

    def set_exception(self, exception):
        if self._state != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError(
                "StopIteration interacts badly with generators "
                "and cannot be raised into a Future")
        self._exception = exception
        self._state     = _FINISHED
        self._fire_callbacks()

    def cancel(self, msg=None):
        if self._state != _PENDING:
            return False
        self._state = _CANCELLED
        self._fire_callbacks()
        return True

    # ---- callbacks ----
    def add_done_callback(self, callback, *, context=None):
        if self._state != _PENDING:
            # Already resolved -- fire this one callback immediately,
            # consistent with asyncio.Future's semantics for late
            # add_done_callback.
            try:
                callback(self)
            except BaseException as e:
                self._report_exc(e)
        else:
            self._callbacks.append((callback, context))

    def remove_done_callback(self, callback):
        filtered = [(cb, ctx) for cb, ctx in self._callbacks if cb is not callback]
        removed  = len(self._callbacks) - len(filtered)
        self._callbacks = filtered
        return removed

    def _fire_callbacks(self):
        cbs, self._callbacks = self._callbacks, []
        for cb, ctx in cbs:
            try:
                if ctx is None:
                    cb(self)
                else:
                    ctx.run(cb, self)
            except BaseException as e:
                self._report_exc(e)

    def _report_exc(self, e):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "exception in PygoFuture callback",
                "exception": e,
                "future": self,
            })

    # ---- await protocol ----
    def __await__(self):
        if self._state == _PENDING:
            self._asyncio_future_blocking = True
            yield self
            assert self._state != _PENDING
        return self.result()

    # Generators implement __iter__; tasks expect that to exist for await.
    __iter__ = __await__


# ====================================================================
# PygoTask -- the heart of the bridge.
# ====================================================================
class PygoTask(PygoFuture):
    """asyncio.Task replacement.  Each task owns a goroutine that drives
    the coroutine; the Future side exposes the asyncio-visible state
    so external code can `await task` etc.

    Subclasses PygoFuture, not asyncio.Future -- the C Future class
    forbids real method overrides and we need set_result/set_exception/
    cancel to fire callbacks synchronously (otherwise our gather-in-
    flight done-callback cascade goes through N call_soon -> N
    goroutine spawns and the bridge ends up slower than asyncio).
    """

    def __init__(self, coro, *, loop=None, name=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        super().__init__(loop=loop)
        self._coro    = coro
        self._name    = name or "pygo-task"
        # 1-buffered channel: every wake (done_callback OR cancel)
        # try_sends one byte; the driver does a single blocking recv.
        # try_send on full channel is a no-op, which matches the
        # "level-triggered wake" we want for repeated cancel calls.
        self._wake_ch = pygo_core.Chan(1)
        self._cancel_requested = False
        # cancelling()/uncancel() are required by asyncio.timeouts in
        # 3.11+.  Mirrors asyncio.Task: count of unresolved cancels.
        self._num_cancels_requested = 0
        # Register in asyncio.all_tasks() for introspection parity.
        if _ALL_TASKS is not None:
            try:
                _ALL_TASKS.add(self)
            except TypeError:
                pass
        # Off we go.  The goroutine owns the coro from here.
        self._g = pygo_core.go(self._driver)

    # ---- asyncio.Task surface ----
    def get_name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    def get_coro(self):
        return self._coro

    def cancel(self, msg=None):
        if self.done():
            return False
        self._cancel_requested = True
        self._num_cancels_requested += 1
        # Unblock the driver so it sees _cancel_requested on next iter.
        self._wake_ch.try_send(None)
        return True

    def cancelling(self):
        """Number of unresolved cancel() calls.  Required by
        asyncio.timeouts / asyncio.TaskGroup in 3.11+."""
        return self._num_cancels_requested

    def uncancel(self):
        """Decrement the cancelling counter.  When it returns to zero,
        clear the cancel-requested flag so the driver stops trying to
        raise CancelledError."""
        if self._num_cancels_requested > 0:
            self._num_cancels_requested -= 1
        if self._num_cancels_requested == 0:
            self._cancel_requested = False
        return self._num_cancels_requested

    # ---- driver: the per-task goroutine body ----
    def _driver(self):
        coro       = self._coro
        send_value = None
        throw_exc  = None

        loop = self._loop

        while True:
            # --- advance the coroutine one step ---
            # Register as the loop's "current task" for the duration of
            # the send/throw.  asyncio.timeouts / current_task() rely on
            # this; without it stdlib helpers think we're not inside a
            # task and raise.
            prev_current = _CURRENT_TASKS.get(loop)
            _CURRENT_TASKS[loop] = self
            try:
                try:
                    if self._cancel_requested and throw_exc is None:
                        throw_exc = asyncio.CancelledError()
                    if throw_exc is not None:
                        e, throw_exc = throw_exc, None
                        yielded = coro.throw(e)
                    else:
                        yielded = coro.send(send_value)
                except StopIteration as si:
                    if not self.done():
                        self.set_result(si.value)
                    return
                except asyncio.CancelledError:
                    if not self.done():
                        super().cancel()
                    return
                except BaseException as e:
                    if not self.done():
                        self.set_exception(e)
                    return
            finally:
                if prev_current is None:
                    _CURRENT_TASKS.pop(loop, None)
                else:
                    _CURRENT_TASKS[loop] = prev_current

            send_value = None

            # --- classify the yielded value ---
            if yielded is None:
                # Bare `yield` (asyncio.sleep(0) shortcut, or any other
                # cooperative checkpoint).  Round-trip through the
                # scheduler so other tasks can run.
                pygo_core.sched_yield_classic()
                continue

            blocking = getattr(yielded, "_asyncio_future_blocking", None)
            if blocking is not True:
                # asyncio's contract: anything yielded from `await` must
                # be a Future-like with _asyncio_future_blocking set to
                # True.  If we get something else, the coro is buggy or
                # used a non-asyncio awaitable; raise into it.
                throw_exc = RuntimeError(
                    "yielded a non-asyncio object from await: %r" % (yielded,))
                continue

            # Mark we've registered our interest (mirrors Task.__step).
            yielded._asyncio_future_blocking = False

            # Fast path: future already resolved at yield time.  Skip
            # the park entirely.  This is the common case for
            # asyncio.gather of finished tasks.
            if yielded.done():
                try:
                    if yielded.cancelled():
                        throw_exc = asyncio.CancelledError()
                    elif yielded.exception() is not None:
                        throw_exc = yielded.exception()
                    else:
                        send_value = yielded.result()
                except asyncio.CancelledError:
                    throw_exc = asyncio.CancelledError()
                continue

            # Slow path: park the goroutine until the future fires.
            # We register a done_callback that try_sends one byte;
            # the recv below blocks the goroutine cooperatively.
            yielded.add_done_callback(self._wake_unpark)
            self._wake_ch.recv()

            # We're back.  Either the future is done, or we were
            # cancelled; figure out which.
            if self._cancel_requested:
                try:
                    yielded.remove_done_callback(self._wake_unpark)
                except Exception:
                    pass
                throw_exc = asyncio.CancelledError()
                continue

            try:
                if yielded.cancelled():
                    throw_exc = asyncio.CancelledError()
                elif yielded.exception() is not None:
                    throw_exc = yielded.exception()
                else:
                    send_value = yielded.result()
            except asyncio.CancelledError:
                throw_exc = asyncio.CancelledError()

    def _wake_unpark(self, fut):
        # add_done_callback gives us the future; we don't need it.
        try:
            self._wake_ch.try_send(None)
        except Exception:
            # Channel closed -- task already cleaned up.  Drop.
            pass


# ====================================================================
# PygoEventLoop -- asyncio.AbstractEventLoop with everything we need
# for sleep / gather / Future / Lock to function.
# ====================================================================
class PygoEventLoop(asyncio.AbstractEventLoop):

    def __init__(self):
        self._running = False
        self._closed  = False
        self._readers = {}
        self._writers = {}
        self._exception_handler = None

    # ---- state ----
    def is_running(self):  return self._running
    def is_closed(self):   return self._closed
    def get_debug(self):   return False
    def close(self):       self._closed = True

    def time(self):
        return _time.monotonic()

    # ---- task / future ----
    def create_task(self, coro, *, name=None, context=None):
        return PygoTask(coro, loop=self, name=name)

    def create_future(self):
        return PygoFuture(loop=self)

    # ---- callback scheduling ----
    def call_soon(self, callback, *args, context=None):
        handle = _Handle(callback, args)
        def runner():
            if not handle._cancelled:
                try:
                    callback(*args)
                except BaseException as e:
                    self.call_exception_handler({"message": "call_soon callback", "exception": e})
        # asyncio's done-callbacks (gather, wait_for) generally don't
        # yield -- they just walk children + set the outer future.
        # We use go_noyield to skip the per-g snap dance.  If a user
        # ever passes a callback that DOES yield, go_noyield's
        # behaviour is undefined; switch back to pygo_core.go.
        pygo_core.go(runner)
        return handle

    call_soon_threadsafe = call_soon

    def call_later(self, delay, callback, *args, context=None):
        handle = _TimerHandle(callback, args)
        loop_self = self
        def runner():
            pygo_core.sched_sleep(delay)
            if not handle._cancelled:
                try:
                    callback(*args)
                except BaseException as e:
                    # Keep this minimal -- printing a traceback from here
                    # can itself recurse if we're near the c_recursion limit.
                    sys.stderr.write("[pygo.aio] call_later cb: %r\n" % (e,))
        pygo_core.go(runner)
        return handle

    def call_at(self, when, callback, *args, context=None):
        delay = max(0.0, when - self.time())
        return self.call_later(delay, callback, *args, context=context)

    # ---- I/O readers / writers (level-triggered, matches selector loops) ----
    def add_reader(self, fd, callback, *args):
        self._add_io(fd, 1, callback, args, self._readers)

    def remove_reader(self, fd):
        return self._remove_io(fd, self._readers)

    def add_writer(self, fd, callback, *args):
        self._add_io(fd, 2, callback, args, self._writers)

    def remove_writer(self, fd):
        return self._remove_io(fd, self._writers)

    def _add_io(self, fd, evt, callback, args, table):
        if fd in table:
            table[fd]._cancelled = True
        handle = _Handle(callback, args)
        table[fd] = handle
        def runner():
            while not handle._cancelled:
                try:
                    pygo_core.wait_fd(fd, evt)
                except Exception:
                    return
                if handle._cancelled:
                    return
                try:
                    callback(*args)
                except Exception as e:
                    self.call_exception_handler({"message": "I/O callback", "exception": e})
                # Yield to scheduler before re-arming (mimic level-triggered).
                pygo_core.sched_yield_classic()
        pygo_core.go(runner)
        return handle

    def _remove_io(self, fd, table):
        h = table.pop(fd, None)
        if h is not None:
            h._cancelled = True
            return True
        return False

    # ---- run loop ----
    def run_until_complete(self, future):
        if asyncio.iscoroutine(future):
            future = self.create_task(future)
        elif not (isinstance(future, asyncio.Future)
                  or isinstance(future, PygoFuture)
                  or asyncio.isfuture(future)):
            raise TypeError("argument must be a Future or coroutine")

        self._running = True
        asyncio._set_running_loop(self)
        try:
            pygo_core.run()
        finally:
            self._running = False
            asyncio._set_running_loop(None)

        if not future.done():
            raise RuntimeError("event loop stopped before Future completed")
        return future.result()

    def run_forever(self):
        self._running = True
        asyncio._set_running_loop(self)
        try:
            pygo_core.run()
        finally:
            self._running = False
            asyncio._set_running_loop(None)

    def stop(self):
        # Schedule a sentinel task that just exits, in case run_forever
        # is waiting.  In practice users should call cancel() on tasks.
        pass

    # asyncio.run() shutdown protocol -- minimal no-ops so user code
    # written against asyncio.run works through `paio.install()`.
    async def shutdown_asyncgens(self):
        return None

    async def shutdown_default_executor(self, timeout=None):
        return None

    def get_task_factory(self):
        return None

    def set_task_factory(self, factory):
        pass

    # ---- exception handling ----
    def set_exception_handler(self, handler):
        self._exception_handler = handler

    def get_exception_handler(self):
        return self._exception_handler

    def default_exception_handler(self, context):
        msg = context.get("message", "unhandled exception")
        exc = context.get("exception")
        sys.stderr.write("[pygo.aio] %s: %r\n" % (msg, exc))
        if exc is not None:
            import traceback
            traceback.print_exception(type(exc), exc, exc.__traceback__,
                                      file=sys.stderr)

    def call_exception_handler(self, context):
        if self._exception_handler is not None:
            try:
                self._exception_handler(self, context)
                return
            except Exception:
                pass
        self.default_exception_handler(context)


# ====================================================================
# Policy + convenience entry points
# ====================================================================
class PygoEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    def __init__(self):
        self._loop = None

    def get_event_loop(self):
        if self._loop is None or self._loop.is_closed():
            self._loop = PygoEventLoop()
        return self._loop

    def set_event_loop(self, loop):
        self._loop = loop

    def new_event_loop(self):
        return PygoEventLoop()

    # Child-watcher stubs (asyncio asks for these on Unix).
    def get_child_watcher(self):
        return None

    def set_child_watcher(self, watcher):
        pass


def install():
    """Install PygoEventLoopPolicy globally.  After this, every
    `asyncio.run(...)` / `asyncio.new_event_loop()` returns a pygo
    loop instead of the stdlib selector / proactor loop."""
    asyncio.set_event_loop_policy(PygoEventLoopPolicy())


def run(coro, *, debug=False):
    """Drop-in for `asyncio.run`.  Creates a fresh PygoEventLoop,
    runs `coro` to completion, returns the result.  Caller doesn't
    need to call install() first."""
    loop = PygoEventLoop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
