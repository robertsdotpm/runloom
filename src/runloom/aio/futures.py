"""_RunloomFutureMixin + RunloomFuture: synchronous-fire Future replacement."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .handles import _CANCELLED, _FINISHED, _PENDING, _STOCK_TASK_TYPES, _run_stock_task_cb  # noqa: F401
# RunloomTask is imported lazily inside _fire_callbacks (below) to avoid a
# futures<->tasks import cycle: tasks subclasses _RunloomFutureMixin from here.

class _RunloomFutureMixin(object):
    """Shared Future logic for RunloomFuture (over asyncio.Future) and RunloomTask
    (over asyncio.Task).

    Why subclass the real asyncio types at all: libraries check
    `isinstance(x, asyncio.Future)` / `asyncio.Task)` (e.g. aiomisc's
    cancel_tasks) and SKIP objects that aren't.  asyncio's own C fast paths only
    fire for CheckExact instances, so a *subclass* gets the generic path that
    calls these public Python methods -- our overrides win and the C state
    fields are never read.

    Why a mixin + _pg* names: the C Future/Task expose _state/_result/_coro/
    _fut_waiter/... as READ-ONLY descriptors, so we can't store our state under
    those names.  We keep our own state in _pg* attrs and override every method.
    `_asyncio_future_blocking` and `_loop` ARE usable (the C base's __init__
    initialises them) so we leave those on the C object.
    """

    def _pg_future_init(self):
        self._pgstate = _PENDING
        self._pgresult = None
        self._pgexc = None
        self._pgcbs = []
        self._pgcancelmsg = None
        # The actual CancelledError instance a cancelled coroutine raised, so
        # result()/exception() re-raise the SAME object (identity + chained
        # context), matching asyncio.Future._cancelled_exc.  None until set.
        self._pg_cancelled_exc = None
        # asyncio's "exception was never retrieved" tracking (libraries assert
        # on _log_traceback).  Our own copy -- the C _log_traceback descriptor
        # forbids being set True.
        self._pglogtb = False

    # ---- query ----
    def done(self):       return self._pgstate != _PENDING
    def cancelled(self):  return self._pgstate == _CANCELLED

    def result(self):
        if self._pgstate == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._pgstate == _CANCELLED:
            raise self._make_cancelled_error()
        self._pglogtb = False
        if self._pgexc is not None:
            raise self._pgexc
        return self._pgresult

    def exception(self):
        if self._pgstate == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._pgstate == _CANCELLED:
            raise self._make_cancelled_error()
        self._pglogtb = False
        return self._pgexc

    def __repr__(self):
        # asyncio-compatible repr.  RunloomFuture/RunloomTask are drop-in asyncio
        # Future/Task; code and tests inspect the repr and expect the asyncio
        # spelling -- aiohttp's test_format_task_get asserts
        # f"{task}".startswith("<Task pending"), and StreamReader.__repr__
        # embeds repr(waiter) expecting "<Future pending>".  So present as
        # Future/Task, not the RunloomFuture/RunloomTask implementation class name
        # that asyncio.Future.__repr__ would otherwise emit.
        state = ("pending" if self._pgstate == _PENDING else
                 "cancelled" if self._pgstate == _CANCELLED else "finished")
        if isinstance(self, asyncio.Task):
            info = ["Task", state, "name=%r" % self._pgname]
            coro = getattr(self, "_pgcoro", None)
            if coro is not None:
                info.append("coro=%r" % (coro,))
        else:
            info = ["Future", state]
            if self._pgstate == _FINISHED:
                if self._pgexc is not None:
                    info.append("exception=%r" % (self._pgexc,))
                else:
                    info.append("result=%r" % (self._pgresult,))
        return "<%s>" % " ".join(info)

    @property
    def _log_traceback(self):
        return self._pglogtb

    @_log_traceback.setter
    def _log_traceback(self, val):
        # Some asyncio code sets this False; honour False, ignore True coming
        # from outside (we set _pglogtb ourselves in set_exception).
        if not val:
            self._pglogtb = False

    # Map the C Future's read-only descriptor NAMES to our _pg* state, so code
    # that pokes the "private" attributes directly (e.g. async-lru reads
    # task._exception to avoid clearing _log_traceback) sees our real state, not
    # the never-updated C fields.  These properties shadow the C descriptors
    # because the mixin precedes asyncio.Future/Task in the MRO.
    @property
    def _exception(self):
        return self._pgexc

    @property
    def _result(self):
        return self._pgresult

    @property
    def _callbacks(self):
        return self._pgcbs

    @property
    def _state(self):
        s = self._pgstate
        return ("PENDING" if s == _PENDING else
                "FINISHED" if s == _FINISHED else "CANCELLED")

    # ---- mutation ----
    def set_result(self, result):
        if self._pgstate != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        self._pgresult = result
        self._pgstate  = _FINISHED
        self._fire_callbacks()

    def set_exception(self, exception):
        if self._pgstate != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError(
                "StopIteration interacts badly with generators "
                "and cannot be raised into a Future")
        self._pgexc = exception
        self._pgstate = _FINISHED
        self._pglogtb = True
        self._fire_callbacks()

    def __del__(self):
        # "exception was never retrieved" warning, now that a completed task is
        # collectable (upstream c9e1db2 releases g->callable at fiber
        # completion, breaking the task->_g->callable->task cycle).  Keep it
        # side-effect-free: for a fire-and-forget task whose only ref is
        # g->callable, this runs in the fiber's own completion context, so
        # we must NOT re-enter the scheduler -- a plain call_exception_handler
        # (logging) is fine.
        # Defensive against a half-constructed object: RunloomTask.__init__
        # rejects a non-coroutine with TypeError BEFORE _pg_future_init() sets
        # these attrs, so a rejected create_task(non_coro) leaves an instance
        # whose __del__ must not AttributeError at GC ("Exception ignored in
        # __del__" noise on every bad create_task).  Missing _pglogtb -> nothing
        # to report.
        if not getattr(self, "_pglogtb", False) or getattr(self, "_pgexc", None) is None:
            return
        loop = self._loop
        # Read the _closed ATTRIBUTE, not the is_closed() method: asyncio's
        # internal machinery never invokes the (user-overridable) is_closed()
        # for its own checks, and janus's test_closed_loop_non_failing asserts
        # an exact is_closed() call count -- calling the method here inflates it.
        if loop is None or getattr(loop, "_closed", False):
            return
        try:
            loop.call_exception_handler({
                "message": "%s exception was never retrieved"
                           % self.__class__.__name__,
                "exception": self._pgexc,
                "future": self,
            })
        except BaseException:
            pass

    def _pg_future_cancel(self, msg=None):
        if self._pgstate != _PENDING:
            return False
        self._pgcancelmsg = msg
        self._pgstate = _CANCELLED
        self._fire_callbacks()
        return True

    # RunloomFuture's public cancel IS the future-cancel; RunloomTask overrides it
    # with the task-cancel and uses _pg_future_cancel internally.
    cancel = _pg_future_cancel

    def _make_cancelled_error(self):
        # Preserve the exact CancelledError a cancelled coroutine raised (its
        # identity and __context__), exactly like asyncio.Future, so
        # `assertIs(awaited_exc, raised)` holds and chained context survives.
        exc = self._pg_cancelled_exc
        if exc is not None:
            self._pg_cancelled_exc = None
            return exc
        msg = self._pgcancelmsg
        if msg is None:
            return asyncio.CancelledError()
        return asyncio.CancelledError(msg)

    # ---- callbacks ----
    def add_done_callback(self, callback, *, context=None):
        if self._pgstate != _PENDING:
            # asyncio contract: a callback added to an ALREADY-DONE future is
            # scheduled via call_soon, NEVER run inline.  Library code depends on
            # this -- e.g. asyncio.as_completed adds _handle_completion to each
            # future inside its own setup loop; firing it synchronously re-enters
            # before _todo exists (AttributeError) and the async-for hangs.  (The
            # PENDING->done path in _fire_callbacks stays synchronous on purpose
            # for runloom's wake timing; only THIS already-done path must defer.)
            loop = self._loop
            if loop is not None and not getattr(loop, "_closed", False):
                try:
                    loop.call_soon(callback, self, context=context)
                except BaseException as e:
                    self._report_exc(e)
            else:
                # No usable loop (teardown): best-effort inline.
                try:
                    if context is None:
                        callback(self)
                    else:
                        context.run(callback, self)
                except BaseException as e:
                    self._report_exc(e)
        else:
            self._pgcbs.append((callback, context))

    def remove_done_callback(self, callback):
        filtered = [(cb, ctx) for cb, ctx in self._pgcbs if cb is not callback]
        removed  = len(self._pgcbs) - len(filtered)
        self._pgcbs = filtered
        return removed

    def _fire_callbacks(self):
        from .tasks import RunloomTask   # lazy: breaks the futures<->tasks cycle
        cbs, self._pgcbs = self._pgcbs, []
        loop = self._loop
        live = loop is not None and not getattr(loop, "_closed", False)
        for cb, ctx in cbs:
            # asyncio's Future.__schedule_callbacks defers EVERY done-callback
            # through loop.call_soon -- a waiting Task's __wakeup AND library/user
            # done-callbacks (gather's _done_callback, aiojobs' job _done_callback,
            # ...). So a setter that completes a future and KEEPS RUNNING, or a
            # task whose own done-callback mutates shared state, is observed in
            # asyncio order: the awaiter that was scheduled first (by an earlier
            # set_result) resumes BEFORE the future's other, later done-callbacks.
            # runloom keeps exactly ONE callback synchronous: RunloomTask._wake_unpark,
            # its own await-wake primitive -- deferring it would spawn a fiber
            # per await and break park/unpark (it only readies the g, which is
            # itself FIFO-after an already-readied waiter, so ordering still
            # holds). A runloom-internal control callback may opt back into sync via
            # the `_runloom_fire_sync` marker (the run loop's _stop_on_done). EVERY
            # other callback is deferred to match asyncio.
            #
            # Stock C-Task/_PyTask __wakeup additionally MUST go through the
            # trampoline, never fire synchronously: the C Task mishandles re-entry
            # (never reschedules __step -> awaiter hangs) and a _PyTask whose
            # _context is already entered higher on the stack raises "cannot enter
            # context". Firing library callbacks synchronously inverted the order
            # above and broke aiojobs (close() exception propagation + pending-job
            # promotion) and the falcon/uvicorn websocket-close ordering.
            host = getattr(cb, "__self__", None)
            sync = (isinstance(host, RunloomTask)
                    or getattr(cb, "_runloom_fire_sync", False)
                    or not live)
            if not sync:
                try:
                    if isinstance(host, _STOCK_TASK_TYPES):
                        loop.call_soon(_run_stock_task_cb, loop, cb, self,
                                       context=ctx)
                    else:
                        loop.call_soon(cb, self, context=ctx)
                except BaseException as e:
                    self._report_exc(e)
                continue
            # Synchronous: RunloomTask._wake_unpark, a marked internal control
            # callback, or the loop is gone (nothing to defer onto).
            try:
                if ctx is None:
                    cb(self)
                else:
                    ctx.run(cb, self)
            except RuntimeError as e:
                # Defensive net for a callback registered with a context that is
                # already entered higher on this stack (a future completed
                # synchronously from inside that very context). Context.run
                # rejects re-entry BEFORE invoking cb, so cb has NOT run;
                # deferring to the next loop tick (the context has exited by
                # then) mirrors asyncio's always-call_soon dispatch rather than
                # dropping the wake and hanging the awaiter.
                if (ctx is not None and loop is not None
                        and not getattr(loop, "_closed", False)
                        and str(e).startswith("cannot enter context")):
                    try:
                        loop.call_soon(cb, self, context=ctx)
                    except BaseException as e2:
                        self._report_exc(e2)
                else:
                    self._report_exc(e)
            except BaseException as e:
                self._report_exc(e)

    def _report_exc(self, e):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "exception in RunloomFuture callback",
                "exception": e,
                "future": self,
            })

    # ---- await protocol ----
    def __await__(self):
        if self._pgstate == _PENDING:
            self._asyncio_future_blocking = True
            yield self
            assert self._pgstate != _PENDING
        return self.result()

    __iter__ = __await__


class RunloomFuture(_RunloomFutureMixin, asyncio.Future):
    """A real asyncio.Future subclass with runloom's synchronous-callback
    dispatch.  isinstance(x, asyncio.Future) holds; asyncio uses our overridden
    methods (subclasses miss the C fast paths)."""

    def __init__(self, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        # Initialise the C Future (gives us a valid _loop + _asyncio_future_
        # blocking field).  Its _state stays PENDING forever -- asyncio reads
        # our done()/result() instead, and a PENDING C Future doesn't warn at
        # GC (only Tasks do).
        asyncio.Future.__init__(self, loop=loop)
        self._asyncio_future_blocking = False
        self._pg_future_init()


def _fut_cancelled_error(fut):
    """Build the CancelledError to throw into a coroutine whose awaited future
    was cancelled, PRESERVING the future's cancel message.  Both RunloomFuture and
    stdlib asyncio.Future expose _make_cancelled_error() (3.9+); fall back to a
    bare CancelledError for any exotic awaitable that lacks it."""
    mk = getattr(fut, "_make_cancelled_error", None)
    if mk is not None:
        try:
            return mk()
        except BaseException:
            pass
    return asyncio.CancelledError()
